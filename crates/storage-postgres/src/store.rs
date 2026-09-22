//! PostgreSQL implementations of ledger ports.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ironledger_domain::{
    Account, AccountId, AssetCode, AtomicAmount, IdempotencyKey, JournalEntry, LedgerEvent,
    TransactionId,
};
use ironledger_ledger::{
    AccountCommit, AccountRepo, BalanceRecord, BalanceRepo, CommitOutcome, ConsumerOffset,
    ConsumerRepo, ConsumerStatus, HealthCheck, IdempotencyRecord, IdempotencyRepo, JournalCommit,
    LedgerError, LedgerRepo, OutboxMessage, OutboxRepo, OutboxStats, Page,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::time::Duration;
use tracing::instrument;

use crate::convert::{
    account_from_row, amount_from_str, amount_to_str, event_from_payload, idempotency_from_row,
    kind_to_str, load_entry, policy_to_str, side_to_str, status_to_str,
};
use crate::error::StorageError;

fn sqlx_ledger(err: sqlx::Error) -> LedgerError {
    LedgerError::Storage(err.to_string())
}

/// Shared PostgreSQL connection pool and adapters.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    default_topic: String,
}

impl PostgresStore {
    /// Connect using `DATABASE_URL`.
    pub async fn connect(
        database_url: &str,
        default_topic: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            default_topic: default_topic.into(),
        })
    }

    /// Run workspace migrations.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../../migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Borrow the pool (for health checks and custom queries).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn insert_idempotency(
        tx: &mut Transaction<'_, Postgres>,
        record: &IdempotencyRecord,
    ) -> Result<CommitOutcome, StorageError> {
        let inserted = sqlx::query(
            r#"
            INSERT INTO idempotency_keys (scope, key, request_hash, response, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (scope, key) DO NOTHING
            "#,
        )
        .bind(&record.scope)
        .bind(record.key.as_str())
        .bind(record.request_hash.as_str())
        .bind(&record.response)
        .bind(record.created_at)
        .execute(&mut **tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(CommitOutcome::DuplicateIdempotencyKey);
        }
        Ok(CommitOutcome::Committed)
    }

    async fn insert_outbox(
        tx: &mut Transaction<'_, Postgres>,
        topic: &str,
        events: &[LedgerEvent],
    ) -> Result<(), StorageError> {
        for event in events {
            let payload = serde_json::to_value(event)?;
            sqlx::query(
                r#"
                INSERT INTO outbox (event_id, topic, partition_key, payload, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (event_id) DO NOTHING
                "#,
            )
            .bind(*event.event_id.as_uuid())
            .bind(topic)
            .bind(&event.aggregate_id)
            .bind(payload)
            .bind(event.created_at)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }

    async fn apply_postings_tx(
        tx: &mut Transaction<'_, Postgres>,
        entry: &JournalEntry,
    ) -> Result<(), LedgerError> {
        let now = Utc::now();
        for posting in &entry.postings {
            let account_row =
                sqlx::query(r#"SELECT policy, status FROM accounts WHERE id = $1 FOR UPDATE"#)
                    .bind(posting.account_id.into_uuid())
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(sqlx_ledger)?;
            let Some(account_row) = account_row else {
                return Err(LedgerError::NotFound {
                    entity: "account",
                    id: posting.account_id.to_string(),
                });
            };
            let policy = account_row
                .try_get::<String, _>("policy")
                .map_err(|err| LedgerError::Storage(err.to_string()))?;
            let status = account_row
                .try_get::<String, _>("status")
                .map_err(|err| LedgerError::Storage(err.to_string()))?;
            if status != "active" {
                return Err(LedgerError::Domain(
                    ironledger_domain::DomainError::AccountNotActive {
                        account_id: posting.account_id.to_string(),
                        status,
                    },
                ));
            }

            let balance_row = sqlx::query(
                r#"
                SELECT amount_atomic FROM balances
                WHERE account_id = $1 AND asset = $2
                FOR UPDATE
                "#,
            )
            .bind(posting.account_id.into_uuid())
            .bind(posting.asset.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(sqlx_ledger)?;

            let current = balance_row
                .as_ref()
                .map(|row| {
                    amount_from_str(
                        row.try_get::<String, _>("amount_atomic")
                            .map_err(|err| LedgerError::Storage(err.to_string()))?
                            .as_str(),
                    )
                    .map_err(LedgerError::from)
                })
                .transpose()?
                .unwrap_or(AtomicAmount::ZERO);
            let updated = current
                .checked_add(posting.amount)
                .map_err(LedgerError::Domain)?;
            if updated.is_negative() && policy == "non_negative" {
                return Err(LedgerError::Domain(
                    ironledger_domain::DomainError::InsufficientBalance {
                        account_id: posting.account_id.to_string(),
                        asset: posting.asset.to_string(),
                        balance: current.raw(),
                        delta: posting.amount.raw(),
                    },
                ));
            }

            if balance_row.is_some() {
                sqlx::query(
                    r#"
                    UPDATE balances SET amount_atomic = $3, updated_at = $4
                    WHERE account_id = $1 AND asset = $2
                    "#,
                )
                .bind(posting.account_id.into_uuid())
                .bind(posting.asset.as_str())
                .bind(amount_to_str(updated))
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(sqlx_ledger)?;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO balances (account_id, asset, amount_atomic, updated_at)
                    VALUES ($1, $2, $3, $4)
                    "#,
                )
                .bind(posting.account_id.into_uuid())
                .bind(posting.asset.as_str())
                .bind(amount_to_str(updated))
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(sqlx_ledger)?;
            }

            sqlx::query(
                r#"
                INSERT INTO postings (entry_id, account_id, asset, amount_atomic, side)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(entry.id.into_uuid())
            .bind(posting.account_id.into_uuid())
            .bind(posting.asset.as_str())
            .bind(amount_to_str(posting.amount))
            .bind(side_to_str(posting.side))
            .execute(&mut **tx)
            .await
            .map_err(sqlx_ledger)?;
        }
        Ok(())
    }
}

#[async_trait]
impl AccountRepo for PostgresStore {
    #[instrument(skip(self, commit))]
    async fn create(&self, commit: &AccountCommit) -> Result<CommitOutcome, LedgerError> {
        let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
        if Self::insert_idempotency(&mut tx, &commit.idempotency).await?
            == CommitOutcome::DuplicateIdempotencyKey
        {
            tx.rollback().await.ok();
            return Ok(CommitOutcome::DuplicateIdempotencyKey);
        }
        let account = &commit.account;
        let result = sqlx::query(
            r#"
            INSERT INTO accounts (id, name, kind, policy, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(account.id.into_uuid())
        .bind(&account.name)
        .bind(kind_to_str(account.kind))
        .bind(policy_to_str(account.policy))
        .bind(status_to_str(account.status))
        .bind(account.created_at)
        .execute(&mut *tx)
        .await;
        if let Err(sqlx::Error::Database(db)) = &result {
            if db.constraint() == Some("accounts_name_key") {
                tx.rollback().await.ok();
                return Err(LedgerError::Conflict(format!(
                    "account name '{}' is already taken",
                    account.name
                )));
            }
        }
        result.map_err(StorageError::from)?;
        Self::insert_outbox(&mut tx, &self.default_topic, &commit.events).await?;
        tx.commit().await.map_err(StorageError::from)?;
        Ok(CommitOutcome::Committed)
    }

    async fn find(&self, id: AccountId) -> Result<Option<Account>, LedgerError> {
        let row = sqlx::query(
            r#"SELECT id, name, kind, policy, status, created_at FROM accounts WHERE id = $1"#,
        )
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::from)?;
        row.as_ref()
            .map(account_from_row)
            .transpose()
            .map_err(Into::into)
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<Account>, LedgerError> {
        let row = sqlx::query(
            r#"SELECT id, name, kind, policy, status, created_at FROM accounts WHERE name = $1"#,
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::from)?;
        row.as_ref()
            .map(account_from_row)
            .transpose()
            .map_err(Into::into)
    }

    async fn load_many(&self, ids: &[AccountId]) -> Result<Vec<Account>, LedgerError> {
        let uuids: Vec<_> = ids.iter().map(|id| id.into_uuid()).collect();
        let rows = sqlx::query(
            r#"
            SELECT id, name, kind, policy, status, created_at
            FROM accounts
            WHERE id = ANY($1)
            "#,
        )
        .bind(&uuids)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        rows.iter()
            .map(account_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn list(&self, page: Page) -> Result<Vec<Account>, LedgerError> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, kind, policy, status, created_at
            FROM accounts
            ORDER BY name ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        rows.iter()
            .map(account_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[async_trait]
impl LedgerRepo for PostgresStore {
    async fn commit(&self, commit: &JournalCommit) -> Result<CommitOutcome, LedgerError> {
        let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
        if Self::insert_idempotency(&mut tx, &commit.idempotency).await?
            == CommitOutcome::DuplicateIdempotencyKey
        {
            tx.rollback().await.ok();
            return Ok(CommitOutcome::DuplicateIdempotencyKey);
        }
        let entry = &commit.entry;
        sqlx::query(
            r#"
            INSERT INTO journal_entries
                (id, idempotency_key, description, status, correlation_id, causation_id, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(entry.id.into_uuid())
        .bind(entry.idempotency_key.as_str())
        .bind(&entry.description)
        .bind("posted")
        .bind(entry.correlation_id.into_uuid())
        .bind(entry.causation_id.into_uuid())
        .bind(entry.created_at)
        .execute(&mut *tx)
        .await
        .map_err(StorageError::from)?;

        if let Err(err) = Self::apply_postings_tx(&mut tx, entry).await {
            tx.rollback().await.ok();
            return Err(err);
        }
        Self::insert_outbox(&mut tx, &self.default_topic, &commit.events).await?;
        tx.commit().await.map_err(StorageError::from)?;
        Ok(CommitOutcome::Committed)
    }

    async fn find_entry(&self, id: TransactionId) -> Result<Option<JournalEntry>, LedgerError> {
        load_entry(&self.pool, id).await.map_err(Into::into)
    }

    async fn list_account_entries(
        &self,
        account_id: AccountId,
        page: Page,
    ) -> Result<Vec<JournalEntry>, LedgerError> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT je.id
            FROM journal_entries je
            JOIN postings p ON p.entry_id = je.id
            WHERE p.account_id = $1
            ORDER BY je.created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(account_id.into_uuid())
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let id = TransactionId::from_uuid(row.try_get("id").map_err(StorageError::from)?);
            if let Some(entry) = load_entry(&self.pool, id).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

#[async_trait]
impl IdempotencyRepo for PostgresStore {
    async fn find(
        &self,
        scope: &str,
        key: &IdempotencyKey,
    ) -> Result<Option<IdempotencyRecord>, LedgerError> {
        let row = sqlx::query(
            r#"
            SELECT scope, key, request_hash, response, created_at
            FROM idempotency_keys
            WHERE scope = $1 AND key = $2
            "#,
        )
        .bind(scope)
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::from)?;
        row.as_ref()
            .map(idempotency_from_row)
            .transpose()
            .map_err(Into::into)
    }

    async fn purge_created_before(&self, cutoff: DateTime<Utc>) -> Result<u64, LedgerError> {
        let result = sqlx::query(r#"DELETE FROM idempotency_keys WHERE created_at < $1"#)
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl OutboxRepo for PostgresStore {
    async fn claim(&self, limit: u32, lease: Duration) -> Result<Vec<OutboxMessage>, LedgerError> {
        let lease_seconds = i64::try_from(lease.as_secs()).unwrap_or(300).max(1);
        let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, topic, partition_key, payload, attempts, created_at, published_at, last_error
            FROM outbox
            WHERE published_at IS NULL
              AND (leased_until IS NULL OR leased_until < NOW())
            ORDER BY id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(StorageError::from)?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get("id").map_err(StorageError::from)?;
            sqlx::query(
                r#"UPDATE outbox SET leased_until = NOW() + ($1 || ' seconds')::interval WHERE id = $2"#,
            )
            .bind(lease_seconds)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::from)?;
            let event_id: uuid::Uuid = row.try_get("event_id").map_err(StorageError::from)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(StorageError::from)?;
            let created_at: DateTime<Utc> =
                row.try_get("created_at").map_err(StorageError::from)?;
            let event = event_from_payload(event_id, payload, created_at)?;
            messages.push(OutboxMessage {
                id,
                topic: row.try_get("topic").map_err(StorageError::from)?,
                partition_key: row.try_get("partition_key").map_err(StorageError::from)?,
                event,
                attempts: row.try_get("attempts").map_err(StorageError::from)?,
                created_at,
                published_at: row.try_get("published_at").map_err(StorageError::from)?,
                last_error: row.try_get("last_error").map_err(StorageError::from)?,
            });
        }
        tx.commit().await.map_err(StorageError::from)?;
        Ok(messages)
    }

    async fn mark_published(&self, ids: &[i64]) -> Result<u64, LedgerError> {
        let result = sqlx::query(
            r#"
            UPDATE outbox
            SET published_at = NOW(), leased_until = NULL, last_error = NULL
            WHERE id = ANY($1) AND published_at IS NULL
            "#,
        )
        .bind(ids)
        .execute(&self.pool)
        .await
        .map_err(StorageError::from)?;
        Ok(result.rows_affected())
    }

    async fn mark_failed(&self, id: i64, error: &str) -> Result<(), LedgerError> {
        sqlx::query(
            r#"
            UPDATE outbox
            SET attempts = attempts + 1, last_error = $2, leased_until = NULL
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }

    async fn stats(&self) -> Result<OutboxStats, LedgerError> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE published_at IS NULL) AS pending,
                COUNT(*) FILTER (WHERE published_at IS NOT NULL) AS published,
                COUNT(*) FILTER (WHERE published_at IS NULL AND leased_until > NOW()) AS in_flight,
                COUNT(*) FILTER (WHERE published_at IS NULL AND attempts > 0) AS retrying,
                MIN(created_at) FILTER (WHERE published_at IS NULL) AS oldest_pending
            FROM outbox
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(StorageError::from)?;
        Ok(OutboxStats {
            pending: row.try_get("pending").map_err(StorageError::from)?,
            published: row.try_get("published").map_err(StorageError::from)?,
            in_flight: row.try_get("in_flight").map_err(StorageError::from)?,
            retrying: row.try_get("retrying").map_err(StorageError::from)?,
            oldest_pending_age_seconds: row
                .try_get::<Option<DateTime<Utc>>, _>("oldest_pending")
                .map_err(StorageError::from)?
                .map(|created| (Utc::now() - created).num_seconds().max(0)),
        })
    }

    async fn pending(&self, page: Page) -> Result<Vec<OutboxMessage>, LedgerError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, topic, partition_key, payload, attempts, created_at, published_at, last_error
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY id ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(page.limit_i64())
        .bind(page.offset_i64())
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id: uuid::Uuid = row.try_get("event_id").map_err(StorageError::from)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(StorageError::from)?;
            let created_at: DateTime<Utc> =
                row.try_get("created_at").map_err(StorageError::from)?;
            let event = event_from_payload(event_id, payload, created_at)?;
            messages.push(OutboxMessage {
                id: row.try_get("id").map_err(StorageError::from)?,
                topic: row.try_get("topic").map_err(StorageError::from)?,
                partition_key: row.try_get("partition_key").map_err(StorageError::from)?,
                event,
                attempts: row.try_get("attempts").map_err(StorageError::from)?,
                created_at,
                published_at: row.try_get("published_at").map_err(StorageError::from)?,
                last_error: row.try_get("last_error").map_err(StorageError::from)?,
            });
        }
        Ok(messages)
    }
}

#[async_trait]
impl BalanceRepo for PostgresStore {
    async fn get(
        &self,
        account_id: AccountId,
        asset: &AssetCode,
    ) -> Result<Option<BalanceRecord>, LedgerError> {
        let row = sqlx::query(
            r#"
            SELECT account_id, asset, amount_atomic, updated_at
            FROM balances WHERE account_id = $1 AND asset = $2
            "#,
        )
        .bind(account_id.into_uuid())
        .bind(asset.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::from)?;
        if let Some(row) = row {
            Ok(Some(BalanceRecord {
                account_id,
                asset: asset.clone(),
                amount: amount_from_str(
                    row.try_get::<String, _>("amount_atomic")
                        .map_err(StorageError::from)?
                        .as_str(),
                )?,
                updated_at: row.try_get("updated_at").map_err(StorageError::from)?,
            }))
        } else {
            Ok(None)
        }
    }

    async fn list_for_account(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<BalanceRecord>, LedgerError> {
        let rows = sqlx::query(
            r#"
            SELECT account_id, asset, amount_atomic, updated_at
            FROM balances WHERE account_id = $1 ORDER BY asset ASC
            "#,
        )
        .bind(account_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        rows.iter()
            .map(|row| {
                Ok(BalanceRecord {
                    account_id,
                    asset: AssetCode::new(row.try_get::<String, _>("asset")?)?,
                    amount: amount_from_str(row.try_get::<String, _>("amount_atomic")?.as_str())?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()
            .map_err(Into::into)
    }

    async fn snapshot(&self) -> Result<Vec<BalanceRecord>, LedgerError> {
        let rows = sqlx::query(
            r#"
            SELECT account_id, asset, amount_atomic, updated_at
            FROM balances ORDER BY account_id, asset
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        rows.iter()
            .map(|row| {
                Ok(BalanceRecord {
                    account_id: AccountId::from_uuid(row.try_get("account_id")?),
                    asset: AssetCode::new(row.try_get::<String, _>("asset")?)?,
                    amount: amount_from_str(row.try_get::<String, _>("amount_atomic")?.as_str())?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()
            .map_err(Into::into)
    }
}

#[async_trait]
impl ConsumerRepo for PostgresStore {
    async fn status(&self) -> Result<Vec<ConsumerStatus>, LedgerError> {
        let consumers = sqlx::query(
            r#"
            SELECT consumer, COUNT(*) AS processed_events, MAX(processed_at) AS last_processed_at
            FROM processed_events
            GROUP BY consumer
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        let mut out = Vec::with_capacity(consumers.len());
        for row in consumers {
            let consumer: String = row.try_get("consumer").map_err(StorageError::from)?;
            let offsets = sqlx::query(
                r#"
                SELECT topic, partition, offset, updated_at
                FROM consumer_offsets WHERE consumer = $1
                "#,
            )
            .bind(&consumer)
            .fetch_all(&self.pool)
            .await
            .map_err(StorageError::from)?;
            let offsets = offsets
                .iter()
                .map(|offset| {
                    Ok(ConsumerOffset {
                        topic: offset.try_get("topic")?,
                        partition: offset.try_get("partition")?,
                        offset: offset.try_get("offset")?,
                        updated_at: offset.try_get("updated_at")?,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()
                .map_err(StorageError::from)?;
            out.push(ConsumerStatus {
                consumer,
                processed_events: row
                    .try_get("processed_events")
                    .map_err(StorageError::from)?,
                last_processed_at: row
                    .try_get("last_processed_at")
                    .map_err(StorageError::from)?,
                offsets,
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl HealthCheck for PostgresStore {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn ping(&self) -> Result<(), LedgerError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
