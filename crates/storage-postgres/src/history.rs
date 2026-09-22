//! Reconciliation adapters.

use async_trait::async_trait;
use ironledger_domain::{AccountId, AssetCode, JournalEntry};
use ironledger_ledger::BalanceRecord;
use ironledger_reconciler::{BalanceView, PostingHistory, ReconcileError};
use sqlx::{PgPool, Row};

use crate::convert::{amount_from_str, load_entry};
use crate::error::StorageError;

/// PostgreSQL posting history reader.
#[derive(Clone)]
pub struct PostgresHistory {
    pool: PgPool,
}

impl PostgresHistory {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PostingHistory for PostgresHistory {
    async fn all_entries(&self) -> Result<Vec<JournalEntry>, ReconcileError> {
        let rows = sqlx::query(r#"SELECT id FROM journal_entries ORDER BY created_at ASC, id ASC"#)
            .fetch_all(&self.pool)
            .await
            .map_err(StorageError::from)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let id = ironledger_domain::TransactionId::from_uuid(
                row.try_get("id").map_err(StorageError::from)?,
            );
            if let Some(entry) = load_entry(&self.pool, id).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

/// Compare authoritative and projection balances.
#[derive(Clone)]
pub struct PostgresBalanceViews {
    pool: PgPool,
}

impl PostgresBalanceViews {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BalanceView for PostgresBalanceViews {
    async fn snapshot(&self) -> Result<Vec<BalanceRecord>, ReconcileError> {
        let rows = sqlx::query(
            r#"
            SELECT account_id, asset, amount_atomic, updated_at FROM balances
            ORDER BY account_id, asset
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

    async fn projection_snapshot(
        &self,
        consumer: &str,
    ) -> Result<Vec<BalanceRecord>, ReconcileError> {
        let rows = sqlx::query(
            r#"
            SELECT account_id, asset, amount_atomic, updated_at
            FROM projection_balances
            WHERE consumer = $1
            ORDER BY account_id, asset
            "#,
        )
        .bind(consumer)
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
