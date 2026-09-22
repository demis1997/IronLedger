//! Domain ↔ SQL row conversions.

use chrono::{DateTime, Utc};
use ironledger_domain::{
    Account, AccountId, AccountKind, AccountPolicy, AccountStatus, AssetCode, AtomicAmount,
    CausationId, CorrelationId, DomainError, EntryStatus, EventId, IdempotencyKey, JournalEntry,
    LedgerEvent, Posting, PostingSide, TransactionId,
};
use ironledger_ledger::IdempotencyRecord;
use sqlx::Row;
use uuid::Uuid;

use crate::error::StorageError;

pub fn amount_to_str(amount: AtomicAmount) -> String {
    amount.raw().to_string()
}

pub fn amount_from_str(text: &str) -> Result<AtomicAmount, StorageError> {
    let raw: i128 = text.parse().map_err(|_| DomainError::InvalidMoneyFormat {
        value: text.to_owned(),
    })?;
    Ok(AtomicAmount::from_raw(raw))
}

pub fn kind_to_str(kind: AccountKind) -> &'static str {
    match kind {
        AccountKind::CustomerAvailable => "customer_available",
        AccountKind::CustomerLocked => "customer_locked",
        AccountKind::ExchangeHotWallet => "exchange_hot_wallet",
        AccountKind::ExchangeColdWallet => "exchange_cold_wallet",
        AccountKind::FeeRevenue => "fee_revenue",
        AccountKind::WithdrawalClearing => "withdrawal_clearing",
        AccountKind::DepositClearing => "deposit_clearing",
        AccountKind::Other => "other",
    }
}

pub fn kind_from_str(text: &str) -> Result<AccountKind, StorageError> {
    Ok(match text {
        "customer_available" => AccountKind::CustomerAvailable,
        "customer_locked" => AccountKind::CustomerLocked,
        "exchange_hot_wallet" => AccountKind::ExchangeHotWallet,
        "exchange_cold_wallet" => AccountKind::ExchangeColdWallet,
        "fee_revenue" => AccountKind::FeeRevenue,
        "withdrawal_clearing" => AccountKind::WithdrawalClearing,
        "deposit_clearing" => AccountKind::DepositClearing,
        "other" => AccountKind::Other,
        other => {
            return Err(DomainError::Invariant(format!("unknown account kind '{other}'")).into())
        }
    })
}

pub fn policy_to_str(policy: AccountPolicy) -> &'static str {
    match policy {
        AccountPolicy::NonNegative => "non_negative",
        AccountPolicy::AllowNegative => "allow_negative",
    }
}

pub fn policy_from_str(text: &str) -> Result<AccountPolicy, StorageError> {
    Ok(match text {
        "non_negative" => AccountPolicy::NonNegative,
        "allow_negative" => AccountPolicy::AllowNegative,
        other => {
            return Err(DomainError::Invariant(format!("unknown account policy '{other}'")).into())
        }
    })
}

pub fn status_to_str(status: AccountStatus) -> &'static str {
    match status {
        AccountStatus::Active => "active",
        AccountStatus::Frozen => "frozen",
        AccountStatus::Closed => "closed",
    }
}

pub fn status_from_str(text: &str) -> Result<AccountStatus, StorageError> {
    Ok(match text {
        "active" => AccountStatus::Active,
        "frozen" => AccountStatus::Frozen,
        "closed" => AccountStatus::Closed,
        other => {
            return Err(DomainError::Invariant(format!("unknown account status '{other}'")).into())
        }
    })
}

pub fn side_to_str(side: PostingSide) -> &'static str {
    match side {
        PostingSide::Debit => "debit",
        PostingSide::Credit => "credit",
    }
}

pub fn account_from_row(row: &sqlx::postgres::PgRow) -> Result<Account, StorageError> {
    Ok(Account {
        id: AccountId::from_uuid(row.try_get("id")?),
        name: row.try_get("name")?,
        kind: kind_from_str(row.try_get::<String, _>("kind")?.as_str())?,
        policy: policy_from_str(row.try_get::<String, _>("policy")?.as_str())?,
        status: status_from_str(row.try_get::<String, _>("status")?.as_str())?,
        created_at: row.try_get("created_at")?,
    })
}

pub fn idempotency_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<IdempotencyRecord, StorageError> {
    Ok(IdempotencyRecord {
        scope: row.try_get("scope")?,
        key: IdempotencyKey::new(row.try_get::<String, _>("key")?)?,
        request_hash: ironledger_ledger::RequestHash::from_hex(
            row.try_get::<String, _>("request_hash")?,
        ),
        response: row.try_get("response")?,
        created_at: row.try_get("created_at")?,
    })
}

pub fn event_from_payload(
    event_id: Uuid,
    payload: serde_json::Value,
    created_at: DateTime<Utc>,
) -> Result<LedgerEvent, StorageError> {
    let mut event: LedgerEvent = serde_json::from_value(payload)?;
    event.event_id = EventId::from_uuid(event_id);
    event.created_at = created_at;
    Ok(event)
}

pub async fn load_postings(
    pool: &sqlx::PgPool,
    entry_id: Uuid,
) -> Result<Vec<Posting>, StorageError> {
    let rows = sqlx::query(
        r#"
        SELECT account_id, asset, amount_atomic, side
        FROM postings
        WHERE entry_id = $1
        ORDER BY id ASC
        "#,
    )
    .bind(entry_id)
    .fetch_all(pool)
    .await?;
    let mut postings = Vec::with_capacity(rows.len());
    for row in rows {
        let side = match row.try_get::<String, _>("side")?.as_str() {
            "debit" => PostingSide::Debit,
            "credit" => PostingSide::Credit,
            other => {
                return Err(
                    DomainError::Invariant(format!("unknown posting side '{other}'")).into(),
                )
            }
        };
        postings.push(Posting {
            account_id: AccountId::from_uuid(row.try_get("account_id")?),
            asset: AssetCode::new(row.try_get::<String, _>("asset")?)?,
            amount: amount_from_str(row.try_get::<String, _>("amount_atomic")?.as_str())?,
            side,
        });
    }
    Ok(postings)
}

pub async fn load_entry(
    pool: &sqlx::PgPool,
    id: TransactionId,
) -> Result<Option<JournalEntry>, StorageError> {
    let row = sqlx::query(
        r#"
        SELECT id, idempotency_key, description, status, correlation_id, causation_id, created_at
        FROM journal_entries
        WHERE id = $1
        "#,
    )
    .bind(id.into_uuid())
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let entry_id: Uuid = row.try_get("id")?;
    let postings = load_postings(pool, entry_id).await?;
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "posted" => EntryStatus::Posted,
        "voided" => EntryStatus::Voided,
        other => {
            return Err(DomainError::Invariant(format!("unknown entry status '{other}'")).into())
        }
    };
    Ok(Some(JournalEntry {
        id: TransactionId::from_uuid(entry_id),
        idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)?,
        description: row.try_get("description")?,
        postings,
        status,
        correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
        causation_id: CausationId::from_uuid(row.try_get("causation_id")?),
        created_at: row.try_get("created_at")?,
    }))
}
