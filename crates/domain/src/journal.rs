//! Double-entry journal entries and postings.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::amount::AtomicAmount;
use crate::asset::AssetCode;
use crate::error::DomainError;
use crate::ids::{AccountId, CausationId, CorrelationId, IdempotencyKey, TransactionId};

/// Debit or credit orientation for reporting; signed amounts remain the source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostingSide {
    /// Debit (positive signed amount by convention in this ledger).
    Debit,
    /// Credit (negative signed amount by convention in this ledger).
    Credit,
}

/// Single leg of a journal entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    /// Account receiving the delta.
    pub account_id: AccountId,
    /// Asset moved.
    pub asset: AssetCode,
    /// Signed atomic delta applied to the account balance.
    pub amount: AtomicAmount,
    /// Reporting side derived from the sign of `amount` at construction.
    pub side: PostingSide,
}

impl Posting {
    /// Build a posting from a signed amount. Zero amounts are rejected.
    pub fn new(
        account_id: AccountId,
        asset: AssetCode,
        amount: AtomicAmount,
    ) -> Result<Self, DomainError> {
        if amount.is_zero() {
            return Err(DomainError::NonPositiveAmount { amount: 0 });
        }
        let side = if amount.is_positive() {
            PostingSide::Debit
        } else {
            PostingSide::Credit
        };
        Ok(Self {
            account_id,
            asset,
            amount,
            side,
        })
    }

    /// Convenience debit (positive amount).
    pub fn debit(
        account_id: AccountId,
        asset: AssetCode,
        amount: AtomicAmount,
    ) -> Result<Self, DomainError> {
        let amount = amount.require_positive()?;
        Self::new(account_id, asset, amount)
    }

    /// Convenience credit (stored as negative amount).
    pub fn credit(
        account_id: AccountId,
        asset: AssetCode,
        amount: AtomicAmount,
    ) -> Result<Self, DomainError> {
        let amount = amount.require_positive()?.checked_neg()?;
        Self::new(account_id, asset, amount)
    }
}

/// Lifecycle of a persisted journal entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    /// Accepted and posted.
    Posted,
    /// Reserved for future multi-phase flows.
    Voided,
}

/// Balanced multi-posting journal entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Transaction id.
    pub id: TransactionId,
    /// Idempotency key for the originating command.
    pub idempotency_key: IdempotencyKey,
    /// Human-readable description / flow name.
    pub description: String,
    /// Postings (validated to balance per asset).
    pub postings: Vec<Posting>,
    /// Status.
    pub status: EntryStatus,
    /// Correlation across services.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

impl JournalEntry {
    /// Validate and construct a posted journal entry.
    pub fn new(
        id: TransactionId,
        idempotency_key: IdempotencyKey,
        description: impl Into<String>,
        postings: Vec<Posting>,
        correlation_id: CorrelationId,
        causation_id: CausationId,
    ) -> Result<Self, DomainError> {
        validate_balanced(&postings)?;
        Ok(Self {
            id,
            idempotency_key,
            description: description.into(),
            postings,
            status: EntryStatus::Posted,
            correlation_id,
            causation_id,
            created_at: Utc::now(),
        })
    }

    /// Per-asset signed sums (must all be zero for a valid entry).
    pub fn asset_totals(
        postings: &[Posting],
    ) -> Result<BTreeMap<AssetCode, AtomicAmount>, DomainError> {
        let mut totals: BTreeMap<AssetCode, AtomicAmount> = BTreeMap::new();
        for posting in postings {
            let entry = totals
                .entry(posting.asset.clone())
                .or_insert(AtomicAmount::ZERO);
            *entry = entry.checked_add(posting.amount)?;
        }
        Ok(totals)
    }
}

/// Ensure ≥2 postings and zero residual per asset.
pub fn validate_balanced(postings: &[Posting]) -> Result<(), DomainError> {
    if postings.len() < 2 {
        return Err(DomainError::InsufficientPostings {
            count: postings.len(),
        });
    }
    let totals = JournalEntry::asset_totals(postings)?;
    for (asset, total) in totals {
        if !total.is_zero() {
            return Err(DomainError::UnbalancedEntry {
                asset: asset.to_string(),
                residual: total.raw(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::AssetCode;

    fn amt(v: i128) -> AtomicAmount {
        AtomicAmount::from_raw(v)
    }

    #[test]
    fn balanced_two_leg_entry() {
        let a = AccountId::new();
        let b = AccountId::new();
        let postings = vec![
            Posting::debit(a, AssetCode::btc(), amt(100)).unwrap(),
            Posting::credit(b, AssetCode::btc(), amt(100)).unwrap(),
        ];
        assert!(validate_balanced(&postings).is_ok());
    }

    #[test]
    fn unbalanced_rejected() {
        let a = AccountId::new();
        let b = AccountId::new();
        let postings = vec![
            Posting::debit(a, AssetCode::btc(), amt(100)).unwrap(),
            Posting::credit(b, AssetCode::btc(), amt(99)).unwrap(),
        ];
        assert!(matches!(
            validate_balanced(&postings),
            Err(DomainError::UnbalancedEntry { .. })
        ));
    }

    #[test]
    fn multi_asset_must_balance_each() {
        let a = AccountId::new();
        let b = AccountId::new();
        let postings = vec![
            Posting::debit(a, AssetCode::btc(), amt(1)).unwrap(),
            Posting::credit(b, AssetCode::btc(), amt(1)).unwrap(),
            Posting::debit(a, AssetCode::usd(), amt(50)).unwrap(),
            Posting::credit(b, AssetCode::usd(), amt(50)).unwrap(),
        ];
        assert!(validate_balanced(&postings).is_ok());
    }

    #[test]
    fn single_posting_rejected() {
        let a = AccountId::new();
        let postings = vec![Posting::debit(a, AssetCode::btc(), amt(1)).unwrap()];
        assert!(matches!(
            validate_balanced(&postings),
            Err(DomainError::InsufficientPostings { count: 1 })
        ));
    }
}
