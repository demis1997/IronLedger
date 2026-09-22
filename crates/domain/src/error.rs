//! Domain error taxonomy.

use thiserror::Error;

/// Errors produced by domain validation and invariant checks.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// Identifier parse failure.
    #[error("invalid {kind} identifier: {value}")]
    InvalidIdentifier {
        /// Identifier kind name.
        kind: &'static str,
        /// Provided value.
        value: String,
    },

    /// Idempotency key rejected.
    #[error("invalid idempotency key: {reason}")]
    InvalidIdempotencyKey {
        /// Reason.
        reason: String,
    },

    /// Asset code rejected.
    #[error("invalid asset code '{code}': {reason}")]
    InvalidAssetCode {
        /// Code.
        code: String,
        /// Reason.
        reason: String,
    },

    /// Asset scale out of range.
    #[error("invalid asset scale {scale}; must be 0..=18")]
    InvalidAssetScale {
        /// Provided scale.
        scale: u8,
    },

    /// Money string parse failure.
    #[error("invalid money format: {value}")]
    InvalidMoneyFormat {
        /// Input.
        value: String,
    },

    /// Integer overflow in money arithmetic.
    #[error("amount arithmetic overflow")]
    AmountOverflow,

    /// Amount must be strictly positive.
    #[error("amount must be positive, got {amount}")]
    NonPositiveAmount {
        /// Raw amount.
        amount: i128,
    },

    /// Mixed assets where same asset required.
    #[error("asset mismatch: {left} vs {right}")]
    AssetMismatch {
        /// Left asset.
        left: String,
        /// Right asset.
        right: String,
    },

    /// Journal entry has fewer than two postings.
    #[error("journal entry requires at least two postings, got {count}")]
    InsufficientPostings {
        /// Count provided.
        count: usize,
    },

    /// Per-asset balance is non-zero.
    #[error("unbalanced journal entry for asset {asset}: residual {residual}")]
    UnbalancedEntry {
        /// Asset that does not balance.
        asset: String,
        /// Residual signed sum.
        residual: i128,
    },

    /// Account would go negative without permission.
    #[error("insufficient balance for account {account_id} asset {asset}: balance {balance}, delta {delta}")]
    InsufficientBalance {
        /// Account.
        account_id: String,
        /// Asset.
        asset: String,
        /// Current balance.
        balance: i128,
        /// Requested delta.
        delta: i128,
    },

    /// Account state does not allow the operation.
    #[error("account {account_id} is {status}")]
    AccountNotActive {
        /// Account.
        account_id: String,
        /// Status.
        status: String,
    },

    /// Administrative adjustment missing reason.
    #[error("administrative adjustment requires a non-empty reason")]
    MissingAdjustmentReason,

    /// Generic invariant violation.
    #[error("invariant violated: {0}")]
    Invariant(String),
}
