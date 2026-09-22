//! Application-level error taxonomy.

use ironledger_domain::DomainError;
use thiserror::Error;

/// Errors returned by ledger command handlers and ports.
#[derive(Debug, Error)]
pub enum LedgerError {
    /// A domain invariant was violated.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// A referenced entity does not exist.
    #[error("{entity} '{id}' not found")]
    NotFound {
        /// Entity name, e.g. `account`.
        entity: &'static str,
        /// Identifier as presented by the caller.
        id: String,
    },

    /// The idempotency key was reused with a different request payload.
    #[error(
        "idempotency key '{key}' in scope '{scope}' was already used with a different request"
    )]
    IdempotencyConflict {
        /// Command scope.
        scope: String,
        /// Client-supplied key.
        key: String,
    },

    /// A uniqueness or state conflict that the caller may be able to resolve.
    #[error("conflict: {0}")]
    Conflict(String),

    /// The request was structurally invalid.
    #[error("invalid request: {0}")]
    Validation(String),

    /// The storage adapter failed.
    #[error("storage failure: {0}")]
    Storage(String),

    /// A downstream dependency is unavailable.
    #[error("dependency unavailable: {0}")]
    Unavailable(String),

    /// Encoding or decoding a persisted payload failed.
    #[error("serialization failure: {0}")]
    Serialization(String),
}

/// Transport-agnostic classification used by the gRPC and HTTP adapters to pick
/// a status code without matching on every variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Caller sent something structurally or semantically invalid.
    InvalidArgument,
    /// Referenced entity is missing.
    NotFound,
    /// Current state forbids the operation (balances, account status, replay conflict).
    FailedPrecondition,
    /// Uniqueness violation.
    AlreadyExists,
    /// Dependency is down; retrying later may succeed.
    Unavailable,
    /// Unexpected internal failure.
    Internal,
}

impl LedgerError {
    /// Classify the error for transport mapping.
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Domain(err) => domain_category(err),
            Self::NotFound { .. } => ErrorCategory::NotFound,
            Self::IdempotencyConflict { .. } => ErrorCategory::FailedPrecondition,
            Self::Conflict(_) => ErrorCategory::AlreadyExists,
            Self::Validation(_) => ErrorCategory::InvalidArgument,
            Self::Unavailable(_) => ErrorCategory::Unavailable,
            Self::Storage(_) | Self::Serialization(_) => ErrorCategory::Internal,
        }
    }

    /// Stable machine-readable code for logs and API payloads.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Domain(err) => domain_code(err),
            Self::NotFound { .. } => "not_found",
            Self::IdempotencyConflict { .. } => "idempotency_conflict",
            Self::Conflict(_) => "conflict",
            Self::Validation(_) => "invalid_request",
            Self::Unavailable(_) => "unavailable",
            Self::Storage(_) => "storage_failure",
            Self::Serialization(_) => "serialization_failure",
        }
    }

    /// Convenience constructor for storage adapters.
    pub fn storage(err: impl std::fmt::Display) -> Self {
        Self::Storage(err.to_string())
    }

    /// Convenience constructor for serialization failures.
    pub fn serialization(err: impl std::fmt::Display) -> Self {
        Self::Serialization(err.to_string())
    }
}

fn domain_category(err: &DomainError) -> ErrorCategory {
    match err {
        DomainError::InsufficientBalance { .. } | DomainError::AccountNotActive { .. } => {
            ErrorCategory::FailedPrecondition
        }
        DomainError::Invariant(_) => ErrorCategory::Internal,
        _ => ErrorCategory::InvalidArgument,
    }
}

fn domain_code(err: &DomainError) -> &'static str {
    match err {
        DomainError::InvalidIdentifier { .. } => "invalid_identifier",
        DomainError::InvalidIdempotencyKey { .. } => "invalid_idempotency_key",
        DomainError::InvalidAssetCode { .. } => "invalid_asset_code",
        DomainError::InvalidAssetScale { .. } => "invalid_asset_scale",
        DomainError::InvalidMoneyFormat { .. } => "invalid_money_format",
        DomainError::AmountOverflow => "amount_overflow",
        DomainError::NonPositiveAmount { .. } => "non_positive_amount",
        DomainError::AssetMismatch { .. } => "asset_mismatch",
        DomainError::InsufficientPostings { .. } => "insufficient_postings",
        DomainError::UnbalancedEntry { .. } => "unbalanced_entry",
        DomainError::InsufficientBalance { .. } => "insufficient_balance",
        DomainError::AccountNotActive { .. } => "account_not_active",
        DomainError::MissingAdjustmentReason => "missing_adjustment_reason",
        DomainError::Invariant(_) => "invariant_violated",
    }
}
