//! Reconciliation errors.

use ironledger_domain::DomainError;
use thiserror::Error;

/// Failures during reconciliation.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// Domain invariant violated while rebuilding.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// Storage read failure.
    #[error("store failure: {0}")]
    Store(String),

    /// Ledger application error.
    #[error(transparent)]
    Ledger(#[from] ironledger_ledger::LedgerError),
}

impl ReconcileError {
    /// Wrap a store error.
    pub fn store(err: impl std::fmt::Display) -> Self {
        Self::Store(err.to_string())
    }
}
