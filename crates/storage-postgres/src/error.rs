//! PostgreSQL adapter errors.

use ironledger_domain::DomainError;
use ironledger_ledger::LedgerError;
use ironledger_projector::ProjectorError;
use ironledger_reconciler::ReconcileError;
use thiserror::Error;

/// Storage-layer failures.
#[derive(Debug, Error)]
pub enum StorageError {
    /// SQLx/database error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Migration error.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// Domain decode failure.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// JSON failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<StorageError> for LedgerError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Domain(err) => Self::Domain(err),
            other => Self::Storage(other.to_string()),
        }
    }
}

impl From<StorageError> for ProjectorError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Domain(err) => ProjectorError::Domain(err),
            other => ProjectorError::store(other),
        }
    }
}

impl From<StorageError> for ReconcileError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Domain(err) => ReconcileError::Domain(err),
            other => ReconcileError::store(other),
        }
    }
}
