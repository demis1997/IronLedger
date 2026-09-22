//! Ports for reconciliation.

use async_trait::async_trait;
use ironledger_domain::{AccountId, AssetCode, AtomicAmount, JournalEntry};
use ironledger_ledger::BalanceRecord;

use crate::error::ReconcileError;

/// Read-only access to posting history.
#[async_trait]
pub trait PostingHistory: Send + Sync + 'static {
    /// All journal entries in insertion order.
    async fn all_entries(&self) -> Result<Vec<JournalEntry>, ReconcileError>;
}

/// Read-only access to a materialized balance view.
#[async_trait]
pub trait BalanceView: Send + Sync + 'static {
    /// Full snapshot.
    async fn snapshot(&self) -> Result<Vec<BalanceRecord>, ReconcileError>;

    /// Optional projection snapshot (read model).
    async fn projection_snapshot(
        &self,
        consumer: &str,
    ) -> Result<Vec<BalanceRecord>, ReconcileError> {
        let _ = consumer;
        Ok(Vec::new())
    }
}

/// One mismatch between expected and observed balances.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Discrepancy {
    /// Account.
    pub account_id: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Balance rebuilt from postings.
    pub expected: AtomicAmount,
    /// Balance in the materialized table.
    pub observed: AtomicAmount,
    /// Which view was compared.
    pub view: String,
}

/// Reconciliation outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReconcileReport {
    /// When the run finished.
    pub finished_at: chrono::DateTime<chrono::Utc>,
    /// Entries scanned.
    pub entries_scanned: u64,
    /// Postings applied while rebuilding.
    pub postings_applied: u64,
    /// Mismatches found.
    pub discrepancies: Vec<Discrepancy>,
}

impl ReconcileReport {
    /// Whether any mismatch was found.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.discrepancies.is_empty()
    }
}
