//! Deterministic balance reconciliation.

use ironledger_domain::{AccountId, AssetCode, AtomicAmount};
use metrics::counter;
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

use crate::error::ReconcileError;
use crate::port::{BalanceView, Discrepancy, PostingHistory, ReconcileReport};

/// Rebuilds balances from journal history and compares against materialized views.
pub struct Reconciler<H: PostingHistory, V: BalanceView> {
    history: H,
    views: V,
}

impl<H: PostingHistory, V: BalanceView> Reconciler<H, V> {
    /// Wire dependencies.
    pub fn new(history: H, views: V) -> Self {
        Self { history, views }
    }

    /// Rebuild balances from all postings.
    pub async fn rebuild(
        &self,
    ) -> Result<HashMap<(Uuid, AssetCode), AtomicAmount>, ReconcileError> {
        let entries = self.history.all_entries().await?;
        let mut totals: HashMap<(Uuid, AssetCode), AtomicAmount> = HashMap::new();
        for entry in &entries {
            for posting in &entry.postings {
                let key = (posting.account_id.into_uuid(), posting.asset.clone());
                let slot = totals.entry(key).or_insert(AtomicAmount::ZERO);
                *slot = slot.checked_add(posting.amount)?;
            }
        }
        Ok(totals)
    }

    /// Compare rebuilt totals against the authoritative balance table.
    pub async fn reconcile_authoritative(&self) -> Result<ReconcileReport, ReconcileError> {
        let rebuilt = self.rebuild().await?;
        let entries = self.history.all_entries().await?;
        let snapshot = self.views.snapshot().await?;
        let discrepancies = compare_maps(&rebuilt, &snapshot, "authoritative_balances");
        counter!(
            "ironledger_reconciliation_discrepancies_total",
            "view" => "authoritative"
        )
        .increment(discrepancies.len() as u64);
        let report = ReconcileReport {
            finished_at: chrono::Utc::now(),
            entries_scanned: entries.len() as u64,
            postings_applied: entries.iter().map(|e| e.postings.len() as u64).sum(),
            discrepancies,
        };
        info!(
            entries = report.entries_scanned,
            discrepancies = report.discrepancies.len(),
            "reconciliation finished"
        );
        Ok(report)
    }

    /// Compare rebuilt totals against a projector read model.
    pub async fn reconcile_projection(
        &self,
        consumer: &str,
    ) -> Result<ReconcileReport, ReconcileError> {
        let rebuilt = self.rebuild().await?;
        let entries = self.history.all_entries().await?;
        let snapshot = self.views.projection_snapshot(consumer).await?;
        let discrepancies = compare_maps(&rebuilt, &snapshot, "projection");
        counter!(
            "ironledger_reconciliation_discrepancies_total",
            "view" => "projection"
        )
        .increment(discrepancies.len() as u64);
        Ok(ReconcileReport {
            finished_at: chrono::Utc::now(),
            entries_scanned: entries.len() as u64,
            postings_applied: entries.iter().map(|e| e.postings.len() as u64).sum(),
            discrepancies,
        })
    }
}

fn compare_maps(
    rebuilt: &HashMap<(Uuid, AssetCode), AtomicAmount>,
    snapshot: &[ironledger_ledger::BalanceRecord],
    view: &str,
) -> Vec<Discrepancy> {
    let mut observed: HashMap<(Uuid, AssetCode), AtomicAmount> = HashMap::new();
    for record in snapshot {
        observed.insert(
            (record.account_id.into_uuid(), record.asset.clone()),
            record.amount,
        );
    }
    let mut keys: std::collections::BTreeSet<_> = rebuilt.keys().cloned().collect();
    keys.extend(observed.keys().cloned());
    let mut out = Vec::new();
    for key in keys {
        let expected = rebuilt.get(&key).copied().unwrap_or(AtomicAmount::ZERO);
        let actual = observed.get(&key).copied().unwrap_or(AtomicAmount::ZERO);
        if expected != actual {
            out.push(Discrepancy {
                account_id: AccountId::from_uuid(key.0),
                asset: key.1.clone(),
                expected,
                observed: actual,
                view: view.to_owned(),
            });
        }
    }
    out
}
