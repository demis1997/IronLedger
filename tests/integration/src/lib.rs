//! Integration tests (PostgreSQL + optional Redpanda).
//!
//! Run with `make up migrate` then `cargo test -p ironledger-integration-tests -- --ignored`.

use ironledger_domain::{AssetCode, AtomicAmount};
use ironledger_ledger::{
    memory::InMemoryLedger, BalanceRepo, CommandMeta, LedgerService, RecordDepositCommand,
};
use ironledger_projector::memory::InMemoryProjectionStore;
use ironledger_projector::{Projector, ReplayFrom};
use ironledger_reconciler::Reconciler;
use std::sync::Arc;

struct MemoryHistory(Arc<InMemoryLedger>);

#[async_trait::async_trait]
impl ironledger_reconciler::PostingHistory for MemoryHistory {
    async fn all_entries(
        &self,
    ) -> Result<Vec<ironledger_domain::JournalEntry>, ironledger_reconciler::ReconcileError> {
        Ok(self.0.journal_entries())
    }
}

struct MemoryBalances(Arc<InMemoryLedger>);

#[async_trait::async_trait]
impl ironledger_reconciler::BalanceView for MemoryBalances {
    async fn snapshot(
        &self,
    ) -> Result<Vec<ironledger_ledger::BalanceRecord>, ironledger_reconciler::ReconcileError> {
        self.0
            .snapshot()
            .await
            .map_err(ironledger_reconciler::ReconcileError::Ledger)
    }
}

#[test]
fn in_memory_deposit_and_idempotency() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let storage = Arc::new(InMemoryLedger::new());
        let ledger = LedgerService::new(
            storage.clone(),
            storage.clone(),
            storage.clone(),
            storage.clone(),
            storage.clone(),
        );
        let customer = ironledger_domain::AccountId::new();
        storage.seed_account(ironledger_domain::Account::new(
            customer,
            "cust",
            ironledger_domain::AccountKind::CustomerAvailable,
            ironledger_domain::AccountPolicy::NonNegative,
        ));
        let clearing = ironledger_domain::AccountId::new();
        storage.seed_account(ironledger_domain::Account::new(
            clearing,
            "clearing",
            ironledger_domain::AccountKind::DepositClearing,
            ironledger_domain::AccountPolicy::AllowNegative,
        ));
        let cmd = RecordDepositCommand {
            meta: CommandMeta::from_key("it:dep").unwrap(),
            customer_available: customer,
            deposit_clearing: clearing,
            hot_wallet: None,
            asset: AssetCode::usd(),
            amount: AtomicAmount::from_raw(100),
            external_reference: None,
        };
        ledger.record_deposit(cmd.clone()).await.unwrap();
        ledger.record_deposit(cmd).await.unwrap();
        assert_eq!(storage.entry_count(), 1);
    });
}

#[test]
fn projection_and_reconciliation_match() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let storage = Arc::new(InMemoryLedger::new());
        let ledger = LedgerService::new(
            storage.clone(),
            storage.clone(),
            storage.clone(),
            storage.clone(),
            storage.clone(),
        );
        let a = ironledger_domain::AccountId::new();
        let b = ironledger_domain::AccountId::new();
        storage.seed_account(ironledger_domain::Account::new(
            a,
            "a",
            ironledger_domain::AccountKind::CustomerAvailable,
            ironledger_domain::AccountPolicy::NonNegative,
        ));
        storage.seed_account(ironledger_domain::Account::new(
            b,
            "b",
            ironledger_domain::AccountKind::CustomerAvailable,
            ironledger_domain::AccountPolicy::NonNegative,
        ));
        storage.seed_account(ironledger_domain::Account::new(
            ironledger_domain::AccountId::new(),
            "clear",
            ironledger_domain::AccountKind::DepositClearing,
            ironledger_domain::AccountPolicy::AllowNegative,
        ));
        let clearing = ledger
            .find_account_by_name("clear")
            .await
            .unwrap()
            .unwrap()
            .id;
        ledger
            .record_deposit(RecordDepositCommand {
                meta: CommandMeta::from_key("p:1").unwrap(),
                customer_available: a,
                deposit_clearing: clearing,
                hot_wallet: None,
                asset: AssetCode::usd(),
                amount: AtomicAmount::from_raw(50),
                external_reference: None,
            })
            .await
            .unwrap();

        let projection_store = InMemoryProjectionStore::new();
        for (idx, event) in storage.events().into_iter().enumerate() {
            projection_store.append_event(idx as i64 + 1, event);
        }
        let projector = Projector::new("it", projection_store.clone());
        projector
            .replay(&projection_store, ReplayFrom::Beginning, 100)
            .await
            .unwrap();

        let reconciler = Reconciler::new(MemoryHistory(storage.clone()), MemoryBalances(storage));
        let report = reconciler.reconcile_authoritative().await.unwrap();
        assert!(report.is_clean());
    });
}
