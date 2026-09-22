//! Optional PostgreSQL smoke test (ignored by default).
//!
//! ```bash
//! make up
//! DATABASE_URL=postgres://ironledger:ironledger@127.0.0.1:5432/ironledger \
//!   cargo test -p ironledger-integration-tests -- --ignored
//! ```

use ironledger_domain::{AssetCode, AtomicAmount};
use ironledger_ledger::{CommandMeta, LedgerService, RecordDepositCommand};
use ironledger_storage_postgres::PostgresStore;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires DATABASE_URL and a running PostgreSQL"]
async fn postgres_deposit_commits_atomically() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let store = Arc::new(
        PostgresStore::connect(&database_url, "ledger.events")
            .await
            .expect("connect"),
    );
    store.migrate().await.expect("migrate");
    let ledger = LedgerService::new(
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    );

    let name = format!("it-cust-{}", uuid::Uuid::new_v4());
    let clearing_name = format!("it-clear-{}", uuid::Uuid::new_v4());
    let customer = ledger
        .create_account(ironledger_ledger::CreateAccountCommand {
            meta: CommandMeta::from_key(&format!("create:{name}")).unwrap(),
            name: name.clone(),
            kind: ironledger_domain::AccountKind::CustomerAvailable,
            policy: ironledger_domain::AccountPolicy::NonNegative,
        })
        .await
        .unwrap()
        .value
        .account
        .id;
    let clearing = ledger
        .create_account(ironledger_ledger::CreateAccountCommand {
            meta: CommandMeta::from_key(&format!("create:{clearing_name}")).unwrap(),
            name: clearing_name,
            kind: ironledger_domain::AccountKind::DepositClearing,
            policy: ironledger_domain::AccountPolicy::AllowNegative,
        })
        .await
        .unwrap()
        .value
        .account
        .id;

    let key = format!("dep:{}", uuid::Uuid::new_v4());
    let cmd = RecordDepositCommand {
        meta: CommandMeta::from_key(&key).unwrap(),
        customer_available: customer,
        deposit_clearing: clearing,
        hot_wallet: None,
        asset: AssetCode::usd(),
        amount: AtomicAmount::from_raw(42),
        external_reference: Some("smoke".into()),
    };
    let first = ledger.record_deposit(cmd.clone()).await.unwrap();
    let second = ledger.record_deposit(cmd).await.unwrap();
    assert!(!first.replayed);
    assert!(second.replayed);
    assert_eq!(first.value.transaction_id, second.value.transaction_id);
}
