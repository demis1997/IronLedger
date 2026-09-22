//! IronLedger operator CLI.

use clap::{Parser, Subcommand};
use ironledger_domain::{AssetCode, AtomicAmount};
use ironledger_ledger::{
    chart::{customer_available, customer_locked, full_chart, EXCHANGE_HOT_WALLET, FEE_REVENUE},
    CommandMeta, CompleteWithdrawalCommand, CreateAccountCommand, LedgerService,
    RecordDepositCommand, RecordTradeSettlementCommand, RecordTradingFeeCommand,
    RequestWithdrawalCommand,
};
use ironledger_projector::{Projector, ReplayFrom};
use ironledger_reconciler::Reconciler;
use ironledger_storage_postgres::{
    PostgresBalanceViews, PostgresHistory, PostgresProjection, PostgresStore,
};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ironledger", about = "IronLedger operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the end-to-end demonstration scenario.
    Demo,
    /// Print balances for a named account (`customer:alice:available`).
    Balance {
        /// Account name.
        account: String,
    },
    /// Run reconciliation against posting history.
    Reconcile,
    /// Replay projection from the outbox log.
    Replay,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ironledger:ironledger@127.0.0.1:5432/ironledger".into());
    let store = Arc::new(PostgresStore::connect(&database_url, "ledger.events").await?);
    store.migrate().await?;
    let ledger = Arc::new(LedgerService::new(
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    ));

    match cli.command {
        Commands::Demo => run_demo(ledger, store).await?,
        Commands::Balance { account } => print_balance(&ledger, &account).await?,
        Commands::Reconcile => run_reconcile(store).await?,
        Commands::Replay => run_replay(store).await?,
    }
    Ok(())
}

async fn ensure_chart(ledger: &LedgerService) -> anyhow::Result<()> {
    for spec in full_chart(&["alice", "bob"]) {
        if ledger.find_account_by_name(&spec.name).await?.is_none() {
            let command = CreateAccountCommand {
                meta: CommandMeta::from_key(&format!("seed:{}", spec.name))?,
                name: spec.name.clone(),
                kind: spec.kind,
                policy: spec.policy,
            };
            let _ = ledger.create_account(command).await?;
        }
    }
    Ok(())
}

async fn account_id(
    ledger: &LedgerService,
    name: &str,
) -> anyhow::Result<ironledger_domain::AccountId> {
    Ok(ledger
        .find_account_by_name(name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("account '{name}' missing"))?
        .id)
}

async fn run_demo(ledger: Arc<LedgerService>, store: Arc<PostgresStore>) -> anyhow::Result<()> {
    ensure_chart(&ledger).await?;

    let alice_avail = account_id(&ledger, &customer_available("alice")).await?;
    let alice_locked = account_id(&ledger, &customer_locked("alice")).await?;
    let bob_avail = account_id(&ledger, &customer_available("bob")).await?;
    let hot = account_id(&ledger, EXCHANGE_HOT_WALLET).await?;
    let fee = account_id(&ledger, FEE_REVENUE).await?;

    let btc = AssetCode::btc();
    let usd = AssetCode::usd();

    let _ = ledger
        .record_deposit(RecordDepositCommand {
            meta: CommandMeta::from_key("demo:deposit:alice:btc")?,
            customer_available: alice_avail,
            deposit_clearing: account_id(&ledger, ironledger_ledger::chart::DEPOSIT_CLEARING)
                .await?,
            hot_wallet: Some(hot),
            asset: btc.clone(),
            amount: AtomicAmount::from_raw(100_000_000),
            external_reference: Some("demo-tx-btc".into()),
        })
        .await?;
    let _ = ledger
        .record_deposit(RecordDepositCommand {
            meta: CommandMeta::from_key("demo:deposit:bob:usd")?,
            customer_available: bob_avail,
            deposit_clearing: account_id(&ledger, ironledger_ledger::chart::DEPOSIT_CLEARING)
                .await?,
            hot_wallet: Some(hot),
            asset: usd.clone(),
            amount: AtomicAmount::from_raw(10_000_000_000),
            external_reference: Some("demo-tx-usd".into()),
        })
        .await?;

    let _ = ledger
        .record_trade_settlement(RecordTradeSettlementCommand {
            meta: CommandMeta::from_key("demo:trade:1")?,
            buyer_available: alice_avail,
            seller_available: bob_avail,
            base_asset: btc.clone(),
            base_amount: AtomicAmount::from_raw(1_000_000),
            quote_asset: usd.clone(),
            quote_amount: AtomicAmount::from_raw(50_000_000_000),
            trade_reference: Some("demo-match-1".into()),
        })
        .await?;

    let _ = ledger
        .record_trading_fee(RecordTradingFeeCommand {
            meta: CommandMeta::from_key("demo:fee:1")?,
            customer_available: alice_avail,
            fee_revenue: fee,
            asset: usd.clone(),
            amount: AtomicAmount::from_raw(50_000_000),
        })
        .await?;

    let _ = ledger
        .request_withdrawal(RequestWithdrawalCommand {
            meta: CommandMeta::from_key("demo:withdraw:req")?,
            customer_available: alice_avail,
            customer_locked: alice_locked,
            asset: btc.clone(),
            amount: AtomicAmount::from_raw(100_000),
            destination_reference: Some("bc1q-demo".into()),
        })
        .await?;

    let _ = ledger
        .complete_withdrawal(CompleteWithdrawalCommand {
            meta: CommandMeta::from_key("demo:withdraw:complete")?,
            customer_locked: alice_locked,
            withdrawal_clearing: account_id(&ledger, ironledger_ledger::chart::WITHDRAWAL_CLEARING)
                .await?,
            hot_wallet: hot,
            asset: btc,
            amount: AtomicAmount::from_raw(100_000),
            settlement_reference: Some("demo-chain-tx".into()),
        })
        .await?;

    print_balance(&ledger, &customer_available("alice")).await?;
    print_balance(&ledger, &customer_available("bob")).await?;

    run_reconcile(store.clone()).await?;
    run_replay(store).await?;

    println!("\nIronLedger demo completed successfully.");
    Ok(())
}

async fn print_balance(ledger: &LedgerService, account_name: &str) -> anyhow::Result<()> {
    let account = ledger
        .find_account_by_name(account_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("account not found"))?;
    let balances = ledger.get_balances(account.id, None).await?;
    println!("Account {account_name}:");
    for balance in balances {
        println!("  {} => {}", balance.asset, balance.amount.raw());
    }
    Ok(())
}

async fn run_reconcile(store: Arc<PostgresStore>) -> anyhow::Result<()> {
    let reconciler = Reconciler::new(
        PostgresHistory::new(store.pool().clone()),
        PostgresBalanceViews::new(store.pool().clone()),
    );
    let report = reconciler.reconcile_authoritative().await?;
    if report.is_clean() {
        println!("Reconciliation: clean ({} entries)", report.entries_scanned);
    } else {
        println!(
            "Reconciliation: {} discrepancies",
            report.discrepancies.len()
        );
    }
    Ok(())
}

async fn run_replay(store: Arc<PostgresStore>) -> anyhow::Result<()> {
    let projection = PostgresProjection::new(store.pool().clone());
    let projector = Projector::new("balance-projector", projection.clone());
    let report = projector
        .replay(&projection, ReplayFrom::Beginning, 500)
        .await?;
    println!(
        "Replay: scanned {} applied {} duplicates {}",
        report.scanned, report.applied, report.duplicates
    );
    Ok(())
}
