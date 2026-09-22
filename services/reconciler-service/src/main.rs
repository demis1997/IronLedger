//! Periodic reconciliation worker.

use ironledger_reconciler::Reconciler;
use ironledger_storage_postgres::{PostgresBalanceViews, PostgresHistory, PostgresStore};
use tokio::signal;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ironledger:ironledger@127.0.0.1:5432/ironledger".into());
    let store = PostgresStore::connect(&database_url, "ledger.events").await?;
    let reconciler = Reconciler::new(
        PostgresHistory::new(store.pool().clone()),
        PostgresBalanceViews::new(store.pool().clone()),
    );

    let period = Duration::from_secs(
        std::env::var("RECONCILE_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60),
    );

    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let _ = signal::ctrl_c().await;
            cancel.cancel();
        }
    });

    let mut ticker = interval(period);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                let report = reconciler.reconcile_authoritative().await?;
                if report.is_clean() {
                    tracing::info!(entries = report.entries_scanned, "reconciliation clean");
                } else {
                    tracing::warn!(
                        discrepancies = report.discrepancies.len(),
                        "reconciliation found mismatches"
                    );
                }
            }
        }
    }
    Ok(())
}
