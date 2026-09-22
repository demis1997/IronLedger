//! Consumes ledger events and maintains the balance projection.

use ironledger_event_bus::{BusConfig, KafkaConsumer};
use ironledger_projector::{Projector, ReplayFrom};
use ironledger_storage_postgres::{PostgresProjection, PostgresStore};
use std::sync::Arc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const CONSUMER: &str = "balance-projector";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ironledger:ironledger@127.0.0.1:5432/ironledger".into());
    let store = PostgresStore::connect(&database_url, "ledger.events").await?;
    let projection = PostgresProjection::new(store.pool().clone());
    let projector = Arc::new(Projector::new(CONSUMER, projection.clone()));

    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let _ = signal::ctrl_c().await;
            cancel.cancel();
        }
    });

    let bus = BusConfig::from_env()?;
    let consumer = KafkaConsumer::new(&bus)?;
    let handler_projector = projector.clone();
    let shutdown = cancel.clone();
    let kafka_task = tokio::spawn(async move {
        consumer
            .run(
                move |event| {
                    let projector = handler_projector.clone();
                    async move {
                        projector.apply(&event).await.map_err(|err| {
                            ironledger_event_bus::BusError::Consume(err.to_string())
                        })?;
                        Ok(())
                    }
                },
                shutdown,
            )
            .await
    });

    // Also support replay from the durable outbox log on startup.
    let replay_report = projector
        .replay(&projection, ReplayFrom::Beginning, 500)
        .await?;
    tracing::info!(?replay_report, "initial replay finished");

    tokio::select! {
        _ = cancel.cancelled() => {},
        result = kafka_task => { result??; }
    }
    Ok(())
}
