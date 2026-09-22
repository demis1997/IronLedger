//! Transactional outbox relay.

use crate::error::BusError;
use crate::kafka::KafkaPublisher;
use ironledger_ledger::{LedgerError, OutboxRepo};
use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

/// Polls the outbox and publishes unpublished events.
pub struct OutboxRelay {
    outbox: Arc<dyn OutboxRepo>,
    publisher: KafkaPublisher,
    batch_size: u32,
    lease: Duration,
    poll_interval: Duration,
}

impl OutboxRelay {
    /// Wire the relay.
    pub fn new(
        outbox: Arc<dyn OutboxRepo>,
        publisher: KafkaPublisher,
        batch_size: u32,
        lease: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            outbox,
            publisher,
            batch_size,
            lease,
            poll_interval,
        }
    }

    /// Run until `cancel` is triggered.
    pub async fn run(&self, cancel: tokio_util::sync::CancellationToken) {
        info!("outbox relay started");
        loop {
            if cancel.is_cancelled() {
                info!("outbox relay stopped");
                break;
            }
            match self.tick().await {
                Ok(published) => {
                    gauge!("ironledger_outbox_backlog").set(
                        self.outbox
                            .stats()
                            .await
                            .map(|s| s.pending as f64)
                            .unwrap_or(0.0),
                    );
                    if published == 0 {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = sleep(self.poll_interval) => {}
                        }
                    }
                }
                Err(err) => {
                    error!(error = %err, "outbox relay tick failed");
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(self.poll_interval) => {}
                    }
                }
            }
        }
    }

    async fn tick(&self) -> Result<u64, BusError> {
        let messages = self
            .outbox
            .claim(self.batch_size, self.lease)
            .await
            .map_err(|err| BusError::Publish(err.to_string()))?;
        if messages.is_empty() {
            return Ok(0);
        }
        let mut published_ids = Vec::with_capacity(messages.len());
        for message in &messages {
            match self
                .publisher
                .publish(&message.partition_key, &message.event)
                .await
            {
                Ok(()) => published_ids.push(message.id),
                Err(err) => {
                    counter!("ironledger_outbox_publish_failures_total").increment(1);
                    let _ = self.outbox.mark_failed(message.id, &err.to_string()).await;
                }
            }
        }
        if published_ids.is_empty() {
            return Ok(0);
        }
        let count = self
            .outbox
            .mark_published(&published_ids)
            .await
            .map_err(|err: LedgerError| BusError::Publish(err.to_string()))?;
        counter!("ironledger_outbox_published_total").increment(count);
        Ok(count)
    }
}
