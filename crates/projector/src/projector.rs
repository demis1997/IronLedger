//! Event-driven balance projection.

use ironledger_domain::{LedgerEvent, LedgerEventPayload};
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::error::ProjectorError;
use crate::port::{ApplyOutcome, EventSource, ProjectionStore, SourcedEvent, StreamPosition};

/// Where to start a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayFrom {
    /// Cursor strictly greater than zero — resume after the last committed offset.
    AfterCursor(i64),
    /// Rebuild from the beginning of the durable log.
    Beginning,
}

/// Summary of a replay run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Events scanned.
    pub scanned: u64,
    /// Newly applied.
    pub applied: u64,
    /// Skipped duplicates.
    pub duplicates: u64,
    /// Ignored (non-balance) events.
    pub ignored: u64,
    /// Last cursor observed.
    pub last_cursor: i64,
}

/// Applies ledger events to a materialized balance projection.
pub struct Projector<S: ProjectionStore> {
    consumer: String,
    store: S,
}

impl<S: ProjectionStore> Projector<S> {
    /// Create a projector for `consumer`.
    pub fn new(consumer: impl Into<String>, store: S) -> Self {
        Self {
            consumer: consumer.into(),
            store,
        }
    }

    /// Consumer name used for deduplication keys.
    #[must_use]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// Apply one event envelope.
    pub async fn apply(&self, event: &LedgerEvent) -> Result<ApplyOutcome, ProjectorError> {
        let start = Instant::now();
        let outcome = match &event.payload {
            LedgerEventPayload::JournalPosted { entry } => {
                self.store
                    .apply_journal(&self.consumer, event.event_id, entry)
                    .await?
            }
            LedgerEventPayload::AccountCreated { .. }
            | LedgerEventPayload::AdminAdjusted { .. } => ApplyOutcome::Ignored,
        };
        counter!(
            "ironledger_projector_events_total",
            "consumer" => self.consumer.clone(),
            "outcome" => outcome.as_str()
        )
        .increment(1);
        histogram!("ironledger_projector_apply_seconds").record(start.elapsed().as_secs_f64());
        debug!(
            consumer = %self.consumer,
            event_id = %event.event_id,
            outcome = outcome.as_str(),
            "projected event"
        );
        Ok(outcome)
    }

    /// Apply a sourced event and record stream position when applied or duplicate.
    pub async fn apply_sourced(
        &self,
        topic: &str,
        partition: i32,
        sourced: &SourcedEvent,
    ) -> Result<ApplyOutcome, ProjectorError> {
        let outcome = self.apply(&sourced.event).await?;
        if matches!(outcome, ApplyOutcome::Applied | ApplyOutcome::Duplicate) {
            self.store
                .record_position(
                    &self.consumer,
                    &StreamPosition {
                        topic: topic.to_owned(),
                        partition,
                        offset: sourced.cursor,
                    },
                )
                .await?;
        }
        Ok(outcome)
    }

    /// Replay events from an [`EventSource`].
    pub async fn replay<E: EventSource>(
        &self,
        source: &E,
        from: ReplayFrom,
        batch_size: u32,
    ) -> Result<ReplayReport, ProjectorError> {
        let mut after = match from {
            ReplayFrom::Beginning => 0,
            ReplayFrom::AfterCursor(cursor) => cursor,
        };
        let mut report = ReplayReport {
            scanned: 0,
            applied: 0,
            duplicates: 0,
            ignored: 0,
            last_cursor: after,
        };
        loop {
            let batch = source.fetch_after(after, batch_size).await?;
            if batch.is_empty() {
                break;
            }
            for sourced in &batch {
                report.scanned += 1;
                report.last_cursor = sourced.cursor;
                match self.apply(&sourced.event).await? {
                    ApplyOutcome::Applied => report.applied += 1,
                    ApplyOutcome::Duplicate => report.duplicates += 1,
                    ApplyOutcome::Ignored => report.ignored += 1,
                }
            }
            after = report.last_cursor;
            if batch.len() < batch_size as usize {
                break;
            }
        }
        info!(
            consumer = %self.consumer,
            scanned = report.scanned,
            applied = report.applied,
            duplicates = report.duplicates,
            "replay finished"
        );
        Ok(report)
    }

    /// Reset projection state for this consumer.
    pub async fn reset(&self) -> Result<(), ProjectorError> {
        warn!(consumer = %self.consumer, "resetting projection");
        self.store.reset(&self.consumer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryProjectionStore;
    use ironledger_domain::{
        amount::AtomicAmount, asset::AssetCode, ids::IdempotencyKey, journal::Posting, CausationId,
        CorrelationId, JournalEntry, TransactionId,
    };

    #[tokio::test]
    async fn duplicate_event_is_idempotent() {
        let store = InMemoryProjectionStore::new();
        let projector = Projector::new("test", store.clone());
        let a = ironledger_domain::AccountId::new();
        let b = ironledger_domain::AccountId::new();
        let entry = JournalEntry::new(
            TransactionId::new(),
            IdempotencyKey::new("k").unwrap(),
            "t",
            vec![
                Posting::debit(a, AssetCode::usd(), AtomicAmount::from_raw(10)).unwrap(),
                Posting::credit(b, AssetCode::usd(), AtomicAmount::from_raw(10)).unwrap(),
            ],
            CorrelationId::new(),
            CausationId::new(),
        )
        .unwrap();
        let event = LedgerEvent::wrap(
            entry.id.to_string(),
            Some(1),
            entry.correlation_id,
            entry.causation_id,
            "",
            LedgerEventPayload::JournalPosted { entry },
        );
        assert_eq!(
            projector.apply(&event).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            projector.apply(&event).await.unwrap(),
            ApplyOutcome::Duplicate
        );
    }
}
