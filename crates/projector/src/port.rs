//! Ports the projector depends on.

use async_trait::async_trait;
use ironledger_domain::{EventId, JournalEntry, LedgerEvent};
use serde::{Deserialize, Serialize};

use crate::error::ProjectorError;

/// Whether an event mutated the projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The event was applied for the first time.
    Applied,
    /// The event was already applied by this consumer and was skipped.
    Duplicate,
    /// The event type does not affect balances.
    Ignored,
}

impl ApplyOutcome {
    /// Metric label for this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Duplicate => "duplicate",
            Self::Ignored => "ignored",
        }
    }
}

/// A consumer's position in a partitioned stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPosition {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Offset of the last processed record.
    pub offset: i64,
}

/// An event read from a durable log, with the cursor needed to resume after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcedEvent {
    /// Monotonic cursor of this event in the log.
    pub cursor: i64,
    /// The event envelope.
    pub event: LedgerEvent,
}

/// Write side of the projection.
#[async_trait]
pub trait ProjectionStore: Send + Sync + 'static {
    /// Apply an entry's postings to the projection, deduplicated by event id.
    ///
    /// Implementations must record `(consumer, event_id)` and apply the balance
    /// deltas in a **single transaction**, returning
    /// [`ApplyOutcome::Duplicate`] when the pair is already present.
    async fn apply_journal(
        &self,
        consumer: &str,
        event_id: EventId,
        entry: &JournalEntry,
    ) -> Result<ApplyOutcome, ProjectorError>;

    /// Commit a stream position for the consumer.
    async fn record_position(
        &self,
        consumer: &str,
        position: &StreamPosition,
    ) -> Result<(), ProjectorError>;

    /// Load a previously committed offset.
    async fn load_position(
        &self,
        consumer: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ProjectorError>;

    /// Drop the projection and dedup state for a consumer so it can be rebuilt
    /// from the beginning of the log.
    async fn reset(&self, consumer: &str) -> Result<(), ProjectorError>;
}

/// Read side of a durable event log, used for replay.
///
/// The PostgreSQL adapter implements this over the outbox table, which retains
/// published events and therefore doubles as a replayable log.
#[async_trait]
pub trait EventSource: Send + Sync + 'static {
    /// Fetch up to `limit` events with a cursor strictly greater than `after`.
    async fn fetch_after(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<Vec<SourcedEvent>, ProjectorError>;
}
