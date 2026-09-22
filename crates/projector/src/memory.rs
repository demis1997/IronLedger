//! In-memory projection store for tests and offline replay.

use async_trait::async_trait;
use ironledger_domain::{AccountId, AssetCode, AtomicAmount, EventId, JournalEntry};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::error::ProjectorError;
use crate::port::{ApplyOutcome, EventSource, ProjectionStore, SourcedEvent, StreamPosition};

#[derive(Debug, Default)]
struct State {
    balances: HashMap<(Uuid, AssetCode), AtomicAmount>,
    processed: HashSet<(String, Uuid)>,
    positions: HashMap<(String, String, i32), i64>,
    event_log: Vec<SourcedEvent>,
}

/// In-memory projection and replay log.
#[derive(Debug, Clone, Default)]
pub struct InMemoryProjectionStore {
    state: Arc<RwLock<State>>,
}

impl InMemoryProjectionStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event to the replay log (for tests).
    pub fn append_event(&self, cursor: i64, event: ironledger_domain::LedgerEvent) {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .event_log
            .push(SourcedEvent { cursor, event });
    }

    /// Balance for `(account, asset)`.
    #[must_use]
    pub fn balance(&self, account: AccountId, asset: &AssetCode) -> AtomicAmount {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .balances
            .get(&(account.into_uuid(), asset.clone()))
            .copied()
            .unwrap_or(AtomicAmount::ZERO)
    }
}

#[async_trait]
impl ProjectionStore for InMemoryProjectionStore {
    async fn apply_journal(
        &self,
        consumer: &str,
        event_id: EventId,
        entry: &JournalEntry,
    ) -> Result<ApplyOutcome, ProjectorError> {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        let key = (consumer.to_owned(), *event_id.as_uuid());
        if state.processed.contains(&key) {
            return Ok(ApplyOutcome::Duplicate);
        }
        for posting in &entry.postings {
            let slot = state
                .balances
                .entry((posting.account_id.into_uuid(), posting.asset.clone()))
                .or_insert(AtomicAmount::ZERO);
            *slot = slot.checked_add(posting.amount)?;
        }
        state.processed.insert(key);
        Ok(ApplyOutcome::Applied)
    }

    async fn record_position(
        &self,
        consumer: &str,
        position: &StreamPosition,
    ) -> Result<(), ProjectorError> {
        self.state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .positions
            .insert(
                (
                    consumer.to_owned(),
                    position.topic.clone(),
                    position.partition,
                ),
                position.offset,
            );
        Ok(())
    }

    async fn load_position(
        &self,
        consumer: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ProjectorError> {
        Ok(self
            .state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .positions
            .get(&(consumer.to_owned(), topic.to_owned(), partition))
            .copied())
    }

    async fn reset(&self, consumer: &str) -> Result<(), ProjectorError> {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.processed.retain(|(c, _)| c != consumer);
        state.positions.retain(|(c, _, _), _| c != consumer);
        state.balances.clear();
        Ok(())
    }
}

#[async_trait]
impl EventSource for InMemoryProjectionStore {
    async fn fetch_after(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<Vec<SourcedEvent>, ProjectorError> {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .event_log
            .iter()
            .filter(|e| e.cursor > after)
            .take(limit as usize)
            .cloned()
            .collect())
    }
}
