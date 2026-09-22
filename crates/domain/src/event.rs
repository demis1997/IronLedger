//! Ledger domain events.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::audit::AuditMetadata;
use crate::error::DomainError;
use crate::ids::{AccountId, CausationId, CorrelationId, TransactionId};
use crate::journal::JournalEntry;

/// Opaque event identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl EventId {
    /// New random event id.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// From UUID.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// As UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Typed event payloads emitted after successful ledger commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerEventPayload {
    /// Account created.
    AccountCreated {
        /// Account id.
        account_id: AccountId,
        /// Name.
        name: String,
    },
    /// Journal entry posted (includes full entry for projectors).
    JournalPosted {
        /// Entry.
        entry: JournalEntry,
    },
    /// Administrative adjustment recorded.
    AdminAdjusted {
        /// Entry id.
        transaction_id: TransactionId,
        /// Audit trail.
        audit: AuditMetadata,
    },
}

impl LedgerEventPayload {
    /// Stable event type name.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::AccountCreated { .. } => "account_created",
            Self::JournalPosted { .. } => "journal_posted",
            Self::AdminAdjusted { .. } => "admin_adjusted",
        }
    }
}

/// Versioned event envelope for the outbox / bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEvent {
    /// Unique event id (also used for consumer deduplication).
    pub event_id: EventId,
    /// Event type string (denormalized for routing).
    pub event_type: String,
    /// Schema version for payload evolution.
    pub schema_version: u32,
    /// Aggregate id (typically transaction or account id).
    pub aggregate_id: String,
    /// Optional aggregate sequence.
    pub sequence: Option<u64>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
    /// W3C traceparent or similar opaque context (may be empty).
    pub trace_context: String,
    /// Typed payload.
    pub payload: LedgerEventPayload,
}

impl LedgerEvent {
    /// Current schema version for new events.
    pub const CURRENT_SCHEMA_VERSION: u32 = 1;

    /// Wrap a payload into an envelope.
    pub fn wrap(
        aggregate_id: impl Into<String>,
        sequence: Option<u64>,
        correlation_id: CorrelationId,
        causation_id: CausationId,
        trace_context: impl Into<String>,
        payload: LedgerEventPayload,
    ) -> Self {
        let event_type = payload.event_type().to_owned();
        Self {
            event_id: EventId::new(),
            event_type,
            schema_version: Self::CURRENT_SCHEMA_VERSION,
            aggregate_id: aggregate_id.into(),
            sequence,
            created_at: Utc::now(),
            correlation_id,
            causation_id,
            trace_context: trace_context.into(),
            payload,
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DomainError> {
        serde_json::to_vec(self).map_err(|e| DomainError::Invariant(e.to_string()))
    }

    /// Deserialize from JSON bytes with schema guard.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DomainError> {
        let event: Self = serde_json::from_slice(bytes)
            .map_err(|e| DomainError::Invariant(format!("event decode: {e}")))?;
        if event.schema_version == 0 || event.schema_version > Self::CURRENT_SCHEMA_VERSION {
            return Err(DomainError::Invariant(format!(
                "unsupported schema version {}",
                event.schema_version
            )));
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amount::AtomicAmount;
    use crate::asset::AssetCode;
    use crate::ids::IdempotencyKey;
    use crate::journal::Posting;

    #[test]
    fn round_trip_serialization() {
        let a = AccountId::new();
        let b = AccountId::new();
        let entry = JournalEntry::new(
            TransactionId::new(),
            IdempotencyKey::new("k1").unwrap(),
            "test",
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
        let bytes = event.to_bytes().unwrap();
        let decoded = LedgerEvent::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.event_id, event.event_id);
    }
}
