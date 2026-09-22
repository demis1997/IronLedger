//! # ironledger-projector
//!
//! Maintains the read-side balance projection from the ledger event stream.
//!
//! Kafka gives at-least-once delivery, so the projector must be idempotent.
//! Every application is keyed by `(consumer, event_id)`: the store records the
//! event id and applies the postings in the same transaction, so a redelivered
//! event is recognized and skipped rather than double-counted.
//!
//! The projection is deliberately *separate* from the authoritative `balances`
//! table written by the ledger service inside its command transaction. Keeping
//! both lets `ironledger-reconciler` compare three independently derived views:
//! the posting history, the authoritative balances and this projection.
//!
//! Replay is supported from the beginning of the durable event log or from a
//! stored cursor — see [`Projector::replay`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod error;
pub mod port;
pub mod projector;

#[cfg(any(test, feature = "memory"))]
pub mod memory;

pub use error::ProjectorError;
pub use port::{ApplyOutcome, EventSource, ProjectionStore, SourcedEvent, StreamPosition};
pub use projector::{Projector, ReplayFrom, ReplayReport};
