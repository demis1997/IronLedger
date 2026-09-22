//! Projector errors.

use ironledger_domain::DomainError;
use thiserror::Error;

/// Failures while projecting or replaying events.
#[derive(Debug, Error)]
pub enum ProjectorError {
    /// A domain invariant was violated by a persisted event.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// The projection store failed.
    #[error("projection store failure: {0}")]
    Store(String),

    /// An event payload could not be decoded.
    #[error("malformed event: {0}")]
    MalformedEvent(String),

    /// The event referenced an account the projection does not know about.
    #[error("unknown account '{account_id}' referenced by event {event_id}")]
    UnknownAccount {
        /// Account id from the posting.
        account_id: String,
        /// Event that referenced it.
        event_id: String,
    },
}

impl ProjectorError {
    /// Convenience constructor for adapters.
    pub fn store(err: impl std::fmt::Display) -> Self {
        Self::Store(err.to_string())
    }
}
