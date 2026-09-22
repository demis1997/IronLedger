//! Event bus errors.

use thiserror::Error;

/// Kafka / relay failures.
#[derive(Debug, Error)]
pub enum BusError {
    /// Configuration rejected.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Publish failure.
    #[error("publish failed: {0}")]
    Publish(String),

    /// Consume failure.
    #[error("consume failed: {0}")]
    Consume(String),

    /// Event encoding failure.
    #[error("encode failed: {0}")]
    Encode(String),
}
