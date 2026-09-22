//! Broker configuration.

use crate::error::BusError;

/// Kafka / Redpanda connection settings.
#[derive(Debug, Clone)]
pub struct BusConfig {
    /// Broker list (`host:port`).
    pub brokers: String,
    /// Primary ledger topic.
    pub ledger_topic: String,
    /// Dead-letter topic.
    pub dead_letter_topic: String,
    /// Consumer group id.
    pub consumer_group: String,
}

impl BusConfig {
    /// Load from environment variables with sensible defaults for local compose.
    pub fn from_env() -> Result<Self, BusError> {
        Ok(Self {
            brokers: std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:19092".into()),
            ledger_topic: std::env::var("KAFKA_TOPIC_LEDGER_EVENTS")
                .unwrap_or_else(|_| "ledger.events".into()),
            dead_letter_topic: std::env::var("KAFKA_TOPIC_DEAD_LETTER")
                .unwrap_or_else(|_| "ledger.events.dlq".into()),
            consumer_group: std::env::var("KAFKA_CONSUMER_GROUP")
                .unwrap_or_else(|_| "ironledger".into()),
        })
    }
}
