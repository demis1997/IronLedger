//! # ironledger-event-bus
//!
//! Kafka-compatible event publication and consumption with retries, dead-letter
//! routing and an outbox relay. Delivery is **at-least-once**; business
//! handlers must deduplicate by event id.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod config;
pub mod error;
pub mod kafka;
pub mod relay;
pub mod retry;

pub use config::BusConfig;
pub use error::BusError;
#[cfg(feature = "kafka")]
pub use kafka::KafkaConsumer;
pub use kafka::KafkaPublisher;
pub use relay::OutboxRelay;
