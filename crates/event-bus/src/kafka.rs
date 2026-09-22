//! rdkafka producer and consumer helpers.

use crate::config::BusConfig;
use crate::error::BusError;
use ironledger_domain::LedgerEvent;
#[cfg(feature = "kafka")]
use metrics::{counter, histogram};
#[cfg(feature = "kafka")]
use std::time::Instant;
#[cfg(feature = "kafka")]
use tracing::{error, info, warn};

#[cfg(feature = "kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::consumer::{Consumer, StreamConsumer};
#[cfg(feature = "kafka")]
use rdkafka::message::Message;
#[cfg(feature = "kafka")]
use rdkafka::producer::{FutureProducer, FutureRecord};
#[cfg(feature = "kafka")]
use rdkafka::util::Timeout;

/// Publishes ledger events to Kafka-compatible brokers.
#[cfg(feature = "kafka")]
#[derive(Clone)]
pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
}

#[cfg(feature = "kafka")]
impl KafkaPublisher {
    /// Connect a producer.
    pub fn new(config: &BusConfig) -> Result<Self, BusError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "5")
            .create()
            .map_err(|err| BusError::Publish(err.to_string()))?;
        Ok(Self {
            producer,
            topic: config.ledger_topic.clone(),
        })
    }

    /// Publish one event; at-least-once on the broker side.
    pub async fn publish(&self, key: &str, event: &LedgerEvent) -> Result<(), BusError> {
        let payload = event
            .to_bytes()
            .map_err(|err| BusError::Encode(err.to_string()))?;
        self.publish_bytes(&self.topic, key, &payload).await
    }

    /// Publish raw bytes to an arbitrary topic (e.g. dead-letter).
    pub async fn publish_bytes(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<(), BusError> {
        let start = Instant::now();
        let record = FutureRecord::to(topic).key(key).payload(payload);
        self.producer
            .send(record, Timeout::After(std::time::Duration::from_secs(5)))
            .await
            .map_err(|(err, _)| BusError::Publish(err.to_string()))?;
        histogram!("ironledger_kafka_publish_seconds").record(start.elapsed().as_secs_f64());
        counter!("ironledger_kafka_published_total").increment(1);
        Ok(())
    }
}

/// Consumes ledger events with manual offset commit after successful handling.
#[cfg(feature = "kafka")]
pub struct KafkaConsumer {
    consumer: StreamConsumer,
    dlq: String,
    publisher: KafkaPublisher,
}

#[cfg(feature = "kafka")]
impl KafkaConsumer {
    /// Subscribe to the ledger topic.
    pub fn new(config: &BusConfig) -> Result<Self, BusError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|err| BusError::Consume(err.to_string()))?;
        consumer
            .subscribe(&[&config.ledger_topic])
            .map_err(|err| BusError::Consume(err.to_string()))?;
        let publisher = KafkaPublisher::new(config)?;
        Ok(Self {
            consumer,
            dlq: config.dead_letter_topic.clone(),
            publisher,
        })
    }

    /// Run until `shutdown` is signalled.
    pub async fn run<F, Fut>(
        &self,
        mut handler: F,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), BusError>
    where
        F: FnMut(LedgerEvent) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), BusError>> + Send,
    {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("kafka consumer shutting down");
                    break;
                }
                message = self.consumer.recv() => {
                    let message = message.map_err(|err| BusError::Consume(err.to_string()))?;
                    let payload = message.payload().unwrap_or_default();
                    match LedgerEvent::from_bytes(payload) {
                        Ok(event) => {
                            let mut handled = false;
                            for attempt in 0..6 {
                                match handler(event.clone()).await {
                                    Ok(()) => {
                                        handled = true;
                                        break;
                                    }
                                    Err(err) if attempt < 5 => {
                                        counter!("ironledger_kafka_consumer_retries_total")
                                            .increment(1);
                                        crate::retry::sleep_backoff(
                                            attempt,
                                            std::time::Duration::from_millis(100),
                                            std::time::Duration::from_secs(5),
                                        )
                                        .await;
                                        if attempt == 5 {
                                            error!(error = %err, "poison message, sending to dlq");
                                            self.send_dlq(&event, &err.to_string()).await?;
                                        }
                                    }
                                    Err(err) => {
                                        error!(error = %err, "handler failed after retries");
                                        self.send_dlq(&event, &err.to_string()).await?;
                                        break;
                                    }
                                }
                            }
                            if handled {
                                if let Err(err) =
                                    self.consumer.store_offset_from_message(&message)
                                {
                                    warn!(error = %err, "failed to store offset");
                                }
                            }
                        }
                        Err(err) => {
                            counter!("ironledger_kafka_decode_errors_total").increment(1);
                            warn!(error = %err, "skipping undecodable message");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_dlq(&self, event: &LedgerEvent, reason: &str) -> Result<(), BusError> {
        counter!("ironledger_kafka_dlq_total").increment(1);
        warn!(
            event_id = %event.event_id,
            reason,
            "routing event to dead-letter topic"
        );
        let payload = event
            .to_bytes()
            .map_err(|err| BusError::Encode(err.to_string()))?;
        self.publisher
            .publish_bytes(&self.dlq, &event.aggregate_id, &payload)
            .await
    }
}

#[cfg(not(feature = "kafka"))]
mod stub {
    use super::*;

    /// Stub publisher when Kafka is disabled at compile time.
    pub struct KafkaPublisher;

    impl KafkaPublisher {
        /// Return a configuration error because Kafka support is disabled.
        pub fn new(_config: &BusConfig) -> Result<Self, BusError> {
            Err(BusError::Config(
                "kafka feature disabled; rebuild with --features kafka".into(),
            ))
        }

        /// Return a configuration error because Kafka support is disabled.
        pub async fn publish(&self, _key: &str, _event: &LedgerEvent) -> Result<(), BusError> {
            Err(BusError::Config("kafka feature disabled".into()))
        }
    }
}

#[cfg(not(feature = "kafka"))]
pub use stub::KafkaPublisher;
