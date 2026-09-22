# ADR 0003: At-least-once processing

## Status

Accepted

## Decision

Use Kafka with manual offset commit after successful handler execution. Retries with backoff; poison messages route to a DLQ topic.

## Consequences

Business handlers must be idempotent; the platform targets effectively-once outcomes, not transport exactly-once.
