# ADR 0002: Transactional outbox

## Status

Accepted

## Decision

Persist outbox rows in the same PostgreSQL transaction as journal entries and idempotency records. A separate relay publishes to Kafka.

## Consequences

At-least-once publication is acceptable; consumers deduplicate by event id.
