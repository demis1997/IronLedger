# Architecture

IronLedger separates **domain** (pure invariants), **application** (commands, idempotency), and **adapters** (PostgreSQL, Kafka, gRPC, HTTP).

## Write path

1. gRPC/CLI builds a command with `CommandMeta` (idempotency key + tracing ids).
2. `LedgerService` fingerprints the payload, checks idempotency, validates domain flows.
3. `PostgresStore::commit` applies balances, journal, idempotency, and outbox in one transaction.
4. `OutboxRelay` publishes to Redpanda; duplicates may occur after crashes.

## Read path

- Authoritative balances live in `balances`.
- `projector-service` maintains `projection_balances` with `(consumer, event_id)` deduplication.
- `reconciler-service` rebuilds from postings and compares views.

## Consistency

This is **not** exactly-once end-to-end. Business idempotency + outbox + consumer dedup yields **effectively-once outcomes** on at-least-once transport.
