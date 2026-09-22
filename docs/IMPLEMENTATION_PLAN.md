# IronLedger Implementation Plan

Educational portfolio system demonstrating production-oriented ledger architecture.
Not audited; not intended to custody real funds.

## Phases

1. Workspace + domain primitives with invariant enforcement
2. PostgreSQL schema, repositories, idempotency, transactional outbox
3. Redpanda producer/consumer with retry, DLQ, deduplication
4. Balance projector + reconciler workers
5. gRPC (Tonic) + HTTP (Axum) operational APIs
6. CLI demo, observability, Docker Compose, Toxiproxy harness
7. Property/integration/failure tests, Criterion benches
8. Docs (ADRs, threat model, ops), CI, Dependabot
9. Validate gates, focused commits, push to `demis1997/IronLedger`

## Architectural choices

- **Workspace crates** mirror bounded contexts (domain → application → adapters).
- **Atomic integer money** (`i128` units) — no floats in domain.
- **Effectively-once business outcomes** via DB uniqueness + outbox + consumer idempotency on at-least-once Kafka delivery.
- **Dev-only auth adapter** behind a trait; not production authentication.

## Non-goals

Cryptocurrency wallets, trading bots, UI, real settlement networks.
