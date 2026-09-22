# IronLedger

IronLedger is an * event-driven, auditable digital-asset ledger and transaction-orchestration platform.

## Architecture

```mermaid
flowchart LR
  CLI[ironledger-cli] --> GRPC[gRPC / HTTP]
  GRPC --> LS[ledger-service]
  LS --> PG[(PostgreSQL)]
  LS --> OB[outbox]
  OB --> RP[Redpanda]
  RP --> PS[projector-service]
  PS --> PG
  RS[reconciler-service] --> PG
```

## Core invariants

- Every accepted journal entry has ≥2 postings and **balances per asset** (integer atomic units only).
- Idempotency keys are unique per command scope; duplicate payloads replay, conflicting payloads fail.
- Journal entry, balances, idempotency record, and outbox rows commit in **one database transaction**.
- Projectors deduplicate by `(consumer, event_id)`.

## Quick start

```bash
cd ironledger
cp .env.example .env
make up
export DATABASE_URL=postgres://ironledger:ironledger@127.0.0.1:5432/ironledger
sqlx database create 2>/dev/null || true
cargo sqlx migrate run --source migrations
cargo run -p ledger-service
# separate terminals:
cargo run -p projector-service
cargo run -p ironledger-cli -- demo
cargo test --workspace
```

## Repository layout

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).
