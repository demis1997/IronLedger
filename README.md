# IronLedger

Event-driven, auditable **digital-asset ledger** and transaction-orchestration platform in Rust.

> Educational / portfolio system demonstrating production-oriented financial infrastructure.
> **Not audited. Not intended to custody real funds.**

## Capabilities demonstrated

| Capability | Where to look |
|------------|---------------|
| Async Rust + Tokio network APIs | `services/ledger-service` (gRPC + Axum HTTP), `crates/api-grpc`, `crates/api-http` |
| Double-entry / append-only correctness | `crates/domain` journal invariants; `docs/ADRS/0001-double-entry-ledger.md` |
| Idempotent transaction processing | `crates/ledger` request hashing + DB uniqueness; integration tests |
| PostgreSQL persistence + atomic TX | `crates/storage-postgres`, `migrations/`; journal + balances + outbox + idempotency in one commit |
| Kafka / event-driven architecture | `crates/event-bus` outbox relay + Redpanda consumer; `docs/ADRS/0002-*`, `0003-*` |
| Concurrency + failure recovery | Outbox leases, consumer retries/DLQ, Toxiproxy in Compose, `docs/FAILURE_MODES.md` |
| Structured logging, metrics, tracing | `tracing` JSON logs, Prometheus `/metrics`, OpenTelemetry deps, correlation IDs on events |
| AuthN / AuthZ + secret handling | `crates/api-http/src/auth.rs` (trait + constant-time bearer); `.env.example` (never commit secrets) |
| Integration, property, fuzz tests | `crates/domain/tests`, `tests/integration`, `fuzz/` |
| Docker, CI, architecture docs | `docker-compose.yml`, `Dockerfile`, `.github/workflows/ci.yml`, `docs/` |

## Architecture

```mermaid
flowchart LR
  CLI[ironledger-cli] --> GRPC[gRPC]
  CLI --> HTTP[HTTP ops]
  GRPC --> LS[ledger-service]
  HTTP --> LS
  LS --> PG[(PostgreSQL)]
  LS --> OB[transactional outbox]
  OB --> RP[Redpanda]
  RP --> PS[projector-service]
  PS --> PG
  RS[reconciler-service] --> PG
```

## Core invariants

- Journal entries have ≥2 postings and **balance per asset** (integer atomic units only — no floats).
- Idempotency keys are unique per command scope: same payload replays; different payload conflicts.
- Journal, balances, idempotency record, and outbox rows commit in **one PostgreSQL transaction**.
- Projectors deduplicate by `(consumer, event_id)` for effectively-once business outcomes on at-least-once delivery.

## Quick start

Install Rust via rustup (the repository pins Rust 1.85.1), a C/C++ toolchain,
CMake, pkg-config, and the Protocol Buffers compiler (`protoc`). Docker Compose
is required for the local PostgreSQL and Redpanda services.

```bash
cp .env.example .env
make up
export DATABASE_URL=postgres://ironledger:ironledger@127.0.0.1:5432/ironledger
cargo sqlx migrate run --source migrations
cargo run -p ironledger-ledger-service
# other terminals:
cargo run -p ironledger-projector-service
cargo run -p ironledger-cli -- demo
cargo test --workspace
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Threat model (STRIDE)](docs/THREAT_MODEL.md)
- [Failure modes](docs/FAILURE_MODES.md)
- [Operations](docs/OPERATIONS.md)
- [Development](docs/DEVELOPMENT.md)
- [ADRs](docs/ADRS/)

## License

Apache-2.0 — see [LICENSE](LICENSE).
