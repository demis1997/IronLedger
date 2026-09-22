# Operations

## Services

| Service | Role |
|---------|------|
| `ledger-service` | gRPC writes, HTTP ops, outbox relay |
| `projector-service` | Kafka consumer + projection replay |
| `reconciler-service` | Periodic authoritative reconciliation |

## Health

- Liveness: `GET /health/live`
- Readiness: `GET /health/ready` (PostgreSQL ping)

## Admin (requires dev bearer when `IRONLEDGER_AUTH_MODE=dev`)

- `GET /admin/outbox`
- `GET /admin/reconciliation`
- `GET /admin/consumers`
