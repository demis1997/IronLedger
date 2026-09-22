# Failure modes

| Failure | Behaviour |
|---------|-----------|
| PostgreSQL unavailable | Commands fail; readiness not ready; no partial commits |
| Kafka unavailable | Outbox backlog grows; relay retries with lease |
| Relay crash after publish | Duplicate Kafka messages; consumers dedupe |
| Duplicate idempotency key | Same payload replays; different payload conflicts |
| Projector restart | Replay from outbox log + resume Kafka offsets |
