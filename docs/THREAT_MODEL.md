# Threat model (STRIDE)

| Threat | Mitigation |
|--------|------------|
| Duplicate transaction submission | Idempotency keys + DB uniqueness; replay vs conflict |
| Replay attacks | Idempotency scope + request hash; consumer event dedup |
| Unauthorised admin adjustments | Dev-only bearer auth interface; audit metadata on adjustments |
| Kafka message tampering | TLS in production (plaintext in local compose); schema version checks |
| Database compromise | Least-privilege example roles; no secrets in repo |
| Secret leakage | `.env` gitignored; structured logs; no secrets in metrics |
| Log injection | Structured tracing; validate input lengths |
| Denial of service | HTTP body limits, timeouts, bounded page sizes |
| Integer overflow | `AtomicAmount` checked arithmetic |
| Insider misuse | Audit metadata on admin flows |
| Race conditions | DB transactions + row locks on balances |
| Supply-chain compromise | `cargo audit`, `cargo deny`, Dependabot |

Production authentication is **not** implemented; use `DevTokenAuthorizer` only locally.
