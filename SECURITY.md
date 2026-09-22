# Security

Report vulnerabilities privately to the repository owner. Do not open public issues for exploitable flaws.

IronLedger is an **unaudited portfolio project** and must not custody real funds.

## Authentication and authorization

Service-to-service gRPC is intentionally unauthenticated in this portfolio build. Administrative HTTP routes go through an [`Authorizer`](crates/api-http/src/auth.rs) trait:

- `DevTokenAuthorizer` — development-only bearer token with **constant-time** comparison
- `AllowAllAuthorizer` — local demos when `IRONLEDGER_AUTH_MODE` is unset

This is **not** production authentication. A real deployment would plug mTLS / OIDC / capability tokens into the same trait.

## Secrets

- Never commit `.env` (see `.gitignore` and `.env.example`)
- Prefer environment variables; redact bearer tokens from logs
- Example Compose credentials are disposable local defaults only

## Automated checks

CI runs `cargo audit` and `cargo deny check`. See `docs/THREAT_MODEL.md` for STRIDE coverage.
