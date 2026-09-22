# Development workflow

## Branches

| Branch | Purpose |
|--------|---------|
| `main` | Stable, CI-green history |
| `dev` | Integration branch |
| `feat/*` | Short-lived features cut from `dev` or `main` |

## Local loop

```bash
make up
export DATABASE_URL=postgres://ironledger:ironledger@127.0.0.1:5432/ironledger
cargo sqlx migrate run --source migrations
cargo test --workspace
cargo run -p ironledger-cli -- demo
```

Prefer small, focused commits. Never commit `.env` or secrets.
