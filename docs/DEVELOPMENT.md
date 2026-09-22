# Development workflow

## Branches

| Branch | Purpose |
|--------|---------|
| `main` | Stable, reviewable history |
| `dev` | Integration branch for ongoing work |
| `feat/*` | Short-lived feature branches cut from `dev` |

## Local loop

```bash
make up
export DATABASE_URL=postgres://ironledger:ironledger@127.0.0.1:5432/ironledger
cargo sqlx migrate run --source migrations
cargo test --workspace
cargo run -p ironledger-cli -- demo
```

Prefer small, focused commits. Do not commit `.env` or secrets.
