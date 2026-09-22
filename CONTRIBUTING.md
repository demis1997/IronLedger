# Contributing

1. Fork and branch from `main`.
2. Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace`.
3. Keep domain logic free of infrastructure crates.
4. Do not commit secrets or `.env` files.
