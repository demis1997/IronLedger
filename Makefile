.PHONY: help setup up down migrate build test test-integration lint audit deny bench demo reconcile clean fmt

help:
	@printf '%s\n' \
		'IronLedger developer targets:' \
		'  setup             fetch rustc toolchain and crates' \
		'  up / down         start or stop Docker dependencies' \
		'  migrate           apply SQL migrations' \
		'  build / test      release build or workspace tests' \
		'  lint / fmt        clippy (-D warnings) / rustfmt check' \
		'  audit / deny      cargo-audit / cargo-deny' \
		'  demo / reconcile  run CLI demo or reconciliation' \
		'  clean             cargo clean'

setup:
	rustup show
	cargo fetch

up:
	docker compose up -d

down:
	docker compose down -v

migrate:
	cargo sqlx migrate run --source migrations

build:
	cargo build --workspace --release

test:
	cargo test --workspace

test-integration:
	cargo test -p ironledger-integration-tests -- --ignored

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all -- --check

audit:
	cargo audit

deny:
	cargo deny check

bench:
	cargo bench -p ironledger-benches

demo:
	cargo run -p ironledger-cli -- demo

reconcile:
	cargo run -p ironledger-cli -- reconcile

clean:
	cargo clean
