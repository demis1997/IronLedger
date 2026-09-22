# Multi-stage image for the ledger write service.
FROM rust:1.85.1-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p ironledger-ledger-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ledger-service /usr/local/bin/ledger-service
ENV RUST_LOG=info,ironledger=debug
EXPOSE 8080 50051
USER nobody
ENTRYPOINT ["/usr/local/bin/ledger-service"]
