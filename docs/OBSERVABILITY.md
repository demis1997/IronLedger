# Observability

## Structured logging

Services initialize `tracing-subscriber` with JSON formatting and `RUST_LOG` filtering.
Financial payloads should never include secrets; correlation and causation IDs travel with commands and events.

## Metrics (Prometheus)

`GET /metrics` exposes counters and histograms including:

- command throughput / failures
- Kafka publish latency
- outbox backlog
- projector apply outcomes
- reconciliation discrepancies
- DLQ / retry counts

## Tracing

OpenTelemetry crates are wired for OTLP export (`OTEL_EXPORTER_OTLP_ENDPOINT`). Event envelopes carry `trace_context` for cross-service correlation.
