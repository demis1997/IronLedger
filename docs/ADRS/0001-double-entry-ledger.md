# ADR 0001: Double-entry ledger

## Status

Accepted

## Decision

All financial effects are recorded as balanced multi-posting journal entries per asset using signed integer atomic units.

## Consequences

Invalid states are rejected before persistence; reconciliation can rebuild balances from postings.
