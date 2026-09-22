# ADR 0004: Fixed-precision money

## Status

Accepted

## Decision

Represent all ledger amounts as signed `i128` atomic units. Parse decimal strings at boundaries without floating point.

## Consequences

Overflow is explicit error; PostgreSQL stores amounts as decimal strings for full range.
