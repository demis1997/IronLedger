//! # ironledger-ledger
//!
//! Application layer for the IronLedger double-entry ledger: command handlers,
//! the ports (traits) they depend on, and idempotent request handling.
//!
//! This crate deliberately has **no infrastructure dependencies** — no SQL, no
//! Kafka, no HTTP. Adapters live in `ironledger-storage-postgres`,
//! `ironledger-event-bus`, `ironledger-api-grpc` and `ironledger-api-http`.
//!
//! ## Command pipeline
//!
//! Every mutating command follows the same path:
//!
//! 1. Fingerprint the request (`sha256` over canonical JSON of the business
//!    payload, excluding per-attempt metadata such as correlation ids).
//! 2. Look up `(scope, idempotency_key)`. A hit with a matching fingerprint
//!    replays the stored response; a hit with a different fingerprint is an
//!    [`LedgerError::IdempotencyConflict`].
//! 3. Build a balanced [`JournalEntry`](ironledger_domain::JournalEntry) via the
//!    domain flow builders.
//! 4. Guard the postings: accounts must exist and be active, and every negative
//!    delta is checked against the account's
//!    [`AccountPolicy`](ironledger_domain::AccountPolicy).
//! 5. Persist entry, postings, balance updates, outbox events and the
//!    idempotency record in a **single** adapter-level transaction.
//!
//! Step 5 is one port call ([`LedgerRepo::commit`]) precisely so that no adapter
//! can split it into multiple transactions.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod chart;
pub mod command;
pub mod error;
pub mod idempotency;
pub mod port;
pub mod service;

#[cfg(any(test, feature = "memory"))]
pub mod memory;

pub use command::{
    AuditRequest, Command, CommandMeta, CompleteWithdrawalCommand, CreateAccountCommand,
    PostingInput, RecordAdminAdjustmentCommand, RecordDepositCommand, RecordTradeSettlementCommand,
    RecordTradingFeeCommand, RejectWithdrawalCommand, RequestWithdrawalCommand,
    SubmitJournalCommand,
};
pub use error::{ErrorCategory, LedgerError};
pub use idempotency::{canonical_json, IdempotencyRecord, RequestHash};
pub use port::{
    AccountCommit, AccountRepo, BalanceRecord, BalanceRepo, CommitOutcome, ConsumerOffset,
    ConsumerRepo, ConsumerStatus, HealthCheck, IdempotencyRepo, JournalCommit, LedgerRepo,
    OutboxMessage, OutboxRepo, OutboxStats, Page,
};
pub use service::{AccountCreated, EntryPosted, LedgerService, Outcome};
