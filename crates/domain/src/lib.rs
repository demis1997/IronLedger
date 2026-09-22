//! # ironledger-domain
//!
//! Pure domain types for an event-driven, double-entry digital-asset ledger.
//!
//! This crate has **no infrastructure dependencies**. Monetary quantities are
//! fixed-precision integer units (`AtomicAmount`). Floating-point types are
//! forbidden in financial logic.
//!
//! ## Core invariants
//!
//! - A [`JournalEntry`] has at least two [`Posting`]s.
//! - Postings balance **per asset** (sum of signed amounts is zero for each asset).
//! - Overflow-checked arithmetic on all money operations.
//! - Negative balances are rejected unless an [`AccountPolicy`] permits them.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod account;
pub mod amount;
pub mod asset;
pub mod audit;
pub mod error;
pub mod event;
pub mod flow;
pub mod ids;
pub mod journal;
pub mod money;

pub use account::{Account, AccountKind, AccountPolicy, AccountStatus};
pub use amount::AtomicAmount;
pub use asset::{Asset, AssetCode, AssetScale};
pub use audit::AuditMetadata;
pub use error::DomainError;
pub use event::{EventId, LedgerEvent, LedgerEventPayload};
pub use flow::{
    AdminAdjustment, ConfirmedDeposit, InternalTransfer, TradeSettlement, TradingFee,
    WithdrawalCompletion, WithdrawalRejection, WithdrawalRequest,
};
pub use ids::{AccountId, CausationId, CorrelationId, IdempotencyKey, TransactionId};
pub use journal::{EntryStatus, JournalEntry, Posting, PostingSide};
pub use money::Money;
