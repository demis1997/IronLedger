//! # ironledger-reconciler
//!
//! Rebuilds balances from immutable posting history and compares them against
//! materialized views. Discrepancies indicate corruption, partial writes or
//! projection bugs — never silent drift.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod error;
pub mod port;
pub mod reconciler;

pub use error::ReconcileError;
pub use port::{BalanceView, Discrepancy, PostingHistory, ReconcileReport};
pub use reconciler::Reconciler;
