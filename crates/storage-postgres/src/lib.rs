//! # ironledger-storage-postgres
//!
//! PostgreSQL adapters for ledger ports, the transactional outbox, balance
//! projections and reconciliation views.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

mod convert;
mod error;
mod history;
mod projection;
mod store;

pub use error::StorageError;
pub use history::{PostgresBalanceViews, PostgresHistory};
pub use projection::PostgresProjection;
pub use store::PostgresStore;
