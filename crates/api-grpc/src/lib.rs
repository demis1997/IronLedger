//! # ironledger-api-grpc
//!
//! Tonic gRPC API for ledger commands and queries.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod map;
pub mod proto;
pub mod server;

pub use server::GrpcServices;
