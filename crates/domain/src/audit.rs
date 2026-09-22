//! Audit metadata for sensitive operations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// Mandatory metadata for administrative adjustments and privileged actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditMetadata {
    /// Operator or service principal identifier (not a secret).
    pub actor: String,
    /// Human reason (required, non-empty).
    pub reason: String,
    /// Optional ticket / change request id.
    pub ticket_id: Option<String>,
    /// When the action was authorized.
    pub authorized_at: DateTime<Utc>,
}

impl AuditMetadata {
    /// Construct validated audit metadata.
    pub fn new(
        actor: impl Into<String>,
        reason: impl Into<String>,
        ticket_id: Option<String>,
    ) -> Result<Self, DomainError> {
        let actor = actor.into();
        let reason = reason.into();
        if actor.trim().is_empty() {
            return Err(DomainError::Invariant(
                "audit actor must not be empty".into(),
            ));
        }
        if reason.trim().is_empty() {
            return Err(DomainError::MissingAdjustmentReason);
        }
        if reason.len() > 1024 {
            return Err(DomainError::Invariant(
                "audit reason exceeds 1024 characters".into(),
            ));
        }
        Ok(Self {
            actor,
            reason,
            ticket_id,
            authorized_at: Utc::now(),
        })
    }
}
