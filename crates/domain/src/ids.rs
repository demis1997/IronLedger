//! Strongly typed identifiers.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use crate::error::DomainError;

macro_rules! uuid_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new random identifier (UUID v4).
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Construct from a raw UUID.
            #[must_use]
            pub const fn from_uuid(id: Uuid) -> Self {
                Self(id)
            }

            /// Borrow the underlying UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            /// Consume into UUID.
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s)
                    .map(Self)
                    .map_err(|_| DomainError::InvalidIdentifier {
                        kind: stringify!($name),
                        value: s.to_owned(),
                    })
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    };
}

uuid_newtype!(
    /// Stable account identifier.
    AccountId
);
uuid_newtype!(
    /// Ledger transaction / journal entry identifier.
    TransactionId
);
uuid_newtype!(
    /// Correlation ID shared across a business workflow.
    CorrelationId
);
uuid_newtype!(
    /// Causation ID linking an effect to its cause.
    CausationId
);

/// Client-supplied idempotency key scoped to an operation.
///
/// Uniqueness is enforced per operation scope in storage; this type only
/// validates non-emptiness and length bounds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Maximum accepted key length.
    pub const MAX_LEN: usize = 128;

    /// Create a validated idempotency key.
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::InvalidIdempotencyKey {
                reason: "key must not be empty".into(),
            });
        }
        if value.len() > Self::MAX_LEN {
            return Err(DomainError::InvalidIdempotencyKey {
                reason: format!("key exceeds {} bytes", Self::MAX_LEN),
            });
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
        {
            return Err(DomainError::InvalidIdempotencyKey {
                reason: "key contains invalid characters".into(),
            });
        }
        Ok(Self(value))
    }

    /// Borrow the raw key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for IdempotencyKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_rejects_empty() {
        assert!(IdempotencyKey::new("").is_err());
    }

    #[test]
    fn idempotency_key_accepts_valid() {
        let k = IdempotencyKey::new("deposit:cust-1:001").unwrap();
        assert_eq!(k.as_str(), "deposit:cust-1:001");
    }
}
