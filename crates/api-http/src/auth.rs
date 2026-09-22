//! Authentication interfaces (development adapter only).

use async_trait::async_trait;
use thiserror::Error;

/// Authorization failure.
#[derive(Debug, Error)]
#[error("unauthorized")]
pub struct AuthError;

/// Validates administrative requests.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Authorize a bearer token (may be absent).
    async fn authorize(&self, bearer: Option<&str>) -> Result<(), AuthError>;
}

/// Compare two secrets in constant time.
fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Development-only bearer token check. **Not production authentication.**
#[derive(Debug, Clone)]
pub struct DevTokenAuthorizer {
    token: String,
}

impl DevTokenAuthorizer {
    /// Create from a shared secret configured out-of-band.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// Load from `IRONLEDGER_DEV_TOKEN` when mode is `dev`.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("IRONLEDGER_DEV_TOKEN").unwrap_or_else(|_| "dev-only-change-me".into()),
        )
    }
}

#[async_trait]
impl Authorizer for DevTokenAuthorizer {
    async fn authorize(&self, bearer: Option<&str>) -> Result<(), AuthError> {
        let Some(token) = bearer else {
            return Err(AuthError);
        };
        if constant_time_eq(token, &self.token) {
            Ok(())
        } else {
            Err(AuthError)
        }
    }
}

/// No-op authorizer for local demos without auth headers.
#[derive(Debug, Clone, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(&self, _bearer: Option<&str>) -> Result<(), AuthError> {
        Ok(())
    }
}
