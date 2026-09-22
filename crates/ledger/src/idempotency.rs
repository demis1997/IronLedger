//! Request fingerprinting and idempotency records.

use chrono::{DateTime, Utc};
use ironledger_domain::IdempotencyKey;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fmt;

use crate::error::LedgerError;

/// SHA-256 fingerprint of a command's business payload, hex encoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestHash(String);

impl RequestHash {
    /// Fingerprint a serializable payload within a command scope.
    ///
    /// The scope is mixed in so that the same key reused for a different
    /// command type can never accidentally match.
    pub fn compute<T: Serialize>(scope: &str, payload: &T) -> Result<Self, LedgerError> {
        let value = serde_json::to_value(payload).map_err(LedgerError::serialization)?;
        let canonical = canonical_json(&value);
        let mut hasher = Sha256::new();
        hasher.update(scope.as_bytes());
        hasher.update([0u8]);
        hasher.update(canonical.as_bytes());
        Ok(Self(hex::encode(hasher.finalize())))
    }

    /// Borrow the hex digest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Wrap an existing hex digest read from storage.
    #[must_use]
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }
}

impl fmt::Display for RequestHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Serialize a JSON value with object keys sorted and no insignificant
/// whitespace, so that logically equal payloads always hash identically.
///
/// `serde_json`'s default `Map` is already ordered, but this function does not
/// rely on that: enabling the `preserve_order` feature anywhere in the
/// dependency graph must not silently change stored fingerprints.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    canonicalize(value).to_string()
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(entries) => {
            let mut sorted: Vec<(&String, &Value)> = entries.iter().collect();
            sorted.sort_by(|(left, _), (right, _)| left.cmp(right));
            let mut out = Map::with_capacity(sorted.len());
            for (key, item) in sorted {
                out.insert(key.clone(), canonicalize(item));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Persisted result of a previously accepted command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    /// Command scope, e.g. `record_deposit`.
    pub scope: String,
    /// Client-supplied key, unique within the scope.
    pub key: IdempotencyKey,
    /// Fingerprint of the original request payload.
    pub request_hash: RequestHash,
    /// Stored response, replayed verbatim on retry.
    pub response: Value,
    /// When the original command was accepted.
    pub created_at: DateTime<Utc>,
}

impl IdempotencyRecord {
    /// Build a record for a command that is about to be committed.
    pub fn new<T: Serialize>(
        scope: impl Into<String>,
        key: IdempotencyKey,
        request_hash: RequestHash,
        response: &T,
    ) -> Result<Self, LedgerError> {
        Ok(Self {
            scope: scope.into(),
            key,
            request_hash,
            response: serde_json::to_value(response).map_err(LedgerError::serialization)?,
            created_at: Utc::now(),
        })
    }

    /// Replay the stored response, or fail if the request payload differs.
    pub fn replay<T: for<'de> Deserialize<'de>>(
        &self,
        request_hash: &RequestHash,
    ) -> Result<T, LedgerError> {
        if &self.request_hash != request_hash {
            return Err(LedgerError::IdempotencyConflict {
                scope: self.scope.clone(),
                key: self.key.to_string(),
            });
        }
        serde_json::from_value(self.response.clone()).map_err(LedgerError::serialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_form_is_key_order_independent() {
        let a = json!({"b": 1, "a": {"d": 2, "c": [3, {"f": 4, "e": 5}]}});
        let b = json!({"a": {"c": [3, {"e": 5, "f": 4}], "d": 2}, "b": 1});
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn scope_changes_the_fingerprint() {
        let payload = json!({"amount": "10"});
        let left = RequestHash::compute("deposit", &payload).unwrap();
        let right = RequestHash::compute("withdrawal", &payload).unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn scope_separator_prevents_boundary_collisions() {
        // Without a delimiter, ("ab", "c") and ("a", "bc") would collide.
        let left = RequestHash::compute("ab", &"c").unwrap();
        let right = RequestHash::compute("a", &"bc").unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn replay_rejects_a_different_payload() {
        let key = IdempotencyKey::new("k-1").unwrap();
        let hash = RequestHash::compute("deposit", &json!({"amount": "10"})).unwrap();
        let record = IdempotencyRecord::new("deposit", key, hash, &json!({"ok": true})).unwrap();

        let other = RequestHash::compute("deposit", &json!({"amount": "11"})).unwrap();
        let replayed = record.replay::<Value>(&other);
        assert!(matches!(
            replayed,
            Err(LedgerError::IdempotencyConflict { .. })
        ));
    }
}
