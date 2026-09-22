//! Asset codes and scale metadata.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::DomainError;

/// Number of decimal places between display units and atomic units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AssetScale(u8);

impl AssetScale {
    /// Construct a scale (0..=18).
    pub fn new(scale: u8) -> Result<Self, DomainError> {
        if scale > 18 {
            return Err(DomainError::InvalidAssetScale { scale });
        }
        Ok(Self(scale))
    }

    /// Raw scale.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Multiplier `10^scale` as i128.
    pub fn multiplier(self) -> Result<i128, DomainError> {
        10_i128
            .checked_pow(u32::from(self.0))
            .ok_or(DomainError::AmountOverflow)
    }
}

/// ISO-like asset ticker used as a ledger partition key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AssetCode(String);

impl AssetCode {
    /// Create a validated asset code (2..=16 uppercase ASCII alphanumerics).
    pub fn new(code: impl Into<String>) -> Result<Self, DomainError> {
        let code = code.into().to_ascii_uppercase();
        if !(2..=16).contains(&code.len()) {
            return Err(DomainError::InvalidAssetCode {
                code,
                reason: "length must be 2..=16".into(),
            });
        }
        if !code.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(DomainError::InvalidAssetCode {
                code,
                reason: "must be ASCII alphanumeric".into(),
            });
        }
        Ok(Self(code))
    }

    /// Borrow the code string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// BTC with 8 decimal places (satoshis).
    pub fn btc() -> Self {
        Self::new("BTC").expect("static")
    }

    /// USD with 6 decimal places (micro-USD) for ledger precision.
    pub fn usd() -> Self {
        Self::new("USD").expect("static")
    }
}

impl fmt::Display for AssetCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for AssetCode {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for AssetCode {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Registered asset definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Asset {
    /// Ticker / code.
    pub code: AssetCode,
    /// Decimal scale for display ↔ atomic conversion at API boundaries.
    pub scale: AssetScale,
    /// Human-readable name.
    pub name: String,
}

impl Asset {
    /// Construct a validated asset.
    pub fn new(code: AssetCode, scale: AssetScale, name: impl Into<String>) -> Self {
        Self {
            code,
            scale,
            name: name.into(),
        }
    }

    /// Built-in BTC asset.
    pub fn btc() -> Self {
        Self::new(
            AssetCode::btc(),
            AssetScale::new(8).expect("static"),
            "Bitcoin",
        )
    }

    /// Built-in USD asset (micro-USD units).
    pub fn usd() -> Self {
        Self::new(
            AssetCode::usd(),
            AssetScale::new(6).expect("static"),
            "US Dollar",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_code_normalizes_case() {
        assert_eq!(AssetCode::new("btc").unwrap().as_str(), "BTC");
    }

    #[test]
    fn asset_code_rejects_symbols() {
        assert!(AssetCode::new("BT$").is_err());
    }
}
