//! Money as (asset, amount) pairs — still integer-only.

use serde::{Deserialize, Serialize};

use crate::amount::AtomicAmount;
use crate::asset::{AssetCode, AssetScale};
use crate::error::DomainError;

/// Quantified holding of a single asset in atomic units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    /// Asset identifier.
    pub asset: AssetCode,
    /// Signed atomic amount.
    pub amount: AtomicAmount,
}

impl Money {
    /// Construct money; amount may be negative for credits/debits in postings.
    pub fn new(asset: AssetCode, amount: AtomicAmount) -> Self {
        Self { asset, amount }
    }

    /// Parse a decimal display string into atomic units using `scale`.
    ///
    /// Accepts optional leading `+`/`-`, an integer part, and an optional
    /// fractional part truncated/padded to `scale`. No floating point is used.
    pub fn parse_decimal(
        asset: AssetCode,
        scale: AssetScale,
        text: &str,
    ) -> Result<Self, DomainError> {
        let text = text.trim();
        if text.is_empty() {
            return Err(DomainError::InvalidMoneyFormat {
                value: text.to_owned(),
            });
        }

        let (negative, body) = match text.as_bytes()[0] {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };

        if body.is_empty() || !body.chars().all(|c| c.is_ascii_digit() || c == '.') {
            return Err(DomainError::InvalidMoneyFormat {
                value: text.to_owned(),
            });
        }

        let (whole, frac) = match body.split_once('.') {
            Some((w, f)) => (w, f),
            None => (body, ""),
        };

        if whole.is_empty() || !whole.chars().all(|c| c.is_ascii_digit()) {
            return Err(DomainError::InvalidMoneyFormat {
                value: text.to_owned(),
            });
        }
        if !frac.chars().all(|c| c.is_ascii_digit()) {
            return Err(DomainError::InvalidMoneyFormat {
                value: text.to_owned(),
            });
        }
        if frac.len() > usize::from(scale.get()) {
            return Err(DomainError::InvalidMoneyFormat {
                value: format!("fractional precision exceeds scale {}", scale.get()),
            });
        }

        let mult = scale.multiplier()?;
        let whole_i: i128 = whole.parse().map_err(|_| DomainError::AmountOverflow)?;
        let whole_units = whole_i
            .checked_mul(mult)
            .ok_or(DomainError::AmountOverflow)?;

        let mut frac_padded = frac.to_owned();
        while frac_padded.len() < usize::from(scale.get()) {
            frac_padded.push('0');
        }
        let frac_units: i128 = if frac_padded.is_empty() {
            0
        } else {
            frac_padded
                .parse()
                .map_err(|_| DomainError::AmountOverflow)?
        };

        let mut units = whole_units
            .checked_add(frac_units)
            .ok_or(DomainError::AmountOverflow)?;
        if negative {
            units = units.checked_neg().ok_or(DomainError::AmountOverflow)?;
        }

        Ok(Self::new(asset, AtomicAmount::from_raw(units)))
    }

    /// Checked addition (same asset required).
    pub fn checked_add(&self, other: &Self) -> Result<Self, DomainError> {
        if self.asset != other.asset {
            return Err(DomainError::AssetMismatch {
                left: self.asset.to_string(),
                right: other.asset.to_string(),
            });
        }
        Ok(Self::new(
            self.asset.clone(),
            self.amount.checked_add(other.amount)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_btc_sats() {
        let scale = AssetScale::new(8).unwrap();
        let m = Money::parse_decimal(AssetCode::btc(), scale, "1.5").unwrap();
        assert_eq!(m.amount.raw(), 150_000_000);
    }

    #[test]
    fn parse_rejects_excess_precision() {
        let scale = AssetScale::new(2).unwrap();
        assert!(Money::parse_decimal(AssetCode::usd(), scale, "1.001").is_err());
    }

    #[test]
    fn no_float_path_for_large_values() {
        let scale = AssetScale::new(0).unwrap();
        let m = Money::parse_decimal(AssetCode::usd(), scale, "9007199254740993").unwrap();
        assert_eq!(m.amount.raw(), 9_007_199_254_740_993);
    }
}
