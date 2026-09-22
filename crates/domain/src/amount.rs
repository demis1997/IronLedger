//! Fixed-precision monetary amounts as integer atomic units.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::{Add, Neg, Sub};
use std::str::FromStr;

use crate::error::DomainError;

/// Signed quantity in the smallest indivisible unit of an asset (e.g. satoshi, micro-USD).
///
/// Never use floating point for ledger math. All arithmetic is overflow-checked.
///
/// Serialization uses a **decimal string** rather than a JSON number: the full
/// `i128` range is not representable as an IEEE-754 double, and most JSON
/// implementations (including `serde_json` without `arbitrary_precision`)
/// cannot round-trip 128-bit integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AtomicAmount(i128);

impl AtomicAmount {
    /// Zero amount.
    pub const ZERO: Self = Self(0);

    /// Construct from raw atomic units.
    #[must_use]
    pub const fn from_raw(units: i128) -> Self {
        Self(units)
    }

    /// Raw integer units.
    #[must_use]
    pub const fn raw(self) -> i128 {
        self.0
    }

    /// Absolute value with overflow check (`i128::MIN` is rejected).
    pub fn checked_abs(self) -> Result<Self, DomainError> {
        self.0
            .checked_abs()
            .map(Self)
            .ok_or(DomainError::AmountOverflow)
    }

    /// Checked addition.
    pub fn checked_add(self, rhs: Self) -> Result<Self, DomainError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(DomainError::AmountOverflow)
    }

    /// Checked subtraction.
    pub fn checked_sub(self, rhs: Self) -> Result<Self, DomainError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(DomainError::AmountOverflow)
    }

    /// Checked negation.
    pub fn checked_neg(self) -> Result<Self, DomainError> {
        self.0
            .checked_neg()
            .map(Self)
            .ok_or(DomainError::AmountOverflow)
    }

    /// Whether the amount is strictly negative.
    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// Whether the amount is zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Whether the amount is strictly positive.
    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    /// Require a strictly positive amount (used for transfer magnitudes).
    pub fn require_positive(self) -> Result<Self, DomainError> {
        if self.0 <= 0 {
            return Err(DomainError::NonPositiveAmount { amount: self.0 });
        }
        Ok(self)
    }
}

impl fmt::Display for AtomicAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AtomicAmount {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.trim()
            .parse::<i128>()
            .map(Self)
            .map_err(|_| DomainError::InvalidMoneyFormat {
                value: s.to_owned(),
            })
    }
}

impl Serialize for AtomicAmount {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AtomicAmount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AmountVisitor;

        impl Visitor<'_> for AmountVisitor {
            type Value = AtomicAmount;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a decimal string or integer of atomic units")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                AtomicAmount::from_str(value).map_err(de::Error::custom)
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(AtomicAmount(i128::from(value)))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(AtomicAmount(i128::from(value)))
            }

            fn visit_i128<E: de::Error>(self, value: i128) -> Result<Self::Value, E> {
                Ok(AtomicAmount(value))
            }

            fn visit_u128<E: de::Error>(self, value: u128) -> Result<Self::Value, E> {
                i128::try_from(value)
                    .map(AtomicAmount)
                    .map_err(|_| de::Error::custom("atomic amount exceeds i128 range"))
            }
        }

        deserializer.deserialize_any(AmountVisitor)
    }
}

impl Add for AtomicAmount {
    type Output = Result<Self, DomainError>;

    fn add(self, rhs: Self) -> Self::Output {
        self.checked_add(rhs)
    }
}

impl Sub for AtomicAmount {
    type Output = Result<Self, DomainError>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.checked_sub(rhs)
    }
}

impl Neg for AtomicAmount {
    type Output = Result<Self, DomainError>;

    fn neg(self) -> Self::Output {
        self.checked_neg()
    }
}

impl From<i64> for AtomicAmount {
    fn from(value: i64) -> Self {
        Self(i128::from(value))
    }
}

impl From<i32> for AtomicAmount {
    fn from(value: i32) -> Self {
        Self(i128::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addition_overflow_is_detected() {
        let a = AtomicAmount::from_raw(i128::MAX);
        assert!(a.checked_add(AtomicAmount::from_raw(1)).is_err());
    }

    #[test]
    fn negation_of_min_overflows() {
        let a = AtomicAmount::from_raw(i128::MIN);
        assert!(a.checked_neg().is_err());
    }

    #[test]
    fn require_positive_rejects_zero() {
        assert!(AtomicAmount::ZERO.require_positive().is_err());
    }

    #[test]
    fn serializes_as_decimal_string() {
        let json = serde_json::to_string(&AtomicAmount::from_raw(i128::MAX)).unwrap();
        assert_eq!(json, format!("\"{}\"", i128::MAX));
    }

    #[test]
    fn round_trips_full_i128_range() {
        for raw in [i128::MIN + 1, -1, 0, 1, i128::MAX] {
            let amount = AtomicAmount::from_raw(raw);
            let json = serde_json::to_string(&amount).unwrap();
            let decoded: AtomicAmount = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, amount);
        }
    }

    #[test]
    fn accepts_json_integers_for_compatibility() {
        let decoded: AtomicAmount = serde_json::from_str("-42").unwrap();
        assert_eq!(decoded.raw(), -42);
    }

    #[test]
    fn rejects_non_numeric_strings() {
        assert!(serde_json::from_str::<AtomicAmount>("\"1.5\"").is_err());
    }
}
