//! Accounts and balance policies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::AccountId;

/// High-level account classification for seeded exchange charts of accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountKind {
    /// Customer available (spendable) balance.
    CustomerAvailable,
    /// Customer funds locked for pending withdrawal / open orders.
    CustomerLocked,
    /// Exchange hot wallet liability / custody.
    ExchangeHotWallet,
    /// Exchange cold wallet.
    ExchangeColdWallet,
    /// Fee revenue.
    FeeRevenue,
    /// Withdrawal clearing suspense.
    WithdrawalClearing,
    /// Deposit clearing suspense.
    DepositClearing,
    /// Generic liability / asset / equity as needed.
    Other,
}

/// Whether an account may carry a negative balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountPolicy {
    /// Reject any posting that would make the balance negative.
    NonNegative,
    /// Allow negative balances (e.g. certain clearing / revenue accounts).
    AllowNegative,
}

/// Lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    /// Accepts postings.
    Active,
    /// Frozen — postings rejected.
    Frozen,
    /// Closed — postings rejected.
    Closed,
}

impl AccountStatus {
    /// Human-readable status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Frozen => "frozen",
            Self::Closed => "closed",
        }
    }
}

/// Ledger account aggregate root (without balances — balances are projected).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    /// Identifier.
    pub id: AccountId,
    /// Display name / owner label.
    pub name: String,
    /// Classification.
    pub kind: AccountKind,
    /// Balance policy.
    pub policy: AccountPolicy,
    /// Status.
    pub status: AccountStatus,
    /// Creation time (UTC).
    pub created_at: DateTime<Utc>,
}

impl Account {
    /// Create an active account.
    pub fn new(
        id: AccountId,
        name: impl Into<String>,
        kind: AccountKind,
        policy: AccountPolicy,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            kind,
            policy,
            status: AccountStatus::Active,
            created_at: Utc::now(),
        }
    }

    /// Whether postings are accepted.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == AccountStatus::Active
    }
}
