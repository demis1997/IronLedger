//! Reference chart of accounts for the demo exchange.
//!
//! Naming is stable so that the CLI demo, integration tests and operators all
//! resolve the same accounts. Policies follow the sign convention documented in
//! [`ironledger_domain::flow`]: customer-facing and revenue accounts are
//! debit-normal and non-negative, while custody and clearing accounts are
//! credit-normal and may go negative.

use ironledger_domain::{AccountKind, AccountPolicy};

/// Custody account holding customer funds (credit-normal).
pub const EXCHANGE_HOT_WALLET: &str = "exchange:hot-wallet";
/// Deep-storage custody account (credit-normal).
pub const EXCHANGE_COLD_WALLET: &str = "exchange:cold-wallet";
/// Incoming-funds suspense account (credit-normal).
pub const DEPOSIT_CLEARING: &str = "exchange:deposit-clearing";
/// Outgoing-payout suspense account (credit-normal).
pub const WITHDRAWAL_CLEARING: &str = "exchange:withdrawal-clearing";
/// Fee revenue account (debit-normal).
pub const FEE_REVENUE: &str = "exchange:fee-revenue";

/// Blueprint for an account that should exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSpec {
    /// Unique account name.
    pub name: String,
    /// Classification.
    pub kind: AccountKind,
    /// Negative-balance policy.
    pub policy: AccountPolicy,
}

impl AccountSpec {
    fn new(name: impl Into<String>, kind: AccountKind, policy: AccountPolicy) -> Self {
        Self {
            name: name.into(),
            kind,
            policy,
        }
    }
}

/// Spendable balance account name for a customer.
#[must_use]
pub fn customer_available(customer: &str) -> String {
    format!("customer:{customer}:available")
}

/// Locked balance account name for a customer.
#[must_use]
pub fn customer_locked(customer: &str) -> String {
    format!("customer:{customer}:locked")
}

/// Exchange-side accounts required by the deposit, withdrawal and fee flows.
#[must_use]
pub fn exchange_accounts() -> Vec<AccountSpec> {
    vec![
        AccountSpec::new(
            EXCHANGE_HOT_WALLET,
            AccountKind::ExchangeHotWallet,
            AccountPolicy::AllowNegative,
        ),
        AccountSpec::new(
            EXCHANGE_COLD_WALLET,
            AccountKind::ExchangeColdWallet,
            AccountPolicy::AllowNegative,
        ),
        AccountSpec::new(
            DEPOSIT_CLEARING,
            AccountKind::DepositClearing,
            AccountPolicy::AllowNegative,
        ),
        AccountSpec::new(
            WITHDRAWAL_CLEARING,
            AccountKind::WithdrawalClearing,
            AccountPolicy::AllowNegative,
        ),
        AccountSpec::new(
            FEE_REVENUE,
            AccountKind::FeeRevenue,
            AccountPolicy::NonNegative,
        ),
    ]
}

/// Available and locked accounts for one customer.
#[must_use]
pub fn customer_accounts(customer: &str) -> Vec<AccountSpec> {
    vec![
        AccountSpec::new(
            customer_available(customer),
            AccountKind::CustomerAvailable,
            AccountPolicy::NonNegative,
        ),
        AccountSpec::new(
            customer_locked(customer),
            AccountKind::CustomerLocked,
            AccountPolicy::NonNegative,
        ),
    ]
}

/// Full chart for the given customers: exchange accounts first, then customers.
#[must_use]
pub fn full_chart(customers: &[&str]) -> Vec<AccountSpec> {
    let mut specs = exchange_accounts();
    for customer in customers {
        specs.extend(customer_accounts(customer));
    }
    specs
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn chart_names_are_unique() {
        let specs = full_chart(&["alice", "bob"]);
        let unique: HashSet<&str> = specs.iter().map(|spec| spec.name.as_str()).collect();
        assert_eq!(unique.len(), specs.len());
    }

    #[test]
    fn customer_accounts_are_non_negative() {
        for spec in customer_accounts("alice") {
            assert_eq!(spec.policy, AccountPolicy::NonNegative);
        }
    }
}
