//! Example transaction flows that produce balanced journal entries.
//!
//! These builders encode the chart-of-accounts semantics for the demo
//! exchange ledger. All flows validate balance before returning.
//!
//! # Sign convention
//!
//! Every posting carries a signed delta applied directly to the account
//! balance: a debit is positive, a credit is negative. Because each entry sums
//! to zero per asset, exactly one side of the exchange must be credit-normal.
//!
//! - **Debit-normal (balances ≥ 0, [`AccountPolicy::NonNegative`])**: customer
//!   available, customer locked, fee revenue. These are the spendable balances
//!   customers and finance teams read.
//! - **Credit-normal (balances ≤ 0, [`AccountPolicy::AllowNegative`])**:
//!   exchange hot/cold wallet, deposit clearing, withdrawal clearing. Their
//!   absolute value is the obligation or in-flight amount they represent.
//!
//! The system-wide invariant is therefore that the sum of *all* balances for a
//! given asset is exactly zero, which is what the reconciler asserts.
//!
//! [`AccountPolicy::NonNegative`]: crate::account::AccountPolicy::NonNegative
//! [`AccountPolicy::AllowNegative`]: crate::account::AccountPolicy::AllowNegative

use crate::amount::AtomicAmount;
use crate::asset::AssetCode;
use crate::audit::AuditMetadata;
use crate::error::DomainError;
use crate::ids::{AccountId, CausationId, CorrelationId, IdempotencyKey, TransactionId};
use crate::journal::{JournalEntry, Posting};

fn transfer_pair(
    from: AccountId,
    to: AccountId,
    asset: AssetCode,
    amount: AtomicAmount,
) -> Result<Vec<Posting>, DomainError> {
    let amount = amount.require_positive()?;
    Ok(vec![
        Posting::credit(from, asset.clone(), amount)?,
        Posting::debit(to, asset, amount)?,
    ])
}

/// Confirmed deposit: clearing → customer available, optionally mirrored into
/// custody.
///
/// Accounting view (simplified):
/// - Debit customer available (customer claim increases)
/// - Credit deposit clearing (incoming-funds suspense)
///
/// When `hot_wallet` is provided a second balanced pair moves the funds out of
/// suspense and into custody (debit deposit clearing, credit hot wallet), so
/// the clearing account nets to zero for a fully processed deposit and the
/// credit-normal hot wallet carries the custody obligation.
#[derive(Debug, Clone)]
pub struct ConfirmedDeposit {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Customer available account.
    pub customer_available: AccountId,
    /// Deposit clearing account.
    pub deposit_clearing: AccountId,
    /// Optional hot wallet account for custody mirror.
    pub hot_wallet: Option<AccountId>,
    /// Asset.
    pub asset: AssetCode,
    /// Positive amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl ConfirmedDeposit {
    /// Build balanced journal entry.
    ///
    /// Without a hot wallet the funds move deposit clearing → customer
    /// available. With a hot wallet the entry additionally moves the deposit
    /// clearing balance into custody, leaving clearing flat.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let amount = self.amount.require_positive()?;
        let mut postings = transfer_pair(
            self.deposit_clearing,
            self.customer_available,
            self.asset.clone(),
            amount,
        )?;
        if let Some(hot) = self.hot_wallet {
            postings.push(Posting::debit(
                self.deposit_clearing,
                self.asset.clone(),
                amount,
            )?);
            postings.push(Posting::credit(hot, self.asset, amount)?);
        }
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "confirmed_deposit",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Internal transfer between two customer available accounts (same asset).
#[derive(Debug, Clone)]
pub struct InternalTransfer {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Source.
    pub from: AccountId,
    /// Destination.
    pub to: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl InternalTransfer {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let postings = transfer_pair(self.from, self.to, self.asset, self.amount)?;
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "internal_transfer",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Trade settlement: customer A gives `base_amount` of `base_asset` to B,
/// customer B gives `quote_amount` of `quote_asset` to A.
#[derive(Debug, Clone)]
pub struct TradeSettlement {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Buyer of base (receives base, pays quote).
    pub buyer_available: AccountId,
    /// Seller of base (receives quote, delivers base).
    pub seller_available: AccountId,
    /// Base asset (e.g. BTC).
    pub base_asset: AssetCode,
    /// Base amount.
    pub base_amount: AtomicAmount,
    /// Quote asset (e.g. USD).
    pub quote_asset: AssetCode,
    /// Quote amount.
    pub quote_amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl TradeSettlement {
    /// Build multi-asset balanced entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let base = self.base_amount.require_positive()?;
        let quote = self.quote_amount.require_positive()?;
        let postings = vec![
            // Base: seller → buyer
            Posting::credit(self.seller_available, self.base_asset.clone(), base)?,
            Posting::debit(self.buyer_available, self.base_asset, base)?,
            // Quote: buyer → seller
            Posting::credit(self.buyer_available, self.quote_asset.clone(), quote)?,
            Posting::debit(self.seller_available, self.quote_asset, quote)?,
        ];
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "trade_settlement",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Trading fee from customer available to fee revenue.
#[derive(Debug, Clone)]
pub struct TradingFee {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Customer paying the fee.
    pub customer_available: AccountId,
    /// Fee revenue account.
    pub fee_revenue: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl TradingFee {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let postings = transfer_pair(
            self.customer_available,
            self.fee_revenue,
            self.asset,
            self.amount,
        )?;
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "trading_fee",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Withdrawal request: available → locked.
#[derive(Debug, Clone)]
pub struct WithdrawalRequest {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Available.
    pub customer_available: AccountId,
    /// Locked.
    pub customer_locked: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl WithdrawalRequest {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let postings = transfer_pair(
            self.customer_available,
            self.customer_locked,
            self.asset,
            self.amount,
        )?;
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "withdrawal_request",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Withdrawal completion: locked → withdrawal clearing → custody release.
///
/// Two balanced pairs:
/// 1. Credit customer locked / debit withdrawal clearing — the customer claim is
///    extinguished and becomes a payable in suspense.
/// 2. Credit withdrawal clearing / debit hot wallet — the payable is settled by
///    sending funds out of custody, moving the credit-normal hot wallet balance
///    back toward zero.
///
/// A completed withdrawal therefore leaves the clearing account flat; a residual
/// clearing balance means a payout is stuck mid-flight, which is exactly what
/// reconciliation surfaces.
#[derive(Debug, Clone)]
pub struct WithdrawalCompletion {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Locked funds.
    pub customer_locked: AccountId,
    /// Withdrawal clearing.
    pub withdrawal_clearing: AccountId,
    /// Hot wallet.
    pub hot_wallet: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl WithdrawalCompletion {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let amount = self.amount.require_positive()?;
        let postings = vec![
            Posting::credit(self.customer_locked, self.asset.clone(), amount)?,
            Posting::debit(self.withdrawal_clearing, self.asset.clone(), amount)?,
            Posting::credit(self.withdrawal_clearing, self.asset.clone(), amount)?,
            Posting::debit(self.hot_wallet, self.asset, amount)?,
        ];
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "withdrawal_completion",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Withdrawal rejection: locked → available.
#[derive(Debug, Clone)]
pub struct WithdrawalRejection {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// Locked.
    pub customer_locked: AccountId,
    /// Available.
    pub customer_available: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl WithdrawalRejection {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let postings = transfer_pair(
            self.customer_locked,
            self.customer_available,
            self.asset,
            self.amount,
        )?;
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            "withdrawal_rejection",
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

/// Administrative adjustment between two accounts with mandatory audit metadata.
#[derive(Debug, Clone)]
pub struct AdminAdjustment {
    /// Idempotency key.
    pub idempotency_key: IdempotencyKey,
    /// From.
    pub from: AccountId,
    /// To.
    pub to: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Amount.
    pub amount: AtomicAmount,
    /// Audit.
    pub audit: AuditMetadata,
    /// Correlation.
    pub correlation_id: CorrelationId,
    /// Causation.
    pub causation_id: CausationId,
}

impl AdminAdjustment {
    /// Build entry.
    pub fn into_entry(self) -> Result<JournalEntry, DomainError> {
        let postings = transfer_pair(self.from, self.to, self.asset, self.amount)?;
        let description = format!("admin_adjustment: {}", self.audit.reason);
        JournalEntry::new(
            TransactionId::new(),
            self.idempotency_key,
            description,
            postings,
            self.correlation_id,
            self.causation_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::validate_balanced;

    fn ids() -> (AccountId, AccountId, AccountId, AccountId) {
        (
            AccountId::new(),
            AccountId::new(),
            AccountId::new(),
            AccountId::new(),
        )
    }

    /// Net signed delta applied to one account by an entry.
    fn net(entry: &JournalEntry, account: AccountId) -> i128 {
        entry
            .postings
            .iter()
            .filter(|p| p.account_id == account)
            .map(|p| p.amount.raw())
            .sum()
    }

    #[test]
    fn deposit_credits_customer_and_leaves_clearing_flat() {
        let (cust, clearing, hot, _) = ids();
        let entry = ConfirmedDeposit {
            idempotency_key: IdempotencyKey::new("d2").unwrap(),
            customer_available: cust,
            deposit_clearing: clearing,
            hot_wallet: Some(hot),
            asset: AssetCode::btc(),
            amount: AtomicAmount::from_raw(100),
            correlation_id: CorrelationId::new(),
            causation_id: CausationId::new(),
        }
        .into_entry()
        .unwrap();

        assert_eq!(net(&entry, cust), 100, "customer available must increase");
        assert_eq!(net(&entry, clearing), 0, "clearing must net to zero");
        assert_eq!(
            net(&entry, hot),
            -100,
            "custody obligation is credit-normal"
        );
    }

    #[test]
    fn withdrawal_completion_releases_locked_and_custody() {
        let (locked, clearing, hot, _) = ids();
        let entry = WithdrawalCompletion {
            idempotency_key: IdempotencyKey::new("w2").unwrap(),
            customer_locked: locked,
            withdrawal_clearing: clearing,
            hot_wallet: hot,
            asset: AssetCode::usd(),
            amount: AtomicAmount::from_raw(25),
            correlation_id: CorrelationId::new(),
            causation_id: CausationId::new(),
        }
        .into_entry()
        .unwrap();

        assert_eq!(net(&entry, locked), -25, "locked funds are released");
        assert_eq!(net(&entry, clearing), 0, "payout passes through suspense");
        assert_eq!(
            net(&entry, hot),
            25,
            "custody obligation shrinks toward zero"
        );
    }

    #[test]
    fn deposit_balances() {
        let (cust, clearing, hot, _) = ids();
        let entry = ConfirmedDeposit {
            idempotency_key: IdempotencyKey::new("d1").unwrap(),
            customer_available: cust,
            deposit_clearing: clearing,
            hot_wallet: Some(hot),
            asset: AssetCode::btc(),
            amount: AtomicAmount::from_raw(100),
            correlation_id: CorrelationId::new(),
            causation_id: CausationId::new(),
        }
        .into_entry()
        .unwrap();
        validate_balanced(&entry.postings).unwrap();
    }

    #[test]
    fn trade_settlement_balances() {
        let (buyer, seller, _, _) = ids();
        let entry = TradeSettlement {
            idempotency_key: IdempotencyKey::new("t1").unwrap(),
            buyer_available: buyer,
            seller_available: seller,
            base_asset: AssetCode::btc(),
            base_amount: AtomicAmount::from_raw(1),
            quote_asset: AssetCode::usd(),
            quote_amount: AtomicAmount::from_raw(50_000_000),
            correlation_id: CorrelationId::new(),
            causation_id: CausationId::new(),
        }
        .into_entry()
        .unwrap();
        validate_balanced(&entry.postings).unwrap();
    }

    #[test]
    fn withdrawal_completion_balances() {
        let (locked, clearing, hot, _) = ids();
        let entry = WithdrawalCompletion {
            idempotency_key: IdempotencyKey::new("w1").unwrap(),
            customer_locked: locked,
            withdrawal_clearing: clearing,
            hot_wallet: hot,
            asset: AssetCode::usd(),
            amount: AtomicAmount::from_raw(25),
            correlation_id: CorrelationId::new(),
            causation_id: CausationId::new(),
        }
        .into_entry()
        .unwrap();
        validate_balanced(&entry.postings).unwrap();
    }
}
