//! Command inputs accepted by [`LedgerService`](crate::service::LedgerService).
//!
//! Commands are plain data: they carry no timestamps or generated identifiers,
//! so fingerprinting them is deterministic and a retry of the same business
//! request always produces the same [`RequestHash`].

use ironledger_domain::{
    AccountId, AccountKind, AccountPolicy, AssetCode, AtomicAmount, AuditMetadata, CausationId,
    CorrelationId, DomainError, IdempotencyKey,
};
use serde::{Deserialize, Serialize};

use crate::error::LedgerError;
use crate::idempotency::RequestHash;

/// Per-attempt metadata: the idempotency key plus tracing correlation.
///
/// Correlation and causation ids are intentionally excluded from the request
/// fingerprint: a legitimate retry carries fresh tracing ids but the same
/// business payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMeta {
    /// Client-supplied idempotency key, unique per command scope.
    pub idempotency_key: IdempotencyKey,
    /// Workflow correlation id.
    pub correlation_id: CorrelationId,
    /// Immediate cause of this command.
    pub causation_id: CausationId,
}

impl CommandMeta {
    /// Build metadata, generating tracing ids when the caller has none.
    pub fn new(
        idempotency_key: IdempotencyKey,
        correlation_id: Option<CorrelationId>,
        causation_id: Option<CausationId>,
    ) -> Self {
        Self {
            idempotency_key,
            correlation_id: correlation_id.unwrap_or_default(),
            causation_id: causation_id.unwrap_or_default(),
        }
    }

    /// Build metadata from a raw key string.
    pub fn from_key(key: &str) -> Result<Self, DomainError> {
        Ok(Self::new(IdempotencyKey::new(key)?, None, None))
    }
}

/// Behaviour shared by every mutating command.
pub trait Command: Serialize {
    /// Stable scope name; also the idempotency namespace.
    fn scope(&self) -> &'static str;

    /// Per-attempt metadata.
    fn meta(&self) -> &CommandMeta;

    /// Fingerprint of the business payload within this scope.
    fn fingerprint(&self) -> Result<RequestHash, LedgerError>
    where
        Self: Sized,
    {
        RequestHash::compute(self.scope(), self)
    }
}

macro_rules! impl_command {
    ($name:ident, $scope:literal) => {
        impl Command for $name {
            fn scope(&self) -> &'static str {
                $scope
            }

            fn meta(&self) -> &CommandMeta {
                &self.meta
            }
        }
    };
}

/// Deterministic audit input; the authorization timestamp is assigned by the
/// server when the command is accepted, so it stays out of the fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRequest {
    /// Operator or service principal (never a secret).
    pub actor: String,
    /// Mandatory human-readable reason.
    pub reason: String,
    /// Optional change-management ticket.
    pub ticket_id: Option<String>,
}

impl AuditRequest {
    /// Validate and stamp the audit trail.
    pub fn into_metadata(self) -> Result<AuditMetadata, DomainError> {
        AuditMetadata::new(self.actor, self.reason, self.ticket_id)
    }
}

/// One leg of a raw journal submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostingInput {
    /// Target account.
    pub account_id: AccountId,
    /// Asset moved.
    pub asset: AssetCode,
    /// Signed atomic delta; positive debits, negative credits.
    pub amount: AtomicAmount,
}

/// Open a new ledger account.
#[derive(Debug, Clone, Serialize)]
pub struct CreateAccountCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Unique account name.
    pub name: String,
    /// Classification.
    pub kind: AccountKind,
    /// Negative-balance policy.
    pub policy: AccountPolicy,
}
impl_command!(CreateAccountCommand, "create_account");

/// Post a caller-constructed balanced journal entry.
#[derive(Debug, Clone, Serialize)]
pub struct SubmitJournalCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Flow description.
    pub description: String,
    /// Legs; must balance per asset.
    pub postings: Vec<PostingInput>,
}
impl_command!(SubmitJournalCommand, "submit_journal_entry");

/// Record a confirmed on-chain or fiat deposit.
#[derive(Debug, Clone, Serialize)]
pub struct RecordDepositCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Customer available account (credited with spendable funds).
    pub customer_available: AccountId,
    /// Incoming-funds suspense account.
    pub deposit_clearing: AccountId,
    /// Optional custody mirror.
    pub hot_wallet: Option<AccountId>,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
    /// Opaque external reference (tx hash, bank reference).
    pub external_reference: Option<String>,
}
impl_command!(RecordDepositCommand, "record_deposit");

/// Lock customer funds for a requested withdrawal.
#[derive(Debug, Clone, Serialize)]
pub struct RequestWithdrawalCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Source of spendable funds.
    pub customer_available: AccountId,
    /// Locked-funds account.
    pub customer_locked: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
    /// Payout destination reference.
    pub destination_reference: Option<String>,
}
impl_command!(RequestWithdrawalCommand, "request_withdrawal");

/// Settle a previously requested withdrawal.
#[derive(Debug, Clone, Serialize)]
pub struct CompleteWithdrawalCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Locked-funds account.
    pub customer_locked: AccountId,
    /// Payout suspense account.
    pub withdrawal_clearing: AccountId,
    /// Custody account funds leave from.
    pub hot_wallet: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
    /// Settlement reference (tx hash, payment id).
    pub settlement_reference: Option<String>,
}
impl_command!(CompleteWithdrawalCommand, "complete_withdrawal");

/// Return locked funds after a rejected withdrawal.
#[derive(Debug, Clone, Serialize)]
pub struct RejectWithdrawalCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Locked-funds account.
    pub customer_locked: AccountId,
    /// Destination for the released funds.
    pub customer_available: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
    /// Rejection reason (recorded in the entry description).
    pub reason: String,
}
impl_command!(RejectWithdrawalCommand, "reject_withdrawal");

/// Settle a matched trade between two customers.
#[derive(Debug, Clone, Serialize)]
pub struct RecordTradeSettlementCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Buyer of the base asset.
    pub buyer_available: AccountId,
    /// Seller of the base asset.
    pub seller_available: AccountId,
    /// Base asset.
    pub base_asset: AssetCode,
    /// Base amount.
    pub base_amount: AtomicAmount,
    /// Quote asset.
    pub quote_asset: AssetCode,
    /// Quote amount.
    pub quote_amount: AtomicAmount,
    /// Matching-engine trade reference.
    pub trade_reference: Option<String>,
}
impl_command!(RecordTradeSettlementCommand, "record_trade_settlement");

/// Charge a trading fee.
#[derive(Debug, Clone, Serialize)]
pub struct RecordTradingFeeCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Paying customer.
    pub customer_available: AccountId,
    /// Fee revenue account.
    pub fee_revenue: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
}
impl_command!(RecordTradingFeeCommand, "record_trading_fee");

/// Move funds administratively, with a mandatory audit trail.
#[derive(Debug, Clone, Serialize)]
pub struct RecordAdminAdjustmentCommand {
    /// Metadata.
    #[serde(skip_serializing)]
    pub meta: CommandMeta,
    /// Source account.
    pub from: AccountId,
    /// Destination account.
    pub to: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Positive atomic amount.
    pub amount: AtomicAmount,
    /// Audit trail; required and recorded on the emitted event.
    pub audit: AuditRequest,
}
impl_command!(RecordAdminAdjustmentCommand, "record_admin_adjustment");

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(key: &str) -> CommandMeta {
        CommandMeta::from_key(key).unwrap()
    }

    fn deposit(amount: i128, key: &str) -> RecordDepositCommand {
        RecordDepositCommand {
            meta: meta(key),
            customer_available: AccountId::from_uuid(uuid::Uuid::nil()),
            deposit_clearing: AccountId::from_uuid(uuid::Uuid::max()),
            hot_wallet: None,
            asset: AssetCode::btc(),
            amount: AtomicAmount::from_raw(amount),
            external_reference: Some("tx-1".into()),
        }
    }

    #[test]
    fn fingerprint_ignores_tracing_metadata() {
        // Same business payload, different correlation/causation ids and key.
        let left = deposit(100, "key-a").fingerprint().unwrap();
        let right = deposit(100, "key-b").fingerprint().unwrap();
        assert_eq!(left, right);
    }

    #[test]
    fn fingerprint_tracks_the_amount() {
        let left = deposit(100, "key-a").fingerprint().unwrap();
        let right = deposit(101, "key-a").fingerprint().unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn audit_request_requires_a_reason() {
        let request = AuditRequest {
            actor: "ops@example.test".into(),
            reason: "   ".into(),
            ticket_id: None,
        };
        assert!(matches!(
            request.into_metadata(),
            Err(DomainError::MissingAdjustmentReason)
        ));
    }
}
