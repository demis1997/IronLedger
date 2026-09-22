//! Domain ↔ protobuf mapping.

#![allow(missing_docs)]

use ironledger_domain::{
    Account, AccountId, AccountKind, AccountPolicy, AccountStatus, AssetCode, AtomicAmount,
    CausationId, CorrelationId, DomainError, IdempotencyKey, JournalEntry, PostingSide,
    TransactionId,
};
use ironledger_ledger::{
    AuditRequest, CommandMeta, CompleteWithdrawalCommand, CreateAccountCommand, PostingInput,
    RecordAdminAdjustmentCommand, RecordDepositCommand, RecordTradeSettlementCommand,
    RecordTradingFeeCommand, RejectWithdrawalCommand, RequestWithdrawalCommand,
    SubmitJournalCommand,
};
use tonic::Status;

use crate::proto;

pub fn parse_uuid(text: &str, field: &str) -> Result<uuid::Uuid, Status> {
    uuid::Uuid::parse_str(text).map_err(|_| Status::invalid_argument(format!("invalid {field}")))
}

pub fn parse_account_id(text: &str, field: &str) -> Result<AccountId, Status> {
    Ok(AccountId::from_uuid(parse_uuid(text, field)?))
}

pub fn parse_amount(text: &str, field: &str) -> Result<AtomicAmount, Status> {
    text.parse::<i128>()
        .map(AtomicAmount::from_raw)
        .map_err(|_| Status::invalid_argument(format!("invalid {field}")))
}

pub fn parse_asset(text: &str) -> Result<AssetCode, Status> {
    AssetCode::new(text).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub fn context(meta: &proto::CommandContext) -> Result<CommandMeta, Status> {
    let key = IdempotencyKey::new(&meta.idempotency_key)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let correlation = if meta.correlation_id.is_empty() {
        None
    } else {
        Some(CorrelationId::from_uuid(parse_uuid(
            &meta.correlation_id,
            "correlation_id",
        )?))
    };
    let causation = if meta.causation_id.is_empty() {
        None
    } else {
        Some(CausationId::from_uuid(parse_uuid(
            &meta.causation_id,
            "causation_id",
        )?))
    };
    Ok(CommandMeta::new(key, correlation, causation))
}

pub fn map_account(account: &Account) -> proto::Account {
    proto::Account {
        id: account.id.to_string(),
        name: account.name.clone(),
        kind: map_kind(account.kind) as i32,
        policy: map_policy(account.policy) as i32,
        status: map_status(account.status) as i32,
        created_at: account.created_at.to_rfc3339(),
    }
}

fn map_kind(kind: AccountKind) -> proto::AccountKind {
    match kind {
        AccountKind::CustomerAvailable => proto::AccountKind::CustomerAvailable,
        AccountKind::CustomerLocked => proto::AccountKind::CustomerLocked,
        AccountKind::ExchangeHotWallet => proto::AccountKind::ExchangeHotWallet,
        AccountKind::ExchangeColdWallet => proto::AccountKind::ExchangeColdWallet,
        AccountKind::FeeRevenue => proto::AccountKind::FeeRevenue,
        AccountKind::WithdrawalClearing => proto::AccountKind::WithdrawalClearing,
        AccountKind::DepositClearing => proto::AccountKind::DepositClearing,
        AccountKind::Other => proto::AccountKind::Other,
    }
}

fn map_policy(policy: AccountPolicy) -> proto::AccountPolicy {
    match policy {
        AccountPolicy::NonNegative => proto::AccountPolicy::NonNegative,
        AccountPolicy::AllowNegative => proto::AccountPolicy::AllowNegative,
    }
}

fn map_status(status: AccountStatus) -> proto::AccountStatus {
    match status {
        AccountStatus::Active => proto::AccountStatus::Active,
        AccountStatus::Frozen => proto::AccountStatus::Frozen,
        AccountStatus::Closed => proto::AccountStatus::Closed,
    }
}

pub fn map_entry(entry: &JournalEntry) -> proto::JournalEntry {
    proto::JournalEntry {
        transaction_id: entry.id.to_string(),
        idempotency_key: entry.idempotency_key.to_string(),
        description: entry.description.clone(),
        postings: entry
            .postings
            .iter()
            .map(|posting| proto::Posting {
                account_id: posting.account_id.to_string(),
                asset: posting.asset.to_string(),
                amount_atomic: posting.amount.raw().to_string(),
                side: match posting.side {
                    PostingSide::Debit => proto::PostingSide::Debit as i32,
                    PostingSide::Credit => proto::PostingSide::Credit as i32,
                },
            })
            .collect(),
        status: "posted".into(),
        correlation_id: entry.correlation_id.to_string(),
        causation_id: entry.causation_id.to_string(),
        created_at: entry.created_at.to_rfc3339(),
    }
}

pub fn map_posting_inputs(postings: &[proto::Posting]) -> Result<Vec<PostingInput>, Status> {
    postings
        .iter()
        .map(|posting| {
            Ok(PostingInput {
                account_id: parse_account_id(&posting.account_id, "account_id")?,
                asset: parse_asset(&posting.asset)?,
                amount: parse_amount(&posting.amount_atomic, "amount_atomic")?,
            })
        })
        .collect()
}

pub fn ledger_status(err: ironledger_ledger::LedgerError) -> Status {
    use ironledger_ledger::ErrorCategory;
    let message = err.to_string();
    match err.category() {
        ErrorCategory::InvalidArgument => Status::invalid_argument(message),
        ErrorCategory::NotFound => Status::not_found(message),
        ErrorCategory::FailedPrecondition => Status::failed_precondition(message),
        ErrorCategory::AlreadyExists => Status::already_exists(message),
        ErrorCategory::Unavailable => Status::unavailable(message),
        ErrorCategory::Internal => Status::internal("internal error"),
    }
}

pub fn domain_status(err: DomainError) -> Status {
    ledger_status(err.into())
}

pub fn create_account(req: proto::CreateAccountRequest) -> Result<CreateAccountCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(CreateAccountCommand {
        meta: context(&ctx)?,
        name: req.name,
        kind: match proto::AccountKind::try_from(req.kind) {
            Ok(proto::AccountKind::CustomerAvailable) => AccountKind::CustomerAvailable,
            Ok(proto::AccountKind::CustomerLocked) => AccountKind::CustomerLocked,
            Ok(proto::AccountKind::ExchangeHotWallet) => AccountKind::ExchangeHotWallet,
            Ok(proto::AccountKind::ExchangeColdWallet) => AccountKind::ExchangeColdWallet,
            Ok(proto::AccountKind::FeeRevenue) => AccountKind::FeeRevenue,
            Ok(proto::AccountKind::WithdrawalClearing) => AccountKind::WithdrawalClearing,
            Ok(proto::AccountKind::DepositClearing) => AccountKind::DepositClearing,
            Ok(proto::AccountKind::Other) => AccountKind::Other,
            _ => return Err(Status::invalid_argument("kind required")),
        },
        policy: match proto::AccountPolicy::try_from(req.policy) {
            Ok(proto::AccountPolicy::NonNegative) => AccountPolicy::NonNegative,
            Ok(proto::AccountPolicy::AllowNegative) => AccountPolicy::AllowNegative,
            _ => return Err(Status::invalid_argument("policy required")),
        },
    })
}

pub fn record_deposit(req: proto::RecordDepositRequest) -> Result<RecordDepositCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(RecordDepositCommand {
        meta: context(&ctx)?,
        customer_available: parse_account_id(&req.customer_available_account_id, "customer")?,
        deposit_clearing: parse_account_id(&req.deposit_clearing_account_id, "clearing")?,
        hot_wallet: if req.hot_wallet_account_id.is_empty() {
            None
        } else {
            Some(parse_account_id(&req.hot_wallet_account_id, "hot_wallet")?)
        },
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
        external_reference: if req.external_reference.is_empty() {
            None
        } else {
            Some(req.external_reference)
        },
    })
}

pub fn request_withdrawal(
    req: proto::RequestWithdrawalRequest,
) -> Result<RequestWithdrawalCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(RequestWithdrawalCommand {
        meta: context(&ctx)?,
        customer_available: parse_account_id(&req.customer_available_account_id, "available")?,
        customer_locked: parse_account_id(&req.customer_locked_account_id, "locked")?,
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
        destination_reference: if req.destination_reference.is_empty() {
            None
        } else {
            Some(req.destination_reference)
        },
    })
}

pub fn complete_withdrawal(
    req: proto::CompleteWithdrawalRequest,
) -> Result<CompleteWithdrawalCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(CompleteWithdrawalCommand {
        meta: context(&ctx)?,
        customer_locked: parse_account_id(&req.customer_locked_account_id, "locked")?,
        withdrawal_clearing: parse_account_id(
            &req.withdrawal_clearing_account_id,
            "withdrawal_clearing",
        )?,
        hot_wallet: parse_account_id(&req.hot_wallet_account_id, "hot_wallet")?,
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
        settlement_reference: if req.settlement_reference.is_empty() {
            None
        } else {
            Some(req.settlement_reference)
        },
    })
}

pub fn reject_withdrawal(
    req: proto::RejectWithdrawalRequest,
) -> Result<RejectWithdrawalCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(RejectWithdrawalCommand {
        meta: context(&ctx)?,
        customer_locked: parse_account_id(&req.customer_locked_account_id, "locked")?,
        customer_available: parse_account_id(&req.customer_available_account_id, "available")?,
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
        reason: req.reason,
    })
}

pub fn trade_settlement(
    req: proto::RecordTradeSettlementRequest,
) -> Result<RecordTradeSettlementCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(RecordTradeSettlementCommand {
        meta: context(&ctx)?,
        buyer_available: parse_account_id(&req.buyer_available_account_id, "buyer")?,
        seller_available: parse_account_id(&req.seller_available_account_id, "seller")?,
        base_asset: parse_asset(&req.base_asset)?,
        base_amount: parse_amount(&req.base_amount_atomic, "base_amount")?,
        quote_asset: parse_asset(&req.quote_asset)?,
        quote_amount: parse_amount(&req.quote_amount_atomic, "quote_amount")?,
        trade_reference: if req.trade_reference.is_empty() {
            None
        } else {
            Some(req.trade_reference)
        },
    })
}

pub fn trading_fee(req: proto::RecordTradingFeeRequest) -> Result<RecordTradingFeeCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(RecordTradingFeeCommand {
        meta: context(&ctx)?,
        customer_available: parse_account_id(&req.customer_available_account_id, "customer")?,
        fee_revenue: parse_account_id(&req.fee_revenue_account_id, "fee_revenue")?,
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
    })
}

pub fn admin_adjustment(
    req: proto::RecordAdminAdjustmentRequest,
) -> Result<RecordAdminAdjustmentCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    let audit = req
        .audit
        .ok_or_else(|| Status::invalid_argument("audit required"))?;
    Ok(RecordAdminAdjustmentCommand {
        meta: context(&ctx)?,
        from: parse_account_id(&req.from_account_id, "from")?,
        to: parse_account_id(&req.to_account_id, "to")?,
        asset: parse_asset(&req.asset)?,
        amount: parse_amount(&req.amount_atomic, "amount")?,
        audit: AuditRequest {
            actor: audit.actor,
            reason: audit.reason,
            ticket_id: if audit.ticket_id.is_empty() {
                None
            } else {
                Some(audit.ticket_id)
            },
        },
    })
}

pub fn submit_journal(
    req: proto::SubmitJournalEntryRequest,
) -> Result<SubmitJournalCommand, Status> {
    let ctx = req
        .context
        .ok_or_else(|| Status::invalid_argument("context required"))?;
    Ok(SubmitJournalCommand {
        meta: context(&ctx)?,
        description: req.description,
        postings: map_posting_inputs(&req.postings)?,
    })
}

pub fn command_result(
    transaction_id: TransactionId,
    replayed: bool,
    posted_at: chrono::DateTime<chrono::Utc>,
) -> proto::CommandResult {
    proto::CommandResult {
        transaction_id: transaction_id.to_string(),
        replayed,
        posted_at: posted_at.to_rfc3339(),
    }
}
