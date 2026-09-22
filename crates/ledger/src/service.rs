//! Command handlers.

use ironledger_domain::{
    Account, AccountId, AccountPolicy, AdminAdjustment, AssetCode, AtomicAmount, ConfirmedDeposit,
    DomainError, JournalEntry, LedgerEvent, LedgerEventPayload, Posting, TradeSettlement,
    TradingFee, TransactionId, WithdrawalCompletion, WithdrawalRejection, WithdrawalRequest,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

use crate::command::{
    Command, CompleteWithdrawalCommand, CreateAccountCommand, RecordAdminAdjustmentCommand,
    RecordDepositCommand, RecordTradeSettlementCommand, RecordTradingFeeCommand,
    RejectWithdrawalCommand, RequestWithdrawalCommand, SubmitJournalCommand,
};
use crate::error::LedgerError;
use crate::idempotency::IdempotencyRecord;
use crate::port::{
    AccountCommit, AccountRepo, BalanceRecord, BalanceRepo, CommitOutcome, IdempotencyRepo,
    JournalCommit, LedgerRepo, OutboxMessage, OutboxRepo, OutboxStats, Page,
};

/// Longest accepted free-text reference attached to an entry description.
const MAX_REFERENCE_LEN: usize = 256;
/// Longest accepted account name.
const MAX_ACCOUNT_NAME_LEN: usize = 128;
/// Longest accepted journal description supplied by a caller.
const MAX_DESCRIPTION_LEN: usize = 512;

/// A handler result plus whether it came from the idempotency store.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Outcome<T> {
    /// The response value.
    pub value: T,
    /// True when this response replays an earlier identical request.
    pub replayed: bool,
}

impl<T> Outcome<T> {
    /// A freshly committed result.
    pub const fn fresh(value: T) -> Self {
        Self {
            value,
            replayed: false,
        }
    }

    /// A replayed result.
    pub const fn replayed(value: T) -> Self {
        Self {
            value,
            replayed: true,
        }
    }
}

/// Response for [`LedgerService::create_account`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountCreated {
    /// The created account.
    pub account: Account,
}

/// Response for every journal-posting command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryPosted {
    /// Identifier of the posted entry.
    pub transaction_id: TransactionId,
    /// Entry description as recorded.
    pub description: String,
    /// Posting timestamp.
    pub posted_at: chrono::DateTime<chrono::Utc>,
}

/// Ledger command handlers wired to their ports.
#[derive(Clone)]
pub struct LedgerService {
    accounts: Arc<dyn AccountRepo>,
    ledger: Arc<dyn LedgerRepo>,
    idempotency: Arc<dyn IdempotencyRepo>,
    outbox: Arc<dyn OutboxRepo>,
    balances: Arc<dyn BalanceRepo>,
}

impl LedgerService {
    /// Wire the handlers to their adapters.
    pub fn new(
        accounts: Arc<dyn AccountRepo>,
        ledger: Arc<dyn LedgerRepo>,
        idempotency: Arc<dyn IdempotencyRepo>,
        outbox: Arc<dyn OutboxRepo>,
        balances: Arc<dyn BalanceRepo>,
    ) -> Self {
        Self {
            accounts,
            ledger,
            idempotency,
            outbox,
            balances,
        }
    }

    // -- commands ----------------------------------------------------------

    /// Open a new account.
    pub async fn create_account(
        &self,
        command: CreateAccountCommand,
    ) -> Result<Outcome<AccountCreated>, LedgerError> {
        let scope = command.scope();
        let hash = command.fingerprint()?;
        let key = command.meta.idempotency_key.clone();

        if let Some(record) = self.idempotency.find(scope, &key).await? {
            return Ok(Outcome::replayed(record.replay::<AccountCreated>(&hash)?));
        }

        let name = command.name.trim();
        if name.is_empty() || name.len() > MAX_ACCOUNT_NAME_LEN {
            return Err(LedgerError::Validation(format!(
                "account name must be 1..={MAX_ACCOUNT_NAME_LEN} characters"
            )));
        }
        if self.accounts.find_by_name(name).await?.is_some() {
            return Err(LedgerError::Conflict(format!(
                "account name '{name}' is already taken"
            )));
        }

        let account = Account::new(AccountId::new(), name, command.kind, command.policy);
        let response = AccountCreated {
            account: account.clone(),
        };
        let event = LedgerEvent::wrap(
            account.id.to_string(),
            None,
            command.meta.correlation_id,
            command.meta.causation_id,
            "",
            LedgerEventPayload::AccountCreated {
                account_id: account.id,
                name: account.name.clone(),
            },
        );
        let idempotency = IdempotencyRecord::new(scope, key.clone(), hash.clone(), &response)?;
        let commit = AccountCommit {
            account,
            events: vec![event],
            idempotency,
        };

        match self.accounts.create(&commit).await? {
            CommitOutcome::Committed => Ok(Outcome::fresh(response)),
            CommitOutcome::DuplicateIdempotencyKey => {
                let record = self.require_record(scope, &key).await?;
                Ok(Outcome::replayed(record.replay::<AccountCreated>(&hash)?))
            }
        }
    }

    /// Post a caller-constructed journal entry.
    pub async fn submit_journal_entry(
        &self,
        command: SubmitJournalCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let description = validated_description(&cmd.description)?;
            let postings = cmd
                .postings
                .iter()
                .map(|input| {
                    Posting::new(input.account_id, input.asset.clone(), input.amount)
                        .map_err(LedgerError::from)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry = JournalEntry::new(
                TransactionId::new(),
                cmd.meta.idempotency_key.clone(),
                description,
                postings,
                cmd.meta.correlation_id,
                cmd.meta.causation_id,
            )?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Record a confirmed deposit.
    pub async fn record_deposit(
        &self,
        command: RecordDepositCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let mut entry = ConfirmedDeposit {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                customer_available: cmd.customer_available,
                deposit_clearing: cmd.deposit_clearing,
                hot_wallet: cmd.hot_wallet,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            annotate(&mut entry, cmd.external_reference.as_deref())?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Lock funds for a withdrawal request.
    pub async fn request_withdrawal(
        &self,
        command: RequestWithdrawalCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let mut entry = WithdrawalRequest {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                customer_available: cmd.customer_available,
                customer_locked: cmd.customer_locked,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            annotate(&mut entry, cmd.destination_reference.as_deref())?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Settle a withdrawal.
    pub async fn complete_withdrawal(
        &self,
        command: CompleteWithdrawalCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let mut entry = WithdrawalCompletion {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                customer_locked: cmd.customer_locked,
                withdrawal_clearing: cmd.withdrawal_clearing,
                hot_wallet: cmd.hot_wallet,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            annotate(&mut entry, cmd.settlement_reference.as_deref())?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Release locked funds after a rejected withdrawal.
    pub async fn reject_withdrawal(
        &self,
        command: RejectWithdrawalCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let reason = cmd.reason.trim();
            if reason.is_empty() {
                return Err(LedgerError::Validation(
                    "withdrawal rejection requires a reason".into(),
                ));
            }
            let mut entry = WithdrawalRejection {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                customer_locked: cmd.customer_locked,
                customer_available: cmd.customer_available,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            annotate(&mut entry, Some(reason))?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Settle a matched trade.
    pub async fn record_trade_settlement(
        &self,
        command: RecordTradeSettlementCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            if cmd.base_asset == cmd.quote_asset {
                return Err(LedgerError::Validation(
                    "trade settlement requires two distinct assets".into(),
                ));
            }
            if cmd.buyer_available == cmd.seller_available {
                return Err(LedgerError::Validation(
                    "trade settlement requires two distinct accounts".into(),
                ));
            }
            let mut entry = TradeSettlement {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                buyer_available: cmd.buyer_available,
                seller_available: cmd.seller_available,
                base_asset: cmd.base_asset.clone(),
                base_amount: cmd.base_amount,
                quote_asset: cmd.quote_asset.clone(),
                quote_amount: cmd.quote_amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            annotate(&mut entry, cmd.trade_reference.as_deref())?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Charge a trading fee.
    pub async fn record_trading_fee(
        &self,
        command: RecordTradingFeeCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            let entry = TradingFee {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                customer_available: cmd.customer_available,
                fee_revenue: cmd.fee_revenue,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            Ok((entry, Vec::new()))
        })
        .await
    }

    /// Record an administrative adjustment.
    ///
    /// Emits an additional `admin_adjusted` event carrying the audit trail so
    /// that privileged movements are independently auditable from the event
    /// stream.
    pub async fn record_admin_adjustment(
        &self,
        command: RecordAdminAdjustmentCommand,
    ) -> Result<Outcome<EntryPosted>, LedgerError> {
        self.post_entry(&command, |cmd| {
            if cmd.from == cmd.to {
                return Err(LedgerError::Validation(
                    "adjustment requires two distinct accounts".into(),
                ));
            }
            let audit = cmd.audit.clone().into_metadata()?;
            let entry = AdminAdjustment {
                idempotency_key: cmd.meta.idempotency_key.clone(),
                from: cmd.from,
                to: cmd.to,
                asset: cmd.asset.clone(),
                amount: cmd.amount,
                audit: audit.clone(),
                correlation_id: cmd.meta.correlation_id,
                causation_id: cmd.meta.causation_id,
            }
            .into_entry()?;
            let extra = vec![LedgerEventPayload::AdminAdjusted {
                transaction_id: entry.id,
                audit,
            }];
            Ok((entry, extra))
        })
        .await
    }

    // -- queries -----------------------------------------------------------

    /// Fetch an entry or fail with [`LedgerError::NotFound`].
    pub async fn get_transaction(&self, id: TransactionId) -> Result<JournalEntry, LedgerError> {
        self.ledger
            .find_entry(id)
            .await?
            .ok_or_else(|| LedgerError::NotFound {
                entity: "transaction",
                id: id.to_string(),
            })
    }

    /// Balances for an account, optionally narrowed to one asset.
    pub async fn get_balances(
        &self,
        account_id: AccountId,
        asset: Option<&AssetCode>,
    ) -> Result<Vec<BalanceRecord>, LedgerError> {
        if self.accounts.find(account_id).await?.is_none() {
            return Err(LedgerError::NotFound {
                entity: "account",
                id: account_id.to_string(),
            });
        }
        match asset {
            Some(asset) => Ok(self
                .balances
                .get(account_id, asset)
                .await?
                .into_iter()
                .collect()),
            None => self.balances.list_for_account(account_id).await,
        }
    }

    /// Entries touching an account, newest first.
    pub async fn list_account_entries(
        &self,
        account_id: AccountId,
        page: Page,
    ) -> Result<Vec<JournalEntry>, LedgerError> {
        self.ledger.list_account_entries(account_id, page).await
    }

    /// Look up an account by id.
    pub async fn find_account(&self, id: AccountId) -> Result<Option<Account>, LedgerError> {
        self.accounts.find(id).await
    }

    /// Look up an account by unique name.
    pub async fn find_account_by_name(&self, name: &str) -> Result<Option<Account>, LedgerError> {
        self.accounts.find_by_name(name).await
    }

    /// List accounts.
    pub async fn list_accounts(&self, page: Page) -> Result<Vec<Account>, LedgerError> {
        self.accounts.list(page).await
    }

    /// Outbox counters for the admin API.
    pub async fn outbox_stats(&self) -> Result<OutboxStats, LedgerError> {
        self.outbox.stats().await
    }

    /// Pending outbox backlog for the admin API.
    pub async fn pending_outbox(&self, page: Page) -> Result<Vec<OutboxMessage>, LedgerError> {
        self.outbox.pending(page).await
    }

    // -- internals ---------------------------------------------------------

    async fn post_entry<C, F>(
        &self,
        command: &C,
        build: F,
    ) -> Result<Outcome<EntryPosted>, LedgerError>
    where
        C: Command,
        F: FnOnce(&C) -> Result<(JournalEntry, Vec<LedgerEventPayload>), LedgerError>,
    {
        let scope = command.scope();
        let key = command.meta().idempotency_key.clone();
        let hash = command.fingerprint()?;

        if let Some(record) = self.idempotency.find(scope, &key).await? {
            return Ok(Outcome::replayed(record.replay::<EntryPosted>(&hash)?));
        }

        let (entry, extra_payloads) = build(command)?;
        self.guard_postings(&entry).await?;

        let response = EntryPosted {
            transaction_id: entry.id,
            description: entry.description.clone(),
            posted_at: entry.created_at,
        };

        let aggregate_id = entry.id.to_string();
        let mut events = Vec::with_capacity(1 + extra_payloads.len());
        events.push(LedgerEvent::wrap(
            aggregate_id.clone(),
            None,
            entry.correlation_id,
            entry.causation_id,
            "",
            LedgerEventPayload::JournalPosted {
                entry: entry.clone(),
            },
        ));
        for payload in extra_payloads {
            events.push(LedgerEvent::wrap(
                aggregate_id.clone(),
                None,
                entry.correlation_id,
                entry.causation_id,
                "",
                payload,
            ));
        }

        let idempotency = IdempotencyRecord::new(scope, key.clone(), hash.clone(), &response)?;
        let commit = JournalCommit {
            entry,
            events,
            idempotency,
        };

        match self.ledger.commit(&commit).await? {
            CommitOutcome::Committed => Ok(Outcome::fresh(response)),
            CommitOutcome::DuplicateIdempotencyKey => {
                let record = self.require_record(scope, &key).await?;
                Ok(Outcome::replayed(record.replay::<EntryPosted>(&hash)?))
            }
        }
    }

    /// Accounts must exist, be active, and respect their balance policy.
    ///
    /// This runs before the commit to return precise errors; the storage
    /// adapter repeats the check inside the transaction, where it is safe
    /// against concurrent writers.
    async fn guard_postings(&self, entry: &JournalEntry) -> Result<(), LedgerError> {
        let mut deltas: BTreeMap<(Uuid, AssetCode), AtomicAmount> = BTreeMap::new();
        for posting in &entry.postings {
            let slot = deltas
                .entry((posting.account_id.into_uuid(), posting.asset.clone()))
                .or_insert(AtomicAmount::ZERO);
            *slot = slot.checked_add(posting.amount)?;
        }

        let ids: Vec<AccountId> = {
            let mut seen: Vec<Uuid> = deltas.keys().map(|(id, _)| *id).collect();
            seen.dedup();
            seen.into_iter().map(AccountId::from_uuid).collect()
        };

        let loaded = self.accounts.load_many(&ids).await?;
        let accounts: HashMap<Uuid, Account> = loaded
            .into_iter()
            .map(|account| (account.id.into_uuid(), account))
            .collect();

        for id in &ids {
            let account = accounts
                .get(id.as_uuid())
                .ok_or_else(|| LedgerError::NotFound {
                    entity: "account",
                    id: id.to_string(),
                })?;
            if !account.is_active() {
                return Err(DomainError::AccountNotActive {
                    account_id: id.to_string(),
                    status: account.status.as_str().to_owned(),
                }
                .into());
            }
        }

        for ((account_uuid, asset), delta) in deltas {
            if !delta.is_negative() {
                continue;
            }
            let account = &accounts[&account_uuid];
            if account.policy == AccountPolicy::AllowNegative {
                continue;
            }
            let account_id = AccountId::from_uuid(account_uuid);
            let balance = self
                .balances
                .get(account_id, &asset)
                .await?
                .map_or(AtomicAmount::ZERO, |record| record.amount);
            if balance.checked_add(delta)?.is_negative() {
                return Err(DomainError::InsufficientBalance {
                    account_id: account_id.to_string(),
                    asset: asset.to_string(),
                    balance: balance.raw(),
                    delta: delta.raw(),
                }
                .into());
            }
        }

        Ok(())
    }

    async fn require_record(
        &self,
        scope: &str,
        key: &ironledger_domain::IdempotencyKey,
    ) -> Result<IdempotencyRecord, LedgerError> {
        self.idempotency.find(scope, key).await?.ok_or_else(|| {
            LedgerError::Storage("idempotency row vanished between commit and read".into())
        })
    }
}

/// Append a caller-supplied reference to the entry description.
fn annotate(entry: &mut JournalEntry, reference: Option<&str>) -> Result<(), LedgerError> {
    let Some(reference) = reference.map(str::trim).filter(|r| !r.is_empty()) else {
        return Ok(());
    };
    if reference.len() > MAX_REFERENCE_LEN {
        return Err(LedgerError::Validation(format!(
            "reference must not exceed {MAX_REFERENCE_LEN} characters"
        )));
    }
    entry.description = format!("{}: {reference}", entry.description);
    Ok(())
}

fn validated_description(description: &str) -> Result<String, LedgerError> {
    let description = description.trim();
    if description.is_empty() || description.len() > MAX_DESCRIPTION_LEN {
        return Err(LedgerError::Validation(format!(
            "description must be 1..={MAX_DESCRIPTION_LEN} characters"
        )));
    }
    Ok(description.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart;
    use crate::command::{AuditRequest, CommandMeta, PostingInput};
    use crate::memory::InMemoryLedger;
    use crate::port::OutboxRepo;
    use ironledger_domain::{AccountKind, AccountStatus, IdempotencyKey};
    use std::time::Duration;

    struct Fixture {
        store: InMemoryLedger,
        service: LedgerService,
        accounts: HashMap<String, AccountId>,
    }

    impl Fixture {
        /// Seed the reference chart of accounts for two customers.
        fn new() -> Self {
            let store = InMemoryLedger::new();
            let mut accounts = HashMap::new();
            for spec in chart::full_chart(&["alice", "bob"]) {
                let account =
                    Account::new(AccountId::new(), spec.name.clone(), spec.kind, spec.policy);
                accounts.insert(spec.name, account.id);
                store.seed_account(account);
            }
            let service = store.clone().service();
            Self {
                store,
                service,
                accounts,
            }
        }

        fn id(&self, name: &str) -> AccountId {
            self.accounts[name]
        }

        fn available(&self, customer: &str) -> AccountId {
            self.id(&chart::customer_available(customer))
        }

        fn locked(&self, customer: &str) -> AccountId {
            self.id(&chart::customer_locked(customer))
        }

        fn deposit(
            &self,
            customer: &str,
            asset: AssetCode,
            amount: i128,
            key: &str,
        ) -> RecordDepositCommand {
            RecordDepositCommand {
                meta: CommandMeta::from_key(key).unwrap(),
                customer_available: self.available(customer),
                deposit_clearing: self.id(chart::DEPOSIT_CLEARING),
                hot_wallet: Some(self.id(chart::EXCHANGE_HOT_WALLET)),
                asset,
                amount: AtomicAmount::from_raw(amount),
                external_reference: Some("0xdeadbeef".into()),
            }
        }
    }

    fn btc() -> AssetCode {
        AssetCode::btc()
    }

    fn usd() -> AssetCode {
        AssetCode::usd()
    }

    #[tokio::test]
    async fn deposit_credits_customer_and_debits_custody() {
        let fx = Fixture::new();
        let outcome = fx
            .service
            .record_deposit(fx.deposit("alice", btc(), 100_000_000, "dep-1"))
            .await
            .unwrap();

        assert!(!outcome.replayed);
        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            100_000_000
        );
        assert_eq!(
            fx.store
                .balance_of(fx.id(chart::EXCHANGE_HOT_WALLET), &btc())
                .raw(),
            -100_000_000,
            "custody obligation is credit-normal"
        );
        assert_eq!(
            fx.store
                .balance_of(fx.id(chart::DEPOSIT_CLEARING), &btc())
                .raw(),
            0,
            "clearing nets to zero for a fully processed deposit"
        );
        assert!(outcome.value.description.contains("0xdeadbeef"));
    }

    #[tokio::test]
    async fn replaying_an_identical_command_returns_the_original_result() {
        let fx = Fixture::new();
        let first = fx
            .service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();
        let second = fx
            .service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();

        assert!(!first.replayed);
        assert!(second.replayed);
        assert_eq!(first.value.transaction_id, second.value.transaction_id);
        assert_eq!(
            fx.store.entry_count(),
            1,
            "the entry is posted exactly once"
        );
        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            100
        );
    }

    #[tokio::test]
    async fn reusing_a_key_with_a_different_payload_conflicts() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();
        let conflict = fx
            .service
            .record_deposit(fx.deposit("alice", btc(), 101, "dep-1"))
            .await;

        assert!(matches!(
            conflict,
            Err(LedgerError::IdempotencyConflict { .. })
        ));
        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            100
        );
    }

    #[tokio::test]
    async fn withdrawal_beyond_the_balance_is_rejected() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();

        let result = fx
            .service
            .request_withdrawal(RequestWithdrawalCommand {
                meta: CommandMeta::from_key("wd-1").unwrap(),
                customer_available: fx.available("alice"),
                customer_locked: fx.locked("alice"),
                asset: btc(),
                amount: AtomicAmount::from_raw(101),
                destination_reference: None,
            })
            .await;

        assert!(matches!(
            result,
            Err(LedgerError::Domain(DomainError::InsufficientBalance { .. }))
        ));
        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            100
        );
        assert_eq!(fx.store.balance_of(fx.locked("alice"), &btc()).raw(), 0);
    }

    #[tokio::test]
    async fn withdrawal_lifecycle_moves_funds_out_of_custody() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 500, "dep-1"))
            .await
            .unwrap();
        fx.service
            .request_withdrawal(RequestWithdrawalCommand {
                meta: CommandMeta::from_key("wd-1").unwrap(),
                customer_available: fx.available("alice"),
                customer_locked: fx.locked("alice"),
                asset: btc(),
                amount: AtomicAmount::from_raw(200),
                destination_reference: Some("bc1qexample".into()),
            })
            .await
            .unwrap();

        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            300
        );
        assert_eq!(fx.store.balance_of(fx.locked("alice"), &btc()).raw(), 200);

        fx.service
            .complete_withdrawal(CompleteWithdrawalCommand {
                meta: CommandMeta::from_key("wd-1-complete").unwrap(),
                customer_locked: fx.locked("alice"),
                withdrawal_clearing: fx.id(chart::WITHDRAWAL_CLEARING),
                hot_wallet: fx.id(chart::EXCHANGE_HOT_WALLET),
                asset: btc(),
                amount: AtomicAmount::from_raw(200),
                settlement_reference: Some("0xpayout".into()),
            })
            .await
            .unwrap();

        assert_eq!(fx.store.balance_of(fx.locked("alice"), &btc()).raw(), 0);
        assert_eq!(
            fx.store
                .balance_of(fx.id(chart::WITHDRAWAL_CLEARING), &btc())
                .raw(),
            0,
            "a completed payout leaves clearing flat"
        );
        assert_eq!(
            fx.store
                .balance_of(fx.id(chart::EXCHANGE_HOT_WALLET), &btc())
                .raw(),
            -300,
            "custody obligation shrinks by the payout"
        );
    }

    #[tokio::test]
    async fn rejected_withdrawal_returns_locked_funds() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 500, "dep-1"))
            .await
            .unwrap();
        fx.service
            .request_withdrawal(RequestWithdrawalCommand {
                meta: CommandMeta::from_key("wd-1").unwrap(),
                customer_available: fx.available("alice"),
                customer_locked: fx.locked("alice"),
                asset: btc(),
                amount: AtomicAmount::from_raw(200),
                destination_reference: None,
            })
            .await
            .unwrap();

        let outcome = fx
            .service
            .reject_withdrawal(RejectWithdrawalCommand {
                meta: CommandMeta::from_key("wd-1-reject").unwrap(),
                customer_locked: fx.locked("alice"),
                customer_available: fx.available("alice"),
                asset: btc(),
                amount: AtomicAmount::from_raw(200),
                reason: "compliance hold".into(),
            })
            .await
            .unwrap();

        assert!(outcome.value.description.contains("compliance hold"));
        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            500
        );
        assert_eq!(fx.store.balance_of(fx.locked("alice"), &btc()).raw(), 0);
    }

    #[tokio::test]
    async fn trade_settlement_swaps_two_assets() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", usd(), 50_000_000, "dep-usd"))
            .await
            .unwrap();
        fx.service
            .record_deposit(fx.deposit("bob", btc(), 100_000_000, "dep-btc"))
            .await
            .unwrap();

        fx.service
            .record_trade_settlement(RecordTradeSettlementCommand {
                meta: CommandMeta::from_key("trade-1").unwrap(),
                buyer_available: fx.available("alice"),
                seller_available: fx.available("bob"),
                base_asset: btc(),
                base_amount: AtomicAmount::from_raw(100_000_000),
                quote_asset: usd(),
                quote_amount: AtomicAmount::from_raw(50_000_000),
                trade_reference: Some("trade-42".into()),
            })
            .await
            .unwrap();

        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &btc()).raw(),
            100_000_000
        );
        assert_eq!(fx.store.balance_of(fx.available("alice"), &usd()).raw(), 0);
        assert_eq!(fx.store.balance_of(fx.available("bob"), &btc()).raw(), 0);
        assert_eq!(
            fx.store.balance_of(fx.available("bob"), &usd()).raw(),
            50_000_000
        );
    }

    #[tokio::test]
    async fn trading_fee_moves_funds_to_revenue() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", usd(), 1_000_000, "dep-usd"))
            .await
            .unwrap();
        fx.service
            .record_trading_fee(RecordTradingFeeCommand {
                meta: CommandMeta::from_key("fee-1").unwrap(),
                customer_available: fx.available("alice"),
                fee_revenue: fx.id(chart::FEE_REVENUE),
                asset: usd(),
                amount: AtomicAmount::from_raw(2_500),
            })
            .await
            .unwrap();

        assert_eq!(
            fx.store.balance_of(fx.available("alice"), &usd()).raw(),
            997_500
        );
        assert_eq!(
            fx.store.balance_of(fx.id(chart::FEE_REVENUE), &usd()).raw(),
            2_500
        );
    }

    #[tokio::test]
    async fn admin_adjustment_emits_an_audit_event() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", usd(), 1_000, "dep-usd"))
            .await
            .unwrap();

        fx.service
            .record_admin_adjustment(RecordAdminAdjustmentCommand {
                meta: CommandMeta::from_key("adj-1").unwrap(),
                from: fx.available("alice"),
                to: fx.available("bob"),
                asset: usd(),
                amount: AtomicAmount::from_raw(400),
                audit: AuditRequest {
                    actor: "ops@example.test".into(),
                    reason: "goodwill credit for incident 42".into(),
                    ticket_id: Some("OPS-42".into()),
                },
            })
            .await
            .unwrap();

        assert_eq!(fx.store.balance_of(fx.available("bob"), &usd()).raw(), 400);
        let audit_events = fx
            .store
            .events()
            .into_iter()
            .filter(|event| event.event_type == "admin_adjusted")
            .count();
        assert_eq!(audit_events, 1);
    }

    #[tokio::test]
    async fn admin_adjustment_requires_a_reason() {
        let fx = Fixture::new();
        let result = fx
            .service
            .record_admin_adjustment(RecordAdminAdjustmentCommand {
                meta: CommandMeta::from_key("adj-2").unwrap(),
                from: fx.available("alice"),
                to: fx.available("bob"),
                asset: usd(),
                amount: AtomicAmount::from_raw(1),
                audit: AuditRequest {
                    actor: "ops@example.test".into(),
                    reason: String::new(),
                    ticket_id: None,
                },
            })
            .await;

        assert!(matches!(
            result,
            Err(LedgerError::Domain(DomainError::MissingAdjustmentReason))
        ));
    }

    #[tokio::test]
    async fn unbalanced_submissions_are_rejected() {
        let fx = Fixture::new();
        let result = fx
            .service
            .submit_journal_entry(SubmitJournalCommand {
                meta: CommandMeta::from_key("raw-1").unwrap(),
                description: "manual".into(),
                postings: vec![
                    PostingInput {
                        account_id: fx.available("alice"),
                        asset: usd(),
                        amount: AtomicAmount::from_raw(100),
                    },
                    PostingInput {
                        account_id: fx.available("bob"),
                        asset: usd(),
                        amount: AtomicAmount::from_raw(-99),
                    },
                ],
            })
            .await;

        assert!(matches!(
            result,
            Err(LedgerError::Domain(DomainError::UnbalancedEntry { .. }))
        ));
    }

    #[tokio::test]
    async fn postings_against_a_frozen_account_are_rejected() {
        let fx = Fixture::new();
        let mut frozen = Account::new(
            AccountId::new(),
            "customer:carol:available",
            AccountKind::CustomerAvailable,
            AccountPolicy::NonNegative,
        );
        frozen.status = AccountStatus::Frozen;
        let frozen_id = frozen.id;
        fx.store.seed_account(frozen);

        let result = fx
            .service
            .record_deposit(RecordDepositCommand {
                meta: CommandMeta::from_key("dep-frozen").unwrap(),
                customer_available: frozen_id,
                deposit_clearing: fx.id(chart::DEPOSIT_CLEARING),
                hot_wallet: None,
                asset: usd(),
                amount: AtomicAmount::from_raw(10),
                external_reference: None,
            })
            .await;

        assert!(matches!(
            result,
            Err(LedgerError::Domain(DomainError::AccountNotActive { .. }))
        ));
    }

    #[tokio::test]
    async fn postings_against_an_unknown_account_are_rejected() {
        let fx = Fixture::new();
        let result = fx
            .service
            .record_deposit(RecordDepositCommand {
                meta: CommandMeta::from_key("dep-missing").unwrap(),
                customer_available: AccountId::new(),
                deposit_clearing: fx.id(chart::DEPOSIT_CLEARING),
                hot_wallet: None,
                asset: usd(),
                amount: AtomicAmount::from_raw(10),
                external_reference: None,
            })
            .await;

        assert!(matches!(
            result,
            Err(LedgerError::NotFound {
                entity: "account",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn create_account_is_idempotent_and_name_unique() {
        let store = InMemoryLedger::new();
        let service = store.clone().service();
        let command = |key: &str, name: &str| CreateAccountCommand {
            meta: CommandMeta::from_key(key).unwrap(),
            name: name.to_owned(),
            kind: AccountKind::CustomerAvailable,
            policy: AccountPolicy::NonNegative,
        };

        let first = service
            .create_account(command("acc-1", "customer:dana:available"))
            .await
            .unwrap();
        let replay = service
            .create_account(command("acc-1", "customer:dana:available"))
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(first.value.account.id, replay.value.account.id);

        let duplicate = service
            .create_account(command("acc-2", "customer:dana:available"))
            .await;
        assert!(matches!(duplicate, Err(LedgerError::Conflict(_))));
    }

    #[tokio::test]
    async fn every_command_enqueues_exactly_one_journal_event() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();

        let journal_events = fx
            .store
            .events()
            .into_iter()
            .filter(|event| event.event_type == "journal_posted")
            .count();
        assert_eq!(journal_events, 1);
    }

    #[tokio::test]
    async fn outbox_claims_are_leased_and_acknowledged() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();

        let claimed = OutboxRepo::claim(&fx.store, 10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let re_claimed = OutboxRepo::claim(&fx.store, 10, Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            re_claimed.is_empty(),
            "leased rows are not handed out twice"
        );

        let ids: Vec<i64> = claimed.iter().map(|message| message.id).collect();
        assert_eq!(fx.store.mark_published(&ids).await.unwrap(), 1);

        let stats = fx.service.outbox_stats().await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.published, 1);
    }

    #[tokio::test]
    async fn queries_expose_entries_and_balances() {
        let fx = Fixture::new();
        let posted = fx
            .service
            .record_deposit(fx.deposit("alice", btc(), 100, "dep-1"))
            .await
            .unwrap();

        let entry = fx
            .service
            .get_transaction(posted.value.transaction_id)
            .await
            .unwrap();
        assert_eq!(entry.id, posted.value.transaction_id);

        let balances = fx
            .service
            .get_balances(fx.available("alice"), Some(&btc()))
            .await
            .unwrap();
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].amount.raw(), 100);

        let entries = fx
            .service
            .list_account_entries(fx.available("alice"), Page::default())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);

        let missing = fx.service.get_transaction(TransactionId::new()).await;
        assert!(matches!(missing, Err(LedgerError::NotFound { .. })));
    }

    #[tokio::test]
    async fn balances_sum_to_zero_per_asset() {
        let fx = Fixture::new();
        fx.service
            .record_deposit(fx.deposit("alice", usd(), 1_000_000, "dep-usd"))
            .await
            .unwrap();
        fx.service
            .record_trading_fee(RecordTradingFeeCommand {
                meta: CommandMeta::from_key("fee-1").unwrap(),
                customer_available: fx.available("alice"),
                fee_revenue: fx.id(chart::FEE_REVENUE),
                asset: usd(),
                amount: AtomicAmount::from_raw(2_500),
            })
            .await
            .unwrap();

        let snapshot = BalanceRepo::snapshot(&fx.store).await.unwrap();
        let total: i128 = snapshot
            .iter()
            .filter(|record| record.asset == usd())
            .map(|record| record.amount.raw())
            .sum();
        assert_eq!(total, 0, "double-entry conserves each asset at zero");
    }
}
