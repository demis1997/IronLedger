//! In-memory port implementations.
//!
//! Behaviour mirrors the PostgreSQL adapter — single-transaction commits,
//! `(scope, key)` uniqueness, balance-policy enforcement at commit time and a
//! leased outbox — so handler tests, benchmarks and offline demos exercise the
//! same semantics without a database.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ironledger_domain::{
    Account, AccountId, AccountPolicy, AssetCode, AtomicAmount, DomainError, IdempotencyKey,
    JournalEntry, LedgerEvent, TransactionId,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

use crate::error::LedgerError;
use crate::idempotency::IdempotencyRecord;
use crate::port::{
    AccountCommit, AccountRepo, BalanceRecord, BalanceRepo, CommitOutcome, ConsumerRepo,
    ConsumerStatus, HealthCheck, IdempotencyRepo, JournalCommit, LedgerRepo, OutboxMessage,
    OutboxRepo, OutboxStats, Page,
};
use crate::service::LedgerService;

/// Topic every in-memory outbox message is routed to.
pub const DEFAULT_TOPIC: &str = "ledger.events";

#[derive(Debug)]
struct OutboxRow {
    message: OutboxMessage,
    leased_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct State {
    accounts: HashMap<Uuid, Account>,
    entries: Vec<JournalEntry>,
    balances: HashMap<(Uuid, AssetCode), BalanceRecord>,
    idempotency: HashMap<(String, String), IdempotencyRecord>,
    outbox: Vec<OutboxRow>,
    next_outbox_id: i64,
}

impl State {
    fn record_events(&mut self, events: &[LedgerEvent]) {
        for event in events {
            if self
                .outbox
                .iter()
                .any(|row| row.message.event.event_id == event.event_id)
            {
                continue;
            }
            self.next_outbox_id += 1;
            self.outbox.push(OutboxRow {
                message: OutboxMessage {
                    id: self.next_outbox_id,
                    topic: DEFAULT_TOPIC.to_owned(),
                    partition_key: event.aggregate_id.clone(),
                    event: event.clone(),
                    attempts: 0,
                    created_at: event.created_at,
                    published_at: None,
                    last_error: None,
                },
                leased_until: None,
            });
        }
    }

    fn insert_idempotency(&mut self, record: &IdempotencyRecord) -> CommitOutcome {
        let key = (record.scope.clone(), record.key.to_string());
        if self.idempotency.contains_key(&key) {
            return CommitOutcome::DuplicateIdempotencyKey;
        }
        self.idempotency.insert(key, record.clone());
        CommitOutcome::Committed
    }

    fn apply_postings(&mut self, entry: &JournalEntry) -> Result<(), LedgerError> {
        let now = Utc::now();
        for posting in &entry.postings {
            let account_uuid = posting.account_id.into_uuid();
            let account = self
                .accounts
                .get(&account_uuid)
                .ok_or_else(|| LedgerError::NotFound {
                    entity: "account",
                    id: posting.account_id.to_string(),
                })?
                .clone();
            if !account.is_active() {
                return Err(DomainError::AccountNotActive {
                    account_id: account.id.to_string(),
                    status: account.status.as_str().to_owned(),
                }
                .into());
            }

            let record = self
                .balances
                .entry((account_uuid, posting.asset.clone()))
                .or_insert_with(|| BalanceRecord {
                    account_id: posting.account_id,
                    asset: posting.asset.clone(),
                    amount: AtomicAmount::ZERO,
                    updated_at: now,
                });
            let updated = record.amount.checked_add(posting.amount)?;
            if updated.is_negative() && account.policy == AccountPolicy::NonNegative {
                return Err(DomainError::InsufficientBalance {
                    account_id: account.id.to_string(),
                    asset: posting.asset.to_string(),
                    balance: record.amount.raw(),
                    delta: posting.amount.raw(),
                }
                .into());
            }
            record.amount = updated;
            record.updated_at = now;
        }
        Ok(())
    }
}

/// Shared in-memory ledger storage.
#[derive(Debug, Clone, Default)]
pub struct InMemoryLedger {
    state: Arc<RwLock<State>>,
}

impl InMemoryLedger {
    /// Create empty storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a [`LedgerService`] backed by this storage.
    #[must_use]
    pub fn service(self) -> LedgerService {
        let shared = Arc::new(self);
        LedgerService::new(
            shared.clone(),
            shared.clone(),
            shared.clone(),
            shared.clone(),
            shared,
        )
    }

    /// Insert an account directly, bypassing command handling.
    ///
    /// Useful for arranging test fixtures without issuing `create_account`.
    pub fn seed_account(&self, account: Account) {
        self.write()
            .accounts
            .insert(account.id.into_uuid(), account);
    }

    /// Current balance for an `(account, asset)` pair.
    #[must_use]
    pub fn balance_of(&self, account_id: AccountId, asset: &AssetCode) -> AtomicAmount {
        self.read()
            .balances
            .get(&(account_id.into_uuid(), asset.clone()))
            .map_or(AtomicAmount::ZERO, |record| record.amount)
    }

    /// Number of journal entries recorded.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.read().entries.len()
    }

    /// Copy of all journal entries in insertion order.
    #[must_use]
    pub fn journal_entries(&self) -> Vec<JournalEntry> {
        self.read().entries.clone()
    }

    /// Every event currently in the outbox, in insertion order.
    #[must_use]
    pub fn events(&self) -> Vec<LedgerEvent> {
        self.read()
            .outbox
            .iter()
            .map(|row| row.message.event.clone())
            .collect()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|err| err.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(|err| err.into_inner())
    }
}

#[async_trait]
impl AccountRepo for InMemoryLedger {
    async fn create(&self, commit: &AccountCommit) -> Result<CommitOutcome, LedgerError> {
        let mut state = self.write();
        if state.insert_idempotency(&commit.idempotency) == CommitOutcome::DuplicateIdempotencyKey {
            return Ok(CommitOutcome::DuplicateIdempotencyKey);
        }
        if state
            .accounts
            .values()
            .any(|account| account.name == commit.account.name)
        {
            return Err(LedgerError::Conflict(format!(
                "account name '{}' is already taken",
                commit.account.name
            )));
        }
        state
            .accounts
            .insert(commit.account.id.into_uuid(), commit.account.clone());
        state.record_events(&commit.events);
        Ok(CommitOutcome::Committed)
    }

    async fn find(&self, id: AccountId) -> Result<Option<Account>, LedgerError> {
        Ok(self.read().accounts.get(id.as_uuid()).cloned())
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<Account>, LedgerError> {
        Ok(self
            .read()
            .accounts
            .values()
            .find(|account| account.name == name)
            .cloned())
    }

    async fn load_many(&self, ids: &[AccountId]) -> Result<Vec<Account>, LedgerError> {
        let state = self.read();
        Ok(ids
            .iter()
            .filter_map(|id| state.accounts.get(id.as_uuid()).cloned())
            .collect())
    }

    async fn list(&self, page: Page) -> Result<Vec<Account>, LedgerError> {
        let state = self.read();
        let mut accounts: Vec<Account> = state.accounts.values().cloned().collect();
        accounts.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(accounts
            .into_iter()
            .skip(usize::try_from(page.offset).unwrap_or(usize::MAX))
            .take(page.limit as usize)
            .collect())
    }
}

#[async_trait]
impl LedgerRepo for InMemoryLedger {
    async fn commit(&self, commit: &JournalCommit) -> Result<CommitOutcome, LedgerError> {
        let mut state = self.write();
        if state.insert_idempotency(&commit.idempotency) == CommitOutcome::DuplicateIdempotencyKey {
            return Ok(CommitOutcome::DuplicateIdempotencyKey);
        }
        // Snapshot balances so a policy rejection leaves no partial state, the
        // way a rolled-back SQL transaction would.
        let snapshot = state.balances.clone();
        if let Err(err) = state.apply_postings(&commit.entry) {
            state.balances = snapshot;
            state.idempotency.remove(&(
                commit.idempotency.scope.clone(),
                commit.idempotency.key.to_string(),
            ));
            return Err(err);
        }
        state.entries.push(commit.entry.clone());
        state.record_events(&commit.events);
        Ok(CommitOutcome::Committed)
    }

    async fn find_entry(&self, id: TransactionId) -> Result<Option<JournalEntry>, LedgerError> {
        Ok(self
            .read()
            .entries
            .iter()
            .find(|entry| entry.id == id)
            .cloned())
    }

    async fn list_account_entries(
        &self,
        account_id: AccountId,
        page: Page,
    ) -> Result<Vec<JournalEntry>, LedgerError> {
        let state = self.read();
        Ok(state
            .entries
            .iter()
            .rev()
            .filter(|entry| {
                entry
                    .postings
                    .iter()
                    .any(|posting| posting.account_id == account_id)
            })
            .skip(usize::try_from(page.offset).unwrap_or(usize::MAX))
            .take(page.limit as usize)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl IdempotencyRepo for InMemoryLedger {
    async fn find(
        &self,
        scope: &str,
        key: &IdempotencyKey,
    ) -> Result<Option<IdempotencyRecord>, LedgerError> {
        Ok(self
            .read()
            .idempotency
            .get(&(scope.to_owned(), key.to_string()))
            .cloned())
    }

    async fn purge_created_before(&self, cutoff: DateTime<Utc>) -> Result<u64, LedgerError> {
        let mut state = self.write();
        let before = state.idempotency.len();
        state
            .idempotency
            .retain(|_, record| record.created_at >= cutoff);
        Ok((before - state.idempotency.len()) as u64)
    }
}

#[async_trait]
impl OutboxRepo for InMemoryLedger {
    async fn claim(&self, limit: u32, lease: Duration) -> Result<Vec<OutboxMessage>, LedgerError> {
        let now = Utc::now();
        let lease_until = now
            + chrono::Duration::from_std(lease)
                .map_err(|err| LedgerError::Validation(err.to_string()))?;
        let mut state = self.write();
        let mut claimed = Vec::new();
        for row in state.outbox.iter_mut() {
            if claimed.len() >= limit as usize {
                break;
            }
            if row.message.published_at.is_some() {
                continue;
            }
            if row.leased_until.is_some_and(|until| until > now) {
                continue;
            }
            row.leased_until = Some(lease_until);
            claimed.push(row.message.clone());
        }
        Ok(claimed)
    }

    async fn mark_published(&self, ids: &[i64]) -> Result<u64, LedgerError> {
        let now = Utc::now();
        let mut state = self.write();
        let mut updated = 0;
        for row in state.outbox.iter_mut() {
            if ids.contains(&row.message.id) && row.message.published_at.is_none() {
                row.message.published_at = Some(now);
                row.leased_until = None;
                updated += 1;
            }
        }
        Ok(updated)
    }

    async fn mark_failed(&self, id: i64, error: &str) -> Result<(), LedgerError> {
        let mut state = self.write();
        if let Some(row) = state.outbox.iter_mut().find(|row| row.message.id == id) {
            row.message.attempts += 1;
            row.message.last_error = Some(error.to_owned());
            row.leased_until = None;
        }
        Ok(())
    }

    async fn stats(&self) -> Result<OutboxStats, LedgerError> {
        let now = Utc::now();
        let state = self.read();
        let mut stats = OutboxStats::default();
        let mut oldest: Option<DateTime<Utc>> = None;
        for row in &state.outbox {
            if row.message.published_at.is_some() {
                stats.published += 1;
                continue;
            }
            stats.pending += 1;
            if row.message.attempts > 0 {
                stats.retrying += 1;
            }
            if row.leased_until.is_some_and(|until| until > now) {
                stats.in_flight += 1;
            }
            oldest = Some(match oldest {
                Some(current) => current.min(row.message.created_at),
                None => row.message.created_at,
            });
        }
        stats.oldest_pending_age_seconds =
            oldest.map(|created| (now - created).num_seconds().max(0));
        Ok(stats)
    }

    async fn pending(&self, page: Page) -> Result<Vec<OutboxMessage>, LedgerError> {
        let state = self.read();
        Ok(state
            .outbox
            .iter()
            .filter(|row| row.message.published_at.is_none())
            .skip(usize::try_from(page.offset).unwrap_or(usize::MAX))
            .take(page.limit as usize)
            .map(|row| row.message.clone())
            .collect())
    }
}

#[async_trait]
impl BalanceRepo for InMemoryLedger {
    async fn get(
        &self,
        account_id: AccountId,
        asset: &AssetCode,
    ) -> Result<Option<BalanceRecord>, LedgerError> {
        Ok(self
            .read()
            .balances
            .get(&(account_id.into_uuid(), asset.clone()))
            .cloned())
    }

    async fn list_for_account(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<BalanceRecord>, LedgerError> {
        let state = self.read();
        let mut records: Vec<BalanceRecord> = state
            .balances
            .values()
            .filter(|record| record.account_id == account_id)
            .cloned()
            .collect();
        records.sort_by(|left, right| left.asset.cmp(&right.asset));
        Ok(records)
    }

    async fn snapshot(&self) -> Result<Vec<BalanceRecord>, LedgerError> {
        let state = self.read();
        let mut records: Vec<BalanceRecord> = state.balances.values().cloned().collect();
        records.sort_by(|left, right| {
            (left.account_id.into_uuid(), &left.asset)
                .cmp(&(right.account_id.into_uuid(), &right.asset))
        });
        Ok(records)
    }
}

#[async_trait]
impl ConsumerRepo for InMemoryLedger {
    async fn status(&self) -> Result<Vec<ConsumerStatus>, LedgerError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl HealthCheck for InMemoryLedger {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    async fn ping(&self) -> Result<(), LedgerError> {
        Ok(())
    }
}
