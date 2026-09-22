//! Ports (traits) the application layer depends on.
//!
//! Adapters implement these; the handlers never see SQL, Kafka or HTTP.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ironledger_domain::{
    Account, AccountId, AssetCode, AtomicAmount, EventId, IdempotencyKey, JournalEntry,
    LedgerEvent, TransactionId,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::LedgerError;
use crate::idempotency::IdempotencyRecord;

/// Result of an atomic commit attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The transaction was applied.
    Committed,
    /// Another writer inserted the same `(scope, idempotency_key)` first.
    ///
    /// The caller re-reads the idempotency record and either replays the
    /// original response or reports a conflict.
    DuplicateIdempotencyKey,
}

/// Pagination request, clamped to a server-side maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Page {
    /// Maximum rows to return.
    pub limit: u32,
    /// Rows to skip.
    pub offset: u64,
}

impl Page {
    /// Largest page any query will return.
    pub const MAX_LIMIT: u32 = 500;
    /// Applied when the caller passes `0`.
    pub const DEFAULT_LIMIT: u32 = 50;

    /// Build a clamped page.
    #[must_use]
    pub fn new(limit: u32, offset: u64) -> Self {
        let limit = match limit {
            0 => Self::DEFAULT_LIMIT,
            other => other.min(Self::MAX_LIMIT),
        };
        Self { limit, offset }
    }

    /// Limit as `i64` for SQL bindings.
    #[must_use]
    pub fn limit_i64(self) -> i64 {
        i64::from(self.limit)
    }

    /// Offset as `i64` for SQL bindings, saturating at `i64::MAX`.
    #[must_use]
    pub fn offset_i64(self) -> i64 {
        i64::try_from(self.offset).unwrap_or(i64::MAX)
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LIMIT, 0)
    }
}

/// Everything that must be persisted atomically when an account is opened.
#[derive(Debug, Clone)]
pub struct AccountCommit {
    /// The new account.
    pub account: Account,
    /// Events to enqueue in the outbox.
    pub events: Vec<LedgerEvent>,
    /// Idempotency record capturing the response.
    pub idempotency: IdempotencyRecord,
}

/// Everything that must be persisted atomically when an entry is posted.
///
/// The adapter applies all of this in **one** database transaction: journal
/// entry, postings, balance updates, outbox rows and the idempotency record.
#[derive(Debug, Clone)]
pub struct JournalCommit {
    /// Validated, balanced entry.
    pub entry: JournalEntry,
    /// Events to enqueue in the outbox.
    pub events: Vec<LedgerEvent>,
    /// Idempotency record capturing the response.
    pub idempotency: IdempotencyRecord,
}

/// Materialized balance for one `(account, asset)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceRecord {
    /// Account.
    pub account_id: AccountId,
    /// Asset.
    pub asset: AssetCode,
    /// Signed atomic balance.
    pub amount: AtomicAmount,
    /// Last mutation time.
    pub updated_at: DateTime<Utc>,
}

/// An event awaiting publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxMessage {
    /// Monotonic outbox row id, also the publication order.
    pub id: i64,
    /// Destination topic.
    pub topic: String,
    /// Partition key; keeps per-aggregate ordering.
    pub partition_key: String,
    /// The event envelope.
    pub event: LedgerEvent,
    /// Failed publication attempts so far.
    pub attempts: i32,
    /// Enqueue time.
    pub created_at: DateTime<Utc>,
    /// Publication time, when already published.
    pub published_at: Option<DateTime<Utc>>,
    /// Most recent publication error.
    pub last_error: Option<String>,
}

impl OutboxMessage {
    /// Event id, used for consumer-side deduplication.
    #[must_use]
    pub fn event_id(&self) -> EventId {
        self.event.event_id
    }
}

/// Outbox health, surfaced on `/admin/outbox`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OutboxStats {
    /// Rows not yet published.
    pub pending: i64,
    /// Rows published.
    pub published: i64,
    /// Rows currently leased by a relay.
    pub in_flight: i64,
    /// Pending rows that have failed at least once.
    pub retrying: i64,
    /// Age of the oldest pending row.
    pub oldest_pending_age_seconds: Option<i64>,
}

/// A consumer's committed position on one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerOffset {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Next offset to read.
    pub offset: i64,
    /// Last commit time.
    pub updated_at: DateTime<Utc>,
}

/// Aggregated consumer progress, surfaced on `/admin/consumers`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStatus {
    /// Consumer group name.
    pub consumer: String,
    /// Deduplicated events applied.
    pub processed_events: i64,
    /// Most recent application time.
    pub last_processed_at: Option<DateTime<Utc>>,
    /// Per-partition offsets.
    pub offsets: Vec<ConsumerOffset>,
}

/// Account persistence.
#[async_trait]
pub trait AccountRepo: Send + Sync + 'static {
    /// Insert an account together with its events and idempotency record.
    async fn create(&self, commit: &AccountCommit) -> Result<CommitOutcome, LedgerError>;

    /// Look up by id.
    async fn find(&self, id: AccountId) -> Result<Option<Account>, LedgerError>;

    /// Look up by unique name.
    async fn find_by_name(&self, name: &str) -> Result<Option<Account>, LedgerError>;

    /// Load several accounts at once; missing ids are simply absent.
    async fn load_many(&self, ids: &[AccountId]) -> Result<Vec<Account>, LedgerError>;

    /// List accounts for operators and the demo.
    async fn list(&self, page: Page) -> Result<Vec<Account>, LedgerError>;
}

/// Journal persistence and reads.
#[async_trait]
pub trait LedgerRepo: Send + Sync + 'static {
    /// Apply a journal commit atomically.
    ///
    /// Implementations must enforce account balance policies inside the same
    /// transaction; the handler's pre-check is an optimization, not the
    /// authority.
    async fn commit(&self, commit: &JournalCommit) -> Result<CommitOutcome, LedgerError>;

    /// Fetch one entry with its postings.
    async fn find_entry(&self, id: TransactionId) -> Result<Option<JournalEntry>, LedgerError>;

    /// Entries touching an account, newest first.
    async fn list_account_entries(
        &self,
        account_id: AccountId,
        page: Page,
    ) -> Result<Vec<JournalEntry>, LedgerError>;
}

/// Idempotency record lookups.
#[async_trait]
pub trait IdempotencyRepo: Send + Sync + 'static {
    /// Find a previously stored response.
    async fn find(
        &self,
        scope: &str,
        key: &IdempotencyKey,
    ) -> Result<Option<IdempotencyRecord>, LedgerError>;

    /// Drop records older than `cutoff`; returns rows removed.
    async fn purge_created_before(&self, cutoff: DateTime<Utc>) -> Result<u64, LedgerError>;
}

/// Transactional outbox operations used by the relay and admin API.
#[async_trait]
pub trait OutboxRepo: Send + Sync + 'static {
    /// Lease up to `limit` unpublished messages in id order.
    ///
    /// A lease expires after `lease`, so a relay that dies mid-publish does not
    /// strand rows.
    async fn claim(&self, limit: u32, lease: Duration) -> Result<Vec<OutboxMessage>, LedgerError>;

    /// Mark messages as published.
    async fn mark_published(&self, ids: &[i64]) -> Result<u64, LedgerError>;

    /// Release a lease and record the failure.
    async fn mark_failed(&self, id: i64, error: &str) -> Result<(), LedgerError>;

    /// Aggregate counters.
    async fn stats(&self) -> Result<OutboxStats, LedgerError>;

    /// Inspect the pending backlog without leasing it.
    async fn pending(&self, page: Page) -> Result<Vec<OutboxMessage>, LedgerError>;
}

/// Balance reads. Writes happen inside [`LedgerRepo::commit`].
#[async_trait]
pub trait BalanceRepo: Send + Sync + 'static {
    /// One `(account, asset)` balance.
    async fn get(
        &self,
        account_id: AccountId,
        asset: &AssetCode,
    ) -> Result<Option<BalanceRecord>, LedgerError>;

    /// Every asset held by an account.
    async fn list_for_account(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<BalanceRecord>, LedgerError>;

    /// Full balance snapshot, used by reconciliation.
    async fn snapshot(&self) -> Result<Vec<BalanceRecord>, LedgerError>;
}

/// Consumer progress reads for the admin API.
#[async_trait]
pub trait ConsumerRepo: Send + Sync + 'static {
    /// Progress per consumer group.
    async fn status(&self) -> Result<Vec<ConsumerStatus>, LedgerError>;
}

/// Readiness probe for a dependency.
#[async_trait]
pub trait HealthCheck: Send + Sync + 'static {
    /// Dependency name reported in the readiness payload.
    fn name(&self) -> &'static str;

    /// Cheap round-trip against the dependency.
    async fn ping(&self) -> Result<(), LedgerError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_clamps_limits() {
        assert_eq!(Page::new(0, 0).limit, Page::DEFAULT_LIMIT);
        assert_eq!(Page::new(10_000, 0).limit, Page::MAX_LIMIT);
        assert_eq!(Page::new(25, 5).limit, 25);
    }

    #[test]
    fn page_offset_saturates() {
        assert_eq!(Page::new(1, u64::MAX).offset_i64(), i64::MAX);
    }
}
