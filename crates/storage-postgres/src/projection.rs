//! Projection store and event source backed by PostgreSQL.

use async_trait::async_trait;
use chrono::Utc;
use ironledger_domain::{EventId, JournalEntry};
use ironledger_projector::{
    ApplyOutcome, EventSource, ProjectionStore, ProjectorError, SourcedEvent, StreamPosition,
};
use sqlx::{PgPool, Row};

use crate::convert::{amount_from_str, amount_to_str, event_from_payload};
use crate::error::StorageError;

/// PostgreSQL projection adapter.
#[derive(Clone)]
pub struct PostgresProjection {
    pool: PgPool,
}

impl PostgresProjection {
    /// Wrap a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProjectionStore for PostgresProjection {
    async fn apply_journal(
        &self,
        consumer: &str,
        event_id: EventId,
        entry: &JournalEntry,
    ) -> Result<ApplyOutcome, ProjectorError> {
        let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO processed_events (consumer, event_id, processed_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (consumer, event_id) DO NOTHING
            "#,
        )
        .bind(consumer)
        .bind(*event_id.as_uuid())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(StorageError::from)?;
        if inserted.rows_affected() == 0 {
            tx.commit().await.map_err(StorageError::from)?;
            return Ok(ApplyOutcome::Duplicate);
        }
        let now = Utc::now();
        for posting in &entry.postings {
            let row = sqlx::query(
                r#"
                SELECT amount_atomic FROM projection_balances
                WHERE consumer = $1 AND account_id = $2 AND asset = $3
                FOR UPDATE
                "#,
            )
            .bind(consumer)
            .bind(posting.account_id.into_uuid())
            .bind(posting.asset.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(StorageError::from)?;
            let current = row
                .as_ref()
                .map(|r| amount_from_str(r.try_get::<String, _>("amount_atomic")?.as_str()))
                .transpose()
                .map_err(StorageError::from)?
                .unwrap_or(ironledger_domain::AtomicAmount::ZERO);
            let updated = current
                .checked_add(posting.amount)
                .map_err(|err| ProjectorError::Domain(err))?;
            if row.is_some() {
                sqlx::query(
                    r#"
                    UPDATE projection_balances
                    SET amount_atomic = $4, updated_at = $5
                    WHERE consumer = $1 AND account_id = $2 AND asset = $3
                    "#,
                )
                .bind(consumer)
                .bind(posting.account_id.into_uuid())
                .bind(posting.asset.as_str())
                .bind(amount_to_str(updated))
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(StorageError::from)?;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO projection_balances (consumer, account_id, asset, amount_atomic, updated_at)
                    VALUES ($1, $2, $3, $4, $5)
                    "#,
                )
                .bind(consumer)
                .bind(posting.account_id.into_uuid())
                .bind(posting.asset.as_str())
                .bind(amount_to_str(updated))
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(StorageError::from)?;
            }
        }
        tx.commit().await.map_err(StorageError::from)?;
        Ok(ApplyOutcome::Applied)
    }

    async fn record_position(
        &self,
        consumer: &str,
        position: &StreamPosition,
    ) -> Result<(), ProjectorError> {
        sqlx::query(
            r#"
            INSERT INTO consumer_offsets (consumer, topic, partition, offset, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (consumer, topic, partition)
            DO UPDATE SET offset = EXCLUDED.offset, updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(consumer)
        .bind(&position.topic)
        .bind(position.partition)
        .bind(position.offset)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }

    async fn load_position(
        &self,
        consumer: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ProjectorError> {
        let row = sqlx::query(
            r#"
            SELECT offset FROM consumer_offsets
            WHERE consumer = $1 AND topic = $2 AND partition = $3
            "#,
        )
        .bind(consumer)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::from)?;
        row.map(|r| r.try_get("offset").map_err(StorageError::from))
            .transpose()
            .map_err(Into::into)
    }

    async fn reset(&self, consumer: &str) -> Result<(), ProjectorError> {
        let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
        sqlx::query(r#"DELETE FROM projection_balances WHERE consumer = $1"#)
            .bind(consumer)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::from)?;
        sqlx::query(r#"DELETE FROM processed_events WHERE consumer = $1"#)
            .bind(consumer)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::from)?;
        sqlx::query(r#"DELETE FROM consumer_offsets WHERE consumer = $1"#)
            .bind(consumer)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::from)?;
        tx.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}

#[async_trait]
impl EventSource for PostgresProjection {
    async fn fetch_after(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<Vec<SourcedEvent>, ProjectorError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, payload, created_at
            FROM outbox
            WHERE id > $1
            ORDER BY id ASC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::from)?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let cursor: i64 = row.try_get("id").map_err(StorageError::from)?;
            let event_id: uuid::Uuid = row.try_get("event_id").map_err(StorageError::from)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(StorageError::from)?;
            let created_at = row.try_get("created_at").map_err(StorageError::from)?;
            let event = event_from_payload(event_id, payload, created_at)?;
            events.push(SourcedEvent { cursor, event });
        }
        Ok(events)
    }
}
