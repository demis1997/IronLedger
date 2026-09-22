//! Exponential backoff with jitter.

use std::time::Duration;
use tokio::time::sleep;

/// Compute delay for attempt `attempt` (0-based).
#[must_use]
pub fn backoff_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    let exp = 2_u32.saturating_pow(attempt.min(12));
    let millis = base.as_millis().saturating_mul(u128::from(exp));
    let capped = millis.min(max.as_millis());
    let jitter = (attempt as u128 % 7).saturating_mul(13);
    Duration::from_millis(u64::try_from(capped.saturating_add(jitter)).unwrap_or(max.as_secs()))
}

/// Sleep for the backoff duration of `attempt`.
pub async fn sleep_backoff(attempt: u32, base: Duration, max: Duration) {
    sleep(backoff_delay(attempt, base, max)).await;
}
