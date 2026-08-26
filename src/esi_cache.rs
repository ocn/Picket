//! Feed-agnostic ESI conditional-request cache metadata and shared
//! rate-limit bookkeeping.
//!
//! These helpers are shared by the contract, sovereignty timer, and
//! watchlist feeds: they parse ESI response headers into [`CacheMetadata`],
//! merge cache metadata across paginated or multi-request fetches, and
//! compute when a feed's collector should pause or pace its next request.
//!
//! Per ADR 0004 (`docs/adr/0004-recover-contract-monitoring-in-process.md`),
//! the legacy `x-esi-error-limit-*` allowance is global per application even
//! where per-route `x-ratelimit-group` buckets exist, so every feed must
//! coordinate through the same persisted limiter row rather than tracking
//! its own. [`EsiLimiterStore`] is the crate-visible seam a feed's store
//! implements to read and record that shared row without exposing the rest
//! of its persistence surface.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{HeaderMap, ETAG, EXPIRES, LAST_MODIFIED, RETRY_AFTER};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CacheMetadata {
    pub etag: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_modified: Option<DateTime<Utc>>,
    pub expected_pages: Option<u32>,
    pub error_limit_remain: Option<i64>,
    pub error_limit_reset: Option<i64>,
    pub rate_limit_group: Option<String>,
    pub rate_limit_limit: Option<String>,
    pub rate_limit_remaining: Option<i64>,
    pub rate_limit_used: Option<i64>,
    pub retry_after: Option<DateTime<Utc>>,
}

impl CacheMetadata {
    pub fn cached_for_seconds(seconds: i64) -> Self {
        Self {
            etag: None,
            expires_at: Some(Utc::now() + ChronoDuration::seconds(seconds)),
            last_modified: None,
            expected_pages: None,
            error_limit_remain: None,
            error_limit_reset: None,
            rate_limit_group: None,
            rate_limit_limit: None,
            rate_limit_remaining: None,
            rate_limit_used: None,
            retry_after: None,
        }
    }

    pub fn page_cached_for_seconds(expected_pages: u32, seconds: i64) -> Self {
        Self {
            expected_pages: Some(expected_pages),
            ..Self::cached_for_seconds(seconds)
        }
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at > Utc::now())
            .unwrap_or(false)
    }

    pub(crate) fn retry_is_active(&self) -> bool {
        self.retry_after
            .map(|retry_after| retry_after > Utc::now())
            .unwrap_or(false)
    }

    pub(crate) fn collection_pause_until_at(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.retry_after.or_else(|| {
            let reset_seconds = if self.error_limit_remain.unwrap_or(1) <= 0 {
                self.error_limit_reset
            } else if self.rate_limit_remaining.unwrap_or(1) <= 0 {
                self.rate_limit_limit
                    .as_deref()
                    .and_then(rate_limit_window_seconds)
            } else {
                None
            };
            reset_seconds.map(|seconds| observed_at + ChronoDuration::seconds(seconds))
        })
    }

    pub(crate) fn collection_pacing_until(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        rate_limit_pacing_deadline(
            self.rate_limit_limit.as_deref()?,
            self.rate_limit_remaining?,
            observed_at,
        )
    }
}

pub(crate) fn esi_limiter_deadline_at(
    metadata: &CacheMetadata,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    [
        metadata.collection_pause_until_at(observed_at),
        metadata.collection_pacing_until(observed_at),
    ]
    .into_iter()
    .flatten()
    .max()
}

#[derive(Clone, Debug)]
pub struct EsiResponse<T> {
    pub value: Option<T>,
    pub metadata: CacheMetadata,
    pub not_modified: bool,
}

impl<T> EsiResponse<T> {
    pub fn fresh(value: T, metadata: CacheMetadata) -> Self {
        Self {
            value: Some(value),
            metadata,
            not_modified: false,
        }
    }

    pub fn not_modified(metadata: CacheMetadata) -> Self {
        Self {
            value: None,
            metadata,
            not_modified: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EsiError {
    pub(crate) message: String,
    pub(crate) status: Option<StatusCode>,
    pub(crate) retry_after: Option<DateTime<Utc>>,
    pub(crate) metadata: CacheMetadata,
}

impl EsiError {
    pub fn retryable(message: impl Into<String>, retry_after: Option<DateTime<Utc>>) -> Self {
        Self {
            message: message.into(),
            status: None,
            retry_after,
            metadata: CacheMetadata {
                retry_after,
                ..CacheMetadata::cached_for_seconds(0)
            },
        }
    }

    pub(crate) fn from_metadata(
        message: impl Into<String>,
        status: Option<StatusCode>,
        metadata: CacheMetadata,
    ) -> Self {
        Self {
            message: message.into(),
            status,
            retry_after: metadata.retry_after,
            metadata,
        }
    }

    pub(crate) fn is_not_found(&self) -> bool {
        self.status == Some(StatusCode::NOT_FOUND)
    }

    pub(crate) fn is_error_charged(&self) -> bool {
        self.status
            .is_some_and(|status| status.is_client_error() || status.is_server_error())
    }
}

impl Display for EsiError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for EsiError {}

pub(crate) fn cache_metadata(headers: &HeaderMap) -> CacheMetadata {
    cache_metadata_at(headers, Utc::now())
}

pub(crate) fn cache_metadata_at(headers: &HeaderMap, observed_at: DateTime<Utc>) -> CacheMetadata {
    let mut metadata = CacheMetadata {
        etag: headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        expires_at: headers
            .get(EXPIRES)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_http_time),
        last_modified: headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_http_time),
        expected_pages: header_number(headers, "x-pages").and_then(|value| value.try_into().ok()),
        error_limit_remain: header_number(headers, "x-esi-error-limit-remain"),
        error_limit_reset: header_number(headers, "x-esi-error-limit-reset"),
        rate_limit_group: headers
            .get("x-ratelimit-group")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        rate_limit_limit: headers
            .get("x-ratelimit-limit")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        rate_limit_remaining: header_number(headers, "x-ratelimit-remaining"),
        rate_limit_used: header_number(headers, "x-ratelimit-used"),
        retry_after: headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_retry_after_at(value, observed_at)),
    };
    if let Some(seconds) = headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .and_then(cache_max_age)
    {
        metadata.expires_at = Some(observed_at + ChronoDuration::seconds(seconds));
    }
    if metadata.retry_after.is_none() && metadata.error_limit_remain.unwrap_or(1) <= 0 {
        metadata.retry_after = metadata
            .error_limit_reset
            .map(|seconds| observed_at + ChronoDuration::seconds(seconds));
    }
    metadata
}

fn header_number(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn parse_http_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn parse_retry_after_at(value: &str, observed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    value
        .parse::<i64>()
        .ok()
        .map(|seconds| observed_at + ChronoDuration::seconds(seconds))
        .or_else(|| parse_http_time(value))
}

fn cache_max_age(value: &str) -> Option<i64> {
    value.split(',').map(str::trim).find_map(|directive| {
        directive
            .strip_prefix("max-age=")
            .and_then(|seconds| seconds.parse().ok())
    })
}

fn rate_limit_window_seconds(value: &str) -> Option<i64> {
    rate_limit_capacity_and_window_seconds(value).map(|(_, window)| window)
}

fn rate_limit_capacity_and_window_seconds(value: &str) -> Option<(i64, i64)> {
    let (limit, window) = value.split_once('/')?;
    let limit = limit.parse::<i64>().ok()?;
    let (number, unit) = window.split_at(window.len().checked_sub(1)?);
    let count = number.parse::<i64>().ok()?;
    let window_seconds = match unit {
        "s" => Some(count),
        "m" => count.checked_mul(60),
        "h" => count.checked_mul(60 * 60),
        "d" => count.checked_mul(60 * 60 * 24),
        _ => None,
    }?;
    (limit > 0 && window_seconds > 0).then_some((limit, window_seconds))
}

pub(crate) fn rate_limit_pacing_deadline(
    rate_limit: &str,
    remaining: i64,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let (limit, window_seconds) = rate_limit_capacity_and_window_seconds(rate_limit)?;
    if remaining < 0 || remaining.saturating_mul(10) > limit {
        return None;
    }
    let spacing_seconds = (window_seconds + remaining.max(1) - 1) / remaining.max(1);
    Some(observed_at + ChronoDuration::seconds(spacing_seconds.max(1)))
}

pub(crate) fn merge_cache_metadata(
    cached: &CacheMetadata,
    response: CacheMetadata,
) -> CacheMetadata {
    CacheMetadata {
        etag: response.etag.or_else(|| cached.etag.clone()),
        expires_at: response.expires_at.or(cached.expires_at),
        last_modified: response.last_modified.or(cached.last_modified),
        expected_pages: response.expected_pages.or(cached.expected_pages),
        error_limit_remain: response.error_limit_remain.or(cached.error_limit_remain),
        error_limit_reset: response.error_limit_reset.or(cached.error_limit_reset),
        rate_limit_group: response
            .rate_limit_group
            .or_else(|| cached.rate_limit_group.clone()),
        rate_limit_limit: response
            .rate_limit_limit
            .or_else(|| cached.rate_limit_limit.clone()),
        rate_limit_remaining: response
            .rate_limit_remaining
            .or(cached.rate_limit_remaining),
        rate_limit_used: response.rate_limit_used.or(cached.rate_limit_used),
        retry_after: response.retry_after.or(cached.retry_after),
    }
}

/// A snapshot of the persisted legacy ESI error-limit row, as last observed.
#[derive(Clone, Copy)]
pub(crate) struct CurrentLegacyErrorLimit {
    pub(crate) remaining: i64,
    pub(crate) reset_at: DateTime<Utc>,
}

/// Crate-visible access to a feed's persisted ESI limiter row.
///
/// The legacy `x-esi-error-limit-*` allowance is global per application
/// (ADR 0004), so every feed's collector coordinates through the same
/// persisted row rather than tracking its own error budget. A feed holds a
/// `dyn EsiLimiterStore` (or is generic over it) to read and record that
/// row without seeing the rest of the underlying store's persistence
/// surface.
// This ticket only prepares the seam: `ContractCollectionStore` implements
// it below (see `src/contract_intelligence.rs`), but no feed holds a
// `dyn EsiLimiterStore` / is generic over it yet, so plain `cargo check`
// (which excludes `#[cfg(test)]`) sees it as unused. The sov timer and
// watchlist feeds are the intended consumers; remove this allow once one
// of them lands.
#[allow(dead_code)]
#[async_trait]
pub(crate) trait EsiLimiterStore: Send + Sync {
    /// The deadline, if any, until which requests should be paused.
    async fn active_esi_limiter_deadline(&self) -> Result<Option<DateTime<Utc>>, sqlx::Error>;

    /// The current legacy error-limit allowance, if still within its reset
    /// window as of `observed_at`.
    async fn current_legacy_error_limit(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<CurrentLegacyErrorLimit>, sqlx::Error>;

    /// Records limiter headers observed at `observed_at`, merging them into
    /// the persisted row.
    async fn record_esi_limiter_at(
        &self,
        metadata: &CacheMetadata,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A fake `EsiLimiterStore` used to exercise callers generic over the
    /// trait, and to prove the trait is object-safe via `_assert_object_safe`.
    #[derive(Default)]
    struct FakeLimiterStore {
        deadline: Mutex<Option<DateTime<Utc>>>,
        record_calls: AtomicUsize,
    }

    #[async_trait]
    impl EsiLimiterStore for FakeLimiterStore {
        async fn active_esi_limiter_deadline(&self) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
            Ok(*self.deadline.lock().unwrap())
        }

        async fn current_legacy_error_limit(
            &self,
            _observed_at: DateTime<Utc>,
        ) -> Result<Option<CurrentLegacyErrorLimit>, sqlx::Error> {
            Ok(None)
        }

        async fn record_esi_limiter_at(
            &self,
            metadata: &CacheMetadata,
            observed_at: DateTime<Utc>,
        ) -> Result<(), sqlx::Error> {
            self.record_calls.fetch_add(1, Ordering::SeqCst);
            *self.deadline.lock().unwrap() = esi_limiter_deadline_at(metadata, observed_at);
            Ok(())
        }
    }

    // Compile-time object-safety check: a second feed must be able to hold
    // a `dyn EsiLimiterStore` alongside its own store.
    fn _assert_object_safe(_: &dyn EsiLimiterStore) {}

    async fn record_and_read_deadline(
        store: &dyn EsiLimiterStore,
        metadata: &CacheMetadata,
        observed_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        store
            .record_esi_limiter_at(metadata, observed_at)
            .await
            .unwrap();
        store.active_esi_limiter_deadline().await.unwrap()
    }

    #[tokio::test]
    async fn fake_store_round_trips_through_the_trait_seam() {
        let store = FakeLimiterStore::default();
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let metadata = CacheMetadata {
            retry_after: Some(observed_at + ChronoDuration::seconds(30)),
            ..CacheMetadata::cached_for_seconds(0)
        };

        let deadline = record_and_read_deadline(&store, &metadata, observed_at).await;

        assert_eq!(deadline, Some(observed_at + ChronoDuration::seconds(30)));
        assert_eq!(store.record_calls.load(Ordering::SeqCst), 1);
    }
}
