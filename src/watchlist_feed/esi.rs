//! ESI wire access for the watchlist collector's hourly corporation-list
//! poll, `GET /alliances/{alliance_id}/corporations/`.
//!
//! Five-axis evidence (`.claude/skills/eve-esi-api/LIMITER-CACHE-HEALTH.md`),
//! captured live against `esi.evetech.net` on 2026-08-28:
//! - **Wire**: OpenAPI operation `GET /alliances/{alliance_id}/corporations`
//!   pins `x-compatibility-date: 2020-01-01`; the response schema
//!   (`AlliancesAllianceIdCorporationsGet`) is a bare JSON array of int64
//!   corporation IDs. A live request for alliance 99004901 (Snuffed Out)
//!   returned 17 IDs and echoed `x-compatibility-date: 2020-01-01`. All
//!   public, no scopes.
//! - **Cache**: observed `cache-control: public` with `expires` =
//!   `last-modified` + 3600s (one hour) and a strong `ETag`; a conditional
//!   `If-None-Match` re-request short-circuits with `304 Not Modified` and a
//!   fresh `expires`. The one-hour freshness lines up with this feed's
//!   hourly cadence, so a conditional re-poll every hour is almost always a
//!   304. `cache_metadata_at` derives `expires_at` from the `expires`
//!   header here (there is no `max-age` directive on this route).
//! - **Limiter**: this route carries the legacy `x-esi-error-limit-remain`
//!   / `x-esi-error-limit-reset` allowance but *no* `x-ratelimit-group`
//!   bucket header (unlike `sovereignty/*`). Per ADR 0004 the legacy
//!   allowance is global-per-application, so the collector coordinates
//!   through the shared [`crate::esi_cache::EsiLimiterStore`] row and
//!   records whatever limiter metadata is present after *every* response,
//!   including 304s and errors.
//! - **Domain**: the array lists exactly the alliance's current member
//!   corporations; a corporation that leaves simply disappears from the
//!   listing next poll (there is no per-corp "left" flag on the wire), which
//!   is why the collector diffs against its own persisted
//!   `watchlist_alliance_corps` snapshot. Corporation names/tickers are not
//!   on this route; they are resolved separately at prepare time via
//!   `GET /corporations/{id}/` through
//!   [`crate::watchlist_feed::model::WatchlistEntityResolver`].
//! - **State**: a non-resumable point-in-time listing with no per-corp
//!   checkpoint on the wire; the collector persists its own first/last-seen
//!   and per-alliance baseline state.
//!
//! Corporation and alliance identity resolution (`GET /corporations/{id}/`,
//! `GET /alliances/{id}/`, both `x-compatibility-date: 2020-01-01`,
//! `cache-control: max-age=3600`) and name resolution (`POST /universe/ids/`,
//! `x-compatibility-date: 2020-01-01`) are performed through the killfeed's
//! shared `EsiClient` rather than this trait; see
//! `crate::esi::EsiClient::resolve_watchlist_entity` and the Discord-side
//! resolver in `src/discord_bot.rs`.

use crate::esi_cache::{cache_metadata, EsiError, EsiResponse};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use reqwest::header::IF_NONE_MATCH;
use reqwest::{Client, StatusCode};
use std::time::Duration;

/// Pinned per the skill workflow: `inspect-openapi.sh
/// '/alliances/{alliance_id}/corporations' get` reports
/// `x-compatibility-date: "2020-01-01"`, confirmed live against
/// `esi.evetech.net` on 2026-08-28.
pub const WATCHLIST_COMPATIBILITY_DATE: &str = "2020-01-01";

const WATCHLIST_COLLECTOR_USER_AGENT: &str = "killbot-rust Watchlist Feed";

/// Fetches a watched alliance's current member corporation list with a
/// conditional request. The returned [`EsiResponse`] carries the parsed
/// corporation ID list (fresh) or `not_modified` (304), plus the cache and
/// limiter metadata the collector records against the shared limiter row.
#[async_trait]
pub trait WatchlistEsi: Send + Sync {
    async fn fetch_alliance_corporations(
        &self,
        alliance_id: i64,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<i64>>, EsiError>;
}

pub struct HttpWatchlistEsi {
    client: Client,
    base_url: String,
}

impl HttpWatchlistEsi {
    pub fn new(timeout: Duration) -> Result<Self, reqwest::Error> {
        Self::with_base_url("https://esi.evetech.net/latest/", timeout)
    }

    pub fn with_base_url(
        base_url: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .user_agent(WATCHLIST_COLLECTOR_USER_AGENT)
                .build()?,
            base_url: base_url.into(),
        })
    }
}

#[async_trait]
impl WatchlistEsi for HttpWatchlistEsi {
    async fn fetch_alliance_corporations(
        &self,
        alliance_id: i64,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        let url = format!(
            "{}alliances/{alliance_id}/corporations/?datasource=tranquility",
            self.base_url
        );
        let mut request = self
            .client
            .get(&url)
            .header("X-Compatibility-Date", WATCHLIST_COMPATIBILITY_DATE);
        if let Some(etag) = &etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        let metadata = cache_metadata(response.headers());
        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(EsiResponse::not_modified(metadata));
        }
        if !response.status().is_success() {
            let mut metadata = metadata;
            metadata.retry_after = metadata.retry_after.or_else(|| {
                if matches!(response.status(), StatusCode::TOO_MANY_REQUESTS)
                    || response.status().as_u16() == 420
                {
                    metadata
                        .error_limit_reset
                        .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
                } else {
                    None
                }
            });
            return Err(EsiError::from_metadata(
                format!("ESI returned {}", response.status()),
                Some(response.status()),
                metadata,
            ));
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| EsiError::from_metadata(error.to_string(), None, metadata.clone()))?;
        let corporation_ids: Vec<i64> = serde_json::from_slice(&body).map_err(|error| {
            EsiError::from_metadata(
                format!("error decoding response body: {error}"),
                None,
                metadata.clone(),
            )
        })?;
        Ok(EsiResponse::fresh(corporation_ids, metadata))
    }
}
