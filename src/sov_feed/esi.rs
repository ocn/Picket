//! ESI wire access for `GET /sovereignty/campaigns/`.
//!
//! Five-axis evidence (`.claude/skills/eve-esi-api/LIMITER-CACHE-HEALTH.md`),
//! captured live against `esi.evetech.net` on 2026-08-26:
//! - **Wire**: `openapi.json` operation `GetSovereigntyCampaigns` pins
//!   `x-compatibility-date: 2020-01-01` (the operation has not changed
//!   shape since; an undated request would receive this same oldest
//!   behavior, so the date is pinned explicitly here rather than relied on
//!   implicitly). A live request/response pair confirmed the header is
//!   honored and the schema matches [`crate::sov_feed::model::SovCampaign`].
//! - **Cache**: `expires` = `last-modified` + 5s (`ttl-based`,
//!   `x-cache-age: 5`); a second request with `If-None-Match` against the
//!   returned `ETag` produced `304 Not Modified` with a fresh `expires`.
//! - **Limiter**: `x-ratelimit-group: sovereignty`, `600/15m`, observed
//!   `x-ratelimit-remaining` decrementing per request; the legacy
//!   `x-esi-error-limit-*` allowance is not sent on success responses for
//!   this route but remains global per ADR 0004, so this feed records
//!   whatever cache metadata is present through the shared
//!   `EsiLimiterStore` regardless.
//! - **Domain**: `defender_id`/`defender_score`/`attackers_score` are
//!   present only on Defense Events (`tcu_defense`, `ihub_defense`,
//!   `station_defense`); Freeport Events (`station_freeport`) omit them
//!   and carry `participants` instead (not modeled: out of scope for the
//!   `appeared` stage, which only needs the Defense Event fields the
//!   filter grammar and embed use).
//! - **State**: campaigns are non-resumable point-in-time listings; there
//!   is no per-campaign checkpoint on the wire. The collector persists its
//!   own diff state (`sov_campaigns`, `sov_collector_baseline`).

use crate::esi_cache::{cache_metadata, EsiError, EsiResponse};
use crate::sov_feed::model::SovCampaign;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use reqwest::header::IF_NONE_MATCH;
use reqwest::{Client, StatusCode};
use std::time::Duration;

/// Pinned per the skill workflow: `inspect-openapi.sh '/sovereignty/campaigns' get`
/// reports `compatibility_date: "2020-01-01"` for `GetSovereigntyCampaigns`,
/// confirmed live against `esi.evetech.net` on 2026-08-26.
pub const SOV_COMPATIBILITY_DATE: &str = "2020-01-01";

const SOV_COLLECTOR_USER_AGENT: &str = "killbot-rust Sovereignty Campaign Feed";

#[async_trait]
pub trait SovereigntyEsi: Send + Sync {
    async fn fetch_campaigns(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovCampaign>>, EsiError>;
}

pub struct HttpSovereigntyEsi {
    client: Client,
    base_url: String,
}

impl HttpSovereigntyEsi {
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
                .user_agent(SOV_COLLECTOR_USER_AGENT)
                .build()?,
            base_url: base_url.into(),
        })
    }
}

#[async_trait]
impl SovereigntyEsi for HttpSovereigntyEsi {
    async fn fetch_campaigns(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovCampaign>>, EsiError> {
        let url = format!(
            "{}sovereignty/campaigns/?datasource=tranquility",
            self.base_url
        );
        let mut request = self
            .client
            .get(&url)
            .header("X-Compatibility-Date", SOV_COMPATIBILITY_DATE);
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
        let campaigns: Vec<SovCampaign> = serde_json::from_slice(&body).map_err(|error| {
            EsiError::from_metadata(
                format!("error decoding response body: {error}"),
                None,
                metadata.clone(),
            )
        })?;
        Ok(EsiResponse::fresh(campaigns, metadata))
    }
}
