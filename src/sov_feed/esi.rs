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
use crate::sov_feed::model::{SovCampaign, SovMapEntry, SovStructure};
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

/// Pinned per the skill workflow: `inspect-openapi.sh '/sovereignty/structures' get`
/// and `inspect-openapi.sh '/sovereignty/map' get` both report
/// `compatibility_date: "2020-01-01"` -- the same date as
/// [`SOV_COMPATIBILITY_DATE`] above, so both routes share that constant
/// rather than needing their own.
///
/// Five-axis evidence (`.claude/skills/eve-esi-api/LIMITER-CACHE-HEALTH.md`),
/// captured live against `esi.evetech.net` on 2026-08-27:
///
/// **`GET /sovereignty/structures/`**
/// - **Wire**: `operationId: GetSovereigntyStructures`, `x-compatibility-date:
///   2020-01-01`. A live request/response pair confirmed the schema
///   matches [`SovStructure`] (`structure_id`, `structure_type_id`,
///   `alliance_id`, `solar_system_id`, `vulnerability_occupancy_level`,
///   `vulnerable_start_time`, `vulnerable_end_time`); 2708 structures
///   returned, all `structure_type_id: 32458` ("Sovereignty Hub" per
///   `GET /universe/types/32458/`) -- see
///   [`crate::sov_feed::model::SOV_HUB_STRUCTURE_TYPE_IDS`] for the full
///   evidence note and the hub-only filtering this collector applies.
/// - **Cache**: observed header `cache-control: public, max-age=300,
///   must-revalidate, stale-if-error=900` (the OpenAPI document's `cache.age:
///   120` is stale relative to this live header; the live wire value is
///   authoritative and matches the spec/research catalog's "300 s"
///   exactly). `expires` = `last-modified` + 300s. An `ETag` was present
///   and honored via `If-None-Match` (not separately re-verified for this
///   route beyond the shared conditional-request code path already proven
///   for `/sovereignty/campaigns/`).
/// - **Limiter**: `x-ratelimit-group: sovereignty`, `600/15m` -- the same
///   bucket and shared `EsiLimiterStore` row as campaigns and the map
///   route below.
/// - **Domain**: `alliance_id`, `vulnerability_occupancy_level`,
///   `vulnerable_start_time`, `vulnerable_end_time` are all optional on
///   the wire (a hub without an owner or an active vulnerability window
///   omits them); modeled as `Option` fields with `#[serde(default)]`.
/// - **State**: like campaigns, a non-resumable point-in-time listing with
///   no per-structure checkpoint; the collector persists its own
///   first/last-seen and baseline state (`sov_structures`,
///   `sov_structures_baseline`).
///
/// **`GET /sovereignty/map/`**
/// - **Wire**: `operationId: GetSovereigntyMap`, `x-compatibility-date:
///   2020-01-01`. A live request/response pair confirmed the schema
///   matches [`SovMapEntry`] (`system_id`, and any subset of
///   `alliance_id`/`corporation_id`/`faction_id`); 8490 systems returned.
/// - **Cache**: observed header `cache-control: public, max-age=3600,
///   must-revalidate, stale-if-error=900`, matching the OpenAPI document's
///   `cache.age: 3600` and the spec/research catalog exactly. `expires` =
///   `last-modified` + 3600s.
/// - **Limiter**: `x-ratelimit-group: sovereignty`, `600/15m`, the same
///   shared bucket.
/// - **Domain**: `alliance_id`/`corporation_id`/`faction_id` are each
///   optional and may appear in any combination (player sov: alliance +
///   corporation; FW/NPC space: faction alone); modeled as `Option` fields.
/// - **State**: a full point-in-time listing with no per-system
///   checkpoint; the collector replaces `sov_map` wholesale each
///   successful poll (`SovStore::replace_map`), since only "current owner"
///   is needed, not history.
#[async_trait]
pub trait SovereigntyEsi: Send + Sync {
    async fn fetch_campaigns(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovCampaign>>, EsiError>;

    async fn fetch_structures(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovStructure>>, EsiError>;

    async fn fetch_map(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovMapEntry>>, EsiError>;
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

impl HttpSovereigntyEsi {
    /// Shared conditional-request/decode path for every `sovereignty/*`
    /// listing route: identical wire behaviour (compatibility date header,
    /// `If-None-Match`, 304 short-circuit, error-limit-aware `retry_after`
    /// derivation, JSON body decode) across `campaigns`, `structures`, and
    /// `map` -- only the path segment and the deserialized element type
    /// differ.
    async fn fetch_listing<T: serde::de::DeserializeOwned>(
        &self,
        path_segment: &str,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<T>>, EsiError> {
        let url = format!(
            "{}sovereignty/{path_segment}/?datasource=tranquility",
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
        let values: Vec<T> = serde_json::from_slice(&body).map_err(|error| {
            EsiError::from_metadata(
                format!("error decoding response body: {error}"),
                None,
                metadata.clone(),
            )
        })?;
        Ok(EsiResponse::fresh(values, metadata))
    }
}

#[async_trait]
impl SovereigntyEsi for HttpSovereigntyEsi {
    async fn fetch_campaigns(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovCampaign>>, EsiError> {
        self.fetch_listing("campaigns", etag).await
    }

    async fn fetch_structures(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovStructure>>, EsiError> {
        self.fetch_listing("structures", etag).await
    }

    async fn fetch_map(
        &self,
        etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovMapEntry>>, EsiError> {
        self.fetch_listing("map", etag).await
    }
}
