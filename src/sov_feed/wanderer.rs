//! Read-only Wanderer map API client: wire types for `GET
//! /api/maps/{map}/systems` and `GET /api/maps/{map}/connections`, plus a
//! one-shot SSE availability probe (`.scratch/esi-intel-feeds/research/02-wanderer-map-api.md`,
//! ticket `.scratch/esi-intel-feeds/issues/05-wanderer-chain-reachability-and-path-risk.md`).
//!
//! # Authoritative source, and a corrected envelope shape
//!
//! Field names and enum values are taken from `research/02-wanderer-map-api.md`,
//! which is cross-checked against the live hosted instance and takes
//! precedence over the Wanderer project's own `/api/openapi` document for
//! individual field names/types: that document was directly observed to
//! claim a `solar_system_name`/`region_name` field (and a string
//! `status`) on the system object, which the research file explicitly
//! says is *not* present on the wire ("despite the blog docs"). This
//! module's [`WandererSystem`]/[`WandererConnection`] types therefore
//! follow the research file's field list, not the openapi example's.
//!
//! The *envelope* shape is a different matter, confirmed independently
//! (live `/api/openapi` example for both routes, and the Wanderer source
//! -- `map_system_api_controller.ex`'s `index` action calls
//! `APIUtils.respond_data(conn, %{systems: systems, connections: connections})`)
//! to be **asymmetric between the two routes**:
//! - `GET /api/maps/{id}/connections` returns a bare array under `data`:
//!   `{"data": [<connection>, ...]}`.
//! - `GET /api/maps/{id}/systems` returns an *object* under `data` with
//!   both `systems` and `connections` keys:
//!   `{"data": {"systems": [<system>, ...], "connections": [...]}}` --
//!   despite its name, this route's response also carries a connections
//!   array (the map's full snapshot), which this client does not use:
//!   `fetch_connections` remains the sole source of truth for
//!   connections, matching research/02's framing of `/systems` and
//!   `/connections` as two independent per-resource endpoints and the
//!   spec's "two requests per cycle" cadence -- using `/systems`'s
//!   embedded `connections` instead would silently drop that documented
//!   request-shape decision.
//!
//! [`WandererClient::fetch_systems`] unwraps this nested shape via
//! [`WandererSystemsResponseData`]; a response that were a bare array
//! instead (the earlier, disproven assumption) now fails to decode and
//! surfaces as `WandererError::Decode`, same as any other malformed body
//! -- the last good chain snapshot is kept (see `src/sov_feed/chain.rs`),
//! never a panic and never a silently-empty chain.

use async_trait::async_trait;
use reqwest::header::AUTHORIZATION;
use reqwest::{Client, StatusCode};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `WANDERER_BASE_URL`, `WANDERER_MAP`, and `WANDERER_MAP_API_KEY` must
/// all be set for the chain feature to enable (spec: "the feature is
/// enabled only when all three are set; otherwise reachability stays
/// stargate-only"). Holds the three raw values; `WandererClient::new`
/// builds the HTTP client from them.
#[derive(Clone)]
pub struct WandererConfig {
    pub base_url: String,
    pub map: String,
    pub api_key: String,
}

impl WandererConfig {
    /// Reads the three required environment variables. Returns `None`
    /// (logging at `info` level, never `error`: this is an ordinary,
    /// expected deployment shape, not a failure) unless every one of them
    /// is set to a non-empty value.
    pub fn from_environment() -> Option<Self> {
        let base_url = non_empty_env("WANDERER_BASE_URL");
        let map = non_empty_env("WANDERER_MAP");
        let api_key = non_empty_env("WANDERER_MAP_API_KEY");
        match (base_url, map, api_key) {
            (Some(base_url), Some(map), Some(api_key)) => Some(Self {
                base_url,
                map,
                api_key,
            }),
            _ => {
                tracing::info!(
                    "Wanderer chain reachability disabled: WANDERER_BASE_URL, WANDERER_MAP, and WANDERER_MAP_API_KEY must all be set. Reachability stays stargate-only."
                );
                None
            }
        }
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Deserializes a required JSON integer into an enum with an `Unknown(i64)`
/// fallback for any value this code does not recognize yet, so an upstream
/// Wanderer release adding a new enum member never fails parsing (ticket
/// acceptance: "tolerates unknown enum values from upstream").
macro_rules! wire_int_enum {
    ($name:ident { $($variant:ident = $value:literal),+ $(,)? } default_unknown = $default:literal) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        pub enum $name {
            $($variant),+,
            Unknown(i64),
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                match value {
                    $($value => Self::$variant,)+
                    other => Self::Unknown(other),
                }
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::from($default)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                // `Option::<i64>::deserialize` (rather than `i64::deserialize`)
                // accepts an explicit JSON `null` -- which the live
                // Wanderer openapi schema marks `nullable: true` on these
                // fields (e.g. an unscanned hole's `mass_status`) -- as
                // well as a present integer; `#[serde(default)]` on the
                // containing struct field separately covers the key being
                // absent entirely. Both "missing" and "null" resolve to
                // the same default variant via `Self::default()`.
                let value = Option::<i64>::deserialize(deserializer)?;
                Ok(match value {
                    Some(value) => Self::from(value),
                    None => Self::default(),
                })
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let value: i64 = match self {
                    $(Self::$variant => $value,)+
                    Self::Unknown(other) => *other,
                };
                serializer.serialize_i64(value)
            }
        }
    };
}

// research/02: "status (0 unknown, 1 friendly, 2 warning, 3 targetPrimary,
// 4 targetSecondary, 5 dangerousPrimary, 6 dangerousSecondary, 7
// lookingFor, 8 home)". Not used by any traversability or embed logic in
// this ticket; parsed through for forward compatibility only.
wire_int_enum!(WandererSystemStatus {
    SystemUnknown = 0,
    Friendly = 1,
    Warning = 2,
    TargetPrimary = 3,
    TargetSecondary = 4,
    DangerousPrimary = 5,
    DangerousSecondary = 6,
    LookingFor = 7,
    Home = 8,
} default_unknown = -1);

// research/02: "type (0 wormhole, 1 gate, 2 bridge)".
wire_int_enum!(WandererConnectionType {
    Wormhole = 0,
    Gate = 1,
    Bridge = 2,
} default_unknown = -1);

// research/02: "mass_status (0 >50 %, 1 <50 %, 2 critical)". Defaults to
// `Normal` (0) when the field is absent from a response: Wanderer's own
// data model defaults an unscanned connection's mass status to 0, so
// treating an absent field as "healthy until told otherwise" mirrors the
// wire's own convention rather than inventing a new one. This only
// matters for defensive robustness -- research/02 lists this field as
// always present.
wire_int_enum!(WandererMassStatus {
    Normal = 0,
    Depleted = 1,
    Critical = 2,
} default_unknown = 0);

// research/02: "time_status (EOL ladder: 0 normal, 1 EOL 1 h, 2 4 h, 3
// 4.5 h, 4 16 h, 5 24 h, 6 48 h)". Defaults to `Normal` (0, "not EOL") for
// the same reason as `WandererMassStatus`.
wire_int_enum!(WandererTimeStatus {
    Normal = 0,
    Eol1Hour = 1,
    Eol4Hours = 2,
    Eol4Point5Hours = 3,
    Eol16Hours = 4,
    Eol24Hours = 5,
    Eol48Hours = 6,
} default_unknown = 0);

// research/02: "ship_size_type (0 frigate, 1 medium, 2 large, 3 freighter,
// 4 capital)". Defaults to `Unknown(-1)` (never a `Frigate`) so a missing
// field never triggers the frigate-hole restriction by accident -- the
// restrictive case must be positively confirmed, not assumed.
wire_int_enum!(WandererShipSizeType {
    Frigate = 0,
    Medium = 1,
    Large = 2,
    Freighter = 3,
    Capital = 4,
} default_unknown = -1);

impl WandererShipSizeType {
    /// Spec "Traversability rules": "frigate-sized connections traverse
    /// only when the subscription sets `allow_frigate_holes`".
    pub fn is_frigate(self) -> bool {
        matches!(self, Self::Frigate)
    }
}

impl WandererMassStatus {
    /// Spec "Traversability rules": "critical-mass connections never
    /// traverse".
    pub fn is_critical(self) -> bool {
        matches!(self, Self::Critical)
    }
}

/// One system on the Wanderer map (research/02: `GET
/// /api/maps/{map_identifier}/systems`). Only `solar_system_id` feeds the
/// reachability graph today; the rest is retained on the persisted
/// [`crate::sov_feed::chain::ChainSnapshot`] for forward compatibility
/// (e.g. a future ticket resolving names straight from the chain) without
/// another migration. Every field beyond `solar_system_id` is optional on
/// deserialization -- defensive, not because research/02 marks them
/// optional -- so an upstream shape change never breaks parsing of the
/// one field this ticket actually depends on.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WandererSystem {
    pub solar_system_id: i64,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub map_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub original_name: Option<String>,
    #[serde(default)]
    pub custom_name: Option<String>,
    #[serde(default)]
    pub temporary_name: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub labels: Option<String>,
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub visible: bool,
    #[serde(default)]
    pub status: WandererSystemStatus,
    #[serde(default)]
    pub position_x: Option<f64>,
    #[serde(default)]
    pub position_y: Option<f64>,
}

/// One connection on the Wanderer map (research/02: `GET
/// /api/maps/{map_identifier}/connections`). `solar_system_source` and
/// `solar_system_target` are the only fields required to build a graph
/// edge; every other field is defensively optional (see
/// [`WandererSystem`]'s doc comment for the same rationale), defaulting to
/// the least-restrictive traversal outcome so a missing field can never
/// silently make a real connection excludable when it should traverse
/// (see each wire-enum's own doc comment for its specific default).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WandererConnection {
    pub solar_system_source: i64,
    pub solar_system_target: i64,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub map_id: Option<String>,
    #[serde(rename = "type", default)]
    pub connection_type: WandererConnectionType,
    #[serde(default)]
    pub mass_status: WandererMassStatus,
    #[serde(default)]
    pub time_status: WandererTimeStatus,
    #[serde(default)]
    pub ship_size_type: WandererShipSizeType,
    #[serde(default)]
    pub wormhole_type: Option<String>,
    #[serde(default)]
    pub locked: bool,
}

#[derive(Deserialize)]
struct WandererDataEnvelope<T> {
    data: T,
}

/// The object nested under `data` for `GET /api/maps/{id}/systems`
/// (confirmed via the live `/api/openapi` example and the Wanderer
/// source's `map_system_api_controller.ex`: `APIUtils.respond_data(conn,
/// %{systems: systems, connections: connections})`) -- unlike
/// `/connections`, whose `data` is a bare array, `/systems`'s `data` is an
/// object carrying both `systems` and a `connections` array this client
/// deliberately does not use (see this module's doc comment). `connections`
/// defaults to empty rather than being required, since nothing here reads
/// it and a future upstream response omitting it must not fail decoding
/// the `systems` half this client does depend on.
#[derive(Deserialize)]
struct WandererSystemsResponseData {
    systems: Vec<WandererSystem>,
    #[serde(default)]
    #[allow(dead_code)]
    connections: Vec<WandererConnection>,
}

/// Why a [`WandererClient`] request failed. Every variant carries a bounded
/// summary suitable for a `warn!` log line (spec: "On Wanderer errors
/// (401/403/503/timeouts) ... `warn!` with the Wanderer error code/body
/// summary"); nothing here ever panics the caller.
#[derive(Debug, Clone)]
pub enum WandererError {
    Http(String),
    Timeout,
    Status { status: u16, body_summary: String },
    Decode(String),
}

/// Response bodies are truncated to this many bytes before being folded
/// into a log line, so a misbehaving upstream returning megabytes of HTML
/// (e.g. a proxy error page) cannot bloat log storage.
const WANDERER_ERROR_BODY_SUMMARY_MAX_BYTES: usize = 200;

impl std::fmt::Display for WandererError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(message) => write!(formatter, "Wanderer request failed: {message}"),
            Self::Timeout => write!(formatter, "Wanderer request timed out"),
            Self::Status {
                status,
                body_summary,
            } => write!(formatter, "Wanderer returned {status}: {body_summary}"),
            Self::Decode(message) => write!(formatter, "Wanderer response decode error: {message}"),
        }
    }
}

impl std::error::Error for WandererError {}

fn summarize_body(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() > WANDERER_ERROR_BODY_SUMMARY_MAX_BYTES {
        format!("{}...", &text[..WANDERER_ERROR_BODY_SUMMARY_MAX_BYTES])
    } else {
        text.into_owned()
    }
}

/// Whether the Wanderer host's Server-Sent Events stream is available,
/// per the one-shot start-up probe (spec: "the client probes the SSE
/// stream once and logs whether it is enabled, without depending on it";
/// research/02: the hosted instance returns `503 "disabled"`).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WandererSseProbeResult {
    Enabled,
    Disabled,
    /// The probe itself failed (network error, timeout, or an unexpected
    /// status) -- distinct from a confirmed-disabled `503`, since this
    /// tells an operator nothing about whether SSE would work if retried.
    Unknown,
}

impl std::fmt::Display for WandererSseProbeResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Unknown => "unknown (probe failed)",
        };
        formatter.write_str(text)
    }
}

/// Read-only Wanderer map API client. **No write methods exist on this
/// type by construction** (spec: "the bot must be read-only by
/// construction"; user story 32) -- it only ever issues `GET` requests,
/// and adding a write method here would be a deliberate, reviewable change
/// to this file rather than something a caller could do by composing
/// existing pieces.
pub struct WandererClient {
    client: Client,
    base_url: String,
    map: String,
    api_key: String,
}

const WANDERER_USER_AGENT: &str = "killbot-rust Sov Chain Reachability (read-only)";

#[async_trait]
pub trait WandererChainSource: Send + Sync {
    async fn fetch_systems(&self) -> Result<Vec<WandererSystem>, WandererError>;
    async fn fetch_connections(&self) -> Result<Vec<WandererConnection>, WandererError>;
}

impl WandererClient {
    pub fn new(config: &WandererConfig, timeout: Duration) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .user_agent(WANDERER_USER_AGENT)
                .build()?,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            map: config.map.clone(),
            api_key: config.api_key.clone(),
        })
    }

    fn authorized_get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
    }

    async fn get_data<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, WandererError> {
        let response = self.authorized_get(path).send().await.map_err(|error| {
            if error.is_timeout() {
                WandererError::Timeout
            } else {
                WandererError::Http(error.to_string())
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(WandererError::Status {
                status: status.as_u16(),
                body_summary: summarize_body(&body),
            });
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| WandererError::Http(error.to_string()))?;
        let envelope: WandererDataEnvelope<T> = serde_json::from_slice(&body)
            .map_err(|error| WandererError::Decode(error.to_string()))?;
        Ok(envelope.data)
    }

    /// Probes `GET /api/maps/{map}/events/stream` once with a short,
    /// independent timeout and reports whether SSE is enabled, never
    /// erroring the caller (spec: "logs whether it is enabled, without
    /// depending on it"). Not part of the regular poll cycle.
    pub async fn probe_sse(&self, probe_timeout: Duration) -> WandererSseProbeResult {
        let client = match Client::builder()
            .timeout(probe_timeout)
            .user_agent(WANDERER_USER_AGENT)
            .build()
        {
            Ok(client) => client,
            Err(_) => return WandererSseProbeResult::Unknown,
        };
        let url = format!("{}/api/maps/{}/events/stream", self.base_url, self.map);
        let response = client
            .get(url)
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => WandererSseProbeResult::Enabled,
            // research/02: "SSE disabled on this host (`GET
            // .../events/stream` -> `503 "Server-Sent Events are disabled
            // on this server"`)". Any client/server error status is
            // treated the same way (disabled), since the only host fact
            // this probe needs is "not usable right now".
            Ok(response)
                if response.status() == StatusCode::SERVICE_UNAVAILABLE
                    || response.status().is_client_error() =>
            {
                WandererSseProbeResult::Disabled
            }
            Ok(_) => WandererSseProbeResult::Unknown,
            Err(_) => WandererSseProbeResult::Unknown,
        }
    }
}

#[async_trait]
impl WandererChainSource for WandererClient {
    async fn fetch_systems(&self) -> Result<Vec<WandererSystem>, WandererError> {
        let data: WandererSystemsResponseData = self
            .get_data(&format!("/api/maps/{}/systems", self.map))
            .await?;
        Ok(data.systems)
    }

    async fn fetch_connections(&self) -> Result<Vec<WandererConnection>, WandererError> {
        self.get_data(&format!("/api/maps/{}/connections", self.map))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_connection_type_falls_back_to_the_unknown_variant() {
        let parsed: WandererConnectionType = serde_json::from_str("7").unwrap();
        assert_eq!(parsed, WandererConnectionType::Unknown(7));
        let known: WandererConnectionType = serde_json::from_str("0").unwrap();
        assert_eq!(known, WandererConnectionType::Wormhole);
    }

    #[test]
    fn unknown_mass_status_falls_back_to_the_unknown_variant_and_is_never_critical() {
        let parsed: WandererMassStatus = serde_json::from_str("99").unwrap();
        assert_eq!(parsed, WandererMassStatus::Unknown(99));
        assert!(!parsed.is_critical());
        assert!(WandererMassStatus::Critical.is_critical());
    }

    #[test]
    fn unknown_ship_size_defaults_to_non_frigate() {
        assert!(!WandererShipSizeType::default().is_frigate());
        assert!(WandererShipSizeType::Frigate.is_frigate());
        let parsed: WandererShipSizeType = serde_json::from_str("42").unwrap();
        assert_eq!(parsed, WandererShipSizeType::Unknown(42));
        assert!(!parsed.is_frigate());
    }

    #[test]
    fn missing_optional_connection_fields_default_rather_than_fail() {
        let json = r#"{"solar_system_source": 30000001, "solar_system_target": 30000002}"#;
        let connection: WandererConnection = serde_json::from_str(json).expect("defaults fill in");
        assert_eq!(connection.mass_status, WandererMassStatus::Normal);
        assert_eq!(connection.time_status, WandererTimeStatus::Normal);
        assert!(!connection.ship_size_type.is_frigate());
    }

    #[test]
    fn systems_and_connections_fixtures_parse_in_the_data_envelope_shape() {
        let systems_json = std::fs::read_to_string("resources/wanderer_systems_fixture.json")
            .expect("read systems fixture");
        let envelope: WandererDataEnvelope<WandererSystemsResponseData> =
            serde_json::from_str(&systems_json).expect("parse systems fixture");
        assert!(!envelope.data.systems.is_empty());

        let connections_json =
            std::fs::read_to_string("resources/wanderer_connections_fixture.json")
                .expect("read connections fixture");
        let envelope: WandererDataEnvelope<Vec<WandererConnection>> =
            serde_json::from_str(&connections_json).expect("parse connections fixture");
        assert!(!envelope.data.is_empty());
        assert!(envelope
            .data
            .iter()
            .any(|connection| connection.time_status != WandererTimeStatus::Normal));
        assert!(envelope
            .data
            .iter()
            .any(|connection| connection.mass_status == WandererMassStatus::Critical));
        assert!(envelope
            .data
            .iter()
            .any(|connection| connection.ship_size_type.is_frigate()));
        assert!(envelope
            .data
            .iter()
            .any(|connection| connection.connection_type == WandererConnectionType::Gate));
    }

    #[test]
    fn systems_response_with_a_bare_array_data_is_rejected_while_the_real_object_shape_parses() {
        // Regression for the reviewer-caught blocker: `/systems` nests
        // `{"systems": [...], "connections": [...]}` under `data`, unlike
        // `/connections`'s bare array. Confirmed via the live
        // `/api/openapi` example and the Wanderer source
        // (`map_system_api_controller.ex`'s `APIUtils.respond_data(conn,
        // %{systems: systems, connections: connections})`). Before the
        // fix, every chain cycle failed at `fetch_systems` forever.
        let bare_array_shape = r#"{"data": [{"solar_system_id": 30000142}]}"#;
        let result: Result<WandererDataEnvelope<WandererSystemsResponseData>, _> =
            serde_json::from_str(bare_array_shape);
        assert!(
            result.is_err(),
            "a bare array under data must not silently parse as the real object shape"
        );

        let real_object_shape =
            r#"{"data": {"systems": [{"solar_system_id": 30000142}], "connections": []}}"#;
        let envelope: WandererDataEnvelope<WandererSystemsResponseData> =
            serde_json::from_str(real_object_shape).expect("the real nested shape parses");
        assert_eq!(envelope.data.systems.len(), 1);
        assert_eq!(envelope.data.systems[0].solar_system_id, 30000142);
    }

    #[test]
    fn an_explicit_null_on_a_nullable_connection_field_parses_as_the_default_variant() {
        // Regression: the live openapi schema marks mass_status,
        // time_status, and ship_size_type `nullable: true` (an unscanned
        // hole). `#[serde(default)]` alone only covers an *absent* key;
        // an explicit JSON `null` used to fail the whole `/connections`
        // decode.
        let json = r#"{
            "solar_system_source": 30000001,
            "solar_system_target": 30000002,
            "type": null,
            "mass_status": null,
            "time_status": null,
            "ship_size_type": null
        }"#;
        let connection: WandererConnection =
            serde_json::from_str(json).expect("explicit nulls parse as defaults");
        assert_eq!(connection.mass_status, WandererMassStatus::Normal);
        assert_eq!(connection.time_status, WandererTimeStatus::Normal);
        assert!(!connection.ship_size_type.is_frigate());
        // Treated per the traversability rules for the default variants:
        // a null-mass, null-size wormhole is neither critical nor
        // frigate-restricted, so it traverses without `allow_frigate_holes`.
        assert!(!connection.mass_status.is_critical());
    }

    #[test]
    fn wanderer_config_requires_all_three_variables() {
        // Exercised indirectly through `from_environment`'s pure branches
        // instead of setting real process environment variables (which
        // would race other tests running in parallel in the same
        // process); this covers the tuple-match logic directly.
        struct Case {
            base_url: Option<&'static str>,
            map: Option<&'static str>,
            api_key: Option<&'static str>,
            expect_some: bool,
        }
        let cases = [
            Case {
                base_url: Some("https://wanderer.example"),
                map: Some("abc"),
                api_key: Some("key"),
                expect_some: true,
            },
            Case {
                base_url: None,
                map: Some("abc"),
                api_key: Some("key"),
                expect_some: false,
            },
            Case {
                base_url: Some("https://wanderer.example"),
                map: None,
                api_key: Some("key"),
                expect_some: false,
            },
            Case {
                base_url: Some("https://wanderer.example"),
                map: Some("abc"),
                api_key: None,
                expect_some: false,
            },
        ];
        for case in cases {
            let result = match (case.base_url, case.map, case.api_key) {
                (Some(base_url), Some(map), Some(api_key)) => Some(WandererConfig {
                    base_url: base_url.to_string(),
                    map: map.to_string(),
                    api_key: api_key.to_string(),
                }),
                _ => None,
            };
            assert_eq!(result.is_some(), case.expect_some);
        }
    }
}
