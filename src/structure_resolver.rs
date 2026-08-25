use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tokio::sync::Mutex;
use url::Url;

use crate::contract_intelligence::{cache_metadata_at, esi_limiter_deadline_at, CacheMetadata};

pub const STRUCTURE_RESOLVER_SCOPE: &str = "esi-universe.read_structures.v1";
const EVE_SSO_AUTHORIZATION_URL: &str = "https://login.eveonline.com/v2/oauth/authorize/";
const EVE_SSO_ISSUERS: &[&str] = &[
    "https://login.eveonline.com",
    "https://login.eveonline.com/",
    "login.eveonline.com",
];
const EVE_SSO_METADATA_URL: &str =
    "https://login.eveonline.com/.well-known/oauth-authorization-server";
const EVE_ESI_BASE_URL: &str = "https://esi.evetech.net/latest/";
const EVE_SSO_TOKEN_URL: &str = "https://login.eveonline.com/v2/oauth/token";
const STRUCTURE_RESOLVER_ENVIRONMENT_VARIABLES: &[&str] = &[
    "STRUCTURE_RESOLVER_ENABLED",
    "STRUCTURE_RESOLVER_CHARACTER_ID",
    "STRUCTURE_RESOLVER_CREDENTIAL_REVISION",
    "STRUCTURE_RESOLVER_REFRESH_TOKEN",
    "STRUCTURE_RESOLVER_CLIENT_ID",
    "STRUCTURE_RESOLVER_CLIENT_SECRET",
    "EVE_CLIENT_ID",
    "EVE_CLIENT_SECRET",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructureResolverProvisioningError {
    message: &'static str,
}

impl StructureResolverProvisioningError {
    fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl Display for StructureResolverProvisioningError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for StructureResolverProvisioningError {}

pub fn build_structure_resolver_authorization_url(
    client_id: &str,
    redirect_uri: &str,
    state: &str,
) -> Result<String, StructureResolverProvisioningError> {
    if client_id.trim().is_empty() || state.trim().is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver authorization request is incomplete",
        ));
    }
    validate_structure_resolver_redirect_uri(redirect_uri)?;
    let mut authorization_url = Url::parse(EVE_SSO_AUTHORIZATION_URL).map_err(|_| {
        StructureResolverProvisioningError::new(
            "structure resolver authorization endpoint is invalid",
        )
    })?;
    authorization_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", STRUCTURE_RESOLVER_SCOPE)
        .append_pair("state", state);
    Ok(authorization_url.into())
}

pub fn parse_structure_resolver_callback(
    callback_uri: &str,
    redirect_uri: &str,
    expected_state: &str,
) -> Result<String, StructureResolverProvisioningError> {
    validate_structure_resolver_redirect_uri(redirect_uri)?;
    if expected_state.trim().is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver authorization state is missing",
        ));
    }
    let mut callback = Url::parse(callback_uri).map_err(|_| {
        StructureResolverProvisioningError::new("structure resolver callback is invalid")
    })?;
    let callback_query = callback.query_pairs().into_owned().collect::<Vec<_>>();
    callback.set_query(None);
    callback.set_fragment(None);
    if callback.as_str() != redirect_uri {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback does not match the registered redirect URI",
        ));
    }
    let states = callback_query
        .iter()
        .filter_map(|(key, value)| (key == "state").then_some(value.as_str()))
        .collect::<Vec<_>>();
    let [state] = states.as_slice() else {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback state is invalid",
        ));
    };
    if state.is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback state is missing",
        ));
    }
    if *state != expected_state {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback state does not match",
        ));
    }
    let codes = callback_query
        .iter()
        .filter_map(|(key, value)| (key == "code").then_some(value.as_str()))
        .collect::<Vec<_>>();
    let [code] = codes.as_slice() else {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback authorization code is invalid",
        ));
    };
    if code.trim().is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver callback authorization code is missing",
        ));
    }
    Ok((*code).to_string())
}

fn validate_structure_resolver_redirect_uri(
    redirect_uri: &str,
) -> Result<(), StructureResolverProvisioningError> {
    let redirect = Url::parse(redirect_uri).map_err(|_| {
        StructureResolverProvisioningError::new("structure resolver redirect URI is invalid")
    })?;
    if !matches!(redirect.scheme(), "http" | "https")
        || redirect.host_str().is_none()
        || redirect.query().is_some()
        || redirect.fragment().is_some()
    {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver redirect URI is invalid",
        ));
    }
    Ok(())
}

pub trait StructureResolverClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemStructureResolverClock;

impl StructureResolverClock for SystemStructureResolverClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct StructureResolverConfig {
    enabled: bool,
    character_id: Option<i64>,
    credential_revision: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
}

impl StructureResolverConfig {
    pub fn from_environment() -> Result<Self, StructureResolverConfigError> {
        Self::from_environment_values(|name| std::env::var_os(name))
    }

    #[doc(hidden)]
    pub fn from_environment_values(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, StructureResolverConfigError> {
        let mut settings = BTreeMap::new();
        for name in STRUCTURE_RESOLVER_ENVIRONMENT_VARIABLES {
            if let Some(value) = lookup(name) {
                let value = value.into_string().map_err(|_| {
                    StructureResolverConfigError::new(format!("{name} must be valid Unicode"))
                })?;
                settings.insert((*name).to_string(), value);
            }
        }
        Self::from_settings(settings)
    }

    pub fn from_settings<K, V>(
        settings: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, StructureResolverConfigError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let settings = settings
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
            .collect::<BTreeMap<_, _>>();
        let enabled = match settings.get("STRUCTURE_RESOLVER_ENABLED") {
            None => false,
            Some(value) => value.parse::<bool>().map_err(|_| {
                StructureResolverConfigError::new(
                    "STRUCTURE_RESOLVER_ENABLED must be true or false",
                )
            })?,
        };
        if !enabled {
            return Ok(Self {
                enabled,
                character_id: None,
                credential_revision: "1".to_string(),
                client_id: None,
                client_secret: None,
                refresh_token: None,
            });
        }
        let character_id = required_positive_i64(&settings, "STRUCTURE_RESOLVER_CHARACTER_ID")?;
        let (client_id, client_secret) = resolver_client_credentials(&settings)?;
        let refresh_token = required_setting(&settings, "STRUCTURE_RESOLVER_REFRESH_TOKEN")?;
        let credential_revision = settings
            .get("STRUCTURE_RESOLVER_CREDENTIAL_REVISION")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| "1".to_string());
        Ok(Self {
            enabled,
            character_id: Some(character_id),
            credential_revision,
            client_id: Some(client_id),
            client_secret: Some(client_secret),
            refresh_token: Some(refresh_token),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn character_id(&self) -> Option<i64> {
        self.character_id
    }

    pub fn required_scope(&self) -> &'static str {
        STRUCTURE_RESOLVER_SCOPE
    }

    pub fn runtime_status(&self) -> StructureResolverRuntimeStatus {
        if self.enabled {
            StructureResolverRuntimeStatus::ready(self.credential_revision.clone())
        } else {
            StructureResolverRuntimeStatus::disabled()
        }
    }

    pub(crate) fn credential_revision(&self) -> &str {
        &self.credential_revision
    }

    pub(crate) fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub(crate) fn client_secret(&self) -> Option<&str> {
        self.client_secret.as_deref()
    }

    pub(crate) fn refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }
}

pub struct StructureResolverProvisioningClient {
    client_id: String,
    client_secret: String,
}

impl StructureResolverProvisioningClient {
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Result<Self, StructureResolverProvisioningError> {
        let client_id = client_id.into();
        let client_secret = client_secret.into();
        if client_id.trim().is_empty() || client_secret.trim().is_empty() {
            return Err(StructureResolverProvisioningError::new(
                "structure resolver client credentials are incomplete",
            ));
        }
        Ok(Self {
            client_id,
            client_secret,
        })
    }
}

pub struct StructureResolverProvisioningAuthorization {
    access_token: String,
    refresh_token: String,
    character_id: i64,
}

impl StructureResolverProvisioningAuthorization {
    #[doc(hidden)]
    pub fn from_parts(access_token: String, refresh_token: String, character_id: i64) -> Self {
        Self {
            access_token,
            refresh_token,
            character_id,
        }
    }

    pub fn character_id(&self) -> i64 {
        self.character_id
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn refresh_token(&self) -> &str {
        &self.refresh_token
    }
}

pub struct StructureResolverProvisioningEndpoints {
    esi_base_url: String,
    token_url: String,
    metadata_url: String,
    require_eve_sso_binding: bool,
}

impl StructureResolverProvisioningEndpoints {
    pub fn official() -> Self {
        Self {
            esi_base_url: EVE_ESI_BASE_URL.to_string(),
            token_url: EVE_SSO_TOKEN_URL.to_string(),
            metadata_url: EVE_SSO_METADATA_URL.to_string(),
            require_eve_sso_binding: true,
        }
    }

    #[doc(hidden)]
    pub fn for_test(
        esi_base_url: impl Into<String>,
        token_url: impl Into<String>,
        metadata_url: impl Into<String>,
    ) -> Self {
        Self {
            esi_base_url: esi_base_url.into(),
            token_url: token_url.into(),
            metadata_url: metadata_url.into(),
            require_eve_sso_binding: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructureProbeCoverage {
    Full,
    Partial,
    Denied,
    Indeterminate,
}

pub struct StructureProbeReport {
    coverage: StructureProbeCoverage,
    successful: usize,
    denied: usize,
    indeterminate: usize,
}

impl StructureProbeReport {
    pub fn coverage(&self) -> StructureProbeCoverage {
        self.coverage
    }

    pub fn successful(&self) -> usize {
        self.successful
    }

    pub fn denied(&self) -> usize {
        self.denied
    }

    pub fn indeterminate(&self) -> usize {
        self.indeterminate
    }
}

pub async fn exchange_structure_resolver_authorization(
    client: StructureResolverProvisioningClient,
    authorization_code: &str,
    timeout: Duration,
) -> Result<StructureResolverProvisioningAuthorization, StructureResolverProvisioningError> {
    exchange_structure_resolver_authorization_with_endpoints(
        client,
        authorization_code,
        StructureResolverProvisioningEndpoints::official(),
        timeout,
    )
    .await
}

#[doc(hidden)]
pub async fn exchange_structure_resolver_authorization_with_endpoints(
    client: StructureResolverProvisioningClient,
    authorization_code: &str,
    endpoints: StructureResolverProvisioningEndpoints,
    timeout: Duration,
) -> Result<StructureResolverProvisioningAuthorization, StructureResolverProvisioningError> {
    if authorization_code.trim().is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver authorization code is missing",
        ));
    }
    let http_client = Client::builder().timeout(timeout).build().map_err(|_| {
        StructureResolverProvisioningError::new("structure resolver provisioning HTTP setup failed")
    })?;
    let response = http_client
        .post(&endpoints.token_url)
        .basic_auth(&client.client_id, Some(&client.client_secret))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", authorization_code),
        ])
        .send()
        .await
        .map_err(|_| {
            StructureResolverProvisioningError::new(
                "structure resolver authorization exchange failed",
            )
        })?;
    if !response.status().is_success() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver authorization exchange was rejected",
        ));
    }
    let response = response.json::<TokenRefreshResponse>().await.map_err(|_| {
        StructureResolverProvisioningError::new(
            "structure resolver authorization exchange returned an invalid response",
        )
    })?;
    if response.access_token.trim().is_empty() {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver authorization exchange returned an invalid response",
        ));
    }
    let refresh_token = response
        .refresh_token
        .filter(|refresh_token| !refresh_token.trim().is_empty())
        .ok_or_else(|| {
            StructureResolverProvisioningError::new(
                "structure resolver authorization exchange did not return a refresh token",
            )
        })?;
    let validation_config = StructureResolverConfig {
        enabled: true,
        character_id: Some(1),
        credential_revision: "provisioning".to_string(),
        client_id: Some(client.client_id.clone()),
        client_secret: Some(client.client_secret.clone()),
        refresh_token: Some(refresh_token.clone()),
    };
    let validator = AuthenticatedStructureResolver::with_endpoints_and_sso_binding(
        validation_config,
        &endpoints.esi_base_url,
        &endpoints.token_url,
        &endpoints.metadata_url,
        timeout,
        endpoints.require_eve_sso_binding,
    )
    .map_err(|_| {
        StructureResolverProvisioningError::new("structure resolver provisioning HTTP setup failed")
    })?;
    let validated = validator
        .validate_eve_access_token_for_client(&response.access_token, &client.client_id)
        .await
        .map_err(|_| {
            StructureResolverProvisioningError::new(
                "structure resolver authorization token validation failed",
            )
        })?;
    Ok(StructureResolverProvisioningAuthorization {
        access_token: response.access_token,
        refresh_token,
        character_id: validated.character_id,
    })
}

pub async fn probe_structure_resolver_coverage(
    authorization: &StructureResolverProvisioningAuthorization,
    structure_ids: &[i64],
    endpoints: &StructureResolverProvisioningEndpoints,
    timeout: Duration,
) -> Result<StructureProbeReport, StructureResolverProvisioningError> {
    if structure_ids.is_empty() || structure_ids.iter().any(|structure_id| *structure_id <= 0) {
        return Err(StructureResolverProvisioningError::new(
            "structure resolver probes must contain positive structure identifiers",
        ));
    }
    let http_client = Client::builder().timeout(timeout).build().map_err(|_| {
        StructureResolverProvisioningError::new("structure resolver provisioning HTTP setup failed")
    })?;
    let mut successful = 0;
    let mut denied = 0;
    let mut indeterminate = 0;
    for structure_id in structure_ids {
        let response = http_client
            .get(format!(
                "{}universe/structures/{structure_id}/",
                endpoints.esi_base_url
            ))
            .bearer_auth(authorization.access_token())
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => successful += 1,
            Ok(response)
                if response.status() == StatusCode::FORBIDDEN
                    || response.status() == StatusCode::NOT_FOUND =>
            {
                denied += 1
            }
            Ok(_) | Err(_) => indeterminate += 1,
        }
    }
    let coverage = if indeterminate > 0 {
        StructureProbeCoverage::Indeterminate
    } else if successful == structure_ids.len() {
        StructureProbeCoverage::Full
    } else if successful > 0 {
        StructureProbeCoverage::Partial
    } else {
        StructureProbeCoverage::Denied
    };
    Ok(StructureProbeReport {
        coverage,
        successful,
        denied,
        indeterminate,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructureResolverConfigError {
    message: String,
}

impl StructureResolverConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for StructureResolverConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StructureResolverConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructureResolverRuntimeStatus {
    enabled: bool,
    credential_revision: String,
    status: &'static str,
    last_error: Option<&'static str>,
}

impl StructureResolverRuntimeStatus {
    fn ready(credential_revision: String) -> Self {
        Self {
            enabled: true,
            credential_revision,
            status: "ready",
            last_error: None,
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            credential_revision: "disabled".to_string(),
            status: "degraded",
            last_error: Some("structure resolver disabled"),
        }
    }

    pub fn invalid_configuration() -> Self {
        Self {
            enabled: false,
            credential_revision: "invalid-configuration".to_string(),
            status: "degraded",
            last_error: Some("structure resolver authorization configuration invalid"),
        }
    }

    pub fn initialization_failed() -> Self {
        Self {
            enabled: false,
            credential_revision: "initialization-failed".to_string(),
            status: "degraded",
            last_error: Some("structure resolver HTTP initialization failed"),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn credential_revision(&self) -> &str {
        &self.credential_revision
    }

    pub(crate) fn status(&self) -> &str {
        self.status
    }

    pub(crate) fn last_error(&self) -> Option<&str> {
        self.last_error
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedStructure {
    pub structure_id: i64,
    pub name: Option<String>,
    pub solar_system_id: i64,
    pub observed_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub response_metadata: CacheMetadata,
    pub representation_cacheable: bool,
}

#[derive(Clone)]
struct StructureResponseMetadata {
    representation: CacheMetadata,
    limiter: CacheMetadata,
    representation_cacheable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructureResolverFailureKind {
    AccessDenied,
    Degraded,
    Transient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructureResolverResponseOrigin {
    None,
    StructureEsi,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StructureResolverError {
    kind: StructureResolverFailureKind,
    message: String,
    retry_after: Option<DateTime<Utc>>,
    global_auth_failure: bool,
    response_metadata: CacheMetadata,
    response_origin: StructureResolverResponseOrigin,
    response_disallows_representation_storage: bool,
}

impl StructureResolverError {
    fn new(
        kind: StructureResolverFailureKind,
        message: impl Into<String>,
        retry_after: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after,
            global_auth_failure: false,
            response_metadata: CacheMetadata {
                retry_after,
                ..CacheMetadata::cached_for_seconds(0)
            },
            response_origin: StructureResolverResponseOrigin::None,
            response_disallows_representation_storage: false,
        }
    }

    fn from_response_metadata(
        kind: StructureResolverFailureKind,
        message: impl Into<String>,
        response_metadata: CacheMetadata,
        response_disallows_representation_storage: bool,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after: response_metadata.retry_after,
            global_auth_failure: false,
            response_metadata,
            response_origin: StructureResolverResponseOrigin::StructureEsi,
            response_disallows_representation_storage,
        }
    }

    pub fn kind(&self) -> StructureResolverFailureKind {
        self.kind
    }

    pub fn retry_after(&self) -> Option<DateTime<Utc>> {
        self.retry_after
    }

    pub fn is_global_auth_failure(&self) -> bool {
        self.global_auth_failure
    }

    pub(crate) fn response_metadata(&self) -> &CacheMetadata {
        &self.response_metadata
    }

    pub(crate) fn has_structure_esi_response(&self) -> bool {
        self.response_origin == StructureResolverResponseOrigin::StructureEsi
    }

    pub(crate) fn response_disallows_representation_storage(&self) -> bool {
        self.response_disallows_representation_storage
    }

    fn global_auth_failure(mut self) -> Self {
        self.global_auth_failure = true;
        self
    }

    fn with_default_retry_after(mut self, deadline: DateTime<Utc>) -> Self {
        if self.retry_after.is_none() {
            self.retry_after = Some(deadline);
        }
        self
    }

    fn retaining_prior_structure_response(
        mut self,
        prior: Option<StructureResponseMetadata>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        let Some(prior) = prior else {
            return self;
        };
        self.response_metadata = if self.has_structure_esi_response() {
            merge_structure_limiter_metadata(&prior.limiter, self.response_metadata, observed_at)
        } else {
            prior.limiter
        };
        self.response_origin = StructureResolverResponseOrigin::StructureEsi;
        self.response_disallows_representation_storage |= !prior.representation_cacheable;
        if self.retry_after.is_none() {
            self.retry_after = self.response_metadata.retry_after;
        }
        self
    }

    pub fn access_denied(retry_after: Option<DateTime<Utc>>) -> Self {
        Self::new(
            StructureResolverFailureKind::AccessDenied,
            "structure resolver access denied",
            retry_after,
        )
    }

    pub fn authorization_invalid() -> Self {
        Self::new(
            StructureResolverFailureKind::Degraded,
            "structure resolver authorization invalid",
            None,
        )
    }

    pub fn transient(retry_after: Option<DateTime<Utc>>) -> Self {
        Self::new(
            StructureResolverFailureKind::Transient,
            "structure resolver temporarily unavailable",
            retry_after,
        )
    }
}

impl Display for StructureResolverError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StructureResolverError {}

pub struct AuthenticatedStructureResolver {
    config: StructureResolverConfig,
    client: Client,
    esi_base_url: String,
    token_url: String,
    metadata_url: String,
    require_eve_sso_binding: bool,
    resolver_identity: String,
    refresh_token: Mutex<String>,
    access_token: Mutex<Option<ValidatedAccessToken>>,
    refresh_failure: Mutex<Option<StructureResolverError>>,
    refresh_gate: Mutex<()>,
    discovery: Mutex<Option<CachedSsoResponse<OpenIdMetadata>>>,
    jwks: Mutex<Option<CachedJwks>>,
    clock: std::sync::Arc<dyn StructureResolverClock>,
}

#[async_trait]
pub trait StructureResolver: Send + Sync {
    fn credential_revision(&self) -> &str;
    fn resolver_identity(&self) -> &str;
    async fn resolve_structure(
        &self,
        structure_id: i64,
    ) -> Result<ResolvedStructure, StructureResolverError>;

    async fn resolve_structure_revalidating(
        &self,
        structure_id: i64,
        _cached: Option<ResolvedStructure>,
        _now: DateTime<Utc>,
    ) -> Result<ResolvedStructure, StructureResolverError> {
        self.resolve_structure(structure_id).await
    }
}

#[derive(Clone)]
struct ValidatedAccessToken {
    value: String,
    expires_at: DateTime<Utc>,
}

#[derive(Clone)]
struct ValidatedEveAccessToken {
    character_id: i64,
    expires_at: DateTime<Utc>,
}

#[derive(Clone)]
struct CachedSsoResponse<T> {
    value: T,
    expires_at: DateTime<Utc>,
}

#[derive(Clone)]
struct CachedJwks {
    jwks_uri: String,
    response: CachedSsoResponse<JsonWebKeySet>,
}

#[derive(Deserialize)]
struct TokenRefreshResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
}

#[derive(Clone, Deserialize)]
struct OpenIdMetadata {
    issuer: Option<String>,
    jwks_uri: String,
}

#[derive(Clone, Deserialize)]
struct JsonWebKeySet {
    keys: Vec<JsonWebKey>,
}

#[derive(Clone, Deserialize)]
struct JsonWebKey {
    #[serde(default)]
    alg: Option<String>,
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    kty: Option<String>,
    #[serde(default)]
    n: Option<String>,
}

#[derive(Deserialize)]
struct EveAccessTokenClaims {
    aud: Vec<String>,
    exp: usize,
    iss: String,
    scp: ScopeClaim,
    sub: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    List(Vec<String>),
    SpaceDelimited(String),
}

impl ScopeClaim {
    fn values(self) -> BTreeSet<String> {
        match self {
            Self::List(scopes) => scopes.into_iter().collect(),
            Self::SpaceDelimited(scopes) => {
                scopes.split_whitespace().map(ToOwned::to_owned).collect()
            }
        }
    }
}

#[derive(Deserialize)]
struct StructureResponse {
    name: Option<String>,
    solar_system_id: i64,
}

fn structure_response_metadata(
    headers: &reqwest::header::HeaderMap,
    observed_at: DateTime<Utc>,
) -> StructureResponseMetadata {
    let mut representation = cache_metadata_at(headers, observed_at);
    let representation_cacheable = !response_disallows_representation_storage(headers);
    if !representation_cacheable {
        representation.etag = None;
        representation.expires_at = None;
        representation.last_modified = None;
        representation.expected_pages = None;
    }
    let limiter = CacheMetadata {
        etag: None,
        expires_at: None,
        last_modified: None,
        expected_pages: None,
        ..representation.clone()
    };
    StructureResponseMetadata {
        representation,
        limiter,
        representation_cacheable,
    }
}

fn merge_structure_limiter_metadata(
    prior: &CacheMetadata,
    response: CacheMetadata,
    observed_at: DateTime<Utc>,
) -> CacheMetadata {
    let (error_limit_remain, error_limit_reset) = merge_error_limit_tuple(prior, &response);
    let rate = merge_rate_limit_tuple(prior, &response, observed_at);
    CacheMetadata {
        etag: None,
        expires_at: None,
        last_modified: None,
        expected_pages: None,
        error_limit_remain,
        error_limit_reset,
        rate_limit_group: rate.rate_limit_group,
        rate_limit_limit: rate.rate_limit_limit,
        rate_limit_remaining: rate.rate_limit_remaining,
        rate_limit_used: rate.rate_limit_used,
        retry_after: prior.retry_after.max(response.retry_after),
    }
}

fn merge_error_limit_tuple(
    prior: &CacheMetadata,
    response: &CacheMetadata,
) -> (Option<i64>, Option<i64>) {
    match (prior.error_limit_remain, response.error_limit_remain) {
        (Some(prior_remain), Some(response_remain)) if prior_remain < response_remain => {
            (Some(prior_remain), prior.error_limit_reset)
        }
        (Some(prior_remain), Some(response_remain)) if response_remain < prior_remain => {
            (Some(response_remain), response.error_limit_reset)
        }
        (Some(remain), Some(_)) => (
            Some(remain),
            prior.error_limit_reset.max(response.error_limit_reset),
        ),
        (Some(remain), None) => (Some(remain), prior.error_limit_reset),
        (None, Some(remain)) => (Some(remain), response.error_limit_reset),
        (None, None) => (
            None,
            prior.error_limit_reset.max(response.error_limit_reset),
        ),
    }
}

fn merge_rate_limit_tuple(
    prior: &CacheMetadata,
    response: &CacheMetadata,
    observed_at: DateTime<Utc>,
) -> CacheMetadata {
    let compatible = (prior.rate_limit_group.is_none()
        || response.rate_limit_group.is_none()
        || prior.rate_limit_group == response.rate_limit_group)
        && (prior.rate_limit_limit.is_none()
            || response.rate_limit_limit.is_none()
            || prior.rate_limit_limit == response.rate_limit_limit);
    let selected = if compatible
        || esi_limiter_deadline_at(prior, observed_at)
            > esi_limiter_deadline_at(response, observed_at)
    {
        prior
    } else {
        response
    };
    CacheMetadata {
        etag: None,
        expires_at: None,
        last_modified: None,
        expected_pages: None,
        error_limit_remain: None,
        error_limit_reset: None,
        rate_limit_group: if compatible {
            response
                .rate_limit_group
                .clone()
                .or_else(|| prior.rate_limit_group.clone())
        } else {
            selected.rate_limit_group.clone()
        },
        rate_limit_limit: if compatible {
            response
                .rate_limit_limit
                .clone()
                .or_else(|| prior.rate_limit_limit.clone())
        } else {
            selected.rate_limit_limit.clone()
        },
        rate_limit_remaining: if compatible {
            more_restrictive_remaining(prior.rate_limit_remaining, response.rate_limit_remaining)
        } else {
            selected.rate_limit_remaining
        },
        rate_limit_used: if compatible {
            prior.rate_limit_used.max(response.rate_limit_used)
        } else {
            selected.rate_limit_used
        },
        retry_after: None,
    }
}

fn more_restrictive_remaining(prior: Option<i64>, response: Option<i64>) -> Option<i64> {
    match (prior, response) {
        (Some(prior), Some(response)) => Some(prior.min(response)),
        (Some(remaining), None) | (None, Some(remaining)) => Some(remaining),
        (None, None) => None,
    }
}

fn transient_structure_response_metadata(response: &StructureResponseMetadata) -> CacheMetadata {
    CacheMetadata {
        expires_at: response.representation.expires_at,
        ..response.limiter.clone()
    }
}

fn response_disallows_representation_storage(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|directive| directive.trim().eq_ignore_ascii_case("no-store"))
        })
}

impl AuthenticatedStructureResolver {
    pub fn new(config: StructureResolverConfig, timeout: Duration) -> Result<Self, reqwest::Error> {
        Self::with_endpoints(
            config,
            EVE_ESI_BASE_URL,
            EVE_SSO_TOKEN_URL,
            EVE_SSO_METADATA_URL,
            timeout,
        )
    }

    pub fn with_endpoints(
        config: StructureResolverConfig,
        esi_base_url: impl Into<String>,
        token_url: impl Into<String>,
        metadata_url: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Self::with_endpoints_and_clock(
            config,
            esi_base_url,
            token_url,
            metadata_url,
            timeout,
            std::sync::Arc::new(SystemStructureResolverClock),
        )
    }

    #[doc(hidden)]
    pub fn with_endpoints_and_clock(
        config: StructureResolverConfig,
        esi_base_url: impl Into<String>,
        token_url: impl Into<String>,
        metadata_url: impl Into<String>,
        timeout: Duration,
        clock: std::sync::Arc<dyn StructureResolverClock>,
    ) -> Result<Self, reqwest::Error> {
        Self::with_endpoints_and_sso_binding_and_clock(
            config,
            esi_base_url,
            token_url,
            metadata_url,
            timeout,
            false,
            clock,
        )
    }

    #[doc(hidden)]
    pub fn with_endpoints_and_sso_binding(
        config: StructureResolverConfig,
        esi_base_url: impl Into<String>,
        token_url: impl Into<String>,
        metadata_url: impl Into<String>,
        timeout: Duration,
        require_eve_sso_binding: bool,
    ) -> Result<Self, reqwest::Error> {
        Self::with_endpoints_and_sso_binding_and_clock(
            config,
            esi_base_url,
            token_url,
            metadata_url,
            timeout,
            require_eve_sso_binding,
            std::sync::Arc::new(SystemStructureResolverClock),
        )
    }

    fn with_endpoints_and_sso_binding_and_clock(
        config: StructureResolverConfig,
        esi_base_url: impl Into<String>,
        token_url: impl Into<String>,
        metadata_url: impl Into<String>,
        timeout: Duration,
        require_eve_sso_binding: bool,
        clock: std::sync::Arc<dyn StructureResolverClock>,
    ) -> Result<Self, reqwest::Error> {
        let metadata_url = metadata_url.into();
        let resolver_identity = config
            .character_id()
            .map(|character_id| format!("character:{character_id}"))
            .unwrap_or_default();
        let refresh_token = config.refresh_token().unwrap_or_default().to_string();
        Ok(Self {
            resolver_identity,
            config,
            client: Client::builder().timeout(timeout).build()?,
            esi_base_url: esi_base_url.into(),
            token_url: token_url.into(),
            require_eve_sso_binding: require_eve_sso_binding
                || metadata_url == EVE_SSO_METADATA_URL,
            metadata_url,
            refresh_token: Mutex::new(refresh_token),
            access_token: Mutex::new(None),
            refresh_failure: Mutex::new(None),
            refresh_gate: Mutex::new(()),
            discovery: Mutex::new(None),
            jwks: Mutex::new(None),
            clock,
        })
    }

    pub async fn resolve_structure(
        &self,
        structure_id: i64,
    ) -> Result<ResolvedStructure, StructureResolverError> {
        self.resolve_structure_revalidating(structure_id, None, self.clock.now())
            .await
    }

    async fn resolve_structure_revalidating(
        &self,
        structure_id: i64,
        cached: Option<ResolvedStructure>,
        now: DateTime<Utc>,
    ) -> Result<ResolvedStructure, StructureResolverError> {
        if structure_id <= 0 {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver requires a positive structure identifier",
                None,
            ));
        }
        if !self.config.is_enabled() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver authorization is disabled",
                None,
            ));
        }
        let mut refreshed_after_unauthorized = false;
        let mut prior_response_metadata: Option<StructureResponseMetadata> = None;
        loop {
            let access_token = match self.access_token().await {
                Ok(access_token) => access_token,
                Err(error) => {
                    return Err(error
                        .retaining_prior_structure_response(prior_response_metadata.clone(), now))
                }
            };
            let mut request = self
                .client
                .get(format!(
                    "{}universe/structures/{structure_id}/",
                    self.esi_base_url
                ))
                .bearer_auth(&access_token);
            if let Some(etag) = cached.as_ref().and_then(|cached| cached.etag.as_deref()) {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(_) => {
                    return Err(StructureResolverError::new(
                        StructureResolverFailureKind::Transient,
                        "structure resolver request failed",
                        None,
                    )
                    .retaining_prior_structure_response(prior_response_metadata.clone(), now))
                }
            };
            let response_received_at = self.clock.now();
            let response_metadata =
                structure_response_metadata(response.headers(), response_received_at);
            let limiter_metadata = prior_response_metadata
                .as_ref()
                .map(|prior| {
                    merge_structure_limiter_metadata(
                        &prior.limiter,
                        response_metadata.limiter.clone(),
                        response_received_at,
                    )
                })
                .unwrap_or_else(|| response_metadata.limiter.clone());
            let representation_storage_disallowed = !response_metadata.representation_cacheable
                || prior_response_metadata
                    .as_ref()
                    .is_some_and(|prior| !prior.representation_cacheable);
            let transient_response_metadata =
                transient_structure_response_metadata(&response_metadata);
            if response.status() == StatusCode::UNAUTHORIZED && !refreshed_after_unauthorized {
                self.evict_access_token_if_matches(&access_token).await;
                refreshed_after_unauthorized = true;
                if esi_limiter_deadline_at(&limiter_metadata, response_received_at)
                    .is_some_and(|deadline| deadline > response_received_at)
                {
                    return Err(StructureResolverError::from_response_metadata(
                        StructureResolverFailureKind::Transient,
                        "structure resolver token refresh deferred by an upstream limiter boundary",
                        limiter_metadata,
                        !response_metadata.representation_cacheable,
                    ));
                }
                prior_response_metadata = Some(response_metadata);
                continue;
            }
            return match response.status() {
                StatusCode::NOT_MODIFIED => {
                    let cached = cached.ok_or_else(|| {
                        StructureResolverError::from_response_metadata(
                            StructureResolverFailureKind::Transient,
                            "structure resolver returned 304 without persisted structure facts",
                            transient_response_metadata.clone(),
                            representation_storage_disallowed,
                        )
                    })?;
                    let expires_at = response_metadata
                        .representation
                        .expires_at
                        .filter(|expiry| *expiry > response_received_at);
                    let Some(expires_at) = expires_at else {
                        return Err(StructureResolverError::from_response_metadata(
                            StructureResolverFailureKind::Transient,
                            "structure resolver returned 304 without a usable cache expiry",
                            transient_response_metadata,
                            representation_storage_disallowed,
                        ));
                    };
                    Ok(ResolvedStructure {
                        structure_id,
                        name: cached.name,
                        solar_system_id: cached.solar_system_id,
                        observed_at: response_received_at,
                        expires_at: Some(expires_at),
                        etag: response_metadata
                            .representation
                            .etag
                            .clone()
                            .or(cached.etag),
                        response_metadata: limiter_metadata,
                        representation_cacheable: response_metadata.representation_cacheable,
                    })
                }
                status if status.is_success() => {
                    let expires_at = response_metadata.representation.expires_at;
                    let etag = response_metadata.representation.etag.clone();
                    let body = response.json::<StructureResponse>().await.map_err(|_| {
                        StructureResolverError::from_response_metadata(
                            StructureResolverFailureKind::Transient,
                            "structure resolver returned an invalid structure response",
                            transient_response_metadata.clone(),
                            representation_storage_disallowed,
                        )
                    })?;
                    if body.solar_system_id <= 0 {
                        return Err(StructureResolverError::from_response_metadata(
                            StructureResolverFailureKind::Transient,
                            "structure resolver returned an invalid solar system identifier",
                            transient_response_metadata,
                            representation_storage_disallowed,
                        ));
                    }
                    Ok(ResolvedStructure {
                        structure_id,
                        name: body.name.filter(|name| !name.trim().is_empty()),
                        solar_system_id: body.solar_system_id,
                        observed_at: response_received_at,
                        expires_at,
                        etag,
                        response_metadata: limiter_metadata,
                        representation_cacheable: response_metadata.representation_cacheable,
                    })
                }
                StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => {
                    Err(StructureResolverError::from_response_metadata(
                        StructureResolverFailureKind::AccessDenied,
                        "structure resolver access was denied",
                        limiter_metadata,
                        representation_storage_disallowed,
                    ))
                }
                StatusCode::UNAUTHORIZED => {
                    self.evict_access_token_if_matches(&access_token).await;
                    let error = StructureResolverError::from_response_metadata(
                        StructureResolverFailureKind::Degraded,
                        "structure resolver authorization was rejected after refresh",
                        limiter_metadata,
                        representation_storage_disallowed,
                    )
                    .global_auth_failure()
                    .with_default_retry_after(response_received_at + ChronoDuration::seconds(30));
                    *self.refresh_failure.lock().await = Some(error.clone());
                    Err(error)
                }
                status => Err(StructureResolverError::from_response_metadata(
                    StructureResolverFailureKind::Transient,
                    format!("structure resolver returned {status}"),
                    transient_response_metadata,
                    representation_storage_disallowed,
                )),
            };
        }
    }

    async fn access_token(&self) -> Result<String, StructureResolverError> {
        let now = self.clock.now();
        if let Some(error) = self.cached_refresh_failure(now).await {
            return Err(error);
        }
        if let Some(token) = self.cached_access_token(now).await {
            return Ok(token.value);
        }
        let _refresh = self.refresh_gate.lock().await;
        if let Some(token) = self.cached_access_token(self.clock.now()).await {
            return Ok(token.value);
        }
        if let Some(error) = self.cached_refresh_failure(self.clock.now()).await {
            return Err(error);
        }
        match self.refresh_access_token().await {
            Ok(token) => {
                *self.refresh_failure.lock().await = None;
                Ok(token)
            }
            Err(error) if error.is_global_auth_failure() => {
                let error =
                    error.with_default_retry_after(self.clock.now() + ChronoDuration::seconds(30));
                *self.refresh_failure.lock().await = Some(error.clone());
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    async fn refresh_access_token(&self) -> Result<String, StructureResolverError> {
        let client_id = self.config.client_id().ok_or_else(|| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver client identifier is unavailable",
                None,
            )
        })?;
        let client_secret = self.config.client_secret().ok_or_else(|| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver client secret is unavailable",
                None,
            )
        })?;
        if self.config.refresh_token().is_none() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver refresh authorization is unavailable",
                None,
            ));
        }
        let refresh_token = self.refresh_token.lock().await.clone();
        if refresh_token.trim().is_empty() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver refresh authorization is unavailable",
                None,
            ));
        }
        let response = self
            .client
            .post(&self.token_url)
            .basic_auth(client_id, Some(client_secret))
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token.as_str()),
            ])
            .send()
            .await
            .map_err(|_| {
                StructureResolverError::new(
                    StructureResolverFailureKind::Degraded,
                    "structure resolver token refresh failed",
                    None,
                )
                .global_auth_failure()
            })?;
        let response_received_at = self.clock.now();
        if !response.status().is_success() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                format!(
                    "structure resolver token refresh returned {}",
                    response.status()
                ),
                retry_after_at(response.headers(), response_received_at),
            )
            .global_auth_failure());
        }
        let response = response.json::<TokenRefreshResponse>().await.map_err(|_| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver token refresh returned an invalid response",
                None,
            )
            .global_auth_failure()
        })?;
        if let Some(rotated_refresh_token) = response
            .refresh_token
            .filter(|refresh_token| !refresh_token.trim().is_empty())
        {
            *self.refresh_token.lock().await = rotated_refresh_token;
        }
        let validated = self
            .validate_access_token(&response.access_token)
            .await
            .map_err(StructureResolverError::global_auth_failure)?;
        *self.access_token.lock().await = Some(validated.clone());
        Ok(validated.value)
    }

    async fn cached_refresh_failure(&self, now: DateTime<Utc>) -> Option<StructureResolverError> {
        self.refresh_failure
            .lock()
            .await
            .as_ref()
            .filter(|error| error.retry_after().is_some_and(|deadline| deadline > now))
            .cloned()
    }

    async fn evict_access_token_if_matches(&self, rejected: &str) {
        let mut cached = self.access_token.lock().await;
        if cached.as_ref().is_some_and(|token| token.value == rejected) {
            *cached = None;
        }
    }

    async fn cached_access_token(&self, now: DateTime<Utc>) -> Option<ValidatedAccessToken> {
        self.access_token
            .lock()
            .await
            .as_ref()
            .filter(|token| token.expires_at > now + ChronoDuration::seconds(30))
            .cloned()
    }

    async fn validate_access_token(
        &self,
        access_token: &str,
    ) -> Result<ValidatedAccessToken, StructureResolverError> {
        let client_id = self.config.client_id().ok_or_else(invalid_access_token)?;
        let validated = self
            .validate_eve_access_token_for_client(access_token, client_id)
            .await?;
        let expected_character = self
            .config
            .character_id()
            .ok_or_else(invalid_access_token)?;
        if validated.character_id != expected_character {
            return Err(invalid_access_token());
        }
        Ok(ValidatedAccessToken {
            value: access_token.to_string(),
            expires_at: validated.expires_at,
        })
    }

    async fn validate_eve_access_token_for_client(
        &self,
        access_token: &str,
        client_id: &str,
    ) -> Result<ValidatedEveAccessToken, StructureResolverError> {
        let metadata = self
            .sso_metadata()
            .await
            .map_err(StructureResolverError::global_auth_failure)?;
        let jwks = self
            .sso_jwks(&metadata.jwks_uri, false)
            .await
            .map_err(StructureResolverError::global_auth_failure)?;
        match validate_eve_access_token_for_client(access_token, &jwks, client_id) {
            Ok(token) => Ok(token),
            Err(_) if jwt_kid_is_missing_from(access_token, &jwks) => {
                let refreshed = self
                    .sso_jwks(&metadata.jwks_uri, true)
                    .await
                    .map_err(StructureResolverError::global_auth_failure)?;
                validate_eve_access_token_for_client(access_token, &refreshed, client_id)
            }
            Err(error) => Err(error),
        }
    }

    async fn sso_metadata(&self) -> Result<OpenIdMetadata, StructureResolverError> {
        let now = self.clock.now();
        if let Some(metadata) = self
            .discovery
            .lock()
            .await
            .as_ref()
            .filter(|metadata| metadata.expires_at > now)
            .cloned()
        {
            return Ok(metadata.value);
        }
        let response = self
            .client
            .get(&self.metadata_url)
            .send()
            .await
            .map_err(|_| {
                StructureResolverError::new(
                    StructureResolverFailureKind::Degraded,
                    "structure resolver SSO discovery failed",
                    None,
                )
            })?;
        let response_received_at = self.clock.now();
        if !response.status().is_success() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                format!(
                    "structure resolver SSO discovery returned {}",
                    response.status()
                ),
                retry_after_at(response.headers(), response_received_at),
            ));
        }
        let cache_expires_at = cache_expires_at(response.headers(), response_received_at);
        let metadata = response.json::<OpenIdMetadata>().await.map_err(|_| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver SSO discovery returned an invalid response",
                None,
            )
        })?;
        if self.require_eve_sso_binding && !trusted_eve_sso_metadata(&metadata) {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver SSO discovery is not trusted",
                None,
            ));
        }
        if let Some(expires_at) =
            cache_expires_at.filter(|expires_at| *expires_at > response_received_at)
        {
            *self.discovery.lock().await = Some(CachedSsoResponse {
                value: metadata.clone(),
                expires_at,
            });
        }
        Ok(metadata)
    }

    async fn sso_jwks(
        &self,
        jwks_uri: &str,
        force_refresh: bool,
    ) -> Result<JsonWebKeySet, StructureResolverError> {
        let now = self.clock.now();
        if !force_refresh {
            if let Some(jwks) = self
                .jwks
                .lock()
                .await
                .as_ref()
                .filter(|jwks| jwks.jwks_uri == jwks_uri && jwks.response.expires_at > now)
                .cloned()
            {
                return Ok(jwks.response.value);
            }
        }
        let response = self.client.get(jwks_uri).send().await.map_err(|_| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver SSO key retrieval failed",
                None,
            )
        })?;
        let response_received_at = self.clock.now();
        if !response.status().is_success() {
            return Err(StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                format!(
                    "structure resolver SSO key retrieval returned {}",
                    response.status()
                ),
                retry_after_at(response.headers(), response_received_at),
            ));
        }
        let cache_expires_at = cache_expires_at(response.headers(), response_received_at);
        let jwks = response.json::<JsonWebKeySet>().await.map_err(|_| {
            StructureResolverError::new(
                StructureResolverFailureKind::Degraded,
                "structure resolver SSO key retrieval returned an invalid response",
                None,
            )
        })?;
        if let Some(expires_at) =
            cache_expires_at.filter(|expires_at| *expires_at > response_received_at)
        {
            *self.jwks.lock().await = Some(CachedJwks {
                jwks_uri: jwks_uri.to_string(),
                response: CachedSsoResponse {
                    value: jwks.clone(),
                    expires_at,
                },
            });
        }
        Ok(jwks)
    }
}

#[async_trait]
impl StructureResolver for AuthenticatedStructureResolver {
    fn credential_revision(&self) -> &str {
        self.config.credential_revision()
    }

    fn resolver_identity(&self) -> &str {
        &self.resolver_identity
    }

    async fn resolve_structure(
        &self,
        structure_id: i64,
    ) -> Result<ResolvedStructure, StructureResolverError> {
        AuthenticatedStructureResolver::resolve_structure(self, structure_id).await
    }

    async fn resolve_structure_revalidating(
        &self,
        structure_id: i64,
        cached: Option<ResolvedStructure>,
        now: DateTime<Utc>,
    ) -> Result<ResolvedStructure, StructureResolverError> {
        AuthenticatedStructureResolver::resolve_structure_revalidating(
            self,
            structure_id,
            cached,
            now,
        )
        .await
    }
}

fn jwt_kid_is_missing_from(access_token: &str, jwks: &JsonWebKeySet) -> bool {
    decode_header(access_token)
        .ok()
        .and_then(|header| header.kid)
        .is_some_and(|kid| {
            !jwks
                .keys
                .iter()
                .any(|key| key.kid.as_deref() == Some(&kid) && is_rs256_rsa_key(key))
        })
}

fn trusted_eve_sso_metadata(metadata: &OpenIdMetadata) -> bool {
    if !metadata
        .issuer
        .as_deref()
        .is_some_and(|issuer| EVE_SSO_ISSUERS.contains(&issuer))
    {
        return false;
    }
    let Ok(jwks_uri) = Url::parse(&metadata.jwks_uri) else {
        return false;
    };
    jwks_uri.scheme() == "https"
        && jwks_uri.host_str() == Some("login.eveonline.com")
        && jwks_uri.port().is_none()
}

fn validate_eve_access_token_for_client(
    access_token: &str,
    jwks: &JsonWebKeySet,
    client_id: &str,
) -> Result<ValidatedEveAccessToken, StructureResolverError> {
    let header = decode_header(access_token).map_err(|_| invalid_access_token())?;
    if header.alg != Algorithm::RS256 {
        return Err(invalid_access_token());
    }
    let kid = header.kid.as_deref().ok_or_else(invalid_access_token)?;
    let key = jwks
        .keys
        .iter()
        .find(|key| key.kid.as_deref() == Some(kid) && is_rs256_rsa_key(key))
        .ok_or_else(invalid_access_token)?;
    let decoding_key = DecodingKey::from_rsa_components(
        key.n.as_deref().ok_or_else(invalid_access_token)?,
        key.e.as_deref().ok_or_else(invalid_access_token)?,
    )
    .map_err(|_| invalid_access_token())?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.leeway = 0;
    validation.set_issuer(EVE_SSO_ISSUERS);
    validation.set_audience(&[client_id]);
    validation.required_spec_claims = ["exp", "iss", "aud", "sub"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    let claims = decode::<EveAccessTokenClaims>(access_token, &decoding_key, &validation)
        .map_err(|_| invalid_access_token())?
        .claims;
    let character_id = claims
        .sub
        .strip_prefix("CHARACTER:EVE:")
        .and_then(|character_id| character_id.parse::<i64>().ok())
        .filter(|character_id| *character_id > 0);
    if claims.iss.is_empty()
        || !claims.aud.iter().any(|audience| audience == "EVE Online")
        || character_id.is_none()
        || claims.scp.values() != BTreeSet::from([STRUCTURE_RESOLVER_SCOPE.to_string()])
    {
        return Err(invalid_access_token());
    }
    let expires_at =
        DateTime::from_timestamp(claims.exp as i64, 0).ok_or_else(invalid_access_token)?;
    Ok(ValidatedEveAccessToken {
        character_id: character_id.expect("checked above"),
        expires_at,
    })
}

fn is_rs256_rsa_key(key: &JsonWebKey) -> bool {
    key.kty.as_deref() == Some("RSA")
        && key.alg.as_deref() == Some("RS256")
        && key.n.as_deref().is_some_and(|n| !n.is_empty())
        && key.e.as_deref().is_some_and(|e| !e.is_empty())
}

fn invalid_access_token() -> StructureResolverError {
    StructureResolverError::new(
        StructureResolverFailureKind::Degraded,
        "structure resolver access token is invalid",
        None,
    )
}

fn expires_at(headers: &reqwest::header::HeaderMap) -> Option<DateTime<Utc>> {
    headers
        .get("expires")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn cache_expires_at(
    headers: &reqwest::header::HeaderMap,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let cache_control = headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if cache_control.as_deref().is_some_and(|value| {
        value
            .split(',')
            .any(|directive| directive.trim().eq_ignore_ascii_case("no-store"))
    }) {
        return None;
    }
    cache_control
        .as_deref()
        .and_then(|value| {
            value.split(',').find_map(|directive| {
                directive
                    .trim()
                    .strip_prefix("max-age=")
                    .and_then(|seconds| seconds.parse::<i64>().ok())
                    .filter(|seconds| *seconds >= 0)
            })
        })
        .and_then(ChronoDuration::try_seconds)
        .map(|duration| observed_at + duration)
        .or_else(|| expires_at(headers))
}

fn retry_after_at(
    headers: &reqwest::header::HeaderMap,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .parse::<i64>()
                .ok()
                .map(|seconds| observed_at + ChronoDuration::seconds(seconds))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|time| time.with_timezone(&Utc))
                })
        })
}

fn required_setting(
    settings: &BTreeMap<String, String>,
    name: &str,
) -> Result<String, StructureResolverConfigError> {
    settings
        .get(name)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            StructureResolverConfigError::new(format!(
                "{name} is required when STRUCTURE_RESOLVER_ENABLED is true"
            ))
        })
}

fn resolver_client_credentials(
    settings: &BTreeMap<String, String>,
) -> Result<(String, String), StructureResolverConfigError> {
    let resolver_override_is_configured = [
        "STRUCTURE_RESOLVER_CLIENT_ID",
        "STRUCTURE_RESOLVER_CLIENT_SECRET",
    ]
    .into_iter()
    .any(|name| {
        settings
            .get(name)
            .is_some_and(|value| !value.trim().is_empty())
    });
    if resolver_override_is_configured {
        return Ok((
            required_setting(settings, "STRUCTURE_RESOLVER_CLIENT_ID")?,
            required_setting(settings, "STRUCTURE_RESOLVER_CLIENT_SECRET")?,
        ));
    }
    Ok((
        required_setting(settings, "EVE_CLIENT_ID")?,
        required_setting(settings, "EVE_CLIENT_SECRET")?,
    ))
}

fn required_positive_i64(
    settings: &BTreeMap<String, String>,
    name: &str,
) -> Result<i64, StructureResolverConfigError> {
    let value = required_setting(settings, name)?;
    let parsed = value.parse::<i64>().map_err(|_| {
        StructureResolverConfigError::new(format!("{name} must be a positive integer"))
    })?;
    if parsed <= 0 {
        return Err(StructureResolverConfigError::new(format!(
            "{name} must be a positive integer"
        )));
    }
    Ok(parsed)
}
