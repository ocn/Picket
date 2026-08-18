use killbot_rust::structure_resolver::{
    build_structure_resolver_authorization_url, exchange_structure_resolver_authorization,
    parse_structure_resolver_callback, probe_structure_resolver_coverage, StructureProbeCoverage,
    StructureResolverProvisioningAuthorization, StructureResolverProvisioningClient,
    StructureResolverProvisioningEndpoints,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

const PROVISION_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Deserialize, Serialize)]
struct AuthorizationBundle {
    client_mode: ClientMode,
    client_id: String,
    client_secret: String,
    redirect_uri: String,
    access_token: String,
    refresh_token: String,
    character_id: i64,
    credential_revision: String,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ClientMode {
    Existing,
    Dedicated,
}

impl ClientMode {
    fn parse(value: &str) -> Result<Self, ()> {
        match value {
            "existing" => Ok(Self::Existing),
            "dedicated" => Ok(Self::Dedicated),
            _ => Err(()),
        }
    }
}

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("structure resolver provisioning did not complete; no credentials were printed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ()> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().ok_or(())?;
    let options = parse_options(arguments.collect())?;
    match command.as_str() {
        "prepare" => prepare(&options),
        "authorize" => authorize(&options).await,
        "probe" => probe(&options).await,
        "env-values" => env_values(&options),
        _ => Err(()),
    }
}

fn parse_options(arguments: Vec<String>) -> Result<BTreeMap<String, String>, ()> {
    let mut options = BTreeMap::new();
    let mut arguments = arguments.into_iter();
    while let Some(key) = arguments.next() {
        if !key.starts_with("--") || options.contains_key(&key) {
            return Err(());
        }
        let value = arguments.next().ok_or(())?;
        options.insert(key, value);
    }
    Ok(options)
}

fn option<'a>(options: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, ()> {
    options
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(())
}

fn prepare(options: &BTreeMap<String, String>) -> Result<(), ()> {
    let client_id = option(options, "--client-id")?;
    let redirect_uri = option(options, "--redirect-uri")?;
    let state_file = option(options, "--state-file")?;
    let state = fresh_state();
    write_secret_file(state_file, state.as_bytes())?;
    let authorization_url =
        build_structure_resolver_authorization_url(client_id, redirect_uri, &state)
            .map_err(|_| ())?;
    println!("{authorization_url}");
    Ok(())
}

async fn authorize(options: &BTreeMap<String, String>) -> Result<(), ()> {
    let client_id = option(options, "--client-id")?;
    let redirect_uri = option(options, "--redirect-uri")?;
    let state = read_secret_file(option(options, "--state-file")?)?;
    let bundle_file = option(options, "--bundle-file")?;
    let env_file = option(options, "--env-file")?;
    let client_mode = ClientMode::parse(option(options, "--client-mode")?).map_err(|_| ())?;
    let (client_secret, callback_uri) = read_authorization_input()?;
    let authorization_code =
        parse_structure_resolver_callback(&callback_uri, redirect_uri, &state).map_err(|_| ())?;
    let client = StructureResolverProvisioningClient::new(client_id, client_secret.as_str())
        .map_err(|_| ())?;
    let authorization =
        exchange_structure_resolver_authorization(client, &authorization_code, PROVISION_TIMEOUT)
            .await
            .map_err(|_| ())?;
    let bundle = AuthorizationBundle::from_authorization(
        client_mode,
        client_id,
        client_secret,
        redirect_uri,
        authorization,
        next_credential_revision(read_environment_value(
            env_file,
            "STRUCTURE_RESOLVER_CREDENTIAL_REVISION",
        )?),
    );
    write_secret_file(bundle_file, &serde_json::to_vec(&bundle).map_err(|_| ())?)
}

async fn probe(options: &BTreeMap<String, String>) -> Result<(), ()> {
    let bundle = read_bundle(option(options, "--bundle-file")?)?;
    let structure_ids = option(options, "--structure-ids")?
        .split(',')
        .map(|id| id.trim().parse::<i64>().map_err(|_| ()))
        .collect::<Result<Vec<_>, _>>()?;
    let authorization = StructureResolverProvisioningAuthorization::from_parts(
        bundle.access_token,
        bundle.refresh_token,
        bundle.character_id,
    );
    let report = probe_structure_resolver_coverage(
        &authorization,
        &structure_ids,
        &StructureResolverProvisioningEndpoints::official(),
        PROVISION_TIMEOUT,
    )
    .await
    .map_err(|_| ())?;
    let coverage = match report.coverage() {
        StructureProbeCoverage::Full => "full",
        StructureProbeCoverage::Partial => "partial",
        StructureProbeCoverage::Denied => "denied",
        StructureProbeCoverage::Indeterminate => "indeterminate",
    };
    println!(
        "{coverage} successful={} denied={} indeterminate={}",
        report.successful(),
        report.denied(),
        report.indeterminate()
    );
    Ok(())
}

fn env_values(options: &BTreeMap<String, String>) -> Result<(), ()> {
    let bundle = read_bundle(option(options, "--bundle-file")?)?;
    let values_file = option(options, "--values-file")?;
    let enabled = option(options, "--enabled")?;
    if enabled != "true" {
        return Err(());
    }
    let mut values = vec![
        ("STRUCTURE_RESOLVER_ENABLED", "true".to_string()),
        (
            "STRUCTURE_RESOLVER_CHARACTER_ID",
            bundle.character_id.to_string(),
        ),
        ("STRUCTURE_RESOLVER_REFRESH_TOKEN", bundle.refresh_token),
        (
            "STRUCTURE_RESOLVER_CREDENTIAL_REVISION",
            bundle.credential_revision,
        ),
        ("STRUCTURE_RESOLVER_REDIRECT_URI", bundle.redirect_uri),
    ];
    match bundle.client_mode {
        ClientMode::Existing => {
            values.push(("EVE_CLIENT_ID", bundle.client_id));
            values.push(("EVE_CLIENT_SECRET", bundle.client_secret));
        }
        ClientMode::Dedicated => {
            values.push(("STRUCTURE_RESOLVER_CLIENT_ID", bundle.client_id));
            values.push(("STRUCTURE_RESOLVER_CLIENT_SECRET", bundle.client_secret));
        }
    }
    let mut rendered = String::new();
    for (key, value) in values {
        if value.contains(['\n', '\r', '\t']) {
            return Err(());
        }
        rendered.push_str(key);
        rendered.push('\t');
        rendered.push_str(&value);
        rendered.push('\n');
    }
    write_secret_file(values_file, rendered.as_bytes())
}

impl AuthorizationBundle {
    fn from_authorization(
        client_mode: ClientMode,
        client_id: &str,
        client_secret: String,
        redirect_uri: &str,
        authorization: StructureResolverProvisioningAuthorization,
        credential_revision: String,
    ) -> Self {
        Self {
            client_mode,
            client_id: client_id.to_string(),
            client_secret,
            redirect_uri: redirect_uri.to_string(),
            access_token: authorization.access_token().to_string(),
            refresh_token: authorization.refresh_token().to_string(),
            character_id: authorization.character_id(),
            credential_revision,
        }
    }
}

fn read_bundle(path: &str) -> Result<AuthorizationBundle, ()> {
    serde_json::from_slice(&fs::read(path).map_err(|_| ())?).map_err(|_| ())
}

fn fresh_state() -> String {
    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn read_authorization_input() -> Result<(String, String), ()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|_| ())?;
    let mut lines = input.lines();
    let client_secret = lines.next().filter(|value| !value.is_empty()).ok_or(())?;
    let callback_uri = lines.next().filter(|value| !value.is_empty()).ok_or(())?;
    if lines.next().is_some() || client_secret.contains('\r') || callback_uri.contains('\r') {
        return Err(());
    }
    Ok((client_secret.to_string(), callback_uri.to_string()))
}

fn read_secret_file(path: &str) -> Result<String, ()> {
    let value = fs::read_to_string(path)
        .map_err(|_| ())
        .map(|value| value.trim_end_matches(['\n', '\r']).to_string())?;
    if value.is_empty() {
        return Err(());
    }
    Ok(value)
}

fn write_secret_file(path: impl AsRef<Path>, contents: &[u8]) -> Result<(), ()> {
    let path = path.as_ref();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| ())?;
    file.write_all(contents).map_err(|_| ())?;
    file.sync_all().map_err(|_| ())?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| ())?;
    Ok(())
}

fn read_environment_value(path: &str, key: &str) -> Result<Option<String>, ()> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
    };
    Ok(contents
        .lines()
        .filter_map(|line| line.strip_prefix(&format!("{key}=")))
        .next_back()
        .map(ToOwned::to_owned))
}

fn next_credential_revision(previous: Option<String>) -> String {
    match previous {
        Some(value) if value.trim().is_empty() => "1".to_string(),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_add(1))
            .map(|value| value.to_string())
            .unwrap_or_else(|| format!("{value}-rotated")),
        None => "1".to_string(),
    }
}
