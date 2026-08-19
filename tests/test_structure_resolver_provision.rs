use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use killbot_rust::structure_resolver::{
    build_structure_resolver_authorization_url,
    exchange_structure_resolver_authorization_with_endpoints, parse_structure_resolver_callback,
    probe_structure_resolver_coverage, StructureProbeCoverage, StructureResolverConfig,
    StructureResolverProvisioningAuthorization, StructureResolverProvisioningClient,
    StructureResolverProvisioningEndpoints,
};
use serde::Serialize;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use url::Url;

const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAzAIQj8deXaYHITcomlORNlShy3YHpHSqWoTnQlugf+/8VAtZ\nOHHusoi6asRXDT0icmwMM6GeCBTusp3nEMZwli/NNYMImSO9s15ZkwPRtMXDA6E2\nJNMmeYeojf7QHM5OdO3MOFa/LgxHLDkI9aghAlblGkSxzNXQ+Ii8WERbO3sbCYCl\n1bS91rdBUKEAXkzSSU9VdmuJbcFwAxTL/Sf5wbAl6urFf2r9woJMdysJCA3euTdu\njGOdEAZxSg+S1e5OU+KmX2sVtaY0+pqKwRku+Awcslu/EEcgQLfccTaekuqySTTY\nteU8UI6V+N1D5jNhFLJ9j20l5udz9bzO7FfHSQIDAQABAoIBAA7+PdpbRCesyoxZ\n4e2Jo7vy91sdJw2il1yEtPxPAJo2eHxywxFfajQL0WuEV4N9ETmIkFMBFzyv0SUm\nbrNwahjXlYTPxwN+OXRjxECGQNTAzgbHw9NsA0FeQ3iAGCptzR1R1rbzRSSsuVRa\nMrrfKuHhof/OuaR8uFlzryfriiryTR/h/psHsNUIR0xQrLMMwlIOw+/MTqPqGK0g\nfXrofUhxf2+vGAaTHZRZ+uD6USDkrBml64wyqz5f5NHVmA3VnBaIk6R2uDHQ9YAQ\nuIgS0dlL99Ddz5LqvuWriolHJ7J0mgWpz/DfgC5DFY/URjqrYxnuyk6BTZRBFPvz\na+G1/ssCgYEA52I4tpS/yMfaUNhBHXjxwF04af1RnXlOSCxMLzvJUtuBYxVfMCu3\njR6owuKwNSHADXz7bDLUztbLqekvCIuhrBwz0FFRwXk/yQDRfnKPrD9Q1JuzOMk8\nzurd5rrd3iz58C93tdmf4aYIK9V2+F/ozFDnGmvvwrCb9mfg1pcDSC8CgYEA4bZD\nXT9mVbopYQL+Kwk1Qp4xVgKWxprTRhHg1+QNWRBKI9nZThHQL7EUwvVWHx+H/bSQ\n/JOj1GnNTgX+fxsXx4xX2IZA1o8zWpFOWD5OuuwVth59xJxvtmERyJjWgdxRTrkX\n29diWFBC3piNYlsS3WecSHfH5nbq4GLP8LnKkgcCgYAS17vYmop3tlbACKxc0xGU\n4cKLVxbDZTKLzBe0LQE7HycNQ5tJ1/WNp3aE0GMbIJF8R7ZN3GHaKkHRp2yuHHjh\nBDbv+v9WayJXoxpsWrX6h/l0Ju3UbQbnrta9SHBy/GSqO6NbCsrrXFMEBtE2btEN\nenUngKy4xRseWN1FfGzG/wKBgQCy1AlDU/vsZ/Zo2kouJrl/8n38O0jiScCif3+5\nDQJWUkWraep1pD9hydc9L8vwFLdWFz3YH9FpdfonmzAr3HdWrqba8mNkm0iAtSdx\nWsxd5La++CGFKLyJrxa76/voH3p7+MIid99/QPf6DLvX9XhY2sJD2EMVIZqt9Rvz\nCgCo+QKBgFM5TePSNXyV9KBglRzv3TGdDyPwxoGfMD484aKCStHuIRp+MwD5r2el\nbtZijAlHlB6S7RwpGYdKcEsBzuD1pcNaP4c/PIX3Kejr8jdVSJyUmo7NqJfh4aH+\n4sUl7JBUSVNiCUPq06FgvtQrDhERkzcoxNF2pIptWf1fkY8kbl3x\n-----END RSA PRIVATE KEY-----\n";
const TEST_JWK_N: &str = "zAIQj8deXaYHITcomlORNlShy3YHpHSqWoTnQlugf-_8VAtZOHHusoi6asRXDT0icmwMM6GeCBTusp3nEMZwli_NNYMImSO9s15ZkwPRtMXDA6E2JNMmeYeojf7QHM5OdO3MOFa_LgxHLDkI9aghAlblGkSxzNXQ-Ii8WERbO3sbCYCl1bS91rdBUKEAXkzSSU9VdmuJbcFwAxTL_Sf5wbAl6urFf2r9woJMdysJCA3euTdujGOdEAZxSg-S1e5OU-KmX2sVtaY0-pqKwRku-Awcslu_EEcgQLfccTaekuqySTTYteU8UI6V-N1D5jNhFLJ9j20l5udz9bzO7FfHSQ";

#[derive(Serialize)]
struct Claims {
    aud: Vec<String>,
    exp: usize,
    iss: String,
    scp: Vec<String>,
    sub: String,
}

fn signed_access_token(scopes: &[&str]) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("provisioning-test-key".to_string());
    encode(
        &header,
        &Claims {
            aud: vec![
                "existing-eve-application".to_string(),
                "EVE Online".to_string(),
            ],
            exp: 4_102_444_800,
            iss: "https://login.eveonline.com".to_string(),
            scp: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            sub: "CHARACTER:EVE:90000001".to_string(),
        },
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY.as_bytes()).expect("test RSA key"),
    )
    .expect("sign test access token")
}

struct TestHttpServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    handle: JoinHandle<()>,
}

impl TestHttpServer {
    fn start(replies: Vec<(u16, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP fixture");
        let address = listener.local_addr().expect("HTTP fixture address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded_requests = requests.clone();
        let handle = std::thread::spawn(move || {
            for (status, body) in replies {
                let (mut stream, _) = listener.accept().expect("accept HTTP fixture request");
                let mut request = [0_u8; 4096];
                let length = stream
                    .read(&mut request)
                    .expect("read HTTP fixture request");
                recorded_requests
                    .lock()
                    .expect("lock HTTP fixture requests")
                    .push(String::from_utf8_lossy(&request[..length]).into_owned());
                let response = format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write HTTP fixture response");
            }
        });
        Self {
            base_url: format!("http://{address}/"),
            requests,
            handle,
        }
    }

    fn finish(self) -> Vec<String> {
        self.handle.join().expect("join HTTP fixture");
        self.requests
            .lock()
            .expect("lock HTTP fixture requests")
            .clone()
    }
}

#[test]
fn provisioner_requests_only_the_structure_scope_and_rejects_a_mismatched_callback_state() {
    let authorization_url = build_structure_resolver_authorization_url(
        "existing-eve-application",
        "https://resolver.example.test/oauth/callback",
        "fresh-state-value",
    )
    .expect("valid registered redirect builds an authorization URL");
    let query = Url::parse(&authorization_url)
        .expect("authorization URL is valid")
        .query_pairs()
        .into_owned()
        .collect::<Vec<_>>();

    assert_eq!(
        query,
        vec![
            ("response_type".to_string(), "code".to_string()),
            (
                "client_id".to_string(),
                "existing-eve-application".to_string()
            ),
            (
                "redirect_uri".to_string(),
                "https://resolver.example.test/oauth/callback".to_string(),
            ),
            (
                "scope".to_string(),
                "esi-universe.read_structures.v1".to_string(),
            ),
            ("state".to_string(), "fresh-state-value".to_string()),
        ]
    );

    let error = parse_structure_resolver_callback(
        "https://resolver.example.test/oauth/callback?code=one-time-code&state=wrong-state",
        "https://resolver.example.test/oauth/callback",
        "fresh-state-value",
    )
    .expect_err("state mismatch must stop before token exchange");
    assert!(!error.to_string().contains("one-time-code"));

    assert!(
        parse_structure_resolver_callback(
            "https://resolver.example.test/oauth/callback?code=one-time-code&state=fresh-state-value&state=wrong-state",
            "https://resolver.example.test/oauth/callback",
            "fresh-state-value",
        )
        .is_err(),
        "ambiguous callback state must stop before token exchange"
    );
}

#[test]
fn dedicated_application_credentials_override_shared_standings_credentials() {
    let config = StructureResolverConfig::from_settings([
        ("STRUCTURE_RESOLVER_ENABLED", "true"),
        ("STRUCTURE_RESOLVER_CHARACTER_ID", "90000001"),
        ("STRUCTURE_RESOLVER_REFRESH_TOKEN", "refresh-token"),
        (
            "STRUCTURE_RESOLVER_CLIENT_ID",
            "dedicated-structure-application",
        ),
        (
            "STRUCTURE_RESOLVER_CLIENT_SECRET",
            "dedicated-structure-secret",
        ),
    ])
    .expect("a dedicated resolver application must not require shared standings credentials");

    assert!(config.is_enabled());
    assert_eq!(config.character_id(), Some(90_000_001));
}

#[test]
fn compose_blank_resolver_overrides_fall_back_to_shared_standings_credentials() {
    let config = StructureResolverConfig::from_settings([
        ("STRUCTURE_RESOLVER_ENABLED", "true"),
        ("STRUCTURE_RESOLVER_CHARACTER_ID", "90000001"),
        ("STRUCTURE_RESOLVER_REFRESH_TOKEN", "refresh-token"),
        ("STRUCTURE_RESOLVER_CLIENT_ID", ""),
        ("STRUCTURE_RESOLVER_CLIENT_SECRET", ""),
        ("EVE_CLIENT_ID", "existing-eve-application"),
        ("EVE_CLIENT_SECRET", "existing-eve-secret"),
    ])
    .expect("Compose's empty default overrides must not disable the shared application fallback");

    assert!(config.is_enabled());
    assert_eq!(config.character_id(), Some(90_000_001));

    assert!(
        StructureResolverConfig::from_settings([
            ("STRUCTURE_RESOLVER_ENABLED", "true"),
            ("STRUCTURE_RESOLVER_CHARACTER_ID", "90000001"),
            ("STRUCTURE_RESOLVER_REFRESH_TOKEN", "refresh-token"),
            (
                "STRUCTURE_RESOLVER_CLIENT_ID",
                "dedicated-structure-application"
            ),
            ("STRUCTURE_RESOLVER_CLIENT_SECRET", ""),
            ("EVE_CLIENT_ID", "existing-eve-application"),
            ("EVE_CLIENT_SECRET", "existing-eve-secret"),
        ])
        .is_err(),
        "a partially configured dedicated override must not silently use a shared secret"
    );
}

#[tokio::test]
async fn provisioner_exchanges_code_and_accepts_only_signed_exact_scope_claims() {
    let access_token = signed_access_token(&["esi-universe.read_structures.v1"]);
    let jwks = TestHttpServer::start(vec![{
        (
            200,
            format!(
                r#"{{"keys":[{{"kty":"RSA","kid":"provisioning-test-key","alg":"RS256","n":"{TEST_JWK_N}","e":"AQAB"}}]}}"#
            ),
        )
    }]);
    let metadata = TestHttpServer::start(vec![{
        (200, format!(r#"{{"jwks_uri":"{}jwks"}}"#, jwks.base_url))
    }]);
    let token = TestHttpServer::start(vec![{
        (
            200,
            format!(
                r#"{{"access_token":"{access_token}","refresh_token":"rotated-refresh-token"}}"#
            ),
        )
    }]);

    let authorization = exchange_structure_resolver_authorization_with_endpoints(
        StructureResolverProvisioningClient::new("existing-eve-application", "client-secret")
            .expect("complete client credentials"),
        "one-time-code",
        StructureResolverProvisioningEndpoints::for_test(
            "http://127.0.0.1:9/",
            format!("{}token", token.base_url),
            format!("{}metadata", metadata.base_url),
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("signed exact-scope authorization succeeds");

    assert_eq!(authorization.character_id(), 90_000_001);
    assert_eq!(authorization.refresh_token(), "rotated-refresh-token");
    assert!(token
        .finish()
        .join("")
        .contains("grant_type=authorization_code&code=one-time-code"));
    metadata.finish();
    jwks.finish();
}

#[tokio::test]
async fn provisioner_rejects_extra_scope_claims_without_leaking_authorization_material() {
    let access_token = signed_access_token(&[
        "esi-universe.read_structures.v1",
        "esi-characters.read_contacts.v1",
    ]);
    let jwks = TestHttpServer::start(vec![{
        (
            200,
            format!(
                r#"{{"keys":[{{"kty":"RSA","kid":"provisioning-test-key","alg":"RS256","n":"{TEST_JWK_N}","e":"AQAB"}}]}}"#
            ),
        )
    }]);
    let metadata = TestHttpServer::start(vec![{
        (200, format!(r#"{{"jwks_uri":"{}jwks"}}"#, jwks.base_url))
    }]);
    let token = TestHttpServer::start(vec![{
        (
            200,
            format!(
                r#"{{"access_token":"{access_token}","refresh_token":"fixture-refresh-secret"}}"#
            ),
        )
    }]);

    let result = exchange_structure_resolver_authorization_with_endpoints(
        StructureResolverProvisioningClient::new(
            "existing-eve-application",
            "fixture-client-secret",
        )
        .expect("complete client credentials"),
        "fixture-one-time-code",
        StructureResolverProvisioningEndpoints::for_test(
            "http://127.0.0.1:9/",
            format!("{}token", token.base_url),
            format!("{}metadata", metadata.base_url),
        ),
        Duration::from_secs(2),
    )
    .await;
    let Err(error) = result else {
        panic!("extra OAuth scope must be rejected")
    };

    let rendered = error.to_string();
    assert!(!rendered.contains("fixture-client-secret"));
    assert!(!rendered.contains("fixture-one-time-code"));
    assert!(!rendered.contains("fixture-refresh-secret"));
    token.finish();
    metadata.finish();
    jwks.finish();
}

#[tokio::test]
async fn provisioner_reports_full_partial_denied_and_indeterminate_known_probe_coverage() {
    let authorization = StructureResolverProvisioningAuthorization::from_parts(
        "access-token".to_string(),
        "refresh-token".to_string(),
        90_000_001,
    );
    let full = TestHttpServer::start(vec![(200, "{}".to_string())]);
    let full_report = probe_structure_resolver_coverage(
        &authorization,
        &[1],
        &StructureResolverProvisioningEndpoints::for_test(
            &full.base_url,
            "http://127.0.0.1:9/token",
            "http://127.0.0.1:9/metadata",
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("full probe response");
    assert_eq!(full_report.coverage(), StructureProbeCoverage::Full);
    full.finish();

    let partial = TestHttpServer::start(vec![(200, "{}".to_string()), (403, "{}".to_string())]);
    let partial_report = probe_structure_resolver_coverage(
        &authorization,
        &[1, 2],
        &StructureResolverProvisioningEndpoints::for_test(
            &partial.base_url,
            "http://127.0.0.1:9/token",
            "http://127.0.0.1:9/metadata",
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("partial probe response");
    assert_eq!(partial_report.coverage(), StructureProbeCoverage::Partial);
    partial.finish();

    let denied = TestHttpServer::start(vec![(403, "{}".to_string()), (404, "{}".to_string())]);
    let denied_report = probe_structure_resolver_coverage(
        &authorization,
        &[1, 2],
        &StructureResolverProvisioningEndpoints::for_test(
            &denied.base_url,
            "http://127.0.0.1:9/token",
            "http://127.0.0.1:9/metadata",
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("denied probe response");
    assert_eq!(denied_report.coverage(), StructureProbeCoverage::Denied);
    denied.finish();

    let indeterminate = TestHttpServer::start(vec![(503, "{}".to_string())]);
    let indeterminate_report = probe_structure_resolver_coverage(
        &authorization,
        &[1],
        &StructureResolverProvisioningEndpoints::for_test(
            &indeterminate.base_url,
            "http://127.0.0.1:9/token",
            "http://127.0.0.1:9/metadata",
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("indeterminate probe response");
    assert_eq!(
        indeterminate_report.coverage(),
        StructureProbeCoverage::Indeterminate
    );
    indeterminate.finish();
}
