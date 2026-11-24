use super::{FeedError, FeedHealthProvider, KillmailFeed};
use crate::config::AppConfig;
use crate::models::{R2z2KillmailResponse, R2z2SequenceResponse, ZkDataNoEsi};
use rand::Rng;
use reqwest::{Client, StatusCode};
use serenity::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info, warn};

const R2Z2_BASE_URL: &str = "https://r2z2.zkillboard.com";
const MAX_BACKOFF_SECS: f64 = 60.0;
const FIXED_429_BASE_SECS: f64 = 10.0;
const SUCCESS_SLEEP_MS: u64 = 100;

#[derive(serde::Serialize, serde::Deserialize)]
struct R2z2Checkpoint {
    sequence: i64,
}

struct R2z2State {
    sequence: i64, // 0 = need initial fetch
    consecutive_404s: u64,
    first_404_at: Option<Instant>,
    backoff_exp: u32,
}

#[derive(serde::Deserialize)]
struct R2z2SequenceProbe {
    #[serde(default)]
    killmail_id: Option<i64>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    zkb: Option<R2z2SequenceProbeZkb>,
}

#[derive(serde::Deserialize)]
struct R2z2SequenceProbeZkb {
    #[serde(default, rename = "href")]
    href: Option<String>,
}

pub struct R2z2Feed {
    client: Client,
    state: Mutex<R2z2State>,
    config: Arc<AppConfig>,
    checkpoint_path: PathBuf,
    base_url: String,
}

impl R2z2Feed {
    pub fn new(config: &Arc<AppConfig>) -> Self {
        Self::with_endpoint(
            config,
            R2Z2_BASE_URL.to_string(),
            PathBuf::from("config/r2z2_sequence.json"),
        )
    }

    fn with_endpoint(config: &Arc<AppConfig>, base_url: String, checkpoint_path: PathBuf) -> Self {
        let initial_sequence = Self::load_checkpoint(&checkpoint_path);

        let client = Client::builder()
            .user_agent("killbot-rust R2Z2 feed consumer")
            .connect_timeout(Duration::from_secs(config.r2z2_connect_timeout_secs))
            .timeout(Duration::from_secs(config.r2z2_request_timeout_secs))
            .build()
            .expect("Failed to build R2Z2 HTTP client");

        R2z2Feed {
            client,
            state: Mutex::new(R2z2State {
                sequence: initial_sequence,
                consecutive_404s: 0,
                first_404_at: None,
                backoff_exp: 0,
            }),
            config: Arc::clone(config),
            checkpoint_path,
            base_url,
        }
    }

    fn load_checkpoint(path: &PathBuf) -> i64 {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str::<R2z2Checkpoint>(&contents) {
                Ok(checkpoint) => {
                    info!(
                        "R2Z2: resuming from persisted checkpoint {}",
                        checkpoint.sequence
                    );
                    checkpoint.sequence
                }
                Err(_) => {
                    warn!("R2Z2: corrupt checkpoint, discarding and fetching fresh sequence");
                    let _ = std::fs::remove_file(path);
                    0
                }
            },
            Err(_) => {
                info!("R2Z2: no checkpoint found, will fetch fresh sequence");
                0
            }
        }
    }

    fn save_checkpoint(&self, sequence: i64) {
        let checkpoint = R2z2Checkpoint { sequence };
        let tmp_path = self.checkpoint_path.with_extension("tmp");
        match serde_json::to_string(&checkpoint) {
            Ok(data) => match std::fs::write(&tmp_path, &data) {
                Ok(()) => {
                    if let Err(e) = std::fs::rename(&tmp_path, &self.checkpoint_path) {
                        warn!("R2Z2: failed to rename checkpoint file: {}", e);
                    }
                }
                Err(e) => warn!("R2Z2: failed to write checkpoint tmp file: {}", e),
            },
            Err(e) => warn!("R2Z2: failed to serialize checkpoint: {}", e),
        }
    }

    /// Fetch the current latest sequence from R2Z2's sequence.json.
    /// Retries indefinitely on failure with exponential backoff.
    async fn fetch_sequence(&self, state: &mut R2z2State) -> i64 {
        let url = format!("{}/ephemeral/sequence.json", self.base_url);

        loop {
            match self.client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        let wait = jittered_wait(FIXED_429_BASE_SECS);
                        warn!(
                            "R2Z2: 429 fetching sequence.json, waiting {:.1}s",
                            wait.as_secs_f64()
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    if status.is_server_error() {
                        let wait = exponential_backoff(state.backoff_exp);
                        state.backoff_exp += 1;
                        error!(
                            "R2Z2: {} fetching sequence.json, backoff {:.1}s",
                            status,
                            wait.as_secs_f64()
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    if !status.is_success() {
                        let wait = exponential_backoff(state.backoff_exp);
                        state.backoff_exp += 1;
                        error!(
                            "R2Z2: unexpected {} fetching sequence.json, backoff {:.1}s",
                            status,
                            wait.as_secs_f64()
                        );
                        tokio::time::sleep(wait).await;
                        continue;
                    }

                    // Success — parse the response
                    match response.json::<R2z2SequenceResponse>().await {
                        Ok(seq) => {
                            state.backoff_exp = 0;
                            info!("R2Z2: fetched latest sequence {}", seq.sequence);
                            return seq.sequence;
                        }
                        Err(e) => {
                            let wait = exponential_backoff(state.backoff_exp);
                            state.backoff_exp += 1;
                            error!(
                                "R2Z2: parse error fetching sequence.json: {}, backoff {:.1}s",
                                e,
                                wait.as_secs_f64()
                            );
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    let wait = exponential_backoff(state.backoff_exp);
                    state.backoff_exp += 1;
                    error!(
                        "R2Z2: transport error fetching sequence.json: {}, backoff {:.1}s",
                        e,
                        wait.as_secs_f64()
                    );
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }
        }
    }

    /// Whether the 404 tracking window has reached either configured threshold.
    fn needs_resync(&self, state: &R2z2State) -> bool {
        if state.consecutive_404s >= self.config.r2z2_max_consecutive_404s {
            return true;
        }
        if let Some(first_404) = state.first_404_at {
            if first_404.elapsed().as_secs() >= self.config.r2z2_resync_timeout_secs {
                return true;
            }
        }
        false
    }

    /// Some R2Z2 sequences can temporarily contain a null placeholder object.
    /// These are not valid killmails and should be skipped so the feed does not stall.
    fn is_null_placeholder_sequence(body: &str) -> bool {
        let parsed = match serde_json::from_str::<R2z2SequenceProbe>(body) {
            Ok(v) => v,
            Err(_) => return false,
        };

        let missing_killmail_id = parsed.killmail_id.is_none();
        let missing_hash = parsed.hash.is_none();
        let invalid_href = parsed
            .zkb
            .as_ref()
            .and_then(|z| z.href.as_deref())
            .map(|href| href.ends_with("/killmails///"))
            .unwrap_or(true);

        missing_killmail_id && missing_hash && invalid_href
    }
}

#[async_trait]
impl KillmailFeed for R2z2Feed {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError> {
        let mut state = self.state.lock().await;

        // If sequence is 0, we need to fetch from sequence.json
        if state.sequence == 0 {
            state.sequence = self.fetch_sequence(&mut state).await;
            self.save_checkpoint(state.sequence);
        }

        let url = format!("{}/ephemeral/{}.json", self.base_url, state.sequence);

        match self.client.get(&url).send().await {
            Ok(response) => {
                let status = response.status();

                if status == StatusCode::TOO_MANY_REQUESTS {
                    let wait = jittered_wait(FIXED_429_BASE_SECS);
                    warn!(
                        "R2Z2: 429 on sequence {}, waiting {:.1}s",
                        state.sequence,
                        wait.as_secs_f64()
                    );
                    tokio::time::sleep(wait).await;
                    return Ok(None);
                }

                if status.is_server_error() {
                    let wait = exponential_backoff(state.backoff_exp);
                    state.backoff_exp += 1;
                    error!(
                        "R2Z2: {} on sequence {}, backoff {:.1}s",
                        status,
                        state.sequence,
                        wait.as_secs_f64()
                    );
                    tokio::time::sleep(wait).await;
                    return Ok(None);
                }

                if status == StatusCode::NOT_FOUND {
                    let missing_sequence = state.sequence;
                    let latest_sequence = self.fetch_sequence(&mut state).await;

                    if missing_sequence < latest_sequence {
                        warn!(
                            "R2Z2: sequence {} returned 404 while latest is {}, skipping missing historical sequence",
                            missing_sequence, latest_sequence
                        );
                        state.sequence = missing_sequence + 1;
                        state.consecutive_404s = 0;
                        state.first_404_at = None;
                        state.backoff_exp = 0;
                        self.save_checkpoint(state.sequence);
                        tokio::time::sleep(Duration::from_millis(SUCCESS_SLEEP_MS)).await;
                        return Ok(None);
                    }

                    // 404 at or ahead of the current head — not yet available.
                    state.backoff_exp = 0;
                    state.consecutive_404s += 1;
                    if state.first_404_at.is_none() {
                        state.first_404_at = Some(Instant::now());
                    }

                    if self.needs_resync(&state) {
                        warn!(
                            "R2Z2: {} consecutive 404s; discovery sequence {} is not ahead of pending sequence {}, retaining cursor",
                            state.consecutive_404s, latest_sequence, missing_sequence
                        );
                        state.consecutive_404s = 0;
                        state.first_404_at = None;
                    }

                    // Wait poll_interval before returning
                    tokio::time::sleep(Duration::from_secs(self.config.r2z2_poll_interval_secs))
                        .await;
                    return Ok(None);
                }

                if !status.is_success() {
                    return Err(FeedError::Transport(format!(
                        "R2Z2: unexpected status {} on sequence {}",
                        status, state.sequence
                    )));
                }

                // 200 OK — parse killmail
                state.backoff_exp = 0;
                state.consecutive_404s = 0;
                state.first_404_at = None;

                let body = response
                    .text()
                    .await
                    .map_err(|e| FeedError::Transport(e.to_string()))?;

                if Self::is_null_placeholder_sequence(&body) {
                    warn!(
                        "R2Z2: sequence {} is a null placeholder, skipping",
                        state.sequence
                    );
                    state.sequence += 1;
                    self.save_checkpoint(state.sequence);
                    // Rate-limit per R2Z2 docs: sleep 100ms between successful fetches (max 10 req/s)
                    tokio::time::sleep(Duration::from_millis(SUCCESS_SLEEP_MS)).await;
                    return Ok(None);
                }

                let km: R2z2KillmailResponse = serde_json::from_str(&body).map_err(|e| {
                    FeedError::Parse(format!(
                        "R2Z2: JSON error on sequence {}: {}",
                        state.sequence, e
                    ))
                })?;

                // Advance sequence and save checkpoint
                state.sequence += 1;
                self.save_checkpoint(state.sequence);

                let zk_data = ZkDataNoEsi {
                    kill_id: km.killmail_id,
                    zkb: km.zkb,
                    inline_killmail: km.esi,
                };

                // Rate-limit per R2Z2 docs: sleep 100ms between successful fetches (max 10 req/s)
                tokio::time::sleep(Duration::from_millis(SUCCESS_SLEEP_MS)).await;

                Ok(Some(zk_data))
            }
            Err(e) => {
                let wait = exponential_backoff(state.backoff_exp);
                state.backoff_exp += 1;
                error!(
                    "R2Z2: transport error on sequence {}: {}, backoff {:.1}s",
                    state.sequence,
                    e,
                    wait.as_secs_f64()
                );
                tokio::time::sleep(wait).await;
                Ok(None)
            }
        }
    }

    fn health_provider(&self) -> FeedHealthProvider {
        FeedHealthProvider::R2z2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex as StdMutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Replies = HashMap<String, VecDeque<(u16, serde_json::Value)>>;

    struct HttpFixture {
        base_url: String,
        directory: tempfile::TempDir,
        replies: Arc<StdMutex<Replies>>,
        requests: Arc<StdMutex<Vec<String>>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl HttpFixture {
        async fn new(sequence: i64) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let replies = Arc::new(StdMutex::new(Replies::new()));
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let server_replies = replies.clone();
            let server_requests = requests.clone();
            let server = tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                        let mut buffer = [0; 1024];
                        let count = stream.read(&mut buffer).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        request.extend_from_slice(&buffer[..count]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    let path = request.split_whitespace().nth(1).unwrap().to_string();
                    server_requests.lock().unwrap().push(path.clone());
                    let (status, body) = {
                        let mut replies = server_replies.lock().unwrap();
                        match replies.get_mut(&path) {
                            Some(queue) if queue.len() > 1 => queue.pop_front().unwrap(),
                            Some(queue) => queue.front().unwrap().clone(),
                            None => (404, serde_json::json!({})),
                        }
                    };
                    let body = body.to_string();
                    let response = format!(
                        "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                }
            });
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(
                directory.path().join("sequence.json"),
                serde_json::json!({"sequence": sequence}).to_string(),
            )
            .unwrap();
            Self {
                base_url,
                directory,
                replies,
                requests,
                server,
            }
        }

        fn reply(&self, path: &str, status: u16, body: serde_json::Value) {
            self.replies
                .lock()
                .unwrap()
                .insert(path.to_string(), VecDeque::from([(status, body)]));
        }

        fn feed(&self, config: &Arc<AppConfig>) -> R2z2Feed {
            R2z2Feed::with_endpoint(
                config,
                self.base_url.clone(),
                self.directory.path().join("sequence.json"),
            )
        }
    }

    impl Drop for HttpFixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    fn fixture_config() -> Arc<AppConfig> {
        Arc::new(
            serde_json::from_value(serde_json::json!({
                "discord_bot_token": "fixture", "discord_client_id": 0,
                "eve_client_id": "", "eve_client_secret": "",
                "r2z2_poll_interval_secs": 1,
                "r2z2_max_consecutive_404s": 1,
                "r2z2_resync_timeout_secs": 300,
                "r2z2_request_timeout_secs": 2,
                "r2z2_connect_timeout_secs": 2
            }))
            .unwrap(),
        )
    }

    fn fixture_killmail(killmail_id: i64) -> serde_json::Value {
        serde_json::json!({
            "killmail_id": killmail_id, "hash": "fixture",
            "zkb": {
                "hash": "fixture", "fittedValue": 1, "droppedValue": 0,
                "destroyedValue": 1, "totalValue": 1, "points": 1,
                "npc": false, "solo": false, "awox": false
            }
        })
    }

    async fn next(feed: &R2z2Feed) -> Option<ZkDataNoEsi> {
        tokio::time::timeout(Duration::from_secs(10), feed.next())
            .await
            .expect("feed iteration deadline")
            .unwrap()
    }

    #[tokio::test]
    async fn count_resync_does_not_replay_and_restart_resumes_pending_sequence() {
        let fixture = HttpFixture::new(100).await;
        fixture.reply("/ephemeral/100.json", 200, fixture_killmail(9001));
        fixture.reply(
            "/ephemeral/sequence.json",
            200,
            serde_json::json!({"sequence": 100}),
        );
        let config = fixture_config();
        let feed = fixture.feed(&config);
        assert_eq!(next(&feed).await.unwrap().kill_id, 9001);
        let started = Instant::now();
        assert!(next(&feed).await.is_none());
        let waited = started.elapsed();
        assert!(
            next(&feed).await.is_none(),
            "resync must not emit sequence 100 again"
        );
        assert!(
            waited >= Duration::from_secs(1),
            "resync must retain the polling delay"
        );
        drop(feed);
        fixture.reply("/ephemeral/101.json", 200, fixture_killmail(9002));
        let restarted = fixture.feed(&config);
        assert_eq!(next(&restarted).await.unwrap().kill_id, 9002);
        assert_eq!(
            fixture.requests.lock().unwrap().as_slice(),
            [
                "/ephemeral/100.json",
                "/ephemeral/101.json",
                "/ephemeral/sequence.json",
                "/ephemeral/101.json",
                "/ephemeral/sequence.json",
                "/ephemeral/101.json",
            ]
        );
    }

    #[tokio::test]
    async fn elapsed_resync_retains_cursor_and_delay_with_stale_or_equal_head() {
        for head in [100, 101] {
            let fixture = HttpFixture::new(100).await;
            fixture.reply("/ephemeral/100.json", 200, fixture_killmail(9001));
            fixture.reply(
                "/ephemeral/sequence.json",
                200,
                serde_json::json!({"sequence": head}),
            );
            let mut config = fixture_config();
            let settings = Arc::make_mut(&mut config);
            settings.r2z2_max_consecutive_404s = 100;
            settings.r2z2_resync_timeout_secs = 1;
            let feed = fixture.feed(&config);
            assert_eq!(next(&feed).await.unwrap().kill_id, 9001);
            for _ in 0..3 {
                let started = Instant::now();
                assert!(next(&feed).await.is_none());
                assert!(started.elapsed() >= Duration::from_secs(1));
            }
            assert_eq!(
                fixture
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|path| path.as_str() == "/ephemeral/100.json")
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn resync_does_not_refetch_head_and_skip_newly_available_pending_sequence() {
        let fixture = HttpFixture::new(100).await;
        fixture.reply("/ephemeral/100.json", 200, fixture_killmail(9001));
        fixture.reply("/ephemeral/103.json", 200, fixture_killmail(9003));
        fixture.replies.lock().unwrap().insert(
            "/ephemeral/sequence.json".to_string(),
            VecDeque::from([
                (200, serde_json::json!({"sequence": 100})),
                (200, serde_json::json!({"sequence": 103})),
            ]),
        );
        let feed = fixture.feed(&fixture_config());
        assert_eq!(next(&feed).await.unwrap().kill_id, 9001);
        assert!(next(&feed).await.is_none());
        fixture.reply("/ephemeral/101.json", 200, fixture_killmail(9002));
        assert_eq!(next(&feed).await.unwrap().kill_id, 9002);
        assert_eq!(
            fixture.requests.lock().unwrap().as_slice(),
            [
                "/ephemeral/100.json",
                "/ephemeral/101.json",
                "/ephemeral/sequence.json",
                "/ephemeral/101.json",
            ]
        );
    }

    #[tokio::test]
    async fn forward_gap_and_null_placeholder_recovery_still_emit_subsequent_killmail() {
        let fixture = HttpFixture::new(100).await;
        fixture.reply(
            "/ephemeral/sequence.json",
            200,
            serde_json::json!({"sequence": 102}),
        );
        fixture.reply(
            "/ephemeral/101.json",
            200,
            serde_json::json!({
                "killmail_id": null, "hash": null, "esi": null,
                "zkb": {"href": "https://esi.evetech.net/killmails///"}
            }),
        );
        fixture.reply("/ephemeral/102.json", 200, fixture_killmail(9002));
        let feed = fixture.feed(&fixture_config());
        assert!(next(&feed).await.is_none());
        assert!(next(&feed).await.is_none());
        assert_eq!(next(&feed).await.unwrap().kill_id, 9002);
        assert_eq!(
            fixture.requests.lock().unwrap().as_slice(),
            [
                "/ephemeral/100.json",
                "/ephemeral/sequence.json",
                "/ephemeral/101.json",
                "/ephemeral/102.json",
            ]
        );
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r2z2_sequence.json");

        // No file yet — should return 0
        assert_eq!(R2z2Feed::load_checkpoint(&path), 0);

        // Write a valid checkpoint
        let checkpoint = R2z2Checkpoint { sequence: 96128620 };
        std::fs::write(&path, serde_json::to_string(&checkpoint).unwrap()).unwrap();
        assert_eq!(R2z2Feed::load_checkpoint(&path), 96128620);
    }

    #[test]
    fn test_checkpoint_corrupt_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r2z2_sequence.json");

        // Write corrupt data
        std::fs::write(&path, "not valid json").unwrap();
        assert_eq!(R2z2Feed::load_checkpoint(&path), 0);
        // File should be deleted
        assert!(!path.exists());
    }

    #[test]
    fn test_checkpoint_bare_integer_treated_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r2z2_sequence.json");

        // Bare integer is not the canonical object format
        std::fs::write(&path, "12345").unwrap();
        assert_eq!(R2z2Feed::load_checkpoint(&path), 0);
        assert!(!path.exists());
    }

    #[test]
    fn test_r2z2_urls_use_ephemeral_path() {
        let seq_url = format!("{}/ephemeral/sequence.json", R2Z2_BASE_URL);
        assert_eq!(
            seq_url,
            "https://r2z2.zkillboard.com/ephemeral/sequence.json"
        );

        let km_url = format!("{}/ephemeral/{}.json", R2Z2_BASE_URL, 96128620);
        assert_eq!(
            km_url,
            "https://r2z2.zkillboard.com/ephemeral/96128620.json"
        );
    }

    #[test]
    fn test_exponential_backoff_bounds() {
        // exp=0: base=1, range [1, 2]
        for _ in 0..20 {
            let d = exponential_backoff(0);
            assert!(d.as_secs_f64() >= 1.0);
            assert!(d.as_secs_f64() <= 2.0);
        }

        // exp=6: base=64 -> capped to 60, range [60, 120]
        for _ in 0..20 {
            let d = exponential_backoff(6);
            assert!(d.as_secs_f64() >= 60.0);
            assert!(d.as_secs_f64() <= 120.0);
        }
    }

    #[test]
    fn test_jittered_wait_bounds() {
        for _ in 0..20 {
            let d = jittered_wait(10.0);
            assert!(d.as_secs_f64() >= 10.0);
            assert!(d.as_secs_f64() <= 20.0);
        }
    }

    #[test]
    fn test_null_placeholder_sequence_detected() {
        let json = r#"{
            "killmail_id": null,
            "hash": null,
            "esi": null,
            "zkb": {
                "npc": null,
                "solo": null,
                "awox": null,
                "labels": null,
                "attackerCount": null,
                "href": "https://esi.evetech.net/killmails///"
            },
            "uploaded_at": 1772304311,
            "sequence_id": 96203634
        }"#;

        assert!(R2z2Feed::is_null_placeholder_sequence(json));
    }

    #[test]
    fn test_valid_sequence_not_treated_as_placeholder() {
        let json = r#"{
            "killmail_id": 133603559,
            "hash": "718e0749c0d17ddee62b2b0e5b290891665b40c8",
            "esi": {},
            "zkb": {
                "href": "https://esi.evetech.net/killmails/133603559/718e0749c0d17ddee62b2b0e5b290891665b40c8/"
            },
            "uploaded_at": 1772304311,
            "sequence_id": 96203634
        }"#;

        assert!(!R2z2Feed::is_null_placeholder_sequence(json));
    }
}

/// Compute exponential backoff with full jitter.
/// Returns `Duration` in range `[base, 2*base]` where `base = min(2^exp, 60)`.
fn exponential_backoff(exp: u32) -> Duration {
    let base = (2.0_f64.powi(exp as i32)).min(MAX_BACKOFF_SECS);
    let jitter = rand::thread_rng().gen_range(0.0..=base);
    Duration::from_secs_f64(base + jitter)
}

/// Fixed base wait with full jitter: `[base, 2*base]`.
fn jittered_wait(base_secs: f64) -> Duration {
    let jitter = rand::thread_rng().gen_range(0.0..=base_secs);
    Duration::from_secs_f64(base_secs + jitter)
}
