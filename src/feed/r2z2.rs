use super::{FeedError, KillmailFeed};
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

pub struct R2z2Feed {
    client: Client,
    state: Mutex<R2z2State>,
    config: Arc<AppConfig>,
    checkpoint_path: PathBuf,
}

impl R2z2Feed {
    pub fn new(config: &Arc<AppConfig>) -> Self {
        let checkpoint_path = PathBuf::from("config/r2z2_sequence.json");
        let initial_sequence = Self::load_checkpoint(&checkpoint_path);

        let client = Client::builder()
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
        }
    }

    fn load_checkpoint(path: &PathBuf) -> i64 {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str::<R2z2Checkpoint>(&contents) {
                Ok(checkpoint) => {
                    info!("R2Z2: resuming from persisted checkpoint {}", checkpoint.sequence);
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
        let url = format!("{}/ephemeral/sequence.json", R2Z2_BASE_URL);

        loop {
            match self.client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        let wait = jittered_wait(FIXED_429_BASE_SECS);
                        warn!("R2Z2: 429 fetching sequence.json, waiting {:.1}s", wait.as_secs_f64());
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    if status.is_server_error() {
                        let wait = exponential_backoff(state.backoff_exp);
                        state.backoff_exp += 1;
                        error!("R2Z2: {} fetching sequence.json, backoff {:.1}s", status, wait.as_secs_f64());
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    if !status.is_success() {
                        let wait = exponential_backoff(state.backoff_exp);
                        state.backoff_exp += 1;
                        error!("R2Z2: unexpected {} fetching sequence.json, backoff {:.1}s", status, wait.as_secs_f64());
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
                            error!("R2Z2: parse error fetching sequence.json: {}, backoff {:.1}s", e, wait.as_secs_f64());
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    let wait = exponential_backoff(state.backoff_exp);
                    state.backoff_exp += 1;
                    error!("R2Z2: transport error fetching sequence.json: {}, backoff {:.1}s", e, wait.as_secs_f64());
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }
        }
    }

    /// Try to resync: reset state and fetch fresh sequence.
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

        let url = format!("{}/ephemeral/{}.json", R2Z2_BASE_URL, state.sequence);

        match self.client.get(&url).send().await {
            Ok(response) => {
                let status = response.status();

                if status == StatusCode::TOO_MANY_REQUESTS {
                    let wait = jittered_wait(FIXED_429_BASE_SECS);
                    warn!("R2Z2: 429 on sequence {}, waiting {:.1}s", state.sequence, wait.as_secs_f64());
                    tokio::time::sleep(wait).await;
                    return Ok(None);
                }

                if status.is_server_error() {
                    let wait = exponential_backoff(state.backoff_exp);
                    state.backoff_exp += 1;
                    error!("R2Z2: {} on sequence {}, backoff {:.1}s", status, state.sequence, wait.as_secs_f64());
                    tokio::time::sleep(wait).await;
                    return Ok(None);
                }

                if status == StatusCode::NOT_FOUND {
                    // 404 — not yet available
                    state.backoff_exp = 0;
                    state.consecutive_404s += 1;
                    if state.first_404_at.is_none() {
                        state.first_404_at = Some(Instant::now());
                    }

                    if self.needs_resync(&state) {
                        warn!(
                            "R2Z2: {} consecutive 404s, resyncing from sequence.json",
                            state.consecutive_404s
                        );
                        state.consecutive_404s = 0;
                        state.first_404_at = None;
                        state.sequence = self.fetch_sequence(&mut state).await;
                        self.save_checkpoint(state.sequence);
                        return Ok(None);
                    }

                    // Wait poll_interval before returning
                    tokio::time::sleep(Duration::from_secs(self.config.r2z2_poll_interval_secs)).await;
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

                let km: R2z2KillmailResponse = serde_json::from_str(&body)
                    .map_err(|e| FeedError::Parse(format!(
                        "R2Z2: JSON error on sequence {}: {}",
                        state.sequence, e
                    )))?;

                // Advance sequence and save checkpoint
                state.sequence += 1;
                self.save_checkpoint(state.sequence);

                // Build the ESI URL from the zkb href field
                let zk_data = ZkDataNoEsi {
                    kill_id: km.killmail_id,
                    zkb: km.zkb,
                };

                Ok(Some(zk_data))
            }
            Err(e) => {
                let wait = exponential_backoff(state.backoff_exp);
                state.backoff_exp += 1;
                error!("R2Z2: transport error on sequence {}: {}, backoff {:.1}s", state.sequence, e, wait.as_secs_f64());
                tokio::time::sleep(wait).await;
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(seq_url, "https://r2z2.zkillboard.com/ephemeral/sequence.json");

        let km_url = format!("{}/ephemeral/{}.json", R2Z2_BASE_URL, 96128620);
        assert_eq!(km_url, "https://r2z2.zkillboard.com/ephemeral/96128620.json");
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
