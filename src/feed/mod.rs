pub mod r2z2;
pub mod redisq;

use crate::models::ZkDataNoEsi;
use chrono::{DateTime, Utc};
use serenity::async_trait;
use std::fmt;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum FeedError {
    Transport(String),
    Parse(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeedHealthSnapshot {
    pub r2z2_progress_at: Option<DateTime<Utc>>,
    pub validation_success_at: Option<DateTime<Utc>>,
    pub validation_failures: i32,
}

#[derive(Debug, Default)]
struct FeedHealthState {
    snapshot: FeedHealthSnapshot,
    validation_initialized: bool,
    pending_validation_failures: i32,
}

#[derive(Debug, Default)]
pub struct FeedHealthTelemetry {
    state: Mutex<FeedHealthState>,
}

impl FeedHealthTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn record_r2z2_progress_at(&self, observed_at: DateTime<Utc>) {
        self.state.lock().await.snapshot.r2z2_progress_at = Some(observed_at);
    }

    pub async fn record_validation_success_at(&self, observed_at: DateTime<Utc>) {
        let mut state = self.state.lock().await;
        state.snapshot.validation_success_at = Some(observed_at);
        state.snapshot.validation_failures = 0;
        state.pending_validation_failures = 0;
        state.validation_initialized = true;
    }

    pub async fn record_validation_failure_at(&self, _observed_at: DateTime<Utc>) {
        let mut state = self.state.lock().await;
        state.snapshot.validation_failures = state.snapshot.validation_failures.saturating_add(1);
        if !state.validation_initialized {
            state.pending_validation_failures = state.pending_validation_failures.saturating_add(1);
        }
    }

    pub async fn hydrate_validation_state(
        &self,
        validation_success_at: Option<DateTime<Utc>>,
        validation_failures: i32,
    ) {
        let mut state = self.state.lock().await;
        if state.validation_initialized {
            return;
        }
        state.snapshot.validation_success_at = validation_success_at;
        state.snapshot.validation_failures =
            validation_failures.saturating_add(state.pending_validation_failures);
        state.pending_validation_failures = 0;
        state.validation_initialized = true;
    }

    pub async fn snapshot(&self) -> FeedHealthSnapshot {
        self.state.lock().await.snapshot.clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedHealthProvider {
    R2z2,
    Redisq,
}

impl fmt::Display for FeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeedError::Transport(msg) => write!(f, "Transport: {}", msg),
            FeedError::Parse(msg) => write!(f, "Parse: {}", msg),
        }
    }
}

#[async_trait]
pub trait KillmailFeed: Send + Sync {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError>;

    fn health_provider(&self) -> FeedHealthProvider {
        FeedHealthProvider::Redisq
    }
}
