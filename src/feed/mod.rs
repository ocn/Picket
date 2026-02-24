pub mod r2z2;
pub mod redisq;

use crate::models::ZkDataNoEsi;
use serenity::async_trait;
use std::fmt;

#[derive(Debug)]
pub enum FeedError {
    Transport(String),
    Parse(String),
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
}
