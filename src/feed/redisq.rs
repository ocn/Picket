use super::{FeedError, KillmailFeed};
use crate::models::{RedisQResponse, ZkDataNoEsi};
use reqwest::Client;
use serenity::async_trait;
use std::time::Duration;
use tracing::info;

const REDISQ_URL: &str = "https://zkillredisq.stream/listen.php";

pub struct RedisQFeed {
    client: Client,
    url: String,
}

impl RedisQFeed {
    pub fn new(queue_id: &str, connect_timeout: Duration, request_timeout: Duration) -> Self {
        let url = format!("{}?queueID={}", REDISQ_URL, queue_id);
        info!("RedisQ feed URL: {}", url);
        RedisQFeed {
            client: Client::builder()
                .connect_timeout(connect_timeout)
                .timeout(request_timeout)
                .build()
                .expect("Failed to build RedisQ HTTP client"),
            url,
        }
    }
}

#[async_trait]
impl KillmailFeed for RedisQFeed {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError> {
        let response = self
            .client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| FeedError::Transport(e.to_string()))?;

        if !response.status().is_success() {
            return Err(FeedError::Transport(format!(
                "Received non-success status: {}",
                response.status()
            )));
        }

        let text = response
            .text()
            .await
            .map_err(|e| FeedError::Transport(e.to_string()))?;

        if text.contains("<!DOCTYPE html>") {
            return Err(FeedError::Parse(
                "Received HTML response instead of JSON".to_string(),
            ));
        }

        let wrapper: RedisQResponse = serde_json::from_str(&text)
            .map_err(|e| FeedError::Parse(format!("JSON parsing error: {}. Response: '{}'", e, text)))?;

        Ok(wrapper.package)
    }
}
