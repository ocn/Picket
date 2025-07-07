use reqwest::Client;
use crate::models::{ZkData, RedisQResponse};
use std::time::Duration;
use tracing::info;

pub struct RedisQListener {
    client: Client,
    url: String,
}

impl RedisQListener {
    pub fn new(base_url: String, queue_id: &str) -> Self {
        let url = format!("{}?queueID={}", base_url, queue_id);
        info!("Listening to RedisQ at: {}", url);
        RedisQListener {
            client: Client::new(),
            url,
        }
    }

    pub async fn listen(&self) -> Result<Option<ZkData>, Box<dyn std::error::Error>> {
        let response = self.client.get(&self.url)
            .timeout(Duration::from_secs(60)) // Add a timeout to prevent indefinite hangs
            .send()
            .await?;

        // Ensure the request was successful
        if !response.status().is_success() {
            return Err(format!("Received non-success status: {}", response.status()).into());
        }

        let text = response.text().await?;
        if text.contains("<!DOCTYPE html>") {
             return Err(format!("Received HTML response instead of JSON").into());
        }

        let wrapper: RedisQResponse = serde_json::from_str(&text)
            .map_err(|e| format!("JSON parsing error: {}. Response text: '{}'", e, text))?;

        Ok(wrapper.package)
    }
}