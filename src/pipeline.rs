use crate::config::{self, AppState};
use crate::discord_bot::{self, PreparedDispatch};
use crate::feed::KillmailFeed;
use crate::models::{ZkData, ZkDataNoEsi};
use crate::processor;
use chrono::{DateTime, Utc};
use serenity::http::error::Error;
use serenity::http::Http;
use serenity::model::id::GuildId;
use serenity::model::prelude::ChannelId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use tracing::{error, info, warn};

pub struct WorkItem {
    pub dispatch_sequence: u64,
    pub kill_id: i64,
    pub zk_data_no_esi: ZkDataNoEsi,
}

pub enum ProcessedResult {
    Matched {
        dispatch_sequence: u64,
        kill_id: i64,
        dispatches: Vec<PreparedDispatch>,
    },
    NoMatch {
        dispatch_sequence: u64,
        kill_id: i64,
    },
    Failed {
        dispatch_sequence: u64,
        kill_id: i64,
        error: String,
    },
}

impl ProcessedResult {
    fn dispatch_sequence(&self) -> u64 {
        match self {
            ProcessedResult::Matched { dispatch_sequence, .. } => *dispatch_sequence,
            ProcessedResult::NoMatch { dispatch_sequence, .. } => *dispatch_sequence,
            ProcessedResult::Failed { dispatch_sequence, .. } => *dispatch_sequence,
        }
    }
}

async fn process_work_item(
    work_item: WorkItem,
    app_state: Arc<AppState>,
) -> ProcessedResult {
    let kill_id = work_item.kill_id;
    let dispatch_sequence = work_item.dispatch_sequence;
    let zk_data_no_esi = work_item.zk_data_no_esi;

    // Use inline killmail if available, otherwise fetch from ESI
    let killmail_data = if let Some(km) = zk_data_no_esi.inline_killmail.clone() {
        info!("[Kill: {}] Using inline ESI data (skipping fetch)", kill_id);
        km
    } else {
        match app_state.esi_client.load_killmail(zk_data_no_esi.zkb.esi.clone()).await {
            Ok(km) => km,
            Err(e) => {
                error!("[Kill: {}] Error loading killmail data from ESI: {}", kill_id, e);
                return ProcessedResult::Failed {
                    dispatch_sequence,
                    kill_id,
                    error: format!("ESI fetch failed: {e}"),
                };
            }
        }
    };

    let zk_data = ZkData {
        kill_id: zk_data_no_esi.kill_id,
        killmail: killmail_data,
        zkb: zk_data_no_esi.zkb,
    };

    let matched = processor::process_killmail(&app_state, &zk_data).await;

    if matched.is_empty() {
        return ProcessedResult::NoMatch { dispatch_sequence, kill_id };
    }

    let mut dispatches = Vec::with_capacity(matched.len());
    for (guild_id, subscription, filter_result) in matched {
        info!(
            "[Kill: {}] Matched subscription '{}' for channel {}, filter: {}",
            kill_id, subscription.description, subscription.action.channel_id, filter_result.name
        );
        let embed = discord_bot::build_killmail_embed(
            &app_state,
            &zk_data,
            &filter_result,
            &subscription,
        )
        .await;

        dispatches.push(PreparedDispatch {
            guild_id,
            subscription,
            zk_data: zk_data.clone(),
            embed,
            filter_result,
        });
    }

    ProcessedResult::Matched {
        dispatch_sequence,
        kill_id,
        dispatches,
    }
}

pub async fn run_producer(
    feed: Box<dyn KillmailFeed>,
    app_state: Arc<AppState>,
    result_tx: mpsc::Sender<ProcessedResult>,
    semaphore: Arc<Semaphore>,
) {
    let mut dispatch_sequence: u64 = 0;
    let timeout_secs = app_state.app_config.killmail_process_timeout_secs;
    let sleep_ms = app_state.app_config.killmail_post_process_sleep_ms;

    loop {
        match feed.next().await {
            Ok(Some(zk_data_no_esi)) => {
                let kill_id = zk_data_no_esi.kill_id;
                info!("[Kill: {}] Received (seq: {})", kill_id, dispatch_sequence);

                let permit = semaphore.clone().acquire_owned().await.expect("semaphore closed");

                let work_item = WorkItem {
                    dispatch_sequence,
                    kill_id,
                    zk_data_no_esi,
                };

                let state = app_state.clone();
                let tx = result_tx.clone();
                let seq = dispatch_sequence;

                tokio::spawn(async move {
                    let _permit = permit; // dropped when task completes

                    let result = tokio::time::timeout(
                        Duration::from_secs(timeout_secs),
                        process_work_item(work_item, state),
                    )
                    .await;

                    let processed = match result {
                        Ok(r) => r,
                        Err(_) => {
                            error!("[Kill: {}] Processing timed out after {}s", kill_id, timeout_secs);
                            ProcessedResult::Failed {
                                dispatch_sequence: seq,
                                kill_id,
                                error: format!("timed out after {timeout_secs}s"),
                            }
                        }
                    };

                    if tx.send(processed).await.is_err() {
                        error!("[Kill: {}] Dispatcher channel closed", kill_id);
                    }
                });

                dispatch_sequence += 1;

                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
            Ok(None) => {
                // Feed already handled its own wait/backoff
            }
            Err(e) => {
                error!("Feed error: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

pub async fn run_dispatcher(
    mut result_rx: mpsc::Receiver<ProcessedResult>,
    app_state: Arc<AppState>,
    http_client: Arc<Http>,
) {
    let gap_timeout = Duration::from_secs(app_state.app_config.killmail_process_timeout_secs * 2);
    let mut reorder = ReorderBuffer::new(gap_timeout);

    loop {
        // If we have the next result buffered, drain immediately
        if reorder.has_next() {
            for result in reorder.drain_ready() {
                dispatch_single(result, &app_state, &http_client).await;
            }
            continue;
        }

        match reorder.gap_deadline() {
            None => {
                // No buffered results or no gap — just wait for the next result
                match result_rx.recv().await {
                    Some(result) => {
                        reorder.insert(result);
                    }
                    None => {
                        info!("Dispatcher: result channel closed, shutting down");
                        return;
                    }
                }
            }
            Some(deadline) => {
                // We have buffered results but are waiting for next_dispatch_sequence.
                // Use select with the persistent deadline.
                tokio::select! {
                    result = result_rx.recv() => {
                        match result {
                            Some(result) => {
                                reorder.insert(result);
                            }
                            None => {
                                warn!("Dispatcher: channel closed with {} buffered results", reorder.buffer_len());
                                for result in reorder.drain_remaining() {
                                    dispatch_single(result, &app_state, &http_client).await;
                                }
                                return;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        warn!(
                            "Dispatcher: gap timeout for sequence {}, advancing (buffer has {} entries)",
                            reorder.next_sequence(), reorder.buffer_len()
                        );
                        let skipped = reorder.skip_gap();
                        dispatch_single(skipped, &app_state, &http_client).await;
                    }
                }
            }
        }

        // After receiving, try to drain
        for result in reorder.drain_ready() {
            dispatch_single(result, &app_state, &http_client).await;
        }
    }
}

async fn dispatch_single(
    result: ProcessedResult,
    app_state: &Arc<AppState>,
    http_client: &Arc<Http>,
) {
    match result {
        ProcessedResult::Matched { dispatch_sequence, kill_id, dispatches } => {
            for dispatch in dispatches {
                send_prepared_dispatch(dispatch, app_state, http_client).await;
            }
            info!("[Kill: {}] Dispatched (seq: {})", kill_id, dispatch_sequence);
        }
        ProcessedResult::NoMatch { dispatch_sequence, kill_id } => {
            info!("[Kill: {}] No match (seq: {})", kill_id, dispatch_sequence);
        }
        ProcessedResult::Failed { dispatch_sequence, kill_id, error } => {
            if kill_id >= 0 {
                error!("[Kill: {}] Failed (seq: {}): {}", kill_id, dispatch_sequence, error);
            } else {
                warn!("[Seq: {}] Gap timeout — skipping", dispatch_sequence);
            }
        }
    }
}

async fn send_prepared_dispatch(
    dispatch: PreparedDispatch,
    app_state: &Arc<AppState>,
    http_client: &Arc<Http>,
) {
    let channel = match dispatch.subscription.action.channel_id.parse::<u64>() {
        Ok(id) => ChannelId(id),
        Err(e) => {
            error!(
                "[Kill: {}] Invalid channel ID '{}': {:#?}",
                dispatch.zk_data.kill_id, dispatch.subscription.action.channel_id, e
            );
            return;
        }
    };

    // Evaluate ping logic
    let content = match &dispatch.subscription.action.ping_type {
        None => None,
        Some(ping_type) => {
            let kill_time = DateTime::parse_from_rfc3339(&dispatch.zk_data.killmail.killmail_time)
                .unwrap_or_else(|_| Utc::now().into());
            let kill_age = Utc::now().signed_duration_since(kill_time);

            let max_delay = ping_type.max_ping_delay_in_minutes().unwrap_or(0);
            if max_delay == 0 || kill_age.num_minutes() <= max_delay as i64 {
                let channel_id = dispatch.subscription.action.channel_id.parse::<u64>().unwrap_or(0);
                let mut ping_times = app_state.last_ping_times.lock().await;

                let now = Instant::now();
                let last_ping = ping_times
                    .entry(channel_id)
                    .or_insert(now - Duration::from_secs(301));

                if now.duration_since(*last_ping) > Duration::from_secs(300) {
                    *last_ping = now;
                    Some(match ping_type {
                        config::PingType::Here { .. } => "@here",
                        config::PingType::Everyone { .. } => "@everyone",
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    let result = channel
        .send_message(http_client, |m| {
            if let Some(content) = content {
                m.content(content)
            } else {
                m
            }
            .set_embed(dispatch.embed)
        })
        .await;

    if let Err(e) = result {
        if let serenity::Error::Http(http_err) = &e {
            if let Error::UnsuccessfulRequest(resp) = &**http_err {
                if matches!(
                    resp.status_code,
                    serenity::http::StatusCode::FORBIDDEN | serenity::http::StatusCode::NOT_FOUND
                ) {
                    error!(
                        "[Kill: {}] Channel {} error ({}). Removing subscriptions.",
                        dispatch.zk_data.kill_id, channel, resp.status_code
                    );
                    cleanup_channel_subscriptions(
                        app_state,
                        dispatch.guild_id,
                        &dispatch.subscription.action.channel_id,
                    )
                    .await;
                    return;
                }
            }
        }
        error!(
            "[Kill: {}] Failed to send message to channel {}: {:#?}",
            dispatch.zk_data.kill_id, channel, e
        );
        return;
    }

    info!(
        "[Kill: {}] Sent message to channel {}",
        dispatch.zk_data.kill_id, channel
    );
}

async fn cleanup_channel_subscriptions(
    app_state: &Arc<AppState>,
    guild_id: GuildId,
    channel_id: &str,
) {
    let _lock = app_state.subscriptions_file_lock.lock().await;
    let mut subs_map = app_state.subscriptions.write().unwrap();

    if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
        guild_subs.retain(|s| s.action.channel_id != channel_id);
        if let Err(save_err) = config::save_subscriptions_for_guild(guild_id, guild_subs) {
            error!(
                "Failed to save subscriptions after cleanup for guild {}: {}",
                guild_id, save_err
            );
        }
    }
}

/// Reorder buffer used by the dispatcher to emit results in strict sequence order.
/// Extracted for testability.
pub(crate) struct ReorderBuffer {
    next_dispatch_sequence: u64,
    buffer: HashMap<u64, ProcessedResult>,
    gap_timeout: Duration,
    gap_deadline: Option<tokio::time::Instant>,
}

impl ReorderBuffer {
    pub fn new(gap_timeout: Duration) -> Self {
        Self {
            next_dispatch_sequence: 0,
            buffer: HashMap::new(),
            gap_timeout,
            gap_deadline: None,
        }
    }

    /// Insert a result into the buffer.
    pub fn insert(&mut self, result: ProcessedResult) {
        let seq = result.dispatch_sequence();
        self.buffer.insert(seq, result);
    }

    /// Drain all consecutive results starting from next_dispatch_sequence.
    /// Returns the dispatched results in order.
    pub fn drain_ready(&mut self) -> Vec<ProcessedResult> {
        let mut out = Vec::new();
        while let Some(result) = self.buffer.remove(&self.next_dispatch_sequence) {
            out.push(result);
            self.next_dispatch_sequence += 1;
            self.gap_deadline = None;
        }
        out
    }

    /// Returns true if the next expected sequence is already buffered.
    pub fn has_next(&self) -> bool {
        self.buffer.contains_key(&self.next_dispatch_sequence)
    }

    /// Returns the current gap deadline, creating one if the buffer is non-empty
    /// and no deadline exists yet. Returns None if buffer is empty.
    pub fn gap_deadline(&mut self) -> Option<tokio::time::Instant> {
        if self.buffer.is_empty() {
            self.gap_deadline = None;
            return None;
        }
        if self.gap_deadline.is_none() {
            self.gap_deadline = Some(tokio::time::Instant::now() + self.gap_timeout);
        }
        self.gap_deadline
    }

    /// Advance past a gap: skip next_dispatch_sequence and return
    /// a synthetic Failed result for it.
    pub fn skip_gap(&mut self) -> ProcessedResult {
        let seq = self.next_dispatch_sequence;
        self.next_dispatch_sequence += 1;
        self.gap_deadline = None;
        ProcessedResult::Failed {
            dispatch_sequence: seq,
            kill_id: -1,
            error: "gap timeout — worker likely panicked".to_string(),
        }
    }

    /// Drain all remaining buffered results, skipping gaps.
    pub fn drain_remaining(&mut self) -> Vec<ProcessedResult> {
        let mut out = Vec::new();
        while !self.buffer.is_empty() {
            if let Some(result) = self.buffer.remove(&self.next_dispatch_sequence) {
                out.push(result);
            }
            // Skip gap or advance
            self.next_dispatch_sequence += 1;
        }
        out
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_dispatch_sequence
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_match(seq: u64) -> ProcessedResult {
        ProcessedResult::NoMatch {
            dispatch_sequence: seq,
            kill_id: seq as i64 + 100,
        }
    }

    fn failed(seq: u64) -> ProcessedResult {
        ProcessedResult::Failed {
            dispatch_sequence: seq,
            kill_id: seq as i64 + 100,
            error: "test error".to_string(),
        }
    }

    fn seq_of(r: &ProcessedResult) -> u64 {
        r.dispatch_sequence()
    }

    #[test]
    fn test_in_order_drain() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));
        buf.insert(no_match(0));
        buf.insert(no_match(1));
        buf.insert(no_match(2));

        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![0, 1, 2]);
        assert_eq!(buf.next_sequence(), 3);
        assert_eq!(buf.buffer_len(), 0);
    }

    #[test]
    fn test_out_of_order_buffering() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        // Seq 1 arrives before seq 0
        buf.insert(no_match(1));
        let drained = buf.drain_ready();
        assert!(drained.is_empty(), "should not drain — seq 0 missing");
        assert_eq!(buf.next_sequence(), 0);

        // Seq 0 arrives — both should drain
        buf.insert(no_match(0));
        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![0, 1]);
        assert_eq!(buf.next_sequence(), 2);
    }

    #[test]
    fn test_out_of_order_with_larger_gap() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        // Seqs 2, 4, 3 arrive — but 0 and 1 are missing
        buf.insert(no_match(2));
        buf.insert(no_match(4));
        buf.insert(no_match(3));

        let drained = buf.drain_ready();
        assert!(drained.is_empty());
        assert_eq!(buf.buffer_len(), 3);

        // Seq 0 arrives — only 0 drains (1 still missing)
        buf.insert(no_match(0));
        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![0]);
        assert_eq!(buf.next_sequence(), 1);
        assert_eq!(buf.buffer_len(), 3);

        // Seq 1 arrives — 1, 2, 3, 4 all drain
        buf.insert(no_match(1));
        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![1, 2, 3, 4]);
        assert_eq!(buf.next_sequence(), 5);
        assert_eq!(buf.buffer_len(), 0);
    }

    #[test]
    fn test_skip_gap_advances_past_missing_sequence() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        // Seqs 1, 2 arrive — seq 0 is missing
        buf.insert(no_match(1));
        buf.insert(no_match(2));

        let drained = buf.drain_ready();
        assert!(drained.is_empty());

        // Skip the gap for seq 0
        let skipped = buf.skip_gap();
        assert_eq!(seq_of(&skipped), 0);
        assert!(matches!(skipped, ProcessedResult::Failed { .. }));
        assert_eq!(buf.next_sequence(), 1);

        // Now 1, 2 should drain
        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![1, 2]);
        assert_eq!(buf.next_sequence(), 3);
    }

    #[test]
    fn test_drain_remaining_skips_gaps() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        // Seqs 1, 3 arrive — seq 0 and 2 are missing
        buf.insert(no_match(1));
        buf.insert(no_match(3));

        let drained: Vec<u64> = buf.drain_remaining().iter().map(seq_of).collect();
        // Should drain 1 and 3 (skipping 0 and 2)
        assert_eq!(drained, vec![1, 3]);
        assert_eq!(buf.buffer_len(), 0);
    }

    #[tokio::test]
    async fn test_gap_deadline_persistence() {
        tokio::time::pause();

        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        // Empty buffer — no deadline
        assert!(buf.gap_deadline().is_none());

        // Insert out-of-order seq — deadline should be created
        buf.insert(no_match(1));
        let deadline1 = buf.gap_deadline().expect("should have deadline");

        // Deadline should be persistent (same value on second call)
        let deadline2 = buf.gap_deadline().expect("should still have deadline");
        assert_eq!(deadline1, deadline2);

        // Insert another out-of-order result — deadline should NOT reset
        buf.insert(no_match(2));
        let deadline3 = buf.gap_deadline().expect("should still have deadline");
        assert_eq!(deadline1, deadline3, "deadline must not reset on new out-of-order arrival");

        // Drain advances past gap — deadline resets
        buf.insert(no_match(0));
        buf.drain_ready();
        // Buffer is now empty
        assert!(buf.gap_deadline().is_none());
    }

    #[tokio::test]
    async fn test_gap_timeout_fires_under_sustained_out_of_order_traffic() {
        // This is the P0 regression test: continuous out-of-order arrivals
        // must not prevent the gap timeout from firing.
        tokio::time::pause();

        let gap_timeout = Duration::from_secs(10);
        let mut buf = ReorderBuffer::new(gap_timeout);

        // Seq 0 is missing. Seqs 1..50 arrive over 20 seconds.
        // Gap deadline should fire after 10s regardless of new arrivals.
        buf.insert(no_match(1));
        let deadline = buf.gap_deadline().expect("should start deadline");

        // Simulate sustained arrivals every 0.5s for 20 seconds
        for seq in 2..42 {
            tokio::time::advance(Duration::from_millis(500)).await;
            buf.insert(no_match(seq));

            // Deadline must remain the same (not reset)
            let current_deadline = buf.gap_deadline().expect("deadline should persist");
            assert_eq!(current_deadline, deadline,
                "deadline must not reset when receiving out-of-order result at seq {seq}");
        }

        // After 20s (40 * 500ms), deadline (set at 10s) should have passed
        assert!(tokio::time::Instant::now() >= deadline,
            "should be past the gap deadline now");

        // Skip the gap for seq 0
        let skipped = buf.skip_gap();
        assert_eq!(seq_of(&skipped), 0);

        // Drain should now process seqs 1..42
        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained.len(), 41); // seqs 1 through 41
        assert_eq!(drained[0], 1);
        assert_eq!(*drained.last().unwrap(), 41);
    }

    #[test]
    fn test_mixed_result_types_maintain_order() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        buf.insert(no_match(0));
        buf.insert(failed(1));
        buf.insert(no_match(2));

        let drained: Vec<u64> = buf.drain_ready().iter().map(seq_of).collect();
        assert_eq!(drained, vec![0, 1, 2]);

        // Verify the types are correct
        // (can't check after drain since we consumed them, but the sequence is correct)
    }

    #[test]
    fn test_has_next() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(10));

        assert!(!buf.has_next());

        buf.insert(no_match(1));
        assert!(!buf.has_next()); // seq 0 missing

        buf.insert(no_match(0));
        assert!(buf.has_next()); // seq 0 present
    }
}
