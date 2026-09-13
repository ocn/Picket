//! Dumps real Killfeed message payloads (content + embed, Discord wire JSON)
//! for the README screenshots, using the production render and prepare path.
//!
//! Run:
//!   SCREENSHOT_PAYLOAD_DIR=/tmp/payloads cargo test --lib screenshot_payloads -- --ignored --nocapture
//!
//! Needs `.env` (DISCORD_BOT_TOKEN for config load) and the cached metadata in
//! `config/`. Nothing is sent to Discord.

#[cfg(test)]
mod tests {
    use crate::config::{
        load_app_config, Action, AppState, Filter, FilterNode, PingType, SimpleFilter,
        Subscription, System, SystemRange, Target, TargetableCondition, TargetedFilter,
    };
    use crate::discord_bot::{
        configure_killmail_notification_message, prepare_killmail_notification,
        render_killmail_embed, KillmailEmbed,
    };
    use crate::models::ZkData;
    use crate::processor::process_killmail;
    use chrono::{DateTime, Duration, Utc};
    use moka::future::Cache;
    use serenity::builder::CreateMessage;
    use serenity::model::id::GuildId;
    use std::collections::HashMap;
    use std::sync::Arc;

    const CHANNEL: &str = "1";

    fn sub(
        id: &str,
        description: &str,
        root_filter: FilterNode,
        ping_type: Option<PingType>,
    ) -> Subscription {
        Subscription {
            id: id.to_string(),
            description: description.to_string(),
            root_filter,
            action: Action {
                channel_id: CHANNEL.to_string(),
                ping_type,
            },
        }
    }

    fn targeted(condition: TargetableCondition, target: Target) -> FilterNode {
        FilterNode::Condition(Filter::Targeted(TargetedFilter { condition, target }))
    }

    fn load_fixture(name: &str) -> ZkData {
        let contents = std::fs::read_to_string(format!("resources/{name}")).expect("fixture");
        serde_json::from_str(&contents).expect("fixture json")
    }

    async fn app_state(subscriptions: Vec<Subscription>) -> Arc<AppState> {
        let app_config = load_app_config().expect("config (.env)");
        let mut subs = HashMap::new();
        subs.insert(GuildId(123_456_789), subscriptions);
        let systems: HashMap<u32, System> = crate::config::load_systems().unwrap_or_default();
        Arc::new(AppState {
            subscriptions: Arc::new(std::sync::RwLock::new(subs)),
            systems: Arc::new(std::sync::RwLock::new(systems)),
            ships: Arc::new(std::sync::RwLock::new(
                crate::config::load_ships().unwrap_or_default(),
            )),
            names: Arc::new(std::sync::RwLock::new(
                crate::config::load_names().unwrap_or_default(),
            )),
            tickers: Arc::new(std::sync::RwLock::new(
                crate::config::load_tickers().unwrap_or_default(),
            )),
            group_names: Arc::new(std::sync::RwLock::new(
                crate::config::load_group_names().unwrap_or_default(),
            )),
            celestial_cache: Cache::new(10_000),
            esi_client: Default::default(),
            app_config: Arc::new(app_config),
            systems_file_lock: Default::default(),
            ships_file_lock: Default::default(),
            names_file_lock: Default::default(),
            tickers_file_lock: Default::default(),
            group_names_file_lock: Default::default(),
            subscriptions_file_lock: Default::default(),
            last_ping_times: Default::default(),
            user_standings: Arc::new(Default::default()),
            user_standings_file_lock: Default::default(),
            sso_states: Arc::new(Default::default()),
        })
    }

    /// (output name, fixture, subscription) triples; each fixture is public
    /// killmail data with no operator affiliation and no staging-system location.
    fn cases() -> Vec<(&'static str, &'static str, Subscription)> {
        vec![
            (
                "01-killfeed-loss-structure",
                "131126432_keepstar_kill.json",
                sub(
                    "structure-losses",
                    "Structures lost",
                    targeted(TargetableCondition::ShipGroup(vec![1657]), Target::Victim),
                    None,
                ),
            ),
            (
                "02-killfeed-kill-dreads",
                "115769073_ostingele.json",
                sub(
                    "dread-kills",
                    "Dreadnoughts on the attacker side",
                    targeted(TargetableCondition::ShipGroup(vec![485]), Target::Attacker),
                    None,
                ),
            ),
            (
                "03-killfeed-radar-ping",
                "132462203_global_feed_test.json",
                sub(
                    "radar-8ly",
                    "Anything within 8 LY of staging",
                    FilterNode::Condition(Filter::Simple(SimpleFilter::LyRangeFrom(vec![
                        SystemRange {
                            system_id: 30001534,
                            range: 8.0,
                        },
                    ]))),
                    Some(PingType::Here {
                        max_ping_delay_minutes: Some(10),
                        ping_cooldown_minutes: None,
                    }),
                ),
            ),
            (
                "04-killfeed-type-tracked",
                "132253134_drill_kill_by_thanatos.json",
                sub(
                    "thanatos-watch",
                    "Thanatos on the attacker side",
                    targeted(TargetableCondition::ShipType(vec![23911]), Target::Attacker),
                    None,
                ),
            ),
        ]
    }

    #[tokio::test]
    #[ignore]
    async fn dump_killfeed_payloads() {
        dotenvy::dotenv().ok();
        let out_dir = std::env::var("SCREENSHOT_PAYLOAD_DIR").expect("SCREENSHOT_PAYLOAD_DIR");
        std::fs::create_dir_all(&out_dir).expect("out dir");

        for (name, fixture, subscription) in cases() {
            let state = app_state(vec![subscription]).await;
            let zk = load_fixture(fixture);
            let matched = process_killmail(&state, &zk).await;
            let Some((_guild, subscription, result)) = matched.into_iter().next() else {
                panic!("{name}: fixture {fixture} matched nothing");
            };
            let KillmailEmbed {
                embed,
                ping_summary,
            } = render_killmail_embed(&state, &zk, &result, &subscription).await;
            // Evaluate one minute after the kill so a pinging subscription is eligible.
            let killed_at = DateTime::parse_from_rfc3339(&zk.killmail.killmail_time)
                .expect("killmail_time")
                .with_timezone(&Utc);
            let notification = prepare_killmail_notification(
                &subscription,
                &zk.killmail.killmail_time,
                killed_at + Duration::minutes(1),
                true,
            );
            let mut builder = CreateMessage::default();
            configure_killmail_notification_message(
                &mut builder,
                notification,
                embed,
                &ping_summary,
            );
            let payload: serde_json::Map<String, serde_json::Value> = builder
                .0
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            let path = format!("{out_dir}/{name}.json");
            std::fs::write(&path, serde_json::to_string_pretty(&payload).unwrap()).expect("write");
            println!("wrote {path}");
        }
    }
}
