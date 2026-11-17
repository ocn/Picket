use async_trait::async_trait;
use killbot_rust::write_authorization::{dispatch_slash, SlashAdapter};
use killbot_rust::write_policy::WritePolicy;
use serde_json::json;
use serenity::model::application::interaction::application_command::ApplicationCommandInteraction;

fn command(name: &str) -> ApplicationCommandInteraction {
    serde_json::from_value(json!({
        "id":"1", "application_id":"2", "type":2,
        "data":{"id":"3", "name":name, "type":1, "options":[]},
        "guild_id":"100", "channel_id":"200", "locale":"en-US",
        "member":{"deaf":false,"mute":false,"roles":[],
            "user":{"id":"300","username":"writer","discriminator":"0001","avatar":null},
            "permissions":"8"},
        "token":"SENTINEL_INTERACTION_TOKEN", "version":1
    }))
    .unwrap()
}

#[derive(Default)]
struct SlashSpy(Vec<String>);

#[async_trait]
impl SlashAdapter for SlashSpy {
    async fn deny(&mut self) {
        self.0.push("deny".into());
    }
    async fn work(&mut self, request: &ApplicationCommandInteraction) {
        self.0.push(format!("parse-and-work:{}", request.data.name));
    }
}

#[tokio::test]
async fn raw_unauthorized_command_never_enters_business_parsing() {
    let mut spy = SlashSpy::default();
    dispatch_slash(&WritePolicy::default(), &command("subscribe"), &mut spy).await;
    assert_eq!(spy.0, ["deny"]);
}

fn policy() -> WritePolicy {
    WritePolicy::from_json(
        r#"{"version":1,"guilds":{"100":{"allowed_user_ids":["300"],"allowed_role_ids":["400"]}}}"#,
    )
    .unwrap()
}

#[tokio::test]
async fn every_registered_operation_has_an_explicit_boundary_decision() {
    let writes = [
        "subscribe",
        "unsubscribe",
        "contract_subscribe",
        "contract_unsubscribe",
        "sov_subscribe",
        "sov_unsubscribe",
        "watch_subscribe",
        "watch_unsubscribe",
        "sync_standings",
        "sync_remove",
        "sync_clear",
    ];
    for name in writes {
        let mut denied = SlashSpy::default();
        dispatch_slash(&WritePolicy::default(), &command(name), &mut denied).await;
        assert_eq!(denied.0, ["deny"], "{name}");
        let mut allowed = SlashSpy::default();
        dispatch_slash(&policy(), &command(name), &mut allowed).await;
        assert_eq!(allowed.0, [format!("parse-and-work:{name}")]);
    }
    for name in ["ping", "diag", "sov_timers", "find_unsubscribed"] {
        let mut spy = SlashSpy::default();
        dispatch_slash(&WritePolicy::default(), &command(name), &mut spy).await;
        assert_eq!(spy.0, [format!("parse-and-work:{name}")]);
    }
    let mut spy = SlashSpy::default();
    dispatch_slash(&policy(), &command("new_unknown_command"), &mut spy).await;
    assert_eq!(spy.0, ["deny"]);
}

#[tokio::test]
async fn missing_or_mismatched_guild_member_and_administrator_do_not_grant_writes() {
    let mut cases = vec![command("subscribe"); 6];
    cases[0].guild_id = None;
    cases[1].member = None;
    cases[2].member.as_mut().unwrap().guild_id.0 = 999;
    cases[3].member.as_mut().unwrap().user.id.0 = 999;
    cases[4].user.id.0 = 999;
    cases[4].member.as_mut().unwrap().user.id.0 = 999;
    cases[5].guild_id.as_mut().unwrap().0 = 999;
    cases[5].member.as_mut().unwrap().guild_id.0 = 999;
    for request in cases {
        let mut spy = SlashSpy::default();
        dispatch_slash(&policy(), &request, &mut spy).await;
        assert_eq!(spy.0, ["deny"]);
    }
    let mut request = command("subscribe");
    request.user.id.0 = 999;
    request.member.as_mut().unwrap().user.id.0 = 999;
    request
        .member
        .as_mut()
        .unwrap()
        .roles
        .push(serenity::model::id::RoleId(400));
    let mut spy = SlashSpy::default();
    dispatch_slash(&policy(), &request, &mut spy).await;
    assert_eq!(spy.0, ["parse-and-work:subscribe"]);
}

#[tokio::test]
async fn watch_requires_one_known_structural_subcommand_before_business_parsing() {
    for (name, allowed_without_grant) in [("add", false), ("remove", false), ("list", true)] {
        let mut request = command("watch");
        request.data.options =
            serde_json::from_value(json!([{"name":name,"type":1,"options":[]}])).unwrap();
        let mut spy = SlashSpy::default();
        dispatch_slash(&WritePolicy::default(), &request, &mut spy).await;
        assert_eq!(
            spy.0,
            if allowed_without_grant {
                vec!["parse-and-work:watch"]
            } else {
                vec!["deny"]
            }
        );
    }
    for options in [
        json!([]),
        json!([{"name":"destroy","type":1}]),
        json!([{"name":"add","type":3,"value":"SENTINEL_BUSINESS_OPTION"}]),
        json!([{"name":"add","type":1},{"name":"remove","type":1}]),
        json!([{"name":"add","type":2}]),
    ] {
        let mut request = command("watch");
        request.data.options = serde_json::from_value(options).unwrap();
        let mut spy = SlashSpy::default();
        dispatch_slash(&policy(), &request, &mut spy).await;
        assert_eq!(spy.0, ["deny"]);
    }
}

use killbot_rust::config::{SsoState, StandingSource};
use killbot_rust::write_authorization::{
    dispatch_component, dispatch_dm, ContinuationAction, ContinuationAdapter, MemberIdentity,
    PendingStates, SubscriptionBinding,
};
use serenity::model::application::interaction::message_component::MessageComponentInteraction;
use serenity::model::channel::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const STATE: &str = "A1b2C3d4E5f6G7h8";

fn message(content: &str) -> Message {
    serde_json::from_value(json!({"id":"10","channel_id":"20","author":{"id":"300","username":"writer","discriminator":"0001","avatar":null},"content":content,"timestamp":"2026-11-17T12:00:00Z","tts":false,"mention_everyone":false,"mentions":[],"mention_roles":[],"attachments":[],"embeds":[],"pinned":false,"type":0})).unwrap()
}

fn component(select: bool) -> MessageComponentInteraction {
    serde_json::from_value(json!({"id":"11","application_id":"2","type":3,"channel_id":"20","locale":"en-US","user":{"id":"300","username":"writer","discriminator":"0001","avatar":null},"token":"SENTINEL_COMPONENT_TOKEN","version":1,"message":message(""),"data":{"custom_id":format!("standings_{}_{}",if select {"select"} else {"reauth"},STATE),"component_type":if select {3} else {2},"values":if select {vec!["500"]} else {vec![]}}})).unwrap()
}

fn pending() -> PendingStates {
    let mut original = command("sync_standings");
    original
        .member
        .as_mut()
        .unwrap()
        .roles
        .push(serenity::model::id::RoleId(400));
    Arc::new(Mutex::new(HashMap::from([(
        STATE.into(),
        Arc::new(SsoState {
            discord_user_id: serenity::model::id::UserId(300),
            subscription_id: "original-sub".into(),
            standing_source: StandingSource::Character,
            original_interaction: original,
        }),
    )])))
}

struct ContinuationSpy {
    events: Vec<&'static str>,
    member: Result<MemberIdentity, ()>,
    target: bool,
    character: bool,
    delay: std::time::Duration,
    drop_target_on_member: bool,
    acknowledged: bool,
}

impl Default for ContinuationSpy {
    fn default() -> Self {
        Self {
            events: vec![],
            member: Ok(MemberIdentity {
                guild_id: 100,
                user_id: 300,
                role_ids: vec![],
            }),
            target: true,
            character: true,
            delay: std::time::Duration::ZERO,
            drop_target_on_member: false,
            acknowledged: true,
        }
    }
}

#[async_trait]
impl ContinuationAdapter for ContinuationSpy {
    async fn acknowledge(&mut self) -> bool {
        self.events.push("ack-private");
        self.acknowledged
    }
    async fn deny(&mut self) {
        self.events.push("deny-private");
    }
    async fn member(&mut self, _: u64, _: u64) -> Result<MemberIdentity, ()> {
        self.events.push("member");
        if self.drop_target_on_member {
            self.target = false;
        }
        tokio::time::sleep(self.delay).await;
        self.member.clone()
    }
    fn target_exists(&mut self, binding: &SubscriptionBinding) -> bool {
        self.events.push("binding");
        self.target
            && binding.guild_id == 100
            && binding.channel_id == 200
            && binding.subscription_id == "original-sub"
    }
    fn owns_character(&mut self, _: u64, _: u64) -> bool {
        self.events.push("character-owner");
        self.character
    }
    async fn work(&mut self, _: Arc<SsoState>, action: ContinuationAction) -> String {
        self.events.push(match action {
            ContinuationAction::Complete { .. } => "oauth-and-persist",
            ContinuationAction::Select { .. } => "affiliation-and-persist",
            ContinuationAction::Reauthorize { .. } => "instructions",
        });
        "completed".into()
    }
    async fn finish(&mut self, _: &str) {
        self.events.push("finish-private");
    }
}

#[tokio::test]
async fn raw_callback_owner_mismatch_never_consumes_or_exchanges() {
    let states = pending();
    let mut request = message(&format!(
        "https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}"
    ));
    request.author.id.0 = 999;
    let mut spy = ContinuationSpy::default();
    dispatch_dm(&policy(), &states, &request, &mut spy).await;
    assert_eq!(spy.events, ["deny-private"]);
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn raw_component_acknowledges_privately_before_current_member_lookup() {
    let states = pending();
    let mut spy = ContinuationSpy::default();
    dispatch_component(&policy(), &states, &component(true), &mut spy).await;
    assert_eq!(
        spy.events,
        [
            "ack-private",
            "binding",
            "member",
            "binding",
            "character-owner",
            "affiliation-and-persist",
            "finish-private"
        ]
    );
    assert!(!states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn current_membership_and_policy_are_required_for_every_continuation() {
    let role_policy =
        WritePolicy::from_json(r#"{"version":1,"guilds":{"100":{"allowed_role_ids":["400"]}}}"#)
            .unwrap();
    for result in [
        Err(()),
        Ok(MemberIdentity {
            guild_id: 100,
            user_id: 300,
            role_ids: vec![],
        }),
        Ok(MemberIdentity {
            guild_id: 999,
            user_id: 300,
            role_ids: vec![400],
        }),
        Ok(MemberIdentity {
            guild_id: 100,
            user_id: 999,
            role_ids: vec![400],
        }),
    ] {
        let states = pending();
        let mut spy = ContinuationSpy {
            member: result,
            ..Default::default()
        };
        dispatch_component(&role_policy, &states, &component(false), &mut spy).await;
        assert_eq!(
            spy.events,
            ["ack-private", "binding", "member", "deny-private"]
        );
        assert!(states.lock().await.contains_key(STATE));
    }
    let states = pending();
    let mut allowed = ContinuationSpy {
        member: Ok(MemberIdentity {
            guild_id: 100,
            user_id: 300,
            role_ids: vec![400],
        }),
        ..Default::default()
    };
    dispatch_component(&role_policy, &states, &component(false), &mut allowed).await;
    assert_eq!(
        allowed.events,
        [
            "ack-private",
            "binding",
            "member",
            "binding",
            "instructions",
            "finish-private"
        ]
    );
    assert!(states.lock().await.contains_key(STATE));
    let mut revoked = ContinuationSpy::default();
    dispatch_component(
        &WritePolicy::default(),
        &states,
        &component(false),
        &mut revoked,
    )
    .await;
    assert_eq!(
        revoked.events,
        ["ack-private", "binding", "member", "deny-private"]
    );
    assert!(states.lock().await.contains_key(STATE));
    let mut departed = ContinuationSpy {
        member: Err(()),
        ..Default::default()
    };
    let callback = message(&format!(
        "https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}"
    ));
    dispatch_dm(&policy(), &states, &callback, &mut departed).await;
    assert_eq!(departed.events, ["binding", "member", "deny-private"]);
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test(start_paused = true)]
async fn membership_lookup_times_out_after_two_seconds_without_consuming_state() {
    let states = pending();
    let mut spy = ContinuationSpy {
        delay: std::time::Duration::from_secs(30),
        ..Default::default()
    };
    let start = tokio::time::Instant::now();
    dispatch_component(&policy(), &states, &component(true), &mut spy).await;
    assert_eq!(start.elapsed(), std::time::Duration::from_secs(2));
    assert_eq!(
        spy.events,
        ["ack-private", "binding", "member", "deny-private"]
    );
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn invalid_bindings_are_rejected_before_membership_or_business_io() {
    for mismatch in [
        "guild",
        "channel",
        "key",
        "owner",
        "missing-guild",
        "deleted-target",
    ] {
        let states = pending();
        let mut snapshot = (*states.lock().await[STATE]).clone();
        match mismatch {
            "guild" => snapshot.original_interaction.guild_id.as_mut().unwrap().0 = 999,
            "channel" => snapshot.original_interaction.channel_id.0 = 999,
            "key" => snapshot.subscription_id = "wrong-sub".into(),
            "owner" => snapshot.original_interaction.user.id.0 = 999,
            "missing-guild" => snapshot.original_interaction.guild_id = None,
            _ => {}
        }
        states.lock().await.insert(STATE.into(), Arc::new(snapshot));
        let mut spy = ContinuationSpy {
            target: mismatch != "deleted-target",
            ..Default::default()
        };
        dispatch_component(&policy(), &states, &component(true), &mut spy).await;
        assert!(!spy.events.contains(&"member"), "{mismatch}");
        assert_eq!(spy.events.last(), Some(&"deny-private"));
        assert!(states.lock().await.contains_key(STATE));
    }
    for mismatch in ["guild", "channel"] {
        let states = pending();
        let mut request = component(true);
        request.guild_id = Some(serenity::model::id::GuildId(if mismatch == "guild" {
            999
        } else {
            100
        }));
        request.channel_id.0 = if mismatch == "channel" { 999 } else { 200 };
        let mut spy = ContinuationSpy::default();
        dispatch_component(&policy(), &states, &request, &mut spy).await;
        assert_eq!(spy.events, ["ack-private", "deny-private"]);
        assert!(states.lock().await.contains_key(STATE));
    }
}

#[tokio::test]
async fn malformed_selection_and_unowned_character_do_not_consume_or_do_work() {
    for values in [
        vec![],
        vec!["500", "501"],
        vec!["no-number"],
        vec!["0"],
        vec!["0500"],
        vec!["18446744073709551616"],
    ] {
        let states = pending();
        let mut request = component(true);
        request.data.values = values.into_iter().map(str::to_string).collect();
        let mut spy = ContinuationSpy::default();
        dispatch_component(&policy(), &states, &request, &mut spy).await;
        assert_eq!(
            spy.events,
            [
                "ack-private",
                "binding",
                "member",
                "binding",
                "deny-private"
            ]
        );
        assert!(states.lock().await.contains_key(STATE));
    }
    let states = pending();
    let mut spy = ContinuationSpy {
        character: false,
        ..Default::default()
    };
    dispatch_component(&policy(), &states, &component(true), &mut spy).await;
    assert_eq!(
        spy.events,
        [
            "ack-private",
            "binding",
            "member",
            "binding",
            "character-owner",
            "deny-private"
        ]
    );
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn malformed_callbacks_and_unknown_components_never_consume_or_do_work() {
    for content in [
        format!("https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}&state={STATE}"),
        format!("https://github.headempty.space/wrong?code=SENTINEL_CODE&state={STATE}"),
        format!("https://github.headempty.space/?state={STATE}"),
        "https://github.headempty.space/?code=SENTINEL_CODE&state=unknown".into(),
    ] {
        let states = pending();
        let mut spy = ContinuationSpy::default();
        dispatch_dm(&policy(), &states, &message(&content), &mut spy).await;
        assert_eq!(spy.events.last(), Some(&"deny-private"));
        assert!(!spy.events.contains(&"oauth-and-persist"));
        assert!(states.lock().await.contains_key(STATE));
    }
    for custom_id in ["unknown", "standings_select_", "standings_reauth_invalid"] {
        let states = pending();
        let mut request = component(false);
        request.data.custom_id = custom_id.into();
        let mut spy = ContinuationSpy::default();
        dispatch_component(&policy(), &states, &request, &mut spy).await;
        assert_eq!(spy.events, ["ack-private", "deny-private"]);
        assert!(states.lock().await.contains_key(STATE));
    }
}

#[tokio::test]
async fn concurrent_completion_and_replay_can_enter_business_work_only_once() {
    let states = pending();
    let request = message(&format!(
        "https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}"
    ));
    let mut first = ContinuationSpy {
        delay: std::time::Duration::from_millis(5),
        ..Default::default()
    };
    let mut second = ContinuationSpy {
        delay: std::time::Duration::from_millis(5),
        ..Default::default()
    };
    let policy = policy();
    tokio::join!(
        dispatch_dm(&policy, &states, &request, &mut first),
        dispatch_dm(&policy, &states, &request, &mut second)
    );
    assert_eq!(
        first
            .events
            .iter()
            .chain(&second.events)
            .filter(|event| **event == "oauth-and-persist")
            .count(),
        1
    );
    let mut replay = ContinuationSpy::default();
    dispatch_dm(&policy, &states, &request, &mut replay).await;
    assert_eq!(replay.events, ["deny-private"]);
    assert!(states.lock().await.is_empty());
}

#[tokio::test]
async fn reauthorization_keeps_the_state_for_later_oauth_completion() {
    let states = pending();
    let mut reauth = ContinuationSpy::default();
    dispatch_component(&policy(), &states, &component(false), &mut reauth).await;
    assert!(states.lock().await.contains_key(STATE));
    let mut completion = ContinuationSpy::default();
    dispatch_dm(
        &policy(),
        &states,
        &message(&format!(
            "https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}"
        )),
        &mut completion,
    )
    .await;
    assert_eq!(
        completion.events,
        [
            "binding",
            "member",
            "binding",
            "oauth-and-persist",
            "finish-private"
        ]
    );
    assert!(states.lock().await.is_empty());
}

#[test]
fn missing_policy_still_initializes_reads_and_feed() {
    let temporary = tempfile::tempdir().unwrap();
    let mut events = vec![];
    killbot_rust::write_authorization::initialize_with_policy(
        &temporary.path().join("missing.json"),
        &temporary.path().join("config"),
        |policy| {
            assert!(!policy.allows(100, 300, &[400]));
            events.push("read-init");
            events.push("feed-init");
        },
    );
    assert_eq!(events, ["read-init", "feed-init"]);
}

#[test]
fn production_registration_population_is_exactly_the_classified_commands() {
    let mut actual = killbot_rust::commands::registered_commands()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = [
        "ping",
        "diag",
        "health",
        "sov_timers",
        "find_unsubscribed",
        "subscribe",
        "unsubscribe",
        "contract_subscribe",
        "contract_unsubscribe",
        "sov_subscribe",
        "sov_unsubscribe",
        "watch",
        "watch_subscribe",
        "watch_unsubscribe",
        "sync_standings",
        "sync_remove",
        "sync_clear",
    ]
    .map(str::to_string)
    .to_vec();
    expected.sort();
    assert_eq!(actual, expected);
}

#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn decision_logs_contain_only_bounded_metadata_and_exclude_secrets() {
    let captured = LogCapture::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_target(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    {
        let states = pending();
        let mut spy = ContinuationSpy::default();
        dispatch_dm(
            &policy(),
            &states,
            &message(&format!(
                "https://github.headempty.space/?code=SENTINEL_CODE&state={STATE}"
            )),
            &mut spy,
        )
        .await;
        let mut request = command("subscribe");
        request.data.options = serde_json::from_value(
            json!([{"name":"secret","type":3,"value":"SENTINEL_POLICY_CONTENT"}]),
        )
        .unwrap();
        let mut spy = SlashSpy::default();
        dispatch_slash(&WritePolicy::default(), &request, &mut spy).await;
    }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("invalid-policy.json");
    std::fs::write(
        &path,
        r#"{"version":1,"SENTINEL_POLICY_CONTENT":"SENTINEL_ACCESS_TOKEN"}"#,
    )
    .unwrap();
    killbot_rust::write_authorization::initialize_with_policy(
        &path,
        &temporary.path().join("config"),
        |_| (),
    );
    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("write_authorization"));
    assert!(logs.contains("actor=300"));
    assert!(logs.contains("guild=100"));
    for sentinel in [
        "SENTINEL_CODE",
        "SENTINEL_INTERACTION_TOKEN",
        "SENTINEL_COMPONENT_TOKEN",
        STATE,
        "https://github.headempty.space/",
        "SENTINEL_POLICY_CONTENT",
        "SENTINEL_ACCESS_TOKEN",
    ] {
        assert!(!logs.contains(sentinel), "{sentinel}");
    }
}

#[tokio::test]
async fn final_update_uses_current_same_key_subscription_and_never_recreates_deleted_target() {
    use killbot_rust::config::{Action, Filter, FilterNode, SimpleFilter, Subscription};
    use killbot_rust::standings_continuation::apply_standings_to_bound_subscription;
    use serenity::model::id::GuildId;
    let states = pending();
    let state = states.lock().await[STATE].clone();
    let make = |id: &str, channel: &str, description: &str| Subscription {
        id: id.into(),
        description: description.into(),
        root_filter: FilterNode::Condition(Filter::Simple(SimpleFilter::IsNpc(false))),
        action: Action {
            channel_id: channel.into(),
            ping_type: None,
        },
    };
    let mut subscriptions = HashMap::from([
        (
            GuildId(100),
            vec![
                make("original-sub", "200", "edited after preflight"),
                make("original-sub", "999", "another channel"),
                make("another-sub", "200", "another key"),
            ],
        ),
        (
            GuildId(999),
            vec![make("original-sub", "200", "another guild")],
        ),
    ]);
    assert!(apply_standings_to_bound_subscription(
        &mut subscriptions,
        &state,
        500
    ));
    assert_eq!(
        subscriptions[&GuildId(100)][0].description,
        "edited after preflight"
    );
    assert!(
        matches!(&subscriptions[&GuildId(100)][0].root_filter,FilterNode::And(filters) if filters.len()==2)
    );
    for (guild, index) in [(100, 1), (100, 2), (999, 0)] {
        assert_eq!(
            subscriptions[&GuildId(guild)][index].root_filter,
            FilterNode::Condition(Filter::Simple(SimpleFilter::IsNpc(false)))
        );
    }
    subscriptions.get_mut(&GuildId(100)).unwrap().remove(0);
    let before = subscriptions.clone();
    assert!(!apply_standings_to_bound_subscription(
        &mut subscriptions,
        &state,
        500
    ));
    assert_eq!(subscriptions, before);
    subscriptions.get_mut(&GuildId(100)).unwrap().push(make(
        "original-sub",
        "200",
        "recreated same key",
    ));
    assert!(apply_standings_to_bound_subscription(
        &mut subscriptions,
        &state,
        500
    ));
    assert_eq!(
        subscriptions[&GuildId(100)][2].description,
        "recreated same key"
    );
}

#[tokio::test]
async fn malformed_operation_kinds_and_nested_watch_discriminators_never_execute() {
    let mut wrong_interaction = command("subscribe");
    wrong_interaction.kind =
        serenity::model::application::interaction::InteractionType::MessageComponent;
    let mut wrong_command = command("subscribe");
    wrong_command.data.kind = serenity::model::application::command::CommandType::User;
    let mut nested_watch = command("watch");
    nested_watch.data.options = serde_json::from_value(
        json!([{"name":"add","type":1,"options":[{"name":"remove","type":1}]}]),
    )
    .unwrap();
    for request in [wrong_interaction, wrong_command, nested_watch] {
        let mut spy = SlashSpy::default();
        dispatch_slash(&policy(), &request, &mut spy).await;
        assert_eq!(spy.0, ["deny"]);
    }
}

#[tokio::test]
async fn health_keeps_its_separate_operator_guard_at_the_request_boundary() {
    let mut spy = SlashSpy::default();
    dispatch_slash(&policy(), &command("health"), &mut spy).await;
    assert_eq!(spy.0, ["deny"]);
    let mut operator = command("health");
    operator.user.id.0 = killbot_rust::commands::health::HEALTH_OPERATOR_ID;
    let mut spy = SlashSpy::default();
    dispatch_slash(&WritePolicy::default(), &operator, &mut spy).await;
    assert_eq!(spy.0, ["parse-and-work:health"]);
}

#[tokio::test]
async fn deletion_during_membership_preflight_prevents_credential_or_affiliation_work() {
    let states = pending();
    let mut spy = ContinuationSpy {
        drop_target_on_member: true,
        ..Default::default()
    };
    dispatch_component(&policy(), &states, &component(true), &mut spy).await;
    assert_eq!(
        spy.events,
        [
            "ack-private",
            "binding",
            "member",
            "binding",
            "deny-private"
        ]
    );
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn acknowledgement_failure_and_component_owner_mismatch_leave_state_untouched() {
    let states = pending();
    let mut spy = ContinuationSpy {
        acknowledged: false,
        ..Default::default()
    };
    dispatch_component(&policy(), &states, &component(true), &mut spy).await;
    assert_eq!(spy.events, ["ack-private"]);
    assert!(states.lock().await.contains_key(STATE));
    let mut wrong_owner = component(false);
    wrong_owner.user.id.0 = 999;
    let mut spy = ContinuationSpy::default();
    dispatch_component(&policy(), &states, &wrong_owner, &mut spy).await;
    assert_eq!(spy.events, ["ack-private", "deny-private"]);
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn unauthorized_continuations_do_not_parse_character_or_code_business_fields() {
    let states = pending();
    let mut request = component(true);
    request.data.values = vec!["SENTINEL_MALFORMED_CHARACTER".into()];
    let mut spy = ContinuationSpy::default();
    dispatch_component(&WritePolicy::default(), &states, &request, &mut spy).await;
    assert_eq!(
        spy.events,
        ["ack-private", "binding", "member", "deny-private"]
    );
    assert!(states.lock().await.contains_key(STATE));
    let mut spy = ContinuationSpy::default();
    dispatch_dm(
        &WritePolicy::default(),
        &states,
        &message(&format!("https://github.headempty.space/?state={STATE}")),
        &mut spy,
    )
    .await;
    assert_eq!(spy.events, ["binding", "member", "deny-private"]);
    assert!(states.lock().await.contains_key(STATE));
}

#[tokio::test]
async fn guild_component_requires_matching_raw_member_identity() {
    for mismatch in ["missing-member", "wrong-user", "wrong-guild"] {
        let states = pending();
        let mut request = component(true);
        request.guild_id = Some(serenity::model::id::GuildId(100));
        request.channel_id.0 = 200;
        request.member = command("sync_standings").member;
        match mismatch {
            "missing-member" => request.member = None,
            "wrong-user" => request.member.as_mut().unwrap().user.id.0 = 999,
            _ => request.member.as_mut().unwrap().guild_id.0 = 999,
        }
        let mut spy = ContinuationSpy::default();
        dispatch_component(&policy(), &states, &request, &mut spy).await;
        assert_eq!(spy.events, ["ack-private", "deny-private"]);
        assert!(states.lock().await.contains_key(STATE));
    }
}
