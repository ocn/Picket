use std::path::Path;
use killbot_rust::config::{load_from_json_file, Subscription};

#[test]
fn test_load_subscription_file() {
    // This path is now relative to the top-level `killbot-rust` directory,
    // where `cargo test` is run from.
    let path = Path::new("../config/888224317991706685.new.json");

    assert!(path.exists(), "Subscription file does not exist at {:?}", path);

    let result = load_from_json_file::<Vec<Subscription>>(path);

    assert!(result.is_ok(), "Failed to parse subscription file: {:?}", result.err());

    let subscriptions = result.unwrap();
    assert_eq!(subscriptions.len(), 202, "Incorrect number of subscriptions loaded");

    assert_eq!(subscriptions[0].id, "1");
    assert_eq!(subscriptions[0].action.channel_id, 1090110979083354200);
}
