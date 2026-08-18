use killbot_rust::contract_intelligence::{
    run_health_watchdog_loop, HealthWatchdogConfig, DEFAULT_HEALTH_CHANNEL_ID,
};
use killbot_rust::discord_bot::DiscordHealthPublisher;
use serenity::http::Http;
use std::env::VarError;
use std::sync::Arc;
use tracing::{error, info};

fn required_watchdog_environment(name: &str) -> Result<String, String> {
    required_watchdog_environment_value(name, std::env::var(name))
}

fn required_watchdog_environment_value(
    name: &str,
    value: Result<String, VarError>,
) -> Result<String, String> {
    match value {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) => Err(format!("{name} must not be empty or whitespace")),
        Err(VarError::NotPresent) => Err(format!("{name} is required")),
        Err(VarError::NotUnicode(_)) => Err(format!("{name} must be valid Unicode")),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let database_url = match required_watchdog_environment("CONTRACT_DATABASE_URL") {
        Ok(value) => value,
        Err(reason) => {
            error!("health watchdog disabled: {reason}");
            return;
        }
    };
    let token = match required_watchdog_environment("DISCORD_BOT_TOKEN") {
        Ok(value) => value,
        Err(reason) => {
            error!("health watchdog disabled: {reason}");
            return;
        }
    };
    let config = match HealthWatchdogConfig::from_environment() {
        Ok(config) => config,
        Err(error) => {
            error!("health watchdog disabled by invalid configuration: {error}");
            return;
        }
    };
    info!(
        channel_id = config.channel_id,
        default_channel_id = DEFAULT_HEALTH_CHANNEL_ID,
        interval_secs = config.evaluation_interval.as_secs(),
        "starting independent Discord health watchdog"
    );
    let publisher = Arc::new(DiscordHealthPublisher::new(Arc::new(Http::new(&token))));
    run_health_watchdog_loop(database_url, config, publisher).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn required_watchdog_environment_rejects_missing_non_unicode_empty_and_whitespace_values() {
        for name in ["CONTRACT_DATABASE_URL", "DISCORD_BOT_TOKEN"] {
            assert!(required_watchdog_environment_value(name, Err(VarError::NotPresent)).is_err());
            assert!(required_watchdog_environment_value(name, Ok(String::new())).is_err());
            assert!(required_watchdog_environment_value(name, Ok(" \t\n ".to_string())).is_err());
            assert_eq!(
                required_watchdog_environment_value(name, Ok("value".to_string())),
                Ok("value".to_string())
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;

            for name in ["CONTRACT_DATABASE_URL", "DISCORD_BOT_TOKEN"] {
                assert!(required_watchdog_environment_value(
                    name,
                    Err(VarError::NotUnicode(OsString::from_vec(vec![0x80])))
                )
                .is_err());
            }
        }
    }
}
