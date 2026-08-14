use async_trait::async_trait;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::future::Future;

#[async_trait]
pub trait ContractCommandResponder {
    async fn defer_ephemeral(&mut self) -> Result<(), String>;
    async fn edit_ephemeral(&mut self, content: String) -> Result<(), String>;
}

pub struct SerenityContractCommandResponder<'a> {
    ctx: &'a Context,
    command: &'a ApplicationCommandInteraction,
}

impl<'a> SerenityContractCommandResponder<'a> {
    pub fn new(ctx: &'a Context, command: &'a ApplicationCommandInteraction) -> Self {
        Self { ctx, command }
    }
}

#[async_trait]
impl ContractCommandResponder for SerenityContractCommandResponder<'_> {
    async fn defer_ephemeral(&mut self) -> Result<(), String> {
        self.command
            .defer_ephemeral(&self.ctx.http)
            .await
            .map_err(|error| error.to_string())
    }

    async fn edit_ephemeral(&mut self, content: String) -> Result<(), String> {
        self.command
            .edit_original_interaction_response(&self.ctx.http, |response| {
                response.content(content)
            })
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub async fn defer_then_edit<R, Work>(responder: &mut R, work: Work) -> Result<(), String>
where
    R: ContractCommandResponder,
    Work: Future<Output = String> + Send,
{
    responder.defer_ephemeral().await?;
    let response = work.await;
    responder.edit_ephemeral(response).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct RecordingResponder {
        events: Arc<Mutex<Vec<String>>>,
        responses: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ContractCommandResponder for RecordingResponder {
        async fn defer_ephemeral(&mut self) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push("defer:ephemeral".to_string());
            Ok(())
        }

        async fn edit_ephemeral(&mut self, content: String) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push("edit:ephemeral".to_string());
            self.responses.lock().unwrap().push(content);
            Ok(())
        }
    }

    #[tokio::test]
    async fn deferred_contract_commands_edit_ephemeral_responses_after_slow_work() {
        for expected in ["saved", "invalid subscription", "database unavailable"] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let responses = Arc::new(Mutex::new(Vec::new()));
            let mut responder = RecordingResponder {
                events: events.clone(),
                responses: responses.clone(),
            };
            defer_then_edit(&mut responder, {
                let events = events.clone();
                async move {
                    assert_eq!(events.lock().unwrap().as_slice(), ["defer:ephemeral"]);
                    events.lock().unwrap().push("slow-store".to_string());
                    expected.to_string()
                }
            })
            .await
            .expect("defer and edit command response");
            assert_eq!(
                events.lock().unwrap().as_slice(),
                ["defer:ephemeral", "slow-store", "edit:ephemeral"]
            );
            assert_eq!(responses.lock().unwrap().as_slice(), [expected]);
        }
    }
}
