# Workstream: Discord Commands

## Overview

Slash commands allow users to manage killmail subscriptions directly in Discord.

## Current Commands

| Command | Purpose | File |
|---------|---------|------|
| `/subscribe` | Create/update subscription | `src/commands/subscribe.rs` |
| `/unsubscribe` | Remove subscription | `src/commands/unsubscribe.rs` |
| `/diag` | Show channel diagnostics | `src/commands/diag.rs` |
| `/sync_standings` | Sync EVE standings | `src/commands/sync_standings.rs` |
| `/sync_remove` | Remove synced standings | `src/commands/sync_remove.rs` |
| `/sync_clear` | Clear all standings | `src/commands/sync_clear.rs` |
| `/ping` | Test command | `src/commands.rs` |

## Command Pattern

All commands implement the `Command` trait:

```rust
#[async_trait]
pub trait Command: Send + Sync {
    fn name(&self) -> String;
    fn register<'a>(&self, cmd: &'a mut CreateApplicationCommand)
        -> &'a mut CreateApplicationCommand;
    async fn execute(&self, ctx: &Context, cmd: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>);
}
```

---

## Backlog

### High Priority

- [ ] **Better error messages** - User-friendly error feedback
- [ ] **Subscription list command** - `/list` to show all subscriptions in channel
- [ ] **Ephemeral responses** - Make responses visible only to command user

### Medium Priority

- [ ] **Subscription editing** - Modify existing subscription without recreating
- [ ] **Permission checks** - Restrict commands to admin roles
- [ ] **Bulk operations** - Enable/disable all subscriptions in server

### Low Priority

- [ ] **Autocomplete** - Suggest region/ship names when typing
- [ ] **Import/export** - JSON import/export of subscriptions
- [ ] **Test mode** - Show what would match without actually subscribing

---

## Technical Notes

### Global vs Guild Commands

Currently using global commands (registered in `ready` event). This means:
- Commands available in all servers
- Takes up to 1 hour to propagate changes
- No per-server customization

For faster iteration during development, consider guild commands:
```rust
GuildId(guild_id).set_application_commands(&ctx.http, |commands| { ... }).await
```

### Interaction Response Timing

Discord requires a response within 3 seconds. For slow operations:
```rust
// 1. Acknowledge immediately
command.defer(&ctx.http).await?;

// 2. Do slow work
let result = slow_operation().await;

// 3. Edit the response
command.edit_original_interaction_response(&ctx.http, |r| {
    r.content("Done!")
}).await?;
```
