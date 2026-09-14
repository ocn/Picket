use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::sov_feed::{
    available_sov_store, parse_tminus_marks_minutes, parse_tz_window, SovFilter,
    SovFilterCondition, SovFilterNode, SovSubscription, SOV_REACHABLE_MAX_JUMPS,
    SOV_TMINUS_MARKS_OPTION_KEY, SOV_TZ_SHIFT_ENABLED_OPTION_KEY, SOV_TZ_WINDOW_OPTION_KEY,
};
use crate::SovStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::model::Permissions;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SovSubscribeCommand;

/// Owned parse of one invocation's documents, used by the unit tests as the
/// composition seam (`subscription_from_documents`). Production parses into
/// [`SovSubscribeInput`] and resolves against the stored subscription
/// instead (ticket 15).
#[cfg(test)]
struct SovSubscriptionDocuments<'a> {
    name: &'a str,
    filter: &'a str,
    region_id: Option<i64>,
    defender_alliance_id: Option<i64>,
    max_jumps: Option<i64>,
    /// Convenience: whether the `Reachable` leaf composed from `max_jumps`
    /// allows frigate-sized wormhole connections on its route (ticket 05,
    /// spec "`/sov_subscribe` gains an `allow_frigate_holes` boolean
    /// convenience option"). Meaningless without `max_jumps` -- validated
    /// in `subscription_from_documents`.
    allow_frigate_holes: Option<bool>,
    role_id: Option<u64>,
    ping_user_id: Option<u64>,
    /// Raw `tminus_marks` command option: a comma-separated list of
    /// minutes, or an empty string to disable marks. `None` when the
    /// option was omitted, which leaves the stored `options` document
    /// without the key so evaluation applies the default (spec: "an
    /// option; default [120, 30] when omitted; an explicit empty list
    /// disables marks").
    tminus_marks: Option<&'a str>,
    /// Raw `tz_window` command option: `HH:MM-HH:MM` EVE/UTC, may cross
    /// midnight (ticket 07). `None` when omitted, leaving the stored
    /// `options` document without the key so evaluation applies the
    /// default (00:00-04:00).
    tz_window: Option<&'a str>,
    /// `tz_shift_enabled` command option: whether the `tz_window_entered`
    /// stage participates for this subscription at all (ticket 07;
    /// default false when omitted).
    tz_shift_enabled: Option<bool>,
}

/// The convenience-leaf provenance keys stored inside a subscription's
/// `options` document (ticket 15). The convenience options
/// (`region_id`/`defender_alliance_id`/`max_jumps`/`allow_frigate_holes`)
/// compose into the stored `filter` tree with no provenance of their own,
/// so the effective value is round-tripped here as well. Recording them
/// alongside the existing `tminus_marks`/`tz_window`/`tz_shift_enabled`
/// keys lets the same per-key [`SovSubscribeCommand::merge_options`] overlay
/// carry an omitted convenience option forward on a partial re-subscribe
/// (instead of silently dropping its leaf), and lets a re-subscribe
/// recompose the filter from the effective values rather than the previous
/// composed tree.
const SOV_REGION_ID_OPTION_KEY: &str = "region_id";
const SOV_DEFENDER_ALLIANCE_ID_OPTION_KEY: &str = "defender_alliance_id";
const SOV_MAX_JUMPS_OPTION_KEY: &str = "max_jumps";
const SOV_ALLOW_FRIGATE_HOLES_OPTION_KEY: &str = "allow_frigate_holes";

/// Provenance key holding the user's *explicit* `filter` JSON (the
/// `SovFilter` they typed), stored alongside the convenience-option
/// provenance keys (ticket 15). The persisted `sov_subscriptions.filter`
/// column always holds the fully *composed* tree (explicit filter ANDed
/// with the convenience leaves) so evaluation is unchanged; but the
/// composed tree cannot be decomposed unambiguously, so a partial
/// re-subscribe recomposes the filter from this recorded explicit filter
/// plus the effective convenience values rather than from the previous
/// composed tree. Carrying it forward is what lets `/sov_subscribe name:X
/// max_jumps:6` (filter omitted) keep the previously-typed filter instead
/// of failing for want of one.
///
/// A subscription persisted before ticket 15 has *none* of these five
/// provenance keys; such a "legacy" row is detected by
/// [`is_ticket15_provenance`] and its whole composed `filter` column is
/// treated as the explicit base (with no convenience provenance), so a
/// carried-forward re-subscribe reproduces exactly what was stored.
const SOV_EXPLICIT_FILTER_OPTION_KEY: &str = "explicit_filter";

/// The ticket-15 provenance keys, in the display order the confirmation and
/// clear-sentinel error messages use.
const SOV_PROVENANCE_KEYS: &[&str] = &[
    SOV_EXPLICIT_FILTER_OPTION_KEY,
    SOV_REGION_ID_OPTION_KEY,
    SOV_DEFENDER_ALLIANCE_ID_OPTION_KEY,
    SOV_MAX_JUMPS_OPTION_KEY,
    SOV_ALLOW_FRIGATE_HOLES_OPTION_KEY,
];

/// Whether a stored `options` document was written by ticket-15 resolution
/// (it carries at least one provenance key) versus a legacy row that
/// predates it (none of the five keys). A legacy row's composed `filter`
/// column is the only record of its filter, so it is treated as the
/// explicit base on the first carry-forward re-subscribe.
fn is_ticket15_provenance(options: &serde_json::Value) -> bool {
    SOV_PROVENANCE_KEYS
        .iter()
        .any(|key| options.get(*key).is_some())
}

/// How one top-level field's effective value was arrived at while composing
/// a re-subscribe against the stored subscription (ticket 15). Drives both
/// the ephemeral confirmation's replaced/preserved/cleared lists and, for
/// `Preserved`, the "carried forward" wording.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FieldProvenance {
    /// The invocation supplied this field; it replaced whatever was stored.
    Replaced,
    /// The invocation omitted this field and the stored subscription had a
    /// value, carried forward unchanged.
    Preserved,
    /// The `clear` sentinel named this field; its stored value was removed.
    Cleared,
    /// The invocation omitted this field and nothing was stored; nothing to
    /// report.
    Untouched,
}

/// A top-level subscription field that the `clear` sentinel can remove on a
/// re-subscribe (ticket 15). A single `clear:<comma list>` string option is
/// the clearing mechanism for every field regardless of its Discord option
/// type: the integer convenience options (`region_id`, `max_jumps`, ...)
/// cannot carry an in-band sentinel value, and a `Role` option cannot
/// either, so one string list is the one consistent, discoverable
/// mechanism across them all while keeping the command within Discord's
/// 25-option limit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SovClearableField {
    Role,
    User,
    RegionId,
    DefenderAllianceId,
    MaxJumps,
    AllowFrigateHoles,
}

impl SovClearableField {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim() {
            "role" => Ok(Self::Role),
            "user" => Ok(Self::User),
            "region_id" => Ok(Self::RegionId),
            "defender_alliance_id" => Ok(Self::DefenderAllianceId),
            "max_jumps" => Ok(Self::MaxJumps),
            "allow_frigate_holes" => Ok(Self::AllowFrigateHoles),
            other => Err(format!(
                "unknown clear field '{other}' (expected one of role, user, region_id, defender_alliance_id, max_jumps, allow_frigate_holes)"
            )),
        }
    }
}

/// Parses the `clear` command option: a comma-separated list of top-level
/// field names to remove from the stored subscription. Empty or
/// all-whitespace input clears nothing; duplicates collapse; an unknown
/// name is rejected so a typo never silently clears nothing.
fn parse_clear_fields(raw: &str) -> Result<Vec<SovClearableField>, String> {
    let mut fields = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let field = SovClearableField::parse(part)?;
        if !fields.contains(&field) {
            fields.push(field);
        }
    }
    Ok(fields)
}

/// The owned parse of one `/sov_subscribe` invocation, produced before the
/// (async) store lookup so the resolved documents can be composed against
/// the previously-stored subscription once it is read.
struct SovSubscribeInput {
    name: String,
    /// The explicit `filter` JSON when supplied; `None` when omitted, in
    /// which case a re-subscribe carries the stored explicit filter forward
    /// (ticket 15). Required for a first-time subscribe (enforced when the
    /// composed filter turns out empty).
    filter: Option<String>,
    region_id: Option<i64>,
    defender_alliance_id: Option<i64>,
    max_jumps: Option<i64>,
    allow_frigate_holes: Option<bool>,
    role_id: Option<u64>,
    /// The `user:` option: an optional direct Discord user ping, a top-level
    /// carry-forward/clear field (ticket 20).
    ping_user_id: Option<u64>,
    tminus_marks: Option<String>,
    tz_window: Option<String>,
    tz_shift_enabled: Option<bool>,
    clear: Vec<SovClearableField>,
}

/// The outcome of composing one `/sov_subscribe` invocation against the
/// previously-stored subscription (ticket 15): the subscription to persist,
/// plus which top-level fields were supplied/replaced, carried forward
/// (preserved), or cleared, so the ephemeral confirmation can state exactly
/// what happened to each.
struct ResolvedSovSubscription {
    subscription: SovSubscription,
    replaced: Vec<&'static str>,
    preserved: Vec<&'static str>,
    cleared: Vec<&'static str>,
}

/// The outcome of reading the previously-stored subscription before
/// composing a re-subscribe (ticket 15, review finding 1). A store read that
/// *errors* (pool exhaustion, timeout, restart, or a row that fails to
/// deserialize) must NOT be collapsed into "no existing subscription" -- that
/// would compose a first-time subscription and overwrite the stored one,
/// dropping exactly the fields this ticket carries forward. Only a confirmed
/// absence is [`Absent`](Self::Absent).
enum ExistingLookup {
    /// The store confirmed no subscription exists for this key yet.
    Absent,
    /// The store returned the stored subscription.
    Present(SovSubscription),
    /// The store could not be read; the carried error message is logged and
    /// the invocation must abort without changing anything.
    Unreadable(String),
}

/// Why a re-subscribe was not persisted, so `execute` can pick the right
/// ephemeral reply (ticket 15, review finding 1).
#[derive(Debug)]
enum ResolveAbort {
    /// Composition rejected the invocation (bad filter JSON, empty tree,
    /// validation, a legacy row needing a re-typed filter, ...).
    Invalid(String),
    /// The stored subscription could not be read; nothing was changed.
    Unreadable,
}

impl SovSubscribeCommand {
    /// Parses one `/sov_subscribe` invocation into an owned
    /// [`SovSubscribeInput`] (before the async store lookup). `filter` is
    /// optional (ticket 15: a re-subscribe may omit it to carry the stored
    /// explicit filter forward); the `clear` sentinel is parsed and
    /// validated here so a typo is rejected before any store access.
    fn input_from_command(
        command: &ApplicationCommandInteraction,
    ) -> Result<SovSubscribeInput, String> {
        let name = match get_option_value(&command.data.options, "name") {
            Some(CommandDataOptionValue::String(value)) => value.clone(),
            _ => return Err("missing required option: name".to_string()),
        };
        let filter = match get_option_value(&command.data.options, "filter") {
            Some(CommandDataOptionValue::String(value)) => Some(value.clone()),
            None => None,
            _ => return Err("filter must be a string option".to_string()),
        };
        let region_id = match get_option_value(&command.data.options, "region_id") {
            Some(CommandDataOptionValue::Integer(value)) => Some(*value),
            None => None,
            _ => return Err("region_id must be an integer option".to_string()),
        };
        let defender_alliance_id =
            match get_option_value(&command.data.options, "defender_alliance_id") {
                Some(CommandDataOptionValue::Integer(value)) => Some(*value),
                None => None,
                _ => return Err("defender_alliance_id must be an integer option".to_string()),
            };
        let max_jumps = match get_option_value(&command.data.options, "max_jumps") {
            Some(CommandDataOptionValue::Integer(value)) => Some(*value),
            None => None,
            _ => return Err("max_jumps must be an integer option".to_string()),
        };
        let allow_frigate_holes =
            match get_option_value(&command.data.options, "allow_frigate_holes") {
                Some(CommandDataOptionValue::Boolean(value)) => Some(*value),
                None => None,
                _ => return Err("allow_frigate_holes must be a boolean option".to_string()),
            };
        let role_id = match get_option_value(&command.data.options, "role") {
            Some(CommandDataOptionValue::Role(role)) => Some(role.id.0),
            None => None,
            _ => return Err("role must be a role option".to_string()),
        };
        let ping_user_id = match get_option_value(&command.data.options, "user") {
            Some(CommandDataOptionValue::User(user, _member)) => Some(user.id.0),
            None => None,
            _ => return Err("user must be a user option".to_string()),
        };
        let tminus_marks = match get_option_value(&command.data.options, "tminus_marks") {
            Some(CommandDataOptionValue::String(value)) => Some(value.clone()),
            None => None,
            _ => return Err("tminus_marks must be a string option".to_string()),
        };
        let tz_window = match get_option_value(&command.data.options, "tz_window") {
            Some(CommandDataOptionValue::String(value)) => Some(value.clone()),
            None => None,
            _ => return Err("tz_window must be a string option".to_string()),
        };
        let tz_shift_enabled = match get_option_value(&command.data.options, "tz_shift_enabled") {
            Some(CommandDataOptionValue::Boolean(value)) => Some(*value),
            None => None,
            _ => return Err("tz_shift_enabled must be a boolean option".to_string()),
        };
        let clear = match get_option_value(&command.data.options, "clear") {
            Some(CommandDataOptionValue::String(value)) => parse_clear_fields(value)?,
            None => Vec::new(),
            _ => return Err("clear must be a string option".to_string()),
        };
        Ok(SovSubscribeInput {
            name,
            filter,
            region_id,
            defender_alliance_id,
            max_jumps,
            allow_frigate_holes,
            role_id,
            ping_user_id,
            tminus_marks,
            tz_window,
            tz_shift_enabled,
            clear,
        })
    }

    #[cfg(test)]
    fn subscription_from_documents(
        guild_id: u64,
        channel_id: u64,
        documents: SovSubscriptionDocuments<'_>,
    ) -> Result<SovSubscription, String> {
        let explicit = serde_json::from_str::<SovFilter>(documents.filter)
            .map_err(|error| format!("invalid filter JSON: {error}"))?;
        let filter = compose_filter(
            Some(explicit),
            documents.region_id,
            documents.defender_alliance_id,
            documents.max_jumps,
            documents.allow_frigate_holes,
        )?;
        let options = behavioral_options(
            documents.tminus_marks,
            documents.tz_window,
            documents.tz_shift_enabled,
        )?;
        let subscription = SovSubscription {
            guild_id,
            channel_id,
            name: documents.name.to_string(),
            filter,
            options,
            role_id: documents.role_id,
            ping_user_id: documents.ping_user_id,
        };
        subscription.validate()?;
        Ok(subscription)
    }

    /// Decides whether to compose a re-subscribe against the store-read
    /// outcome (ticket 15, review finding 1). An [`ExistingLookup::Unreadable`]
    /// read aborts *without* resolving or upserting so a transient DB error or
    /// an undeserializable row can never silently overwrite the stored
    /// subscription with a first-time one; only a confirmed
    /// [`ExistingLookup::Absent`] composes as first-time.
    fn resolve_from_lookup(
        guild_id: u64,
        channel_id: u64,
        input: &SovSubscribeInput,
        lookup: ExistingLookup,
    ) -> Result<ResolvedSovSubscription, ResolveAbort> {
        let existing = match lookup {
            ExistingLookup::Unreadable(error) => {
                error!("cannot read existing sov subscription for carry-forward: {error}");
                return Err(ResolveAbort::Unreadable);
            }
            ExistingLookup::Absent => None,
            ExistingLookup::Present(subscription) => Some(subscription),
        };
        Self::resolve_subscription(guild_id, channel_id, input, existing.as_ref())
            .map_err(ResolveAbort::Invalid)
    }

    /// Composes one `/sov_subscribe` invocation against the
    /// previously-stored subscription of the same `(guild, channel, name)`
    /// and produces the subscription to persist plus the per-field
    /// provenance the confirmation reports (ticket 15).
    ///
    /// Every optional top-level field the invocation omitted is carried
    /// forward from `existing`; a supplied field replaces; the `clear`
    /// sentinel removes. The composed `filter` column is always rebuilt
    /// from the *effective* explicit filter and the *effective* convenience
    /// values (never decomposed from the stored composed tree), and the
    /// final subscription is validated so a carried-forward value can never
    /// bypass validation.
    fn resolve_subscription(
        guild_id: u64,
        channel_id: u64,
        input: &SovSubscribeInput,
        existing: Option<&SovSubscription>,
    ) -> Result<ResolvedSovSubscription, String> {
        let cleared = |field: SovClearableField| input.clear.contains(&field);
        let existing_options = existing.map(|found| &found.options);
        let legacy = existing_options.is_some_and(|options| !is_ticket15_provenance(options));

        // A legacy row's whole composed `filter` is its only record of the
        // filter, so it is treated as the explicit base. Composing a fresh
        // convenience leaf (e.g. `region_id:B`) onto that opaque tree would
        // AND it alongside a leaf of the same type already baked in
        // (`region(A)`), producing a subscription that silently matches
        // nothing. Require the caller to re-type the filter once in the same
        // command; the write then stores provenance and later partial
        // updates work.
        if legacy && input.filter.is_none() {
            let supplies_convenience = input.region_id.is_some()
                || input.defender_alliance_id.is_some()
                || input.max_jumps.is_some()
                || input.allow_frigate_holes.is_some();
            if supplies_convenience {
                return Err(
                    "this subscription predates provenance tracking; re-type its filter in the same command (add filter:...) when changing a convenience option, after which partial updates work"
                        .to_string(),
                );
            }
        }

        // Stored (provenance) values to carry forward when omitted. A
        // legacy row carries only its whole composed `filter` as the
        // explicit base; a ticket-15 row carries each recorded provenance
        // key independently.
        let stored_explicit: Option<SovFilter> = match existing {
            Some(found) if legacy => Some(found.filter.clone()),
            Some(found) => found
                .options
                .get(SOV_EXPLICIT_FILTER_OPTION_KEY)
                .map(|value| serde_json::from_value::<SovFilter>(value.clone()))
                .transpose()
                .map_err(|error| format!("stored explicit filter is corrupt: {error}"))?,
            None => None,
        };
        let stored_region = stored_i64(existing_options, legacy, SOV_REGION_ID_OPTION_KEY);
        let stored_defender = stored_i64(
            existing_options,
            legacy,
            SOV_DEFENDER_ALLIANCE_ID_OPTION_KEY,
        );
        let stored_max_jumps = stored_i64(existing_options, legacy, SOV_MAX_JUMPS_OPTION_KEY);
        let stored_allow_frigate_holes =
            stored_bool(existing_options, legacy, SOV_ALLOW_FRIGATE_HOLES_OPTION_KEY);

        // --- Resolve each top-level field to its effective value. ---
        // The explicit filter is a top-level optional input but is not
        // clearable (a subscription always needs a filter or a convenience
        // leaf); omitting it carries the stored explicit filter forward.
        let (explicit, filter_prov) = match &input.filter {
            Some(raw) => (
                Some(
                    serde_json::from_str::<SovFilter>(raw)
                        .map_err(|error| format!("invalid filter JSON: {error}"))?,
                ),
                FieldProvenance::Replaced,
            ),
            None => match stored_explicit {
                Some(filter) => (Some(filter), FieldProvenance::Preserved),
                None => (None, FieldProvenance::Untouched),
            },
        };

        let (role_id, role_prov) = resolve_field(
            "role",
            input.role_id,
            cleared(SovClearableField::Role),
            existing.and_then(|found| found.role_id),
        )?;
        let (ping_user_id, user_prov) = resolve_field(
            "user",
            input.ping_user_id,
            cleared(SovClearableField::User),
            existing.and_then(|found| found.ping_user_id),
        )?;
        let (region_id, region_prov) = resolve_field(
            "region_id",
            input.region_id,
            cleared(SovClearableField::RegionId),
            stored_region,
        )?;
        let (defender_alliance_id, defender_prov) = resolve_field(
            "defender_alliance_id",
            input.defender_alliance_id,
            cleared(SovClearableField::DefenderAllianceId),
            stored_defender,
        )?;
        let (max_jumps, max_jumps_prov) = resolve_field(
            "max_jumps",
            input.max_jumps,
            cleared(SovClearableField::MaxJumps),
            stored_max_jumps,
        )?;
        let (mut allow_frigate_holes, mut afh_prov) = resolve_field(
            "allow_frigate_holes",
            input.allow_frigate_holes,
            cleared(SovClearableField::AllowFrigateHoles),
            stored_allow_frigate_holes,
        )?;
        // A carried-forward `allow_frigate_holes` is meaningless once
        // `max_jumps` is no longer in effect (e.g. the caller cleared it);
        // drop it silently rather than rejecting the re-subscribe. A value
        // *supplied this invocation* without `max_jumps` is still the
        // existing user error, surfaced by `compose_filter`.
        if max_jumps.is_none() && afh_prov == FieldProvenance::Preserved {
            allow_frigate_holes = None;
            afh_prov = FieldProvenance::Untouched;
        }

        let filter = compose_filter(
            explicit.clone(),
            region_id,
            defender_alliance_id,
            max_jumps,
            allow_frigate_holes,
        )?;

        // --- Build the options document. ---
        // Behavioral keys (tminus/tz) keep their per-key overlay merge; the
        // provenance keys are recomputed from the effective values so a
        // cleared field's key is removed rather than carried forward.
        let requested = behavioral_options(
            input.tminus_marks.as_deref(),
            input.tz_window.as_deref(),
            input.tz_shift_enabled,
        )?;
        let mut options = Self::merge_options(existing_options, requested);
        // Persist the explicit-filter provenance only when this invocation
        // actually supplied a filter (`Replaced`) or the row already carried
        // ticket-15 provenance. A legacy row edited without a re-typed filter
        // (a role- or behavioural-only change) must stay legacy: writing its
        // opaque composed tree into `explicit_filter` here would upgrade the
        // row to ticket-15 status with no convenience provenance, letting a
        // later `region_id:B` bypass the legacy guard above and compose a
        // match-nothing `And`. Re-typing the filter is the one path that
        // records provenance; the composed `filter` column is still written
        // (unchanged) and the confirmation still reports the filter as
        // `Preserved`.
        if matches!(filter_prov, FieldProvenance::Replaced) || !legacy {
            set_or_remove(
                &mut options,
                SOV_EXPLICIT_FILTER_OPTION_KEY,
                explicit
                    .map(|filter| serde_json::to_value(&filter))
                    .transpose()
                    .map_err(|error| format!("cannot serialize explicit filter: {error}"))?,
            );
        }
        set_or_remove(
            &mut options,
            SOV_REGION_ID_OPTION_KEY,
            region_id.map(|value| serde_json::json!(value)),
        );
        set_or_remove(
            &mut options,
            SOV_DEFENDER_ALLIANCE_ID_OPTION_KEY,
            defender_alliance_id.map(|value| serde_json::json!(value)),
        );
        set_or_remove(
            &mut options,
            SOV_MAX_JUMPS_OPTION_KEY,
            max_jumps.map(|value| serde_json::json!(value)),
        );
        set_or_remove(
            &mut options,
            SOV_ALLOW_FRIGATE_HOLES_OPTION_KEY,
            allow_frigate_holes.map(|value| serde_json::json!(value)),
        );

        let subscription = SovSubscription {
            guild_id,
            channel_id,
            name: input.name.clone(),
            filter,
            options,
            role_id,
            ping_user_id,
        };
        subscription.validate()?;

        let mut replaced = Vec::new();
        let mut preserved = Vec::new();
        let mut cleared_fields = Vec::new();
        for (label, provenance) in [
            ("filter", filter_prov),
            ("role", role_prov),
            ("user", user_prov),
            ("region_id", region_prov),
            ("defender_alliance_id", defender_prov),
            ("max_jumps", max_jumps_prov),
            ("allow_frigate_holes", afh_prov),
        ] {
            match provenance {
                FieldProvenance::Replaced => replaced.push(label),
                FieldProvenance::Preserved => preserved.push(label),
                FieldProvenance::Cleared => cleared_fields.push(label),
                FieldProvenance::Untouched => {}
            }
        }
        Ok(ResolvedSovSubscription {
            subscription,
            replaced,
            preserved,
            cleared: cleared_fields,
        })
    }

    /// Merges a freshly-built subscription document's `options` against
    /// whatever was already persisted for the same subscription, so
    /// re-running `/sov_subscribe` with only some options preserves the
    /// keys this invocation did not touch instead of silently resetting
    /// them to their defaults (review finding 2 on ticket 03; extended in
    /// ticket 07 now that `tz_window`/`tz_shift_enabled` share the same
    /// document as `tminus_marks_minutes`).
    ///
    /// This is a per-key object merge, not all-or-nothing: start from the
    /// existing document (or `{}` when none / not an object) and overlay
    /// every key *present* in `requested`. Because
    /// `subscription_from_documents` only writes a key when its command
    /// option was supplied, a key present in `requested` is exactly a key
    /// the user set this invocation, and it always wins over the stored
    /// value -- including an explicit empty `tminus_marks_minutes: []`
    /// (a present key that disables marks) while any untouched `tz_window`
    /// / `tz_shift_enabled` survive, and vice versa. A brand-new
    /// subscription (`existing` is `None`) with no options supplied stays
    /// `{}`, so evaluation applies every default as usual.
    fn merge_options(
        existing: Option<&serde_json::Value>,
        requested: serde_json::Value,
    ) -> serde_json::Value {
        let mut merged = existing
            .and_then(|value| value.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(object) = requested.as_object() {
            for (key, value) in object {
                merged.insert(key.clone(), value.clone());
            }
        }
        serde_json::Value::Object(merged)
    }
}

/// Composes the persisted `filter` tree from an explicit filter and the
/// convenience-option values, ANDing each supplied convenience value onto
/// the explicit root as an extra leaf (the historical order: region,
/// defender, reachable). When `explicit` is `None` (a first-time subscribe
/// that supplied only convenience options), the leaves alone form the
/// filter; with neither an explicit filter nor any convenience leaf there
/// is nothing to build and the subscribe is rejected.
fn compose_filter(
    explicit: Option<SovFilter>,
    region_id: Option<i64>,
    defender_alliance_id: Option<i64>,
    max_jumps: Option<i64>,
    allow_frigate_holes: Option<bool>,
) -> Result<SovFilter, String> {
    let mut extra = Vec::new();
    if let Some(region_id) = region_id {
        extra.push(SovFilterNode::Condition(SovFilterCondition::Region(vec![
            region_id,
        ])));
    }
    if let Some(alliance_id) = defender_alliance_id {
        extra.push(SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![alliance_id],
            watchlist: false,
        }));
    }
    if let Some(max_jumps) = max_jumps {
        extra.push(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps,
            allow_frigate_holes: allow_frigate_holes.unwrap_or(false),
        }));
    } else if allow_frigate_holes.is_some() {
        return Err("allow_frigate_holes requires max_jumps to also be set".to_string());
    }
    match explicit {
        Some(mut filter) => {
            if !extra.is_empty() {
                extra.insert(0, filter.root);
                filter.root = SovFilterNode::And(extra);
            }
            Ok(filter)
        }
        None => {
            if extra.is_empty() {
                return Err(
                    "a sov subscription needs a filter or at least one convenience option"
                        .to_string(),
                );
            }
            let root = if extra.len() == 1 {
                extra.into_iter().next().expect("one leaf")
            } else {
                SovFilterNode::And(extra)
            };
            Ok(SovFilter { root })
        }
    }
}

/// Builds the behavioral (`tminus_marks`/`tz_window`/`tz_shift_enabled`)
/// slice of a subscription's `options` document from the raw command
/// options, writing a key only when its option was supplied (so an omitted
/// key stays absent and evaluation applies its default, and the per-key
/// [`SovSubscribeCommand::merge_options`] overlay carries an untouched key
/// forward on a partial re-subscribe). Ticket-15 provenance keys are added
/// separately by the caller.
fn behavioral_options(
    tminus_marks: Option<&str>,
    tz_window: Option<&str>,
    tz_shift_enabled: Option<bool>,
) -> Result<serde_json::Value, String> {
    let mut options = serde_json::json!({});
    if let Some(raw) = tminus_marks {
        let marks = parse_tminus_marks_minutes(raw)
            .map_err(|error| format!("invalid tminus_marks: {error}"))?;
        options[SOV_TMINUS_MARKS_OPTION_KEY] = serde_json::json!(marks);
    }
    if let Some(raw) = tz_window {
        // Validated eagerly (rather than deferred to evaluation time) so
        // `/sov_subscribe` rejects a malformed window immediately; the
        // normalized `HH:MM-HH:MM` text is stored, not the raw input, so
        // extra whitespace never round-trips into `options`.
        let window = parse_tz_window(raw).map_err(|error| format!("invalid tz_window: {error}"))?;
        options[SOV_TZ_WINDOW_OPTION_KEY] = serde_json::json!(window.to_option_string());
    }
    if let Some(enabled) = tz_shift_enabled {
        options[SOV_TZ_SHIFT_ENABLED_OPTION_KEY] = serde_json::json!(enabled);
    }
    Ok(options)
}

/// Resolves one clearable top-level field to its effective value and
/// provenance (ticket 15): supplying and clearing the same field in one
/// invocation is rejected; a bare `clear` removes it; a supplied value
/// replaces; omitting it carries the stored value forward. Clearing a field
/// that has no stored value (including on a first-time subscribe) is a
/// no-op reported as [`FieldProvenance::Untouched`], never `Cleared`.
fn resolve_field<T>(
    field: &str,
    supplied: Option<T>,
    cleared: bool,
    stored: Option<T>,
) -> Result<(Option<T>, FieldProvenance), String> {
    if supplied.is_some() && cleared {
        return Err(format!(
            "cannot both set and clear {field} in the same command"
        ));
    }
    if cleared {
        return match stored {
            Some(_) => Ok((None, FieldProvenance::Cleared)),
            None => Ok((None, FieldProvenance::Untouched)),
        };
    }
    if supplied.is_some() {
        return Ok((supplied, FieldProvenance::Replaced));
    }
    match stored {
        Some(value) => Ok((Some(value), FieldProvenance::Preserved)),
        None => Ok((None, FieldProvenance::Untouched)),
    }
}

/// Reads a carried-forward integer provenance value from a stored `options`
/// document. Always `None` for a legacy row (which has no provenance keys).
fn stored_i64(options: Option<&serde_json::Value>, legacy: bool, key: &str) -> Option<i64> {
    if legacy {
        return None;
    }
    options
        .and_then(|options| options.get(key))
        .and_then(|v| v.as_i64())
}

/// Reads a carried-forward boolean provenance value from a stored `options`
/// document. Always `None` for a legacy row.
fn stored_bool(options: Option<&serde_json::Value>, legacy: bool, key: &str) -> Option<bool> {
    if legacy {
        return None;
    }
    options
        .and_then(|options| options.get(key))
        .and_then(|v| v.as_bool())
}

/// Sets an `options` object key to `value` when `Some`, or removes it when
/// `None`. Used for the ticket-15 provenance keys so a cleared field's key
/// is deleted rather than left behind by the per-key overlay merge.
fn set_or_remove(options: &mut serde_json::Value, key: &str, value: Option<serde_json::Value>) {
    if let Some(object) = options.as_object_mut() {
        match value {
            Some(value) => {
                object.insert(key.to_string(), value);
            }
            None => {
                object.remove(key);
            }
        }
    }
}

/// Renders the ephemeral confirmation for a resolved re-subscribe (ticket
/// 15), appending only the non-empty replaced/preserved/cleared clauses so
/// a first-time subscribe reads exactly as before.
fn confirmation_message(resolved: &ResolvedSovSubscription) -> String {
    let mut message = format!(
        "Sov subscription '{}' saved for this channel.",
        resolved.subscription.name
    );
    if !resolved.replaced.is_empty() {
        message.push_str(&format!(" Replaced: {}.", resolved.replaced.join(", ")));
    }
    if !resolved.preserved.is_empty() {
        message.push_str(&format!(" Preserved: {}.", resolved.preserved.join(", ")));
    }
    if !resolved.cleared.is_empty() {
        message.push_str(&format!(" Cleared: {}.", resolved.cleared.join(", ")));
    }
    message
}

#[async_trait]
impl Command for SovSubscribeCommand {
    fn name(&self) -> String {
        "sov_subscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sov_subscribe")
            .default_member_permissions(Permissions::MANAGE_GUILD)
            .dm_permission(false)
            .description("Create or replace a sovereignty campaign subscription for this channel.")
            .create_option(|option| {
                option
                    .name("name")
                    .description("Stable subscription name for this channel.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("filter")
                    .description(
                        "Sov filter JSON (condition/and/or/not). Omit on re-subscribe to keep the stored filter.",
                    )
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("region_id")
                    .description("Convenience: also require this region ID.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("defender_alliance_id")
                    .description("Convenience: also require this defending alliance ID.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("max_jumps")
                    .description("Convenience: also require reachable from home within this many jumps (1-11).")
                    .kind(CommandOptionType::Integer)
                    .min_int_value(1)
                    .max_int_value(SOV_REACHABLE_MAX_JUMPS)
            })
            .create_option(|option| {
                option
                    .name("allow_frigate_holes")
                    .description("With max_jumps: allow frigate-sized wormholes on the route.")
                    .kind(CommandOptionType::Boolean)
            })
            .create_option(|option| {
                option
                    .name("role")
                    .description("Role to ping on alerts from this subscription.")
                    .kind(CommandOptionType::Role)
            })
            .create_option(|option| {
                option
                    .name("user")
                    .description("User to ping directly on alerts from this subscription.")
                    .kind(CommandOptionType::User)
            })
            .create_option(|option| {
                option
                    .name("tminus_marks")
                    .description(
                        "Comma-separated T-minus marks, minutes (default 120,30; empty disables; capped by VulnerableWithin).",
                    )
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("tz_window")
                    .description(
                        "Timezone window HH:MM-HH:MM EVE (default 00:00-04:00; may cross midnight).",
                    )
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("tz_shift_enabled")
                    .description("Alert when a watched hub's vulnerability window enters tz_window.")
                    .kind(CommandOptionType::Boolean)
            })
            .create_option(|option| {
                option
                    .name("clear")
                    .description(
                        "Comma list to clear: role, user, region_id, defender_alliance_id, max_jumps, allow_frigate_holes.",
                    )
                    .kind(CommandOptionType::String)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        let store_handle = ctx.data.read().await.get::<SovStoreContainer>().cloned();
        let parsed = match command.guild_id {
            Some(guild_id) => Self::input_from_command(command)
                .map(|input| (guild_id.0, input))
                .map_err(|error| format!("Invalid sov subscription: {error}")),
            None => Err("Sov subscriptions can only be created in a server channel.".to_string()),
        };
        let channel_id = command.channel_id.0;
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let (guild_id, input) = match parsed {
                Ok(parsed) => parsed,
                Err(response) => return response,
            };
            let Some(store_handle) = store_handle else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            let Some(store) = available_sov_store(&store_handle).await else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            // Read the previously-stored subscription (if any) so omitted
            // top-level fields carry forward, the `clear` sentinel can
            // remove them, and behavioral options merge per key (ticket 15;
            // review finding 2 on ticket 03). A store *error* must not be
            // collapsed into "no existing subscription" -- that would compose
            // a first-time subscription and overwrite the stored one, dropping
            // the very fields this ticket carries forward (review finding 1).
            let lookup = match store.subscription(guild_id, channel_id, &input.name).await {
                Ok(Some(subscription)) => ExistingLookup::Present(subscription),
                Ok(None) => ExistingLookup::Absent,
                Err(error) => ExistingLookup::Unreadable(error.to_string()),
            };
            let resolved = match SovSubscribeCommand::resolve_from_lookup(
                guild_id, channel_id, &input, lookup,
            ) {
                Ok(resolved) => resolved,
                Err(ResolveAbort::Invalid(error)) => {
                    return format!("Invalid sov subscription: {error}")
                }
                Err(ResolveAbort::Unreadable) => {
                    return "Could not read the existing subscription; nothing was changed."
                        .to_string()
                }
            };
            match store.upsert_subscription(&resolved.subscription).await {
                Ok(()) => confirmation_message(&resolved),
                Err(error) => {
                    error!("cannot persist sov subscription: {error}");
                    "Sov subscription could not be saved.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to sov subscribe command: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILTER: &str = r#"{
        "root": {
            "and": [
                {"condition": {"vulnerable_within": {"hours": 12}}},
                {"condition": {"event_type": ["ihub_defense", "tcu_defense"]}}
            ]
        }
    }"#;

    #[test]
    fn sov_subscribe_documents_express_the_recursive_grammar() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "nullsec-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid recursive subscription document");
        assert_eq!(subscription.guild_id, 42);
        assert_eq!(subscription.channel_id, 77);
        assert_eq!(subscription.name, "nullsec-front");
        assert!(matches!(subscription.filter.root, SovFilterNode::And(_)));
    }

    #[test]
    fn sov_subscribe_composes_convenience_options_with_the_filter_root() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "region-and-defender",
                filter: FILTER,
                region_id: Some(10_000_060),
                defender_alliance_id: Some(99_000_001),
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: Some(555),
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid composed document");
        assert_eq!(subscription.role_id, Some(555));
        assert_eq!(subscription.options, serde_json::json!({}));
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert_eq!(nodes.len(), 3);
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Region(ref ids)) if ids == &vec![10_000_060]
                ));
                assert!(matches!(
                    nodes[2],
                    SovFilterNode::Condition(SovFilterCondition::Defender { ref alliance_ids, .. }) if alliance_ids == &vec![99_000_001]
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_composes_max_jumps_as_a_reachable_condition() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "reachable-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: Some(11),
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document with max_jumps");
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert_eq!(nodes.len(), 2);
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Reachable {
                        max_jumps: 11,
                        allow_frigate_holes: false,
                    })
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_composes_allow_frigate_holes_onto_the_reachable_condition() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "frigate-holes-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: Some(6),
                allow_frigate_holes: Some(true),
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document with max_jumps and allow_frigate_holes");
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Reachable {
                        max_jumps: 6,
                        allow_frigate_holes: true,
                    })
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_rejects_allow_frigate_holes_without_max_jumps() {
        assert!(SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "orphan-frigate-flag",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: Some(true),
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .is_err());
    }

    #[test]
    fn sov_subscribe_rejects_max_jumps_outside_one_through_eleven() {
        for max_jumps in [0, SOV_REACHABLE_MAX_JUMPS + 1] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-max-jumps",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: Some(max_jumps),
                    allow_frigate_holes: None,
                    role_id: None,
                    ping_user_id: None,
                    tminus_marks: None,
                    tz_window: None,
                    tz_shift_enabled: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_rejects_invalid_documents() {
        for (name, filter) in [
            ("", FILTER),
            ("capitals", r#"{"root":{"and":[]}}"#),
            ("capitals", "not json"),
            ("capitals", r#"{"root":{"condition":{"event_type":[]}}}"#),
        ] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name,
                    filter,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    ping_user_id: None,
                    tminus_marks: None,
                    tz_window: None,
                    tz_shift_enabled: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_stores_parsed_and_deduped_tminus_marks_in_options() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "custom-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: Some("120, 45, 120"),
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document with custom tminus marks");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tminus_marks_minutes": [120, 45]})
        );
    }

    #[test]
    fn sov_subscribe_empty_tminus_marks_stores_an_explicit_empty_list() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "no-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: Some(""),
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document disabling tminus marks");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tminus_marks_minutes": []})
        );
    }

    #[test]
    fn sov_subscribe_omitted_tminus_marks_leaves_options_without_the_key() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "default-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document omitting tminus marks");
        assert_eq!(subscription.options, serde_json::json!({}));
    }

    #[test]
    fn sov_subscribe_rejects_invalid_tminus_marks() {
        for raw in ["0", "-5", "not-a-number", "120,abc"] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-marks",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    ping_user_id: None,
                    tminus_marks: Some(raw),
                    tz_window: None,
                    tz_shift_enabled: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_exposes_bounded_options() {
        let mut command = CreateApplicationCommand::default();
        SovSubscribeCommand.register(&mut command);
        let options = command.0["options"]
            .as_array()
            .expect("sov subscribe options");
        assert!(options.len() <= 25);
        let role = options
            .iter()
            .find(|option| option["name"] == "role")
            .expect("configured role option");
        assert_eq!(role["type"], CommandOptionType::Role.num());
        // Discord rejects command registration if any option description
        // exceeds 100 characters.
        for option in options {
            let description = option["description"]
                .as_str()
                .expect("every option has a description");
            assert!(
                description.len() <= 100,
                "option '{}' description is {} chars: {description}",
                option["name"],
                description.len()
            );
        }
    }

    #[test]
    fn merge_options_preserves_existing_marks_when_the_command_omits_them() {
        // Review finding 2: re-running `/sov_subscribe` without
        // `tminus_marks` must not silently reset previously-configured
        // marks back to the default.
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), serde_json::json!({}));
        assert_eq!(merged, existing);
    }

    #[test]
    fn merge_options_uses_the_omitted_document_for_a_brand_new_subscription() {
        let merged = SovSubscribeCommand::merge_options(None, serde_json::json!({}));
        assert_eq!(merged, serde_json::json!({}));
    }

    #[test]
    fn merge_options_lets_explicit_new_marks_replace_whatever_was_stored() {
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tminus_marks_minutes": [120, 30]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested.clone());
        assert_eq!(merged, requested);
    }

    #[test]
    fn merge_options_lets_an_explicit_empty_list_disable_marks_even_when_marks_existed_before() {
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tminus_marks_minutes": []});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested.clone());
        assert_eq!(merged, requested);
    }

    #[test]
    fn merge_options_overlays_tz_shift_without_dropping_stored_tminus_marks() {
        // Fix round finding 1: `/sov_subscribe name:X tminus_marks:90`
        // then `/sov_subscribe name:X tz_shift_enabled:true` must keep the
        // custom marks -- a partial re-subscribe overlays one key rather
        // than replacing the whole document.
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tz_shift_enabled": true});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({"tminus_marks_minutes": [90], "tz_shift_enabled": true})
        );
    }

    #[test]
    fn merge_options_overlays_tminus_marks_without_dropping_stored_tz_keys() {
        // The reverse order of the finding-1 scenario: tz keys were set
        // first, then a later invocation supplies only `tminus_marks`.
        let existing = serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true});
        let requested = serde_json::json!({"tminus_marks_minutes": [90]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({
                "tz_window": "06:00-10:00",
                "tz_shift_enabled": true,
                "tminus_marks_minutes": [90],
            })
        );
    }

    #[test]
    fn merge_options_lets_an_explicit_empty_tminus_disable_while_tz_keys_survive() {
        // An explicit empty list is a present key (disables marks) yet the
        // untouched tz keys must still survive the overlay.
        let existing = serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true});
        let requested = serde_json::json!({"tminus_marks_minutes": []});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({
                "tz_window": "06:00-10:00",
                "tz_shift_enabled": true,
                "tminus_marks_minutes": [],
            })
        );
    }

    // --- tz_window / tz_shift_enabled (ticket 07) ---

    #[test]
    fn sov_subscribe_stores_a_normalized_tz_window_and_tz_shift_enabled_in_options() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "tz-shift-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: Some(" 06:00-10:00 "),
                tz_shift_enabled: Some(true),
            },
        )
        .expect("valid document with tz_window and tz_shift_enabled");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true})
        );
    }

    #[test]
    fn sov_subscribe_omitted_tz_options_leave_options_without_those_keys() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "default-tz",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document omitting tz options");
        assert_eq!(subscription.options, serde_json::json!({}));
    }

    #[test]
    fn sov_subscribe_rejects_a_malformed_tz_window() {
        for raw in ["not-a-window", "24:00-01:00", "06:00-06:00"] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-tz-window",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    ping_user_id: None,
                    tminus_marks: None,
                    tz_window: Some(raw),
                    tz_shift_enabled: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_tz_window_option_and_tminus_marks_compose_in_the_same_options_document() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "everything-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                ping_user_id: None,
                tminus_marks: Some("120"),
                tz_window: Some("22:00-02:00"),
                tz_shift_enabled: Some(true),
            },
        )
        .expect("valid document combining tminus_marks and tz options");
        assert_eq!(
            subscription.options,
            serde_json::json!({
                "tminus_marks_minutes": [120],
                "tz_window": "22:00-02:00",
                "tz_shift_enabled": true,
            })
        );
    }

    // --- Carry-forward of top-level fields on re-subscribe (ticket 15) ---

    const F2: &str = r#"{"root": {"condition": {"system": [30004737]}}}"#;

    fn base_input(name: &str) -> SovSubscribeInput {
        SovSubscribeInput {
            name: name.to_string(),
            filter: None,
            region_id: None,
            defender_alliance_id: None,
            max_jumps: None,
            allow_frigate_holes: None,
            role_id: None,
            ping_user_id: None,
            tminus_marks: None,
            tz_window: None,
            tz_shift_enabled: None,
            clear: Vec::new(),
        }
    }

    fn resolve(
        input: &SovSubscribeInput,
        existing: Option<&SovSubscription>,
    ) -> Result<ResolvedSovSubscription, String> {
        SovSubscribeCommand::resolve_subscription(42, 77, input, existing)
    }

    /// A first-time subscribe with an explicit filter and convenience
    /// options, used as the stored fixture other carry-forward tests
    /// re-subscribe against.
    fn stored_with_region_defender_max_jumps() -> SovSubscription {
        let mut input = base_input("front");
        input.filter = Some(FILTER.to_string());
        input.region_id = Some(10_000_060);
        input.defender_alliance_id = Some(99_000_001);
        input.max_jumps = Some(6);
        input.role_id = Some(555);
        resolve(&input, None)
            .expect("valid first-time subscribe")
            .subscription
    }

    #[test]
    fn first_time_subscribe_stores_the_explicit_filter_as_provenance() {
        let mut input = base_input("front");
        input.filter = Some(FILTER.to_string());
        let resolved = resolve(&input, None).expect("valid first-time subscribe");
        assert_eq!(
            resolved.subscription.options[SOV_EXPLICIT_FILTER_OPTION_KEY],
            serde_json::from_str::<serde_json::Value>(FILTER).unwrap()
        );
        assert_eq!(resolved.replaced, vec!["filter"]);
        assert!(resolved.preserved.is_empty());
        assert!(resolved.cleared.is_empty());
    }

    #[test]
    fn first_time_subscribe_without_a_filter_or_convenience_is_rejected() {
        assert!(resolve(&base_input("front"), None).is_err());
    }

    #[test]
    fn first_time_subscribe_with_only_convenience_composes_from_leaves_alone() {
        let mut input = base_input("front");
        input.region_id = Some(10_000_060);
        let resolved = resolve(&input, None).expect("convenience-only subscribe");
        assert!(matches!(
            resolved.subscription.filter.root,
            SovFilterNode::Condition(SovFilterCondition::Region(ref ids)) if ids == &vec![10_000_060]
        ));
        assert!(resolved
            .subscription
            .options
            .get(SOV_EXPLICIT_FILTER_OPTION_KEY)
            .is_none());
    }

    #[test]
    fn re_subscribe_preserves_role_when_only_the_filter_changes() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.filter = Some(F2.to_string());
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe");
        assert_eq!(resolved.subscription.role_id, Some(555));
        assert!(resolved.replaced.contains(&"filter"));
        assert!(resolved.preserved.contains(&"role"));
    }

    #[test]
    fn clear_role_removes_it() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.clear = vec![SovClearableField::Role];
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe clearing role");
        assert_eq!(resolved.subscription.role_id, None);
        assert!(resolved.cleared.contains(&"role"));
    }

    #[test]
    fn explicit_new_role_replaces_the_stored_one() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.role_id = Some(777);
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe");
        assert_eq!(resolved.subscription.role_id, Some(777));
        assert!(resolved.replaced.contains(&"role"));
    }

    #[test]
    fn omitted_convenience_and_filter_recompose_an_identical_tree() {
        let stored = stored_with_region_defender_max_jumps();
        // Re-subscribe touching only a behavioral option; every top-level
        // field is carried forward and the composed filter is rebuilt
        // identically (so it still matches the same campaigns).
        let mut input = base_input("front");
        input.tz_shift_enabled = Some(true);
        let resolved = resolve(&input, Some(&stored)).expect("valid partial re-subscribe");
        assert_eq!(
            resolved.subscription.filter, stored.filter,
            "the recomposed filter must be identical to the stored one"
        );
        assert_eq!(resolved.subscription.role_id, Some(555));
        assert_eq!(
            resolved.subscription.options[SOV_REGION_ID_OPTION_KEY],
            serde_json::json!(10_000_060)
        );
        assert_eq!(
            resolved.subscription.options[SOV_MAX_JUMPS_OPTION_KEY],
            serde_json::json!(6)
        );
        assert!(resolved.preserved.contains(&"filter"));
        assert!(resolved.preserved.contains(&"region_id"));
        assert!(resolved.preserved.contains(&"max_jumps"));
    }

    #[test]
    fn new_filter_with_carried_convenience_composes_the_carried_leaves_onto_the_new_filter() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.filter = Some(F2.to_string());
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe with new filter");
        // The new explicit filter replaces the old one...
        assert_eq!(
            resolved.subscription.options[SOV_EXPLICIT_FILTER_OPTION_KEY],
            serde_json::from_str::<serde_json::Value>(F2).unwrap()
        );
        // ...and the carried convenience leaves compose onto it.
        match resolved.subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert!(matches!(
                    nodes[0],
                    SovFilterNode::Condition(SovFilterCondition::System(ref ids)) if ids == &vec![30_004_737]
                ));
                assert!(nodes.iter().any(|node| matches!(
                    node,
                    SovFilterNode::Condition(SovFilterCondition::Reachable { max_jumps: 6, .. })
                )));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn re_subscribe_omitting_the_filter_keeps_the_old_explicit_filter() {
        let mut first = base_input("front");
        first.filter = Some(FILTER.to_string());
        let stored = resolve(&first, None).expect("first subscribe").subscription;

        // Re-subscribe changing only a convenience option, no filter.
        let mut input = base_input("front");
        input.max_jumps = Some(8);
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe");
        assert_eq!(
            resolved.subscription.options[SOV_EXPLICIT_FILTER_OPTION_KEY],
            serde_json::from_str::<serde_json::Value>(FILTER).unwrap()
        );
        assert!(resolved.preserved.contains(&"filter"));
        assert!(resolved.replaced.contains(&"max_jumps"));
    }

    #[test]
    fn clearing_a_convenience_field_removes_its_leaf_from_the_composed_filter() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.clear = vec![SovClearableField::RegionId];
        let resolved = resolve(&input, Some(&stored)).expect("valid re-subscribe clearing region");
        assert!(resolved
            .subscription
            .options
            .get(SOV_REGION_ID_OPTION_KEY)
            .is_none());
        assert!(!filter_contains_region(&resolved.subscription.filter.root));
        assert!(resolved.cleared.contains(&"region_id"));
    }

    fn filter_contains_region(node: &SovFilterNode) -> bool {
        match node {
            SovFilterNode::Condition(SovFilterCondition::Region(_)) => true,
            SovFilterNode::Condition(_) => false,
            SovFilterNode::And(nodes) | SovFilterNode::Or(nodes) => {
                nodes.iter().any(filter_contains_region)
            }
            SovFilterNode::Not(inner) => filter_contains_region(inner),
        }
    }

    #[test]
    fn supplying_and_clearing_the_same_field_is_rejected() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.region_id = Some(10_000_002);
        input.clear = vec![SovClearableField::RegionId];
        assert!(resolve(&input, Some(&stored)).is_err());
    }

    #[test]
    fn clearing_max_jumps_drops_a_carried_allow_frigate_holes() {
        // Store a subscription whose reachable leaf allows frigate holes,
        // then clear max_jumps: the now-meaningless allow_frigate_holes is
        // dropped rather than rejected.
        let mut first = base_input("front");
        first.filter = Some(FILTER.to_string());
        first.max_jumps = Some(6);
        first.allow_frigate_holes = Some(true);
        let stored = resolve(&first, None).expect("first subscribe").subscription;

        let mut input = base_input("front");
        input.clear = vec![SovClearableField::MaxJumps];
        let resolved = resolve(&input, Some(&stored)).expect("clearing max_jumps must succeed");
        assert!(resolved
            .subscription
            .options
            .get(SOV_MAX_JUMPS_OPTION_KEY)
            .is_none());
        assert!(resolved
            .subscription
            .options
            .get(SOV_ALLOW_FRIGATE_HOLES_OPTION_KEY)
            .is_none());
    }

    #[test]
    fn validation_rejects_a_carried_forward_out_of_range_value() {
        // A max_jumps provenance value stored some other way (out of the
        // command's 1..=11 bound) must be caught when it is carried forward
        // and recomposed, not silently persisted.
        let existing = SovSubscription {
            guild_id: 42,
            channel_id: 77,
            name: "front".to_string(),
            filter: serde_json::from_str::<SovFilter>(FILTER).unwrap(),
            options: serde_json::json!({
                SOV_EXPLICIT_FILTER_OPTION_KEY:
                    serde_json::from_str::<serde_json::Value>(FILTER).unwrap(),
                SOV_MAX_JUMPS_OPTION_KEY: 99,
            }),
            role_id: None,
            ping_user_id: None,
        };
        let mut input = base_input("front");
        input.tz_shift_enabled = Some(true);
        assert!(resolve(&input, Some(&existing)).is_err());
    }

    #[test]
    fn a_legacy_subscription_carries_its_composed_filter_forward() {
        // A row stored before ticket 15 has no provenance keys; its whole
        // composed `filter` column is treated as the explicit base so a
        // carried-forward re-subscribe reproduces it exactly.
        let legacy_filter = serde_json::from_str::<SovFilter>(FILTER).unwrap();
        let existing = SovSubscription {
            guild_id: 42,
            channel_id: 77,
            name: "front".to_string(),
            filter: legacy_filter.clone(),
            options: serde_json::json!({}),
            role_id: Some(321),
            ping_user_id: None,
        };
        let mut input = base_input("front");
        input.tz_shift_enabled = Some(true);
        let resolved = resolve(&input, Some(&existing)).expect("legacy carry-forward");
        assert_eq!(resolved.subscription.filter, legacy_filter);
        assert_eq!(resolved.subscription.role_id, Some(321));
        assert!(resolved.preserved.contains(&"filter"));
    }

    #[test]
    fn parse_clear_fields_rejects_an_unknown_name_and_dedupes() {
        assert!(parse_clear_fields("role,not_a_field").is_err());
        assert_eq!(
            parse_clear_fields("role, role, max_jumps").unwrap(),
            vec![SovClearableField::Role, SovClearableField::MaxJumps]
        );
        assert!(parse_clear_fields("").unwrap().is_empty());
    }

    // --- Review finding 1: an unreadable store read must abort. ---

    #[test]
    fn resolve_from_lookup_aborts_on_an_unreadable_lookup() {
        // A transient DB error (or an undeserializable row) must never be
        // collapsed into a first-time subscribe: it aborts without yielding
        // a subscription to upsert.
        let mut input = base_input("front");
        input.filter = Some(FILTER.to_string());
        let outcome = SovSubscribeCommand::resolve_from_lookup(
            42,
            77,
            &input,
            ExistingLookup::Unreadable("connection reset".to_string()),
        );
        assert!(matches!(outcome, Err(ResolveAbort::Unreadable)));
    }

    #[test]
    fn resolve_from_lookup_composes_a_first_time_subscribe_only_on_a_confirmed_absence() {
        let mut input = base_input("front");
        input.filter = Some(FILTER.to_string());
        let resolved =
            SovSubscribeCommand::resolve_from_lookup(42, 77, &input, ExistingLookup::Absent)
                .expect("a confirmed absence composes a first-time subscribe");
        assert_eq!(resolved.replaced, vec!["filter"]);
    }

    #[test]
    fn resolve_from_lookup_carries_forward_against_a_present_subscription() {
        let stored = stored_with_region_defender_max_jumps();
        let mut input = base_input("front");
        input.filter = Some(F2.to_string());
        let resolved = SovSubscribeCommand::resolve_from_lookup(
            42,
            77,
            &input,
            ExistingLookup::Present(stored),
        )
        .expect("a present subscription carries omitted fields forward");
        assert_eq!(resolved.subscription.role_id, Some(555));
    }

    // --- Review finding 2: legacy rows and convenience options. ---

    /// A row stored before ticket 15: no provenance keys, its whole composed
    /// `filter` is the only record of its filter.
    fn legacy_stored() -> SovSubscription {
        SovSubscription {
            guild_id: 42,
            channel_id: 77,
            name: "front".to_string(),
            filter: serde_json::from_str::<SovFilter>(FILTER).unwrap(),
            options: serde_json::json!({}),
            role_id: Some(321),
            ping_user_id: None,
        }
    }

    #[test]
    fn legacy_row_rejects_a_convenience_option_without_a_filter() {
        // region_id:B over a legacy tree that already bakes in region(A)
        // would compose a contradictory And that silently matches nothing;
        // reject it and leave the row unchanged.
        let mut input = base_input("front");
        input.region_id = Some(10_000_002);
        assert!(resolve(&input, Some(&legacy_stored())).is_err());
    }

    #[test]
    fn legacy_row_with_convenience_and_a_filter_composes_without_a_duplicate_leaf() {
        // Supplying the filter re-types provenance, so the convenience leaf
        // composes onto the freshly-typed filter (not the opaque old tree)
        // and provenance is stored for future partial updates.
        let mut input = base_input("front");
        input.filter = Some(F2.to_string());
        input.region_id = Some(10_000_002);
        let resolved = resolve(&input, Some(&legacy_stored())).expect("re-typed legacy row");
        assert_eq!(
            resolved.subscription.options[SOV_EXPLICIT_FILTER_OPTION_KEY],
            serde_json::from_str::<serde_json::Value>(F2).unwrap()
        );
        assert_eq!(
            resolved.subscription.options[SOV_REGION_ID_OPTION_KEY],
            serde_json::json!(10_000_002)
        );
        // Exactly one region leaf: F2 (a system filter) ANDed with region(B).
        match resolved.subscription.filter.root {
            SovFilterNode::And(ref nodes) => {
                let regions = nodes
                    .iter()
                    .filter(|node| filter_contains_region(node))
                    .count();
                assert_eq!(regions, 1);
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn legacy_row_with_only_a_role_change_carries_the_filter_forward() {
        // Role is not a convenience leaf, so a legacy row can change it
        // without re-typing the filter: today's plain carry-forward.
        let mut input = base_input("front");
        input.role_id = Some(999);
        let resolved = resolve(&input, Some(&legacy_stored())).expect("legacy role change");
        assert_eq!(resolved.subscription.role_id, Some(999));
        assert_eq!(
            resolved.subscription.filter,
            serde_json::from_str::<SovFilter>(FILTER).unwrap()
        );
        assert!(resolved.preserved.contains(&"filter"));
        // The row must stay legacy: a role-only edit must not write the
        // opaque composed tree into explicit_filter and upgrade the row.
        assert!(!is_ticket15_provenance(&resolved.subscription.options));
    }

    #[test]
    fn a_legacy_row_role_only_edit_stays_legacy_and_keeps_guarding_convenience() {
        // A role-only edit to a legacy row must leave it legacy: if its
        // opaque composed tree leaked into explicit_filter the row would
        // count as ticket-15 with no convenience provenance, letting a later
        // region_id:B bypass the guard and compose a match-nothing And.
        let mut role_edit = base_input("front");
        role_edit.role_id = Some(999);
        let after_role = resolve(&role_edit, Some(&legacy_stored()))
            .expect("legacy role change")
            .subscription;
        assert!(!is_ticket15_provenance(&after_role.options));
        assert_eq!(
            after_role.filter,
            serde_json::from_str::<SovFilter>(FILTER).unwrap()
        );

        // Still legacy, so a convenience-only edit is still rejected by the
        // guard rather than composing a contradictory tree.
        let mut convenience = base_input("front");
        convenience.region_id = Some(10_000_002);
        assert!(resolve(&convenience, Some(&after_role)).is_err());
    }

    #[test]
    fn a_legacy_row_behavioural_only_edit_stays_legacy_and_merges_the_key() {
        // A behavioural-only edit (tminus_marks) must not upgrade a legacy
        // row either; the behavioural key merges as before and the composed
        // filter column is unchanged.
        let mut input = base_input("front");
        input.tminus_marks = Some("120".to_string());
        let resolved = resolve(&input, Some(&legacy_stored()))
            .expect("legacy behavioural change")
            .subscription;
        assert!(!is_ticket15_provenance(&resolved.options));
        assert_eq!(
            resolved.options[SOV_TMINUS_MARKS_OPTION_KEY],
            serde_json::json!([120])
        );
        assert_eq!(
            resolved.filter,
            serde_json::from_str::<SovFilter>(FILTER).unwrap()
        );
    }

    #[test]
    fn a_legacy_row_upgrades_only_once_the_filter_is_retyped_then_updates_partially() {
        // Re-typing the filter alongside region_id:B records provenance and
        // upgrades the row; a subsequent max_jumps-only edit then composes
        // cleanly (one region leaf, one reachable leaf).
        let mut retype = base_input("front");
        retype.filter = Some(F2.to_string());
        retype.region_id = Some(10_000_002);
        let upgraded = resolve(&retype, Some(&legacy_stored()))
            .expect("re-typed legacy row")
            .subscription;
        assert!(is_ticket15_provenance(&upgraded.options));

        let mut partial = base_input("front");
        partial.max_jumps = Some(5);
        let resolved = resolve(&partial, Some(&upgraded))
            .expect("partial update on the upgraded row")
            .subscription;
        match resolved.filter.root {
            SovFilterNode::And(ref nodes) => {
                let regions = nodes
                    .iter()
                    .filter(|node| filter_contains_region(node))
                    .count();
                assert_eq!(regions, 1, "exactly one region leaf");
                let reachable = nodes
                    .iter()
                    .filter(|node| {
                        matches!(
                            node,
                            SovFilterNode::Condition(SovFilterCondition::Reachable { .. })
                        )
                    })
                    .count();
                assert_eq!(reachable, 1, "exactly one reachable leaf");
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    // --- Review finding 3: clearing a field with no stored value. ---

    #[test]
    fn clearing_a_field_with_no_stored_value_is_not_reported_as_cleared() {
        // The stored subscription has no role; clear:role is a no-op, not a
        // "Cleared: role".
        let mut first = base_input("front");
        first.filter = Some(FILTER.to_string());
        let stored = resolve(&first, None).expect("first subscribe").subscription;
        assert_eq!(stored.role_id, None);

        let mut input = base_input("front");
        input.clear = vec![SovClearableField::Role];
        let resolved = resolve(&input, Some(&stored)).expect("clearing an absent field is a no-op");
        assert_eq!(resolved.subscription.role_id, None);
        assert!(!resolved.cleared.contains(&"role"));
    }

    #[test]
    fn first_time_subscribe_with_clear_role_is_not_an_error_and_not_cleared() {
        let mut input = base_input("front");
        input.filter = Some(FILTER.to_string());
        input.clear = vec![SovClearableField::Role];
        let resolved = resolve(&input, None).expect("clear on a first-time subscribe is a no-op");
        assert_eq!(resolved.subscription.role_id, None);
        assert!(!resolved.cleared.contains(&"role"));
    }
}
