use killbot_rust::write_policy::{PolicyLoadError, WritePolicy};
use std::fs;

const USER_TWO_POLICY: &str = r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["2"]}}}"#;

#[test]
fn default_policy_denies_every_actor() {
    assert!(!WritePolicy::default().allows(1, 2, &[3]));
}

#[test]
fn explicit_user_or_role_grants_apply_only_in_their_guild() {
    let policy = WritePolicy::from_json(
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["2"],"allowed_role_ids":["3"]},"4":{"allowed_user_ids":["5"]}}}"#,
    ).unwrap();
    assert!(policy.allows(1, 2, &[]));
    assert!(policy.allows(1, 9, &[8, 3]));
    assert!(!policy.allows(4, 2, &[3]));
    assert!(!policy.allows(1, 5, &[]));
    assert!(!policy.allows(1, 9, &[1, 8]));
    assert!(!policy.allows(7, 2, &[3]));
}

#[test]
fn every_id_position_requires_canonical_positive_decimal_u64_strings() {
    for id in [
        "0",
        "00",
        "01",
        "+2",
        "-2",
        " 2",
        "2 ",
        "2.0",
        "2e1",
        "",
        "*",
        "٢",
        "18446744073709551616",
    ] {
        for document in [
            serde_json::json!({"version":1,"guilds":{id:{}}}),
            serde_json::json!({"version":1,"guilds":{"1":{"allowed_user_ids":[id]}}}),
            serde_json::json!({"version":1,"guilds":{"1":{"allowed_role_ids":[id]}}}),
        ] {
            assert_eq!(
                WritePolicy::from_json(&document.to_string()).err(),
                Some(PolicyLoadError::InvalidSchema),
                "{document}"
            );
        }
    }
    let maximum = WritePolicy::from_json(r#"{"version":1,"guilds":{"18446744073709551615":{"allowed_user_ids":["18446744073709551615"],"allowed_role_ids":["18446744073709551614"]}}}"#).unwrap();
    assert!(maximum.allows(u64::MAX, u64::MAX, &[]));
    assert!(maximum.allows(u64::MAX, 2, &[u64::MAX - 1]));
}

#[test]
fn schema_rejects_unknown_duplicate_and_mistyped_configuration() {
    for document in [
        r#"{"version":1,"unknown":"POLICY_SENTINEL"}"#,
        r#"{"version":1,"guilds":{"1":{"unknown":true}}}"#,
        r#"{"version":1,"version":1}"#,
        r#"{"version":1,"guilds":{},"guilds":{}}"#,
        r#"{"version":1,"guilds":{"1":{},"1":{}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":[],"allowed_user_ids":["2"]}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_role_ids":[],"allowed_role_ids":["3"]}}}"#,
        r#"{"version":0}"#,
        r#"{"version":2}"#,
        r#"{"version":-1}"#,
        r#"{"version":"1"}"#,
        r#"{"version":1.0}"#,
        r#"{"version":true}"#,
        r#"{"version":null}"#,
        r#"{}"#,
        r#"[]"#,
        r#"[1,{}]"#,
        r#"{"version":1,"guilds":{"1":[["2"],["3"]]}}"#,
        r#"{"version":1,"guilds":null}"#,
        r#"{"version":1,"guilds":[]}"#,
        r#"{"version":1,"guilds":{"1":null}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":null}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_role_ids":"3"}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":[2]}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_role_ids":[3]}}}"#,
    ] {
        assert_eq!(
            WritePolicy::from_json(document).err(),
            Some(PolicyLoadError::InvalidSchema),
            "{document}"
        );
    }
    for document in ["", "{", r#"{"version":1,}"#, r#"{"version":1}{}"#] {
        assert_eq!(
            WritePolicy::from_json(document).err(),
            Some(PolicyLoadError::InvalidJson)
        );
    }
    for document in [
        r#"{"version":1}"#,
        r#"{"version":1,"guilds":{}}"#,
        r#"{"version":1,"guilds":{"1":{}}}"#,
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":[],"allowed_role_ids":[]}}}"#,
    ] {
        assert!(!WritePolicy::from_json(document).unwrap().allows(1, 2, &[3]));
    }
}

#[test]
fn granting_the_guilds_everyone_role_invalidates_the_whole_policy() {
    assert_eq!(WritePolicy::from_json(r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["2"],"allowed_role_ids":["3","1"]}}}"#).err(), Some(PolicyLoadError::InvalidSchema));
    let explicit_user =
        WritePolicy::from_json(r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["1"]}}}"#)
            .unwrap();
    assert!(explicit_user.allows(1, 1, &[]));
}

#[test]
fn file_loading_accepts_absolute_and_relative_paths_and_freezes_each_snapshot() {
    let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
    let temp = tempfile::tempdir_in(&cwd).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let policy_path = root.join("policy.json");
    let state = root.join("not-yet-created-state");
    fs::write(&policy_path, USER_TWO_POLICY).unwrap();
    let snapshot = WritePolicy::load(&policy_path, &state).unwrap();
    let relative = WritePolicy::load(
        policy_path.strip_prefix(&cwd).unwrap(),
        state.strip_prefix(&cwd).unwrap(),
    )
    .unwrap();
    assert!(snapshot.allows(1, 2, &[]));
    assert!(relative.allows(1, 2, &[]));
    fs::write(
        &policy_path,
        r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["5"]}}}"#,
    )
    .unwrap();
    assert!(snapshot.allows(1, 2, &[]));
    assert!(!snapshot.allows(1, 5, &[]));
    let restarted = WritePolicy::load(&policy_path, &state).unwrap();
    assert!(!restarted.allows(1, 2, &[]));
    assert!(restarted.allows(1, 5, &[]));
}

#[test]
fn writable_state_paths_are_rejected_after_normalization_even_before_creation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("config");
    fs::create_dir_all(state.join("nested")).unwrap();
    let inside = state.join("policy.json");
    fs::write(&inside, USER_TWO_POLICY).unwrap();
    for (policy, state_path) in [
        (inside.clone(), state.clone()),
        (state.join("nested/../policy.json"), state.clone()),
        (inside, state.join("nested/..")),
        (
            root.join("future-state/policy.json"),
            root.join("future-state"),
        ),
    ] {
        assert_eq!(
            WritePolicy::load(&policy, &state_path).err(),
            Some(PolicyLoadError::WritableStatePath)
        );
    }
    fs::write(root.join("policy.json"), USER_TWO_POLICY).unwrap();
    assert!(WritePolicy::load(&state.join("../policy.json"), &state)
        .unwrap()
        .allows(1, 2, &[]));
    fs::create_dir(root.join("config-backup")).unwrap();
    let sibling = root.join("config-backup/policy.json");
    fs::write(&sibling, USER_TWO_POLICY).unwrap();
    assert!(WritePolicy::load(&sibling, &state)
        .unwrap()
        .allows(1, 2, &[]));
}

#[cfg(unix)]
#[test]
fn policy_path_rejects_file_directory_dangling_and_normalized_away_symlinks() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    fs::create_dir(root.join("policy")).unwrap();
    fs::write(root.join("policy/policy.json"), USER_TWO_POLICY).unwrap();
    symlink(root.join("policy/policy.json"), root.join("file-link")).unwrap();
    symlink(root.join("policy"), root.join("dir-link")).unwrap();
    symlink(root.join("missing"), root.join("dangling")).unwrap();
    for path in [
        "file-link",
        "dir-link/policy.json",
        "dir-link/../policy/policy.json",
        "dangling",
        "dangling/../policy/policy.json",
    ] {
        assert_eq!(
            WritePolicy::load(&root.join(path), &root.join("config")).err(),
            Some(PolicyLoadError::SymlinkPath),
            "{path}"
        );
    }
}

#[cfg(unix)]
#[test]
fn writable_state_aliases_and_their_missing_descendants_cannot_hide_policy_paths() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let real_state = root.join("real-state");
    fs::create_dir(&real_state).unwrap();
    let state_alias = root.join("state-alias");
    symlink(&real_state, &state_alias).unwrap();
    let policy_path = real_state.join("policy.json");
    fs::write(&policy_path, USER_TWO_POLICY).unwrap();
    assert_eq!(
        WritePolicy::load(&policy_path, &state_alias).err(),
        Some(PolicyLoadError::WritableStatePath)
    );
    assert_eq!(
        WritePolicy::load(
            &real_state.join("future/policy.json"),
            &state_alias.join("future")
        )
        .err(),
        Some(PolicyLoadError::WritableStatePath)
    );
    let outside = root.join("outside.json");
    fs::write(&outside, USER_TWO_POLICY).unwrap();
    assert!(WritePolicy::load(&outside, &state_alias.join("future"))
        .unwrap()
        .allows(1, 2, &[]));
}

#[test]
fn file_failures_expose_only_bounded_reason_categories() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("config");
    let policy = root.join("POLICY_CONTENT_SENTINEL.json");
    let missing = WritePolicy::load(&policy, &state).err().unwrap();
    assert_eq!(missing, PolicyLoadError::MissingFile);
    assert_eq!(format!("{missing:?}"), "MissingFile");
    assert_eq!(missing.to_string(), "missing_file");
    assert!(std::error::Error::source(&missing).is_none());
    fs::write(&policy, r#"{"version":1,"POLICY_CONTENT_SENTINEL":true}"#).unwrap();
    let invalid = WritePolicy::load(&policy, &state).err().unwrap();
    assert_eq!(invalid, PolicyLoadError::InvalidSchema);
    assert_eq!(invalid.to_string(), "invalid_schema");
    fs::write(&policy, [0xff]).unwrap();
    assert_eq!(
        WritePolicy::load(&policy, &state).err(),
        Some(PolicyLoadError::InvalidJson)
    );
    assert_eq!(
        WritePolicy::load(&root, &state).err(),
        Some(PolicyLoadError::UnreadableFile)
    );
    assert_eq!(
        WritePolicy::load(std::path::Path::new(""), &state).err(),
        Some(PolicyLoadError::InvalidPath)
    );
    assert_eq!(
        WritePolicy::load(&policy, std::path::Path::new("")).err(),
        Some(PolicyLoadError::InvalidPath)
    );
    assert_eq!(
        WritePolicy::load(std::path::Path::new("\0"), &state).err(),
        Some(PolicyLoadError::InvalidPath)
    );
    assert_eq!(
        WritePolicy::load(&policy, &policy).err(),
        Some(PolicyLoadError::InvalidPath)
    );
}

#[cfg(unix)]
#[test]
fn unreadable_policy_file_is_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let policy = root.join("policy.json");
    fs::write(&policy, USER_TWO_POLICY).unwrap();
    let permissions = fs::metadata(&policy).unwrap().permissions();
    fs::set_permissions(&policy, fs::Permissions::from_mode(0)).unwrap();
    if fs::File::open(&policy).is_ok() {
        fs::set_permissions(&policy, permissions).unwrap();
        eprintln!("permission-denial case unavailable: this process can read mode-000 files");
        return;
    }
    let result = WritePolicy::load(&policy, &root.join("config")).err();
    fs::set_permissions(&policy, permissions).unwrap();
    assert_eq!(result, Some(PolicyLoadError::UnreadableFile));
}
