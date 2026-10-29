//! Every rule of the access model (contract K1).

use super::*;

fn set(names: &[&str]) -> Permissions {
    names.iter().map(|n| n.to_string()).collect()
}

fn map(json: &str) -> Map {
    Value::from_json_str(json).unwrap().to_map()
}

fn teams(items: &[(&str, TeamRole)]) -> Vec<(String, TeamRole)> {
    items.iter().map(|(n, r)| (n.to_string(), *r)).collect()
}

fn user(name: &str, permissions: &[&str], items: &[(&str, TeamRole)]) -> Principal {
    Principal::new(name, false, set(permissions), teams(items))
}

fn pipe(ck: &str, mk: &str, lk: Option<&str>) -> Pipe {
    Pipe::builder(ck, mk)
        .location(lk)
        .instance("sql:main")
        .build()
        .unwrap()
}

#[test]
fn permissions_match_the_static_scopes() {
    assert_eq!(
        static_config().scope_names(),
        ALL_PERMISSIONS
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
    );
    for p in ALL_PERMISSIONS {
        assert!(permission_description(p).is_some(), "{p}");
    }
    assert_eq!(all_permissions().len(), 26);
    assert_eq!(Permission::WEBTERM_USE, "webterm:use");
    assert_eq!(Permission::PIPES_REGISTER, "pipes:register");
    assert_eq!(Permission::PIPES_DEFINE_SQL, "pipes:define_sql");
}

#[test]
fn role_presets() {
    let viewer = Role::Viewer.permissions();
    let editor = Role::Editor.permissions();
    let operator = Role::Operator.permissions();
    // Each preset contains the one before it.
    assert!(viewer.is_subset(&editor));
    assert!(editor.is_subset(&operator));
    assert!(operator.is_subset(&Role::Admin.permissions()));
    assert_eq!(Role::Admin.permissions(), all_permissions());
    assert!(Role::Custom.permissions().is_empty());

    // Viewer: reads pipes, jobs, logs, plugins, connectors; its own account.
    for p in [
        "pipes:read",
        "jobs:read",
        "logs:read",
        "plugins:read",
        "connectors:read",
        "users:read",
        "users:write",
    ] {
        assert!(viewer.contains(p), "{p}");
    }
    for p in ["pipes:write", "sql:read", "actions:execute", "webterm:use"] {
        assert!(!viewer.contains(p), "{p}");
    }
    // Editor: registers, edits and syncs pipes (SQL included), starts jobs, reads SQL.
    for p in [
        "pipes:write",
        "pipes:register",
        "pipes:define_sql",
        "jobs:execute",
        "sql:read",
    ] {
        assert!(editor.contains(p), "{p}");
    }
    for p in [
        "actions:execute",
        "webterm:use",
        "jobs:write",
        "jobs:stop",
        "pipes:drop",
        "pipes:delete",
        "users:register",
    ] {
        assert!(!editor.contains(p), "{p}");
    }
    // Operator: actions, the web terminal, every job control.
    for p in [
        "actions:execute",
        "webterm:use",
        "jobs:write",
        "jobs:stop",
        "jobs:pause",
        "jobs:delete",
    ] {
        assert!(operator.contains(p), "{p}");
    }
    for p in [
        "users:register",
        "users:delete",
        "instance:chain",
        "pipes:drop",
    ] {
        assert!(!operator.contains(p), "{p}");
    }
    assert_eq!(Role::parse(" Editor "), Some(Role::Editor));
    assert_eq!(Role::parse("nope"), None);
    assert_eq!(Role::matching(&editor), Some(Role::Editor));
    assert_eq!(Role::matching(&all_permissions()), None);
}

#[test]
fn default_role_resolution() {
    assert_eq!(resolve_default_role(None, None), Role::Editor);
    assert_eq!(resolve_default_role(Some("viewer"), None), Role::Viewer);
    assert_eq!(
        resolve_default_role(Some("Operator"), Some(false)),
        Role::Operator
    );
    // v4's key set explicitly to true: actions for everyone, so `operator`.
    assert_eq!(resolve_default_role(None, Some(true)), Role::Operator);
    assert_eq!(resolve_default_role(None, Some(false)), Role::Editor);
    // An explicit default role wins over the v4 key.
    assert_eq!(
        resolve_default_role(Some("viewer"), Some(true)),
        Role::Viewer
    );
    // A value naming no default role gives the least permissions.
    assert_eq!(resolve_default_role(Some("admin"), None), Role::Viewer);
    assert_eq!(resolve_default_role(Some("editer"), None), Role::Viewer);
    assert_eq!(resolve_default_role(Some("  "), None), Role::Editor);
}

#[test]
fn effective_permissions_rules() {
    let none: Option<&[String]> = None;
    // Administrators hold everything.
    assert_eq!(
        effective_permissions(
            Some("admin"),
            Some(&["pipes:read".to_string()]),
            Role::Viewer
        ),
        all_permissions()
    );
    // No explicit scopes: the default role.
    assert_eq!(
        effective_permissions(None, none, Role::Viewer),
        Role::Viewer.permissions()
    );
    assert_eq!(
        effective_permissions(Some("user"), none, Role::Editor),
        Role::Editor.permissions()
    );
    // Explicit scopes as listed (a list naming a 5.0 permission is taken as written).
    let explicit = vec!["pipes:read".to_string(), "webterm:use".to_string()];
    assert_eq!(
        effective_permissions(None, Some(&explicit), Role::Operator),
        set(&["pipes:read", "webterm:use"])
    );
    // `*` is every permission.
    assert_eq!(
        effective_permissions(None, Some(&["*".to_string()]), Role::Viewer),
        all_permissions()
    );
    // An explicit empty list revokes every permission, regardless of the default role.
    let empty: Vec<String> = vec![];
    assert_eq!(
        effective_permissions(None, Some(&empty), Role::Operator),
        Permissions::new()
    );
}

#[test]
fn legacy_scope_lists_keep_what_they_allowed() {
    let v4 = ["pipes:read", "pipes:write", "actions:execute"];
    let expanded = expand_legacy_scopes(&v4);
    for p in [
        "pipes:read",
        "pipes:write",
        "pipes:register",
        "pipes:define_sql",
        "actions:execute",
        "webterm:use",
    ] {
        assert!(expanded.contains(p), "{p}");
    }
    // A 5.0 list is exact: `pipes:write` without `pipes:register` cannot register.
    let new = ["pipes:write", "webterm:use"];
    assert_eq!(expand_legacy_scopes(&new), set(&new));
    assert!(expand_legacy_scopes(&["pipes:read"]).len() == 1);
}

#[test]
fn stored_user_records() {
    // New permission grids are exact even when they name no new permissions. Old v4
    // scopes-only records retain their aliases; adding a role marks a current grid.
    for scope in [Permission::PIPES_WRITE, Permission::ACTIONS_EXECUTE] {
        let attrs = attributes_with_permissions(&Map::new(), &[scope]);
        assert_eq!(
            user_permissions(None, Some(&attrs), Role::Operator),
            set(&[scope])
        );
        assert_eq!(user_role(None, Some(&attrs), Role::Operator), Role::Custom);
    }
    for json in [
        r#"{"scopes": []}"#,
        r#"{"role": "operator", "scopes": "[]"}"#,
    ] {
        let attrs = map(json);
        assert!(user_permissions(None, Some(&attrs), Role::Operator).is_empty());
        assert_eq!(user_role(None, Some(&attrs), Role::Operator), Role::Custom);
    }
    // No attributes: the default role.
    assert_eq!(
        user_permissions(None, None, Role::Editor),
        Role::Editor.permissions()
    );
    assert_eq!(user_role(None, None, Role::Editor), Role::Editor);
    // A stored role.
    let attrs = map(r#"{"role": "viewer"}"#);
    assert_eq!(user_role(None, Some(&attrs), Role::Operator), Role::Viewer);
    assert_eq!(
        user_permissions(None, Some(&attrs), Role::Operator),
        Role::Viewer.permissions()
    );
    // A stored `admin` role is not the admin type.
    let attrs = map(r#"{"role": "admin"}"#);
    assert_eq!(user_role(None, Some(&attrs), Role::Viewer), Role::Viewer);
    assert_eq!(user_role(Some("admin"), None, Role::Viewer), Role::Admin);
    // Explicit scopes win, and name their preset when they equal one.
    let attrs = attributes_with_permissions(
        &Map::new(),
        &ordered_permissions(&Role::Operator.permissions()),
    );
    assert_eq!(user_role(None, Some(&attrs), Role::Viewer), Role::Operator);
    let attrs = map(r#"{"scopes": ["pipes:read", "sql:read"]}"#);
    assert_eq!(user_role(None, Some(&attrs), Role::Viewer), Role::Custom);
    assert_eq!(
        user_permissions(None, Some(&attrs), Role::Operator),
        set(&["pipes:read", "sql:read"])
    );
    // Scopes stored as JSON text (an instance that returns the column as text).
    let attrs = map(r#"{"scopes": "[\"pipes:read\"]"}"#);
    assert_eq!(
        user_permissions(None, Some(&attrs), Role::Operator),
        set(&["pipes:read"])
    );
    // Choosing a preset drops explicit scopes.
    let with_role =
        attributes_with_role(&map(r#"{"scopes": ["pipes:read"], "x": 1}"#), Role::Editor);
    assert_eq!(with_role.get("role"), Some(&Value::Str("editor".into())));
    assert!(!with_role.contains_key("scopes"));
    assert_eq!(with_role.get("x"), Some(&Value::Int(1)));
    let custom =
        attributes_with_permissions(&Map::new(), &["sql:read", "pipes:read", "zzz:plugin"]);
    assert_eq!(
        custom.get("scopes"),
        Some(&Value::from_json_str(r#"["pipes:read", "sql:read", "zzz:plugin"]"#).unwrap())
    );
    assert_eq!(custom.get("role"), Some(&Value::Str("custom".into())));
}

#[test]
fn tokens_never_exceed_their_user() {
    let user = Role::Editor.permissions();
    // A token with every scope holds only its user's.
    assert_eq!(token_permissions(&ALL_PERMISSIONS, &user), user);
    assert_eq!(token_permissions(&["*"], &user), user);
    // A narrower token stays narrow.
    assert_eq!(
        token_permissions(&["pipes:read"], &user),
        set(&["pipes:read"])
    );
    // A pre-5.0 token with `pipes:write` may still register for an Editor.
    assert!(token_permissions(&["pipes:write"], &user).contains("pipes:register"));
    // But not for a Viewer.
    assert!(token_permissions(&["pipes:write"], &Role::Viewer.permissions()).is_empty());
}

#[test]
fn team_names_and_records() {
    assert!(valid_team_name("data-eng").success);
    assert!(valid_team_name("ops.team@x").success);
    assert!(!valid_team_name("").success);
    assert!(!valid_team_name(&"x".repeat(61)).success);
    assert!(!valid_team_name("has space").success);
    let everyone = valid_team_name("everyone");
    assert!(!everyone.success);
    assert!(everyone.message.contains("built-in"));

    assert!(is_everyone(None));
    assert!(is_everyone(Some(" EVERYONE ")));
    assert!(!is_everyone(Some("ops")));
    assert!(same_team(Some("Ops"), Some("ops")));
    assert!(same_team(None, Some("Everyone")));
    assert!(!same_team(None, Some("ops")));
    assert_eq!(normalize_team(Some("Everyone")), None);
    assert_eq!(normalize_team(Some(" ops ")), Some("ops".into()));

    let mut team = Team::new("ops");
    team.add_member("alice", TeamRole::Manager);
    team.add_member("bob", TeamRole::Member);
    assert_eq!(team.managers(), vec!["alice".to_string()]);
    assert_eq!(
        team.member_names(),
        vec!["alice".to_string(), "bob".to_string()]
    );
    assert_eq!(team.role_of("bob"), Some(TeamRole::Member));
    assert_eq!(team.role_of("carol"), None);
    let round = Team::from_value(&team.to_value()).unwrap();
    assert_eq!(round, team);
    assert!(team.remove_member("bob"));
    assert!(!team.remove_member("bob"));

    // Everyone: every user is a member.
    let everyone = Team::everyone();
    assert!(everyone.is_everyone());
    assert_eq!(everyone.role_of("anyone"), Some(TeamRole::Member));

    // Members stored as a list, as `{username, role}` maps, or as JSON text.
    let listed = parse_members(Some(
        &Value::from_json_str(r#"["a", {"username": "b", "role": "manager"}]"#).unwrap(),
    ));
    assert_eq!(listed.get("a"), Some(&TeamRole::Member));
    assert_eq!(listed.get("b"), Some(&TeamRole::Manager));
    let text = parse_members(Some(&Value::Str(r#"{"c": "manager"}"#.into())));
    assert_eq!(text.get("c"), Some(&TeamRole::Manager));
    let from_row = Team::from_value(
        &Value::from_json_str(
            r#"{"team_id": "x", "name": "t1", "members": "{\"d\": \"member\"}"}"#,
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(from_row.id.as_deref(), Some("x"));
    assert_eq!(from_row.role_of("d"), Some(TeamRole::Member));
    assert!(Team::from_value(&Value::from_json_str(r#"{"name": " "}"#).unwrap()).is_none());
}

#[test]
fn primary_teams() {
    let mine = teams(&[
        ("Everyone", TeamRole::Member),
        ("ops", TeamRole::Member),
        ("data", TeamRole::Manager),
    ]);
    // Default: the first team other than Everyone.
    assert_eq!(primary_team(None, &mine), "ops");
    // A stored team the user is in.
    assert_eq!(
        primary_team(Some(&map(r#"{"primary_team": "DATA"}"#)), &mine),
        "data"
    );
    // A stored team the user left falls back.
    assert_eq!(
        primary_team(Some(&map(r#"{"primary_team": "gone"}"#)), &mine),
        "ops"
    );
    assert_eq!(
        primary_team(Some(&map(r#"{"primary_team": "Everyone"}"#)), &mine),
        "Everyone"
    );
    // Only Everyone.
    assert_eq!(
        primary_team(None, &teams(&[("Everyone", TeamRole::Member)])),
        "Everyone"
    );
    assert_eq!(primary_team(None, &[]), "Everyone");
}

#[test]
fn pipe_access_parsing() {
    // No key: the default (Everyone, team, team).
    let default = PipeAccess::from_parameters(&Map::new());
    assert!(default.is_default());
    assert_eq!(default.team_name(), "Everyone");
    let access = PipeAccess::from_parameters(&map(
        r#"{"access": {"owner": "alice", "team": "ops", "visibility": "private", "edit": "owner"}}"#,
    ));
    assert_eq!(access.owner.as_deref(), Some("alice"));
    assert_eq!(access.team.as_deref(), Some("ops"));
    assert_eq!(access.visibility, Visibility::Private);
    assert_eq!(access.edit, EditMode::Owner);
    assert_eq!(PipeAccess::from_value(Some(&access.to_value())), access);
    // `Everyone` is stored as null.
    let everyone = PipeAccess::from_value(Some(&map(r#"{"team": "Everyone"}"#).into_value()));
    assert_eq!(everyone.team, None);
    assert_eq!(
        PipeAccess {
            team: Some("Everyone".into()),
            ..Default::default()
        }
        .to_value()
        .get("team"),
        Some(&Value::Null)
    );
    // A typo never opens a pipe wider than intended.
    let typo = PipeAccess::from_value(Some(
        &map(r#"{"visibility": "privat", "edit": "onwer"}"#).into_value(),
    ));
    assert_eq!(typo.visibility, Visibility::Private);
    assert_eq!(typo.edit, EditMode::Owner);
    // Writers are strict.
    assert!(PipeAccess::parse(&map(r#"{"visibility": "privat"}"#).into_value()).is_err());
    assert!(PipeAccess::parse(&map(r#"{"edit": "nobody"}"#).into_value()).is_err());
    assert!(PipeAccess::parse(&map(r#"{"colour": "red"}"#).into_value()).is_err());
    assert!(PipeAccess::parse(&Value::Str("team".into())).is_err());
    assert_eq!(
        PipeAccess::parse(&map(r#"{"visibility": "everyone"}"#).into_value())
            .unwrap()
            .visibility,
        Visibility::Everyone
    );
    assert_eq!(
        PipeAccess::parse(&Value::Null).unwrap(),
        PipeAccess::default()
    );
    let params = parameters_with_access(&map(r#"{"columns": {}}"#), Some(&access));
    assert_eq!(PipeAccess::from_parameters(&params), access);
    assert!(!parameters_with_access(&params, None).contains_key("access"));
}

trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Map {
    fn into_value(self) -> Value {
        Value::Map(self)
    }
}

#[test]
fn locks_parsing_and_changes() {
    let none = Locks::from_parameters(&Map::new());
    assert!(none.is_empty());
    let locks = Locks::from_parameters(&map(r#"{"locks": ["sync", "DROP", "nope"]}"#));
    assert!(locks.contains(Lock::Sync));
    assert!(locks.contains(Lock::Drop));
    assert!(!locks.contains(Lock::Clear));
    assert_eq!(locks.unknown(), &["nope".to_string()]);
    assert_eq!(locks.names(), vec!["sync", "drop"]);
    // `all` covers every kind.
    let all = Locks::from_parameters(&map(r#"{"locks": ["all"]}"#));
    for kind in Lock::KINDS {
        assert!(all.contains(kind));
    }
    assert_eq!(Locks::from_value(Some(&Value::Bool(true))), Locks::all());
    assert!(Locks::from_value(Some(&Value::Bool(false))).is_empty());
    assert_eq!(
        Locks::from_value(Some(&Value::Str("sync, clear".into()))).names(),
        vec!["sync", "clear"]
    );
    assert_eq!(
        Locks::from_value(Some(&Value::Str(r#"["delete"]"#.into()))).names(),
        vec!["delete"]
    );
    // Typed lists are strict; empty is `all`.
    assert_eq!(
        Locks::parse_list("drop,sync").unwrap().names(),
        vec!["sync", "drop"]
    );
    assert_eq!(Locks::parse_list("").unwrap(), Locks::all());
    let err = Locks::parse_list("drop,syn").unwrap_err();
    assert!(err.contains("'syn'"), "{err}");
    // Adding and removing.
    let sync = Locks::of(&[Lock::Sync]);
    assert_eq!(
        sync.with(&Locks::of(&[Lock::Drop])).names(),
        vec!["sync", "drop"]
    );
    assert_eq!(sync.with(&Locks::all()), Locks::all());
    assert_eq!(
        all.without(&sync).names(),
        vec!["parameters", "clear", "drop", "delete"]
    );
    assert!(all.without(&Locks::all()).is_empty());
    assert_eq!(all.without(&Locks::new()), Locks::all());
    assert!(sync.without(&sync).is_empty());
    let params = parameters_with_locks(&Map::new(), &sync);
    assert_eq!(
        params.get("locks"),
        Some(&Value::from_json_str(r#"["sync"]"#).unwrap())
    );
    assert!(!parameters_with_locks(&params, &Locks::new()).contains_key("locks"));
}

#[test]
fn every_write_is_refused_by_its_locks() {
    let cases: [(LockOp, &[Lock]); 10] = [
        (LockOp::EditParameters, &[Lock::Parameters]),
        (LockOp::Sync, &[Lock::Sync]),
        (LockOp::Replace, &[Lock::Sync, Lock::Clear]),
        (LockOp::Deduplicate, &[Lock::Sync]),
        (LockOp::Clear, &[Lock::Clear]),
        (LockOp::Drop, &[Lock::Drop]),
        (LockOp::Rebuild, &[Lock::Sync, Lock::Clear, Lock::Drop]),
        (LockOp::Schema, &[Lock::Drop]),
        (LockOp::Delete, &[Lock::Delete]),
        (LockOp::DeleteAndDrop, &[Lock::Drop, Lock::Delete]),
    ];
    for (op, refusing) in cases {
        assert!(check_lock(&Locks::new(), op).is_ok(), "{op}");
        assert!(check_lock(&Locks::all(), op).is_err(), "{op}");
        for kind in Lock::KINDS {
            let result = check_lock(&Locks::of(&[kind]), op);
            assert_eq!(
                result.is_err(),
                refusing.contains(&kind),
                "{op} under {kind}"
            );
        }
    }
    // A lock kind converts to the write it refuses.
    assert!(check_lock(&Locks::of(&[Lock::Sync]), Lock::Sync).is_err());
    assert!(check_lock(&Locks::of(&[Lock::Sync]), Lock::Clear).is_ok());
    assert_eq!(
        check_lock(&Locks::of(&[Lock::Sync, Lock::Clear]), LockOp::Replace).unwrap_err(),
        "it is locked (sync, clear)"
    );
    assert_eq!(
        check_lock(&Locks::all(), LockOp::Sync).unwrap_err(),
        "it is locked (all)"
    );
    assert_eq!(LockOp::parse("verify"), Some(LockOp::Sync));
    assert_eq!(LockOp::parse("schema"), Some(LockOp::Schema));
}

#[test]
fn refusal_messages_name_the_pipe_the_lock_and_the_unlock() {
    let p = pipe("sql:main", "weather", None);
    let msg = lock_refusal(&p, &Locks::of(&[Lock::Sync]), LockOp::Sync).unwrap();
    assert!(
        msg.starts_with("Cannot sync Pipe('sql:main', 'weather'"),
        "{msg}"
    );
    assert!(msg.contains("it is locked (sync)"), "{msg}");
    assert!(
        msg.contains("`mrsm unlock pipes -c sql:main -m weather"),
        "{msg}"
    );
    assert!(msg.contains("--locks sync`"), "{msg}");
    assert!(msg.contains("Manage > Locks"), "{msg}");
    assert!(is_lock_refusal(&msg));
    assert!(!is_lock_refusal("Failed to sync."));
    // Under `all`, the command unlocks what this write needs.
    let p = pipe("plugin:noaa", "temp", Some("atl"));
    let msg = lock_refusal(&p, &Locks::all(), LockOp::Replace).unwrap();
    assert!(msg.contains("it is locked (all)"), "{msg}");
    assert!(msg.contains("-l atl"), "{msg}");
    assert!(msg.contains("--locks sync,clear`"), "{msg}");
    assert!(lock_refusal(&p, &Locks::of(&[Lock::Delete]), LockOp::Sync).is_none());
    let cmd = unlock_command(&pipe("a", "b", None), &[]);
    assert_eq!(cmd, "mrsm unlock pipes -c a -m b");
}

#[test]
fn parameter_lock_allows_lock_changes_only() {
    let old = map(r#"{"columns": {"datetime": "ts"}, "locks": ["parameters"]}"#);
    let locked = Locks::from_parameters(&old);
    // Changing `locks` itself.
    let unlock = map(r#"{"columns": {"datetime": "ts"}, "locks": []}"#);
    assert!(parameters_edit_allowed(&old, &unlock, &locked, false));
    // Any other change.
    let edit = map(r#"{"columns": {"datetime": "dt"}, "locks": ["parameters"]}"#);
    assert!(!parameters_edit_allowed(&old, &edit, &locked, false));
    let access =
        map(r#"{"columns": {"datetime": "ts"}, "locks": ["parameters"], "access": {"team": "x"}}"#);
    assert!(!parameters_edit_allowed(&old, &access, &locked, false));
    // Team and user maintenance may move `access`.
    assert!(parameters_edit_allowed(&old, &access, &locked, true));
    // Removing a key is a change.
    let removed = map(r#"{"locks": ["parameters"]}"#);
    assert!(!parameters_edit_allowed(&old, &removed, &locked, false));
    // Without the lock anything goes.
    assert!(parameters_edit_allowed(
        &old,
        &edit,
        &Locks::of(&[Lock::Sync]),
        false
    ));
    assert_eq!(
        changed_parameter_keys(&old, &removed, &[]),
        vec!["columns".to_string()]
    );
    assert_eq!(
        changed_parameter_keys(&old, &edit, &[]),
        vec!["columns".to_string()]
    );
}

#[test]
fn visibility_and_edit_rules() {
    let admin = Principal::admin("root");
    let alice = user(
        "alice",
        &["pipes:read", "pipes:write"],
        &[("Everyone", TeamRole::Member), ("ops", TeamRole::Member)],
    );
    let manager = user("mia", &["pipes:read"], &[("ops", TeamRole::Manager)]);
    let other = user("otto", &["pipes:read"], &[("data", TeamRole::Member)]);
    let owner = user("olga", &["pipes:read"], &[]);

    // The default: everyone sees and edits.
    let open = PipeAccess::default();
    for p in [&admin, &alice, &manager, &other, &owner] {
        assert!(can_see(p, &open), "{}", p.username);
        assert!(can_edit(p, &open), "{}", p.username);
    }
    // Only Everyone's managers and admins manage the default pipe's access.
    assert!(can_manage_access(&admin, &open));
    assert!(!can_manage_access(&alice, &open));
    let everyone_manager = user("eve", &[], &[("Everyone", TeamRole::Manager)]);
    assert!(can_manage_access(&everyone_manager, &open));

    // Team visibility: the team's members and the owner.
    let team = PipeAccess {
        owner: Some("olga".into()),
        team: Some("OPS".into()),
        ..Default::default()
    };
    assert!(can_see(&alice, &team));
    assert!(can_see(&manager, &team));
    assert!(can_see(&owner, &team));
    assert!(can_see(&admin, &team));
    assert!(!can_see(&other, &team));
    assert!(can_edit(&alice, &team));
    assert!(!can_edit(&other, &team));

    // Private: the owner, the team's managers and admins.
    let private = PipeAccess {
        visibility: Visibility::Private,
        ..team.clone()
    };
    assert!(!can_see(&alice, &private));
    assert!(can_see(&manager, &private));
    assert!(can_see(&owner, &private));
    assert!(can_see(&admin, &private));
    assert!(!can_see(&other, &private));

    // Everyone: every user sees it; edit `team` lets anyone who sees it change it.
    let public = PipeAccess {
        visibility: Visibility::Everyone,
        ..team.clone()
    };
    assert!(can_see(&other, &public));
    assert!(can_edit(&other, &public));

    // Edit `owner`: the owner, the team's managers and admins.
    let owner_only = PipeAccess {
        edit: EditMode::Owner,
        ..public.clone()
    };
    assert!(can_see(&other, &owner_only));
    assert!(!can_edit(&other, &owner_only));
    assert!(!can_edit(&alice, &owner_only));
    assert!(can_edit(&manager, &owner_only));
    assert!(can_edit(&owner, &owner_only));
    assert!(can_edit(&admin, &owner_only));

    // Managing access and locks: owner, team managers, admins.
    for (p, expected) in [
        (&admin, true),
        (&owner, true),
        (&manager, true),
        (&alice, false),
        (&other, false),
    ] {
        assert_eq!(can_manage_access(p, &team), expected, "{}", p.username);
        assert_eq!(can_lock(p, &team), expected, "{}", p.username);
    }
    // An anonymous caller owns nothing.
    let anonymous = user("", &[], &[]);
    assert!(!can_manage_access(&anonymous, &PipeAccess::default()));
}

#[test]
fn principal_permissions_and_teams() {
    let p = user(
        "alice",
        &["pipes:read"],
        &[("ops", TeamRole::Member), ("Ops", TeamRole::Manager)],
    );
    assert!(p.has("pipes:read"));
    assert!(!p.has("pipes:write"));
    assert!(Principal::admin("root").has("anything:at:all"));
    assert!(user("x", &["*"], &[]).has("pipes:drop"));
    // The highest role listed for a team counts.
    assert_eq!(p.team_role(Some("ops")), Some(TeamRole::Manager));
    assert!(p.is_member(None));
    assert!(p.is_member(Some("Everyone")));
    assert!(!p.is_manager(None));
    assert_eq!(
        p.team_names(),
        vec!["Everyone".to_string(), "ops".to_string()]
    );
    let admin = Principal::new("root", true, Permissions::new(), vec![]);
    assert_eq!(admin.permissions, all_permissions());
}

#[test]
fn email_privacy() {
    let target = teams(&[("Everyone", TeamRole::Member), ("ops", TeamRole::Member)]);
    assert!(can_see_email(&Principal::admin("root"), &target));
    assert!(can_see_email(
        &user("mia", &[], &[("ops", TeamRole::Manager)]),
        &target
    ));
    assert!(!can_see_email(
        &user("max", &[], &[("ops", TeamRole::Member)]),
        &target
    ));
    assert!(!can_see_email(
        &user("dan", &[], &[("data", TeamRole::Manager)]),
        &target
    ));
    // Everyone's managers manage every user.
    assert!(can_see_email(
        &user("eve", &[], &[("Everyone", TeamRole::Manager)]),
        &[]
    ));
    // One's own address.
    assert!(can_see_email_of(&user("max", &[], &[]), "max", &target));
    assert!(!can_see_email_of(&user("max", &[], &[]), "bob", &target));
    assert!(!can_see_email_of(&user("", &[], &[]), "", &[]));
}

#[test]
fn team_management_rules() {
    let manager = user("mia", &[], &[("ops", TeamRole::Manager)]);
    let member = user("max", &[], &[("ops", TeamRole::Member)]);
    assert!(can_manage_team(&manager, "OPS"));
    assert!(!can_manage_team(&manager, "data"));
    assert!(!can_manage_team(&member, "ops"));
    assert!(can_manage_team(&Principal::admin("root"), "anything"));
    assert!(!can_administer_teams(&manager));
    assert!(can_administer_teams(&Principal::admin("root")));
}

#[test]
fn new_pipes_belong_to_their_creator() {
    let alice = user("alice", &[], &[("ops", TeamRole::Member)]);
    let access = access_for_new_pipe(&alice, None, "ops");
    assert_eq!(access.owner.as_deref(), Some("alice"));
    assert_eq!(access.team.as_deref(), Some("ops"));
    // Someone else's name is replaced by the caller's.
    let requested = PipeAccess {
        owner: Some("mallory".into()),
        team: Some("data".into()),
        visibility: Visibility::Private,
        ..Default::default()
    };
    let access = access_for_new_pipe(&alice, Some(&requested), "ops");
    assert_eq!(access.owner.as_deref(), Some("alice"));
    assert_eq!(access.team.as_deref(), Some("data"));
    assert_eq!(access.visibility, Visibility::Private);
    // Administrators may name another owner.
    let access = access_for_new_pipe(&Principal::admin("root"), Some(&requested), "Everyone");
    assert_eq!(access.owner.as_deref(), Some("mallory"));
    let access = access_for_new_pipe(&Principal::admin("root"), None, "Everyone");
    assert_eq!(access.owner.as_deref(), Some("root"));
    assert_eq!(access.team, None);
}

#[test]
fn access_changes() {
    let manager = user(
        "mia",
        &[],
        &[("ops", TeamRole::Manager), ("data", TeamRole::Member)],
    );
    let owner = user("olga", &[], &[("ops", TeamRole::Member)]);
    let stranger = user("sam", &[], &[("ops", TeamRole::Member)]);
    let old = PipeAccess {
        owner: Some("olga".into()),
        team: Some("ops".into()),
        ..Default::default()
    };
    let private = PipeAccess {
        visibility: Visibility::Private,
        ..old.clone()
    };
    assert!(check_access_change(&owner, &old, &private).is_ok());
    assert!(check_access_change(&stranger, &old, &private).is_err());
    // To a team the caller is not in.
    let to_data = PipeAccess {
        team: Some("data".into()),
        ..old.clone()
    };
    assert!(check_access_change(&owner, &old, &to_data).is_err());
    assert!(check_access_change(&manager, &old, &to_data).is_ok());
    // Another owner: team managers only; the owner may give it up.
    let to_sam = PipeAccess {
        owner: Some("sam".into()),
        ..old.clone()
    };
    assert!(check_access_change(&owner, &old, &to_sam).is_err());
    assert!(check_access_change(&manager, &old, &to_sam).is_ok());
    let nobody = PipeAccess {
        owner: None,
        ..old.clone()
    };
    assert!(check_access_change(&owner, &old, &nobody).is_ok());
    assert!(check_access_change(&Principal::admin("root"), &old, &to_sam).is_ok());
}

#[test]
fn sql_definitions() {
    let old =
        map(r#"{"fetch": {"definition": "SELECT 1", "backtrack_minutes": 5}, "columns": {}}"#);
    let same = old.clone();
    assert!(!sql_definition_changed("sql:main", &old, &same));
    let mut definition = old.clone();
    definition.insert(
        "fetch".into(),
        Value::from_json_str(r#"{"definition": "SELECT 2", "backtrack_minutes": 5}"#).unwrap(),
    );
    assert!(sql_definition_changed("sql:main", &old, &definition));
    assert!(sql_definition_changed("plugin:x", &old, &definition));
    // Other fetch parameters count for `sql:` connectors only.
    let mut backtrack = old.clone();
    backtrack.insert(
        "fetch".into(),
        Value::from_json_str(r#"{"definition": "SELECT 1", "backtrack_minutes": 10}"#).unwrap(),
    );
    assert!(sql_definition_changed("sql:main", &old, &backtrack));
    assert!(!sql_definition_changed("plugin:x", &old, &backtrack));
    let mut sql = old.clone();
    sql.insert("sql".into(), Value::Str("SELECT 3".into()));
    assert!(sql_definition_changed("plugin:x", &old, &sql));
    let mut columns = old.clone();
    columns.insert(
        "columns".into(),
        Value::from_json_str(r#"{"datetime": "ts"}"#).unwrap(),
    );
    assert!(!sql_definition_changed("sql:main", &old, &columns));
}

#[test]
fn serde_names() {
    assert_eq!(
        serde_json::to_string(&Role::Operator).unwrap(),
        "\"operator\""
    );
    assert_eq!(
        serde_json::to_string(&TeamRole::Manager).unwrap(),
        "\"manager\""
    );
    assert_eq!(
        serde_json::to_string(&Visibility::Private).unwrap(),
        "\"private\""
    );
    assert_eq!(
        serde_json::to_string(&EditMode::Owner).unwrap(),
        "\"owner\""
    );
    assert_eq!(
        serde_json::to_string(&Lock::Parameters).unwrap(),
        "\"parameters\""
    );
    assert_eq!(
        serde_json::to_string(&LockOp::DeleteAndDrop).unwrap(),
        "\"delete_and_drop\""
    );
    let p = user("a", &["pipes:read"], &[("ops", TeamRole::Member)]);
    let back: Principal = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    assert_eq!(back, p);
}
