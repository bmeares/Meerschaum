//! Shared checks of the access model on an instance (feature `testing`): every instance
//! connector crate runs them in its own tests, so locks, the team store and the user lookups
//! behave the same on SQL, Valkey and custom instance connectors.
//!
//! Each check panics on the first failure and cleans up what it created. `prefix` keeps the
//! keys, usernames and team names of concurrent tests apart (letters, digits and `_` only).

use meerschaum_core::access::{
    EVERYONE, Lock, Locks, PRIMARY_TEAM_ATTRIBUTE, PipeAccess, Team, TeamRole, Visibility,
    is_lock_refusal,
};
use meerschaum_core::users::{User, verify_password};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::pipe_ops::{self, Doc};
use crate::traits::{DataQuery, InstanceConnector, SyncOptions};

#[track_caller]
fn ok(t: SuccessTuple) -> SuccessTuple {
    assert!(t.success, "expected success: {}", t.message);
    t
}

#[track_caller]
fn refused(t: SuccessTuple, lock: &str, what: &str) {
    assert!(!t.success, "{what} should be refused: {}", t.message);
    assert!(is_lock_refusal(&t.message), "{what}: {}", t.message);
    assert!(
        t.message.contains(&format!("it is locked ({lock}")),
        "{what}: {}",
        t.message
    );
}

fn ts(day: u32) -> KValue {
    let dt = chrono::NaiveDate::from_ymd_opt(2024, 1, day)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("valid date");
    KValue::Datetime(DateTimeValue::utc(dt))
}

fn row(day: u32, id: i64, v: f64) -> Doc {
    [
        ("ts".to_string(), ts(day)),
        ("id".to_string(), KValue::Int(id)),
        ("v".to_string(), KValue::Float(v)),
    ]
    .into_iter()
    .collect()
}

fn map(json: &str) -> Map {
    Value::from_json_str(json).expect("valid JSON").to_map()
}

fn pipe_on<C: InstanceConnector + ?Sized>(conn: &C, ck: &str, mk: &str, params: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(&conn.connector_keys())
        .parameters(map(params))
        .build()
        .expect("valid pipe")
}

async fn registered<C: InstanceConnector + ?Sized>(conn: &C, pipe: &Pipe) -> Map {
    crate::locks::registered_parameters(conn, pipe)
        .await
        .expect("read")
        .expect("registered")
}

async fn rowcount<C: InstanceConnector + ?Sized>(conn: &C, pipe: &Pipe) -> Option<i64> {
    conn.get_pipe_rowcount(pipe, &DataQuery::default())
        .await
        .expect("rowcount")
}

async fn sync<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    docs: Vec<Doc>,
) -> SuccessTuple {
    pipe_ops::sync_docs(conn, pipe, docs, true).await
}

async fn cleanup_pipe<C: InstanceConnector + ?Sized>(conn: &C, pipe: &Pipe) {
    let _ = conn.unlock_pipe(pipe, &Locks::all()).await;
    let _ = conn.drop_pipe(pipe).await;
    let _ = conn.delete_pipe(pipe).await;
}

/// Locks refuse every write they name, with the message that names the pipe, the lock and how
/// to unlock it; `parameters` allows changing `locks`; unlocking restores every write.
pub async fn check_locks<C: InstanceConnector + ?Sized>(conn: &C, prefix: &str) {
    let pipe = pipe_on(
        conn,
        &format!("plugin:{prefix}"),
        "locks",
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    cleanup_pipe(conn, &pipe).await;
    ok(conn.register_pipe(&pipe).await);
    ok(sync(conn, &pipe, vec![row(1, 1, 1.0), row(2, 1, 2.0)]).await);
    assert_eq!(rowcount(conn, &pipe).await, Some(2));

    // `all`: every write is refused, whatever the surface asked.
    ok(conn.lock_pipe(&pipe, &Locks::all()).await);
    assert_eq!(
        Locks::from_parameters(&registered(conn, &pipe).await).names(),
        vec!["all"]
    );
    refused(sync(conn, &pipe, vec![row(3, 1, 3.0)]).await, "all", "sync");
    let df = meerschaum_kernel::docs::frame_from_docs(&[row(3, 1, 3.0)]).expect("frame");
    refused(
        conn.sync_pipe(
            &pipe,
            df.clone(),
            &SyncOptions {
                replace: true,
                ..Default::default()
            },
        )
        .await,
        "all",
        "replace",
    );
    refused(
        conn.clear_pipe(&pipe, &DataQuery::default()).await,
        "all",
        "clear",
    );
    refused(conn.drop_pipe(&pipe).await, "all", "drop");
    refused(conn.delete_pipe(&pipe).await, "all", "delete");
    let dedup = conn.deduplicate_pipe(&pipe, &DataQuery::default()).await;
    assert!(
        !dedup.success
            && (is_lock_refusal(&dedup.message) || dedup.message.contains("not supported")),
        "deduplicate: {}",
        dedup.message
    );
    let mut edited = pipe.clone();
    edited.update_parameters(map(r#"{"tags": ["edited"]}"#));
    refused(conn.edit_pipe(&edited, true).await, "all", "edit");
    refused(conn.edit_pipe(&edited, false).await, "all", "edit");
    refused(
        conn.set_pipe_access(
            &pipe,
            &PipeAccess {
                visibility: Visibility::Private,
                ..Default::default()
            },
        )
        .await,
        "all",
        "set access",
    );
    let msg = sync(conn, &pipe, vec![row(4, 1, 4.0)]).await.message;
    assert!(msg.contains(&format!("plugin:{prefix}")), "{msg}");
    assert!(msg.contains("mrsm unlock pipes"), "{msg}");
    assert!(msg.contains("--locks sync"), "{msg}");
    assert_eq!(rowcount(conn, &pipe).await, Some(2));

    // Changing `locks` is the one edit a `parameters` lock allows: unlock `sync` only.
    ok(conn.unlock_pipe(&pipe, &Locks::of(&[Lock::Sync])).await);
    assert_eq!(
        Locks::from_parameters(&registered(conn, &pipe).await).names(),
        vec!["parameters", "clear", "drop", "delete"]
    );
    ok(sync(conn, &pipe, vec![row(3, 1, 3.0)]).await);
    assert_eq!(rowcount(conn, &pipe).await, Some(3));
    // A replacing sync deletes rows: `clear` still refuses it.
    refused(
        conn.sync_pipe(
            &pipe,
            df,
            &SyncOptions {
                replace: true,
                ..Default::default()
            },
        )
        .await,
        "clear",
        "replace under clear",
    );
    refused(
        conn.clear_pipe(&pipe, &DataQuery::default()).await,
        "clear",
        "clear",
    );
    refused(conn.drop_pipe(&pipe).await, "drop", "drop");
    refused(conn.edit_pipe(&edited, true).await, "parameters", "edit");

    // Each kind alone refuses only its writes.
    ok(conn.unlock_pipe(&pipe, &Locks::all()).await);
    assert!(Locks::from_parameters(&registered(conn, &pipe).await).is_empty());
    ok(conn.lock_pipe(&pipe, &Locks::of(&[Lock::Delete])).await);
    refused(conn.delete_pipe(&pipe).await, "delete", "delete");
    ok(sync(conn, &pipe, vec![row(4, 1, 4.0)]).await);
    ok(conn.edit_pipe(&edited, true).await);
    ok(conn.unlock_pipe(&pipe, &Locks::of(&[Lock::Delete])).await);
    ok(conn.lock_pipe(&pipe, &Locks::of(&[Lock::Clear])).await);
    let one_day = DataQuery {
        begin: Some(crate::traits::DateBound::Datetime(DateTimeValue::utc(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 4)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("valid date"),
        ))),
        ..Default::default()
    };
    refused(conn.clear_pipe(&pipe, &one_day).await, "clear", "clear");
    ok(conn.unlock_pipe(&pipe, &Locks::all()).await);
    ok(conn.clear_pipe(&pipe, &one_day).await);
    // Counted by reading (Valkey's unfiltered rowcount keeps v4's stale index entries).
    let rows = conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .expect("data")
        .map(|df| df.height());
    assert_eq!(rows, Some(3));
    ok(conn.drop_pipe(&pipe).await);
    ok(conn.delete_pipe(&pipe).await);
    // An unregistered pipe cannot be locked through the instance.
    assert!(!conn.lock_pipe(&pipe, &Locks::all()).await.success);
}

async fn register_user<C: InstanceConnector + ?Sized>(
    conn: &C,
    username: &str,
    password: &str,
    user_type: Option<&str>,
    attributes: &str,
) {
    let _ = conn.delete_user(&User::new(username, None)).await;
    let mut user = User::new(username, Some(password));
    user.user_type = user_type.map(String::from);
    user.email = Some(format!("{username}@example.com"));
    user.attributes = Some(map(attributes));
    ok(conn.register_user(&mut user).await);
}

/// The team store: CRUD, Everyone, members' roles, renames and deletions carried into the
/// pipes' `access` (under a `parameters` lock too) and the users' primary teams, and a deleted
/// user leaving their teams and pipes.
pub async fn check_teams<C: InstanceConnector + ?Sized>(conn: &C, prefix: &str) {
    let alice = format!("{prefix}_alice");
    let bob = format!("{prefix}_bob");
    register_user(conn, &alice, "a-pass", None, "{}").await;
    register_user(conn, &bob, "b-pass", None, "{}").await;
    let ops_name = format!("{prefix}-ops");
    let renamed_name = format!("{prefix}-ops2");
    for name in [&ops_name, &renamed_name] {
        let _ = conn.delete_team(name).await;
    }

    // Everyone always exists and comes first.
    let teams = conn.get_teams().await.expect("teams");
    assert_eq!(teams[0].name, EVERYONE);
    assert!(conn.get_team("everyone").await.expect("read").is_some());

    // Register.
    let mut ops = Team::new(&ops_name);
    ops.add_member(&alice, TeamRole::Manager);
    ok(conn.register_team(&mut ops).await);
    assert!(ops.id.is_some());
    let mut twin = Team::new(&ops_name.to_uppercase());
    assert!(
        !conn.register_team(&mut twin).await.success,
        "names are unique without case"
    );
    let mut stranger = Team::new(&format!("{prefix}-x"));
    stranger.add_member(&format!("{prefix}_nobody"), TeamRole::Member);
    let unknown = conn.register_team(&mut stranger).await;
    assert!(
        !unknown.success && unknown.message.contains("not registered"),
        "{}",
        unknown.message
    );
    assert!(!conn.register_team(&mut Team::new("bad name")).await.success);
    assert!(!conn.register_team(&mut Team::new("Everyone")).await.success);

    // Read.
    let found = conn
        .get_team(&ops_name.to_uppercase())
        .await
        .expect("read")
        .expect("found");
    assert_eq!(found.name, ops_name);
    assert_eq!(found.id, ops.id);
    assert_eq!(found.role_of(&alice), Some(TeamRole::Manager));
    let names: Vec<String> = conn
        .get_teams()
        .await
        .expect("teams")
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(names.contains(&ops_name), "{names:?}");
    assert_eq!(
        conn.get_user_teams(&alice).await.expect("teams"),
        vec![
            (EVERYONE.to_string(), TeamRole::Member),
            (ops_name.clone(), TeamRole::Manager)
        ]
    );

    // Edit: members and Everyone's managers.
    let mut edit = found.clone();
    edit.add_member(&bob, TeamRole::Member);
    ok(conn.edit_team(&edit).await);
    let found = conn
        .get_team(&ops_name)
        .await
        .expect("read")
        .expect("found");
    assert_eq!(found.role_of(&bob), Some(TeamRole::Member));
    let mut everyone = Team::everyone();
    everyone.add_member(&bob, TeamRole::Manager);
    ok(conn.edit_team(&everyone).await);
    assert_eq!(
        conn.get_user_teams(&bob).await.expect("teams")[0],
        (EVERYONE.to_string(), TeamRole::Manager)
    );
    ok(conn.edit_team(&Team::everyone()).await);
    let mut renaming_by_edit = found.clone();
    renaming_by_edit.name = format!("{prefix}-other");
    assert!(!conn.edit_team(&renaming_by_edit).await.success);

    // A pipe of the team, locked against parameter edits, and a primary team.
    let pipe = pipe_on(
        conn,
        &format!("plugin:{prefix}"),
        "teams",
        &format!(
            r#"{{"columns": {{"datetime": "ts"}}, "access": {{"owner": "{bob}", "team": "{ops_name}", "visibility": "private"}}, "locks": ["parameters"]}}"#
        ),
    );
    cleanup_pipe(conn, &pipe).await;
    ok(conn.register_pipe(&pipe).await);
    let mut attributes = Map::new();
    attributes.insert(PRIMARY_TEAM_ATTRIBUTE.into(), Value::Str(ops_name.clone()));
    ok(conn
        .set_user_attributes(&User::new(&alice, None), &attributes)
        .await);
    let hash = conn
        .get_user_password_hash(&User::new(&alice, None))
        .await
        .expect("hash")
        .expect("set");
    assert!(
        verify_password("a-pass", &hash).expect("verify"),
        "only the attributes changed"
    );

    // Rename: the pipe (under its `parameters` lock) and the primary team follow.
    assert!(!conn.rename_team(&ops_name, "Everyone").await.success);
    assert!(!conn.rename_team("Everyone", &renamed_name).await.success);
    let renamed = ok(conn.rename_team(&ops_name, &renamed_name).await);
    assert!(
        renamed.message.contains("1 pipe updated"),
        "{}",
        renamed.message
    );
    assert!(conn.get_team(&ops_name).await.expect("read").is_none());
    let access = PipeAccess::from_parameters(&registered(conn, &pipe).await);
    assert_eq!(access.team.as_deref(), Some(renamed_name.as_str()));
    assert_eq!(access.visibility, Visibility::Private);
    let attrs = conn
        .get_user_attributes(&User::new(&alice, None))
        .await
        .expect("read")
        .expect("set");
    assert_eq!(
        attrs.get(PRIMARY_TEAM_ATTRIBUTE),
        Some(&Value::Str(renamed_name.clone()))
    );
    assert!(
        meerschaum_core::access::check_lock(
            &Locks::from_parameters(&registered(conn, &pipe).await),
            meerschaum_core::access::LockOp::EditParameters
        )
        .is_err()
    );

    // Deleting a user: they leave the team and their pipe loses its owner.
    ok(conn.delete_user(&User::new(&bob, None)).await);
    let team = conn
        .get_team(&renamed_name)
        .await
        .expect("read")
        .expect("found");
    assert_eq!(team.role_of(&bob), None);
    let access = PipeAccess::from_parameters(&registered(conn, &pipe).await);
    assert_eq!(access.owner, None);
    assert_eq!(access.team.as_deref(), Some(renamed_name.as_str()));

    // Delete: the pipe moves to Everyone, the primary team falls back.
    assert!(!conn.delete_team("Everyone").await.success);
    assert!(!conn.delete_team(&format!("{prefix}-missing")).await.success);
    let deleted = ok(conn.delete_team(&renamed_name.to_uppercase()).await);
    assert!(
        deleted.message.contains("1 pipe moved to Everyone"),
        "{}",
        deleted.message
    );
    assert!(conn.get_team(&renamed_name).await.expect("read").is_none());
    let access = PipeAccess::from_parameters(&registered(conn, &pipe).await);
    assert_eq!(access.team, None);
    let attrs = conn
        .get_user_attributes(&User::new(&alice, None))
        .await
        .expect("read")
        .unwrap_or_default();
    assert!(
        attrs
            .get(PRIMARY_TEAM_ATTRIBUTE)
            .is_none_or(|v| v.is_null()),
        "{attrs:?}"
    );

    cleanup_pipe(conn, &pipe).await;
    let _ = conn.delete_user(&User::new(&alice, None)).await;
}

/// FX1: users are looked up by their exact username. A username with a leading `_` (which a
/// `params` filter reads as a negation) never reaches another user's row, and an unknown user
/// has no fields.
pub async fn check_user_lookups<C: InstanceConnector + ?Sized>(conn: &C, prefix: &str) {
    let alice = format!("{prefix}alice");
    let underscored = format!("_{prefix}alice");
    let dashed = format!("{prefix}a--b");
    register_user(
        conn,
        &alice,
        "alice-pass",
        Some("admin"),
        r#"{"who": "alice"}"#,
    )
    .await;
    register_user(
        conn,
        &underscored,
        "under-pass",
        None,
        r#"{"who": "underscored"}"#,
    )
    .await;
    register_user(conn, &dashed, "dash-pass", None, r#"{"who": "dashed"}"#).await;

    let a = User::new(&alice, None);
    let u = User::new(&underscored, None);
    let d = User::new(&dashed, None);
    let a_id = conn.get_user_id(&a).await.expect("id").expect("alice");
    let u_id = conn.get_user_id(&u).await.expect("id").expect("_alice");
    assert_ne!(a_id, u_id);
    assert!(conn.get_user_id(&d).await.expect("id").is_some());
    assert_eq!(
        conn.get_username(&u_id)
            .await
            .expect("username")
            .map(|v| meerschaum_core::pyfmt::py_str(&v)),
        Some(underscored.clone())
    );
    let hash = conn
        .get_user_password_hash(&u)
        .await
        .expect("hash")
        .expect("set");
    assert!(verify_password("under-pass", &hash).expect("verify"));
    assert!(!verify_password("alice-pass", &hash).expect("verify"));
    assert_ne!(
        conn.get_user_type(&u).await.expect("type").as_deref(),
        Some("admin")
    );
    assert_eq!(
        conn.get_user_type(&a).await.expect("type").as_deref(),
        Some("admin")
    );
    let attrs = conn
        .get_user_attributes(&u)
        .await
        .expect("attributes")
        .expect("set");
    assert_eq!(attrs.get("who"), Some(&Value::Str("underscored".into())));
    let attrs = conn
        .get_user_attributes(&d)
        .await
        .expect("attributes")
        .expect("set");
    assert_eq!(attrs.get("who"), Some(&Value::Str("dashed".into())));

    // An unknown user has nothing, and nothing of theirs is edited or deleted.
    let nobody = User::new(&format!("_{prefix}nobody"), None);
    assert_eq!(conn.get_user_id(&nobody).await.expect("id"), None);
    assert_eq!(
        conn.get_user_attributes(&nobody).await.expect("attributes"),
        None
    );
    assert_eq!(conn.get_user_type(&nobody).await.expect("type"), None);
    assert_eq!(
        conn.get_user_password_hash(&nobody).await.expect("hash"),
        None
    );
    let mut edit = nobody.clone();
    edit.attributes = Some(map(r#"{"who": "mallory"}"#));
    assert!(!conn.edit_user(&edit).await.success);
    // Deleting an unknown user deletes nobody else (Valkey answers success, as v4 did).
    let _ = conn.delete_user(&nobody).await;
    for user in [&a, &u, &d] {
        assert!(
            conn.get_user_id(user).await.expect("id").is_some(),
            "{}",
            user.username
        );
    }
    let attrs = conn
        .get_user_attributes(&a)
        .await
        .expect("attributes")
        .expect("set");
    assert_eq!(attrs.get("who"), Some(&Value::Str("alice".into())));

    // Deleting `_alice` deletes `_alice` only.
    ok(conn.delete_user(&u).await);
    assert_eq!(conn.get_user_id(&u).await.expect("id"), None);
    assert!(conn.get_user_id(&a).await.expect("id").is_some());
    let _ = conn.delete_user(&a).await;
    let _ = conn.delete_user(&d).await;
}
