//! A deleted user's tokens never outlive them: `delete_user` deletes them first, and on SQLite
//! (which hands a new row the highest deleted rowid again) a new user's ID is above every token
//! owner's, so a key left by a user deleted before this rule existed is never handed to them.
//!
//! SQLite always; the server flavors when their URIs are set.

mod common;
use chrono::Utc;
use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::users::{Expiration, Token, User};
use meerschaum_kernel::value::Value as KValue;

fn unique(tag: &str) -> String {
    format!(
        "rs_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

async fn token_for(c: &Ctx, user: &User) -> Token {
    let mut token = Token::new(
        Some(&unique("key")),
        Expiration::Default,
        Some(vec!["pipes:read".into()]),
        366,
        Utc::now(),
    );
    token.user = Some(user.clone());
    ok(InstanceConnector::register_token(&c.conn, &mut token).await);
    token
}

flavor_tests!(
    test_deleting_a_user_deletes_their_tokens,
    |c: Ctx| async move {
        let mut keeper = User::new(&unique("keeper"), Some("test1234"));
        ok(c.conn.register_user(&mut keeper).await);
        let kept = token_for(&c, &keeper).await;
        let mut leaver = User::new(&unique("leaver"), Some("test1234"));
        ok(c.conn.register_user(&mut leaver).await);
        let gone = token_for(&c, &leaver).await;

        ok(c.conn.delete_user(&leaver).await);
        let model = InstanceConnector::get_token_model(&c.conn, &gone.id.unwrap())
            .await
            .unwrap();
        assert!(model.is_none(), "a deleted user's token survived");
        // Another user's token is untouched.
        assert!(
            InstanceConnector::get_token_model(&c.conn, &kept.id.unwrap())
                .await
                .unwrap()
                .is_some()
        );
        ok(InstanceConnector::delete_token(&c.conn, &kept).await);
        ok(c.conn.delete_user(&keeper).await);
    }
);

flavor_tests!(
    test_a_new_user_never_inherits_an_orphaned_token,
    |c: Ctx| async move {
        let mut alice = User::new(&unique("alice"), Some("test1234"));
        ok(c.conn.register_user(&mut alice).await);
        let alice_id = c.conn.get_user_id(&alice).await.unwrap().unwrap();
        let key = token_for(&c, &alice).await;
        // A user deleted before deletions removed tokens: only the row goes.
        let users = c.conn.instance_table_name("mrsm_users");
        let removed = c
            .conn
            .exec(&format!(
                "DELETE FROM {users} WHERE user_id = {}",
                alice_id.as_i64().unwrap()
            ))
            .await;
        assert!(removed.is_some());

        let mut bob = User::new(&unique("bob"), Some("test1234"));
        ok(c.conn.register_user(&mut bob).await);
        let bob_id = c.conn.get_user_id(&bob).await.unwrap().unwrap();
        assert_ne!(
            bob_id.as_i64(),
            alice_id.as_i64(),
            "the new user took the ID an orphaned token names"
        );
        let owner_id = InstanceConnector::get_token_model(&c.conn, &key.id.unwrap())
            .await
            .unwrap()
            .unwrap()
            .user_id
            .unwrap();
        assert!(
            InstanceConnector::get_username(&c.conn, &owner_id)
                .await
                .unwrap()
                .is_none(),
            "the orphaned token resolves to a user"
        );
        ok(InstanceConnector::delete_token(&c.conn, &key).await);
        ok(c.conn.delete_user(&bob).await);
    }
);

// The token-registration race, step by step: a registration reads its owner's ID, the owner is
// deleted with their keys, a new user registers, and only then is the key written. The new user
// never takes the deleted owner's ID, so the late key names nobody.
flavor_tests!(
    test_a_key_written_after_its_owner_is_deleted_names_nobody,
    |c: Ctx| async move {
        let mut owner = User::new(&unique("owner"), Some("test1234"));
        ok(c.conn.register_user(&mut owner).await);
        let owner_id = c.conn.get_user_id(&owner).await.unwrap().unwrap();
        ok(c.conn.delete_user(&owner).await);
        let mut newcomer = User::new(&unique("newcomer"), Some("test1234"));
        ok(c.conn.register_user(&mut newcomer).await);
        let newcomer_id = c.conn.get_user_id(&newcomer).await.unwrap().unwrap();
        assert_ne!(newcomer_id.as_i64(), owner_id.as_i64());
        // The late write of `register_token` (it read the owner's ID before the deletion and
        // writes its row now, with that ID).
        use meerschaum_connector::pipe_ops;
        let late_id = uuid::Uuid::new_v4();
        let doc: pipe_ops::Doc = [
            ("id".to_string(), KValue::Uuid(late_id)),
            ("user_id".to_string(), pipe_ops::user_id_value(&owner_id)),
            ("creation".to_string(), pipe_ops::utc_value(Utc::now())),
            ("label".to_string(), KValue::Str(unique("late"))),
            ("is_valid".to_string(), KValue::Bool(true)),
            ("secret_hash".to_string(), KValue::Str("x".into())),
        ]
        .into_iter()
        .collect();
        let tokens = InstanceConnector::get_tokens_pipe(&c.conn);
        ok(pipe_ops::sync_docs(&c.conn, &tokens, vec![doc], false).await);
        let late_owner = InstanceConnector::get_token_model(&c.conn, &late_id)
            .await
            .unwrap()
            .expect("the late key was written")
            .user_id
            .unwrap();
        assert!(
            InstanceConnector::get_username(&c.conn, &late_owner)
                .await
                .unwrap()
                .is_none(),
            "the late key names a user"
        );
        ok(c.conn.delete_user(&newcomer).await);
    }
);

/// SQLite: reserved IDs are never handed out twice, concurrent registrations get distinct IDs,
/// a registration that fails after reserving wastes its ID, and the largest ID is an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_user_ids_are_reserved_once() {
    let Some(c) = ctx("sqlite", "reserved_ids").await else {
        return;
    };
    let mut first = User::new(&unique("first"), Some("test1234"));
    ok(c.conn.register_user(&mut first).await);
    let mut last = User::new(&unique("last"), Some("test1234"));
    ok(c.conn.register_user(&mut last).await);
    let last_id = c
        .conn
        .get_user_id(&last)
        .await
        .unwrap()
        .unwrap()
        .as_i64()
        .unwrap();
    ok(c.conn.delete_user(&last).await);

    // Concurrent registrations, two of them for the same username.
    let twin = unique("twin");
    let mut names: Vec<String> = (0..6).map(|i| unique(&format!("racer{i}"))).collect();
    names.push(twin.clone());
    names.push(twin.clone());
    let conn = &c.conn;
    let outcomes = futures_util::future::join_all(names.into_iter().map(|name| async move {
        let mut user = User::new(&name, Some("test1234"));
        (name.clone(), conn.register_user(&mut user).await.success)
    }))
    .await;
    let registered: Vec<String> = outcomes
        .into_iter()
        .filter(|(_, success)| *success)
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        registered.iter().filter(|n| **n == twin).count(),
        1,
        "one twin"
    );
    assert_eq!(registered.len(), 7);
    let mut ids = Vec::new();
    for name in &registered {
        let id = c
            .conn
            .get_user_id(&User::new(name, None))
            .await
            .unwrap()
            .unwrap()
            .as_i64()
            .unwrap();
        assert!(id > last_id, "{name} took a deleted user's ID");
        ids.push(id);
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 7, "distinct IDs");
    let highest = *ids.last().unwrap();
    // A reservation whose user insert failed (as the losing twin's may) is spent: the next
    // user is above it, and nothing below it is handed out again.
    let reserved = c.conn.instance_table_name("mrsm_user_ids");
    assert!(
        c.conn
            .exec(&format!(
                "INSERT INTO {reserved} (user_id) VALUES ({})",
                highest + 5
            ))
            .await
            .is_some()
    );
    let mut after = User::new(&unique("after"), Some("test1234"));
    ok(c.conn.register_user(&mut after).await);
    let after_id = c
        .conn
        .get_user_id(&after)
        .await
        .unwrap()
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(after_id, highest + 6, "a spent reservation was handed out");

    // No ID left: refused, never an overflow.
    assert!(
        c.conn
            .exec(&format!(
                "INSERT INTO {reserved} (user_id) VALUES ({})",
                i64::MAX
            ))
            .await
            .is_some()
    );
    let mut overflow = User::new(&unique("overflow"), Some("test1234"));
    let refused = c.conn.register_user(&mut overflow).await;
    assert!(!refused.success);
    assert!(
        refused.message.contains("No user ID is left"),
        "{}",
        refused.message
    );
}

/// SQLite: a username held twice (rows from before the unique key) names nobody, and no new
/// user is registered until the duplicates are repaired.
#[tokio::test]
async fn sqlite_duplicate_usernames_fail_closed() {
    let Some(c) = ctx("sqlite", "dupe_names").await else {
        return;
    };
    let mut dupe = User::new(&unique("dupe"), Some("test1234"));
    ok(c.conn.register_user(&mut dupe).await);
    let users = c.conn.instance_table_name("mrsm_users");
    assert!(
        c.conn
            .exec("DROP INDEX IF EXISTS uq_mrsm_users_username")
            .await
            .is_some()
    );
    assert!(
        c.conn
            .exec(&format!(
                "INSERT INTO {users} (user_id, username, password_hash) VALUES (9000, '{}', 'x')",
                dupe.username
            ))
            .await
            .is_some()
    );
    let looked_up = c.conn.get_user_id(&User::new(&dupe.username, None)).await;
    assert!(looked_up.is_err(), "{looked_up:?}");
    let mut next = User::new(&unique("next"), Some("test1234"));
    let refused = c.conn.register_user(&mut next).await;
    assert!(!refused.success);
    assert!(
        refused.message.contains("mrsm doctor"),
        "{}",
        refused.message
    );
}

/// SQLite: a user registered before ID reservations existed has no reservation; deleting them
/// reserves their ID first, so the next user never takes it and a key written late for the
/// deleted user names nobody.
#[tokio::test]
async fn sqlite_a_legacy_users_id_stays_reserved_after_deletion() {
    let Some(c) = ctx("sqlite", "legacy_ids").await else {
        return;
    };
    let mut first = User::new(&unique("first"), Some("test1234"));
    ok(c.conn.register_user(&mut first).await);
    let first_id = c
        .conn
        .get_user_id(&first)
        .await
        .unwrap()
        .unwrap()
        .as_i64()
        .unwrap();
    // Written before reservations: above every reserved ID, with none of its own.
    let users = c.conn.instance_table_name("mrsm_users");
    let (legacy_id, legacy_name) = (first_id + 1, unique("legacy"));
    assert!(
        c.conn
            .exec(&format!(
                "INSERT INTO {users} (user_id, username, password_hash) \
                 VALUES ({legacy_id}, '{legacy_name}', 'x')"
            ))
            .await
            .is_some()
    );
    ok(c.conn.delete_user(&User::new(&legacy_name, None)).await);

    let mut next = User::new(&unique("next"), Some("test1234"));
    ok(c.conn.register_user(&mut next).await);
    let next_id = c
        .conn
        .get_user_id(&next)
        .await
        .unwrap()
        .unwrap()
        .as_i64()
        .unwrap();
    assert!(
        next_id > legacy_id,
        "the new user took the deleted legacy user's ID {legacy_id}"
    );

    use meerschaum_connector::pipe_ops;
    let late_id = uuid::Uuid::new_v4();
    let doc: pipe_ops::Doc = [
        ("id".to_string(), KValue::Uuid(late_id)),
        (
            "user_id".to_string(),
            pipe_ops::user_id_value(&meerschaum_core::Value::Int(legacy_id)),
        ),
        ("creation".to_string(), pipe_ops::utc_value(Utc::now())),
        ("label".to_string(), KValue::Str(unique("late"))),
        ("is_valid".to_string(), KValue::Bool(true)),
        ("secret_hash".to_string(), KValue::Str("x".into())),
    ]
    .into_iter()
    .collect();
    let tokens = InstanceConnector::get_tokens_pipe(&c.conn);
    ok(pipe_ops::sync_docs(&c.conn, &tokens, vec![doc], false).await);
    let late_owner = InstanceConnector::get_token_model(&c.conn, &late_id)
        .await
        .unwrap()
        .expect("the late key was written")
        .user_id
        .unwrap();
    assert!(
        InstanceConnector::get_username(&c.conn, &late_owner)
            .await
            .unwrap()
            .is_none(),
        "the late key names a user"
    );
}

/// SQLite: when the deleted user's ID cannot be reserved, nothing is deleted.
#[tokio::test]
async fn sqlite_a_deletion_that_cannot_reserve_the_id_deletes_nothing() {
    let Some(c) = ctx("sqlite", "unreserved").await else {
        return;
    };
    let mut kept = User::new(&unique("kept"), Some("test1234"));
    ok(c.conn.register_user(&mut kept).await);
    let key = token_for(&c, &kept).await;
    let reserved = c.conn.instance_table_name("mrsm_user_ids");
    assert!(
        c.conn
            .exec(&format!(
                "CREATE TRIGGER refuse_reservations BEFORE INSERT ON {reserved} \
                 BEGIN SELECT RAISE(ABORT, 'refused'); END"
            ))
            .await
            .is_some()
    );
    // The registration already reserved this user's ID: `INSERT OR IGNORE` never fires the
    // trigger then. Free it, so the deletion has to reserve it again.
    let kept_id = c.conn.get_user_id(&kept).await.unwrap().unwrap();
    assert!(
        c.conn
            .exec(&format!(
                "DELETE FROM {reserved} WHERE user_id = {}",
                kept_id.as_i64().unwrap()
            ))
            .await
            .is_some()
    );
    let refused = c.conn.delete_user(&kept).await;
    assert!(!refused.success, "{}", refused.message);
    assert!(c.conn.get_user_id(&kept).await.unwrap().is_some());
    assert!(
        InstanceConnector::get_token_model(&c.conn, &key.id.unwrap())
            .await
            .unwrap()
            .is_some(),
        "a refused deletion deleted the user's keys"
    );
}
