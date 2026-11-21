//! `register_first_user` on Valkey: the check that no user is registered and the username claim
//! are one script, so of concurrent first registrations from independent connectors exactly one
//! becomes the administrator and the others are refused cleanly, leaving nothing behind. A
//! registration that never claimed its username does not count as a user; any row the check
//! cannot read does. A failed script writes nothing and a retry succeeds.
//!
//! In memory always; on a server only with `MRSM_VALKEY_FIRST_USER_TEST_URL` naming a database
//! without users (the check reads every user of the database, so it never shares one).

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector_valkey::keys::{USERS_TABLE, quote_table};
use meerschaum_connector_valkey::{
    FaultHook, FredBackend, MemoryBackend, ValkeyBackend, ValkeyConnector, WriteOp,
};
use meerschaum_core::Map;
use meerschaum_core::users::User;
use meerschaum_kernel::value::Value as KValue;

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

fn admin(name: &str) -> User {
    let mut user = User::new(name, Some("password"));
    user.user_type = Some("admin".into());
    user
}

/// Two independent connectors over one in-memory backend.
fn memory_pair() -> (ValkeyConnector, ValkeyConnector) {
    let memory = Arc::new(MemoryBackend::new());
    (
        ValkeyConnector::with_backend("first_a", Map::new(), memory.clone()),
        ValkeyConnector::with_backend("first_b", Map::new(), memory),
    )
}

async fn live_pair() -> Option<(ValkeyConnector, ValkeyConnector)> {
    let Ok(url) = std::env::var("MRSM_VALKEY_FIRST_USER_TEST_URL") else {
        eprintln!("SKIPPED: set MRSM_VALKEY_FIRST_USER_TEST_URL to a database without users.");
        return None;
    };
    let mut conns = Vec::new();
    for label in ["first_a", "first_b"] {
        let backend = tokio::time::timeout(
            Duration::from_secs(3),
            FredBackend::connect(&url, Some(Duration::from_secs(3))),
        )
        .await
        .expect("connects in time")
        .expect("connects");
        conns.push(ValkeyConnector::with_backend(
            label,
            Map::new(),
            Arc::new(backend),
        ));
    }
    let b = conns.pop().unwrap();
    let a = conns.pop().unwrap();
    assert!(
        a.backend()
            .smembers(&quote_table(USERS_TABLE))
            .await
            .unwrap()
            .is_empty(),
        "MRSM_VALKEY_FIRST_USER_TEST_URL must name a database without users"
    );
    Some((a, b))
}

/// The `mrsm_users` rows (document keys; deleted rows leave stale set members, as in v4).
async fn rows(conn: &ValkeyConnector) -> usize {
    conn.backend()
        .keys(&format!("{}:indices:*", quote_table(USERS_TABLE)))
        .await
        .unwrap()
        .len()
}

/// Eight first registrations at once: one administrator, seven clean refusals, one row.
async fn check_one_first_user(a: &ValkeyConnector, b: &ValkeyConnector) -> String {
    let names: Vec<String> = (0..8).map(|i| unique(&format!("first{i}"))).collect();
    // Concurrent tasks over the two connectors (separate connections on a server).
    let mut tasks = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let conn = if i % 2 == 0 { a.clone() } else { b.clone() };
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            InstanceConnector::register_first_user(&conn, &mut admin(&name)).await
        }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    let won: Vec<&String> = names
        .iter()
        .zip(&outcomes)
        .filter(|(_, o)| matches!(o, Ok(true)))
        .map(|(n, _)| n)
        .collect();
    let lost = outcomes.iter().filter(|o| matches!(o, Ok(false))).count();
    assert_eq!(won.len(), 1, "{outcomes:?}");
    assert_eq!(lost, 7, "no registration may fail: {outcomes:?}");
    let winner = won[0].clone();
    assert_eq!(a.valkey_get_users().await.unwrap(), vec![winner.clone()]);
    assert_eq!(
        a.valkey_get_user_type(&User::new(&winner, None))
            .await
            .unwrap()
            .as_deref(),
        Some("admin")
    );
    assert_eq!(rows(a).await, 1, "a refused registration left its row");

    // Once a user exists, nothing more is promoted and nothing is kept.
    let late = unique("late");
    let outcome = InstanceConnector::register_first_user(b, &mut admin(&late)).await;
    assert!(matches!(outcome, Ok(false)), "{outcome:?}");
    assert_eq!(
        a.valkey_get_user_id(&User::new(&late, None)).await.unwrap(),
        None
    );
    assert_eq!(rows(a).await, 1);
    winner
}

/// A registration that never claimed its username is no user; a row the check cannot read is.
async fn check_unclaimed_and_unreadable_rows(conn: &ValkeyConnector) {
    let ghost: Doc = [
        ("user_id".to_string(), KValue::Str(unique("ghostid"))),
        ("username".to_string(), KValue::Str(unique("ghost"))),
    ]
    .into_iter()
    .collect();
    assert!(
        pipe_ops::sync_docs(conn, &conn.valkey_users_pipe(), vec![ghost], false)
            .await
            .success
    );
    let name = unique("first");
    let outcome = InstanceConnector::register_first_user(conn, &mut admin(&name)).await;
    assert!(matches!(outcome, Ok(true)), "{outcome:?}");
    assert!(
        conn.valkey_delete_user(&User::new(&name, None))
            .await
            .success
    );

    // The deleted user's stale member is no user: a first registration still succeeds.
    let again = unique("again");
    let outcome = InstanceConnector::register_first_user(conn, &mut admin(&again)).await;
    assert!(matches!(outcome, Ok(true)), "{outcome:?}");
    assert!(
        conn.valkey_delete_user(&User::new(&again, None))
            .await
            .success
    );

    // A member that is not a user row: counted as a user, never skipped.
    let users = quote_table(USERS_TABLE);
    for member in ["unreadable", r#"{"ix":"other-_1"}"#, r#"{"other":1}"#] {
        conn.backend().sadd(&users, member).await.unwrap();
        let refused = unique("refused");
        let outcome = InstanceConnector::register_first_user(conn, &mut admin(&refused)).await;
        assert!(matches!(outcome, Ok(false)), "{member}: {outcome:?}");
        assert_eq!(
            conn.valkey_get_user_id(&User::new(&refused, None))
                .await
                .unwrap(),
            None
        );
        conn.backend().srem(&users, member).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_first_user_in_memory() {
    let (a, b) = memory_pair();
    check_one_first_user(&a, &b).await;
    let conn = ValkeyConnector::in_memory("first");
    check_unclaimed_and_unreadable_rows(&conn).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_first_user_live() {
    let Some((a, b)) = live_pair().await else {
        return;
    };
    let winner = check_one_first_user(&a, &b).await;
    assert!(
        a.valkey_delete_user(&User::new(&winner, None))
            .await
            .success
    );
    check_unclaimed_and_unreadable_rows(&a).await;
    check_a_failed_script_leaves_no_claim(a.backend().as_ref()).await;
}

/// A script the server stops part-way (a score it refuses, after a field was written) never
/// leaves its claim: the claim is the last write, so the lost-reply recovery never takes a
/// partial write for a registered user. Only a server fails a script part-way.
async fn check_a_failed_script_leaves_no_claim(backend: &dyn ValkeyBackend) {
    let claim = unique("_mrsm_rs_test_claim");
    let (field, scores) = (format!("{claim}:field"), format!("{claim}:scores"));
    let ops = [
        WriteOp::Set {
            key: field.clone(),
            value: "written".into(),
        },
        WriteOp::ZAdd {
            key: scores.clone(),
            member: "m".into(),
            score: f64::NAN,
        },
    ];
    let users = quote_table(USERS_TABLE);
    assert!(
        backend
            .claim_first_user(&users, &claim, "1", &ops)
            .await
            .is_err()
    );
    assert_eq!(backend.get(&claim).await.unwrap(), None, "claim_first_user");
    assert!(backend.claim_and_write(&claim, "1", &ops).await.is_err());
    assert_eq!(backend.get(&claim).await.unwrap(), None, "claim_and_write");
    backend.del(&[claim, field, scores]).await.unwrap();
}

#[tokio::test]
async fn a_failed_first_registration_writes_nothing_and_a_retry_succeeds() {
    let backend = Arc::new(MemoryBackend::new());
    let conn = ValkeyConnector::with_backend("first", Map::new(), backend.clone());
    let fault: FaultHook = Arc::new(|cmd: &str, _key: &str| {
        (cmd == "EVAL").then(|| "injected failure: EVAL".to_string())
    });
    let name = unique("first");
    backend.set_fault_hook(Some(fault));
    let failed = InstanceConnector::register_first_user(&conn, &mut admin(&name)).await;
    backend.set_fault_hook(None);
    assert!(failed.is_err(), "{failed:?}");
    assert!(
        backend
            .keys("*")
            .await
            .unwrap()
            .iter()
            .all(|k| !k.starts_with("mrsm_user:"))
    );
    assert_eq!(rows(&conn).await, 0, "the failed attempt left its row");

    let invalid = InstanceConnector::register_first_user(&conn, &mut admin("no:colons")).await;
    assert!(invalid.is_err(), "{invalid:?}");
    assert_eq!(rows(&conn).await, 0);

    let won = InstanceConnector::register_first_user(&conn, &mut admin(&name)).await;
    assert!(matches!(won, Ok(true)), "{won:?}");
    assert_eq!(conn.valkey_get_users().await.unwrap(), vec![name]);
}
