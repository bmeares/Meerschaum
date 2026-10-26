//! Registration is all-or-nothing: a failure at any write of `register_pipe` /
//! `valkey_register_user` leaves no claim and no row, and a retry registers exactly once.
//! Faults are injected through the in-memory backend's hook; the `_live` tests repeat what a
//! real server can reproduce (`MRSM_VALKEY_TEST_URL`, default `redis://localhost:6399/12`).

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::PipesKeysQuery;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector_valkey::keys::{USERS_TABLE, get_pipe_key, quote_table};
use meerschaum_connector_valkey::{
    FaultHook, FredBackend, MemoryBackend, ValkeyBackend, ValkeyConnector, WriteOp,
};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe};
use meerschaum_kernel::value::Value as KValue;

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

async fn live_backend() -> Option<Arc<dyn ValkeyBackend>> {
    let url = std::env::var("MRSM_VALKEY_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6399/12".to_string());
    match tokio::time::timeout(
        Duration::from_secs(3),
        FredBackend::connect(&url, Some(Duration::from_secs(3))),
    )
    .await
    {
        Ok(Ok(b)) => Some(Arc::new(b)),
        _ => {
            eprintln!("SKIPPED: no Valkey server reachable at {url}");
            None
        }
    }
}

/// Fail `command` on `key` (`*suffix` / `prefix*`: any key ending / starting with it).
fn fail_on(command: &'static str, key: String) -> FaultHook {
    Arc::new(move |cmd: &str, k: &str| {
        let hit = if let Some(suffix) = key.strip_prefix('*') {
            k.ends_with(suffix)
        } else if let Some(prefix) = key.strip_suffix('*') {
            k.starts_with(prefix)
        } else {
            k == key
        };
        (cmd == command && hit).then(|| format!("injected failure: {cmd} {k}"))
    })
}

async fn snapshot(backend: &MemoryBackend) -> Vec<String> {
    let mut keys = backend.keys("*").await.unwrap();
    keys.retain(|k| k != "mrsm_pipes:counter");
    keys
}

async fn pipes_rows(conn: &ValkeyConnector, metric: &str) -> usize {
    let query = PipesKeysQuery {
        metric_keys: vec![metric.to_string()],
        ..Default::default()
    };
    conn.fetch_pipes_keys(&query).await.unwrap().len()
}

#[tokio::test]
async fn a_failed_pipe_registration_leaves_nothing_and_a_retry_registers_once() {
    let backend = Arc::new(MemoryBackend::new());
    let conn = ValkeyConnector::with_backend("reg", Map::new(), backend.clone());
    // The pipes table, its datetime column key and the counter exist already.
    let other = Pipe::builder("plugin:reg", "other")
        .instance("valkey:reg")
        .build()
        .unwrap();
    assert!(conn.register_pipe(&other).await.success);

    let metric = unique("faulty");
    let pipe = Pipe::builder("plugin:reg", &metric)
        .instance("valkey:reg")
        .build()
        .unwrap();
    let pipe_key = get_pipe_key(pipe.connector_keys(), pipe.metric_key(), None);
    let faults = [
        fail_on("SET", pipe_key.clone()),
        fail_on("ZADD", quote_table("mrsm_pipes")),
        fail_on("SET", "*:datetime_column".into()),
        fail_on("SET", "*:parameters".into()),
    ];
    let before = snapshot(&backend).await;
    for fault in faults {
        backend.set_fault_hook(Some(fault));
        let res = conn.register_pipe(&pipe).await;
        backend.set_fault_hook(None);
        assert!(!res.success);
        assert!(res.message.contains("injected failure"), "{}", res.message);
        assert_eq!(snapshot(&backend).await, before);
        assert_eq!(conn.get_pipe_id(&pipe).await.unwrap(), None);
        assert_eq!(pipes_rows(&conn, &metric).await, 0);
    }

    let res = conn.register_pipe(&pipe).await;
    assert!(res.success, "{}", res.message);
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    assert_eq!(pipes_rows(&conn, &metric).await, 1);
    assert!(conn.get_pipe_parameters(&pipe).await.unwrap().is_some());
    let res = conn.register_pipe(&pipe).await;
    assert!(res.message.ends_with("is already registered."));
    assert_eq!(pipes_rows(&conn, &metric).await, 1);
}

#[tokio::test]
async fn a_failed_user_registration_leaves_nothing_and_a_retry_registers_once() {
    let backend = Arc::new(MemoryBackend::new());
    let conn = ValkeyConnector::with_backend("reg", Map::new(), backend.clone());
    let mut first = User::new("first", Some("password"));
    assert!(conn.valkey_register_user(&mut first).await.success);

    let username = unique("faulty");
    let claim_key = format!("mrsm_user:username:{username}:user_id");
    let faults = [
        // The row (its document key, then its index member).
        fail_on("SET", format!("{}:indices:*", quote_table(USERS_TABLE))),
        fail_on("SADD", quote_table(USERS_TABLE)),
        // The atomic claim and its field keys.
        fail_on("SET", claim_key.clone()),
        fail_on("SET", "*:password_hash".into()),
        fail_on("SET", "*:attributes".into()),
    ];
    let before = snapshot(&backend).await;
    for fault in faults {
        backend.set_fault_hook(Some(fault));
        let mut user = User::new(&username, Some("password"));
        let res = conn.valkey_register_user(&mut user).await;
        backend.set_fault_hook(None);
        assert!(!res.success);
        assert_eq!(snapshot(&backend).await, before, "{}", res.message);
        assert_eq!(
            conn.valkey_get_user_id(&User::new(&username, None)).await,
            None
        );
        assert_eq!(conn.valkey_get_users().await.unwrap(), ["first"]);
    }

    let mut user = User::new(&username, Some("password"));
    let res = conn.valkey_register_user(&mut user).await;
    assert!(res.success, "{}", res.message);
    let mut listed = conn.valkey_get_users().await.unwrap();
    listed.sort();
    let mut expected = vec!["first".to_string(), username.clone()];
    expected.sort();
    assert_eq!(listed, expected);
    let hash = conn
        .valkey_get_user_password_hash(&User::new(&username, None))
        .await
        .unwrap();
    assert!(meerschaum_core::users::verify_password("password", &hash).unwrap());
}

/// A row written by a registration that never claimed its username (a process that died in
/// between) is not listed, and the next registration of that username removes it.
async fn check_unclaimed_user_row(conn: &ValkeyConnector) {
    let username = unique("ghost");
    let ghost: Doc = [
        ("user_id".to_string(), KValue::Str(unique("ghostid"))),
        ("username".to_string(), KValue::Str(username.clone())),
    ]
    .into_iter()
    .collect();
    let users_pipe = conn.valkey_users_pipe();
    assert!(
        pipe_ops::sync_docs(conn, &users_pipe, vec![ghost], false)
            .await
            .success
    );
    assert!(!conn.valkey_get_users().await.unwrap().contains(&username));

    let mut user = User::new(&username, Some("password"));
    assert!(conn.valkey_register_user(&mut user).await.success);
    let listed = conn.valkey_get_users().await.unwrap();
    assert_eq!(listed.iter().filter(|u| **u == username).count(), 1);
    let mut params = Map::new();
    params.insert(
        "username".into(),
        meerschaum_core::Value::Str(username.clone()),
    );
    let rows = pipe_ops::get_docs(
        conn,
        &users_pipe,
        &meerschaum_connector::DataQuery {
            params: Some(params),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(conn.valkey_delete_user(&user).await.success);
}

/// `claim_and_write`: an existing claim writes nothing; a wrong-typed target is an error that
/// writes nothing (not even the claim); otherwise everything is written.
async fn check_claim_and_write(backend: &dyn ValkeyBackend) {
    let claim = unique("_mrsm_rs_test_claim");
    let (s, z, x) = (
        format!("{claim}:s"),
        format!("{claim}:z"),
        format!("{claim}:x"),
    );
    let ops = [
        WriteOp::Set {
            key: s.clone(),
            value: "v".into(),
        },
        WriteOp::ZAdd {
            key: z.clone(),
            member: "m".into(),
            score: 3.0,
        },
        WriteOp::SAdd {
            key: x.clone(),
            member: "m".into(),
        },
    ];
    backend.set(&x, "a string, not a set").await.unwrap();
    assert!(backend.claim_and_write(&claim, "1", &ops).await.is_err());
    assert_eq!(backend.get(&claim).await.unwrap(), None);
    assert_eq!(backend.get(&s).await.unwrap(), None);
    assert_eq!(backend.zcard(&z).await.unwrap(), 0);

    backend.del(std::slice::from_ref(&x)).await.unwrap();
    assert!(backend.claim_and_write(&claim, "1", &ops).await.unwrap());
    assert_eq!(backend.get(&claim).await.unwrap().as_deref(), Some("1"));
    assert_eq!(backend.get(&s).await.unwrap().as_deref(), Some("v"));
    assert_eq!(
        backend.zrange(&z, 0, -1, false).await.unwrap(),
        [("m".to_string(), 3.0)]
    );
    assert_eq!(backend.smembers(&x).await.unwrap(), ["m"]);

    let again = [WriteOp::Set {
        key: s.clone(),
        value: "w".into(),
    }];
    assert!(!backend.claim_and_write(&claim, "2", &again).await.unwrap());
    assert_eq!(backend.get(&claim).await.unwrap().as_deref(), Some("1"));
    assert_eq!(backend.get(&s).await.unwrap().as_deref(), Some("v"));
    backend.del(&[claim, s, z, x]).await.unwrap();
}

#[tokio::test]
async fn claims_and_unclaimed_rows_in_memory() {
    let conn = ValkeyConnector::in_memory("reg");
    check_claim_and_write(conn.backend().as_ref()).await;
    check_unclaimed_user_row(&conn).await;
}

#[tokio::test]
async fn claims_and_unclaimed_rows_live() {
    let Some(backend) = live_backend().await else {
        return;
    };
    check_claim_and_write(backend.as_ref()).await;
    let conn = ValkeyConnector::with_backend("reg", Map::new(), backend);
    check_unclaimed_user_row(&conn).await;
}
