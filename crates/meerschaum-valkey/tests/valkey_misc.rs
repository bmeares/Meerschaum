//! Offline tests (in-memory backend) for the parts of the connector the Python fixture does not
//! cover: URI building, `push_df` / `read`, `fetch`, tokens through the generic tokens pipe, and
//! the §5.8 lease pair. `leases_live` repeats the lease checks against a real server when one is
//! reachable (`MRSM_VALKEY_TEST_URL`, default `redis://localhost:6399/12`).

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::pipe_ops::Doc;
use meerschaum_connector::traits::{DataQuery, DateBound, InstanceConnector};
use meerschaum_core::users::{Expiration, Token, User};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::value::{Value as KValue, parse_datetime_value};
use meerschaum_valkey::{FredBackend, MemoryBackend, ValkeyConnector};

fn attrs(pairs: &[(&str, Value)]) -> Map {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn uri_from_attributes() {
    let conn = ValkeyConnector::with_backend(
        "x",
        attrs(&[
            ("host", Value::Str("localhost".into())),
            ("port", Value::Int(6399)),
            ("db", Value::Int(11)),
        ]),
        Arc::new(MemoryBackend::new()),
    );
    assert_eq!(conn.uri(), "valkey://localhost:6399/11");
    assert_eq!(conn.connection_url(), "redis://localhost:6399/11");
    let conn = ValkeyConnector::with_backend(
        "x",
        attrs(&[
            ("username", Value::Str("default".into())),
            ("password", Value::Str("p@ss word".into())),
            ("host", Value::Str("h".into())),
            ("port", Value::Int(6379)),
            ("db", Value::Int(0)),
            ("socket_timeout", Value::Int(300)),
        ]),
        Arc::new(MemoryBackend::new()),
    );
    assert_eq!(
        conn.uri(),
        "valkey://default:p%40ss+word@h:6379/0?timeout=300s"
    );
    assert_eq!(
        conn.connection_url(),
        "redis://default:p%40ss+word@h:6379/0"
    );
    assert_eq!(conn.to_string(), "valkey:x");
}

#[tokio::test]
async fn push_and_read_docs() {
    let conn = ValkeyConnector::in_memory("x");
    let ts = |s: &str| KValue::Datetime(parse_datetime_value(s).unwrap());
    let docs: Vec<Doc> = vec![
        [
            ("dt".to_string(), ts("2024-01-01T00:00:00+00:00")),
            ("v".to_string(), KValue::Int(1)),
        ]
        .into_iter()
        .collect(),
        [
            ("dt".to_string(), ts("2024-01-02T00:00:00+00:00")),
            ("v".to_string(), KValue::Int(2)),
        ]
        .into_iter()
        .collect(),
    ];
    assert_eq!(
        conn.push_docs(&docs, "events", Some("dt")).await.unwrap(),
        2
    );
    // Re-pushing the same documents adds nothing.
    assert_eq!(conn.push_docs(&docs, "events", None).await.unwrap(), 0);
    assert_eq!(
        conn.get("events:datetime_column").await.unwrap().as_deref(),
        Some("dt")
    );
    let begin = ts("2024-01-02T00:00:00+00:00");
    let read = conn.read_docs("events", Some(&begin), None).await.unwrap();
    assert_eq!(read.len(), 1);
    let df = conn
        .read("events", None, None, None, None, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(df.height(), 2);

    let set_docs: Vec<Doc> = vec![
        [("a".to_string(), KValue::Str("x".into()))]
            .into_iter()
            .collect(),
    ];
    assert_eq!(conn.push_docs(&set_docs, "plain", None).await.unwrap(), 1);
    assert_eq!(conn.backend().key_type("plain").await.unwrap(), "set");

    assert_eq!(
        conn.push_hash_docs_to_list(&set_docs, "lst").await.unwrap(),
        2
    );
    let back = conn.read_docs_from_list("lst", None, None).await.unwrap();
    assert_eq!(back[0]["a"], "x");

    conn.drop_table("events").await.unwrap();
    assert_eq!(conn.backend().exists("events").await.unwrap(), 0);
    assert_eq!(conn.get("events:datetime_column").await.unwrap(), None);
}

#[tokio::test]
async fn fetch_source_keys() {
    let conn = ValkeyConnector::in_memory("src");
    conn.backend()
        .zadd(
            "readings",
            r#"{"dt":"2024-01-01T00:00:00+00:00","v":1}"#,
            1704067200.0,
        )
        .await
        .unwrap();
    conn.backend()
        .zadd(
            "readings",
            r#"{"dt":"2024-01-03T00:00:00+00:00","v":3}"#,
            1704240000.0,
        )
        .await
        .unwrap();
    conn.set("scalar", "42").await.unwrap();
    let mut params = Map::new();
    params.insert(
        "valkey".into(),
        Value::Map(attrs(&[("key", Value::Str("readings".into()))])),
    );
    let pipe = Pipe::builder("valkey:src", "readings")
        .parameters(params)
        .build()
        .unwrap();
    let begin = DateBound::parse("2024-01-02T00:00:00+00:00");
    let docs = conn
        .fetch_docs(
            &pipe,
            &DataQuery {
                begin,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert!(matches!(docs[0]["v"], KValue::Int(3)));

    let mut params = Map::new();
    params.insert(
        "valkey".into(),
        Value::Map(attrs(&[("key", Value::Str("scalar".into()))])),
    );
    let pipe = Pipe::builder("valkey:src", "scalar")
        .parameters(params)
        .build()
        .unwrap();
    let docs = conn.fetch_docs(&pipe, &DataQuery::default()).await.unwrap();
    assert!(matches!(&docs[0]["scalar"], KValue::Str(s) if s == "42"));
}

#[tokio::test]
async fn tokens_through_the_tokens_pipe() {
    let conn = ValkeyConnector::in_memory("tok");
    let mut user = User::new("alice", Some("hunter2"));
    user.user_type = Some("admin".into());
    user.email = Some("a@example.com".into());
    assert!(conn.register_user(&mut user).await.success);
    let now = chrono::Utc::now();
    let mut token = Token::new(Some("device"), Expiration::Never, None, 366, now);
    token.user = Some(user.clone());
    let res = conn.register_token(&mut token).await;
    assert!(res.success, "{}", res.message);
    let id = token.id.unwrap();
    assert!(conn.token_exists(&id).await.unwrap());
    let model = conn.get_token_model(&id).await.unwrap().expect("model");
    assert_eq!(model.label.as_deref(), Some("device"));
    assert!(model.is_valid);
    assert_eq!(
        model.user_id,
        Some(Value::Str(
            user.user_id
                .clone()
                .map(|v| meerschaum_core::pyfmt::py_str(&v))
                .unwrap()
        ))
    );
    let hash = conn.get_token_secret_hash(&id).await.unwrap().unwrap();
    assert!(
        meerschaum_core::users::verify_password(token.secret.as_deref().unwrap(), &hash).unwrap()
    );
    assert_eq!(conn.get_token_scopes(&id).await.unwrap(), token.scopes);
    let tokens = conn.get_tokens(Some(&user), None, None).await.unwrap();
    assert_eq!(tokens.len(), 1);

    let res = conn.invalidate_token(&token).await;
    assert!(res.success, "{}", res.message);
    assert!(!conn.get_token_model(&id).await.unwrap().unwrap().is_valid);

    let res = conn.delete_token(&token).await;
    assert!(res.success, "{}", res.message);
    // PARITY: `clear_pipe` leaves the set member behind but deletes the document, so the token
    // no longer reads back.
    assert!(!conn.token_exists(&id).await.unwrap());
}

async fn check_leases(conn: &ValkeyConnector) {
    let pipe = Pipe::builder("plugin:noaa", "weather")
        .instance("valkey:lease")
        .build()
        .unwrap();
    let ttl = Duration::from_millis(400);
    assert!(conn.acquire_pipe_lease(&pipe, "a", ttl).await.unwrap());
    assert!(!conn.acquire_pipe_lease(&pipe, "b", ttl).await.unwrap());
    // The holder extends its own lease.
    assert!(conn.acquire_pipe_lease(&pipe, "a", ttl).await.unwrap());
    assert!(!conn.release_pipe_lease(&pipe, "b").await.unwrap());
    assert!(conn.release_pipe_lease(&pipe, "a").await.unwrap());
    assert!(conn.acquire_pipe_lease(&pipe, "b", ttl).await.unwrap());
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Expired: another holder may take it.
    assert!(conn.acquire_pipe_lease(&pipe, "c", ttl).await.unwrap());
    assert_eq!(
        conn.get(&format!("mrsm_lease:{}", pipe.pipe_ref()))
            .await
            .unwrap()
            .as_deref(),
        Some("c")
    );
    assert!(conn.release_pipe_lease(&pipe, "c").await.unwrap());
}

#[tokio::test]
async fn leases_in_memory() {
    check_leases(&ValkeyConnector::in_memory("lease")).await;
}

#[tokio::test]
async fn leases_live() {
    let url = std::env::var("MRSM_VALKEY_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6399/12".to_string());
    let backend = match tokio::time::timeout(
        Duration::from_secs(3),
        FredBackend::connect(&url, Some(Duration::from_secs(3))),
    )
    .await
    {
        Ok(Ok(b)) => b,
        _ => {
            eprintln!("SKIPPED leases_live: no Valkey server reachable at {url}");
            return;
        }
    };
    let conn = ValkeyConnector::with_backend("lease", Map::new(), Arc::new(backend));
    check_leases(&conn).await;
}
