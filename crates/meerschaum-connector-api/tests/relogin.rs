//! A logged-in JWT the server refuses is replaced by one fresh login: a safe request is sent
//! again once, an unsafe one is not, and refusals within `RELOGIN_INTERVAL` never become a
//! storm of logins. API keys are never retried.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{Method, RequestOptions};
use meerschaum_core::{Map, Value};

fn json(status: u16, body: &str) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: body.as_bytes().to_vec(),
    }
}

fn login(token: &str) -> Canned {
    json(
        200,
        &format!(
            r#"{{"access_token": "{token}", "token_type": "bearer", "expires": "2099-01-01T00:00:00"}}"#
        ),
    )
}

fn connector(port: u16, api_key: bool) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    if api_key {
        attrs.insert("api_key".into(), Value::Str("mrsm-key:abc".into()));
    } else {
        attrs.insert("username".into(), Value::Str("alice".into()));
        attrs.insert("password".into(), Value::Str("pw".into()));
    }
    APIConnector::new(Some("relogin"), attrs).unwrap()
}

fn authorization(r: &common::Recorded) -> String {
    r.header("authorization").unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_jwt_is_replaced_by_one_fresh_login() {
    let server = MockServer::start().await;
    let conn = connector(server.port, false);

    // A safe request: refused, one fresh login, sent again with the new JWT.
    server.push_response(login("first"));
    server.push_response(json(401, r#"{"detail": "Invalid credentials"}"#));
    server.push_response(login("second"));
    server.push_response(json(200, "true"));
    let r = conn
        .make_request(Method::Get, "/x", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let seen: Vec<(String, String, String)> = requests
        .iter()
        .map(|r| (r.method.clone(), r.target.clone(), authorization(r)))
        .collect();
    assert_eq!(seen.len(), 4, "{seen:?}");
    assert_eq!(seen[1].2, "Bearer first");
    assert!(seen[2].1.starts_with("/login"), "{seen:?}");
    assert_eq!(seen[3].2, "Bearer second");

    // Refused again within the interval: no second login, no replay, and an unsafe request is
    // never sent twice.
    server.push_response(json(401, r#"{"detail": "Invalid credentials"}"#));
    let r = conn
        .make_request(Method::Post, "/y", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.status, 401);
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsafe_request_is_not_sent_again() {
    let server = MockServer::start().await;
    let conn = connector(server.port, false);
    server.push_response(login("first"));
    server.push_response(json(401, r#"{"detail": "Invalid credentials"}"#));
    let r = conn
        .make_request(Method::Post, "/sync", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.status, 401);
    // The next request logs in again.
    server.push_response(login("second"));
    server.push_response(json(200, "true"));
    let r = conn
        .make_request(Method::Post, "/sync", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let posts: Vec<&common::Recorded> = requests.iter().filter(|r| r.target == "/sync").collect();
    assert_eq!(posts.len(), 2);
    assert_eq!(authorization(posts[0]), "Bearer first");
    assert_eq!(authorization(posts[1]), "Bearer second");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_api_key_is_never_retried() {
    let server = MockServer::start().await;
    let conn = connector(server.port, true);
    server.push_response(json(401, r#"{"detail": "Invalid or revoked token."}"#));
    let r = conn
        .make_request(Method::Get, "/x", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.status, 401);
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    assert_eq!(requests.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_share_one_login() {
    let server = MockServer::start().await;
    let conn = std::sync::Arc::new(connector(server.port, false));
    server.push_response(login("only"));
    for _ in 0..5 {
        server.push_response(json(200, "true"));
    }
    let mut handles = Vec::new();
    for _ in 0..5 {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            conn.make_request(Method::Get, "/x", RequestOptions::default())
                .await
                .unwrap()
                .status
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), 200);
    }
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let logins = requests
        .iter()
        .filter(|r| r.target.starts_with("/login"))
        .count();
    assert_eq!(logins, 1, "one login for concurrent requests");
    assert!(
        requests
            .iter()
            .filter(|r| r.target == "/x")
            .all(|r| authorization(r) == "Bearer only")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_that_no_longer_work_are_not_tried_per_request() {
    let server = MockServer::start().await;
    let conn = connector(server.port, false);
    server.push_response(json(401, r#"{"detail": "Invalid credentials"}"#));
    server.push_response(json(401, r#"{"detail": "Not authenticated."}"#));
    server.push_response(json(401, r#"{"detail": "Not authenticated."}"#));
    for _ in 0..2 {
        let r = conn
            .make_request(Method::Get, "/x", RequestOptions::default())
            .await
            .unwrap();
        assert_eq!(r.status, 401);
    }
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let logins = requests
        .iter()
        .filter(|r| r.target.starts_with("/login"))
        .count();
    assert_eq!(logins, 1, "a failed login is not tried again at once");
    // An explicit login still tries.
    server.push_response(login("again"));
    assert!(conn.login().await.success);
}

/// A server for the race of two refused requests: it holds both requests carrying the old JWT
/// until both have arrived, refuses both, counts logins (each answers the new JWT), and accepts
/// the new JWT. Routing by path and `Authorization`, so arrival order does not matter.
mod race {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Barrier;

    pub struct Server {
        pub port: u16,
        pub logins: Arc<AtomicUsize>,
        pub accepted: Arc<AtomicUsize>,
    }

    pub async fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let logins = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let (l, a) = (logins.clone(), accepted.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (l, a, b) = (l.clone(), a.clone(), barrier.clone());
                tokio::spawn(serve(stream, l, a, b));
            }
        });
        Server {
            port,
            logins,
            accepted,
        }
    }

    async fn serve(
        stream: TcpStream,
        logins: Arc<AtomicUsize>,
        accepted: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    ) {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            let target = line.split_whitespace().nth(1).unwrap_or("").to_string();
            let (mut length, mut authorization) = (0usize, String::new());
            loop {
                let mut header = String::new();
                if read.read_line(&mut header).await.unwrap_or(0) == 0 {
                    return;
                }
                let header = header.trim_end();
                if header.is_empty() {
                    break;
                }
                let (name, value) = header.split_once(':').unwrap_or((header, ""));
                match name.trim().to_ascii_lowercase().as_str() {
                    "content-length" => length = value.trim().parse().unwrap_or(0),
                    "authorization" => authorization = value.trim().to_string(),
                    _ => {}
                }
            }
            let mut body = vec![0u8; length];
            if read.read_exact(&mut body).await.is_err() {
                return;
            }
            let (status, reply) = if target.starts_with("/login") {
                logins.fetch_add(1, Ordering::SeqCst);
                (
                    200,
                    r#"{"access_token": "new", "token_type": "bearer", "expires": "2099-01-01T00:00:00"}"#,
                )
            } else if authorization == "Bearer old" {
                // Both requests carrying the old JWT are refused only once both have arrived.
                barrier.wait().await;
                (401, r#"{"detail": "Invalid credentials"}"#)
            } else if authorization == "Bearer new" {
                accepted.fetch_add(1, Ordering::SeqCst);
                (200, "true")
            } else {
                (401, r#"{"detail": "Not authenticated."}"#)
            };
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
                reply.len()
            );
            if write.write_all(response.as_bytes()).await.is_err() {
                return;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneously_refused_requests_share_one_fresh_login() {
    let server = race::start().await;
    let conn = std::sync::Arc::new(connector(server.port, false));
    conn.set_token(
        Some("old".into()),
        chrono::NaiveDate::from_ymd_opt(2099, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0),
    )
    .await;
    let mut handles = Vec::new();
    for path in ["/a", "/b"] {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            conn.make_request(Method::Get, path, RequestOptions::default())
                .await
                .unwrap()
                .status
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), 200);
    }
    use std::sync::atomic::Ordering;
    assert_eq!(server.logins.load(Ordering::SeqCst), 1, "one fresh login");
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        2,
        "both sent again with it"
    );
}
