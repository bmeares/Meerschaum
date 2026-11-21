//! Custom credentials bind the encoded request and do not fall back to password login.

use crate::common::{Canned, MockServer};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_connector_api::wire::QueryValue;
use meerschaum_core::{Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Authentication {
    calls: AtomicUsize,
    requests: Mutex<Vec<(String, String, Vec<u8>)>>,
}

impl RequestAuthentication for Authentication {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        let body = request
            .body
            .ok_or_else(|| ConnectorError::Invalid("A buffered body is required.".into()))?;
        self.requests.lock().unwrap().push((
            request.method.into(),
            request.path.into(),
            body.into(),
        ));
        Ok(format!(
            "Bearer request-{}",
            self.calls.fetch_add(1, Ordering::SeqCst)
        ))
    }
}

fn connector(port: u16, limit: usize, authentication: Arc<Authentication>) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port.into()));
    // A custom credential must never attempt to use these stored password credentials.
    attrs.insert("username".into(), Value::Str("unused".into()));
    attrs.insert("password".into(), Value::Str("unused".into()));
    APIConnector::new(Some("control"), attrs)
        .unwrap()
        .with_request_authentication(authentication, limit)
        .unwrap()
}

fn response(status: u16, body: &[u8]) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: body.into(),
    }
}

#[tokio::test]
async fn encoded_request_is_authenticated_without_login_or_stale_headers() {
    let server = MockServer::start().await;
    let authentication = Arc::new(Authentication::default());
    let conn = connector(server.port, 4096, authentication.clone());
    server.push_response(response(200, b"true"));
    let options = RequestOptions {
        query: vec![("filter".into(), QueryValue::opt(Some("a b")))],
        body: Body::Json("{\"value\": 3}".into()),
        headers: vec![
            ("Authorization".into(), "Bearer stale".into()),
            ("Cookie".into(), "session=stale".into()),
        ],
        ..Default::default()
    };
    assert_eq!(
        conn.make_request(Method::Post, "/jobs/a b", options)
            .await
            .unwrap()
            .status,
        200
    );
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/jobs/a%20b?filter=a+b");
    assert_eq!(request.header("authorization"), Some("Bearer request-0"));
    assert_eq!(request.header("cookie"), None);
    assert_eq!(
        *authentication.requests.lock().unwrap(),
        vec![(
            request.method.clone(),
            request.target.clone(),
            request.body.clone()
        )]
    );
}

#[tokio::test]
async fn refusals_never_log_in_or_replay_a_custom_authenticated_request() {
    for method in [Method::Get, Method::Post] {
        let server = MockServer::start().await;
        let authentication = Arc::new(Authentication::default());
        let conn = connector(server.port, 4096, authentication.clone());
        server.push_response(response(401, b"false"));
        assert_eq!(
            conn.make_request(method, "/operation", RequestOptions::default())
                .await
                .unwrap()
                .status,
            401
        );
        let (requests, _, _) = server.take();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].target, "/operation");
        assert_eq!(authentication.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn oversized_content_length_is_refused() {
    let server = MockServer::start().await;
    let conn = connector(server.port, 64, Arc::new(Authentication::default()));
    server.push_response(response(200, &[b'x'; 256]));
    let error = conn
        .get("/metadata", RequestOptions::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("size limit"), "{error}");
}

#[tokio::test]
async fn multipart_authentication_binds_the_encoder_boundary_and_file_bytes() {
    let server = MockServer::start().await;
    let authentication = Arc::new(Authentication::default());
    let conn = connector(server.port, 4096, authentication.clone());
    server.push_response(response(200, b"true"));
    let response = conn
        .post(
            "/plugins/test",
            RequestOptions {
                body: Body::File {
                    field: "file".into(),
                    file_name: "test.tar.gz".into(),
                    data: b"synthetic archive".to_vec(),
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    let (requests, _, _) = server.take();
    let request = &requests[0];
    assert!(
        request
            .header("content-type")
            .unwrap()
            .starts_with("multipart/form-data; boundary=")
    );
    assert!(
        request
            .body
            .windows(b"synthetic archive".len())
            .any(|w| w == b"synthetic archive")
    );
    assert_eq!(authentication.requests.lock().unwrap()[0].2, request.body);
}

#[tokio::test]
async fn credential_files_rotate_per_request_and_expiration_never_logs_in() {
    use meerschaum_connector_api::client::authentication::FileCredential;
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("credential.json");
    let write = |token: &str, expires_at| {
        meerschaum_core::team_scope::write_private(
            &path,
            &serde_json::to_string(&FileCredential {
                token: token.into(),
                expires_at,
            })
            .unwrap(),
        )
        .unwrap();
    };
    let expires = chrono::Utc::now().timestamp() + 60;
    write("first", expires);
    let attributes = Map::from_iter([
        ("host".into(), "127.0.0.1".into()),
        ("port".into(), Value::Int(server.port.into())),
        ("credential_file".into(), path.display().to_string().into()),
        ("username".into(), "must-not-login".into()),
        ("password".into(), "must-not-login".into()),
    ]);
    let api = APIConnector::new(Some("user-context"), attributes).unwrap();
    for token in ["first", "rotated"] {
        write(token, expires);
        server.push_response(response(200, b"true"));
        assert_eq!(
            api.get("/operation", RequestOptions::default())
                .await
                .unwrap()
                .status,
            200
        );
    }
    write("expired-secret-never-print", 0);
    let error = api
        .get("/operation", RequestOptions::default())
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("expired-secret"));
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].header("authorization"), Some("Bearer first"));
    assert_eq!(requests[1].header("authorization"), Some("Bearer rotated"));
}

#[cfg(unix)]
#[test]
fn permissive_or_symlink_credential_files_are_refused() {
    use meerschaum_connector_api::client::authentication::CredentialFile;
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("credential.json");
    std::fs::write(
        &path,
        format!(
            r#"{{"token":"secret","expires_at":{}}}"#,
            chrono::Utc::now().timestamp() + 60
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let request = Request {
        method: "GET",
        path: "/jobs",
        body: Some(&[]),
    };
    assert!(
        CredentialFile::new(path.clone())
            .authorization(request)
            .is_err()
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let link = root.path().join("link.json");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(CredentialFile::new(link).authorization(request).is_err());
}

#[tokio::test]
async fn asynchronous_log_channel_authenticates_and_forwards_only_requested_input() {
    use meerschaum_connector_api::client::LogMessage;
    let server = MockServer::start().await;
    server.push_ws_script(vec![
        ("send".into(), None),
        ("recv".into(), Some("line".into())),
        (
            "recv".into(),
            Some(meerschaum_connector_api::routes::JOBS_STDIN_MESSAGE.into()),
        ),
        ("send".into(), None),
        (
            "recv".into(),
            Some(meerschaum_connector_api::routes::JOBS_STOP_MESSAGE.into()),
        ),
        ("recv".into(), Some("[true, \"done\"]".into())),
        ("close".into(), None),
    ]);
    let authentication = Arc::new(Authentication::default());
    let api = connector(server.port, 4096, authentication);
    let mut channel = api.open_log_channel("owned-job").await.unwrap();
    let (requests, _, _) = server.take();
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer request-0")
    );
    assert!(channel.send_input("early".into()).await.is_err());
    assert!(
        matches!(channel.next().await.unwrap(), Some(LogMessage::Line(text)) if text == "line")
    );
    assert!(matches!(
        channel.next().await.unwrap(),
        Some(LogMessage::InputRequested)
    ));
    channel.send_input("answer".into()).await.unwrap();
    assert!(
        matches!(channel.next().await.unwrap(), Some(LogMessage::Stopped(result)) if result.success && result.message == "done")
    );
    channel.close().await;
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let (_, _, received) = server.take();
            if !received.is_empty() {
                break received;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(received[0], ["request-1", "answer"]);
}

#[tokio::test]
async fn oversized_executor_log_messages_are_refused() {
    let server = MockServer::start().await;
    server.push_ws_script(vec![
        ("send".into(), None),
        ("recv".into(), Some("x".repeat(8192))),
        ("close".into(), None),
    ]);
    let api = connector(server.port, 4096, Arc::new(Authentication::default()));
    let mut channel = api.open_log_channel("owned-job").await.unwrap();
    assert!(channel.next().await.is_err());
    channel.close().await;
}

#[tokio::test]
async fn redirect_responses_do_not_forward_credentials_or_request_another_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let mut count = 0;
        while let Ok(Ok((mut socket, _))) =
            tokio::time::timeout(std::time::Duration::from_millis(300), listener.accept()).await
        {
            count += 1;
            let mut data = [0; 8192];
            if socket.read(&mut data).await.unwrap() == 0 {
                break;
            }
            if count == 1 {
                socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            } else {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
            }
        }
        count
    });
    let conn = connector(port, 4096, Arc::new(Authentication::default()));
    assert_eq!(
        conn.get("/operation", RequestOptions::default())
            .await
            .unwrap()
            .status,
        302
    );
    assert_eq!(task.await.unwrap(), 1);
}
