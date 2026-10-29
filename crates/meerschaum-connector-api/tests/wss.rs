//! `monitor_logs` over `wss://`: a local self-signed TLS websocket server. A connector that
//! verifies certificates refuses it; `verify: false` (the connector attribute) streams the logs.
//! The scheme and connector selection (`ws://` takes no TLS connector, `wss://` takes rustls) is
//! unit-tested in `client::wss`.
#![cfg(feature = "client")]

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use meerschaum_connector_api::client::{APIConnector, LogsMonitor};
use meerschaum_connector_api::routes::JOBS_STOP_MESSAGE;
use meerschaum_core::{Map, SuccessTuple, Value};
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

/// A TLS websocket server (a self-signed certificate for `localhost`) that accepts one client,
/// reads its token and plays one log line plus the job's stop message. Returns its port.
async fn serve_one() -> u16 {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = certified.cert.der().clone();
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let config =
        rustls::ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        // A refused handshake (the verifying client) just ends this connection.
        while let Ok((tcp, _)) = listener.accept().await {
            let Ok(tls) = acceptor.accept(tcp).await else {
                continue;
            };
            let Ok(mut ws) = tokio_tungstenite::accept_async(tls).await else {
                continue;
            };
            let _token = ws.next().await;
            let _ = ws.send(Message::text("a log line")).await;
            let _ = ws.send(Message::text(JOBS_STOP_MESSAGE)).await;
            let _ = ws.send(Message::text(r#"[true, "done"]"#)).await;
            let _ = ws.next().await;
        }
    });
    port
}

fn connector(port: u16, verify: Option<bool>) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("protocol".into(), Value::Str("https".into()));
    attrs.insert("host".into(), Value::Str("localhost".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    if let Some(verify) = verify {
        attrs.insert("verify".into(), Value::Bool(verify));
    }
    APIConnector::new(Some("wss_fixture"), attrs).unwrap()
}

async fn monitor(conn: &APIConnector) -> Result<(Vec<String>, Option<SuccessTuple>), String> {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let result: Arc<Mutex<Option<SuccessTuple>>> = Arc::new(Mutex::new(None));
    let (l2, r2) = (lines.clone(), result.clone());
    conn.monitor_logs(
        "job",
        LogsMonitor {
            on_line: Box::new(move |line| l2.lock().unwrap().push(line)),
            on_input: None,
            on_stop: Some(Box::new(move |r| *r2.lock().unwrap() = Some(r))),
            stop_on_exit: true,
            strip_timestamps: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let lines = lines.lock().unwrap().clone();
    let result = result.lock().unwrap().clone();
    Ok((lines, result))
}

#[test]
fn https_connectors_use_a_wss_url() {
    assert!(
        connector(1, None)
            .logs_websocket_url("job")
            .starts_with("wss://localhost:1/logs/job/ws")
    );
}

#[tokio::test]
async fn verify_false_streams_logs_over_wss() {
    let port = serve_one().await;
    let (lines, result) = monitor(&connector(port, Some(false))).await.unwrap();
    assert_eq!(lines, vec!["a log line".to_string()]);
    let result = result.expect("the job's stop message");
    assert!(result.success, "{result:?}");
}

#[tokio::test]
async fn a_verifying_connector_refuses_a_self_signed_certificate() {
    let port = serve_one().await;
    let err = monitor(&connector(port, None)).await.unwrap_err();
    assert!(!err.is_empty());
    // Nothing was streamed: the handshake failed before the first message.
    assert!(
        err.to_lowercase().contains("cert") || err.to_lowercase().contains("tls"),
        "{err}"
    );
}
