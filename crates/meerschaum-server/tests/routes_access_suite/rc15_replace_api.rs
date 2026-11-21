//! Replacing syncs through the API (rc15 Q19): `POST /pipes/{ck}/{mk}/{lk}/data?replace=true`
//! takes a window bound that is not sent from the posted rows, as `pipe.sync(df, replace=True)`
//! does, so a replace never clears the whole table by omission; an empty body needs a window
//! (`400`). The client's direct `sync_pipe(..., replace=True)` (the Python
//! `APIConnector.sync_pipe` keyword arguments) follows the same rule.

use std::net::SocketAddr;
use std::sync::Arc;

use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use meerschaum_server::{AppState, ServerConfig};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";

struct Served {
    conn: Arc<SqlConnector>,
    client: APIConnector,
    port: u16,
    _dir: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve() -> Served {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("rc15_replace.db").display());
    let conn = Arc::new(SqlConnector::from_uri("rc15rp", &uri).await.unwrap());
    let mut cfg = ServerConfig::for_root(&conn.keys(), dir.path());
    cfg.no_auth = true;
    cfg.no_webterm = true;
    cfg.no_dash = true;
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = meerschaum_server::app(state);
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    let client = APIConnector::new(Some("rc15rp"), attrs).unwrap();
    Served {
        conn,
        client,
        port,
        _dir: dir,
        task,
    }
}

fn day(d: u32) -> KValue {
    KValue::Datetime(DateTimeValue::naive(
        chrono::NaiveDate::from_ymd_opt(2024, 1, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    ))
}

fn pipe(conn: &SqlConnector, metric: &str) -> Pipe {
    let params = Value::from_json_str(r#"{"columns": {"datetime": "ts", "id": "id"}}"#)
        .unwrap()
        .to_map();
    Pipe::builder("plugin:rc15rp", metric)
        .instance(&conn.keys())
        .parameters(params)
        .build()
        .unwrap()
}

async fn frame(conn: &SqlConnector, pipe: &Pipe, days: &[(u32, i64)]) -> Frame {
    let docs: Vec<Doc> = days
        .iter()
        .map(|(d, v)| {
            [
                ("ts", day(*d)),
                ("id", KValue::Int(1)),
                ("v", KValue::Int(*v)),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
        })
        .collect();
    let ctx = pipe_ops::dtypes_context(conn, pipe, pipe.parameters())
        .await
        .unwrap();
    pipe_enforce_dtypes(PipeInput::Docs(docs), &ctx).unwrap()
}

async fn values(conn: &SqlConnector, pipe: &Pipe) -> Vec<i64> {
    let df = conn
        .get_pipe_data(pipe, &DataQuery::default())
        .await
        .unwrap()
        .expect("rows");
    let mut v: Vec<i64> = pipe_ops::frame_to_docs(&df)
        .iter()
        .filter_map(|d| match d.get("v") {
            Some(KValue::Int(i)) => Some(*i),
            Some(KValue::Float(f)) => Some(*f as i64),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

/// A raw `POST` over the loopback socket: `(status, body)`.
async fn post(port: u16, path: &str, body: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_without_bounds_replaces_only_the_rows_window() {
    let served = serve().await;
    let conn = &served.conn;
    let pipe = pipe(conn, "replace_api");
    let all: Vec<(u32, i64)> = (1..=5).map(|d| (d, d as i64)).collect();
    let res = served
        .client
        .sync_pipe(
            &pipe,
            frame(conn, &pipe, &all).await,
            &SyncOptions::default(),
        )
        .await;
    assert!(res.success, "{}", res.message);

    // `APIConnector.sync_pipe(pipe, df, replace=True)` from Python: the keyword argument only.
    let mut extra = Map::new();
    extra.insert("replace".into(), Value::Bool(true));
    let opts = SyncOptions {
        extra,
        ..Default::default()
    };
    let res = served
        .client
        .sync_pipe(&pipe, frame(conn, &pipe, &[(3, 30)]).await, &opts)
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(values(conn, &pipe).await, vec![1, 2, 4, 5, 30]);

    // A raw post with `replace=true` and no bounds: the posted row's own day only.
    let target = "/pipes/plugin:rc15rp/replace_api/None/data?replace=true";
    let (status, body) = post(
        served.port,
        target,
        r#"{"ts": {"0": "2024-01-04 00:00:00"}, "id": {"0": 1}, "v": {"0": 40}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("Replaced 1 row with 1 row."), "{body}");
    assert_eq!(values(conn, &pipe).await, vec![1, 2, 5, 30, 40]);

    // No rows and no window: refused, nothing cleared.
    let (status, body) = post(served.port, target, "").await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("without rows"), "{body}");
    assert_eq!(values(conn, &pipe).await.len(), 5);

    // `begin` sent and `end=None`: no upper bound (the client sends the resolved window so).
    let opts = SyncOptions {
        begin: Some(DateBound::Datetime(match day(4) {
            KValue::Datetime(d) => d,
            _ => unreachable!(),
        })),
        end: None,
        replace: true,
        ..Default::default()
    };
    let res = served
        .client
        .sync_pipe(&pipe, frame(conn, &pipe, &[]).await, &opts)
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(values(conn, &pipe).await, vec![1, 2, 30]);

    let _ = conn.drop_pipe(&pipe).await;
    let _ = conn.delete_pipe(&pipe).await;
}
