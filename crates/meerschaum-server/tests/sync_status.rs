//! The per-pipe sync status of an `api:` instance (rc15 contract C2): the server records the
//! syncs posted to `POST /pipes/.../data` on its own instance, a v5 client records its own
//! outcomes through `POST /engine/sync-status` (and marks its data posts so nothing is recorded
//! twice), and `GET /engine/sync-status` reads them, re-keyed with the client's `api:` keys.
//! SQLite behind the router, driven by `meerschaum-connector-api`'s client over loopback.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::sync_status::{
    SyncOutcome, SyncSource, SyncStatus, recorded_scope, reset_debounce,
};
use meerschaum_connector::{InstanceConnector, SyncOptions, pipe_ops};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::{AppState, ServerConfig};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";
const CLIENT: &str = "api:rc15st";

struct Served {
    conn: Arc<SqlConnector>,
    client: APIConnector,
    _dir: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn unique(tag: &str) -> String {
    format!(
        "rc15st_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

async fn serve() -> Served {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("rc15st.db").display());
    let conn = Arc::new(SqlConnector::from_uri("rc15st", &uri).await.unwrap());
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
    let client = APIConnector::new(Some("rc15st"), attrs).unwrap();
    Served {
        conn,
        client,
        _dir: dir,
        task,
    }
}

/// The pipe as the client sees it (`api:rc15st`) and as the server keeps it.
fn pipes(served: &Served, metric: &str) -> (Pipe, Pipe) {
    let build = |instance: &str| {
        Pipe::builder("plugin:rc15st", metric)
            .instance(instance)
            .parameters(
                Value::from_json_str(r#"{"columns": {"datetime": "ts", "id": "id"}}"#)
                    .unwrap()
                    .to_map(),
            )
            .build()
            .unwrap()
    };
    (build(CLIENT), build(&served.conn.keys()))
}

async fn frame(conn: &SqlConnector, pipe: &Pipe, n: i64) -> Frame {
    let docs: Vec<pipe_ops::Doc> = (0..n)
        .map(|i| {
            [
                (
                    "ts".to_string(),
                    KValue::Str(format!("2026-01-01 0{}:00:00", i % 10)),
                ),
                ("id".to_string(), KValue::Int(i)),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    let ctx = pipe_ops::dtypes_context(conn, pipe, pipe.parameters())
        .await
        .unwrap();
    pipe_enforce_dtypes(PipeInput::Docs(docs), &ctx).unwrap()
}

/// The server's status of `pipe`, polled briefly (the server records in the background).
async fn server_status(
    served: &Served,
    pipe: &Pipe,
    want: impl Fn(&SyncStatus) -> bool,
) -> SyncStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(s) = served.conn.get_sync_status(pipe).await.unwrap()
            && want(&s)
        {
            return s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no matching status for {pipe}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_records_posted_syncs_and_the_client_reads_them() {
    let served = serve().await;
    let metric = unique("posted");
    let (client_pipe, server_pipe) = pipes(&served, &metric);
    assert!(served.client.register_pipe(&client_pipe).await.success);

    // A post outside a recording sync path (a v4 client, a copy): the server records it.
    let df = frame(&served.conn, &server_pipe, 3).await;
    let res = served
        .client
        .sync_pipe(&client_pipe, df, &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);
    let s = server_status(&served, &server_pipe, |s| s.success).await;
    assert_eq!(s.source, "api");
    assert_eq!(s.job, None);
    assert_eq!(s.rows_inserted, 3, "{s:?}");
    assert_eq!(s.pipe_ref, server_pipe.pipe_ref());

    // The client reads it under its own keys, alone and in bulk.
    let mine = served
        .client
        .get_sync_status(&client_pipe)
        .await
        .unwrap()
        .expect("the client sees the server's record");
    assert_eq!(mine.pipe_ref, client_pipe.pipe_ref());
    assert_eq!(mine.instance_keys, CLIENT);
    assert_eq!(mine.rows_inserted, 3);
    let all = served.client.get_sync_statuses(CLIENT).await.unwrap();
    assert!(all.iter().any(|x| x.pipe_ref == client_pipe.pipe_ref()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_counts_the_rows_its_server_wrote() {
    let served = serve().await;
    let metric = unique("counts");
    let (client_pipe, server_pipe) = pipes(&served, &metric);
    assert!(served.client.register_pipe(&client_pipe).await.success);

    // Three rows in two chunks: the server's counts of each, summed; the message stays v4's.
    let opts = SyncOptions {
        chunksize: Some(2),
        ..SyncOptions::default()
    };
    let df = frame(&served.conn, &server_pipe, 3).await;
    let result = served
        .client
        .sync_pipe_with_counts(&client_pipe, df, &opts)
        .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(
        (result.inserted, result.updated, result.upserted),
        (3, 0, 0),
        "{}",
        result.message
    );
    assert!(
        result.message.starts_with("It took ")
            && result.message.contains("to sync 3 rows across 2 chunks"),
        "{}",
        result.message
    );

    // The same rows again: nothing new, nothing counted.
    let df = frame(&served.conn, &server_pipe, 3).await;
    let again = served
        .client
        .sync_pipe_with_counts(&client_pipe, df, &opts)
        .await;
    assert!(again.success, "{}", again.message);
    assert_eq!(again.rows_written(), 0, "{}", again.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_pipe_keeps_no_status_on_the_server() {
    let served = serve().await;
    let metric = unique("deleted");
    let (client_pipe, server_pipe) = pipes(&served, &metric);
    assert!(served.client.register_pipe(&client_pipe).await.success);
    // Two posts the server records itself: the second success is held by its debounce.
    for n in [2, 3] {
        let df = frame(&served.conn, &server_pipe, n).await;
        let res = served
            .client
            .sync_pipe(&client_pipe, df, &SyncOptions::default())
            .await;
        assert!(res.success, "{}", res.message);
    }
    let res = InstanceConnector::delete_pipe(&served.client, &client_pipe).await;
    assert!(res.success, "{}", res.message);
    assert!(
        served
            .client
            .get_sync_statuses(CLIENT)
            .await
            .unwrap()
            .iter()
            .all(|s| s.pipe_ref != client_pipe.pipe_ref())
    );

    // The server's debounce window ends: the status does not come back.
    tokio::time::sleep(meerschaum_connector::sync_status::DEBOUNCE + Duration::from_secs(1)).await;
    let stored =
        meerschaum_connector::sync_status::stored_status(served.conn.as_ref(), &server_pipe)
            .await
            .unwrap();
    assert!(stored.is_none(), "{stored:?}");
    assert!(
        served
            .client
            .get_sync_status(&client_pipe)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_records_its_own_outcomes_once() {
    let served = serve().await;
    let metric = unique("client");
    let (client_pipe, server_pipe) = pipes(&served, &metric);
    assert!(served.client.register_pipe(&client_pipe).await.success);

    // A client-side failure (a source the server never hears about) reaches the server.
    let failed = SyncOutcome::new(
        false,
        "The source is down.",
        SyncSource::Job,
        Some("weather".into()),
    );
    let stored = served
        .client
        .record_sync_status(&client_pipe, std::slice::from_ref(&failed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.pipe_ref, client_pipe.pipe_ref());
    assert_eq!(stored.consecutive_failures, 1);
    let s = served
        .conn
        .get_sync_status(&server_pipe)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (s.source.as_str(), s.job.as_deref()),
        ("job", Some("weather"))
    );
    assert_eq!(s.message, "The source is down.");

    // A post inside a recording sync path is marked: the server does not record it again.
    reset_debounce(&server_pipe.pipe_ref());
    let df = frame(&served.conn, &server_pipe, 2).await;
    let res = recorded_scope(
        served
            .client
            .sync_pipe(&client_pipe, df, &SyncOptions::default()),
    )
    .await;
    assert!(res.success, "{}", res.message);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s = served
        .conn
        .get_sync_status(&server_pipe)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !s.success && s.consecutive_failures == 1,
        "the post was recorded: {s:?}"
    );

    // The client's own success resets the count on the server.
    let ok = SyncOutcome::new(true, "Inserted 2, updated 0 rows.", SyncSource::Cli, None)
        .with_counts(2, 0, 0);
    let stored = served
        .client
        .record_sync_status(&client_pipe, &[ok])
        .await
        .unwrap()
        .unwrap();
    assert!(stored.success);
    assert_eq!(stored.consecutive_failures, 0);
    assert_eq!(stored.rows_inserted, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn internal_pipes_are_refused_and_unknown_pipes_have_no_status() {
    let served = serve().await;
    let internal = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance(CLIENT)
        .build()
        .unwrap();
    let outcome = SyncOutcome::new(true, "x", SyncSource::Cli, None);
    let err = served
        .client
        .record_sync_status(&internal, &[outcome])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("internal pipe"), "{err}");
    let (client_pipe, _) = pipes(&served, &unique("none"));
    assert!(
        served
            .client
            .get_sync_status(&client_pipe)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        served
            .client
            .get_sync_statuses(CLIENT)
            .await
            .unwrap()
            .is_empty()
    );
}
