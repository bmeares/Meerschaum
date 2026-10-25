//! `GET /engine/runs/{run_id}/events/stream` (§5.7, §5.8): a run's events as Server-Sent
//! Events, from the persisted rows (a run of another process) and from the live side (an
//! engine on an `api:` instance writing through this server, or one running in the server
//! process), resumable with `Last-Event-ID`, authorized like `/engine/runs`, and followed by
//! the `APIConnector` client.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use common::{SECRET, login, req, send};
use futures_util::StreamExt;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Value};
use meerschaum_server::AppState;
use meerschaum_server::engine_store::InstanceEngine;
use meerschaum_server::engine_stream::BrowserAuth;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

/// A state whose users are the in-memory fixture's (`admin` / `pass`) and whose engine state
/// lives on the SQLite database at `db`.
async fn state(dir: &Path, db: &Path) -> AppState {
    state_on(dir, &format!("sqlite:///{}", db.display())).await
}

/// [`state`] with the engine state on the instance at `uri`.
async fn state_on(dir: &Path, uri: &str) -> AppState {
    let store = Arc::new(SqlConnector::from_uri("evstream", uri).await.unwrap());
    let store: Arc<dyn InstanceConnector> = store;
    let users = Arc::new(common::MemoryInstance::new("fixture"));
    users.seed_user("admin", "pass", Some("admin"));
    AppState::builder(common::config(dir), users)
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store, "sql:evstream")))
        .build()
        .unwrap()
}

fn body(j: Json) -> Option<(&'static str, Vec<u8>)> {
    Some(("application/json", j.to_string().into_bytes()))
}

/// The time of event `seq`: recent (a stream ends a run that has been quiet too long), one
/// millisecond apart.
fn event_ts(seq: i64) -> chrono::DateTime<chrono::Utc> {
    static BASE: std::sync::OnceLock<chrono::DateTime<chrono::Utc>> = std::sync::OnceLock::new();
    *BASE.get_or_init(|| chrono::SubsecRound::trunc_subsecs(chrono::Utc::now(), 3))
        + chrono::TimeDelta::milliseconds(seq)
}

fn row(run_id: &str, seq: i64, event: &str, extra: Json) -> Json {
    let mut payload = json!({"event": event});
    if let (Some(p), Some(e)) = (payload.as_object_mut(), extra.as_object()) {
        p.extend(e.clone());
    }
    json!({
        "ts": event_ts(seq).to_rfc3339(),
        "run_id": run_id,
        "seq": seq,
        "pipe_ref": if event.starts_with("run_") { Json::Null } else { json!("sql:evstream|sql:src|m|None") },
        "event": event,
        "payload": payload,
    })
}

/// The events of a whole run: started, one node with a chunk, finished.
fn run_rows(run_id: &str) -> Vec<Json> {
    vec![
        row(
            run_id,
            0,
            "run_started",
            json!({"nodes": 1, "workers": 1, "targets": []}),
        ),
        row(
            run_id,
            1,
            "node_planned",
            json!({"action": "incremental", "reason": "", "provisional": false}),
        ),
        row(
            run_id,
            2,
            "node_started",
            json!({"action": "incremental", "attempt": 1}),
        ),
        row(
            run_id,
            3,
            "chunk_started",
            json!({"chunk": 0, "begin": null, "end": null, "source": "fetch", "rows": 5}),
        ),
        row(
            run_id,
            4,
            "chunk_finished",
            json!({"chunk": 0, "success": true, "message": "ok", "source": "fetch", "rows": 5, "inserted": 5}),
        ),
        row(
            run_id,
            5,
            "node_finished",
            json!({"result": {"success": true, "message": "ok"}}),
        ),
        row(
            run_id,
            6,
            "run_finished",
            json!({"success": true, "message": "1 succeeded"}),
        ),
    ]
}

async fn write_events(app: &axum::Router, token: &str, rows: &[Json]) {
    let r = send(
        app,
        req(
            "POST",
            "/engine/pipes/engine_events/write",
            Some(token),
            body(Json::Array(rows.to_vec())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
}

async fn record_run(app: &axum::Router, token: &str, run_id: &str, finished: bool) {
    record_run_held(
        app,
        token,
        run_id,
        finished,
        &format!("mrsm:{}", std::process::id()),
    )
    .await;
}

/// [`record_run`] with the run's lease holder (`mrsm:<pid>`).
async fn record_run_held(
    app: &axum::Router,
    token: &str,
    run_id: &str,
    finished: bool,
    holder: &str,
) {
    let mut record = json!({"run_id": run_id, "started": "2026-01-01T00:00:00Z",
                            "status": if finished { "success" } else { "running" },
                            "holder": holder});
    if finished {
        record["finished"] = json!("2026-01-01T00:01:00Z");
        record["success"] = json!(true);
    }
    let r = send(
        app,
        req("POST", "/engine/runs", Some(token), body(json!([record]))),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

/// One SSE event: `(event, id, data)`.
type Frame = (String, String, Json);

/// Open the stream in-process; the response's status, headers and body.
async fn open(
    app: &axum::Router,
    path: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Body) {
    let mut b = Request::builder().method("GET").uri(path);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = resp.into_parts();
    (parts.status, parts.headers, body)
}

/// Read SSE frames until the stream ends, `max` frames, or `timeout`. The `bool` is whether
/// the stream ended.
async fn frames(body: Body, max: usize, timeout: Duration) -> (Vec<Frame>, bool) {
    let mut stream = body.into_data_stream();
    let mut buf = String::new();
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        while let Some(end) = buf.find("\n\n") {
            let block: String = buf.drain(..end + 2).collect();
            let (mut event, mut id, mut data) = (String::new(), String::new(), String::new());
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = v.trim().to_string();
                } else if let Some(v) = line.strip_prefix("id:") {
                    id = v.trim().to_string();
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push_str(v.trim_start());
                }
            }
            if !data.is_empty() {
                out.push((event, id, serde_json::from_str(&data).unwrap()));
                if out.len() >= max {
                    return (out, false);
                }
            }
        }
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(bytes))) => buf.push_str(&String::from_utf8_lossy(&bytes)),
            Ok(Some(Err(e))) => panic!("body error: {e}"),
            Ok(None) => return (out, true),
            Err(_) => return (out, false),
        }
    }
}

fn names(frames: &[Frame]) -> Vec<&str> {
    frames.iter().map(|f| f.0.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_run_streams_in_order_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;
    record_run(&app, &admin, "persisted-run", true).await;
    write_events(&app, &admin, &run_rows("persisted-run")).await;

    let path = "/engine/runs/persisted-run/events/stream";
    // Authorization is that of `/engine/runs`.
    let (status, _, _) = open(&app, path, &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = open(&app, path, &[("authorization", &reader)]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // An unknown run (and an invalid run ID).
    let (status, _, _) = open(
        &app,
        "/engine/runs/no-such-run/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = open(
        &app,
        "/engine/runs/bad%20id/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // The whole run, in `seq` order, then the stream ends.
    let (status, headers, body) = open(&app, path, &[("authorization", &admin)]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    assert_eq!(headers["cache-control"], "no-cache");
    let (got, ended) = frames(body, 100, Duration::from_secs(10)).await;
    assert!(ended, "{got:?}");
    assert_eq!(
        names(&got),
        [
            "run_started",
            "node_planned",
            "node_started",
            "chunk_started",
            "chunk_finished",
            "node_finished",
            "run_finished"
        ]
    );
    let ids: Vec<&str> = got.iter().map(|f| f.1.as_str()).collect();
    assert_eq!(ids, ["0", "1", "2", "3", "4", "5", "6"]);
    // The data is the typed event: the envelope with the kind's fields flattened in.
    let chunk = &got[4].2;
    assert_eq!(chunk["run_id"], "persisted-run");
    assert_eq!(chunk["seq"], 4);
    assert_eq!(chunk["event"], "chunk_finished");
    assert_eq!(chunk["pipe_ref"], "sql:evstream|sql:src|m|None");
    assert_eq!(chunk["rows"], 5);
    assert_eq!(chunk["inserted"], 5);
    assert_eq!(chunk["source"], "fetch");
    let ts: chrono::DateTime<chrono::Utc> = chunk["ts"].as_str().unwrap().parse().unwrap();
    assert_eq!(ts, event_ts(4));
    assert_eq!(got[0].2["pipe_ref"], Json::Null);

    // `Last-Event-ID` (and `?after_seq=`) resume after that event.
    let (_, _, body) = open(
        &app,
        path,
        &[("authorization", &admin), ("last-event-id", "4")],
    )
    .await;
    let (got, ended) = frames(body, 100, Duration::from_secs(10)).await;
    assert!(ended);
    assert_eq!(names(&got), ["node_finished", "run_finished"]);
    let (_, _, body) = open(
        &app,
        &format!("{path}?after_seq=5"),
        &[("authorization", &admin)],
    )
    .await;
    let (got, _) = frames(body, 100, Duration::from_secs(10)).await;
    assert_eq!(names(&got), ["run_finished"]);
    let (status, _, _) = open(
        &app,
        path,
        &[("authorization", &admin), ("last-event-id", "x")],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_events_arrive_while_the_run_executes() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state.clone());
    let admin = login(&app, "admin", "pass", None).await;
    let rows = run_rows("live-run");

    // The client opens the stream before the run exists (`?wait=`), as the console does right
    // after starting a job.
    let opening = {
        let app = app.clone();
        let admin = admin.clone();
        tokio::spawn(async move {
            open(
                &app,
                "/engine/runs/live-run/events/stream?wait=10",
                &[("authorization", &admin)],
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    record_run(&app, &admin, "live-run", false).await;
    write_events(&app, &admin, &rows[..2]).await;
    let (status, _, body) = opening.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    let reading = tokio::spawn(frames(body, 100, Duration::from_secs(15)));

    // An engine on an `api:` instance flushing through this server.
    tokio::time::sleep(Duration::from_millis(200)).await;
    write_events(&app, &admin, &rows[2..5]).await;
    // An engine running in the server process: published, never persisted here.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for r in &rows[5..] {
        let mut event = r["payload"].clone();
        for key in ["run_id", "seq", "ts", "pipe_ref"] {
            event[key] = r[key].clone();
        }
        assert!(state.engine_streams.publish_json(&event));
    }
    let (got, ended) = reading.await.unwrap();
    assert!(ended, "{got:?}");
    assert_eq!(got.len(), 7, "{got:?}");
    let ids: Vec<&str> = got.iter().map(|f| f.1.as_str()).collect();
    assert_eq!(ids, ["0", "1", "2", "3", "4", "5", "6"]);
    assert_eq!(got[6].0, "run_finished");
    assert_eq!(state.engine_streams.open_streams(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gaps_are_waited_for_briefly_and_interrupted_runs_end() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let rows = run_rows("gappy");
    record_run(&app, &admin, "gappy", false).await;
    // `system.engine.events: nodes` leaves the chunk events out: seq 1, 3 and 4 never arrive.
    write_events(&app, &admin, &[rows[0].clone(), rows[2].clone()]).await;
    let (_, _, body) = open(
        &app,
        "/engine/runs/gappy/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    let reading = tokio::spawn(frames(body, 100, Duration::from_secs(20)));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    write_events(&app, &admin, &[rows[5].clone()]).await;
    // The run is interrupted: its record finishes, `run_finished` never comes. The stream
    // still ends with a terminal event (`run_closed`, no `id:`), so no client reconnects.
    record_run(&app, &admin, "gappy", true).await;
    let (got, ended) = reading.await.unwrap();
    assert!(ended, "{got:?}");
    assert_eq!(
        names(&got),
        ["run_started", "node_started", "node_finished", "run_closed"]
    );
    let closed = &got[3];
    assert_eq!(closed.1, "", "run_closed carries no id");
    assert_eq!(closed.2["reason"], "interrupted");
    assert_eq!(closed.2["finished"], true);
    assert_eq!(closed.2["success"], true);
    assert_eq!(closed.2["seq"], 5);
}

/// `system.engine.events: nodes` leaves a gap at every chunk event: the stream waits one grace
/// for all the events it has, not one per gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_gaps_cost_one_grace() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    record_run(&app, &admin, "gaps", false).await;
    write_events(
        &app,
        &admin,
        &[row("gaps", 0, "run_started", json!({"nodes": 1}))],
    )
    .await;
    let (_, _, body) = open(
        &app,
        "/engine/runs/gaps/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    let reading = tokio::spawn(frames(body, 7, Duration::from_secs(20)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = tokio::time::Instant::now();
    // Six gaps (seq 1, 3, ..., 11 are never persisted).
    let rows: Vec<Json> = (1..=6)
        .map(|i| row("gaps", i * 2, "node_started", json!({"attempt": i})))
        .collect();
    write_events(&app, &admin, &rows).await;
    let (got, _) = reading.await.unwrap();
    assert_eq!(got.len(), 7, "{got:?}");
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
}

/// A dead process number (a child that has exited and been reaped).
fn dead_pid() -> u32 {
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

/// A run whose process died never closes its record: its stream still ends, with
/// `run_closed` (`holder_gone` when the holder is a dead local process, else `idle`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_runs_end_with_run_closed() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state.clone());
    let admin = login(&app, "admin", "pass", None).await;
    let rows = run_rows("killed");
    if cfg!(unix) {
        record_run_held(
            &app,
            &admin,
            "killed",
            false,
            &format!("mrsm:{}", dead_pid()),
        )
        .await;
        // Its last event was minutes ago.
        let mut old = rows[..3].to_vec();
        for (i, r) in old.iter_mut().enumerate() {
            r["ts"] = json!(format!("2026-01-01T00:00:0{i}+00:00"));
        }
        write_events(&app, &admin, &old).await;
        let (_, _, body) = open(
            &app,
            "/engine/runs/killed/events/stream",
            &[("authorization", &admin)],
        )
        .await;
        let (got, ended) = frames(body, 100, Duration::from_secs(10)).await;
        assert!(ended, "{got:?}");
        assert_eq!(
            names(&got),
            ["run_started", "node_planned", "node_started", "run_closed"]
        );
        assert_eq!(got[3].2["reason"], "holder_gone");
        assert_eq!(got[3].2["finished"], false);
        assert_eq!(got[3].2["success"], Json::Null);
    }

    // A holder that is alive (this process), quiet for longer than `?idle_timeout=`.
    record_run(&app, &admin, "quiet", false).await;
    let mut old = run_rows("quiet")[..2].to_vec();
    for (i, r) in old.iter_mut().enumerate() {
        r["ts"] = json!(format!("2026-01-01T00:00:0{i}+00:00"));
    }
    write_events(&app, &admin, &old).await;
    let (_, _, body) = open(
        &app,
        "/engine/runs/quiet/events/stream?idle_timeout=10",
        &[("authorization", &admin)],
    )
    .await;
    let (got, ended) = frames(body, 100, Duration::from_secs(10)).await;
    assert!(ended, "{got:?}");
    assert_eq!(names(&got), ["run_started", "node_planned", "run_closed"]);
    assert_eq!(got[2].2["reason"], "idle");
    assert_eq!(state.engine_streams.open_streams(), 0);

    // A live run with recent events is not closed.
    record_run(&app, &admin, "busy", false).await;
    write_events(&app, &admin, &run_rows("busy")[..2]).await;
    let (_, _, body) = open(
        &app,
        "/engine/runs/busy/events/stream?idle_timeout=10",
        &[("authorization", &admin)],
    )
    .await;
    let (got, ended) = frames(body, 100, Duration::from_secs(3)).await;
    assert!(!ended, "{got:?}");
    assert_eq!(names(&got), ["run_started", "node_planned"]);
}

/// Waiting for a run (`?wait=`) holds no stream slot, and a `--no-auth` server does not count
/// every client's streams against one shared user.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_slots_are_counted_per_client() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.log_streams_per_user = 1;
    let store = Arc::new(
        SqlConnector::from_uri(
            "evstream",
            &format!("sqlite:///{}", dir.path().join("store.db").display()),
        )
        .await
        .unwrap(),
    );
    let users = Arc::new(common::MemoryInstance::new("fixture"));
    users.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(cfg.clone(), users.clone())
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store.clone(), "sql:evstream")))
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    record_run(&app, &admin, "done", true).await;
    write_events(&app, &admin, &run_rows("done")).await;
    let waiting = {
        let (app, admin) = (app.clone(), admin.clone());
        tokio::spawn(async move {
            open(
                &app,
                "/engine/runs/not-yet/events/stream?wait=3",
                &[("authorization", &admin)],
            )
            .await
            .0
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (status, _, body) = open(
        &app,
        "/engine/runs/done/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a wait held the user's only slot");
    // The open stream does hold it.
    let (status, _, _) = open(
        &app,
        "/engine/runs/done/events/stream",
        &[("authorization", &admin)],
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    drop(body);
    assert_eq!(waiting.await.unwrap(), StatusCode::NOT_FOUND);

    // `--no-auth`: anonymous clients without a console cookie share only the server-wide cap.
    cfg.no_auth = true;
    let state = AppState::builder(cfg, users)
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store, "sql:evstream")))
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let mut open_bodies = Vec::new();
    for _ in 0..3 {
        let (status, _, body) = open(&app, "/engine/runs/done/events/stream", &[]).await;
        assert_eq!(status, StatusCode::OK);
        open_bodies.push(body);
    }
}

/// Accepts the cookie `mrsm-session-id=good` as the admin's console session.
struct FakeSessions {
    bearer: String,
}

#[async_trait::async_trait]
impl BrowserAuth for FakeSessions {
    async fn authorize(
        &self,
        _state: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>> {
        let cookie = headers.get("cookie")?.to_str().ok()?;
        if cookie != "mrsm-session-id=good" {
            return Some(Err(StatusCode::UNAUTHORIZED));
        }
        let mut h = HeaderMap::new();
        h.insert("authorization", self.bearer.parse().unwrap());
        Some(Ok((h, "good".into())))
    }

    async fn session_active(&self, _state: &AppState, _session_id: &str) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_console_session_authorizes_an_event_source() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state.clone());
    let admin = login(&app, "admin", "pass", None).await;
    record_run(&app, &admin, "cookie-run", true).await;
    write_events(&app, &admin, &run_rows("cookie-run")).await;
    let path = "/engine/runs/cookie-run/events/stream";
    let (status, _, _) = open(&app, path, &[("cookie", "mrsm-session-id=good")]).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "no browser auth installed"
    );
    assert!(
        state
            .engine_streams
            .set_browser_auth(Arc::new(FakeSessions { bearer: admin }))
    );
    let (status, _, body) = open(&app, path, &[("cookie", "mrsm-session-id=good")]).await;
    assert_eq!(status, StatusCode::OK);
    let (got, ended) = frames(body, 100, Duration::from_secs(10)).await;
    assert!(ended);
    assert_eq!(got.len(), 7);
    let (status, _, _) = open(&app, path, &[("cookie", "mrsm-session-id=bad")]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_api_client_follows_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path(), &dir.path().join("store.db")).await;
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let rows = run_rows("client-run");
    record_run(&app, &admin, "client-run", false).await;
    write_events(&app, &admin, &rows[..3]).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let served = app.clone();
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            served.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("username".into(), Value::Str("admin".into()));
    attrs.insert("password".into(), Value::Str("pass".into()));
    let client = APIConnector::new(Some("evstream"), attrs).unwrap();

    let mut events = client
        .follow_engine_run("client-run", None, None)
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let e = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        seen.push(e);
    }
    write_events(&app, &admin, &rows[3..]).await;
    while let Some(e) = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap()
    {
        seen.push(e.unwrap());
    }
    let seqs: Vec<i64> = seen.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, [0, 1, 2, 3, 4, 5, 6]);
    assert!(seen.last().unwrap().is_run_finished());
    assert_eq!(seen[4].fields["inserted"], 5);

    // Resuming after an event, and an unknown run.
    let resumed: Vec<_> = client
        .follow_engine_run("client-run", Some(4), None)
        .await
        .unwrap()
        .collect()
        .await;
    let seqs: Vec<i64> = resumed.into_iter().map(|e| e.unwrap().seq).collect();
    assert_eq!(seqs, [5, 6]);
    match client.follow_engine_run("nope", None, None).await {
        Err(meerschaum_connector::ConnectorError::Http { status, .. }) => assert_eq!(status, 404),
        Err(e) => panic!("unexpected error: {e}"),
        Ok(_) => panic!("an unknown run streamed"),
    }

    // `wait` follows a run launched a moment later.
    let later = {
        let (app, admin) = (app.clone(), admin.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            record_run(&app, &admin, "later-run", true).await;
            write_events(&app, &admin, &run_rows("later-run")).await;
        })
    };
    let waited: Vec<_> = client
        .follow_engine_run_with(
            "later-run",
            meerschaum_connector_api::client::FollowRunOptions {
                wait: Some(Duration::from_secs(10)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .collect()
        .await;
    later.await.unwrap();
    assert_eq!(waited.len(), 7);

    // A run that will never finish ends with the server's `run_closed`.
    record_run(&app, &admin, "stalled-run", false).await;
    let mut old = run_rows("stalled-run")[..2].to_vec();
    for (i, r) in old.iter_mut().enumerate() {
        r["ts"] = json!(format!("2026-01-01T00:00:0{i}+00:00"));
    }
    write_events(&app, &admin, &old).await;
    let stalled: Vec<_> = tokio::time::timeout(
        Duration::from_secs(20),
        client
            .follow_engine_run_with(
                "stalled-run",
                meerschaum_connector_api::client::FollowRunOptions {
                    idle_timeout: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let last = stalled.last().unwrap().as_ref().unwrap();
    assert!(last.is_run_closed() && last.is_terminal(), "{stalled:?}");
    assert_eq!(last.fields["reason"], "idle");
    assert_eq!(stalled.len(), 3);
    task.abort();
}

/// The persisted tail on a server-flavored instance (`MRSM_TEST_PG_URI`): the `ts`-bounded reads
/// of `mrsm_engine_events` find every row of the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_tail_on_postgresql() {
    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let app = meerschaum_server::app(state_on(dir.path(), &uri).await);
    let admin = login(&app, "admin", "pass", None).await;
    let run_id = format!(
        "rs-test-stream-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let rows = run_rows(&run_id);
    record_run(&app, &admin, &run_id, false).await;
    write_events(&app, &admin, &rows[..4]).await;
    let (status, _, events) = open(
        &app,
        &format!("/engine/runs/{run_id}/events/stream"),
        &[("authorization", &admin)],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reading = tokio::spawn(frames(events, 100, Duration::from_secs(20)));
    tokio::time::sleep(Duration::from_millis(700)).await;
    write_events(&app, &admin, &rows[4..]).await;
    let (got, ended) = reading.await.unwrap();
    assert!(ended, "{got:?}");
    let ids: Vec<&str> = got.iter().map(|f| f.1.as_str()).collect();
    assert_eq!(ids, ["0", "1", "2", "3", "4", "5", "6"]);
    // Clean up this run's rows.
    for metric in ["engine_events", "engine_runs"] {
        let r = send(
            &app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/clear"),
                Some(&admin),
                body(json!({"params": {"run_id": run_id}})),
            ),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    }
}
