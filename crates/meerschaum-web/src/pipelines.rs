//! The pipelines page (§8.2): the DAG with status and ledger lag, a Gantt per run (planned vs
//! actual), a filtered runs table, Parents/Children relations for the pipe card and a live
//! event stream. Everything reads the engine's instance-backed state through
//! [`meerschaum_engine::EngineReader`]; nothing is written.

use super::*;
use axum::response::sse::{Event, KeepAlive, Sse};
use meerschaum_core::success::Bound;
use meerschaum_engine::{
    DirtyRange, EdgeView, EngineReader, PipeRef, PipelineView, RunDetail, RunFilter, RunStatus,
    RunSummary,
};
use meerschaum_server::query::Query;
use std::convert::Infallible;
use std::time::Duration;

fn reader(state: &WebState) -> WebResult<&Arc<EngineReader>> {
    state.config.engine.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Engine state is not configured for the console.",
        )
            .into_response()
            .into()
    })
}

fn fmt_bound(b: &Option<Bound>) -> String {
    match b {
        None => "∞".into(),
        Some(Bound::Int(i)) => i.to_string(),
        Some(Bound::Datetime(d)) => d.format("%Y-%m-%d %H:%M:%S").to_string(),
    }
}

fn fmt_ranges(ranges: &[DirtyRange]) -> String {
    ranges
        .iter()
        .map(|r| format!("[{} → {})", fmt_bound(&r.begin), fmt_bound(&r.end)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A short human label for an edge's ledger lag.
pub(crate) fn lag_label(e: &EdgeView) -> String {
    if !e.has_cursor {
        "no cursor yet".into()
    } else if e.lagging() {
        format!("{} pending {}", e.pending_entries, fmt_ranges(&e.dirty))
    } else {
        "caught up".into()
    }
}

fn status_name(s: Option<meerschaum_engine::NodeStatus>) -> String {
    s.and_then(|s| serde_json::to_value(s).ok())
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "never_run".into())
}

fn filter_from(query: &Query) -> RunFilter {
    RunFilter {
        run_id: query.opt_str("run_id").filter(|s| !s.is_empty()),
        status: match query.str_or("status", "").as_str() {
            "running" => Some(RunStatus::Running),
            "succeeded" => Some(RunStatus::Succeeded),
            "failed" => Some(RunStatus::Failed),
            _ => None,
        },
        pipe: query.opt_str("pipe").filter(|s| !s.is_empty()),
        since: None,
        limit: Some(100),
    }
}

fn graph_html(view: &PipelineView) -> String {
    let mut elements = Vec::new();
    for n in &view.nodes {
        elements.push(json!({"data":{
            "id": n.pipe_ref.as_str(), "label": n.label, "status": status_name(n.status),
        }}));
    }
    for e in &view.edges {
        elements.push(json!({"data":{
            "id": format!("{}->{}", e.parent.as_str(), e.child.as_str()),
            "source": e.parent.as_str(), "target": e.child.as_str(),
            "label": lag_label(e), "lag": e.pending_entries,
        }}));
    }
    let mut out = format!(
        "<div data-mrsm-graph=\"{}\" role=\"img\" aria-label=\"Pipeline dependencies, status and ledger lag\"></div>",
        escape(&json!({"elements": elements}).to_string())
    );
    if view.nodes.is_empty() {
        out.push_str("<p>No pipes are registered.</p>");
    }
    out.push_str("<table data-sortable><thead><tr><th>Parent</th><th>Child</th><th>Declared by</th><th>Ledger lag</th></tr></thead><tbody>");
    for e in &view.edges {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(e.parent.as_str()),
            escape(e.child.as_str()),
            escape(&e.sources.join(", ")),
            escape(&lag_label(e)),
        ));
    }
    out.push_str("</tbody></table>");
    for w in &view.warnings {
        out.push_str(&format!("<p class=\"warning\">{}</p>", escape(w)));
    }
    out
}

fn gantt_figure(detail: &RunDetail) -> Json {
    let mut traces = Vec::new();
    for (name, color, planned) in [("Planned", "#9aa5b1", true), ("Actual", "#3486bd", false)] {
        let (mut y, mut base, mut x) = (vec![], vec![], vec![]);
        for b in &detail.bars {
            let (start, end) = if planned {
                (b.planned_start, b.planned_end)
            } else {
                (b.actual_start, b.actual_end)
            };
            if let (Some(s), Some(e)) = (start, end) {
                y.push(b.pipe_ref.as_str().to_string());
                base.push(s.to_rfc3339());
                x.push((e - s).num_milliseconds().max(0));
            }
        }
        traces.push(json!({"type":"bar","orientation":"h","name":name,
            "y":y,"base":base,"x":x,"marker":{"color":color}}));
    }
    json!({"data": traces, "layout": {
        "title": {"text": format!("Run {}", detail.summary.run.run_id)},
        "xaxis": {"type":"date"}, "yaxis": {"autorange":"reversed"}, "barmode": "group"}})
}

fn gantt_html(detail: Option<&RunDetail>) -> String {
    let Some(detail) = detail else {
        return "<p>No run selected.</p>".into();
    };
    let mut out = format!(
        "<div data-mrsm-figure=\"{}\" role=\"img\" aria-label=\"Planned and actual run timing\"></div>",
        escape(&gantt_figure(detail).to_string())
    );
    out.push_str("<table data-sortable><thead><tr><th>Pipe</th><th>Action</th><th>Status</th><th>Planned start</th><th>Actual start</th><th>Actual end</th></tr></thead><tbody>");
    let t = |d: &Option<chrono::DateTime<chrono::Utc>>| {
        d.map(|d| d.format("%H:%M:%S%.3f").to_string())
            .unwrap_or_default()
    };
    for b in &detail.bars {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(b.pipe_ref.as_str()),
            escape(&format!("{:?}", b.action).to_lowercase()),
            escape(&status_name(Some(b.status))),
            t(&b.planned_start),
            t(&b.actual_start),
            t(&b.actual_end),
        ));
    }
    out.push_str("</tbody></table>");
    out
}

fn runs_html(runs: &[RunSummary]) -> String {
    if runs.is_empty() {
        return "<p>No runs match.</p>".into();
    }
    let mut out = String::from(
        "<table data-sortable><thead><tr><th>Run</th><th>Status</th><th>Started</th><th>Duration (s)</th><th>Nodes</th><th>Message</th></tr></thead><tbody>",
    );
    for r in runs {
        let counts = r
            .counts
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "<tr><td><a href=\"/dash/pipelines?run_id={id}\">{id_text}</a></td><td>{}</td><td>{}</td><td>{:.1}</td><td>{}</td><td>{}</td></tr>",
            r.status.name(),
            r.run.started.format("%Y-%m-%d %H:%M:%S"),
            r.duration_seconds,
            escape(&counts),
            escape(r.run.message.as_deref().unwrap_or("")),
            id = encode(&r.run.run_id),
            id_text = escape(&r.run.run_id),
        ));
    }
    out.push_str("</tbody></table>");
    out
}

fn unavailable(e: impl std::fmt::Display) -> String {
    format!(
        "<p>Engine state unavailable: {}</p>",
        escape(&e.to_string())
    )
}

/// The full pipelines page content.
pub(crate) async fn page_content(state: &WebState, query: &Query) -> WebResult<String> {
    let reader = reader(state)?.clone();
    let filter = filter_from(query);
    let status = query.str_or("status", "");
    let mut content = format!(
        "<form method=\"get\" action=\"/dash/pipelines\"><label>Run ID <input name=\"run_id\" value=\"{}\"></label><label>Pipe <input name=\"pipe\" value=\"{}\"></label><label>Status <select name=\"status\">{}</select></label><button>Filter runs</button></form>",
        escape(&query.str_or("run_id", "")),
        escape(&query.str_or("pipe", "")),
        ["", "running", "succeeded", "failed"]
            .iter()
            .map(|s| format!(
                "<option value=\"{s}\"{}>{}</option>",
                if *s == status { " selected" } else { "" },
                if s.is_empty() { "any" } else { s }
            ))
            .collect::<String>(),
    );
    let qs = query_string(query);
    content.push_str(&format!(
        "<h2>Pipeline dependencies</h2><section id=\"pipeline-graph\" hx-get=\"/dash/pipelines/graph\" hx-trigger=\"mrsm-pipelines from:body, every 15s\" hx-swap=\"innerHTML\">{}</section>",
        match reader.pipeline(&[]).await {
            Ok(v) => graph_html(&v),
            Err(e) => unavailable(e),
        }
    ));
    let run_id = match &filter.run_id {
        Some(id) => Some(id.clone()),
        None => reader.current_run_id().await.ok().flatten(),
    };
    let detail = match &run_id {
        Some(id) => reader.run_detail(id).await.ok().flatten(),
        None => None,
    };
    let gantt_target = run_id
        .as_deref()
        .map(|id| format!("/dash/pipelines/gantt?run_id={}", encode(id)))
        .unwrap_or_else(|| "/dash/pipelines/gantt".into());
    content.push_str(&format!(
        "<h2>Timeline</h2><section id=\"pipeline-gantt\" hx-get=\"{}\" hx-trigger=\"mrsm-pipelines from:body\" hx-swap=\"innerHTML\">{}</section>",
        escape(&gantt_target),
        gantt_html(detail.as_ref()),
    ));
    content.push_str(&format!(
        "<h2>Runs</h2><section id=\"pipeline-runs\" hx-get=\"/dash/pipelines/runs{}\" hx-trigger=\"mrsm-pipelines from:body, every 15s\" hx-swap=\"innerHTML\">{}</section>",
        escape(&qs),
        match reader.runs(&filter).await {
            Ok(r) => runs_html(&r),
            Err(e) => unavailable(e),
        }
    ));
    if let Some(id) = run_id {
        let last = reader
            .events_after(&id, None)
            .await
            .ok()
            .and_then(|e| e.last().map(|e| e.seq))
            .unwrap_or(0);
        let _ = last;
        content.push_str(&format!(
            "<h2>Live events</h2><pre data-mrsm-events=\"/dash/pipelines/events?run_id={}\" aria-live=\"polite\"></pre>",
            encode(&id)
        ));
    }
    Ok(content)
}

fn query_string(query: &Query) -> String {
    let mut parts = Vec::new();
    for key in ["run_id", "pipe", "status"] {
        let v = query.str_or(key, "");
        if !v.is_empty() {
            parts.push(format!("{key}={}", encode(&v)));
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

async fn authed(state: &WebState, headers: &HeaderMap) -> WebResult<()> {
    session(state, headers).await.map(|_| ())
}

/// `GET /dash/pipelines/{graph,gantt,runs}`: HTMX partials.
pub(crate) async fn partial(State(state): State<WebState>, req: Request) -> Response {
    if let Err(r) = authed(&state, req.headers()).await {
        return r.into_response();
    }
    let reader = match reader(&state) {
        Ok(r) => r.clone(),
        Err(r) => return r.into_response(),
    };
    let query = Query::parse(req.uri().query());
    let body = match req.uri().path().rsplit('/').next().unwrap_or("") {
        "graph" => match reader.pipeline(&[]).await {
            Ok(v) => graph_html(&v),
            Err(e) => unavailable(e),
        },
        "gantt" => {
            let id = match query.opt_str("run_id").filter(|s| !s.is_empty()) {
                Some(id) => Some(id),
                None => reader.current_run_id().await.ok().flatten(),
            };
            match id {
                Some(id) => match reader.run_detail(&id).await {
                    Ok(d) => gantt_html(d.as_ref()),
                    Err(e) => unavailable(e),
                },
                None => gantt_html(None),
            }
        }
        "runs" => match reader.runs(&filter_from(&query)).await {
            Ok(r) => runs_html(&r),
            Err(e) => unavailable(e),
        },
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let mut response = Html(body).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

/// `GET /dash/pipelines/events?run_id=&after=`: the run's persisted events as server-sent
/// events. The stream ends (after an `end` event) once the run has finished and every event
/// has been delivered; a console in another process follows the same rows the engine wrote.
pub(crate) async fn events(State(state): State<WebState>, req: Request) -> Response {
    if let Err(r) = authed(&state, req.headers()).await {
        return r.into_response();
    }
    let reader = match reader(&state) {
        Ok(r) => r.clone(),
        Err(r) => return r.into_response(),
    };
    let query = Query::parse(req.uri().query());
    let Some(run_id) = query.opt_str("run_id").filter(|s| !s.is_empty()) else {
        return (StatusCode::BAD_REQUEST, "run_id is required").into_response();
    };
    let after = query.get("after").and_then(|a| a.parse::<u64>().ok());
    let id = cookie(req.headers()).unwrap_or_default();
    let stream = futures_util::stream::unfold(
        (
            reader,
            state,
            id,
            run_id,
            after,
            false,
            std::collections::VecDeque::new(),
        ),
        |(reader, state, id, run_id, mut after, done, mut queue)| async move {
            loop {
                if let Some(ev) = queue.pop_front() {
                    return Some((
                        Ok::<_, Infallible>(ev),
                        (reader, state, id, run_id, after, done, queue),
                    ));
                }
                if done {
                    return None;
                }
                if !matches!(state.config.sessions.get(&id).await, Ok(Some(_))) {
                    return None;
                }
                let rows = reader
                    .events_after(&run_id, after)
                    .await
                    .unwrap_or_default();
                let finished = match reader.run_detail(&run_id).await {
                    Ok(Some(d)) => d.summary.status != RunStatus::Running,
                    Ok(None) => false,
                    Err(_) => false,
                };
                for r in &rows {
                    after = Some(r.seq);
                    let payload = serde_json::to_value(r).unwrap_or(Json::Null);
                    queue.push_back(
                        Event::default()
                            .id(r.seq.to_string())
                            .data(payload.to_string()),
                    );
                }
                if rows.is_empty() {
                    if finished {
                        queue.push_back(Event::default().event("end").data("run finished"));
                        return Some((
                            Ok(queue.pop_front().expect("queued")),
                            (reader, state, id, run_id, after, true, queue),
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Parents / Children relations of one pipe for its card, with ledger lag per edge.
pub(crate) fn relations_html(view: &PipelineView, node: &PipeRef) -> String {
    let rows = |edges: Vec<&EdgeView>, other_is_parent: bool| {
        if edges.is_empty() {
            return "<p>None.</p>".to_string();
        }
        let mut out = String::from(
            "<table data-sortable><thead><tr><th>Pipe</th><th>Declared by</th><th>Ledger lag</th></tr></thead><tbody>",
        );
        for e in edges {
            let other = if other_is_parent { &e.parent } else { &e.child };
            out.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape(other.as_str()),
                escape(&e.sources.join(", ")),
                escape(&lag_label(e))
            ));
        }
        out.push_str("</tbody></table>");
        out
    };
    let parents: Vec<_> = view.parents_of(node).collect();
    let children: Vec<_> = view.children_of(node).collect();
    format!(
        "<details class=\"relations\"><summary>Parents ({})</summary>{}</details><details class=\"relations\"><summary>Children ({})</summary>{}</details>",
        parents.len(),
        rows(parents, true),
        children.len(),
        rows(children, false),
    )
}
