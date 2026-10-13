use super::*;
use axum::extract::Path;
use axum::response::sse::{Event, KeepAlive, Sse};
use meerschaum_server::{
    backends::LogEvent,
    query::Query,
    routes::{form_get, parse_form},
};
use std::convert::Infallible;

async fn get_json(state: &WebState, data: &Json, uri: &str) -> WebResult<Json> {
    read_json(api(state, data, Method::GET, uri, String::new(), false).await).await
}
fn pretty(value: &Json) -> String {
    escape(&serde_json::to_string_pretty(value).unwrap_or_default())
}
fn hidden(key: &str, value: &str) -> String {
    format!(
        "<input type=\"hidden\" name=\"{}\" value=\"{}\">",
        escape(key),
        escape(value)
    )
}
fn form(data: &Json, op: &str, target: &str, fields: &str, label: &str) -> String {
    format!(
        "<form method=\"post\" action=\"/dash/action\">{}{}{}{fields}<button>{}</button></form>",
        hidden("csrf", data["csrf"].as_str().unwrap_or("")),
        hidden("op", op),
        hidden("target", target),
        escape(label)
    )
}
fn table(value: &Json) -> String {
    let mut out = String::from(
        "<table data-sortable><thead><tr><th>Name</th><th>Details</th></tr></thead><tbody>",
    );
    match value {
        Json::Object(map) => {
            for (key, value) in map {
                out.push_str(&format!(
                    "<tr><td>{}</td><td><pre>{}</pre></td></tr>",
                    escape(key),
                    pretty(value)
                ));
            }
        }
        Json::Array(items) => {
            for (i, value) in items.iter().enumerate() {
                out.push_str(&format!(
                    "<tr><td>{i}</td><td><pre>{}</pre></td></tr>",
                    pretty(value)
                ));
            }
        }
        _ => out.push_str(&format!(
            "<tr><td>Result</td><td><pre>{}</pre></td></tr>",
            pretty(value)
        )),
    }
    out.push_str("</tbody></table>");
    out
}
pub(crate) async fn page(State(state): State<WebState>, req: Request) -> Response {
    let headers = req.headers();
    let data = match session(&state, headers).await {
        Ok(d) => d,
        Err(r) => return r.into_response(),
    };
    let name = req
        .uri()
        .path()
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("dashboard");
    let query = Query::parse(req.uri().query());
    let result = match name {
        "dash" | "dashboard" | "pipes" => pipes(&state, &data, &query).await,
        "jobs" => jobs(&state, &data).await,
        "plugins" => plugins(&state, &data, &query).await,
        "users" => users(&state, &data).await,
        "tokens" => tokens(&state, &data).await,
        "settings" | "password-reset" => Ok(form(
            &data,
            "password",
            "",
            "<label>New password <input type=\"password\" name=\"password\" autocomplete=\"new-password\" required></label>",
            "Change password",
        )),
        "webterm" => Ok(
            match state
                .config
                .webterm
                .as_deref()
                .filter(|s| s.starts_with('/') && !s.starts_with("//"))
            {
                Some(url) => format!("<a href=\"{}\">Open web terminal</a>", escape(url)),
                None => "<p>Web terminal is not configured.</p>".into(),
            },
        ),
        "pipelines" => pipelines(&state, &data, &query).await,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    match result {
        Ok(content) => render(
            &state,
            headers,
            if name == "dash" { "dashboard" } else { name },
            &content,
            &data,
        ),
        Err(r) => r.into_response(),
    }
}
async fn pipes(state: &WebState, data: &Json, query: &Query) -> WebResult<String> {
    let mut content = String::from(
        "<form method=\"get\" action=\"/dash/pipes\" hx-get=\"/dash/pipes\" hx-target=\"#content\" hx-push-url=\"true\">",
    );
    let mut params = Vec::new();
    for (key, label) in [
        ("connector_keys", "Connector"),
        ("metric_keys", "Metric"),
        ("location_keys", "Location"),
        ("tags", "Tags"),
    ] {
        let input = query.str_or(key, "");
        content.push_str(&format!("<label>{label}<input name=\"{key}\" value=\"{}\" aria-describedby=\"filter-help\"></label>", escape(&input)));
        // PARITY: key negation `_` and tag AND/OR groups are interpreted by fetch_pipes_keys,
        // exactly as the v4 dashboard's get_pipes call, not by substring filtering here.
        let values: Json = if input.trim().starts_with('[') {
            serde_json::from_str(&input).map_err(|_| {
                (StatusCode::BAD_REQUEST, "Filters must be valid JSON lists").into_response()
            })?
        } else {
            json!(
                input
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            )
        };
        params.push(format!("{key}={}", encode(&values.to_string())));
    }
    content.push_str("<button>Search pipes</button><p id=\"filter-help\">Comma-separated keys or JSON lists; prefix a key with _ to exclude it. Tags accept JSON groups.</p></form>");
    let keys = get_json(state, data, &format!("/pipes/keys?{}", params.join("&"))).await?;
    let Some(keys) = keys.as_array() else {
        return Ok(content + "<p>No pipes found.</p>");
    };
    if keys.is_empty() {
        content.push_str("<p>No pipes found.</p>");
    }
    for keys in keys {
        let Some(keys) = keys.as_array().filter(|v| v.len() >= 3) else {
            continue;
        };
        let ck = keys[0].as_str().unwrap_or("");
        if ck == "mrsm" {
            continue;
        }
        let mk = keys[1].as_str().unwrap_or("");
        let lk = keys[2].as_str().unwrap_or("None");
        let target = format!("/pipes/{}/{}/{}", encode(ck), encode(mk), encode(lk));
        let attributes = get_json(state, data, &format!("{target}/attributes")).await?;
        content.push_str(&format!(
            "<article class=\"pipe-card\"><h2>{} / {} / {}</h2>",
            escape(ck),
            escape(mk),
            escape(lk)
        ));
        let parameters = &attributes["parameters"];
        for (title, value) in [
            ("Parameters", parameters),
            ("Columns", &parameters["columns"]),
            ("Dtypes", &parameters["dtypes"]),
            ("Parents", &parameters["parents"]),
            ("Children", &parameters["children"]),
        ] {
            content.push_str(&format!(
                "<details><summary>{title}</summary><pre>{}</pre></details>",
                pretty(value)
            ));
        }
        for (title, suffix) in [("Row count", "rowcount"), ("Sync time", "sync_time")] {
            let value = get_json(state, data, &format!("{target}/{suffix}")).await;
            content.push_str(&format!(
                "<p>{title}: {}</p>",
                value
                    .as_ref()
                    .map(pretty)
                    .unwrap_or_else(|_| "Unavailable".into())
            ));
        }
        content.push_str(&form(
            data,
            "pipe-edit",
            &target,
            &format!(
                "<label>Parameters (JSON)<textarea name=\"payload\">{}</textarea></label>",
                pretty(parameters)
            ),
            "Save parameters",
        ));
        content.push_str(&form(
            data,
            "pipe-sync",
            &target,
            "<label>Documents (JSON)<textarea name=\"payload\" required>[]</textarea></label>",
            "Sync documents",
        ));
        content.push_str(&form(data, "pipe-fetch", &target, "", "Sync from source"));
        content.push_str(&format!(
            "<a href=\"/dash/pipes?preview={}\">Preview data</a>",
            encode(&target)
        ));
        if query.get("preview") == Some(target.as_str()) {
            let docs = get_json(state, data, &format!("{target}/docs?limit=100")).await?;
            content.push_str(&format!("<div data-mrsm-figure=\"{}\" role=\"img\" aria-label=\"Pipe data chart\"></div><details open><summary>Data preview</summary>{}</details>", escape(&preview_figure(&docs).to_string()), table(&docs)));
        }
        content.push_str("</article>");
    }
    Ok(content)
}
/// Plotly JSON, with a tabular no-JS counterpart supplied by the caller.
pub fn preview_figure(docs: &Json) -> Json {
    let rows = docs.as_array().cloned().unwrap_or_default();
    let Some(first) = rows.first().and_then(Json::as_object) else {
        return json!({"data":[],"layout":{}});
    };
    let xkey = first.keys().next().cloned().unwrap_or_default();
    let traces: Vec<_> = first.keys().filter(|key| *key != &xkey && rows.iter().any(|row| row[*key].is_number())).map(|key| json!({"type":"scatter","mode":"lines+markers","name":key,"x":rows.iter().map(|r| r[&xkey].clone()).collect::<Vec<_>>(),"y":rows.iter().map(|r| r[key].clone()).collect::<Vec<_>>() })).collect();
    json!({"data":traces,"layout":{"title":"Pipe data preview","xaxis":{"title":xkey}}})
}
async fn jobs(state: &WebState, data: &Json) -> WebResult<String> {
    let jobs = get_json(state, data, "/jobs").await?;
    let mut content = String::new();
    if let Some(jobs) = jobs.as_object() {
        for (name, details) in jobs {
            content.push_str(&format!(
                "<article><h2>{}</h2><pre>{}</pre>",
                escape(name),
                pretty(details)
            ));
            for op in ["start", "stop", "pause"] {
                content.push_str(&form(data, &format!("job-{op}"), name, "", op));
            }
            let logs = get_json(state, data, &format!("/logs/{}", encode(name)))
                .await
                .unwrap_or(Json::Null);
            content.push_str(&format!("<h3>Logs</h3><pre role=\"log\" aria-live=\"polite\" data-logs=\"/dash/job-logs/{}\">{}</pre></article>", encode(name), escape(logs.as_str().unwrap_or(""))));
        }
    }
    if content.is_empty() {
        content.push_str("<p>No jobs found.</p>");
    }
    Ok(content)
}
async fn users(state: &WebState, data: &Json) -> WebResult<String> {
    let users = get_json(state, data, "/users").await?;
    let mut content = table(&users);
    content.push_str(&form(data, "user-register", "", "<label>Username <input name=\"username\" required></label><label>Password <input type=\"password\" name=\"password\" required></label>", "Register user"));
    if let Some(users) = users.as_array() {
        for user in users.iter().filter_map(Json::as_str) {
            let attrs = get_json(state, data, &format!("/users/{}/attributes", encode(user)))
                .await
                .unwrap_or(json!({}));
            content.push_str(&format!("<h2>{}</h2>", escape(user)));
            content.push_str(&form(data, "user-edit", user, &format!("<label>Email <input name=\"email\" type=\"email\"></label><label>Type <select name=\"type\"><option value=\"\">Unchanged</option><option>admin</option><option>user</option></select></label><label>Attributes (JSON)<textarea name=\"attributes\">{}</textarea></label>", pretty(&attrs)), "Save user"));
            content.push_str(&form(
                data,
                "user-delete",
                user,
                "",
                &format!("Delete {user}"),
            ));
        }
    }
    Ok(content)
}
async fn tokens(state: &WebState, data: &Json) -> WebResult<String> {
    let tokens = get_json(state, data, "/tokens").await?;
    let mut content = table(&tokens);
    content.push_str(&form(data, "token-register", "", "<label>Token properties (JSON)<textarea name=\"payload\">{\"label\":\"console\",\"scopes\":[]}</textarea></label>", "Create token"));
    if let Some(tokens) = tokens.as_array() {
        for token in tokens {
            if let Some(id) = token["id"].as_str().or(token.as_str()) {
                content.push_str(&form(data, "token-edit", id, &format!("<label>Token properties (JSON)<textarea name=\"payload\">{}</textarea></label>", pretty(token)), "Save token"));
                content.push_str(&form(data, "token-invalidate", id, "", "Invalidate token"));
                content.push_str(&form(data, "token-delete", id, "", "Delete token"));
            }
        }
    }
    Ok(content)
}
pub(crate) async fn action(
    State(state): State<WebState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let data = match session(&state, &headers).await {
        Ok(d) => d,
        Err(r) => return r.into_response(),
    };
    let fields = parse_form(body.as_bytes());
    if !csrf_ok(&headers, &data, &fields) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let op = form_get(&fields, "op").unwrap_or("");
    let target = form_get(&fields, "target").unwrap_or("");
    let payload = form_get(&fields, "payload").unwrap_or("{}");
    let mut form_body = false;
    let (method, uri, payload) = match op {
        "job-start" | "job-stop" | "job-pause" => (
            Method::POST,
            format!("/jobs/{}/{}", encode(target), op.trim_start_matches("job-")),
            "{}".into(),
        ),
        "pipe-edit" | "pipe-sync" | "pipe-fetch" if valid_pipe_target(target) => {
            if serde_json::from_str::<Json>(payload).is_err() {
                return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
            }
            if op == "pipe-fetch" {
                (Method::POST, format!("{target}/data"), "null".into())
            } else {
                (
                    if op == "pipe-edit" {
                        Method::PATCH
                    } else {
                        Method::POST
                    },
                    format!(
                        "{target}/{}",
                        if op == "pipe-edit" { "edit" } else { "data" }
                    ),
                    payload.to_string(),
                )
            }
        }
        "user-delete" => (
            Method::DELETE,
            format!("/users/{}", encode(target)),
            String::new(),
        ),
        "user-register" | "password" => {
            form_body = true;
            let user = if op == "password" {
                data["username"].as_str().unwrap_or("")
            } else {
                form_get(&fields, "username").unwrap_or("")
            };
            (
                Method::POST,
                if op == "password" {
                    "/users/edit"
                } else {
                    "/users/register"
                }
                .into(),
                format!(
                    "username={}&password={}",
                    encode(user),
                    encode(form_get(&fields, "password").unwrap_or(""))
                ),
            )
        }
        "user-edit" => {
            form_body = true;
            let mut body = format!("username={}", encode(target));
            for field in ["email", "type", "attributes"] {
                if let Some(value) = form_get(&fields, field).filter(|s| !s.is_empty()) {
                    body.push_str(&format!("&{field}={}", encode(value)));
                }
            }
            (Method::POST, "/users/edit".into(), body)
        }
        "token-edit" => (
            Method::POST,
            format!("/tokens/{}/edit", encode(target)),
            payload.into(),
        ),
        "plugin-delete" => (
            Method::DELETE,
            format!("/plugins/{}", encode(target)),
            String::new(),
        ),
        "plugin-install" => (
            Method::POST,
            "/actions/install".into(),
            json!({"action":["plugins",target]}).to_string(),
        ),
        "token-register" => (Method::POST, "/tokens/register".into(), payload.into()),
        "token-invalidate" => (
            Method::POST,
            format!("/tokens/{}/invalidate", encode(target)),
            "{}".into(),
        ),
        "token-delete" => (
            Method::DELETE,
            format!("/tokens/{}", encode(target)),
            String::new(),
        ),
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let response = api(&state, &data, method, &uri, payload, form_body).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let mut response = render(
        &state,
        &headers,
        "Action result",
        &format!(
            "<p role=\"status\">{}</p><a href=\"/dash\">Return to dashboard</a>",
            escape(&String::from_utf8_lossy(&bytes))
        ),
        &data,
    );
    *response.status_mut() = status;
    response
}
fn valid_pipe_target(target: &str) -> bool {
    target.starts_with("/pipes/")
        && target.split('/').count() == 5
        && !target.contains(['?', '#', '\\'])
        && !target.split('/').any(|s| s == "." || s == "..")
}
pub(crate) async fn register_page(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if !state.server.config.registration_users {
        return StatusCode::FORBIDDEN.into_response();
    }
    render(
        &state,
        &headers,
        "Register",
        "<form method=\"post\"><label>Username <input name=\"username\" required autocomplete=\"username\"></label><label>Password <input name=\"password\" type=\"password\" required autocomplete=\"new-password\"></label><button>Register</button></form>",
        &Json::Null,
    )
}
pub(crate) async fn register(
    State(state): State<WebState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if cross_site(&headers) || !state.server.config.registration_users {
        return StatusCode::FORBIDDEN.into_response();
    }
    let request = Request::builder()
        .method(Method::POST)
        .uri("/users/register")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(body.clone()))
        .unwrap();
    let response = state.api.clone().oneshot(request).await.expect("router");
    let result = match read_json(response).await {
        Ok(d) => d,
        Err(r) => return r.into_response(),
    };
    // PARITY: api/dash/callbacks/register.py immediately creates a session after registration.
    if result.get(0).and_then(Json::as_bool) == Some(true) {
        return login(State(state), headers, body).await;
    }
    render(
        &state,
        &headers,
        "Registration",
        &format!(
            "<p role=\"status\">{}</p><a href=\"/dash/login\">Log in</a>",
            pretty(&result)
        ),
        &Json::Null,
    )
}
pub(crate) async fn plugin_page(State(state): State<WebState>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches("/dash/");
    let Some(page) = state
        .config
        .pages
        .iter()
        .find(|p| p.path.trim_matches('/') == path)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let data = if page.login_required {
        match session(&state, req.headers()).await {
            Ok(d) => d,
            Err(r) => return r.into_response(),
        }
    } else {
        Json::Null
    };
    let (content, status) = match &page.content {
        PageContent::Html(html) => (html.clone(), StatusCode::OK),
        PageContent::Handler(handler) => {
            handler.render(req.uri().query().unwrap_or(""), &data).await
        }
        PageContent::Route(route) => {
            if !route.starts_with('/')
                || route.starts_with("//")
                || route.starts_with("/dash")
                || route.starts_with("/_mrsm")
            {
                return StatusCode::BAD_GATEWAY.into_response();
            }
            let uri = match req.uri().query() {
                Some(q) => format!("{route}{}{q}", if route.contains('?') { "&" } else { "?" }),
                None => route.clone(),
            };
            let mut request = match Request::builder().uri(uri).body(Body::empty()) {
                Ok(r) => r,
                Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
            };
            if let Some(cookie) = req.headers().get(header::COOKIE) {
                request.headers_mut().insert(header::COOKIE, cookie.clone());
            }
            let response = state.api.clone().oneshot(request).await.expect("router");
            if response.status().is_redirection() {
                return response;
            }
            let status = response.status();
            let bytes = match axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
            };
            (String::from_utf8_lossy(&bytes).into_owned(), status)
        }
    };
    let mut response = render_options(
        &state,
        req.headers(),
        path,
        &content,
        &data,
        page.skip_navbar,
        page.dark_theme,
    );
    *response.status_mut() = status;
    response
}
pub(crate) async fn logs(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let data = match session(&state, &headers).await {
        Ok(d) => d,
        Err(r) => return r.into_response(),
    };
    let auth_headers = match api_headers(&state, &data).await {
        Ok(h) => h,
        Err(r) => return r.into_response(),
    };
    if let Err(e) = auth::scoped_auth(&state.server, &auth_headers, &["jobs:read"]).await {
        return e.into_response();
    }
    if let Err(r) = get_json(&state, &data, &format!("/logs/{}", encode(&name))).await {
        return r.into_response();
    }
    let Some(monitor) = state.server.jobs.monitor_logs(&name).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let id = cookie(&headers).unwrap_or_default();
    let stream = futures_util::stream::unfold(
        (monitor, state, id),
        |(mut monitor, state, id)| async move {
            let event = monitor.events.recv().await?;
            if !matches!(state.config.sessions.get(&id).await, Ok(Some(_))) {
                return None;
            }
            let event = match event {
                LogEvent::Line(line) => Event::default().data(line),
                LogEvent::Stopped(result) => Event::default().event("stopped").data(result.message),
                LogEvent::StdinRequested => Event::default().event("stdin").data("Input requested"),
            };
            Some((Ok::<_, Infallible>(event), (monitor, state, id)))
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn pipelines(state: &WebState, data: &Json, query: &Query) -> WebResult<String> {
    let run_id = query.str_or("run_id", "");
    let runs = get_json(
        state,
        data,
        &if run_id.is_empty() {
            "/engine/runs?limit=100".into()
        } else {
            format!("/engine/runs?limit=100&run_id={}", encode(&run_id))
        },
    )
    .await?;
    let mut nodes = BTreeMap::new();
    let mut traces = Vec::new();
    let mut edges = BTreeMap::new();
    let keys = get_json(state, data, "/pipes/keys").await?;
    if let Some(keys) = keys.as_array() {
        for key in keys {
            let Some(key) = key.as_array().filter(|k| k.len() >= 3) else {
                continue;
            };
            let ck = key[0].as_str().unwrap_or("");
            if ck == "mrsm" {
                continue;
            }
            let handle = state
                .server
                .get_pipe(
                    ck,
                    key[1].as_str().unwrap_or(""),
                    key[2].as_str(),
                    None,
                    true,
                )
                .await
                .map_err(IntoResponse::into_response)?;
            let pipe = &handle.pipe;
            let id = pipe.pipe_ref();
            nodes
                .entry(id.clone())
                .or_insert_with(|| json!({"data":{"id":id,"label":pipe.to_string()}}));
            let parents = pipe
                .parents(&state.server.config.pipe_defaults)
                .map_err(|_| {
                    (StatusCode::UNPROCESSABLE_ENTITY, "Invalid parent reference").into_response()
                })?;
            let children = pipe
                .children(&state.server.config.pipe_defaults)
                .map_err(|_| {
                    (StatusCode::UNPROCESSABLE_ENTITY, "Invalid child reference").into_response()
                })?;
            for (parent, child) in parents
                .into_iter()
                .flatten()
                .map(|p| (p, pipe.clone()))
                .chain(children.into_iter().flatten().map(|c| (pipe.clone(), c)))
            {
                let source = parent.pipe_ref();
                let target = child.pipe_ref();
                nodes
                    .entry(source.clone())
                    .or_insert_with(|| json!({"data":{"id":source,"label":parent.to_string()}}));
                nodes
                    .entry(target.clone())
                    .or_insert_with(|| json!({"data":{"id":target,"label":child.to_string()}}));
                edges.insert((source.clone(), target.clone()), json!({"data":{"source":source,"target":target,"label":"Ledger lag unavailable"}}));
            }
        }
    }
    let mut latest = std::collections::HashSet::new();
    if let Some(runs) = runs.as_array() {
        for run in runs {
            let name = run["pipe_ref"]
                .as_str()
                .or(run["run_id"].as_str())
                .unwrap_or("run");
            // The API returns newest runs first; older history must not replace node status.
            if latest.insert(name.to_string()) {
                nodes.insert(name.to_string(), json!({"data":{"id":name,"label":format!("{}: {}", name, run["status"].as_str().unwrap_or("unknown"))}}));
            }
            for (label, start_key, end_key) in [
                ("Actual", "started", "finished"),
                ("Planned", "planned_started", "planned_finished"),
            ] {
                let Some(start) = run[start_key]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                else {
                    continue;
                };
                let Some(end) = run[end_key]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                else {
                    continue;
                };
                traces.push(json!({"type":"bar","orientation":"h","name":format!("{name} {label}"),"y":[name],"base":[start.to_rfc3339()],"x":[(end-start).num_milliseconds()]}));
            }
        }
    }
    let figure = json!({"data":traces,"layout":{"title":"Run timing","xaxis":{"type":"date"},"barmode":"overlay"}});
    let graph: Vec<_> = nodes.into_values().chain(edges.into_values()).collect();
    Ok(format!(
        "<form method=\"get\"><label>Run ID <input name=\"run_id\" value=\"{}\"></label><button>Filter runs</button></form><h2>Pipeline dependencies</h2><div data-mrsm-graph=\"{}\" role=\"img\" aria-label=\"Pipeline dependencies and run status\"></div><h2>Timeline</h2><div data-mrsm-figure=\"{}\" role=\"img\" aria-label=\"Planned and actual run timing\"></div><h2>Runs</h2>{}",
        escape(&run_id),
        escape(&json!({"elements":graph}).to_string()),
        escape(&figure.to_string()),
        table(&runs)
    ))
}

async fn plugins(state: &WebState, data: &Json, query: &Query) -> WebResult<String> {
    let search = query.str_or("search_term", "");
    let plugins = get_json(
        state,
        data,
        &format!("/plugins?search_term={}", encode(&search)),
    )
    .await?;
    let mut content = format!(
        "<form method=\"get\"><label>Search plugins <input name=\"search_term\" value=\"{}\"></label><button>Search</button></form>",
        escape(&search)
    );
    if let Some(plugins) = plugins.as_array() {
        for name in plugins.iter().filter_map(Json::as_str) {
            content.push_str(&format!("<article><h2>{}</h2>", escape(name)));
            for part in ["version", "attributes"] {
                if let Ok(value) =
                    get_json(state, data, &format!("/plugins/{}/{part}", encode(name))).await
                {
                    content.push_str(&format!("<h3>{part}</h3><pre>{}</pre>", pretty(&value)));
                }
            }
            content.push_str(&form(data, "plugin-install", name, "", "Install plugin"));
            content.push_str(&form(
                data,
                "plugin-delete",
                name,
                "",
                "Delete repository plugin",
            ));
            content.push_str("</article>");
        }
    }
    Ok(content)
}
