//! Test plugin for host calls over the Arrow ABI, written the way a new plugin is: the manifest is
//! built with `Manifest` (no JSON), the version comes from Cargo, rows become a typed frame with
//! `Frame::from_rows`, and the plugin reads and writes pipes, SQL and its own config through the
//! host. All data is synthetic.
//!
//! - `plugin:native_host` pipes sync with this plugin's `sync`: the metric `parent` gets
//!   `plugin_config().rows` (default 3) hourly rows from 2024-01-01; any other metric replaces
//!   `[begin, end)` with the parent's values times `plugin_config().multiplier` (default 10), in
//!   one transaction.
//! - `mrsm native host sql` counts the parent's rows with `read_sql` and runs `exec_sql`.
//! - `mrsm native host configure` writes the plugin's config.
//! - `mrsm native host trim` empties the child's first hour (a `replace` without rows).
//! - `GET /native-host/{metric}/rowcount` and `POST /native-host/{metric}/clear` reach pipes for
//!   the caller.
use meerschaum_plugin::api::{RouteRequest, RouteResponse, RouteSpec};
use meerschaum_plugin::chrono;
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct HostProbe;

const DTYPES: &[(&str, &str)] = &[("ts", "datetime"), ("value", "int")];

fn fail(e: impl std::fmt::Display) -> Error {
    Error::Message(e.to_string())
}

/// The parent's synthetic rows.
fn parent_rows(count: i64) -> Result<Frame> {
    let start = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .map_err(fail)?
        .with_timezone(&chrono::Utc);
    let rows = (0..count).map(|i| {
        json!({
            "ts": Bound::from(start + chrono::Duration::hours(i)).to_string(),
            "value": i,
        })
    });
    Frame::from_rows(rows, DTYPES)
}

fn config_int(key: &str, default: i64) -> Result<i64> {
    Ok(host::plugin_config()?[key].as_i64().unwrap_or(default))
}

impl Plugin for HostProbe {
    const NAME: &'static str = "native_host";

    fn manifest(&self) -> Value {
        Manifest::new()
            .sync()
            .action("native_host_sql", "Count the parent's rows with SQL.")
            .action("native_host_configure", "Write this plugin's config.")
            .action("native_host_trim", "Empty the child's first hour.")
            .api_route(RouteSpec::new(
                "rowcount",
                "GET",
                "/native-host/{metric}/rowcount",
            ))
            .api_route(RouteSpec::new(
                "clear",
                "POST",
                "/native-host/{metric}/clear",
            ))
            .into()
    }

    fn sync(&self, request: &Request, _data: Option<Frame>) -> Result<Value> {
        let pipe = request.pipe()?;
        let result = if pipe.metric_key() == "parent" {
            pipe.sync(parent_rows(config_int("rows", 3)?)?)?
        } else {
            let (begin, end) = (request.bound("begin")?, request.bound("end")?);
            let parent = host::pipe(pipe.connector_keys(), "parent", None, None)?;
            let multiplier = config_int("multiplier", 10)?;
            let rows: Vec<Value> = parent
                .get_data(begin.clone(), end.clone(), None)?
                .to_rows()
                .into_iter()
                .map(|mut row| {
                    row["value"] = json!(row["value"].as_i64().unwrap_or(0) * multiplier);
                    row
                })
                .collect();
            pipe.replace(Frame::from_rows(rows, DTYPES)?, begin, end)?
        };
        serde_json::to_value(result).map_err(fail)
    }

    fn api_route(&self, request: &RouteRequest) -> Result<RouteResponse> {
        let metric = request.path_param("metric").unwrap_or("parent");
        let pipe = host::pipe("plugin:native_host", metric, None, None)?;
        Ok(match request.name.as_str() {
            "rowcount" => RouteResponse::json(&json!({
                "rowcount": pipe.get_rowcount(None, None, None)?,
                "exists": pipe.exists()?,
            })),
            _ => RouteResponse::json(&json!(pipe.clear(None, None, None)?)),
        })
    }

    fn call(&self, request: &Request, data: Option<Frame>) -> Result<Reply> {
        match (request.method.as_str(), request.context["name"].as_str()) {
            ("sync", _) | ("sync_pipe", _) => self.sync(request, data).map(Reply::json),
            ("api_route", _) => meerschaum_plugin::sdk::call_server_extension(self, request),
            ("action", Some("native_host_sql")) => {
                let instance = request.context["kwargs"]["mrsm_instance"]
                    .as_str()
                    .unwrap_or("sql:local")
                    .to_string();
                let parent = host::pipe("plugin:native_host", "parent", None, Some(&instance))?;
                let count = host::read_sql(
                    &instance,
                    &format!("SELECT COUNT(*) AS n FROM \"{}\"", parent.target()),
                )?;
                let n = count.to_rows()[0]["n"].as_i64().unwrap_or(-1);
                let created = host::exec_sql(
                    &instance,
                    "CREATE TABLE IF NOT EXISTS native_host_marker (id INTEGER)",
                )?;
                Ok(Reply::json(json!([
                    created.success,
                    format!(
                        "parent rows: {n}; columns: {:?}",
                        parent.get_columns_types()?.keys().collect::<Vec<_>>()
                    )
                ])))
            }
            ("action", Some("native_host_trim")) => {
                let child = host::pipe("plugin:native_host", "child", None, None)?;
                let trimmed = child.replace(
                    Vec::<Value>::new(),
                    Some(Bound::from("2024-01-01T01:00:00+00:00")),
                    Some(Bound::from("2024-01-01T02:00:00+00:00")),
                )?;
                serde_json::to_value(trimmed).map(Reply::json).map_err(fail)
            }
            ("action", Some("native_host_configure")) => {
                let written = host::write_plugin_config(&json!({"rows": 4, "multiplier": 100}))?;
                Ok(Reply::json(json!([
                    written.success,
                    format!("config: {}", host::plugin_config()?)
                ])))
            }
            (method, _) => Err(Error::Unsupported(format!("{method} is not implemented"))),
        }
    }
}

meerschaum_plugin::export_plugin!(HostProbe);
