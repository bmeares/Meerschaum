//! The built-in MCP prompts (`meerschaum/mcp/_prompts.py`): workflows a client surfaces to the
//! user. Each returns a single user message; the text is v4's, verbatim.

use meerschaum_core::{Map, Value};

use super::protocol::{map_of, s};

const BOOTSTRAP: &str = r#"Help me register a new Meerschaum pipe for this data source:

{source}

{instance_line}

Work in this order and stop for my confirmation before writing anything:

1. Read the `mrsm://docs` resource if you have not already, then inspect what already exists —
   `list_pipes` to check for an overlapping pipe, and `read_sql` or `mrsm://connectors` to look at
   the source's shape if it is a SQL connector.
2. Propose the three pipe keys (`connector_keys`, `metric_key`, `location_key`), explaining what
   each one means for this source.
3. Propose the index columns: which column is the `datetime` axis that drives incremental sync,
   which columns form the `id`/`primary` uniqueness key, and why.
4. Propose the dtypes, calling out anything that needs an explicit Meerschaum dtype
   (`numeric`, `json`, `uuid`, `bytes`, `geometry[srid]`) rather than a Pandas default.
5. Show me the exact `register_pipe` arguments you intend to use, then wait for my go-ahead.
6. After I confirm, register it, run `sync_pipe`, and report the resulting `get_pipe_stats`."#;

const DIAGNOSE: &str = r#"Diagnose the sync behavior of the Meerschaum pipe {keys_line}.{symptom_line}

Gather evidence before forming a hypothesis:

1. `get_pipe_attributes` — read its `columns` (especially the `datetime` axis), `dtypes`,
   `fetch` config (including `backtrack_minutes`), `verify` config, and whether `upsert`,
   `static`, or `enforce` are set.
2. `get_pipe_stats` — confirm the table exists, and compare its rowcount, newest and oldest sync
   times, and actual database column types against what the parameters claim.
3. `get_pipe_data` with `backtrack_minutes` — look at the most recent rows actually stored.
4. If it is a SQL-connector pipe, use `read_sql` against the source to compare what the source
   currently returns for the same window.

Then tell me:

- The most likely cause, and which specific piece of evidence supports it.
- Whether a plain `sync_pipe`, a `verify_pipe` over a range, or a `deduplicate_pipe` is the right
  remedy — and what each would do to the stored rows.
- Anything in the pipe's configuration that will cause this again.

Do not modify the pipe or its data. Recommend the fix and let me run it."#;

const EXPLAIN: &str = r#"Explain the Meerschaum pipe {keys_line} to someone who has not seen this instance before.

Read `get_pipe_attributes`, `get_pipe_stats`, and a small sample via `get_pipe_data`
(`limit` 10, `backtrack_minutes` set) — then cover:

- What this data stream is, judging from its keys, tags, and columns.
- Where it comes from, and how new data arrives (its source connector and fetch config).
- Its time axis and grain: which column is the `datetime` index, what a single row represents,
  and which columns make a row unique.
- Its actual size and freshness — rowcount, and how far behind the newest row is.
- Any configuration worth knowing about: `upsert`, `static`, `autoincrement`, `autotime`,
  a non-default `precision`, or partitioning settings.
- Anything that looks inconsistent, such as declared dtypes disagreeing with the database's
  real column types, or a datetime axis with no recent rows.

Do not modify anything."#;

/// Substitute `{name}` placeholders in one pass (values are not re-scanned), like an f-string.
fn fill(template: &str, values: &[(&str, String)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}').and_then(|close| {
            let name = &after[..close];
            values
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (close, v))
        }) {
            Some((close, v)) => {
                out.push_str(v);
                rest = &after[close + 1..];
            }
            None => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// `str(value)` of a prompt argument (`None` when absent).
fn arg(args: &Map, key: &str) -> Option<String> {
    match args.get(key) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.py_str()),
    }
}

/// `_user_message(text)`.
fn user_message(text: String) -> Value {
    Value::List(vec![Value::Map(map_of([
        ("role", s("user")),
        (
            "content",
            Value::Map(map_of([("type", s("text")), ("text", s(text))])),
        ),
    ]))])
}

/// Render a built-in prompt's messages, or `None` for an unknown prompt.
pub fn render(name: &str, args: &Map) -> Option<Value> {
    // `{location_key or 'None'}`: a falsy value renders as `None`.
    let truthy = |k: &str| args.get(k).filter(|v| v.truthy()).map(Value::py_str);
    let keys_line = || {
        format!(
            "`{}` / `{}` / `{}`",
            arg(args, "connector_keys").unwrap_or_default(),
            arg(args, "metric_key").unwrap_or_default(),
            truthy("location_key").unwrap_or_else(|| "None".into()),
        )
    };
    let text = match name {
        "bootstrap_pipe" => {
            let instance_line = match truthy("instance_keys") {
                Some(ik) => format!("Register it on the instance `{ik}`."),
                None => "Register it on the default instance.".into(),
            };
            fill(
                BOOTSTRAP,
                &[
                    ("source", arg(args, "source").unwrap_or_default()),
                    ("instance_line", instance_line),
                ],
            )
        }
        "diagnose_sync_failure" => {
            let symptom_line = match truthy("symptom") {
                Some(sym) => format!("\n\nThe symptom I am seeing: {sym}"),
                None => String::new(),
            };
            fill(
                DIAGNOSE,
                &[("keys_line", keys_line()), ("symptom_line", symptom_line)],
            )
        }
        "explain_pipe" => fill(EXPLAIN, &[("keys_line", keys_line())]),
        _ => return None,
    };
    Some(user_message(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_is_single_pass() {
        assert_eq!(
            fill("a {x} {y} {z}", &[("x", "{y}".into()), ("y", "2".into())]),
            "a {y} 2 {z}"
        );
    }

    #[test]
    fn explain_renders_keys() {
        let mut args = Map::new();
        args.insert("connector_keys".into(), s("demo"));
        args.insert("metric_key".into(), s("temperature"));
        let v = render("explain_pipe", &args).unwrap();
        let text = v.as_list().unwrap()[0]
            .get("content")
            .and_then(|c| c.get("text"))
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        assert!(text.starts_with("Explain the Meerschaum pipe `demo` / `temperature` / `None`"));
    }
}
