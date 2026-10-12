//! Cross-implementation test: the same synthetic pipes are synced by the Python implementation
//! (`tests/fixtures/cross_impl/cross_impl.py`, run as a subprocess against this repository) into
//! one SQLite file and by Rust into another. The test asserts identical schemas (`sqlite_master`
//! DDL and `PRAGMA table_info`), row counts and stored values (with SQLite storage classes), and
//! that each implementation reads, syncs into and queries the other's database with the same
//! results.
//!
//! Requires the Python reference environment: `MRSM_TEST_PYTHON` (default
//! `~/.config/meerschaum/venvs/mrsm/bin/python`). Skips when it is missing.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use meerschaum_sql::SqlConnector;
use serde_json::{Value as J, json};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cross_impl")
}

fn python() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("MRSM_TEST_PYTHON") {
        return Some(PathBuf::from(p));
    }
    let home = std::env::var("HOME").ok()?;
    let p = Path::new(&home).join(".config/meerschaum/venvs/mrsm/bin/python");
    p.exists().then_some(p)
}

struct Py {
    exe: PathBuf,
    root_dir: PathBuf,
    plugins_dir: PathBuf,
}

impl Py {
    fn new(exe: PathBuf, work: &Path) -> Py {
        let root_dir = work.join("mrsm_root");
        let plugins_dir = work.join("mrsm_plugins");
        std::fs::create_dir_all(&root_dir).unwrap();
        std::fs::create_dir_all(&plugins_dir).unwrap();
        Py {
            exe,
            root_dir,
            plugins_dir,
        }
    }

    fn run(&self, args: &[&str]) -> J {
        let out = Command::new(&self.exe)
            .arg(fixtures().join("cross_impl.py"))
            .args(args)
            .current_dir(repo_root())
            .env("PYTHONPATH", repo_root())
            .env("MRSM_ROOT_DIR", &self.root_dir)
            .env("MRSM_PLUGINS_DIR", &self.plugins_dir)
            .env("PYTHONHASHSEED", "0")
            .output()
            .expect("python runs");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let last = stdout
            .lines()
            .rev()
            .find(|l| l.trim_start().starts_with(['[', '{']));
        match last {
            Some(l) => serde_json::from_str(l).expect("JSON output"),
            None => panic!(
                "python {args:?} failed:\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&out.stderr)
            ),
        }
    }
}

/// Decode the scenario's tagged values (`$dt`, `$date`, `$dec`, `$uuid`, `$bytes`).
fn decode(v: &J) -> KValue {
    match v {
        J::Null => KValue::Null,
        J::Bool(b) => KValue::Bool(*b),
        J::Number(n) => match n.as_i64() {
            Some(i) => KValue::Int(i),
            None => KValue::Float(n.as_f64().unwrap()),
        },
        J::String(s) => KValue::Str(s.clone()),
        J::Object(o) if o.len() == 1 && o.keys().next().unwrap().starts_with('$') => {
            let (tag, val) = o.iter().next().unwrap();
            let s = val.as_str().unwrap();
            match tag.as_str() {
                "$dt" => {
                    let dt = chrono::DateTime::parse_from_rfc3339(s).unwrap();
                    KValue::Datetime(DateTimeValue::utc(dt.naive_utc()))
                }
                "$date" => KValue::Date(chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()),
                "$dec" => KValue::Decimal(meerschaum_kernel::Decimal::parse(s).unwrap()),
                "$uuid" => KValue::Uuid(uuid::Uuid::parse_str(s).unwrap()),
                "$bytes" => KValue::Bytes(
                    (0..s.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                        .collect(),
                ),
                other => panic!("unknown tag {other}"),
            }
        }
        other => KValue::Json(other.clone()),
    }
}

fn scenario() -> J {
    let text = std::fs::read_to_string(fixtures().join("scenario.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}

fn spec_pipe(conn: &SqlConnector, spec: &J) -> Pipe {
    let params = Value::from_json_str(&spec["parameters"].to_string())
        .unwrap()
        .to_map();
    Pipe::builder(
        spec["connector_keys"].as_str().unwrap(),
        spec["metric_key"].as_str().unwrap(),
    )
    .location(spec["location_key"].as_str())
    .instance(&conn.keys())
    .parameters(params)
    .build()
    .unwrap()
}

fn spec_key(spec: &J) -> String {
    format!(
        "{}:{}:{}",
        spec["connector_keys"].as_str().unwrap(),
        spec["metric_key"].as_str().unwrap(),
        spec["location_key"].as_str().unwrap_or("None")
    )
}

async fn connect(db: &Path) -> SqlConnector {
    SqlConnector::from_uri("xi_rs", &format!("sqlite:///{}", db.display()))
        .await
        .unwrap()
}

async fn rust_sync(db: &Path, phase: usize) -> Vec<(bool, String)> {
    let conn = connect(db).await;
    let sc = scenario();
    let mut out = Vec::new();
    for spec in sc["pipes"].as_array().unwrap() {
        let pipe = spec_pipe(&conn, spec);
        if spec["inplace"].as_bool() == Some(true) {
            // `Pipe.sync()` registers the pipe before syncing in place.
            if conn.get_pipe_id(&pipe).await.unwrap().is_none() {
                let reg = conn.register_pipe(&pipe).await;
                assert!(reg.success, "{}", reg.message);
            }
            let res = conn
                .sync_pipe_inplace(
                    &pipe,
                    None,
                    meerschaum_sql::queries::metadef::Bound::Default,
                    meerschaum_sql::queries::metadef::Bound::None,
                    true,
                )
                .await;
            out.push((res.success, res.message));
            continue;
        }
        let docs = spec["phases"][phase]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                d.as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), decode(v)))
                    .collect()
            })
            .collect();
        let res = conn
            .sync(&pipe, PipeInput::Docs(docs), &SyncOptions::default())
            .await;
        out.push((res.success, res.message));
    }
    conn.driver().close().await;
    out
}

fn normalize(v: &KValue) -> J {
    if v.is_na() {
        J::Null
    } else {
        J::String(v.py_str())
    }
}

async fn rust_read(db: &Path) -> J {
    let conn = connect(db).await;
    let sc = scenario();
    let mut out = serde_json::Map::new();
    for spec in sc["pipes"].as_array().unwrap() {
        let pipe = spec_pipe(&conn, spec);
        let key = spec_key(spec);
        let docs = conn
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .map(|df| {
                J::Array(
                    meerschaum_connector::pipe_ops::frame_to_docs(&df)
                        .iter()
                        .map(|d| {
                            J::Object(d.iter().map(|(k, v)| (k.clone(), normalize(v))).collect())
                        })
                        .collect(),
                )
            })
            .unwrap_or(J::Null);
        out.insert(key.clone(), docs);
        let sp = conn.sql_pipe(&pipe).unwrap();
        let state = conn.pipe_state(&sp).await;
        let st = conn.get_sync_time(&pipe, None, true, false).await.unwrap();
        let st = conn.parse_date_bound(&sp, &state.dtypes, st).unwrap();
        out.insert(
            format!("{key}:sync_time"),
            st.map(|b| J::String(b.py_str())).unwrap_or(J::Null),
        );
        out.insert(
            format!("{key}:rowcount"),
            json!(
                conn.get_pipe_rowcount(&pipe, &DataQuery::default())
                    .await
                    .unwrap()
            ),
        );
        out.insert(
            format!("{key}:columns_types"),
            serde_json::to_value(conn.get_pipe_columns_types(&pipe).await.unwrap()).unwrap(),
        );
    }
    conn.driver().close().await;
    J::Object(out)
}

/// Remove the parts of a dump that legitimately differ between runs (temporary-table
/// bookkeeping rows carry timestamps and random names).
///
/// PARITY: v4 orders the columns of composite indices (and their `IX_<target>_<cols>` names)
/// by iterating Python `set`s, so the order depends on `PYTHONHASHSEED`; Rust keeps the
/// `columns` order (see `queries/index.rs`). Index objects are compared with their column
/// lists sorted.
fn stable_dump(mut d: J) -> J {
    if let Some(t) = d["tables"].get_mut("mrsm_temp_tables") {
        t["rows"] = J::Null;
        t["count"] = J::Null;
    }
    if let Some(objects) = d["objects"].as_array_mut() {
        for obj in objects.iter_mut() {
            if obj[0] != "index" {
                continue;
            }
            let (Some(name), Some(tbl)) = (obj[1].as_str(), obj[2].as_str()) else {
                continue;
            };
            let prefix = format!("IX_{tbl}_");
            let Some(cols) = name.strip_prefix(&prefix) else {
                continue;
            };
            let mut parts: Vec<&str> = cols.split('_').collect();
            parts.sort();
            let new_name = format!("{prefix}{}", parts.join("_"));
            if let Some(sql) = obj[3].as_str() {
                let sql = sql.replacen(name, &new_name, 1);
                if let (Some(open), Some(close)) = (sql.rfind('('), sql.rfind(')')) {
                    let mut cols: Vec<&str> = sql[open + 1..close].split(", ").collect();
                    cols.sort();
                    obj[3] = J::String(format!("{}({})", &sql[..open], cols.join(", ")));
                }
            }
            obj[1] = J::String(new_name);
        }
        objects.sort_by_key(|o| o.to_string());
    }
    d
}

fn diff(label: &str, a: &J, b: &J) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(path: String, a: &J, b: &J, out: &mut Vec<String>) {
        match (a, b) {
            (J::Object(x), J::Object(y)) => {
                for k in x.keys().chain(y.keys().filter(|k| !x.contains_key(*k))) {
                    walk(
                        format!("{path}.{k}"),
                        x.get(k).unwrap_or(&J::Null),
                        y.get(k).unwrap_or(&J::Null),
                        out,
                    );
                }
            }
            (J::Array(x), J::Array(y)) if x.len() == y.len() => {
                for (i, (p, q)) in x.iter().zip(y).enumerate() {
                    walk(format!("{path}[{i}]"), p, q, out);
                }
            }
            _ if a != b => out.push(format!("{path}:\n    python: {a}\n    rust:   {b}")),
            _ => {}
        }
    }
    walk(label.to_string(), a, b, &mut out);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_implementation_sqlite() {
    let Some(exe) = python() else {
        eprintln!("Skipping: no Python reference environment (set MRSM_TEST_PYTHON).");
        return;
    };
    let work = std::env::temp_dir().join(format!("_mrsm_rs_xi_{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&work).unwrap();
    let py = Py::new(exe, &work);
    let scenario_path = fixtures().join("scenario.json");
    let sp = scenario_path.to_str().unwrap();
    let db = |name: &str| work.join(name);
    let s = |p: PathBuf| p.to_str().unwrap().to_string();
    let mut failures: Vec<String> = Vec::new();

    // Phase 0: each implementation creates its own database.
    let py_res = py.run(&["sync", &s(db("py.db")), sp, "0"]);
    let rs_res = rust_sync(&db("rs.db"), 0).await;
    for (i, (ok, msg)) in rs_res.iter().enumerate() {
        assert!(ok, "rust phase 0 pipe {i}: {msg}");
        assert_eq!(
            py_res[i][1].as_str().unwrap(),
            msg,
            "phase 0 message of pipe {i}"
        );
    }
    let py_dump = stable_dump(py.run(&["dump", &s(db("py.db"))]));
    let rs_dump = stable_dump(py.run(&["dump", &s(db("rs.db"))]));
    failures.extend(diff("phase0", &py_dump, &rs_dump));

    // Cross-writes: each implementation syncs phase 1 into the other's database.
    std::fs::copy(db("py.db"), db("py_then_rs.db")).unwrap();
    std::fs::copy(db("rs.db"), db("rs_then_py.db")).unwrap();
    let py_res = py.run(&["sync", &s(db("py.db")), sp, "1"]);
    let rs_res = rust_sync(&db("rs.db"), 1).await;
    for (i, (ok, msg)) in rs_res.iter().enumerate() {
        assert!(ok, "rust phase 1 pipe {i}: {msg}");
        assert_eq!(
            py_res[i][1].as_str().unwrap(),
            msg,
            "phase 1 message of pipe {i}"
        );
    }
    let mixed_rs = rust_sync(&db("py_then_rs.db"), 1).await;
    for (i, (ok, msg)) in mixed_rs.iter().enumerate() {
        assert!(ok, "rust sync into python db, pipe {i}: {msg}");
    }
    let mixed_py = py.run(&["sync", &s(db("rs_then_py.db")), sp, "1"]);
    for (i, r) in mixed_py.as_array().unwrap().iter().enumerate() {
        assert!(
            r[0].as_bool().unwrap(),
            "python sync into rust db, pipe {i}: {r}"
        );
    }

    let py_dump = stable_dump(py.run(&["dump", &s(db("py.db"))]));
    let rs_dump = stable_dump(py.run(&["dump", &s(db("rs.db"))]));
    let py_rs_dump = stable_dump(py.run(&["dump", &s(db("py_then_rs.db"))]));
    let rs_py_dump = stable_dump(py.run(&["dump", &s(db("rs_then_py.db"))]));
    failures.extend(diff("phase1 py.db vs rs.db", &py_dump, &rs_dump));
    failures.extend(diff("phase1 py.db vs py_then_rs.db", &py_dump, &py_rs_dump));
    failures.extend(diff("phase1 py.db vs rs_then_py.db", &py_dump, &rs_py_dump));

    // Reads: both implementations read both databases.
    let py_read_py = py.run(&["read", &s(db("py.db")), sp]);
    let py_read_rs = py.run(&["read", &s(db("rs.db")), sp]);
    let rs_read_py = rust_read(&db("py.db")).await;
    let rs_read_rs = rust_read(&db("rs.db")).await;
    failures.extend(diff(
        "read py.db (python vs rust)",
        &py_read_py,
        &rs_read_py,
    ));
    failures.extend(diff(
        "read rs.db (python vs rust)",
        &py_read_rs,
        &rs_read_rs,
    ));
    failures.extend(diff(
        "python reads py.db vs rs.db",
        &py_read_py,
        &py_read_rs,
    ));

    let report = failures.join("\n");
    std::fs::write(work.join("report.txt"), &report).unwrap();
    if failures.is_empty() && std::env::var("MRSM_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&work);
    }
    eprintln!("cross-implementation artifacts: {}", work.display());
    assert!(
        failures.is_empty(),
        "{} difference(s) (artifacts in {}):\n{report}",
        failures.len(),
        work.display()
    );
}
