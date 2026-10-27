//! Instance and job actions against recorded v4.1.4 transcripts (milestone 5.0.0b1).
//!
//! `tests/fixtures/actions_transcript.json` is recorded by `gen_actions.py` (see that script):
//! a scripted sequence of `mrsm` commands on a SQLite instance (`sql:local`) in a temporary
//! root, interleaved with SQL seeding and job waits. This test replays the same sequence with
//! the `mrsm` binary in a fresh root, normalizes the output the same way, and compares stdout,
//! stderr and the exit code of every step. Every job daemon started by the replay is killed at
//! the end.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use regex::Regex;
use serde_json::Value;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("actions_transcript.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("JSON")
}

/// `normalize()` in `gen_actions.py`.
fn normalize(text: &str, root: &str) -> String {
    let text = text.replace(root, "<ROOT>");
    let text = Regex::new(r"It took .+? to sync")
        .unwrap()
        .replace_all(&text, "It took <DURATION> to sync")
        .to_string();
    let text = Regex::new(r"PID: \d+")
        .unwrap()
        .replace_all(&text, "PID: <PID>")
        .to_string();
    Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ")
        .unwrap()
        .replace_all(&text, "<TS> | ")
        .to_string()
}

fn run_sql(root: &Path, queries: &[String]) {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_connector_sql::flavor::Flavor;
    use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};
    let db = root.join("sqlite").join("mrsm_local.db");
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    let attrs = ConnectorAttributes {
        label: Some("seed".into()),
        flavor: Some(Flavor::Sqlite),
        database: Some(db.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let conn = SqlConnector::connect("seed", spec).await.unwrap();
        let results = conn.exec_queries(queries, true, true).await;
        assert!(
            results.iter().all(Option::is_some),
            "seed queries failed: {queries:?}"
        );
    });
}

/// `wait_jobs()` in `gen_actions.py`.
fn wait_jobs(root: &Path, names: &[String]) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for name in names {
        let path = root.join("jobs").join(name).join("properties.json");
        while Instant::now() < deadline {
            let done = std::fs::read_to_string(&path)
                .ok()
                .and_then(|t| serde_json::from_str::<Value>(&t).ok())
                .is_some_and(|p| {
                    let ended = p["process"]["ended"].as_str().unwrap_or_default();
                    let began = p["process"]["began"].as_str().unwrap_or_default();
                    !ended.is_empty() && (began.is_empty() || ended >= began)
                });
            if done {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    std::thread::sleep(Duration::from_millis(500));
}

/// Kill every process whose environment has `MRSM_ROOT_DIR=<root>` (job supervisors and
/// workers detach from the test's process group).
fn kill_root_processes(root: &Path) {
    let marker = format!("MRSM_ROOT_DIR={}", root.display());
    let me = std::process::id().to_string();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.chars().all(|c| c.is_ascii_digit()) || name == me {
            continue;
        }
        let Ok(environ) = std::fs::read(entry.path().join("environ")) else {
            continue;
        };
        let found = environ
            .split(|b| *b == 0)
            .any(|var| var == marker.as_bytes());
        if found {
            let _ = Command::new("kill").args(["-9", &name]).status();
        }
    }
}

struct KillOnDrop(PathBuf);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        kill_root_processes(&self.0);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn actions_match_v4_transcripts() {
    let fixture = fixture();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let _guard = KillOnDrop(root.clone());
    let root_str = root.to_string_lossy().to_string();
    let columns = fixture["columns"].as_i64().unwrap_or(100).to_string();
    let bin = env!("CARGO_BIN_EXE_mrsm-cli");

    let mut failures: Vec<String> = Vec::new();
    for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
        let strings = |v: &Value| -> Vec<String> {
            v.as_array()
                .map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect())
                .unwrap_or_default()
        };
        match step["kind"].as_str().unwrap() {
            "sql" => run_sql(&root, &strings(&step["queries"])),
            "wait_jobs" => wait_jobs(&root, &strings(&step["names"])),
            "sleep" => std::thread::sleep(Duration::from_secs_f64(
                step["seconds"].as_f64().unwrap_or(1.0),
            )),
            "mrsm" => {
                let args = strings(&step["args"]);
                let mut cmd = Command::new(bin);
                cmd.args(&args)
                    .current_dir(tmp.path())
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                for (k, _) in std::env::vars() {
                    if k.starts_with("MRSM_")
                        || matches!(
                            k.as_str(),
                            "NO_COLOR" | "FORCE_COLOR" | "TERM" | "COLUMNS" | "LINES" | "RUST_LOG"
                        )
                    {
                        cmd.env_remove(k);
                    }
                }
                // v4 styled its output even off a terminal (the transcripts carry its escape
                // sequences); v5 prints plain text there unless `FORCE_COLOR` is set (rc15).
                cmd.env("MRSM_ROOT_DIR", &root)
                    .env("MRSM_PLUGINS_DIR", &plugins)
                    .env("COLUMNS", &columns)
                    .env("LINES", "40")
                    .env("FORCE_COLOR", "1");
                if let Some(env) = step["env"].as_object() {
                    for (k, v) in env {
                        cmd.env(
                            k,
                            v.as_str().unwrap_or_default().replace("{root}", &root_str),
                        );
                    }
                }
                let mut child = cmd.spawn().expect("spawn mrsm");
                {
                    use std::io::Write;
                    let mut stdin = child.stdin.take().unwrap();
                    let _ = stdin.write_all(step["stdin"].as_str().unwrap_or("").as_bytes());
                }
                let out = child.wait_with_output().expect("mrsm output");
                let stdout = normalize(&String::from_utf8_lossy(&out.stdout), &root_str);
                let stderr = normalize(&String::from_utf8_lossy(&out.stderr), &root_str);
                let rc = out.status.code().unwrap_or(-1) as i64;
                let compare = strings(&step["compare"]);
                let mut diffs = Vec::new();
                if compare.iter().any(|c| c == "stdout") && stdout != step["stdout"] {
                    diffs.push(format!(
                        "  stdout:\n--- v4\n{}\n--- rust\n{stdout}",
                        step["stdout"].as_str().unwrap()
                    ));
                }
                if compare.iter().any(|c| c == "stderr") && stderr != step["stderr"] {
                    diffs.push(format!(
                        "  stderr:\n--- v4\n{}\n--- rust\n{stderr}",
                        step["stderr"].as_str().unwrap()
                    ));
                }
                // Intended wave-18 difference (PARITY W18-SW-005): `--nopretty` exits 1 when the
                // result is a failure; v4 exited 0 whatever the command returned. The overlay
                // names each affected step; nothing else may differ.
                let expected_rc = match args.join(" ").as_str() {
                    "show tags -i sql:local --nopretty" => Some(1),
                    _ => step["rc"].as_i64(),
                };
                if compare.iter().any(|c| c == "rc") && Some(rc) != expected_rc {
                    diffs.push(format!("  rc: v4={} rust={rc}", step["rc"]));
                }
                if !diffs.is_empty() {
                    failures.push(format!(
                        "[{i}] mrsm {}\n{}",
                        args.join(" "),
                        diffs.join("\n")
                    ));
                }
            }
            other => panic!("unknown step kind {other}"),
        }
    }
    assert!(
        failures.is_empty(),
        "{} step(s) differ from v4:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
