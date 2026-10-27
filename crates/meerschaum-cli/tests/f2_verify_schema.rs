//! `verify schema` (rc15 Q32) and protected instances (Q20) through the dispatcher, on a SQLite
//! instance in a temporary root. One test: the protected allowance is process-wide.

use std::path::Path;

use meerschaum_cli::dispatch::{self, RecordingHost, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

fn no_env(_: &str) -> Option<String> {
    None
}

fn run(root: &Path, db: &Path, sysargs: &[&str], answers: &[&str]) -> (SuccessTuple, String) {
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 120, &mut out, no_env);
        let mut opts = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.join("plugins").to_string_lossy().into_owned(),
            ),
            (
                "MRSM_SQL_F2".into(),
                format!("sqlite:///{}", db.to_string_lossy()),
            ),
            (
                "MRSM_SQL_F2P".into(),
                format!("sqlite:///{}?protected=true", db.to_string_lossy()),
            ),
        ]);
        opts.allow_writes = true;
        opts.home = root.to_path_buf();
        rt.config = Some(Config::new(opts).unwrap());
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 120,
            cols: 120,
            no_color: true,
        });
        let host = RecordingHost {
            input: answers.iter().map(|a| a.to_string()).collect(),
            ..Default::default()
        };
        rt.host = Box::new(host);
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8(out).unwrap())
}

fn exec_sql(db: &Path, statements: &[&str]) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let conn = meerschaum_connector_sql::SqlConnector::from_uri(
            "f2_setup",
            &format!("sqlite:///{}", db.to_string_lossy()),
        )
        .await
        .unwrap();
        for s in statements {
            assert!(conn.exec(s).await.is_some(), "{s}");
        }
    });
}

#[test]
fn verify_schema_and_protected_instances() {
    assert!(std::env::var("MRSM_ALLOW_PROTECTED").is_err());
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let db = root.join("f2.db");
    let keys = ["-c", "sql:f2", "-m", "weather", "-i", "sql:f2"];
    let with = |extra: &[&'static str]| -> Vec<&str> {
        let mut v: Vec<&str> = keys.to_vec();
        v.extend_from_slice(extra);
        v
    };

    let mut args = vec!["register", "pipes"];
    args.extend(with(&[
        "-P",
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"temp": "float", "note": "string"}}"#,
        "-y",
    ]));
    let (result, out) = run(root, &db, &args, &[]);
    assert!(result.success, "{result:?}\n{out}");
    exec_sql(
        &db,
        &[
            "CREATE TABLE sql_f2_weather (ts DATETIME, temp INTEGER)",
            "INSERT INTO sql_f2_weather VALUES ('2024-01-01 00:00:00', 10)",
        ],
    );

    // `--dry-run` prints the SQL and changes nothing.
    let mut args = vec!["verify", "schema"];
    args.extend(with(&["--dry-run"]));
    let (result, out) = run(root, &db, &args, &[]);
    assert!(result.success, "{result:?}\n{out}");
    assert!(
        out.contains("CREATE TABLE \"sql_f2_weather_mrsm_verify\""),
        "{out}"
    );
    assert!(out.contains("RENAME TO \"sql_f2_weather\""), "{out}");

    // `--noask` applies the safe change (the added column) and refuses the rebuild.
    let mut args = vec!["verify", "schemas"];
    args.extend(with(&["--noask"]));
    let (result, out) = run(root, &db, &args, &[]);
    assert!(!result.success, "{result:?}\n{out}");
    assert!(out.contains("added column 'note'"), "{out}");
    assert!(
        result.message.contains("1 needs confirmation"),
        "{result:?}"
    );

    // Declining leaves the table alone.
    let mut args = vec!["verify", "schema"];
    args.extend(with(&[]));
    let (result, out) = run(root, &db, &args, &["n"]);
    assert!(!result.success, "{result:?}\n{out}");
    assert!(out.contains("rebuilds the table"), "{out}");
    assert!(out.contains("unchanged"), "{out}");

    // On the protected twin, `-y` is not enough.
    let mut prot = vec![
        "verify", "schema", "-c", "sql:f2", "-m", "weather", "-i", "sql:f2p",
    ];
    prot.push("-y");
    // (The refusal itself is a warning on stderr; the core tests pin its text.)
    let (result, out) = run(root, &db, &prot, &[]);
    assert!(!result.success, "{result:?}\n{out}");
    assert!(result.message.contains("1 failed"), "{result:?}");
    let clear = ["clear", "pipes", "-c", "sql:f2", "-i", "sql:f2p", "-y"];
    let (result, out) = run(root, &db, &clear, &[]);
    assert!(!result.success, "{result:?}\n{out}");

    // `--allow-protected` allows it (for that action only).
    prot.push("--allow-protected");
    let (result, out) = run(root, &db, &prot, &[]);
    assert!(result.success, "{result:?}\n{out}");
    assert!(out.contains("changed 'temp': INTEGER -> FLOAT"), "{out}");
    assert!(!meerschaum_core::protected::allowed_by_flag());

    // `verify schemata` is the same action; the table now matches.
    let mut args = vec!["verify", "schemata"];
    args.extend(with(&[]));
    let (result, out) = run(root, &db, &args, &[]);
    assert!(result.success, "{result:?}\n{out}");
    assert!(out.contains("matches its dtypes"), "{out}");

    // `mrsm sql` on the protected twin: a read runs (read-only); a write is refused, whether
    // it would run as a statement or as a read (`... AS SELECT`).
    exec_sql(
        &db,
        &[
            "CREATE TABLE f2_canary (x INTEGER)",
            "INSERT INTO f2_canary VALUES (41), (42)",
        ],
    );
    let (result, out) = run(root, &db, &["sql", "f2p", "SELECT x FROM f2_canary"], &[]);
    assert!(result.success, "{result:?}\n{out}");
    assert!(out.contains("42"), "{out}");
    for statement in [
        "DELETE FROM f2_canary",
        "DROP TABLE f2_canary",
        "CREATE TABLE f2_copy AS SELECT * FROM f2_canary",
        "WITH d AS (SELECT 1) DELETE FROM f2_canary",
    ] {
        let (result, out) = run(root, &db, &["sql", "f2p", statement], &[]);
        assert!(!result.success, "{statement}: {result:?}\n{out}");
        assert_eq!(
            result.message,
            "Refusing to execute SQL on protected instance 'sql:f2p'. \
             Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1).",
            "{statement}"
        );
    }
    assert_eq!(rows(&db, "f2_canary"), 2);
    let (result, out) = run(
        root,
        &db,
        &["sql", "f2p", "DELETE FROM f2_canary", "--allow-protected"],
        &[],
    );
    assert!(result.success, "{result:?}\n{out}");
    assert_eq!(rows(&db, "f2_canary"), 0);
    assert!(!meerschaum_core::protected::allowed_by_flag());
}

/// The number of rows in `table`.
fn rows(db: &Path, table: &str) -> i64 {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let conn = meerschaum_connector_sql::SqlConnector::from_uri(
            "f2_count",
            &format!("sqlite:///{}", db.to_string_lossy()),
        )
        .await
        .unwrap();
        conn.value(&format!("SELECT COUNT(*) FROM {table}"))
            .await
            .and_then(|v| v.as_i64())
            .unwrap()
    })
}
