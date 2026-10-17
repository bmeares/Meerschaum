//! `tests/test_maintenance.py`: sizes, vacuum, analyze, compress and decompress.
mod common;
mod fork_helpers;
use common::*;
use fork_helpers::stress_docs;
use meerschaum_connector::DataQuery;
use meerschaum_core::{Pipe, Value};
use meerschaum_sql::Flavor;
use meerschaum_sql::pipe::SqlPipe;
use meerschaum_sql::queries::ConnectorCtx;
use meerschaum_sql::queries::compress::{
    get_columnstore_policy_query, get_columnstore_settings_query, get_compress_settings,
    get_integer_now_func_queries, normalize_columnstore_cols,
};

const SIZE_FLAVORS: &[&str] = &[
    "timescaledb",
    "postgresql",
    "postgis",
    "mysql",
    "mariadb",
    "mssql",
    "sqlite",
    "geopackage",
];
const VACUUMABLE: &[&str] = &[
    "timescaledb",
    "postgresql",
    "postgis",
    "mysql",
    "mariadb",
    "mssql",
    "sqlite",
    "geopackage",
];
const FULL_VACUUM: &[&str] = &["postgresql", "postgis"];
const RELIABLE_COMPRESS: &[&str] = &["timescaledb", "mysql", "mariadb"];
const COMPRESSIBLE: &[&str] = &["timescaledb", "mysql", "mariadb", "mssql"];

/// `_build_synced_pipe(conn, location)`.
async fn build_synced_pipe(c: &Ctx, location: &str) -> Pipe {
    let pipe = c.pipe(
        "plugin:stress",
        "test",
        Some(location),
        r#"{"columns": {"datetime": "datetime", "id": "id"}, "fetch": {"rows": 100, "id": 3}}"#,
    );
    ok(c.sync(
        &pipe,
        stress_docs(
            naive(2023, 1, 1, 0, 0, 0),
            naive(2023, 1, 2, 0, 0, 0),
            100,
            3,
        ),
    )
    .await);
    pipe
}

async fn rows(c: &Ctx, pipe: &Pipe) -> Option<i64> {
    c.rowcount(pipe, DataQuery::default()).await
}

flavor_tests!(test_get_size, |c: Ctx| async move {
    let pipe = build_synced_pipe(&c, "maintenance_size").await;
    let size = c.conn.get_pipe_size(&pipe).await;
    if SIZE_FLAVORS.contains(&c.flavor) {
        assert!(size.is_some_and(|s| s > 0), "{size:?}");
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_vacuum, |c: Ctx| async move {
    let pipe = build_synced_pipe(&c, "maintenance_vacuum").await;
    let before = rows(&c, &pipe).await;
    let res = c.conn.vacuum_pipe(&pipe, false).await;
    if VACUUMABLE.contains(&c.flavor) {
        assert!(res.success, "{}", res.message);
        assert_eq!(rows(&c, &pipe).await, before);
    } else {
        assert!(!res.success);
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_vacuum_full, |c: Ctx| async move {
    if !FULL_VACUUM.contains(&c.flavor) {
        return;
    }
    let pipe = build_synced_pipe(&c, "maintenance_vacuum_full").await;
    let before = rows(&c, &pipe).await;
    ok(c.conn.vacuum_pipe(&pipe, true).await);
    assert_eq!(rows(&c, &pipe).await, before);
    c.delete(&pipe).await;
});

flavor_tests!(test_analyze, |c: Ctx| async move {
    let pipe = build_synced_pipe(&c, "maintenance_analyze").await;
    let before = rows(&c, &pipe).await;
    let res = c.conn.analyze_pipe(&pipe).await;
    if c.conn.flavor().analyzable() {
        let res = ok(res);
        assert!(res.message.starts_with("Analyzed "), "{}", res.message);
    } else {
        // PARITY: v4 does not support analyze_pipe on Oracle.
        assert!(!res.success);
        assert!(res.message.contains("not supported"));
    }
    assert_eq!(rows(&c, &pipe).await, before);
    c.delete(&pipe).await;
});

flavor_tests!(test_compress, |c: Ctx| async move {
    let pipe = build_synced_pipe(&c, "maintenance_compress").await;
    let before = rows(&c, &pipe).await;
    let res = c.conn.compress_pipe(&pipe, false).await;
    if RELIABLE_COMPRESS.contains(&c.flavor) {
        assert!(res.success, "{}", res.message);
        assert_eq!(rows(&c, &pipe).await, before);
    } else if !COMPRESSIBLE.contains(&c.flavor) {
        assert!(!res.success);
        assert_eq!(
            res.message,
            format!(
                "Compression is not supported for flavor '{}'.",
                c.conn.flavor().as_str()
            )
        );
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_decompress, |c: Ctx| async move {
    let pipe = build_synced_pipe(&c, "maintenance_decompress").await;
    let before = rows(&c, &pipe).await;
    if RELIABLE_COMPRESS.contains(&c.flavor) {
        ok(c.conn.compress_pipe(&pipe, false).await);
    }
    let res = c.conn.decompress_pipe(&pipe, false).await;
    if RELIABLE_COMPRESS.contains(&c.flavor) {
        assert!(res.success, "{}", res.message);
        assert_eq!(rows(&c, &pipe).await, before);
        ok(c.conn.compress_pipe(&pipe, false).await);
    } else if !COMPRESSIBLE.contains(&c.flavor) {
        assert!(!res.success);
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_maintenance_missing_table, |c: Ctx| async move {
    // Every operation fails gracefully on a pipe without a table.
    let pipe = c.pipe(
        "plugin:stress",
        "test",
        Some("maintenance_missing"),
        r#"{}"#,
    );
    assert_eq!(c.conn.get_pipe_size(&pipe).await, None);
    let res = c.conn.vacuum_pipe(&pipe, false).await;
    assert!(!res.success);
    assert_eq!(
        res.message,
        format!("{pipe} does not exist; nothing to vacuum.")
    );
    assert!(!c.conn.analyze_pipe(&pipe).await.success);
    assert!(!c.conn.compress_pipe(&pipe, false).await.success);
    assert!(!c.conn.decompress_pipe(&pipe, false).await.success);
});

// test_unsupported_instance_maintenance: skipped — Valkey instances only.
// test_maintenance_actions: skipped — CLI actions.
// test_compress_with_upsert_warns_once: skipped — asserts on Python's `warn` capture; the Rust
// side warns once per pipe via `tracing` (`SqlConnector::apply_compression_policy`).

fn sql_pipe(json: &str, flavor: Flavor) -> SqlPipe {
    let pipe = Pipe::builder("plugin:x", "y")
        .parameters(params(json))
        .build()
        .unwrap();
    SqlPipe::new(pipe, flavor).unwrap()
}

#[test]
fn test_normalize_columnstore_cols() {
    let v = |j: &str| -> Value { serde_json::from_str(j).unwrap() };
    let cases: Vec<(Value, Flavor, bool, Vec<&str>)> = vec![
        (
            v(r#""startTime""#),
            Flavor::Postgresql,
            false,
            vec!["\"startTime\""],
        ),
        (
            v(r#""col1, col2""#),
            Flavor::Postgresql,
            false,
            vec!["\"col1\"", "\"col2\""],
        ),
        (
            v(r#""startTime desc""#),
            Flavor::Postgresql,
            true,
            vec!["\"startTime\" DESC"],
        ),
        (
            v(r#""startTime DESC, endTime""#),
            Flavor::Postgresql,
            true,
            vec!["\"startTime\" DESC", "\"endTime\""],
        ),
        (
            v(r#"["startTime desc", "endTime asc"]"#),
            Flavor::Postgresql,
            true,
            vec!["\"startTime\" DESC", "\"endTime\" ASC"],
        ),
        (
            v(r#"["a, b DESC", "c"]"#),
            Flavor::Postgresql,
            true,
            vec!["\"a\"", "\"b\" DESC", "\"c\""],
        ),
        (
            v(r#""ts DESC NULLS LAST""#),
            Flavor::Postgresql,
            true,
            vec!["\"ts\" DESC NULLS LAST"],
        ),
        (
            v(r#""ts NULLS FIRST""#),
            Flavor::Postgresql,
            true,
            vec!["\"ts\" NULLS FIRST"],
        ),
        (
            v(r#""a, , b""#),
            Flavor::Postgresql,
            false,
            vec!["\"a\"", "\"b\""],
        ),
        (v(r#""""#), Flavor::Postgresql, false, vec![]),
        (v(r#"["", "  "]"#), Flavor::Postgresql, true, vec![]),
        (
            v(r#""startTime""#),
            Flavor::Mysql,
            false,
            vec!["`startTime`"],
        ),
        (
            v(r#""startTime""#),
            Flavor::Mariadb,
            false,
            vec!["`startTime`"],
        ),
        (
            v(r#""startTime DESC""#),
            Flavor::Postgresql,
            false,
            vec!["\"startTime DESC\""],
        ),
        (
            v(r#""col FOO""#),
            Flavor::Postgresql,
            true,
            vec!["\"col FOO\""],
        ),
    ];
    for (value, flavor, allow, expected) in cases {
        assert_eq!(
            normalize_columnstore_cols(&value, flavor, allow),
            expected,
            "{value:?}"
        );
    }
}

#[test]
fn test_compress_settings_default_quoting() {
    let pipe = sql_pipe(
        r#"{"columns": {"datetime": "startTime"}}"#,
        Flavor::Timescaledb,
    );
    assert_eq!(
        get_compress_settings(Flavor::Timescaledb, &pipe).orderby,
        vec!["\"startTime\" DESC"]
    );
    let q = get_columnstore_settings_query(&ConnectorCtx::new(Flavor::Timescaledb), &pipe);
    assert!(
        q.contains("timescaledb.orderby = '\"startTime\" DESC'"),
        "{q}"
    );
    assert!(!q.contains("orderby = 'startTime DESC'"));
}

#[test]
fn test_compress_settings_id_placement() {
    let f = Flavor::Timescaledb;
    let non_unique = sql_pipe(r#"{"columns": {"datetime": "ts", "id": "device"}}"#, f);
    let s = get_compress_settings(f, &non_unique);
    assert_eq!(s.segmentby, vec!["\"device\""]);
    assert_eq!(s.orderby, vec!["\"ts\" DESC"]);
    let unique = sql_pipe(
        r#"{"columns": {"datetime": "ts", "id": "pk", "primary": "pk"}}"#,
        f,
    );
    let s = get_compress_settings(f, &unique);
    assert!(s.segmentby.is_empty());
    assert_eq!(s.orderby, vec!["\"ts\" DESC", "\"pk\""]);
}

#[test]
fn test_compress_settings_user_overrides() {
    let f = Flavor::Timescaledb;
    let pipe = sql_pipe(
        r#"{"columns": {"datetime": "startTime"}, "compress": {"segmentby": "col1, col2", "orderby": "startTime DESC, endTime"}}"#,
        f,
    );
    let s = get_compress_settings(f, &pipe);
    assert_eq!(s.segmentby, vec!["\"col1\"", "\"col2\""]);
    assert_eq!(s.orderby, vec!["\"startTime\" DESC", "\"endTime\""]);
}

fn int_axis_pipe(extra: &str) -> SqlPipe {
    sql_pipe(
        &format!(
            r#"{{"columns": {{"datetime": "ts"}}, "dtypes": {{"ts": "int"}}, "precision": {{"unit": "millisecond"}}{extra}}}"#
        ),
        Flavor::Timescaledb,
    )
}

#[test]
fn test_columnstore_policy_after_scales_on_int_axis() {
    let ctx = ConnectorCtx::new(Flavor::Timescaledb);
    let q = get_columnstore_policy_query(
        &ctx,
        &int_axis_pipe(r#", "compress": {"after": "30 days"}"#),
    );
    assert!(q.contains("after => 2592000000"), "{q}");
    let q = get_columnstore_policy_query(&ctx, &int_axis_pipe(r#", "compress": {"after": 12345}"#));
    assert!(q.contains("after => 12345"), "{q}");
    for value in ["30.0", "\"30\""] {
        let q = get_columnstore_policy_query(
            &ctx,
            &int_axis_pipe(&format!(r#", "compress": {{"after": {value}}}"#)),
        );
        assert!(q.contains("after => 30"), "{q}");
    }
    let q = get_columnstore_policy_query(&ctx, &int_axis_pipe(r#", "compress": true"#));
    assert!(q.contains("after => 604800000"), "{q}");
    let dt = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "compress": {"after": "30 days"}}"#,
        Flavor::Timescaledb,
    );
    assert!(get_columnstore_policy_query(&ctx, &dt).contains("INTERVAL '30 days'"));
}

#[test]
fn test_integer_now_func_registered_for_int_axis() {
    let ctx = ConnectorCtx::new(Flavor::Timescaledb);
    let queries = get_integer_now_func_queries(&ctx, &int_axis_pipe(r#", "compress": true"#));
    assert_eq!(queries.len(), 2);
    assert!(queries[0].contains("CREATE OR REPLACE FUNCTION mrsm_integer_now_millisecond()"));
    assert!(queries[0].contains("* 1000"));
    assert!(queries[1].contains("set_integer_now_func"));
    assert!(queries[1].contains("'mrsm_integer_now_millisecond'"));
    let dt = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "compress": true}"#,
        Flavor::Timescaledb,
    );
    assert!(get_integer_now_func_queries(&ctx, &dt).is_empty());
}

// Live: an integer-axis TimescaleDB hypertable gets its `integer_now` function so the
// Hypercore columnstore policy is valid, and compresses.
flavor_tests!(test_integer_axis_hypertable_compress, |c: Ctx| async move {
    if c.flavor != "timescaledb" {
        return;
    }
    let pipe = c.pipe(
        "test",
        "compress",
        Some("integer_now"),
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "precision": {"unit": "millisecond"}, "compress": true}"#,
    );
    let docs: Vec<Doc> = (0..10)
        .map(|k| doc(vec![("ts", i(1_700_000_000_000 + k * 60_000)), ("v", i(k))]))
        .collect();
    ok(c.sync(&pipe, docs).await);
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    assert!(c.conn.is_hypertable(&sp).await);
    ok(c.conn.compress_pipe(&pipe, false).await);
    assert_eq!(rows(&c, &pipe).await, Some(10));

    // `set_integer_now_func` registers `mrsm_integer_now_millisecond` on the hypertable, and
    // re-running it is a successful no-op.
    let target = sp
        .target
        .rsplit('.')
        .next()
        .unwrap()
        .trim_matches('"')
        .to_string();
    let registered = format!(
        "SELECT integer_now_func FROM timescaledb_information.dimensions WHERE hypertable_name = '{target}'"
    );
    for _ in 0..2 {
        assert!(c.conn.set_integer_now_func(&sp).await);
        let func = c
            .conn
            .value(&registered)
            .await
            .expect("a registered function");
        assert!(
            format!("{func:?}").contains("mrsm_integer_now_millisecond"),
            "unexpected integer_now_func: {func:?}"
        );
    }
    c.delete(&pipe).await;
});

// Live: `set_integer_now_func` is a successful no-op where no function is needed (a datetime
// axis, any other flavor), and on TimescaleDB it (re-)registers `mrsm_integer_now_millisecond`
// on an integer-axis hypertable, idempotently.
flavor_tests!(test_set_integer_now_func, |c: Ctx| async move {
    let datetime_pipe = c.pipe(
        "test",
        "integer_now",
        Some("datetime_axis"),
        r#"{"columns": {"datetime": "ts"}, "compress": true}"#,
    );
    let sp = c.conn.sql_pipe(&datetime_pipe).unwrap();
    assert!(
        c.conn.set_integer_now_func(&sp).await,
        "a datetime axis needs no integer_now function"
    );
    // The integer-axis TimescaleDB case registers a database-wide function by name, so it runs
    // inside `test_integer_axis_hypertable_compress`: two tests replacing it concurrently race
    // ("tuple concurrently updated").
    if c.flavor != "timescaledb" {
        let int_pipe = c.pipe(
            "test",
            "integer_now",
            Some("int_axis"),
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "compress": true}"#,
        );
        let sp = c.conn.sql_pipe(&int_pipe).unwrap();
        assert!(c.conn.set_integer_now_func(&sp).await);
    }
});

/// The `mrsm_temp_tables` column holding the table name, quoted for the flavor.
fn temp_table_column(flavor: &str) -> &'static str {
    match flavor {
        "mysql" | "mariadb" => "`table`",
        "mssql" => "[table]",
        _ => "\"table\"",
    }
}

/// Count the bookkeeping rows for `name`, optionally only the unflagged ones.
async fn temp_table_rows(c: &Ctx, name: &str, unflagged_only: bool) -> i64 {
    let table = c.conn.instance_table_name("mrsm_temp_tables");
    let col = temp_table_column(c.flavor);
    let flag = if unflagged_only {
        " AND ready_to_drop IS NULL"
    } else {
        ""
    };
    let query = format!("SELECT COUNT(*) FROM {table} WHERE {col} = '{name}'{flag}");
    c.conn
        .value(&query)
        .await
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

/// Seed one stale (30 days old, unflagged) bookkeeping row.
async fn seed_stale_temp_table(c: &Ctx, name: &str) {
    use meerschaum_sql::driver::params::{SqlParam, placeholders};
    let table = c.conn.instance_table_name("mrsm_temp_tables");
    let col = temp_table_column(c.flavor);
    let created = chrono::Utc::now().naive_utc() - chrono::Duration::days(30);
    let created = if c.flavor == "sqlite" || c.flavor == "geopackage" {
        SqlParam::Text(created.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
    } else {
        SqlParam::Timestamp(created)
    };
    c.conn
        .driver()
        .exec_with(
            &format!(
                "INSERT INTO {table} (date_created, {col}, ready_to_drop) VALUES ({})",
                placeholders(c.conn.flavor(), 1, 3)
            ),
            &[created, SqlParam::Text(name.to_string()), SqlParam::Null],
        )
        .await
        .expect("seed a stale temporary table row");
}

// `test_stale_temporary_tables_scan_is_throttled`: the stale-table scan runs at most once a
// minute and never duplicates bookkeeping rows. The first call flags (and drops) the stale
// row; on a private database a row seeded afterwards is untouched by the next four calls and by a `refresh=True`
// call, which only drops what is already flagged.
flavor_tests!(
    test_stale_temporary_tables_scan_is_throttled,
    |c: Ctx| async move {
        assert!(c.conn.ensure_instance_tables(true).await);
        let first = format!("_mrsm_test_stale_a_{}", &c.prefix[6..14]);
        let second = format!("_mrsm_test_stale_b_{}", &c.prefix[6..14]);
        seed_stale_temp_table(&c, &first).await;

        let (success, msg) = c.conn.drop_old_temporary_tables(false).await;
        assert!(success, "{msg}");
        assert_eq!(
            temp_table_rows(&c, &first, true).await,
            0,
            "the first call scans and flags the stale table"
        );
        assert!(temp_table_rows(&c, &first, false).await <= 1);

        seed_stale_temp_table(&c, &second).await;
        for _ in 0..4 {
            let (success, msg) = c.conn.drop_old_temporary_tables(false).await;
            assert!(success, "{msg}");
        }
        let (success, msg) = c.conn.drop_old_temporary_tables(true).await;
        assert!(success, "{msg}");
        // Server flavors share the bookkeeping table with every concurrently running test,
        // whose own syncs scan (and flag) any 30-day-old row; only a private database can
        // assert that the row stays unflagged. `stale_scan_throttle_window` (lib tests) pins
        // the exact timing on SQLite.
        if matches!(c.flavor, "sqlite" | "geopackage" | "duckdb") {
            assert_eq!(
                temp_table_rows(&c, &second, true).await,
                1,
                "throttled calls neither scan nor flag nor re-insert"
            );
        }
        assert!(
            temp_table_rows(&c, &second, false).await <= 1,
            "the bookkeeping row is never duplicated"
        );

        let table = c.conn.instance_table_name("mrsm_temp_tables");
        let col = temp_table_column(c.flavor);
        c.conn
            .exec(&format!(
                "DELETE FROM {table} WHERE {col} IN ('{first}', '{second}')"
            ))
            .await;
    }
);

/// A `tracing` writer collecting everything logged on the current thread.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Captured {
        self.clone()
    }
}

// `test_compress_with_upsert_warns_once`: declaring both `upsert` and `compress` warns once per
// pipe per connector (the message names the `ON CONFLICT` failure); a pipe without `upsert`
// never warns, and a second pipe warns on its own. The warning precedes any table lookup, so
// the pipes need not exist. A current-thread runtime keeps the thread-local subscriber on the
// thread running the calls.
#[tokio::test]
async fn test_compress_with_upsert_warns_once() {
    let Some(c) = ctx("timescaledb", "compress_upsert_warn").await else {
        return;
    };
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let warnings = || {
        let text = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        text.matches("ON CONFLICT").count()
    };
    let upsert = |location: &str, upsert: bool| {
        c.pipe(
            "plugin:timeline",
            "activities",
            Some(location),
            &format!(
                r#"{{"columns": {{"datetime": "time", "id": "id"}}, "upsert": {upsert}, "compress": {{"after": "30 days"}}}}"#
            ),
        )
    };

    let pipe = upsert("compress_upsert_warn", true);
    ok(c.conn.apply_compression_policy(&pipe).await);
    ok(c.conn.apply_compression_policy(&pipe).await);
    assert_eq!(warnings(), 1, "warned once for two applications");

    let plain = upsert("compress_plain", false);
    ok(c.conn.apply_compression_policy(&plain).await);
    assert_eq!(warnings(), 1, "no warning without `upsert`");

    let other = upsert("compress_upsert_other", true);
    ok(c.conn.apply_compression_policy(&other).await);
    assert_eq!(warnings(), 2, "a second pipe warns on its own");
}
