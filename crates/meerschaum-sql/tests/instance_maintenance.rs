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
    "sqlite",
];
const VACUUMABLE: &[&str] = &[
    "timescaledb",
    "postgresql",
    "postgis",
    "mysql",
    "mariadb",
    "sqlite",
];
const FULL_VACUUM: &[&str] = &["postgresql", "postgis"];
const RELIABLE_COMPRESS: &[&str] = &["timescaledb", "mysql", "mariadb"];
const COMPRESSIBLE: &[&str] = &["timescaledb", "mysql", "mariadb"];

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
    assert!(SIZE_FLAVORS.contains(&c.flavor));
    assert!(size.is_some_and(|s| s > 0), "{size:?}");
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
    let res = ok(c.conn.analyze_pipe(&pipe).await);
    assert!(res.message.starts_with("Analyzed "), "{}", res.message);
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
    c.delete(&pipe).await;
});
