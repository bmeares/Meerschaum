//! `upsert_changed_only`: a PostgreSQL-family upsert leaves the rows whose values would not
//! change untouched (same `xmin` and `ctid`), on the frame path (`sync_pipe`) and the in-place
//! path (`sync_pipe_inplace`); other flavors ignore the parameter. Table prefix `w5sql_`.
#[allow(unused_imports)]
use crate::common;
use crate::common::*;
use meerschaum_connector::SyncOptions;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_connector_sql::instance::inplace::InplaceObserver;
use meerschaum_connector_sql::queries::metadef::Bound;
use meerschaum_core::SuccessTuple;
use meerschaum_kernel::value::Value as KValue;

type FrameChange = (&'static str, fn(&mut Vec<Doc>), Vec<i64>);

fn pg_family(c: &Ctx) -> bool {
    matches!(
        c.flavor,
        "postgresql" | "timescaledb" | "postgis" | "timescaledb-ha"
    )
}

async fn has_extension(c: &Ctx, name: &str) -> bool {
    c.conn
        .value(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = '{name}')"
        ))
        .await
        == Some(SqlValue::Bool(true))
}

/// `(id, "xmin/ctid")` of every row of `table`: a row rewritten by an update gets new ones.
async fn versions(c: &Ctx, table: &str) -> Vec<(i64, String)> {
    let sql = format!(
        "SELECT string_agg(COALESCE(id::text, 'null') || '=' || xmin::text || '/' || ctid::text, ';' ORDER BY id NULLS FIRST) FROM {table}"
    );
    let Some(SqlValue::Text(text)) = c.conn.value(&sql).await else {
        panic!("{}: no row versions from {table}", c.flavor);
    };
    text.split(';')
        .map(|item| {
            let (id, version) = item.split_once('=').expect("id=version");
            (id.parse().unwrap_or(-1), version.to_string())
        })
        .collect()
}

/// The ids whose row versions differ between two snapshots (`-1` is the NULL id).
fn rewritten(before: &[(i64, String)], after: &[(i64, String)]) -> Vec<i64> {
    assert_eq!(before.len(), after.len(), "{before:?} {after:?}");
    before
        .iter()
        .zip(after)
        .filter(|(b, a)| b.1 != a.1)
        .map(|(b, a)| {
            assert_eq!(b.0, a.0);
            b.0
        })
        .collect()
}

fn upserted(n: usize) -> String {
    format!("Upserted {n} row{}.", if n == 1 { "" } else { "s" })
}

fn no_diff() -> SyncOptions {
    SyncOptions {
        check_existing: false,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn row(
    id: i64,
    v: Option<f64>,
    n: Option<&str>,
    t: KValue,
    doc_json: &str,
    bytes: &[u8],
    wkt: &str,
) -> Doc {
    doc(vec![
        ("ts", udt(2024, 1, 1, 0, 0)),
        ("id", i(id)),
        ("v", v.map_or(KValue::Null, fl)),
        ("n", n.map_or(KValue::Null, s)),
        ("t", t),
        ("doc", j(doc_json)),
        ("b", KValue::Bytes(bytes.to_vec())),
        ("g", s(wkt)),
    ])
}

fn base_rows() -> Vec<Doc> {
    vec![
        row(
            1,
            Some(1.5),
            Some("1.25"),
            udt(2024, 1, 1, 1, 0),
            r#"{"a": 1, "b": 2}"#,
            &[0, 1],
            "POINT (1 2)",
        ),
        row(
            2,
            Some(2.5),
            Some("2.50"),
            udt(2024, 1, 1, 2, 0),
            r#"{"a": 2}"#,
            &[2],
            "POINT (3 4)",
        ),
        row(
            3,
            Some(3.5),
            None,
            udt(2024, 1, 1, 3, 0),
            "[1, 2]",
            &[3],
            "LINESTRING (0 0, 1 1)",
        ),
    ]
}

const FRAME_PARAMS: &str = r#"{
    "columns": {"datetime": "ts", "id": "id"},
    "upsert": true,
    "dtypes": {
        "v": "float", "n": "numeric", "t": "datetime64[ns, UTC]",
        "doc": "json", "b": "bytes", "g": "geometry"
    }
    CHANGED
}"#;

fn frame_params(changed_only: Option<bool>) -> String {
    FRAME_PARAMS.replace(
        "CHANGED",
        &changed_only
            .map(|b| format!(", \"upsert_changed_only\": {b}"))
            .unwrap_or_default(),
    )
}

// The frame path. `check_existing=False` hands every row to the database, as a sync whose
// in-memory diff cannot tell rows apart (or a caller that skips it) does.
flavor_tests!(frame_upserts_skip_unchanged_rows, |c: Ctx| async move {
    let pipe = c.pipe("ucs", "frame", None, &frame_params(Some(true)));
    ok(c.sync(&pipe, base_rows()).await);
    // A second identical sync succeeds and reports only the rows written on PostgreSQL.
    let result = ok(c.sync_with(&pipe, base_rows(), no_diff()).await);
    if !pg_family(&c) {
        // Other flavors keep their existing behavior, including backends without upserts.
        let baseline = c.pipe("ucs", "frame_off", None, &frame_params(Some(false)));
        ok(c.sync(&baseline, base_rows()).await);
        let baseline_result = ok(c.sync_with(&baseline, base_rows(), no_diff()).await);
        assert_eq!(result.message, baseline_result.message, "{}", c.flavor);
        let mut rows = base_rows();
        rows[1].insert("v".into(), fl(9.0));
        ok(c.sync_with(&pipe, rows, no_diff()).await);
        let docs = c.docs(&pipe).await;
        assert!(
            docs.iter().any(|d| d["v"] == fl(9.0)),
            "{}: {docs:?}",
            c.flavor
        );
        c.delete(&pipe).await;
        c.delete(&baseline).await;
        return;
    }
    assert_eq!(result.message, upserted(0), "{}", c.flavor);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let mut last = versions(&c, &table).await;
    let ok_rows = ok(c.sync_with(&pipe, base_rows(), no_diff()).await);
    assert_eq!(ok_rows.message, upserted(0));
    let now = versions(&c, &table).await;
    assert_eq!(rewritten(&last, &now), Vec::<i64>::new(), "{}", c.flavor);

    let postgis = has_extension(&c, "postgis").await;
    let mut rows = base_rows();
    let mut steps: Vec<FrameChange> = vec![
        (
            "one value",
            |r: &mut Vec<Doc>| {
                r[1].insert("v".into(), fl(2.75));
            },
            vec![2],
        ),
        (
            "value to NULL",
            |r: &mut Vec<Doc>| {
                r[0].insert("v".into(), KValue::Null);
            },
            vec![1],
        ),
        (
            "NULL to value",
            |r: &mut Vec<Doc>| {
                r[2].insert("n".into(), s("7.5"));
            },
            vec![3],
        ),
        (
            "json value",
            |r: &mut Vec<Doc>| {
                r[0].insert("doc".into(), j(r#"{"a": 1, "b": 3}"#));
            },
            vec![1],
        ),
        (
            "json key order",
            |r: &mut Vec<Doc>| {
                r[0].insert("doc".into(), j(r#"{"b": 3, "a": 1}"#));
            },
            vec![],
        ),
        (
            "bytes",
            |r: &mut Vec<Doc>| {
                r[1].insert("b".into(), KValue::Bytes(vec![2, 0]));
            },
            vec![2],
        ),
        (
            "timestamp",
            |r: &mut Vec<Doc>| {
                r[2].insert("t".into(), udt(2024, 1, 1, 3, 1));
            },
            vec![3],
        ),
    ];
    if postgis {
        steps.push((
            "geometry vertex order",
            |r: &mut Vec<Doc>| {
                r[2].insert("g".into(), s("LINESTRING (1 1, 0 0)"));
            },
            vec![3],
        ));
    }
    for (name, change, expected) in steps {
        change(&mut rows);
        let result: SuccessTuple = ok(c.sync_with(&pipe, rows.clone(), no_diff()).await);
        assert_eq!(
            result.message,
            upserted(expected.len()),
            "{}: {name}",
            c.flavor
        );
        let now = versions(&c, &table).await;
        assert_eq!(rewritten(&last, &now), expected, "{}: {name}", c.flavor);
        last = now;
    }
    // The stored values follow the last frame.
    let docs = c.docs(&pipe).await;
    assert_eq!(docs.len(), 3);
    assert!(docs.iter().any(|d| d["v"] == fl(2.75)), "{docs:?}");
    assert!(docs.iter().any(|d| d["v"].is_na()), "{docs:?}");

    // Off: every conflicting row is rewritten again.
    let pipe_off = c.pipe("ucs", "frame", None, &frame_params(Some(false)));
    ok(c.sync_with(&pipe_off, rows.clone(), no_diff()).await);
    let now = versions(&c, &table).await;
    assert_eq!(rewritten(&last, &now), vec![1, 2, 3], "{}", c.flavor);
    c.delete(&pipe).await;
});

// Without the parameter (the default) an identical upsert rewrites every row.
flavor_tests!(frame_upserts_rewrite_by_default, |c: Ctx| async move {
    if !pg_family(&c) {
        return;
    }
    let pipe = c.pipe("ucs", "default", None, &frame_params(None));
    ok(c.sync(&pipe, base_rows()).await);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let before = versions(&c, &table).await;
    ok(c.sync_with(&pipe, base_rows(), no_diff()).await);
    let after = versions(&c, &table).await;
    assert_eq!(rewritten(&before, &after), vec![1, 2, 3], "{}", c.flavor);
    c.delete(&pipe).await;
});

// A table whose unique key treats NULLs as distinct (v4 / rc14, PostgreSQL before 15) upserts
// rows with a NULL key with an `UPDATE` of its own: that write skips unchanged rows too.
flavor_tests!(null_key_upserts_skip_unchanged_rows, |c: Ctx| async move {
    if !pg_family(&c) {
        return;
    }
    let row = |id: Option<i64>, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", id.map_or(KValue::Null, i)),
            ("v", i(v)),
        ])
    };
    let legacy = c.pipe(
        "ucs",
        "nullkey",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "null_indices": false}"#,
    );
    ok(c.sync(&legacy, vec![row(Some(1), 1)]).await);
    let pipe = c.pipe(
        "ucs",
        "nullkey",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "upsert_changed_only": true}"#,
    );
    ok(
        c.sync_with(&pipe, vec![row(Some(1), 1), row(None, 2)], no_diff())
            .await,
    );
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let before = versions(&c, &table).await;
    assert_eq!(before.len(), 2, "{before:?}");
    let result = ok(c
        .sync_with(&pipe, vec![row(Some(1), 1), row(None, 2)], no_diff())
        .await);
    assert_eq!(result.message, upserted(0));
    let after = versions(&c, &table).await;
    assert_eq!(
        rewritten(&before, &after),
        Vec::<i64>::new(),
        "{}",
        c.flavor
    );
    let result = ok(c
        .sync_with(&pipe, vec![row(Some(1), 1), row(None, 3)], no_diff())
        .await);
    assert_eq!(result.message, upserted(1));
    let changed = versions(&c, &table).await;
    assert_eq!(rewritten(&after, &changed), vec![-1], "{}", c.flavor);
    let result = ok(c
        .sync_with(&pipe, vec![row(Some(1), 4), row(None, 3)], no_diff())
        .await);
    assert_eq!(result.message, upserted(1));
    let last = versions(&c, &table).await;
    assert_eq!(rewritten(&changed, &last), vec![1], "{}", c.flavor);
    c.delete(&pipe).await;
});

/// A source table holding the special PostgreSQL types (PostGIS ones when installed).
async fn special_source(c: &Ctx, postgis: bool) -> String {
    let table = format!("{}_src", c.prefix);
    let geo_cols = if postgis {
        ", g geometry(LineString, 4326), gg geography(Point, 4326)"
    } else {
        ""
    };
    let geo_vals = |line: &str, point: &str| {
        if postgis {
            format!(", 'SRID=4326;{line}', 'SRID=4326;{point}'")
        } else {
            String::new()
        }
    };
    let mut sqls = vec![format!(
        "CREATE TABLE \"{table}\" (ts timestamptz NOT NULL, id bigint NOT NULL, v double precision, \
         n numeric, t timestamp, doc json, jb jsonb, b bytea, arr integer[], x xml, pt point, iv interval{geo_cols})"
    )];
    for id in 1..=6 {
        sqls.push(format!(
            "INSERT INTO \"{table}\" VALUES ('2024-01-01 00:00:00+00', {id}, {id}.5, NULL, \
             '2024-01-01 00:00:00', '{{\"a\": {id}, \"b\": 2}}', '{{\"k\": {id}}}', '\\x00'::bytea, \
             '{{1,2}}', '<a>{id}</a>', point({id}, {id}), interval '1 day'{})",
            geo_vals("LINESTRING(0 0, 1 1)", "POINT(1 2)")
        ));
    }
    for sql in sqls {
        assert!(c.conn.exec(&sql).await.is_some(), "{}: {sql}", c.flavor);
    }
    table
}

#[derive(Default)]
struct Changes(std::sync::Mutex<Vec<usize>>);

#[meerschaum_connector::async_trait]
impl InplaceObserver for Changes {
    async fn before_merge(&self, delta: &meerschaum_kernel::Frame) -> Result<(), String> {
        self.0.lock().unwrap().push(delta.height());
        Ok(())
    }
}

async fn inplace_sync(c: &Ctx, pipe: &meerschaum_core::Pipe) -> String {
    inplace_sync_with(c, pipe, pipe.upsert_changed_only()).await
}

/// An in-place sync; `observe` adds an observer, as a pipe with children has, which runs the
/// query selecting the rows that change.
async fn inplace_sync_with(c: &Ctx, pipe: &meerschaum_core::Pipe, observe: bool) -> String {
    if !observe {
        return ok(c
            .conn
            .sync_pipe_inplace(pipe, None, Bound::None, Bound::None, true)
            .await)
        .message;
    }
    let changes = Changes::default();
    let result = ok(c
        .conn
        .sync_pipe_inplace_observed(pipe, None, Bound::None, Bound::None, true, &changes)
        .await);
    if pipe.upsert_changed_only() && result.message.starts_with("Upserted ") {
        let changed: usize = changes.0.lock().unwrap().iter().sum();
        assert_eq!(
            result.message,
            upserted(changed),
            "observer must agree with physical writes"
        );
    }
    result.message
}

// The in-place path: the statement's own row count is the sync's count, so a lap that changes
// nothing reports `Upserted 0 rows.` and still succeeds.
flavor_tests!(inplace_upserts_skip_unchanged_rows, |c: Ctx| async move {
    if !pg_family(&c) {
        return;
    }
    let postgis = has_extension(&c, "postgis").await;
    let source = special_source(&c, postgis).await;
    let geo_cols = if postgis { ", g, gg" } else { "" };
    let params = |changed_only: bool| {
        serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "upsert": true,
            "upsert_changed_only": changed_only,
            "sql": format!("SELECT ts, id, v, n, t, doc, jb, b, arr, x, pt, iv{geo_cols} FROM \"{source}\""),
        })
        .to_string()
    };
    let pipe = c.pipe(&c.conn.keys(), "inplace", None, &params(true));
    inplace_sync(&c, &pipe).await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let mut last = versions(&c, &table).await;
    assert_eq!(last.len(), 6);
    assert_eq!(inplace_sync(&c, &pipe).await, upserted(0), "{}", c.flavor);
    let now = versions(&c, &table).await;
    assert_eq!(rewritten(&last, &now), Vec::<i64>::new(), "{}", c.flavor);

    let mut steps: Vec<(&str, &str, Vec<i64>)> = vec![
        ("one value", "v = v + 1 WHERE id = 2", vec![2]),
        ("value to NULL", "v = NULL WHERE id = 1", vec![1]),
        ("NULL to value", "n = 2.5 WHERE id = 3", vec![3]),
        ("numeric scale only", "n = 2.50 WHERE id = 3", vec![]),
        (
            "json key order",
            r#"doc = '{"b": 2, "a": 4}' WHERE id = 4"#,
            vec![4],
        ),
        (
            "json value",
            r#"doc = '{"a": 4, "b": 3}' WHERE id = 4"#,
            vec![4],
        ),
        (
            "json that jsonb rejects",
            r#"doc = '{"v": "\u0000"}' WHERE id = 4"#,
            vec![4],
        ),
        (
            "same json that jsonb rejects",
            "doc = doc WHERE id = 4",
            vec![],
        ),
        (
            "interval representation",
            "iv = interval '24 hours' WHERE id = 1",
            vec![1],
        ),
        ("jsonb", r#"jb = '{"z": 0}' WHERE id = 5"#, vec![5]),
        ("bytea", "b = '\\x0001'::bytea WHERE id = 6", vec![6]),
        ("array order", "arr = '{2,1}' WHERE id = 1", vec![1]),
        ("same array", "arr = '{2,1}' WHERE id = 1", vec![]),
        (
            "xml (no equality)",
            "x = '<a>changed</a>' WHERE id = 2",
            vec![2],
        ),
        (
            "point (no equality)",
            "pt = point(9, 9) WHERE id = 3",
            vec![3],
        ),
        (
            "timestamp",
            "t = t + interval '1 microsecond' WHERE id = 4",
            vec![4],
        ),
        ("same values", "v = v, doc = doc WHERE id IN (5, 6)", vec![]),
        ("two rows", "v = -1 WHERE id IN (5, 6)", vec![5, 6]),
    ];
    if postgis {
        steps.push((
            "geometry vertex order",
            "g = 'SRID=4326;LINESTRING(1 1, 0 0)' WHERE id = 5",
            vec![5],
        ));
        steps.push((
            "geography",
            "gg = 'SRID=4326;POINT(1 2.000001)' WHERE id = 6",
            vec![6],
        ));
        steps.push((
            "geometry unchanged",
            "g = 'SRID=4326;LINESTRING(1 1, 0 0)' WHERE id = 5",
            vec![],
        ));
    }
    for (name, update, expected) in steps {
        let sql = format!("UPDATE \"{source}\" SET {update}");
        assert!(c.conn.exec(&sql).await.is_some(), "{}: {sql}", c.flavor);
        let message = inplace_sync(&c, &pipe).await;
        assert_eq!(message, upserted(expected.len()), "{}: {name}", c.flavor);
        let now = versions(&c, &table).await;
        assert_eq!(rewritten(&last, &now), expected, "{}: {name}", c.flavor);
        last = now;
    }

    // Off: every row of the window is rewritten and counted.
    let pipe_off = c.pipe(&c.conn.keys(), "inplace", None, &params(false));
    assert_eq!(
        inplace_sync(&c, &pipe_off).await,
        upserted(6),
        "{}",
        c.flavor
    );
    let now = versions(&c, &table).await;
    assert_eq!(
        rewritten(&last, &now),
        vec![1, 2, 3, 4, 5, 6],
        "{}",
        c.flavor
    );
    c.delete(&pipe).await;
    let _ = c.conn.exec(&format!("DROP TABLE \"{source}\"")).await;
});

// Array columns upsert uncast (`enforce: false`); their order is part of the value.
flavor_tests!(
    inplace_array_upserts_skip_unchanged_rows,
    |c: Ctx| async move {
        if !pg_family(&c) {
            return;
        }
        let source = special_source(&c, false).await;
        let params = serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "upsert": true,
            "upsert_changed_only": true,
            "enforce": false,
            "sql": format!("SELECT ts, id, arr, doc FROM \"{source}\""),
        })
        .to_string();
        let pipe = c.pipe(&c.conn.keys(), "arrays", None, &params);
        inplace_sync(&c, &pipe).await;
        let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
        let mut last = versions(&c, &table).await;
        for (update, expected) in [
            ("arr = arr WHERE id = 1", vec![]),
            ("arr = '{2,1}' WHERE id = 1", vec![1]),
            ("arr = NULL WHERE id = 2", vec![2]),
            (r#"doc = '{"b": 2, "a": 3}' WHERE id = 3"#, vec![3]),
        ] {
            let sql = format!("UPDATE \"{source}\" SET {update}");
            assert!(c.conn.exec(&sql).await.is_some(), "{}: {sql}", c.flavor);
            assert_eq!(
                inplace_sync(&c, &pipe).await,
                upserted(expected.len()),
                "{update}"
            );
            let now = versions(&c, &table).await;
            assert_eq!(rewritten(&last, &now), expected, "{}: {update}", c.flavor);
            last = now;
        }
        c.delete(&pipe).await;
        let _ = c.conn.exec(&format!("DROP TABLE \"{source}\"")).await;
    }
);

// TimescaleDB: an upsert that reaches a compressed chunk still succeeds; the guard skips the
// update, not the decompression of the conflicting batches.
flavor_tests!(
    compressed_chunk_upserts_skip_unchanged_rows,
    |c: Ctx| async move {
        if !matches!(c.flavor, "timescaledb" | "timescaledb-ha")
            || !has_extension(&c, "timescaledb").await
        {
            return;
        }
        let pipe = c.pipe(
        "ucs",
        "compressed",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "upsert_changed_only": true,
            "dtypes": {"v": "int"}}"#,
    );
        let rows = |v: i64| {
            (0..4)
                .map(|id| {
                    doc(vec![
                        ("ts", udt(2024, 1, 1, 0, id as u32)),
                        ("id", i(id)),
                        ("v", i(if id == 0 { v } else { id })),
                    ])
                })
                .collect::<Vec<_>>()
        };
        ok(c.sync(&pipe, rows(0)).await);
        let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
        for sql in [
            format!(
                "ALTER TABLE {table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'id')"
            ),
            format!("SELECT compress_chunk(c) FROM show_chunks('{table}'::regclass) c"),
        ] {
            assert!(c.conn.exec(&sql).await.is_some(), "{sql}");
        }
        let result = ok(c.sync_with(&pipe, rows(0), no_diff()).await);
        assert_eq!(result.message, upserted(0));
        ok(c.sync_with(&pipe, rows(42), no_diff()).await);
        let docs = c.docs(&pipe).await;
        assert!(docs.iter().any(|d| d["v"] == i(42)), "{docs:?}");
        assert_eq!(docs.len(), 4);
        c.delete(&pipe).await;
    }
);

/// Run `sqls` in order, asserting that each succeeds.
async fn exec_all(c: &Ctx, sqls: &[String]) {
    for sql in sqls {
        assert!(c.conn.exec(sql).await.is_some(), "{}: {sql}", c.flavor);
    }
}

/// Whether the `citext` extension is installed (created when the role may).
async fn citext(c: &Ctx) -> bool {
    for _ in 0..3 {
        if has_extension(c, "citext").await {
            return true;
        }
        let _ = c.conn.exec("CREATE EXTENSION IF NOT EXISTS citext").await;
    }
    has_extension(c, "citext").await
}

/// An enum type of this test's own (`USER-DEFINED` in the catalog).
async fn mood_type(c: &Ctx) -> String {
    let name = format!("{}_mood", c.prefix);
    exec_all(
        c,
        &[format!(
            "CREATE TYPE \"{name}\" AS ENUM ('happy', 'sad', 'meh')"
        )],
    )
    .await;
    name
}

/// One string per row of `select` (`id` order): what the table holds.
async fn stored(c: &Ctx, table: &str, select: &str) -> String {
    let sql = format!("SELECT string_agg({select}, ';' ORDER BY id NULLS FIRST) FROM {table}");
    match c.conn.value(&sql).await {
        Some(SqlValue::Text(text)) => text,
        other => panic!("{}: {sql}: {other:?}", c.flavor),
    }
}

/// Apply `steps` (`UPDATE <source> SET ...` clauses) one in-place sync at a time and check the
/// rows each one rewrites (with the guard off, every row of the window).
async fn inplace_steps(
    c: &Ctx,
    pipe: &meerschaum_core::Pipe,
    source: &str,
    table: &str,
    observe: bool,
    steps: &[(&str, Vec<i64>)],
) {
    let changed_only = pipe.upsert_changed_only();
    let mut last = versions(c, table).await;
    let all: Vec<i64> = last.iter().map(|(id, _)| *id).collect();
    for (update, expected) in steps {
        if !update.is_empty() {
            exec_all(c, &[format!("UPDATE \"{source}\" SET {update}")]).await;
        }
        let expected = if changed_only { expected } else { &all };
        let message = inplace_sync_with(c, pipe, observe).await;
        assert_eq!(message, upserted(expected.len()), "{}: {update}", c.flavor);
        let now = versions(c, table).await;
        assert_eq!(&rewritten(&last, &now), expected, "{}: {update}", c.flavor);
        last = now;
    }
}

/// Array, enum and `citext` value columns upsert in place with `enforce` on: the catalog names
/// their types `ARRAY` and `USER-DEFINED`, which are not types to cast to. With or without
/// PostGIS: the geometry lookup a `USER-DEFINED` column calls for is skipped where PostGIS is
/// not installed, never sent inside the sync's transaction to fail there.
async fn user_defined_values(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let ci_type = if citext(&c).await { "citext" } else { "text" };
    let mood = mood_type(&c).await;
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!(
                "CREATE TABLE \"{source}\" (ts timestamptz NOT NULL, id bigint NOT NULL, \
                 arr text[], m \"{mood}\", ci {ci_type}, v integer)"
            ),
            format!(
                "INSERT INTO \"{source}\" SELECT '2024-01-01 00:00:00+00', id, '{{a,b}}', 'happy', \
                 'Abc', id FROM generate_series(1, 3) AS id"
            ),
        ],
    )
    .await;
    let params = serde_json::json!({
        "columns": {"datetime": "ts", "id": "id"},
        "upsert": true,
        "upsert_changed_only": changed_only,
        "sql": format!("SELECT ts, id, arr, m, ci, v FROM \"{source}\""),
    })
    .to_string();
    let pipe = c.pipe(&c.conn.keys(), "udvalues", None, &params);
    inplace_sync(&c, &pipe).await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let steps = vec![
        ("", vec![]),
        ("arr = '{b,a}' WHERE id = 1", vec![1]),
        ("v = v, arr = arr WHERE id = 1", vec![]),
        ("m = 'sad' WHERE id = 2", vec![2]),
        ("ci = 'ABC' WHERE id = 3", vec![3]),
        ("m = m, ci = ci WHERE id = 3", vec![]),
    ];
    // Observed (as a pipe with children is), so the change-observation query runs too.
    inplace_steps(&c, &pipe, &source, &table, true, &steps).await;
    assert_eq!(
        stored(&c, &table, "arr::text || '|' || m::text || '|' || ci::text").await,
        "{b,a}|happy|Abc;{a,b}|sad|Abc;{a,b}|happy|ABC",
        "{}",
        c.flavor
    );
    c.delete(&pipe).await;
    // `CASCADE`: an observed sync leaves its staged tables for a later sync to drop.
    exec_all(
        &c,
        &[
            format!("DROP TABLE \"{source}\""),
            format!("DROP TYPE \"{mood}\" CASCADE"),
        ],
    )
    .await;
}

flavor_tests!(user_defined_values_upsert_in_place, |c: Ctx| async move {
    user_defined_values(c, false).await;
});

flavor_tests!(
    user_defined_values_upsert_in_place_changed_only,
    |c: Ctx| async move {
        user_defined_values(c, true).await;
    }
);

/// `citext` and enum KEY columns: the guard compares them as the upsert's keys match, never by
/// casting to their catalog type (`USER-DEFINED`).
async fn user_defined_keys(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let ci_type = if citext(&c).await { "citext" } else { "text" };
    let mood = mood_type(&c).await;
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!(
                "CREATE TABLE \"{source}\" (ts timestamptz NOT NULL, k {ci_type} NOT NULL, \
                 e \"{mood}\" NOT NULL, id bigint NOT NULL, v integer)"
            ),
            format!(
                "INSERT INTO \"{source}\" VALUES \
                 ('2024-01-01 00:00:00+00', 'K1', 'happy', 1, 10), \
                 ('2024-01-01 00:00:00+00', 'K2', 'sad', 2, 20), \
                 ('2024-01-01 00:00:00+00', 'K1', 'sad', 3, 30)"
            ),
        ],
    )
    .await;
    let params = serde_json::json!({
        "columns": {"datetime": "ts", "k": "k", "e": "e"},
        "upsert": true,
        "upsert_changed_only": changed_only,
        "sql": format!("SELECT ts, k, e, id, v FROM \"{source}\""),
    })
    .to_string();
    let pipe = c.pipe(&c.conn.keys(), "udkeys", None, &params);
    inplace_sync(&c, &pipe).await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    inplace_steps(
        &c,
        &pipe,
        &source,
        &table,
        true,
        &[
            ("", vec![]),
            ("v = v + 1 WHERE id = 2", vec![2]),
            ("id = id WHERE e = 'sad'", vec![]),
        ],
    )
    .await;
    assert_eq!(
        stored(&c, &table, "k::text || e::text || v::text").await,
        "K1happy10;K2sad21;K1sad30"
    );
    c.delete(&pipe).await;
    // `CASCADE`: an observed sync leaves its staged tables for a later sync to drop.
    exec_all(
        &c,
        &[
            format!("DROP TABLE \"{source}\""),
            format!("DROP TYPE \"{mood}\" CASCADE"),
        ],
    )
    .await;
}

flavor_tests!(user_defined_keys_upsert_in_place, |c: Ctx| async move {
    user_defined_keys(c, true).await;
});

// Without the guard (observed: the change-observation query compares the enum key without
// v4's NULL stand-in, which is not a valid enum value).
flavor_tests!(
    user_defined_keys_upsert_in_place_by_default,
    |c: Ctx| async move {
        user_defined_keys(c, false).await;
    }
);

/// `enforce: false` and a source column retyped after the first sync: integers staged into the
/// table's text column. The write converts them on assignment; the guard compares them the same
/// way (`integer = text` has no operator), in the `ON CONFLICT` statement and, for the row with
/// a NULL key (the table's unique key treats NULLs as distinct), in the `UPDATE` that compares
/// the staged values themselves.
async fn retyped_inplace(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!("CREATE TABLE \"{source}\" (ts timestamptz NOT NULL, id bigint, v text)"),
            format!(
                "INSERT INTO \"{source}\" VALUES ('2024-01-01 00:00:00+00', 1, '1'), \
                 ('2024-01-01 00:00:00+00', 2, '2'), ('2024-01-01 00:00:00+00', NULL, '3')"
            ),
        ],
    )
    .await;
    let params = |null_indices: bool| {
        serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "upsert": true,
            "upsert_changed_only": changed_only,
            "null_indices": null_indices,
            "enforce": false,
            "dtypes": {"v": "str"},
            "sql": format!("SELECT ts, id, v FROM \"{source}\""),
        })
        .to_string()
    };
    let legacy = c.pipe(&c.conn.keys(), "retyped", None, &params(false));
    inplace_sync_with(&c, &legacy, false).await;
    let pipe = c.pipe(&c.conn.keys(), "retyped", None, &params(true));
    exec_all(
        &c,
        &[format!(
            "ALTER TABLE \"{source}\" ALTER COLUMN v TYPE integer USING v::integer"
        )],
    )
    .await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let mut last = versions(&c, &table).await;
    for (update, expected) in [
        ("v = v", vec![]),
        ("v = 5 WHERE id = 2", vec![2]),
        ("v = 6 WHERE id IS NULL", vec![-1]),
    ] {
        exec_all(&c, &[format!("UPDATE \"{source}\" SET {update}")]).await;
        let message = inplace_sync_with(&c, &pipe, true).await;
        let now = versions(&c, &table).await;
        if changed_only {
            assert_eq!(message, upserted(expected.len()), "{}: {update}", c.flavor);
            assert_eq!(rewritten(&last, &now), expected, "{}: {update}", c.flavor);
        } else {
            assert_eq!(
                rewritten(&last, &now),
                vec![-1, 1, 2],
                "{}: {update}",
                c.flavor
            );
        }
        last = now;
    }
    // The table keeps its text column: the case under test.
    assert_eq!(
        stored(&c, &table, "pg_typeof(v)::text || v").await,
        "text6;text1;text5"
    );
    c.delete(&pipe).await;
    exec_all(&c, &[format!("DROP TABLE \"{source}\"")]).await;
}

flavor_tests!(retyped_enforce_off_upserts_in_place, |c: Ctx| async move {
    retyped_inplace(c, true).await;
});

flavor_tests!(
    retyped_enforce_off_upserts_in_place_by_default,
    |c: Ctx| async move {
        retyped_inplace(c, false).await;
    }
);

/// A table whose unique key treats NULLs as distinct, synced in place: the rows with a NULL key
/// take the `UPDATE` of their own, which skips unchanged rows too.
async fn null_key_inplace(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!(
                "CREATE TABLE \"{source}\" (ts timestamp NOT NULL, id bigint, v integer, \
                 rid bigint NOT NULL)"
            ),
            format!(
                "INSERT INTO \"{source}\" VALUES ('2024-01-01 00:00:00', 1, 1, 1), \
                 ('2024-01-01 00:00:00', NULL, 2, 2)"
            ),
        ],
    )
    .await;
    let params = |null_indices: bool| {
        serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "upsert": true,
            "upsert_changed_only": changed_only,
            "null_indices": null_indices,
            "sql": format!("SELECT ts, id, v FROM \"{source}\""),
        })
        .to_string()
    };
    let legacy = c.pipe(&c.conn.keys(), "nullkey", None, &params(false));
    inplace_sync_with(&c, &legacy, false).await;
    let pipe = c.pipe(&c.conn.keys(), "nullkey", None, &params(true));
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let mut last = versions(&c, &table).await;
    assert_eq!(last.len(), 2, "{last:?}");
    for (update, expected) in [
        ("v = v", vec![]),
        ("v = 3 WHERE id IS NULL", vec![-1]),
        ("v = 4 WHERE id = 1", vec![1]),
    ] {
        exec_all(&c, &[format!("UPDATE \"{source}\" SET {update}")]).await;
        let message = inplace_sync_with(&c, &pipe, true).await;
        let now = versions(&c, &table).await;
        if changed_only {
            assert_eq!(message, upserted(expected.len()), "{}: {update}", c.flavor);
            assert_eq!(rewritten(&last, &now), expected, "{}: {update}", c.flavor);
        } else {
            assert_eq!(
                rewritten(&last, &now),
                vec![-1, 1],
                "{}: {update}",
                c.flavor
            );
        }
        last = now;
    }
    assert_eq!(stored(&c, &table, "v::text").await, "3;4");
    c.delete(&pipe).await;
    exec_all(&c, &[format!("DROP TABLE \"{source}\"")]).await;
}

flavor_tests!(null_key_upserts_in_place, |c: Ctx| async move {
    null_key_inplace(c, true).await;
});

flavor_tests!(null_key_upserts_in_place_by_default, |c: Ctx| async move {
    null_key_inplace(c, false).await;
});

/// `(ts, id, v)` rows for the frame path (`id` may be NULL).
fn kv_rows(rows: &[(Option<i64>, KValue)]) -> Vec<Doc> {
    rows.iter()
        .map(|(id, v)| {
            doc(vec![
                ("ts", ndt(2024, 1, 1, 0, 0)),
                ("id", id.map_or(KValue::Null, i)),
                ("v", v.clone()),
            ])
        })
        .collect()
}

/// The frame path with `enforce: false`: integers synced into a text column, a NULL key among
/// them (the `UPDATE` compares the staged values, not `EXCLUDED`'s). The frame path stages the
/// rows with the table's own column types, so the staged and stored types agree here.
async fn retyped_frame(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let legacy = c.pipe(
        "ucs",
        "retyped",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "null_indices": false,
            "dtypes": {"v": "str"}}"#,
    );
    ok(
        c.sync(&legacy, kv_rows(&[(Some(1), s("1")), (None, s("2"))]))
            .await,
    );
    let pipe = c.pipe(
        "ucs",
        "retyped",
        None,
        &format!(
            r#"{{"columns": {{"datetime": "ts", "id": "id"}}, "upsert": true, "enforce": false,
                "upsert_changed_only": {changed_only}}}"#
        ),
    );
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let mut last = versions(&c, &table).await;
    for (rows, expected) in [
        (vec![(Some(1), i(1)), (None, i(2))], vec![]),
        (vec![(Some(1), i(1)), (None, i(3))], vec![-1]),
        (vec![(Some(1), i(4)), (None, i(3))], vec![1]),
    ] {
        let result = ok(c.sync_with(&pipe, kv_rows(&rows), no_diff()).await);
        let now = versions(&c, &table).await;
        if changed_only {
            assert_eq!(result.message, upserted(expected.len()), "{}", c.flavor);
            assert_eq!(rewritten(&last, &now), expected, "{}", c.flavor);
        } else {
            assert_eq!(rewritten(&last, &now), vec![-1, 1], "{}", c.flavor);
        }
        last = now;
    }
    assert_eq!(
        stored(&c, &table, "pg_typeof(v)::text || v").await,
        "text3;text4"
    );
    c.delete(&pipe).await;
}

flavor_tests!(retyped_enforce_off_frame_upserts, |c: Ctx| async move {
    retyped_frame(c, true).await;
});

flavor_tests!(
    retyped_enforce_off_frame_upserts_by_default,
    |c: Ctx| async move {
        retyped_frame(c, false).await;
    }
);

// PostGIS: a change of SRID alone (same coordinates) is a change, on both paths.
flavor_tests!(srid_changes_are_changes, |c: Ctx| async move {
    if !pg_family(&c) || !has_extension(&c, "postgis").await {
        return;
    }
    let geo_row = |wkt: &str| {
        vec![doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", i(1)),
            ("g", s(wkt)),
        ])]
    };
    let pipe = c.pipe(
        "ucs",
        "srid",
        None,
        // `static`: a frame whose SRID differs from the column's would otherwise retype the
        // column (to text) before the upsert, guard or not.
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "static": true,
            "upsert_changed_only": true, "dtypes": {"g": "geometry"}}"#,
    );
    ok(c.sync(&pipe, geo_row("SRID=4326;POINT (1 2)")).await);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    assert_eq!(stored(&c, &table, "ST_SRID(g)::text").await, "4326");
    let before = versions(&c, &table).await;
    let result = ok(c
        .sync_with(&pipe, geo_row("SRID=4326;POINT (1 2)"), no_diff())
        .await);
    assert_eq!(result.message, upserted(0), "{}", c.flavor);
    let result = ok(c
        .sync_with(&pipe, geo_row("SRID=3857;POINT (1 2)"), no_diff())
        .await);
    assert_eq!(result.message, upserted(1), "{}", c.flavor);
    let after = versions(&c, &table).await;
    assert_eq!(rewritten(&before, &after), vec![1], "{}", c.flavor);
    assert_eq!(stored(&c, &table, "ST_SRID(g)::text").await, "3857");
    c.delete(&pipe).await;

    // In place.
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!("CREATE TABLE \"{source}\" (ts timestamptz NOT NULL, id bigint NOT NULL, g geometry)"),
            format!(
                "INSERT INTO \"{source}\" SELECT '2024-01-01 00:00:00+00', id, \
                 'SRID=4326;POINT(1 2)' FROM generate_series(1, 2) AS id"
            ),
        ],
    )
    .await;
    let params = serde_json::json!({
        "columns": {"datetime": "ts", "id": "id"},
        "upsert": true,
        "upsert_changed_only": true,
        "sql": format!("SELECT ts, id, g FROM \"{source}\""),
    })
    .to_string();
    let pipe = c.pipe(&c.conn.keys(), "srid", None, &params);
    inplace_sync(&c, &pipe).await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    inplace_steps(
        &c,
        &pipe,
        &source,
        &table,
        true,
        &[
            ("", vec![]),
            ("g = ST_SetSRID(g, 3857) WHERE id = 2", vec![2]),
        ],
    )
    .await;
    assert_eq!(stored(&c, &table, "ST_SRID(g)::text").await, "4326;3857");
    c.delete(&pipe).await;
    exec_all(&c, &[format!("DROP TABLE \"{source}\"")]).await;
});

/// Replace the table's unique keys with a unique index on key expressions (as a table made by
/// a release whose NULL-safe key was `COALESCE(...)`), so that the upsert's conflict target is
/// `("ts", (COALESCE("id", -987654321)))`.
async fn expression_unique_key(c: &Ctx, table: &str) {
    let literal = table.replace('\'', "''");
    exec_all(
        c,
        &[
            format!(
                "DO $$ DECLARE r record; BEGIN \
                 FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = '{literal}'::regclass \
                 AND contype = 'u' LOOP \
                 EXECUTE format('ALTER TABLE {literal} DROP CONSTRAINT %I', r.conname); END LOOP; \
                 FOR r IN SELECT indexrelid::regclass::text AS name FROM pg_index \
                 WHERE indrelid = '{literal}'::regclass AND indisunique AND NOT indisprimary LOOP \
                 EXECUTE 'DROP INDEX ' || r.name; END LOOP; END $$"
            ),
            format!(
                "CREATE UNIQUE INDEX \"{}_ux\" ON {table} (ts, (COALESCE(id, -987654321)))",
                c.prefix
            ),
        ],
    )
    .await;
    let keys = c
        .conn
        .value(&format!(
            "SELECT string_agg(pg_get_indexdef(indexrelid), ';') FROM pg_index \
             WHERE indrelid = '{literal}'::regclass AND indisunique"
        ))
        .await;
    let Some(SqlValue::Text(keys)) = keys else {
        panic!("{}: {keys:?}", c.flavor);
    };
    assert!(
        !keys.contains(';') && keys.contains("COALESCE(id, "),
        "{}: {keys}",
        c.flavor
    );
}

// A unique index on key expressions: the guard's `ON CONFLICT` names those expressions.
flavor_tests!(expression_conflict_target_upserts, |c: Ctx| async move {
    if !pg_family(&c) {
        return;
    }
    let params = |changed_only: bool| {
        format!(
            r#"{{"columns": {{"datetime": "ts", "id": "id"}}, "upsert": true, "hypertable": false,
                "upsert_changed_only": {changed_only}}}"#
        )
    };
    let pipe = c.pipe("ucs", "expr", None, &params(true));
    let rows = |v: i64| kv_rows(&[(Some(1), i(1)), (None, i(v))]);
    ok(c.sync(&pipe, rows(2)).await);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    expression_unique_key(&c, &table).await;
    let before = versions(&c, &table).await;
    let result = ok(c.sync_with(&pipe, rows(2), no_diff()).await);
    assert_eq!(result.message, upserted(0), "{}", c.flavor);
    let result = ok(c.sync_with(&pipe, rows(3), no_diff()).await);
    assert_eq!(result.message, upserted(1), "{}", c.flavor);
    let after = versions(&c, &table).await;
    assert_eq!(rewritten(&before, &after), vec![-1], "{}", c.flavor);
    // Off: both rows are rewritten.
    let pipe_off = c.pipe("ucs", "expr", None, &params(false));
    ok(c.sync_with(&pipe_off, rows(3), no_diff()).await);
    let last = versions(&c, &table).await;
    assert_eq!(rewritten(&after, &last), vec![-1, 1], "{}", c.flavor);
    assert_eq!(stored(&c, &table, "v::text").await, "3;1");
    c.delete(&pipe).await;

    // In place.
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!("CREATE TABLE \"{source}\" (ts timestamp NOT NULL, id bigint, v integer)"),
            format!(
                "INSERT INTO \"{source}\" VALUES ('2024-01-01 00:00:00', 1, 1), \
                 ('2024-01-01 00:00:00', NULL, 2)"
            ),
        ],
    )
    .await;
    let params = serde_json::json!({
        "columns": {"datetime": "ts", "id": "id"},
        "upsert": true,
        "hypertable": false,
        "upsert_changed_only": true,
        "sql": format!("SELECT ts, id, v FROM \"{source}\""),
    })
    .to_string();
    let pipe = c.pipe(&c.conn.keys(), "expr", None, &params);
    inplace_sync(&c, &pipe).await;
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    expression_unique_key(&c, &table).await;
    inplace_steps(
        &c,
        &pipe,
        &source,
        &table,
        true,
        &[
            ("", vec![]),
            ("v = 3 WHERE id IS NULL", vec![-1]),
            ("v = 4 WHERE id = 1", vec![1]),
        ],
    )
    .await;
    assert_eq!(stored(&c, &table, "v::text").await, "3;4");
    c.delete(&pipe).await;
    exec_all(&c, &[format!("DROP TABLE \"{source}\"")]).await;
});

/// A partitioned target (native partitions on PostgreSQL and PostGIS, a hypertable on
/// TimescaleDB) keyed by its primary column: the datetime column is folded into the key. Frame
/// syncs, then in-place syncs into the same table, which conflict on its own key.
async fn partitioned(c: Ctx, changed_only: bool) {
    if !pg_family(&c) {
        return;
    }
    let rows = |v: i64| {
        (1..=4)
            .map(|id| {
                doc(vec![
                    ("ts", udt(2024, 1, id as u32, 0, 0)),
                    ("id", i(id)),
                    ("v", i(if id == 3 { v } else { id })),
                ])
            })
            .collect::<Vec<_>>()
    };
    let params = format!(
        r#"{{"columns": {{"datetime": "ts", "primary": "id"}}, "upsert": true,
            "upsert_changed_only": {changed_only}, "verify": {{"chunk_minutes": 1440}}}}"#
    );
    let pipe = c.pipe("ucs", "partitioned", None, &params);
    ok(c.sync(&pipe, rows(3)).await);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    if matches!(c.flavor, "postgresql" | "postgis") {
        let kind = c
            .conn
            .value(&format!(
                "SELECT relkind::text FROM pg_class WHERE oid = '{}'::regclass",
                table.replace('\'', "''")
            ))
            .await;
        assert_eq!(kind, Some(SqlValue::Text("p".into())), "{}", c.flavor);
    }
    let mut last = versions(&c, &table).await;
    for (v, expected) in [(3, vec![]), (30, vec![3])] {
        let result = ok(c.sync_with(&pipe, rows(v), no_diff()).await);
        let now = versions(&c, &table).await;
        if changed_only {
            assert_eq!(result.message, upserted(expected.len()), "{}", c.flavor);
            assert_eq!(rewritten(&last, &now), expected, "{}", c.flavor);
        } else {
            assert_eq!(rewritten(&last, &now), vec![1, 2, 3, 4], "{}", c.flavor);
        }
        last = now;
    }
    assert_eq!(stored(&c, &table, "v::text").await, "1;2;30;4");

    // In place, into the same table.
    let source = format!("{}_src", c.prefix);
    exec_all(
        &c,
        &[
            format!("CREATE TABLE \"{source}\" (ts timestamptz NOT NULL, id bigint NOT NULL, v bigint)"),
            format!(
                "INSERT INTO \"{source}\" SELECT '2024-01-01'::timestamptz + ((id - 1) || ' days')::interval, \
                 id, CASE WHEN id = 3 THEN 30 ELSE id END FROM generate_series(1, 4) AS id"
            ),
        ],
    )
    .await;
    let inplace_params = serde_json::json!({
        "columns": {"datetime": "ts", "primary": "id"},
        "upsert": true,
        "upsert_changed_only": changed_only,
        "verify": {"chunk_minutes": 1440},
        "sql": format!("SELECT ts, id, v FROM \"{source}\""),
    })
    .to_string();
    let inplace_pipe = c.pipe("ucs", "partitioned", None, &inplace_params);
    inplace_steps(
        &c,
        &inplace_pipe,
        &source,
        &table,
        true,
        &[("", vec![]), ("v = 31 WHERE id = 3", vec![3])],
    )
    .await;
    assert_eq!(stored(&c, &table, "v::text").await, "1;2;31;4");
    exec_all(&c, &[format!("DROP TABLE \"{source}\"")]).await;
    c.delete(&pipe).await;
}

flavor_tests!(partitioned_upserts, |c: Ctx| async move {
    partitioned(c, true).await;
});

flavor_tests!(partitioned_upserts_by_default, |c: Ctx| async move {
    partitioned(c, false).await;
});
