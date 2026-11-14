//! `upsert_changed_only`: a PostgreSQL-family upsert leaves the rows whose values would not
//! change untouched (same `xmin` and `ctid`), on the frame path (`sync_pipe`) and the in-place
//! path (`sync_pipe_inplace`); other flavors ignore the parameter. Table prefix `w5sql_`.
mod common;

use common::*;
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
    if !pipe.upsert_changed_only() {
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
    // `arr` is left out: an upsert casts to the column's catalog type, `ARRAY` for every
    // array (see `inplace_array_upserts_skip_unchanged_rows`).
    let geo_cols = if postgis { ", g, gg" } else { "" };
    let params = |changed_only: bool| {
        serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "upsert": true,
            "upsert_changed_only": changed_only,
            "sql": format!("SELECT ts, id, v, n, t, doc, jb, b, x, pt, iv{geo_cols} FROM \"{source}\""),
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
