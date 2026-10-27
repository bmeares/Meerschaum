//! In-place syncs of rollups (rc15 B1): a `sql:` pipe whose definition computes its datetime
//! (`date(ts)`, `CAST(ts AS DATE)`, ...) re-syncs without duplicating the rows inside the
//! backtrack window, on every flavor. On SQLite the table is created with typed columns and the
//! computed datetimes are stored in the canonical text form the other syncs write.
mod common;
use common::*;
use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::queries::metadef::Bound;
use meerschaum_core::Value;

/// `day` truncation of `ts` in the flavor's dialect.
fn day_expr(flavor: &str) -> &'static str {
    match flavor {
        "sqlite" | "geopackage" => "date(ts)",
        "mysql" | "mariadb" => "DATE(ts)",
        "oracle" => "TRUNC(ts)",
        _ => "CAST(ts AS DATE)",
    }
}

async fn source(c: &Ctx) -> (meerschaum_core::Pipe, String) {
    let src = c.pipe(
        "b1",
        "temps",
        Some("src"),
        r#"{"columns": {"datetime": "ts", "id": "station"}}"#,
    );
    let mut docs = Vec::new();
    for d in 0..4u32 {
        for h in [0u32, 6, 12, 18] {
            for st in ["a", "b"] {
                docs.push(doc(vec![
                    ("ts", ndt(2024, 1, 1 + d, h, 0)),
                    ("station", s(st)),
                    ("temperature", fl((d * 10 + h) as f64)),
                ]));
            }
        }
    }
    ok(c.sync(&src, docs).await);
    let sp = c.conn.sql_pipe(&src).unwrap();
    let table = c.conn.pipe_table_name(&sp);
    (src, table)
}

fn rollup_pipe(c: &Ctx, location: &str, definition: &str) -> meerschaum_core::Pipe {
    let mut params = params(r#"{"columns": {"datetime": "day", "id": "station"}}"#);
    let mut fetch = meerschaum_core::Map::new();
    fetch.insert("definition".into(), Value::Str(definition.into()));
    params.insert("fetch".into(), Value::Map(fetch));
    c.pipe(
        "b1",
        "rollup",
        Some(location),
        &Value::Map(params).json_dumps(),
    )
}

async fn sync_inplace(c: &Ctx, pipe: &meerschaum_core::Pipe) -> meerschaum_core::SuccessTuple {
    c.conn
        .sync_pipe_inplace(pipe, None, Bound::Default, Bound::None, true)
        .await
}

flavor_tests!(
    inplace_rollups_resync_without_duplicates,
    |c: Ctx| async move {
        let (src, src_table) = source(&c).await;
        let day = day_expr(c.flavor);
        let rollup = rollup_pipe(
            &c,
            "r",
            &format!(
                "SELECT {day} AS day, station, AVG(temperature) AS avg_t FROM {src_table} GROUP BY {day}, station"
            ),
        );
        ok(c.conn.register_pipe(&rollup).await);
        for attempt in 0..3 {
            let res = ok(sync_inplace(&c, &rollup).await);
            assert_eq!(
                c.rowcount(&rollup, DataQuery::default()).await,
                Some(8),
                "{} sync {attempt}: {}",
                c.flavor,
                res.message
            );
        }

        // A chained child of the rollup stays at its 8 keys too.
        let rollup_sp = c.conn.sql_pipe(&rollup).unwrap();
        let rollup_table = c.conn.pipe_table_name(&rollup_sp);
        let child = rollup_pipe(
            &c,
            "child",
            &format!("SELECT day, station, avg_t * 2 AS double_t FROM {rollup_table}"),
        );
        ok(c.conn.register_pipe(&child).await);
        for attempt in 0..3 {
            let res = ok(sync_inplace(&c, &child).await);
            assert_eq!(
                c.rowcount(&child, DataQuery::default()).await,
                Some(8),
                "{} child sync {attempt}: {}",
                c.flavor,
                res.message
            );
        }

        if c.is_sqlite() || c.flavor == "geopackage" {
            // Typed columns: the computed `day` and `avg_t` are not untyped.
            let types = c
                .conn
                .get_table_cols_types(
                    &rollup_sp.target,
                    c.conn.get_pipe_schema(&rollup_sp).as_deref(),
                )
                .await;
            assert!(
                types.values().all(|t| !t.is_empty() && t != "NULL"),
                "untyped columns: {types:?}"
            );
            let canonical = c
            .conn
            .value(&format!(
                "SELECT COUNT(*) FROM {rollup_table} WHERE day GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]'"
            ))
            .await
            .and_then(|v| v.as_i64());
            assert_eq!(canonical, Some(8), "datetimes are not canonical");
        }
        c.delete(&child).await;
        c.delete(&rollup).await;
        c.delete(&src).await;
    }
);

// A table an older version created with `CREATE TABLE ... AS SELECT date(ts) ...` (untyped
// columns, date-only text) heals: the next syncs add no duplicates.
flavor_tests!(
    inplace_rollups_heal_old_sqlite_tables,
    |c: Ctx| async move {
        if !c.is_sqlite() {
            return;
        }
        let (src, src_table) = source(&c).await;
        let definition = format!(
            "SELECT date(ts) AS day, station, AVG(temperature) AS avg_t FROM {src_table} GROUP BY date(ts), station"
        );
        let rollup = rollup_pipe(&c, "old", &definition);
        let sp = c.conn.sql_pipe(&rollup).unwrap();
        let table = c.conn.pipe_table_name(&sp);
        assert!(
            c.conn
                .exec(&format!(
                    "CREATE TABLE {table} AS SELECT * FROM ({definition})"
                ))
                .await
                .is_some()
        );
        ok(c.conn.register_pipe(&rollup).await);
        for attempt in 0..3 {
            let res = ok(sync_inplace(&c, &rollup).await);
            assert_eq!(
                c.rowcount(&rollup, DataQuery::default()).await,
                Some(8),
                "sync {attempt}: {}",
                res.message
            );
        }
        c.delete(&rollup).await;
        c.delete(&src).await;
    }
);

// `fetch: {replace: true}` with sub-second datetimes: the definition's bound may be wider than
// the window deleted (SQLite's `datetime(...)` drops the fraction of the sync time minus the
// backtrack), so only the staged rows of the window are inserted; re-syncs add nothing.
flavor_tests!(inplace_replace_sub_second_rows, |c: Ctx| async move {
    let src = c.pipe(
        "b1",
        "subsec",
        Some("src"),
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    let at = |h: u32, m: u32, s: u32, us: u32| {
        meerschaum_kernel::value::Value::Datetime(meerschaum_kernel::value::DateTimeValue::naive(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_micro_opt(h, m, s, us)
                .unwrap(),
        ))
    };
    ok(c.sync(
        &src,
        vec![
            doc(vec![
                ("ts", at(9, 59, 0, 250_000)),
                ("id", i(1)),
                ("v", i(1)),
            ]),
            doc(vec![
                ("ts", at(10, 0, 0, 500_000)),
                ("id", i(1)),
                ("v", i(2)),
            ]),
        ],
    )
    .await);
    let src_sp = c.conn.sql_pipe(&src).unwrap();
    let src_table = c.conn.pipe_table_name(&src_sp);
    let mut parameters = params(r#"{"columns": {"datetime": "ts", "id": "id"}}"#);
    let mut fetch = meerschaum_core::Map::new();
    fetch.insert(
        "definition".into(),
        Value::Str(format!("SELECT * FROM {src_table}")),
    );
    fetch.insert("backtrack_minutes".into(), Value::Int(1));
    fetch.insert("replace".into(), Value::Bool(true));
    parameters.insert("fetch".into(), Value::Map(fetch));
    let child = c.pipe("b1", "subsec", None, &Value::Map(parameters).json_dumps());
    ok(c.conn.register_pipe(&child).await);
    for attempt in 0..3 {
        let res = ok(sync_inplace(&c, &child).await);
        assert_eq!(
            c.rowcount(&child, DataQuery::default()).await,
            Some(2),
            "{} sync {attempt}: {}",
            c.flavor,
            res.message
        );
    }
    c.delete(&child).await;
    c.delete(&src).await;
});

// `fetch: {replace: true}`: the fetched window is replaced, so rows that left the source's
// window leave the pipe too.
flavor_tests!(inplace_replace_windows, |c: Ctx| async move {
    let (src, src_table) = source(&c).await;
    let day = day_expr(c.flavor);
    let mut parameters = params(r#"{"columns": {"datetime": "day", "id": "station"}}"#);
    let mut fetch = meerschaum_core::Map::new();
    fetch.insert(
        "definition".into(),
        Value::Str(format!(
            "SELECT {day} AS day, station, AVG(temperature) AS avg_t FROM {src_table} GROUP BY {day}, station"
        )),
    );
    fetch.insert("replace".into(), Value::Bool(true));
    parameters.insert("fetch".into(), Value::Map(fetch));
    let rollup = c.pipe("b1", "replace", None, &Value::Map(parameters).json_dumps());
    ok(c.conn.register_pipe(&rollup).await);
    ok(sync_inplace(&c, &rollup).await);
    assert_eq!(c.rowcount(&rollup, DataQuery::default()).await, Some(8));
    let res = ok(sync_inplace(&c, &rollup).await);
    assert!(res.message.starts_with("Replaced "), "{}", res.message);
    assert_eq!(c.rowcount(&rollup, DataQuery::default()).await, Some(8));

    // Station `b` disappears from the source's last day: the replaced window drops it.
    ok(c.conn
        .clear_pipe(
            &src,
            &DataQuery {
                begin: Some(nbound(2024, 1, 4, 0, 0)),
                params: Some(params(r#"{"station": "b"}"#)),
                ..Default::default()
            },
        )
        .await);
    ok(sync_inplace(&c, &rollup).await);
    assert_eq!(c.rowcount(&rollup, DataQuery::default()).await, Some(7));
    c.delete(&rollup).await;
    c.delete(&src).await;
});
