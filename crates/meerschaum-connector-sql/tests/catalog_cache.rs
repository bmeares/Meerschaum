//! The connector's catalog cache (`instance/catalog.rs`) and the in-place sync's schema reads:
//! a static in-place pipe reads no columns once warm and ignores a column its definition gains
//! (v4: `get_add_columns_queries` returns `[]` for a static pipe), a non-static one adds it and
//! sees it at once, and warm internal (engine) writes skip the catalog.
mod common;
use common::*;
use meerschaum_connector::SyncOptions;
use meerschaum_connector::pipe_ops::engine_state_parameters;
use meerschaum_connector_sql::queries::metadef::Bound;
use meerschaum_core::Value;

async fn source(c: &Ctx) -> String {
    let src = c.pipe(
        "cc",
        "src",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    let docs = (0..4u32)
        .map(|d| {
            doc(vec![
                ("ts", ndt(2024, 1, 1 + d, 0, 0)),
                ("id", i(d as i64)),
                ("a", i(10 * d as i64)),
                ("c", s(&format!("c{d}"))),
            ])
        })
        .collect();
    ok(c.sync(&src, docs).await);
    let sp = c.conn.sql_pipe(&src).unwrap();
    c.conn.pipe_table_name(&sp)
}

fn inplace_pipe(
    c: &Ctx,
    location: &str,
    is_static: bool,
    definition: &str,
) -> meerschaum_core::Pipe {
    let mut params = params(r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true}"#);
    params.insert("static".into(), Value::Bool(is_static));
    let mut fetch = meerschaum_core::Map::new();
    fetch.insert("definition".into(), Value::Str(definition.into()));
    params.insert("fetch".into(), Value::Map(fetch));
    c.pipe(
        "cc",
        "dst",
        Some(location),
        &Value::Map(params).json_dumps(),
    )
}

fn with_definition(pipe: &meerschaum_core::Pipe, definition: &str) -> meerschaum_core::Pipe {
    let mut pipe = pipe.clone();
    let mut fetch = meerschaum_core::Map::new();
    fetch.insert("definition".into(), Value::Str(definition.into()));
    pipe.parameters_mut()
        .insert("fetch".into(), Value::Map(fetch));
    pipe
}

struct Noop;

#[meerschaum_connector::async_trait]
impl meerschaum_connector_sql::instance::inplace::InplaceObserver for Noop {
    async fn before_merge(&self, _delta: &meerschaum_kernel::Frame) -> Result<(), String> {
        Ok(())
    }
}

async fn sync_inplace(c: &Ctx, pipe: &meerschaum_core::Pipe) -> meerschaum_core::SuccessTuple {
    c.conn
        .sync_pipe_inplace(pipe, None, Bound::Default, Bound::None, true)
        .await
}

async fn columns(c: &Ctx, pipe: &meerschaum_core::Pipe) -> Vec<String> {
    let mut cols: Vec<String> = c
        .conn
        .get_pipe_columns_types(pipe)
        .await
        .unwrap()
        .keys()
        .map(|k| k.to_lowercase())
        .collect();
    cols.sort();
    cols
}

/// Catalog statements in a query log: column, table and index lookups.
fn catalog_reads(log: &[String]) -> Vec<String> {
    log.iter()
        .filter(|q| {
            let q = q.to_lowercase();
            q.contains("information_schema.columns")
                || q.contains("information_schema.tables")
                || q.contains("pragma_table_info")
                || q.contains("sqlite_master")
                || q.contains("pg_catalog.pg_index")
                || q.contains("all_tab_columns")
                || q.contains("sys.columns")
        })
        .cloned()
        .collect()
}

flavor_tests!(
    static_inplace_ignores_new_definition_columns,
    |c: Ctx| async move {
        let src = source(&c).await;
        let pipe = inplace_pipe(&c, "static", true, &format!("SELECT ts, id, a FROM {src}"));
        ok(c.conn.register_pipe(&pipe).await);
        ok(sync_inplace(&c, &pipe).await);
        ok(sync_inplace(&c, &pipe).await);
        assert_eq!(columns(&c, &pipe).await, vec!["a", "id", "ts"]);

        // Warm: the static sync asks the catalog nothing (SQLite stages through its own path).
        c.conn.enable_query_log();
        ok(sync_inplace(&c, &pipe).await);
        let reads = catalog_reads(&c.conn.take_query_log());
        if !matches!(c.flavor, "sqlite" | "geopackage" | "mssql" | "oracle") {
            assert!(reads.is_empty(), "{reads:#?}");
        }

        // An observed UPSERT (the engine's ledger) reads its staged columns once, too.
        for _ in 0..2 {
            ok(c.conn
                .sync_pipe_inplace_observed(&pipe, None, Bound::Default, Bound::None, true, &Noop)
                .await);
        }
        c.conn.enable_query_log();
        ok(c.conn
            .sync_pipe_inplace_observed(&pipe, None, Bound::Default, Bound::None, true, &Noop)
            .await);
        let reads = catalog_reads(&c.conn.take_query_log());
        if !matches!(c.flavor, "sqlite" | "geopackage" | "mssql" | "oracle") {
            assert!(reads.is_empty(), "{reads:#?}");
        }

        // PARITY: v4 keeps a static table as it is when the definition gains a column.
        let gained = with_definition(&pipe, &format!("SELECT ts, id, a, c FROM {src}"));
        ok(sync_inplace(&c, &gained).await);
        assert_eq!(columns(&c, &gained).await, vec!["a", "id", "ts"]);
        let rows = c
            .conn
            .get_pipe_rowcount(&gained, &Default::default())
            .await
            .unwrap();
        assert_eq!(rows, Some(4));
    }
);

flavor_tests!(inplace_sees_columns_it_adds, |c: Ctx| async move {
    let src = source(&c).await;
    let pipe = inplace_pipe(
        &c,
        "evolving",
        false,
        &format!("SELECT ts, id, a FROM {src}"),
    );
    ok(c.conn.register_pipe(&pipe).await);
    ok(sync_inplace(&c, &pipe).await);
    ok(sync_inplace(&c, &pipe).await);
    assert_eq!(columns(&c, &pipe).await, vec!["a", "id", "ts"]);

    // The cached layout is forgotten by the connector's own `ALTER TABLE ... ADD`.
    let gained = with_definition(&pipe, &format!("SELECT ts, id, a, c FROM {src}"));
    ok(sync_inplace(&c, &gained).await);
    assert_eq!(columns(&c, &gained).await, vec!["a", "c", "id", "ts"]);
    ok(sync_inplace(&c, &gained).await);
    assert_eq!(columns(&c, &gained).await, vec!["a", "c", "id", "ts"]);
});

flavor_tests!(dropped_table_is_seen_after_drop, |c: Ctx| async move {
    let src = c.pipe(
        "cc",
        "dropme",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    ok(c.sync(
        &src,
        vec![doc(vec![("ts", ndt(2024, 1, 1, 0, 0)), ("id", i(1))])],
    )
    .await);
    assert!(c.conn.pipe_exists(&src).await.unwrap());
    assert!(!columns(&c, &src).await.is_empty());
    ok(c.conn.drop_pipe(&src).await);
    assert!(!c.conn.pipe_exists(&src).await.unwrap());
    assert!(columns(&c, &src).await.is_empty());
    // A re-sync recreates the table with the new layout.
    ok(c.sync(
        &src,
        vec![doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", i(1)),
            ("b", s("x")),
        ])],
    )
    .await);
    assert_eq!(columns(&c, &src).await, vec!["b", "id", "ts"]);
});

// Engine state writes (non-static `mrsm` nodes, as `InstanceState::write_rows` sends them):
// once warm they take neither the initialization lock nor a catalog read, and rows with a new
// column still extend the table.
flavor_tests!(warm_internal_writes_skip_the_catalog, |c: Ctx| async move {
    if matches!(c.flavor, "oracle" | "mssql" | "duckdb" | "geopackage") {
        return;
    }
    let mut parameters = engine_state_parameters("engine_nodes").unwrap();
    parameters.insert("target".into(), format!("{}nodes", c.prefix).into());
    parameters.insert("static".into(), Value::Bool(false));
    let pipe = meerschaum_core::Pipe::builder("mrsm", "engine_nodes")
        .instance(&c.conn.keys())
        .parameters(parameters)
        .kwarg("temporary", true)
        .build()
        .unwrap();
    let row = |n: i64, extra: bool| {
        let mut pairs = vec![
            ("run_id", s("synthetic-run")),
            ("pipe_ref", s(&format!("synthetic-{n}"))),
            ("started", ndt(2026, 1, 1, 0, 0)),
            ("message", s("ok")),
        ];
        if extra {
            pairs.push(("cc_extra", s("x")));
        }
        doc(pairs)
    };
    let opts = SyncOptions::default();
    ok(c.sync_with(&pipe, vec![row(1, false)], opts.clone()).await);
    ok(c.sync_with(&pipe, vec![row(2, false)], opts.clone()).await);
    c.conn.enable_query_log();
    ok(c.sync_with(&pipe, vec![row(3, false)], opts.clone()).await);
    let log = c.conn.take_query_log();
    let reads = catalog_reads(&log);
    assert!(reads.is_empty(), "{log:#?}");
    // A new column takes the cold path: the table is extended.
    ok(c.sync_with(&pipe, vec![row(4, true)], opts.clone()).await);
    let cols = columns(&c, &pipe).await;
    assert!(cols.contains(&"cc_extra".to_string()), "{cols:?}");
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let table = c.conn.pipe_table_name(&sp);
    let _ = c.conn.exec(&format!("DROP TABLE {table}")).await;
});

// A table dropped behind the connector's back (another process: the driver directly here) is
// recreated by the second sync at the latest: the first may fail against the cached layout,
// and that failure forgets it.
flavor_tests!(table_dropped_elsewhere_recovers, |c: Ctx| async move {
    let pipe = c.pipe(
        "cc",
        "elsewhere",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    let row = |d: u32| doc(vec![("ts", ndt(2024, 1, d, 0, 0)), ("id", i(d as i64))]);
    ok(c.sync(&pipe, vec![row(1)]).await);
    ok(c.sync(&pipe, vec![row(2)]).await);
    assert!(c.conn.pipe_exists(&pipe).await.unwrap());
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let table = c.conn.pipe_table_name(&sp);
    c.conn
        .driver()
        .exec(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    let first = c.sync(&pipe, vec![row(3)]).await;
    if !first.success {
        ok(c.sync(&pipe, vec![row(3)]).await);
    }
    assert!(c.conn.pipe_exists(&pipe).await.unwrap());
    let rows = c
        .conn
        .get_pipe_rowcount(&pipe, &Default::default())
        .await
        .unwrap();
    assert_eq!(rows, Some(1));
});
