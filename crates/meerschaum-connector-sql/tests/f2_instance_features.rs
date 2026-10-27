//! rc15 instance features on every flavor under test: `verify schema` plans and applies them
//! (never dropping a column), row estimates (`Pipe.get_rowcount(estimate=True)`), the hidden
//! internal `mrsm` pipes of `fetch_pipes_keys`, and reads that report database errors instead
//! of "no table".

mod common;

use common::*;
use meerschaum_connector::schema::SchemaChangeKind;
use meerschaum_connector::{DataQuery, InstanceConnector, PipesKeysQuery};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::{Map, Pipe, Value};

fn rows() -> Vec<Doc> {
    (0..4)
        .map(|n| {
            doc(vec![
                ("ts", ndt(2024, 1, 1, n, 0)),
                ("id", i(n as i64)),
                ("v", i(10 * n as i64)),
                ("code", s(&format!("{}", 100 + n))),
                ("label", s(&format!("row {n}"))),
            ])
        })
        .collect()
}

fn with_dtypes(pipe: &Pipe, dtypes: &str) -> Pipe {
    let mut out = pipe.clone();
    let mut patch = Map::new();
    patch.insert("dtypes".into(), Value::Map(params(dtypes)));
    out.update_parameters(patch);
    out
}

flavor_tests!(verify_schema_plans_and_applies, |c: Ctx| async move {
    let pipe = c.pipe(
        "schema",
        "weather",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    ok(c.sync(&pipe, rows()).await);

    // Matching dtypes: nothing to do.
    let same = with_dtypes(&pipe, r#"{"id": "int", "label": "string"}"#);
    let plan = c.conn.get_pipe_schema_plan(&same).await.unwrap();
    assert!(plan.exists);
    assert!(plan.is_empty(), "{} {:?}", c.flavor, plan.changes);

    // A widening, an added column and a type change that needs confirmation.
    let declared = with_dtypes(
        &pipe,
        r#"{"id": "int", "v": "float", "extra": "string", "code": "int"}"#,
    );
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    let kind = |col: &str| {
        plan.changes
            .iter()
            .find(|ch| ch.column.eq_ignore_ascii_case(col))
            .map(|ch| ch.kind)
    };
    assert_eq!(
        kind("extra"),
        Some(SchemaChangeKind::AddColumn),
        "{}",
        c.flavor
    );
    if c.flavor == "sqlite" {
        // SQLite rebuilds the table for any type change (always confirmed).
        assert_eq!(kind("v"), Some(SchemaChangeKind::Change));
    } else {
        assert_eq!(
            kind("v"),
            Some(SchemaChangeKind::Widen),
            "{} {:?}",
            c.flavor,
            plan.changes
        );
    }
    assert_eq!(
        kind("code"),
        Some(SchemaChangeKind::Change),
        "{} {:?}",
        c.flavor,
        plan.changes
    );
    assert!(kind("id").is_none(), "{} {:?}", c.flavor, plan.changes);
    for q in &plan.queries {
        let upper = q.to_uppercase();
        assert!(
            !upper.contains("DROP COLUMN") || upper.contains("_MRSM_TMP"),
            "never drops a column: {q}"
        );
    }

    if c.flavor == "geopackage" {
        // GeoPackage tables are never rebuilt: only the added column applies.
        assert!(plan.blocked.is_some());
        c.delete(&pipe).await;
        return;
    }

    // `--noask` (safe only): the added column (and, off SQLite, the widening) apply.
    let res = c.conn.apply_pipe_schema(&declared, &plan, true).await;
    assert!(res.success, "{}: {}", c.flavor, res.message);
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    assert!(
        plan.changes.iter().all(|ch| !ch.is_safe()),
        "{} {:?}",
        c.flavor,
        plan.changes
    );
    assert!(kind_after(&plan, "code") == Some(SchemaChangeKind::Change));

    // Confirmed: everything applies, and the table then matches.
    let res = c.conn.apply_pipe_schema(&declared, &plan, false).await;
    assert!(res.success, "{}: {}", c.flavor, res.message);
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    assert!(plan.is_empty(), "{} {:?}", c.flavor, plan.changes);

    // Every row survived, with its values.
    assert_eq!(c.rowcount(&declared, DataQuery::default()).await, Some(4));
    let docs = c.docs(&declared).await;
    let codes: Vec<i64> = docs
        .iter()
        .filter_map(|d| d.get("code").and_then(as_int))
        .collect();
    assert_eq!(codes.len(), 4, "{} {docs:?}", c.flavor);
    assert!(codes.contains(&103), "{} {codes:?}", c.flavor);

    // A stale plan is refused.
    let stale = c
        .conn
        .get_pipe_schema_plan(&with_dtypes(&declared, r#"{"more": "int"}"#))
        .await
        .unwrap();
    let refused = c.conn.apply_pipe_schema(&declared, &stale, false).await;
    assert!(!refused.success);
    c.delete(&pipe).await;
});

flavor_tests!(verify_schema_keeps_partition_keys, |c: Ctx| async move {
    // Tables of pipes with a `datetime` column are partitioned by it by default (PostgreSQL,
    // PostGIS, MySQL, MariaDB and MSSQL partitions; TimescaleDB hypertables).
    if !matches!(
        c.flavor,
        "postgresql" | "postgis" | "timescaledb" | "mysql" | "mariadb" | "mssql"
    ) {
        return;
    }
    let pipe = c.pipe(
        "partkey",
        "weather",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    ok(c.sync(&pipe, rows()).await);
    let change = |plan: &meerschaum_connector::SchemaPlan, col: &str| {
        plan.changes
            .iter()
            .find(|ch| ch.column.eq_ignore_ascii_case(col))
            .cloned()
    };

    // No database turns a timestamp partition key into an integer in place: that change is
    // blocked, and the plan's other changes still apply.
    let declared = with_dtypes(&pipe, r#"{"ts": "int", "v": "float", "extra": "string"}"#);
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    let ts = change(&plan, "ts").expect("a change for ts");
    assert!(!ts.is_applicable(), "{} {:?}", c.flavor, plan.changes);
    let reason = plan.blocked.clone().unwrap_or_default();
    assert!(
        reason.contains("because the table is partitioned by it")
            || reason.contains("cannot change hypertable column 'ts'"),
        "{}: {reason}",
        c.flavor
    );
    let res = c.conn.apply_pipe_schema(&declared, &plan, false).await;
    assert!(!res.success, "{}: {}", c.flavor, res.message);
    assert!(
        res.message.contains("added column 'extra'"),
        "{}: {}",
        c.flavor,
        res.message
    );
    let after = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    assert_eq!(after.changes.len(), 1, "{} {:?}", c.flavor, after.changes);
    assert!(change(&after, "ts").is_some_and(|ch| !ch.is_applicable()));
    assert_eq!(c.rowcount(&declared, DataQuery::default()).await, Some(4));

    if c.flavor == "timescaledb" {
        // A hypertable's time column may change between timestamp types.
        let aware = with_dtypes(&pipe, r#"{"ts": "datetime64[ns, UTC]"}"#);
        let plan = c.conn.get_pipe_schema_plan(&aware).await.unwrap();
        assert!(plan.blocked.is_none(), "{:?}", plan.blocked);
        let res = c.conn.apply_pipe_schema(&aware, &plan, false).await;
        assert!(res.success, "{}", res.message);
        assert!(
            c.conn
                .get_pipe_schema_plan(&aware)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(c.rowcount(&aware, DataQuery::default()).await, Some(4));
    }
    c.delete(&pipe).await;
});

fn kind_after(plan: &meerschaum_connector::SchemaPlan, col: &str) -> Option<SchemaChangeKind> {
    plan.changes
        .iter()
        .find(|ch| ch.column.eq_ignore_ascii_case(col))
        .map(|ch| ch.kind)
}

flavor_tests!(verify_schema_includes_static_pipes, |c: Ctx| async move {
    let pipe = c.pipe(
        "static",
        "readings",
        None,
        r#"{"columns": {"datetime": "ts"}, "static": true}"#,
    );
    ok(c.sync(&pipe, rows()).await);
    let declared = with_dtypes(&pipe, r#"{"extra": "float"}"#);
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    assert_eq!(plan.changes.len(), 1, "{} {:?}", c.flavor, plan.changes);
    assert_eq!(plan.changes[0].kind, SchemaChangeKind::AddColumn);
    let res = c.conn.apply_pipe_schema(&declared, &plan, true).await;
    assert!(res.success, "{}: {}", c.flavor, res.message);
    assert!(
        c.conn
            .get_pipe_schema_plan(&declared)
            .await
            .unwrap()
            .is_empty()
    );
    c.delete(&pipe).await;
});

flavor_tests!(verify_schema_of_a_missing_table, |c: Ctx| async move {
    let pipe = c.pipe("missing", "nothing", None, r#"{"dtypes": {"a": "int"}}"#);
    let plan = c.conn.get_pipe_schema_plan(&pipe).await.unwrap();
    assert!(!plan.exists);
    assert!(plan.is_empty());
});

flavor_tests!(row_estimates, |c: Ctx| async move {
    let pipe = c.pipe(
        "estimate",
        "rows",
        None,
        r#"{"columns": {"datetime": "ts"}}"#,
    );
    // A missing table has no rows.
    assert_eq!(
        c.conn
            .get_pipe_rowcount_estimate(&pipe, &DataQuery::default(), None)
            .await
            .unwrap(),
        None
    );
    ok(c.sync(&pipe, rows()).await);
    // Small tables are counted exactly.
    let estimate =
        InstanceConnector::get_pipe_rowcount_estimate(&c.conn, &pipe, &DataQuery::default())
            .await
            .unwrap();
    assert_eq!(estimate, Some(4), "{}", c.flavor);
    // Bounds and params are always exact.
    let bounded = DataQuery {
        begin: Some(nbound(2024, 1, 1, 2, 0)),
        ..Default::default()
    };
    let n = c
        .conn
        .get_pipe_rowcount_estimate(&pipe, &bounded, None)
        .await
        .unwrap();
    assert_eq!(n, c.rowcount(&pipe, bounded.clone()).await, "{}", c.flavor);
    c.delete(&pipe).await;
});

flavor_tests!(
    internal_pipes_are_hidden_from_listings,
    |c: Ctx| async move {
        let user = c.pipe("visible", "m", None, "{}");
        let internal = Pipe::builder("mrsm", "pipeline")
            .location(Some(&format!("{}_daily", c.prefix)))
            .instance(&c.conn.keys())
            .build()
            .unwrap();
        ok(c.conn.register_pipe(&user).await);
        ok(c.conn.register_pipe(&internal).await);

        let all = c
            .conn
            .fetch_pipes_keys(&PipesKeysQuery::default())
            .await
            .unwrap();
        let names: Vec<String> = all
            .entries()
            .iter()
            .map(|e| e.connector_keys.clone())
            .collect();
        assert!(
            names.iter().any(|n| n == user.connector_keys()),
            "{names:?}"
        );
        assert!(!names.iter().any(|n| n == "mrsm"), "{} {names:?}", c.flavor);

        // Named explicitly, the registry is still found (saved Pipelines read it this way).
        let registry = c
            .conn
            .fetch_pipes_keys(&PipesKeysQuery {
                connector_keys: vec!["mrsm".into()],
                metric_keys: vec!["pipeline".into()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            registry
                .entries()
                .iter()
                .any(|e| e.location_key.as_deref() == internal.location_key()),
            "{}",
            c.flavor
        );
        let _ = c.conn.delete_pipe(&internal).await;
        let _ = c.conn.delete_pipe(&user).await;
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_report_database_errors_and_missing_tables_are_empty() {
    // A missing table: no data, no rows, no sync time.
    let c = ctx("sqlite", "f2_read_errors").await.unwrap();
    let pipe = c.pipe("ghost", "m", None, r#"{"columns": {"datetime": "ts"}}"#);
    assert!(
        c.conn
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        c.conn
            .get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        c.conn
            .get_sync_time(&pipe, None, true, false)
            .await
            .unwrap(),
        None
    );
    // A table without the datetime column has no sync time (no query error).
    let other = c.pipe(
        "nodt",
        "m",
        None,
        r#"{"columns": {"datetime": "missing_ts"}}"#,
    );
    ok(c.sync(&other, vec![doc(vec![("a", i(1))])]).await);
    assert_eq!(
        c.conn
            .get_sync_time(&other, None, true, false)
            .await
            .unwrap(),
        None
    );

    // An unreachable database is an error, not a missing table.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let spec = ConnectionSpec::from_uri(
        &format!("postgresql://nobody:not-a-secret@127.0.0.1:{port}/nodb"),
        Some("f2_dead"),
        None,
    )
    .unwrap();
    let settings = InstanceSettings::defaults(spec.flavor);
    let dead = SqlConnector::connect_lazy("f2_dead", spec, settings).unwrap();
    let pipe = Pipe::builder("plugin:x", "y")
        .instance("sql:f2_dead")
        .parameters(params(r#"{"columns": {"datetime": "ts"}}"#))
        .build()
        .unwrap();
    assert!(
        dead.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .is_err()
    );
    assert!(dead.get_sync_time(&pipe, None, true, false).await.is_err());
    assert!(
        dead.get_pipe_data(&pipe, &DataQuery::default())
            .await
            .is_err()
    );
    assert!(
        dead.get_pipe_rowcount_cached(&pipe, &DataQuery::default(), None)
            .await
            .is_err()
    );
}

flavor_tests!(large_tables_use_the_catalog_estimate, |c: Ctx| async move {
    let table = format!("{}_big", c.prefix);
    let n: i64 = 150_000;
    let t = meerschaum_connector_sql::ident::sql_item_name(&table, c.conn.flavor(), None);
    let setup: Vec<String> = match c.flavor {
        "postgresql" | "timescaledb" | "postgis" => vec![
            format!("CREATE TABLE {t} AS SELECT generate_series(1, {n}) AS id"),
            format!("ANALYZE {t}"),
        ],
        "mysql" | "mariadb" => vec![
            format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY)"),
            format!(
                "INSERT INTO {t} (id) WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 \
                 FROM seq WHERE n < {n}) SELECT n FROM seq"
            ),
            format!("ANALYZE TABLE {t}"),
        ],
        "mssql" => vec![format!(
            "SELECT TOP {n} ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id INTO {t} \
             FROM sys.all_objects a CROSS JOIN sys.all_objects b"
        )],
        _ => return,
    };
    if matches!(c.flavor, "mysql" | "mariadb") {
        // `WITH RECURSIVE` stops at 1000 levels by default.
        let _ = c
            .conn
            .exec("SET SESSION cte_max_recursion_depth = 1000000")
            .await;
        let _ = c
            .conn
            .exec("SET SESSION max_recursive_iterations = 1000000")
            .await;
    }
    for q in &setup {
        if c.conn.exec(q).await.is_none() {
            eprintln!("Skipping {}: could not run {q}", c.flavor);
            let _ = c.conn.exec(&format!("DROP TABLE {t}")).await;
            return;
        }
    }
    let pipe = c.pipe("big", "rows", None, &format!(r#"{{"target": "{table}"}}"#));
    c.conn.enable_query_log();
    let estimate = c
        .conn
        .get_pipe_rowcount_estimate(&pipe, &DataQuery::default(), None)
        .await
        .unwrap()
        .unwrap();
    let log = c.conn.take_query_log();
    // The exact count's query (`SELECT COUNT(*)\nFROM "src"`).
    let counted = log
        .iter()
        .any(|q| q.to_uppercase().contains("COUNT(*)\nFROM"));
    // The statistics may lag; a missing estimate falls back to counting (still correct).
    if counted {
        assert_eq!(estimate, n, "{}", c.flavor);
    } else {
        assert!(
            (estimate - n).abs() <= n / 4,
            "{}: estimate {estimate} vs {n}",
            c.flavor
        );
    }
    if c.flavor != "mysql" {
        // MySQL 5.7 caches `information_schema.TABLES`; the others read fresh statistics.
        assert!(!counted, "{}: {log:?}", c.flavor);
    }
    // The exact count is still exact.
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(n));
    let _ = c.conn.exec(&format!("DROP TABLE {t}")).await;
});
