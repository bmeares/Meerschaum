//! Placement of the Syncing Engine's state pipes (§5.8, GAP-001): on schema flavors the
//! `mrsm_engine_*` targets live in the internal schema (`_mrsm_internal`), suffixed with the
//! instance schema when that is not the flavor's default, so two instances on one database whose
//! connector schemas differ keep disjoint state; schema-less flavors keep them beside the
//! instance tables (where v4 also puts its temporary tables). Leases already key on the resolved
//! schema, never on the connector label. Schemas and tables are `w17bg1_*`, dropped at the end.

mod common;
use common::*;

use meerschaum_connector::{DataQuery, InstanceConnector, SyncOptions, pipe_ops};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::engine_state_namespace;
use meerschaum_connector_sql::queries::table::get_create_schema_if_not_exists_queries;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use std::time::Duration;

const INTERNAL: &str = "_mrsm_internal";
const LABEL: &str = "w17bg1";

/// A connector labelled `w17bg1` on the same database as `ctx.conn`, in `schema`.
async fn in_schema(ctx: &Ctx, schema: &str) -> SqlConnector {
    let flavor = ctx.conn.flavor();
    for q in get_create_schema_if_not_exists_queries(Some(schema), flavor).unwrap() {
        // Parallel creators can race; the connection below fails if the schema is missing.
        let _ = ctx.conn.driver().exec(&q).await;
    }
    let mut spec = ctx.conn.spec().clone();
    spec.schema = Some(schema.to_string());
    SqlConnector::connect(LABEL, spec).await.expect("connects")
}

async fn drop_schema(ctx: &Ctx, schema: &str) {
    let sql = if ctx.is_mssql() {
        format!("DROP SCHEMA IF EXISTS [{schema}]")
    } else {
        format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE")
    };
    let _ = ctx.conn.driver().exec(&sql).await;
}

fn schema_flavor(ctx: &Ctx) -> bool {
    !ctx.conn.flavor().no_schema()
}

/// One row for `mrsm:engine_ledger`.
fn ledger_frame(pipe_ref: &str) -> meerschaum_kernel::Frame {
    let params = pipe_ops::engine_state_parameters("engine_ledger").unwrap();
    let dtypes: indexmap::IndexMap<String, String> = params["dtypes"]
        .to_map()
        .iter()
        .map(|(k, v)| (k.clone(), v.py_str()))
        .collect();
    let docs = vec![doc(vec![
        ("created", udt(2026, 1, 1, 0, 0)),
        ("pipe_ref", s(pipe_ref)),
        ("seq", i(1)),
        ("begin", j(r#"{"datetime": "2026-01-01T00:00:00+00:00"}"#)),
        ("end", j(r#"{"datetime": "2026-01-02T00:00:00+00:00"}"#)),
    ])];
    pipe_enforce_dtypes(
        PipeInput::Docs(docs),
        &PipeDtypesContext {
            explicit_dtypes: dtypes.clone(),
            dtypes,
            enforce: true,
            tz_aware: true,
            mixed_numerics: true,
        },
    )
    .unwrap()
}

async fn write_ledger(conn: &SqlConnector, pipe: &Pipe, pipe_ref: &str) {
    let mut extra = Map::new();
    extra.insert("_check_temporary_tables".into(), Value::Bool(false));
    extra.insert("enforce".into(), Value::Bool(false));
    let res = InstanceConnector::sync_pipe(
        conn,
        pipe,
        ledger_frame(pipe_ref),
        &SyncOptions {
            check_existing: false,
            extra,
            ..Default::default()
        },
    )
    .await;
    assert!(res.success, "{}", res.message);
}

async fn ledger_refs(conn: &SqlConnector, pipe: &Pipe) -> Vec<String> {
    match InstanceConnector::get_pipe_data(conn, pipe, &DataQuery::default())
        .await
        .unwrap()
    {
        Some(df) => pipe_ops::frame_to_docs(&df)
            .iter()
            .filter_map(|d| d.get("pipe_ref").map(|v| v.py_str()))
            .collect(),
        None => Vec::new(),
    }
}

/// (a) where the state pipes go, per flavor; (b) two same-label connectors on one database
/// with different schemas write and forget disjoint state.
async fn placement(ctx: Ctx) {
    let flavor = ctx.conn.flavor();
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    // 15 characters (verbatim suffix) and a long one (hashed suffix).
    let (a, b) = (
        format!("w17bg1_{tag}"),
        format!("w17bg1_{tag}_hashed_namespace"),
    );

    // The instance's own (default) schema: no suffix.
    let default = InstanceConnector::engine_state_pipe(&ctx.conn, "sql:x", "engine_runs")
        .await
        .unwrap()
        .unwrap();
    let not_engine = InstanceConnector::engine_state_pipe(&ctx.conn, "sql:x", "users")
        .await
        .unwrap();
    assert!(not_engine.is_none());
    if !schema_flavor(&ctx) {
        // Schema-less flavors: unchanged, beside the instance tables.
        assert_eq!(default.param("schema"), None);
        assert_eq!(default.target(None).unwrap(), "mrsm_engine_runs");
        let pipe = InstanceConnector::engine_state_pipe(&ctx.conn, LABEL, "engine_ledger")
            .await
            .unwrap()
            .unwrap();
        let pipe_ref = format!("{LABEL}|{}|x|None", ctx.prefix);
        write_ledger(&ctx.conn, &pipe, &pipe_ref).await;
        assert!(ctx.conn.table_exists("mrsm_engine_ledger", None).await);
        assert!(ledger_refs(&ctx.conn, &pipe).await.contains(&pipe_ref));
        let user = Pipe::builder(&ctx.prefix, "x")
            .instance(LABEL)
            .build()
            .unwrap();
        assert!(
            pipe_ops::forget_engine_state(&ctx.conn, &user)
                .await
                .success
        );
        assert!(!ledger_refs(&ctx.conn, &pipe).await.contains(&pipe_ref));
        return;
    }
    if ctx.conn.schema() == flavor.default_schema() {
        assert_eq!(default.target(None).unwrap(), "mrsm_engine_runs");
    }
    assert_eq!(default.param("schema"), Some(&Value::Str(INTERNAL.into())));

    let conn_a = in_schema(&ctx, &a).await;
    let conn_b = in_schema(&ctx, &b).await;
    let ns_a = engine_state_namespace(flavor, Some(&a)).unwrap();
    let ns_b = engine_state_namespace(flavor, Some(&b)).unwrap();
    assert_eq!(ns_a, a);
    assert_eq!(ns_b.len(), 16);
    assert_eq!(conn_a.engine_state_namespace(), Some(ns_a.clone()));

    // Identical instance keys and pipe keys on both connectors.
    let pipe_ref = format!("sql:{LABEL}|sql:{LABEL}|w17bg1_src|None");
    let user = Pipe::builder(&format!("sql:{LABEL}"), "w17bg1_src")
        .instance(&format!("sql:{LABEL}"))
        .build()
        .unwrap();
    let mut ledgers = Vec::new();
    for (conn, ns) in [(&conn_a, &ns_a), (&conn_b, &ns_b)] {
        let pipe =
            InstanceConnector::engine_state_pipe(conn, &format!("sql:{LABEL}"), "engine_ledger")
                .await
                .unwrap()
                .unwrap();
        let target = format!("mrsm_engine_ledger_{ns}");
        assert_eq!(pipe.target(None).unwrap(), target);
        assert_eq!(pipe.param("schema"), Some(&Value::Str(INTERNAL.into())));
        write_ledger(conn, &pipe, &pipe_ref).await;
        assert!(conn.table_exists(&target, Some(INTERNAL)).await, "{target}");
        // Nothing named `mrsm_engine_*` in the instance schema.
        assert!(!conn.table_exists("mrsm_engine_ledger", conn.schema()).await);
        assert!(!conn.table_exists(&target, conn.schema()).await);
        ledgers.push(pipe);
    }
    // One row each: the second write did not land in the first instance's table.
    assert_eq!(
        ledger_refs(&conn_a, &ledgers[0]).await,
        vec![pipe_ref.clone()]
    );
    assert_eq!(
        ledger_refs(&conn_b, &ledgers[1]).await,
        vec![pipe_ref.clone()]
    );

    // Forgetting the pipe on A (the drop/delete/clear path) leaves B's state alone.
    assert!(pipe_ops::forget_engine_state(&conn_a, &user).await.success);
    assert!(ledger_refs(&conn_a, &ledgers[0]).await.is_empty());
    assert_eq!(
        ledger_refs(&conn_b, &ledgers[1]).await,
        vec![pipe_ref.clone()]
    );

    for (conn, pipe) in [(&conn_a, &ledgers[0]), (&conn_b, &ledgers[1])] {
        let res = InstanceConnector::drop_pipe(conn, pipe).await;
        assert!(res.success, "{}", res.message);
        assert!(
            !conn
                .table_exists(&pipe.target(None).unwrap(), Some(INTERNAL))
                .await
        );
    }
    drop_schema(&ctx, &a).await;
    drop_schema(&ctx, &b).await;
}
flavor_tests!(engine_state_placement, placement);

/// (c) Leases: two connectors with the same label on different schemas of one database never
/// contend (the lease key is the resolved schema plus the pipe keys, not the label); a second
/// holder on the same schema does.
async fn leases_by_schema(ctx: Ctx) {
    if !schema_flavor(&ctx) {
        return;
    }
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let (a, b) = (format!("w17bg1_l{tag}"), format!("w17bg1_m{tag}"));
    let conn_a = in_schema(&ctx, &a).await;
    let conn_b = in_schema(&ctx, &b).await;
    let conn_a2 = in_schema(&ctx, &a).await;
    let pipe = Pipe::builder(&format!("sql:{LABEL}"), &format!("lease_{tag}"))
        .instance(&format!("sql:{LABEL}"))
        .build()
        .unwrap();
    let ttl = Duration::from_secs(10);
    let (ga, gb) = tokio::join!(
        InstanceConnector::acquire_pipe_lease(&conn_a, &pipe, "holder-a", ttl),
        InstanceConnector::acquire_pipe_lease(&conn_b, &pipe, "holder-b", ttl)
    );
    assert!(ga.unwrap(), "schema A acquires");
    assert!(gb.unwrap(), "schema B acquires the same keys independently");
    assert!(
        !InstanceConnector::acquire_pipe_lease(&conn_a2, &pipe, "holder-c", ttl)
            .await
            .unwrap(),
        "a second holder on schema A is refused"
    );
    assert!(
        InstanceConnector::release_pipe_lease(&conn_a, &pipe, "holder-a")
            .await
            .unwrap()
    );
    assert!(
        InstanceConnector::release_pipe_lease(&conn_b, &pipe, "holder-b")
            .await
            .unwrap()
    );
    drop_schema(&ctx, &a).await;
    drop_schema(&ctx, &b).await;
}
flavor_tests!(engine_leases_by_schema, leases_by_schema);
