//! Protected instances (`protected: true`, rc15 Q20): every mutation through the connector is
//! refused unless writes are allowed, reads are never blocked, and Meerschaum's internal `mrsm`
//! pipes keep working. One sequential test: the allowance is process-wide.

mod common;

use common::*;
use meerschaum_connector::schema::SchemaPlan;
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{SqlConnector, SqlError};
use meerschaum_core::{Map, Pipe, Value};

/// A protected connector to the same database as `c`.
async fn protected_twin(c: &Ctx) -> SqlConnector {
    let mut spec: ConnectionSpec = c.conn.spec().clone();
    spec.protected = true;
    let settings = InstanceSettings::defaults(spec.flavor);
    SqlConnector::connect_with(c.conn.label(), spec, settings)
        .await
        .expect("connects")
}

#[track_caller]
fn refused(t: meerschaum_core::SuccessTuple, op: &str) {
    assert!(!t.success, "{op} should be refused: {}", t.message);
    assert!(
        t.message.starts_with(&format!("Refusing to {op} ")),
        "{op}: {}",
        t.message
    );
    assert!(
        t.message.contains("on protected instance 'sql:"),
        "{}",
        t.message
    );
    assert!(
        t.message
            .ends_with("Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1)."),
        "{}",
        t.message
    );
}

async fn check(c: Ctx) {
    assert!(
        std::env::var("MRSM_ALLOW_PROTECTED").is_err(),
        "unset MRSM_ALLOW_PROTECTED to run this test"
    );
    let pipe = c.pipe("prot", "m", None, r#"{"columns": {"datetime": "ts"}}"#);
    // Data written while unprotected.
    ok(c.sync(
        &pipe,
        vec![doc(vec![("ts", ndt(2024, 1, 1, 0, 0)), ("v", i(1))])],
    )
    .await);
    let p = protected_twin(&c).await;
    assert!(meerschaum_connector::Connector::is_protected(&p));

    // Reads are never blocked.
    assert_eq!(
        p.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );
    assert!(
        p.get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .is_some()
    );
    assert!(p.fetch_pipes_keys(&Default::default()).await.is_ok());
    let plan = p.get_pipe_schema_plan(&pipe).await.unwrap();
    assert!(plan.exists);

    // Every mutation is refused.
    let docs = vec![doc(vec![("ts", ndt(2024, 1, 2, 0, 0)), ("v", i(2))])];
    refused(
        p.sync(
            &pipe,
            meerschaum_kernel::enforce::PipeInput::Docs(docs.clone()),
            &Default::default(),
        )
        .await,
        "sync",
    );
    refused(
        InstanceConnector::sync_pipe(
            &p,
            &pipe,
            meerschaum_kernel::Frame::empty(),
            &Default::default(),
        )
        .await,
        "sync",
    );
    refused(p.clear_pipe(&pipe, &DataQuery::default()).await, "clear");
    refused(p.drop_pipe(&pipe).await, "drop");
    let other = c.pipe("prot_new", "m", None, "{}");
    refused(p.register_pipe(&other).await, "register");
    refused(p.edit_pipe(&pipe, false).await, "edit");
    refused(p.delete_pipe(&pipe).await, "delete");
    refused(
        p.deduplicate_pipe(&pipe, &DataQuery::default()).await,
        "deduplicate",
    );
    refused(p.drop_pipe_indices(&pipe, None).await, "drop indices for");
    refused(
        p.create_pipe_indices(&pipe, None).await,
        "create indices for",
    );
    let mut patch = Map::new();
    patch.insert("dtypes".into(), Value::Map(params(r#"{"extra": "int"}"#)));
    let mut declared = pipe.clone();
    declared.update_parameters(patch);
    let plan: SchemaPlan = p.get_pipe_schema_plan(&declared).await.unwrap();
    assert!(!plan.is_empty());
    refused(
        p.apply_pipe_schema(&declared, &plan, false).await,
        "change the table of",
    );
    assert!(p.check_writable("execute SQL").is_err());

    // A caller's own SQL (`mrsm sql`, `host::read_sql`): one read statement runs, read-only;
    // anything else is refused before it reaches the database.
    let table = p.pipe_table_name(&p.sql_pipe(&pipe).unwrap());
    let frame = p.read_raw(&format!("SELECT * FROM {table}")).await.unwrap();
    assert_eq!(frame.height(), 1, "{}", c.flavor);
    for statement in [
        format!("DELETE FROM {table}"),
        format!("DROP TABLE {table}"),
        format!("SELECT 1 FROM {table}; DELETE FROM {table}"),
        format!("WITH d AS (SELECT 1 AS x) DELETE FROM {table}"),
    ] {
        match p.read_raw(&statement).await {
            Err(SqlError::Protected(message)) => assert!(
                message.starts_with("Refusing to execute SQL on protected instance 'sql:"),
                "{message}"
            ),
            other => panic!(
                "{}: {statement} ran: {:?}",
                c.flavor,
                other.map(|f| f.height())
            ),
        }
    }
    if c.is_pg() {
        // The transaction is read-only: a write the word scan cannot see fails too.
        let seq = format!("{}_seq", c.prefix);
        assert!(
            c.conn
                .exec(&format!("CREATE SEQUENCE {seq}"))
                .await
                .is_some()
        );
        assert!(
            p.read_raw(&format!("SELECT nextval('{seq}')"))
                .await
                .is_err()
        );
        let last = c
            .conn
            .value(&format!("SELECT last_value FROM {seq}"))
            .await
            .and_then(|v| v.as_i64());
        assert_eq!(last, Some(1));
        let called = c
            .conn
            .value(&format!("SELECT is_called FROM {seq}"))
            .await
            .map(|v| v.to_string());
        assert_eq!(called.as_deref(), Some("False"), "nextval() ran");
        let _ = c.conn.exec(&format!("DROP SEQUENCE {seq}")).await;
    }

    // Nothing changed.
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(1));

    // Internal bookkeeping pipes are not user data: they keep working.
    let internal = Pipe::builder("mrsm", "pipeline")
        .location(Some(&format!("{}_protected", c.prefix)))
        .instance(&p.keys())
        .build()
        .unwrap();
    ok(p.register_pipe(&internal).await);
    ok(p.delete_pipe(&internal).await);

    // `-y` is not permission; `--allow-protected` (a scope) is.
    {
        let _allowed = meerschaum_core::protected::allow(true);
        ok(p.sync(
            &pipe,
            meerschaum_kernel::enforce::PipeInput::Docs(docs),
            &Default::default(),
        )
        .await);
        assert!(p.check_writable("execute SQL").is_ok());
    }
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
    refused(p.clear_pipe(&pipe, &DataQuery::default()).await, "clear");
    c.delete(&pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_instances_refuse_every_mutation() {
    // Sequential on purpose (see the module docs).
    let mut flavors = vec![
        "sqlite",
        "postgresql",
        "timescaledb",
        "postgis",
        "mysql",
        "mariadb",
        "mssql",
        "duckdb",
        "geopackage",
    ];
    if cfg!(feature = "oracle") {
        flavors.push("oracle");
    }
    for flavor in flavors {
        if let Some(c) = ctx(flavor, "f2_protected").await {
            check(c).await;
        }
    }
}

#[test]
fn protected_parses_from_uris_and_attributes() {
    let spec = ConnectionSpec::from_uri("sqlite:////tmp/f2.db?protected=true", None, None).unwrap();
    assert!(spec.protected);
    assert!(!spec.params.contains_key("protected"));
}
