//! The access model on SQL instances (rc15 wave 2): locks refuse every write through the SQL
//! connector (AC5), the team store over `mrsm_teams` (AC3) and exact user lookups (FX1). The
//! checks are `meerschaum_connector::testing`'s, shared with every instance crate.

mod common;

use common::*;
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_core::access::{Lock, LockOp, Locks};
use meerschaum_core::{Map, Value};

fn tag(c: &Ctx) -> String {
    c.prefix.replace('-', "_")
}

flavor_tests!(locks_refuse_every_write, |c: Ctx| async move {
    meerschaum_connector::testing::check_locks(&c.conn, &tag(&c)).await;
});

flavor_tests!(teams_store, |c: Ctx| async move {
    meerschaum_connector::testing::check_teams(&c.conn, &tag(&c)).await;
});

flavor_tests!(exact_user_lookups, |c: Ctx| async move {
    meerschaum_connector::testing::check_user_lookups(&c.conn, &tag(&c)).await;
});

// The SQL-only writes: schema changes, repartitioning, dropping indices, in-place syncs.
flavor_tests!(sql_schema_writes_are_drop_class, |c: Ctx| async move {
    let pipe = c.pipe(
        "plugin:locks",
        "schema",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", i(1)),
            ("v", i(1)),
        ])],
    )
    .await);
    ok(c.conn.lock_pipe(&pipe, &Locks::of(&[Lock::Drop])).await);
    let mut patch = Map::new();
    patch.insert("dtypes".into(), Value::Map(params(r#"{"extra": "int"}"#)));
    let mut declared = pipe.clone();
    declared.update_parameters(patch);
    let plan = c.conn.get_pipe_schema_plan(&declared).await.unwrap();
    let res = c.conn.apply_pipe_schema(&declared, &plan, false).await;
    assert!(
        !res.success && res.message.contains("it is locked (drop)"),
        "{}",
        res.message
    );
    let res = c.conn.drop_pipe_indices(&pipe, None).await;
    assert!(
        !res.success && res.message.contains("it is locked (drop)"),
        "{}",
        res.message
    );
    let res = c.conn.partition_pipe(&pipe, Some(60)).await;
    assert!(
        !res.success && res.message.contains("it is locked"),
        "{}",
        res.message
    );
    // A sync and a clear stay allowed under `drop` alone.
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("ts", ndt(2024, 1, 2, 0, 0)),
            ("id", i(1)),
            ("v", i(2)),
        ])],
    )
    .await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
    // A `sync` lock refuses an in-place sync's write too.
    ok(c.conn.lock_pipe(&pipe, &Locks::of(&[Lock::Sync])).await);
    let refusal = meerschaum_connector::lock_refusal(&c.conn, &pipe, LockOp::Sync)
        .await
        .expect("refused");
    assert!(
        refusal.message.contains("it is locked (sync, drop)")
            || refusal.message.contains("it is locked (sync)"),
        "{}",
        refusal.message
    );
    ok(c.conn.unlock_pipe(&pipe, &Locks::all()).await);
    c.delete(&pipe).await;
});
