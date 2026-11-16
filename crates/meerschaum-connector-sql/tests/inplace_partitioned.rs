//! In-place syncs into partitioned targets on the PostgreSQL family: TimescaleDB hypertables
//! and native range partitions (PostgreSQL, PostGIS), whose primary key folds in the datetime
//! column. Every combination of a target created by the in-place sync itself or by a frame
//! sync, `upsert` on and off, a `primary` or an `id` key, and `hypertable` on and off. The
//! in-place sync creates the target as the frame path does (a hypertable, a partitioned table
//! with its partitions), creates the partitions later rows need, and upserts on the table's
//! own unique key. Table prefix `w5sql_`.
mod common;

use common::*;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_connector_sql::queries::metadef::Bound;

fn pg_family(c: &Ctx) -> bool {
    matches!(c.flavor, "postgresql" | "timescaledb" | "postgis")
}

async fn exec(c: &Ctx, sql: &str) {
    assert!(c.conn.exec(sql).await.is_some(), "{}: {sql}", c.flavor);
}

async fn text(c: &Ctx, sql: &str) -> Option<String> {
    match c.conn.value(sql).await {
        Some(SqlValue::Text(t)) => Some(t),
        Some(SqlValue::Int(i)) => Some(i.to_string()),
        Some(other) => Some(format!("{other:?}")),
        None => None,
    }
}

/// `(relkind, hypertable, primary key)` of `table`.
async fn shape(c: &Ctx, table: &str) -> (String, bool, String) {
    let literal = table.replace('\'', "''");
    let kind = text(
        c,
        &format!("SELECT relkind::text FROM pg_class WHERE oid = '{literal}'::regclass"),
    )
    .await
    .unwrap_or_default();
    let hypertable = c.flavor == "timescaledb"
        && text(
            c,
            &format!(
                "SELECT hypertable_name::text FROM timescaledb_information.hypertables \
                 WHERE format('%I.%I', hypertable_schema, hypertable_name)::regclass = '{literal}'::regclass"
            ),
        )
        .await
        .is_some();
    let pk = text(
        c,
        &format!(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid = '{literal}'::regclass AND contype = 'p'"
        ),
    )
    .await
    .unwrap_or_default();
    (kind, hypertable, pk)
}

/// `id=v` for every row of `table`, in `id` order.
async fn rows(c: &Ctx, table: &str) -> String {
    text(
        c,
        &format!("SELECT string_agg(id::text || '=' || v::text, ',' ORDER BY id, ts) FROM {table}"),
    )
    .await
    .unwrap_or_default()
}

async fn inplace(c: &Ctx, pipe: &meerschaum_core::Pipe) -> String {
    let result = c
        .conn
        .sync_pipe_inplace(pipe, None, Bound::None, Bound::None, true)
        .await;
    assert!(result.success, "{}: {}", c.flavor, result.message);
    result.message
}

/// One combination: sync the source in place twice (the second time with a changed value and
/// rows outside the first sync's partitions) and check the target's shape and rows.
async fn scenario(c: &Ctx, key: &str, upsert: bool, hypertable: bool, frame_first: bool) {
    let source = format!("{}_src", c.prefix);
    exec(c, &format!("DROP TABLE IF EXISTS {source}")).await;
    exec(
        c,
        &format!("CREATE TABLE {source} (ts timestamptz NOT NULL, id bigint NOT NULL, v integer)"),
    )
    .await;
    exec(
        c,
        &format!(
            "INSERT INTO {source} SELECT '2024-01-01'::timestamptz + (i || ' days')::interval, i, i \
             FROM generate_series(1, 4) AS i"
        ),
    )
    .await;
    let params = serde_json::json!({
        "columns": {"datetime": "ts", key: "id"},
        "upsert": upsert,
        "hypertable": hypertable,
        "verify": {"chunk_minutes": 1440},
        "sql": format!("SELECT ts, id, v FROM {source}"),
    })
    .to_string();
    let name = format!(
        "part_{key}_{}_{}_{}",
        upsert as u8, hypertable as u8, frame_first as u8
    );
    let pipe = c.pipe(&c.conn.keys(), &name, None, &params);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    let label = format!("{}: {name}", c.flavor);
    if frame_first {
        // The frame path creates the target (its partitions span 2024-01-02 and 2024-01-03).
        let docs = (1..=2)
            .map(|id| {
                doc(vec![
                    ("ts", udt(2024, 1, 1 + id as u32, 0, 0)),
                    ("id", i(id)),
                    ("v", i(0)),
                ])
            })
            .collect();
        ok(c.sync(&pipe, docs).await);
    }
    let frame_shape = if frame_first {
        Some(shape(c, &table).await)
    } else {
        None
    };
    inplace(c, &pipe).await;
    let created = shape(c, &table).await;
    if let Some(expected) = &frame_shape {
        assert_eq!(
            &created, expected,
            "{label}: the in-place sync kept the table"
        );
    }
    // The shape the frame path gives the same pipe.
    let partitioned = hypertable && c.flavor != "timescaledb";
    assert_eq!(
        created.0,
        if partitioned { "p" } else { "r" },
        "{label}: {created:?}"
    );
    assert_eq!(
        created.1,
        hypertable && c.flavor == "timescaledb",
        "{label}: {created:?}"
    );
    if key == "primary" {
        let folded = partitioned || c.flavor == "timescaledb";
        assert_eq!(
            created.2,
            if folded {
                "PRIMARY KEY (ts, id)"
            } else {
                "PRIMARY KEY (id)"
            },
            "{label}"
        );
    }
    assert_eq!(rows(c, &table).await, "1=1,2=2,3=3,4=4", "{label}");

    // A changed value and rows beyond the partitions created so far.
    exec(c, &format!("UPDATE {source} SET v = 30 WHERE id = 3")).await;
    exec(
        c,
        &format!(
            "INSERT INTO {source} SELECT '2024-03-01'::timestamptz + (i || ' days')::interval, i, i \
             FROM generate_series(5, 6) AS i"
        ),
    )
    .await;
    let message = inplace(c, &pipe).await;
    if upsert {
        assert!(message.starts_with("Upserted "), "{label}: {message}");
    } else {
        assert_eq!(message, "Inserted 2, updated 1 rows.", "{label}");
    }
    assert_eq!(rows(c, &table).await, "1=1,2=2,3=30,4=4,5=5,6=6", "{label}");
    assert_eq!(shape(c, &table).await, created, "{label}");
    c.delete(&pipe).await;
    exec(c, &format!("DROP TABLE {source}")).await;
}

/// Every combination for `key`, each on a connection of its own; the failures are reported
/// together.
async fn all_scenarios(c: Ctx, key: &'static str) {
    if !pg_family(&c) {
        return;
    }
    let mut failures = Vec::new();
    for upsert in [true, false] {
        for hypertable in [true, false] {
            for frame_first in [false, true] {
                let name = format!(
                    "{key}{}{}{}",
                    upsert as u8, hypertable as u8, frame_first as u8
                );
                let ctx = common::ctx(c.flavor, &name).await.expect("connect");
                let run = tokio::spawn(async move {
                    scenario(&ctx, key, upsert, hypertable, frame_first).await;
                });
                if let Err(e) = run.await {
                    let reason = e
                        .into_panic()
                        .downcast::<String>()
                        .map(|s| *s)
                        .unwrap_or_else(|_| "panic".into());
                    failures.push(format!(
                        "upsert={upsert} hypertable={hypertable} frame_first={frame_first}: {}",
                        reason.lines().take(3).collect::<Vec<_>>().join(" | ")
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{}:\n{}",
        c.flavor,
        failures.join("\n")
    );
}

flavor_tests!(
    inplace_syncs_into_partitioned_targets_by_primary,
    |c: Ctx| async move {
        all_scenarios(c, "primary").await;
    }
);

flavor_tests!(
    inplace_syncs_into_partitioned_targets_by_id,
    |c: Ctx| async move {
        all_scenarios(c, "id").await;
    }
);
