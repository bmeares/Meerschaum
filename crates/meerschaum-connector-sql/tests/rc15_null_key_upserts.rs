//! Upserts with NULL keys (rc15 S3): with the default `null_indices: true`, a row whose key
//! columns are NULL upserts in place on every flavor (insert, update, never a duplicate).
//! PostgreSQL 15+ declares v4's unique constraint `NULLS NOT DISTINCT`; where the unique key
//! treats NULLs as distinct (SQLite, older PostgreSQL, tables made before rc15) the upsert
//! updates then inserts the rows with a NULL key itself; MySQL / MariaDB compare the keys with
//! `<=>`; MSSQL and Oracle `MERGE` on the `COALESCE` equality.
mod common;
use common::*;
use meerschaum_connector::DataQuery;
use meerschaum_kernel::value::Value as KValue;

/// Three syncs of the same keys (one NULL key): two rows with the last values.
async fn null_key_upserts(c: &Ctx, location: &str, columns: &str, with_ts: bool, with_b: bool) {
    let pipe = c.pipe(
        "s3",
        "upsert",
        Some(location),
        &format!(r#"{{"columns": {columns}, "upsert": true}}"#),
    );
    let docs = |v1: i64, v2: i64| -> Vec<Doc> {
        let mut a = vec![("id", i(1)), ("v", i(v1))];
        let mut b = vec![("id", KValue::Null), ("v", i(v2))];
        if with_ts {
            a.push(("ts", ndt(2024, 1, 1, 0, 0)));
            b.push(("ts", ndt(2024, 1, 1, 0, 0)));
        }
        if with_b {
            a.push(("b", s("x")));
            b.push(("b", KValue::Null));
        }
        vec![doc(a), doc(b)]
    };
    ok(c.sync(&pipe, docs(1, 2)).await);
    ok(c.sync(&pipe, docs(10, 20)).await);
    ok(c.sync(&pipe, docs(10, 20)).await);
    let rows = c.docs(&pipe).await;
    let mut values: Vec<i64> = rows.iter().filter_map(|d| as_int(&d["v"])).collect();
    values.sort();
    assert_eq!(
        values,
        vec![10, 20],
        "{} {location}: NULL-key upserts duplicated rows: {rows:?}",
        c.flavor
    );
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
    c.delete(&pipe).await;
}

flavor_tests!(null_key_upserts_with_datetime, |c: Ctx| async move {
    null_key_upserts(
        &c,
        "dt_id",
        r#"{"datetime": "ts", "id": "id"}"#,
        true,
        false,
    )
    .await;
});

flavor_tests!(null_key_upserts_without_datetime, |c: Ctx| async move {
    null_key_upserts(&c, "id_only", r#"{"id": "id"}"#, false, false).await;
});

flavor_tests!(null_key_upserts_with_two_ids, |c: Ctx| async move {
    null_key_upserts(
        &c,
        "two_ids",
        r#"{"datetime": "ts", "id": "id", "other": "b"}"#,
        true,
        true,
    )
    .await;
});

// A batch that repeats a key (new and existing keys, a NULL key among them) keeps the last row
// of each key, as v4's `INSERT ... ON DUPLICATE KEY UPDATE` resolves it (MySQL / MariaDB, whose
// NULL-safe upsert is an update then an insert, and SQLite's `ON CONFLICT DO UPDATE`).
// PostgreSQL, MSSQL and Oracle refuse a batch that touches a row twice, as in v4.
flavor_tests!(
    repeated_keys_in_one_upsert_keep_the_last_row,
    |c: Ctx| async move {
        if !(c.is_mysql() || c.is_sqlite()) {
            return;
        }
        let pipe = c.pipe(
            "s3",
            "repeats",
            None,
            r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true}"#,
        );
        let row = |id: Option<i64>, v: i64| {
            doc(vec![
                ("ts", ndt(2024, 1, 1, 0, 0)),
                ("id", id.map_or(KValue::Null, i)),
                ("v", i(v)),
            ])
        };
        ok(c.sync(&pipe, vec![row(Some(1), 1), row(Some(2), 2)]).await);
        let res = c
            .sync(
                &pipe,
                vec![
                    row(Some(1), 3),
                    row(Some(2), 4),
                    row(Some(2), 5),
                    row(Some(1), 6),
                    row(Some(7), 70),
                    row(Some(7), 71),
                    row(None, 8),
                    row(None, 9),
                ],
            )
            .await;
        assert!(res.success, "{}: {}", c.flavor, res.message);
        let mut pairs: Vec<(Option<i64>, i64)> = c
            .docs(&pipe)
            .await
            .iter()
            .map(|d| (as_int(&d["id"]), as_int(&d["v"]).unwrap_or(-1)))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![(None, 9), (Some(1), 6), (Some(2), 5), (Some(7), 71)],
            "{}",
            c.flavor
        );
        c.delete(&pipe).await;
    }
);

// SQLite: the unique key stays v4's own, so v4's upsert statement (`ON CONFLICT (<columns>)`,
// what a 4.1 process sharing the database or a rollback runs) still matches it.
flavor_tests!(v4_upserts_match_sqlite_unique_keys, |c: Ctx| async move {
    if !c.is_sqlite() {
        return;
    }
    let pipe = c.pipe(
        "s3",
        "v4sqlite",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true}"#,
    );
    let row = |id: Option<i64>, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", id.map_or(KValue::Null, i)),
            ("v", i(v)),
        ])
    };
    ok(c.sync(&pipe, vec![row(Some(1), 1), row(None, 2)]).await);
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let table = c.conn.pipe_table_name(&sp);
    let sql = format!(
        "INSERT INTO {table} (\"ts\", \"id\", \"v\") SELECT '2024-01-01 00:00:00.000000', 1, 10 \
         WHERE true ON CONFLICT (\"id\", \"ts\") DO UPDATE SET \"v\" = EXCLUDED.\"v\""
    );
    assert!(c.conn.exec(&sql).await.is_some(), "{}: {sql}", c.flavor);
    let mut values: Vec<i64> = c
        .docs(&pipe)
        .await
        .iter()
        .filter_map(|d| as_int(&d["v"]))
        .collect();
    values.sort();
    assert_eq!(values, vec![2, 10], "{}", c.flavor);
    c.delete(&pipe).await;
});

// PostgreSQL 15+: the NULL-safe unique key is v4's own `UQ_` constraint with
// `NULLS NOT DISTINCT`, so v4's upsert statement (`ON CONFLICT (<columns>)`, what a 4.1 process
// sharing the database or a rollback runs) still matches it, NULL keys included.
flavor_tests!(v4_upserts_match_the_null_safe_key, |c: Ctx| async move {
    if !c.is_pg() {
        return;
    }
    let pipe = c.pipe(
        "s3",
        "v4compat",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true}"#,
    );
    let row = |id: Option<i64>, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", id.map_or(KValue::Null, i)),
            ("v", i(v)),
        ])
    };
    ok(c.sync(&pipe, vec![row(Some(1), 1), row(None, 2)]).await);
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let table = c.conn.pipe_table_name(&sp);
    let version = c.conn.db_version().await.unwrap_or_default();
    let major: u32 = version
        .trim_start_matches("PostgreSQL ")
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0);
    if major < 15 {
        eprintln!("Skipping the NULLS NOT DISTINCT check on {version}.");
        c.delete(&pipe).await;
        return;
    }
    let nulls_not_distinct = c
        .conn
        .value(&format!(
            "SELECT COUNT(*) FROM pg_catalog.pg_index WHERE indrelid = '{}'::regclass \
             AND indisunique AND indnullsnotdistinct",
            table.replace('\'', "''")
        ))
        .await
        .and_then(|v| v.as_i64());
    assert_eq!(nulls_not_distinct, Some(1), "{}", c.flavor);
    // v4.1.4's statement shape, plain conflict columns.
    for (id, v) in [("1", 10), ("NULL", 20)] {
        let sql = format!(
            "INSERT INTO {table} (\"ts\", \"id\", \"v\") VALUES ('2024-01-01 00:00:00', {id}, {v}) \
             ON CONFLICT (\"id\", \"ts\") DO UPDATE SET \"v\" = EXCLUDED.\"v\""
        );
        assert!(c.conn.exec(&sql).await.is_some(), "{}: {sql}", c.flavor);
    }
    let mut values: Vec<i64> = c
        .docs(&pipe)
        .await
        .iter()
        .filter_map(|d| as_int(&d["v"]))
        .collect();
    values.sort();
    assert_eq!(values, vec![10, 20], "{}", c.flavor);
    c.delete(&pipe).await;
});

// A table made by v4 / rc14 with the plain unique constraint keeps upserting: the conflict
// target follows the table's own unique key, and rows with a NULL key upsert in place there too
// (no index rebuild needed).
flavor_tests!(plain_unique_keys_keep_upserting, |c: Ctx| async move {
    if !(c.is_pg() || c.is_sqlite()) {
        return;
    }
    let pipe = c.pipe(
        "s3",
        "legacy",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true, "null_indices": false}"#,
    );
    let row = |id: Option<i64>, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, 1, 0, 0)),
            ("id", id.map_or(KValue::Null, i)),
            ("v", i(v)),
        ])
    };
    ok(c.sync(&pipe, vec![row(Some(1), 1)]).await);
    // The table now has v4's plain unique key; turn `null_indices` back on (the default).
    let pipe = c.pipe(
        "s3",
        "legacy",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}, "upsert": true}"#,
    );
    ok(c.sync(&pipe, vec![row(Some(1), 2), row(None, 20)]).await);
    ok(c.sync(&pipe, vec![row(Some(1), 3), row(None, 30)]).await);
    ok(c.sync(&pipe, vec![row(None, 31)]).await);
    let mut pairs: Vec<(Option<i64>, Option<i64>)> = c
        .docs(&pipe)
        .await
        .iter()
        .map(|d| (as_int(&d["id"]), as_int(&d["v"])))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![(None, Some(31)), (Some(1), Some(3))],
        "{}",
        c.flavor
    );
    c.delete(&pipe).await;
});
