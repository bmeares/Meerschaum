//! Wave 9 (task U): bound parameters into typed PostgreSQL columns.
//!
//! The small-batch upsert of internal (`mrsm`) pipes binds every cell. Before the fix, datetimes,
//! dates, decimals and UUIDs were bound as `TEXT` and `NULL`s as typed `TEXT`, which PostgreSQL
//! rejects for non-text columns ("column ... is of type timestamp with time zone but expression
//! is of type text"). Every flavor runs the same round trip. Table prefix `w9u_`.

mod common;
use common::*;
use meerschaum_core::Pipe;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_sql::SqlConnector;

fn short() -> String {
    format!("w9u_{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

fn pipe_on(conn: &SqlConnector, ck: &str, mk: &str, parameters: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(&conn.keys())
        .parameters(params(parameters))
        .build()
        .unwrap()
}

fn row(id: i64, updated: KValue, payload: KValue, amount: KValue, uid: KValue) -> Doc {
    doc(vec![
        ("ts", udt(2024, 1, 1, 0, 0)),
        ("id", i(id)),
        ("updated", updated),
        ("payload", payload),
        ("amount", amount),
        ("uid", uid),
    ])
}

flavor_tests!(internal_upsert_binds_typed_cells, |c: Ctx| async move {
    // `mrsm` connector keys select the small-batch internal upsert (`upsert_small_internal`).
    let mk = short();
    let pipe = pipe_on(
        &c.conn,
        "mrsm",
        &mk,
        r#"{
                "columns": {"datetime": "ts", "id": "id"},
                "upsert": true,
                "dtypes": {
                    "updated": "datetime64[ns, UTC]",
                    "payload": "json",
                    "amount": "numeric",
                    "uid": "uuid"
                }
            }"#,
    );
    let uid_a = "8f1d3b2c-6a4e-4f1a-9a77-0b5e2c1d9e01";
    let uid_b = "1c2d3e4f-5a6b-4c7d-8e9f-a0b1c2d3e4f5";
    // First sync creates the table (bulk insert path).
    ok(c.sync(
        &pipe,
        vec![
            row(
                1,
                udt(2024, 1, 1, 1, 0),
                j(r#"{"a": 1}"#),
                s("1.25"),
                s(uid_a),
            ),
            row(
                2,
                udt(2024, 1, 1, 2, 0),
                j(r#"[1, 2]"#),
                s("2.50"),
                s(uid_b),
            ),
        ],
    )
    .await);
    // Second sync updates both rows through the bound `INSERT ... ON CONFLICT` and sets
    // every typed column of row 2 to NULL.
    ok(c.sync(
        &pipe,
        vec![
            row(
                1,
                udt(2024, 1, 2, 3, 30),
                j(r#"{"a": 2}"#),
                s("10.75"),
                s(uid_b),
            ),
            row(2, KValue::Null, KValue::Null, KValue::Null, KValue::Null),
        ],
    )
    .await);
    let df = c.get_data(&pipe, Default::default()).await.expect("data");
    assert_eq!(df.height(), 2);
    let ids: Vec<Option<i64>> = df.data("id").unwrap().values().iter().map(as_int).collect();
    let row1 = ids.iter().position(|v| *v == Some(1)).expect("row 1");
    let row2 = ids.iter().position(|v| *v == Some(2)).expect("row 2");
    let updated = df.data("updated").unwrap().values();
    assert_eq!(
        as_naive_utc(&updated[row1]),
        Some(naive(2024, 1, 2, 3, 30, 0)),
        "{:?}",
        updated[row1]
    );
    assert!(updated[row2].is_na(), "{:?}", updated[row2]);
    let amount = df.data("amount").unwrap().values();
    let amount1: f64 = amount[row1].py_str().parse().expect("decimal");
    assert!((amount1 - 10.75).abs() < 1e-9, "{:?}", amount[row1]);
    assert!(amount[row2].is_na(), "{:?}", amount[row2]);
    let uid = df.data("uid").unwrap().values();
    assert_eq!(uid[row1].py_str(), uid_b);
    assert!(uid[row2].is_na(), "{:?}", uid[row2]);
    let payload = df.data("payload").unwrap().values();
    assert!(payload[row1].py_str().contains('2'), "{:?}", payload[row1]);
    assert!(payload[row2].is_na(), "{:?}", payload[row2]);
    c.delete(&pipe).await;
});

flavor_tests!(
    temporary_tables_log_binds_null_timestamp,
    |c: Ctx| async move {
        // `ready_to_drop` is a nullable timestamp: a pending table binds `NULL` into it.
        let table = format!("_{}_tmp", short());
        let (success, msg) = c
            .conn
            .log_temporary_tables_creation(std::slice::from_ref(&table), false, true)
            .await;
        assert!(success, "{msg}");
        let (success, msg) = c
            .conn
            .log_temporary_tables_creation(std::slice::from_ref(&table), true, true)
            .await;
        assert!(success, "{msg}");
    }
);

flavor_tests!(empty_read_describes_recreated_table, |c: Ctx| async move {
    // An empty result is typed by describing the statement. `sqlx` caches the described
    // statement per connection; once the table is recreated with other columns, the stale
    // statement must not turn the next empty read into a frame without columns.
    if matches!(c.flavor, "mssql" | "oracle" | "duckdb") {
        return;
    }
    let table = short();
    let name = meerschaum_sql::ident::sql_item_name(&table, c.conn.flavor(), None);
    let sql = format!("SELECT * FROM {name} WHERE 1 = 0");
    assert!(
        c.conn
            .exec(&format!("CREATE TABLE {name} (a INTEGER)"))
            .await
            .is_some()
    );
    for _ in 0..3 {
        let df = c.conn.read(&sql).await.expect("read");
        assert_eq!(df.names(), vec!["a".to_string()]);
    }
    assert!(c.conn.exec(&format!("DROP TABLE {name}")).await.is_some());
    assert!(
        c.conn
            .exec(&format!("CREATE TABLE {name} (a INTEGER, b TEXT)"))
            .await
            .is_some()
    );
    for _ in 0..3 {
        let df = c.conn.read(&sql).await.expect("read");
        assert_eq!(df.names(), vec!["a".to_string(), "b".to_string()]);
    }
    let _ = c.conn.exec(&format!("DROP TABLE {name}")).await;
});

flavor_tests!(
    prepared_statements_survive_add_column,
    |c: Ctx| async move {
        // PostgreSQL rejects a prepared statement whose result columns changed since it was
        // prepared (SQLSTATE 0A000, "cached plan must not change result type"). Neither the
        // statements `describe` caches nor the prepared catalog reads may surface it.
        if matches!(c.flavor, "mssql" | "oracle" | "duckdb") {
            return;
        }
        let table = short();
        let name = meerschaum_sql::ident::sql_item_name(&table, c.conn.flavor(), None);
        let empty = format!("SELECT * FROM {name} WHERE 1 = 0");
        let all = format!("SELECT * FROM {name}");
        assert!(
            c.conn
                .exec(&format!("CREATE TABLE {name} (a INTEGER)"))
                .await
                .is_some()
        );
        assert!(
            c.conn
                .exec(&format!("INSERT INTO {name} (a) VALUES (1)"))
                .await
                .is_some()
        );
        for _ in 0..3 {
            assert_eq!(c.conn.read(&empty).await.expect("read").names(), vec!["a"]);
            let df = c.conn.read_catalog(&all).await.expect("prepared read");
            assert_eq!(df.names(), vec!["a"]);
        }
        assert!(
            c.conn
                .exec(&format!("ALTER TABLE {name} ADD b TEXT"))
                .await
                .is_some()
        );
        for _ in 0..3 {
            assert_eq!(
                c.conn.read(&empty).await.expect("read").names(),
                vec!["a", "b"]
            );
            let df = c.conn.read_catalog(&all).await.expect("prepared read");
            assert_eq!(df.names(), vec!["a", "b"]);
            assert_eq!(df.height(), 1);
        }
        let _ = c.conn.exec(&format!("DROP TABLE {name}")).await;
    }
);
