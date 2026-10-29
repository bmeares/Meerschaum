//! Wave 8 (task O): `{{ Pipe('a', 'b') }}` renders the REGISTERED target of the referenced pipe
//! (built from keys only at the call site), through `prepare_pipe`'s snapshot resolver, and the
//! datetime-bounded SQLite backtrack query keeps the planner on the datetime index.
//! Table prefix `w8o_`.

mod common;
use common::*;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;

fn short() -> String {
    format!("w8o_{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

fn pipe_on(conn: &SqlConnector, ck: &str, mk: &str, parameters: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(&conn.keys())
        .parameters(params(parameters))
        .build()
        .unwrap()
}

flavor_tests!(
    reference_renders_registered_explicit_target,
    |c: Ctx| async move {
        let ck = short();
        let inst = c.conn.keys();
        let target = format!("{ck}_custom_target");
        let parent = pipe_on(
            &c.conn,
            &ck,
            "par",
            &serde_json::json!({
                "columns": {"datetime": "dt", "id": "id"},
                "target": target,
            })
            .to_string(),
        );
        assert!(c.conn.register_pipe(&parent).await.success);
        let child = pipe_on(
            &c.conn,
            &inst,
            &format!("{ck}child"),
            &serde_json::json!({
                "sql": format!("SELECT * FROM {{{{ Pipe('{ck}', 'par', instance='{inst}') }}}}"),
            })
            .to_string(),
        );
        let prepared = c.conn.prepare_pipe(&child).await;
        let sql = prepared.parameters()["sql"].py_str();
        assert!(
            sql.contains(&target),
            "the explicit target is missing from `{sql}`"
        );
        assert!(
            !sql.contains(&format!("{ck}_par")),
            "the default table name leaked into `{sql}`"
        );
        // The stored parameters of the referencing pipe keep the template.
        assert!(
            prepared.raw_parameters()["sql"]
                .py_str()
                .contains("{{ Pipe(")
        );
        c.delete(&parent).await;
    }
);

// SQLite has no statistics on a fresh table, so an `IN` list over (nearly) every id outranked the
// datetime range and each narrow backtrack query walked a whole id's rows. The unary `+` keeps
// the planner on the datetime index; the rows returned do not change.
flavor_tests!(
    sqlite_backtrack_query_stays_on_datetime,
    |c: Ctx| async move {
        use meerschaum_connector::{DataQuery, DateBound};
        if c.conn.flavor() != meerschaum_connector_sql::Flavor::Sqlite {
            return;
        }
        let ck = short();
        let pipe = pipe_on(
            &c.conn,
            &ck,
            "bt",
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        assert!(c.conn.register_pipe(&pipe).await.success);
        ok(c.sync(
            &pipe,
            (0u32..6)
                .map(|n| {
                    doc(vec![
                        ("dt", ndt(2024, 1, 1 + n, 0, 0)),
                        ("id", i(i64::from(n % 2))),
                        ("val", i(i64::from(n))),
                    ])
                })
                .collect(),
        )
        .await);
        let mut params = indexmap::IndexMap::new();
        params.insert(
            "id".to_string(),
            meerschaum_core::Value::List(vec![
                meerschaum_core::Value::Int(0),
                meerschaum_core::Value::Int(1),
            ]),
        );
        let begin = DateBound::from_value(&ndt(2024, 1, 2, 0, 0));
        let query = DataQuery {
            begin,
            params: Some(params.clone()),
            ..Default::default()
        };
        let sql = c
            .conn
            .get_pipe_data_query(&pipe, &query, &Default::default())
            .await
            .unwrap();
        assert!(sql.contains("(\"id\" IN ('0', '1')) = 1"), "{sql}");
        // Without a datetime bound the plain index-friendly form stays.
        let unbounded = DataQuery {
            params: Some(params),
            ..Default::default()
        };
        let sql = c
            .conn
            .get_pipe_data_query(&pipe, &unbounded, &Default::default())
            .await
            .unwrap();
        assert!(sql.contains("(\"id\" IN ('0', '1'))"), "{sql}");
        let df = c.conn.get_pipe_data(&pipe, &query).await.unwrap().unwrap();
        assert_eq!(df.height(), 5);
        c.delete(&pipe).await;
    }
);
