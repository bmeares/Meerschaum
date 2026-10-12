//! Port of `tests/test_sync.py` (data shapes only) against the SQL instance connector, one test
//! per flavor (SQLite always; the container flavors when their `MRSM_TEST_*_URI` is set).
//!
//! Not ported (one line each):
//! - `test_sync_engine`: drives the `actions['sync']` engine.
//! - `test_sync_inplace`, `test_sync_inplace_upsert`, `test_sync_inplace_no_datetime`: need
//!   `sync_pipe_inplace` (not ported yet).
//! - `test_sync_dask_dataframe`, `test_sync_polars_dataframe`, `test_sync_polars_upsert`: dask /
//!   polars inputs.
//! - `test_no_null_indices` (in-place half), `test_sync_sql_small_chunksize`,
//!   `test_mixed_datetime_dtypes_sql`: `sql:` source fetch / in-place syncs through another
//!   connector; the instance-side half of `test_no_null_indices` is kept below.
//! - `test_sync_pipes_listen_stays_alive_until_stop`: threads / `--listen`.

mod common;

use common::*;
use meerschaum_connector::{DataQuery, DateBound, SyncOptions};
use meerschaum_core::Pipe;
use meerschaum_kernel::Decimal;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_sql::pipe::ChunkIntervalArg;

/// `pipe.get_data(**query).to_dict(orient='records')`.
async fn docs_q(c: &Ctx, pipe: &Pipe, query: DataQuery) -> Vec<Doc> {
    c.get_data(pipe, query)
        .await
        .map(|df| meerschaum_connector::pipe_ops::frame_to_docs(&df))
        .unwrap_or_default()
}

fn select(cols: &[&str]) -> DataQuery {
    DataQuery {
        select_columns: Some(cols.iter().map(|c| c.to_string()).collect()),
        ..Default::default()
    }
}

fn select_params(cols: &[&str], params: &str) -> DataQuery {
    DataQuery {
        select_columns: Some(cols.iter().map(|c| c.to_string()).collect()),
        params: Some(common::params(params)),
        ..Default::default()
    }
}

fn col(docs: &[Doc], name: &str) -> Vec<KValue> {
    docs.iter()
        .map(|d| d.get(name).cloned().unwrap_or(KValue::Null))
        .collect()
}

fn ints(docs: &[Doc], name: &str) -> Vec<Option<i64>> {
    col(docs, name).iter().map(as_int).collect()
}

fn strs(docs: &[Doc], name: &str) -> Vec<String> {
    col(docs, name).iter().map(|v| v.py_str()).collect()
}

/// `pipe.sync(docs)` mutating the in-memory pipe like the Python `Pipe` (new special dtypes,
/// `autoincrement`).
async fn sync_live(c: &Ctx, pipe: &mut Pipe, docs: Vec<Doc>) -> meerschaum_core::SuccessTuple {
    c.conn
        .sync_mut(
            pipe,
            meerschaum_kernel::enforce::PipeInput::Docs(docs),
            &SyncOptions::default(),
        )
        .await
}

flavor_tests!(test_register_and_delete, |c: Ctx| async move {
    let pipe = c.pipe("test", "register", Some("delete"), "{}");
    c.delete(&pipe).await;
    let res = c.conn.register_pipe(&pipe).await;
    assert!(res.success, "{}", res.message);
    assert!(c.conn.get_pipe_id(&pipe).await.unwrap().is_some());
    // `pipe.delete()`.
    c.conn.drop_pipe(&pipe).await;
    let res = c.conn.delete_pipe(&pipe).await;
    assert!(res.success, "{}", res.message);
    assert!(c.conn.get_pipe_id(&pipe).await.unwrap().is_none());
    let res = c.conn.register_pipe(&pipe).await;
    assert!(res.success, "{}", res.message);
    // Registering twice fails.
    assert!(!c.conn.register_pipe(&pipe).await.success);
    c.delete(&pipe).await;
});

flavor_tests!(test_drop_and_sync, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "drop_sync",
        None,
        r#"{"columns": ["datetime", "id"]}"#,
    );
    assert!(!c.conn.pipe_exists(&pipe).await.unwrap());
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("datetime", ndt(2021, 1, 1, 12, 0)),
            ("id", i(1)),
            ("val", i(1)),
        ])],
    )
    .await);
    assert!(c.conn.pipe_exists(&pipe).await.unwrap());
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("datetime", ndt(2021, 1, 1, 12, 1)),
            ("id", i(1)),
            ("val", i(1)),
        ])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 2);
    c.delete(&pipe).await;
});

flavor_tests!(test_drop_and_sync_duplicate, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "drop_sync",
        Some("duplicate"),
        r#"{"columns": ["datetime", "id"]}"#,
    );
    let row = || {
        vec![doc(vec![
            ("datetime", ndt(2021, 1, 1, 12, 0)),
            ("id", i(1)),
            ("val", i(1)),
        ])]
    };
    ok(sync_live(&c, &mut pipe, row()).await);
    assert_eq!(c.docs(&pipe).await.len(), 1);
    let res = ok(sync_live(&c, &mut pipe, row()).await);
    assert_eq!(res.message, "Inserted 0, updated 0 rows.");
    assert_eq!(c.docs(&pipe).await.len(), 1);
    c.delete(&pipe).await;
});

async fn read_count(c: &Ctx, target: &str) -> meerschaum_sql::Result<usize> {
    let schema = if c.conn.flavor().no_schema() {
        None
    } else {
        c.conn.schema()
    };
    let t = meerschaum_sql::ident::sql_item_name(target, c.conn.flavor(), schema);
    c.conn
        .read(&format!("SELECT * FROM {t}"))
        .await
        .map(|f| f.height())
}

flavor_tests!(test_target_mutable, |c: Ctx| async move {
    let target = format!("{}FooBar!", c.prefix);
    let mut pipe = c.pipe(
        "target",
        "mutable",
        None,
        &format!(r#"{{"target": "{target}", "columns": {{"datetime": "dt", "id": "id"}}}}"#),
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", ndt(2022, 6, 8, 0, 0)),
            ("id", i(1)),
            ("vl", i(10)),
        ])],
    )
    .await);
    assert_eq!(read_count(&c, &target).await.unwrap(), 1);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", ndt(2022, 6, 8, 0, 0)),
            ("id", i(1)),
            ("vl", i(10)),
        ])],
    )
    .await);
    assert_eq!(read_count(&c, &target).await.unwrap(), 1);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", ndt(2022, 6, 8, 0, 0)),
            ("id", i(1)),
            ("vl", i(100)),
        ])],
    )
    .await);
    assert_eq!(read_count(&c, &target).await.unwrap(), 1);
    assert_eq!(as_int(&c.docs(&pipe).await[0]["vl"]), Some(100));
    ok(c.conn.drop_pipe(&pipe).await);
    assert!(read_count(&c, &target).await.is_err());
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_new_columns, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "new",
        "cols",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2022-01-01")),
            ("id", i(1)),
            ("a", i(10)),
        ])],
    )
    .await);
    let df = c.get_data(&pipe, DataQuery::default()).await.unwrap();
    assert_eq!(df.width(), 3);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2022-01-01")),
            ("id", i(1)),
            ("b", i(20)),
        ])],
    )
    .await);
    let df = c.get_data(&pipe, DataQuery::default()).await.unwrap();
    assert_eq!(df.width(), 4);
    assert_eq!(df.height(), 1);
    c.delete(&pipe).await;
});

flavor_tests!(test_temporary_pipes, |c: Ctx| async move {
    // Python runs this on SQLite only (a fresh database file).
    if c.flavor != "sqlite" {
        return;
    }
    let path = temp_sqlite_path("temporary_pipes");
    let conn =
        meerschaum_sql::SqlConnector::from_uri("rs_temp", &format!("sqlite:///{}", path.display()))
            .await
            .unwrap();
    let pipe = Pipe::builder("foo", "bar")
        .instance(&conn.keys())
        .kwarg("temporary", true)
        .parameters(common::params(r#"{"columns": {"id": "id"}}"#))
        .build()
        .unwrap();
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_none());
    assert_eq!(
        conn.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        None
    );
    let sync = |docs: Vec<Doc>| {
        let conn = &conn;
        let pipe = &pipe;
        async move {
            conn.sync(
                pipe,
                meerschaum_kernel::enforce::PipeInput::Docs(docs),
                &SyncOptions::default(),
            )
            .await
        }
    };
    ok(sync(vec![doc(vec![("id", i(1)), ("a", i(2))])]).await);
    ok(sync(vec![doc(vec![("id", i(2)), ("b", i(3))])]).await);
    let _ = sync(vec![
        doc(vec![("id", i(1)), ("b", i(4))]),
        doc(vec![("id", i(2)), ("a", i(5))]),
    ])
    .await;
    // `pipe.delete()` fails for an unregistered (temporary) pipe.
    assert!(!conn.delete_pipe(&pipe).await.success);
    assert_eq!(
        conn.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(2)
    );
    for t in [
        "mrsm_pipes",
        "mrsm_users",
        "mrsm_plugins",
        "pipes",
        "users",
        "plugins",
    ] {
        assert!(!conn.table_exists(t, None).await, "{t} exists");
    }
    let _ = std::fs::remove_file(&path);
});

flavor_tests!(test_id_index_col, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test_id",
        "index_col",
        Some("table"),
        r#"{"dtypes": {"id": "Int64"}, "columns": {"datetime": "id"}}"#,
    );
    let make = |n: i64, mul: i64, div: f64, round: bool| -> Vec<Doc> {
        (0..n)
            .map(|k| {
                let c = k as f64 / div;
                let c = if round {
                    (c * 100.0).round() / 100.0
                } else {
                    c
                };
                doc(vec![
                    ("id", i(k)),
                    ("a", i(k * mul)),
                    ("b", KValue::Json(serde_json::json!({ "c": c }))),
                ])
            })
            .collect()
    };
    let check = |synced: &[Doc], expected: &[Doc]| {
        assert_eq!(synced.len(), expected.len());
        for (s, e) in synced.iter().zip(expected) {
            assert_eq!(as_int(&s["id"]), as_int(&e["id"]));
            assert_eq!(as_int(&s["a"]), as_int(&e["a"]));
            let (KValue::Json(sj), KValue::Json(ej)) = (&s["b"], &e["b"]) else {
                panic!("b is not JSON: {:?}", s["b"]);
            };
            assert_eq!(sj["c"].as_f64(), ej["c"].as_f64());
        }
    };
    let docs = make(100, 2, 2.0, false);
    ok(sync_live(&c, &mut pipe, docs.clone()).await);
    check(&c.docs(&pipe).await, &docs);
    ok(sync_live(&c, &mut pipe, docs.clone()).await);
    check(&c.docs(&pipe).await, &docs);
    let new_docs = make(10, 3, 3.0, true);
    ok(sync_live(&c, &mut pipe, new_docs.clone()).await);
    let small = docs_q(
        &c,
        &pipe,
        DataQuery {
            end: Some(DateBound::Int(10)),
            ..Default::default()
        },
    )
    .await;
    check(&small, &new_docs);
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_generators, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test_generators",
        "foo",
        None,
        r#"{"columns": {"datetime": "dt"}}"#,
    );
    for d in 0..3u32 {
        ok(sync_live(
            &c,
            &mut pipe,
            vec![doc(vec![("dt", ndt(2023, 1, 1 + d, 0, 0))])],
        )
        .await);
    }
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(3));
    c.delete(&pipe).await;
});

flavor_tests!(test_add_new_columns, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test_add",
        "columns",
        None,
        r#"{"columns": {"datetime": "dt"}}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("dt", s("2023-01-01")), ("a", i(1))])],
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2023-01-01")),
            ("b", s("foo")),
            ("c", fl(12.3)),
            ("d", j(r#"{"e": 5}"#)),
        ])],
    )
    .await);
    let types = c.conn.get_pipe_columns_types(&pipe).await.unwrap();
    assert_eq!(types.len(), 5, "{types:?}");
    let docs = c.docs(&pipe).await;
    assert_eq!(docs.len(), 1);
    for k in ["dt", "a", "b", "c", "d"] {
        assert!(docs[0].contains_key(k), "missing {k}");
    }
    match &docs[0]["d"] {
        KValue::Json(j) => assert!(j.get("e").is_some()),
        other => panic!("d is not JSON: {other:?}"),
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_get_data_iterator, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "get_data_iterator",
        Some("foo"),
        r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "Int64"}}"#,
    );
    let docs: Vec<Doc> = (0..7)
        .map(|k| {
            doc(vec![
                ("id", i(k)),
                ("color", s(if k % 2 == 0 { "a" } else { "b" })),
            ])
        })
        .collect();
    ok(sync_live(&c, &mut pipe, docs).await);
    let chunks = c
        .conn
        .get_pipe_data_chunks(
            &pipe,
            &DataQuery {
                begin: Some(DateBound::Int(1)),
                end: Some(DateBound::Int(6)),
                ..Default::default()
            },
            Some(ChunkIntervalArg::Int(2)),
        )
        .await
        .unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[2].height(), 1);
    let chunks = c
        .conn
        .get_pipe_data_chunks(
            &pipe,
            &q_params(r#"{"color": "a"}"#),
            Some(ChunkIntervalArg::Int(3)),
        )
        .await
        .unwrap();
    let ids: Vec<Option<i64>> = chunks
        .iter()
        .flat_map(|df| {
            let d = meerschaum_connector::pipe_ops::frame_to_docs(df);
            ints(&d, "id")
        })
        .collect();
    assert_eq!(ids, vec![Some(0), Some(2), Some(4), Some(6)]);
    c.delete(&pipe).await;
});

flavor_tests!(test_nested_chunks, |c: Ctx| async move {
    let mut pipe = c.pipe("test", "chunks", Some("nested"), "{}");
    for k in 0..3 {
        ok(sync_live(
            &c,
            &mut pipe,
            vec![doc(vec![("chunk_ix", i(k)), ("a", i(k))])],
        )
        .await);
    }
    assert_eq!(c.docs(&pipe).await.len(), 3);
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_null_indices, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "sync",
        "null",
        Some("indices"),
        r#"{"columns": ["a", "b"], "mixed_numerics": true}"#,
    );
    let n = KValue::Null;
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("a", i(1)), ("b", i(1)), ("c", i(1))])],
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("a", n.clone()), ("b", i(1)), ("c", i(1))])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 2);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("a", i(1)), ("b", n.clone()), ("c", i(1))])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 3);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("a", i(1)), ("b", n.clone()), ("c", i(1))])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 3);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("a", n.clone()), ("b", n.clone()), ("c", i(1))])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 4);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![(
            "c",
            KValue::Decimal(Decimal::parse("2.2").unwrap()),
        )])],
    )
    .await);
    assert_eq!(c.docs(&pipe).await.len(), 4);
    let df = docs_q(
        &c,
        &pipe,
        select_params(&["c"], r#"{"a": null, "b": [null]}"#),
    )
    .await;
    assert_eq!(df.len(), 1);
    // `df['c'][0] == Decimal('2.2')` (Decimal equality ignores trailing zeros).
    match &df[0]["c"] {
        KValue::Decimal(d) => assert_eq!(
            d.numeric_cmp(&Decimal::parse("2.2").unwrap()),
            Some(std::cmp::Ordering::Equal),
            "{d:?}"
        ),
        other => panic!("c is not a Decimal: {other:?}"),
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_upsert_sync, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "upsert",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}, "indices": {"all": ["dt", "id"]},
            "dtypes": {"num": "numeric"}, "upsert": true}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![
                ("dt", s("2023-01-01")),
                ("id", i(1)),
                ("yes", KValue::Bool(true)),
                ("num", s("1.1")),
            ]),
            doc(vec![
                ("dt", s("2023-01-02")),
                ("id", i(2)),
                ("yes", KValue::Bool(false)),
                ("num", s("2")),
            ]),
        ],
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2023-01-02")),
            ("id", i(2)),
            ("yes", KValue::Null),
            ("num", s("2")),
        ])],
    )
    .await);
    let df = docs_q(&c, &pipe, q_params(r#"{"id": 2}"#)).await;
    assert_eq!(df.len(), 1);
    assert!(df[0]["yes"].is_na(), "{:?}", df[0]["yes"]);
    match &df[0]["num"] {
        KValue::Decimal(d) => assert_eq!(d.to_f64(), 2.0),
        other => panic!("num is not a Decimal: {other:?}"),
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_upsert_no_value_cols, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "upsert",
        Some("no_vals"),
        r#"{"columns": {"datetime": "dt", "id": "id"}, "upsert": true}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        (1..=3)
            .map(|k| doc(vec![("dt", s(&format!("2023-01-0{k}"))), ("id", i(k))]))
            .collect(),
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("dt", s("2023-01-03")), ("id", i(3))])],
    )
    .await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(3));
    c.delete(&pipe).await;
});

flavor_tests!(test_primary_key, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test_sync",
        "primary",
        Some("key"),
        r#"{"columns": {"primary": "pk"}}"#,
    );
    let res = sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("pk", i(1))]), doc(vec![("pk", i(1))])],
    )
    .await;
    assert!(!res.success, "{}", res.message);
    c.conn.drop_pipe(&pipe).await;
    ok(sync_live(&c, &mut pipe, vec![doc(vec![("pk", i(1))])]).await);
    ok(sync_live(&c, &mut pipe, vec![doc(vec![("pk", i(1))])]).await);
    ok(sync_live(&c, &mut pipe, vec![doc(vec![("pk", i(2))])]).await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
    c.delete(&pipe).await;
});

flavor_tests!(test_autoincrement_primary_key, |c: Ctx| async move {
    let id_col = "My ID";
    let mut pipe = c.pipe(
        "test_sync",
        "primary_key",
        Some("autoincrement"),
        &format!(
            r#"{{"target": "{}Test Sync Primary Key Autoincrement", "columns": {{"primary": "{id_col}"}},
                "autoincrement": true, "dtypes": {{"{id_col}": "int"}}}}"#,
            c.prefix
        ),
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![("color", s("red"))]),
            doc(vec![("color", s("blue"))]),
        ],
    )
    .await);
    let df = docs_q(&c, &pipe, select(&[id_col])).await;
    assert_eq!(ints(&df, id_col), vec![Some(1), Some(2)]);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![(id_col, i(1)), ("color", s("green"))])],
    )
    .await);
    let df = docs_q(&c, &pipe, q_params(&format!(r#"{{"{id_col}": 1}}"#))).await;
    assert_eq!(df[0]["color"].py_str(), "green");
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![(id_col, i(4)), ("shirt_size", s("L"))]),
            doc(vec![(id_col, i(5)), ("shirt_size", s("M"))]),
        ],
    )
    .await);
    let df = docs_q(
        &c,
        &pipe,
        select_params(&["shirt_size"], &format!(r#"{{"{id_col}": [4, 5]}}"#)),
    )
    .await;
    assert_eq!(strs(&df, "shirt_size"), vec!["L", "M"]);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![("color", s("purple"))]),
            doc(vec![("shirt_size", s("S"))]),
        ],
    )
    .await);
    let df = docs_q(
        &c,
        &pipe,
        select_params(&["shirt_size"], &format!(r#"{{"{id_col}": 7}}"#)),
    )
    .await;
    assert_eq!(df[0]["shirt_size"].py_str(), "S");
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(6));
    c.delete(&pipe).await;
});

flavor_tests!(
    test_autoincrement_primary_key_inferred,
    |c: Ctx| async move {
        let mut pipe = c.pipe(
            "test_sync",
            "pk",
            Some("implicit"),
            r#"{"columns": {"primary": "id"}}"#,
        );
        ok(sync_live(
            &c,
            &mut pipe,
            vec![
                doc(vec![("color", s("red"))]),
                doc(vec![("color", s("blue"))]),
            ],
        )
        .await);
        let df = docs_q(&c, &pipe, select(&["id"])).await;
        assert_eq!(ints(&df, "id"), vec![Some(1), Some(2)]);
        ok(sync_live(
            &c,
            &mut pipe,
            vec![doc(vec![("id", i(1)), ("color", s("green"))])],
        )
        .await);
        let df = docs_q(&c, &pipe, q_params(r#"{"id": 1}"#)).await;
        assert_eq!(df[0]["color"].py_str(), "green");
        ok(sync_live(
            &c,
            &mut pipe,
            vec![
                doc(vec![("id", i(4)), ("shirt_size", s("L"))]),
                doc(vec![("id", i(5)), ("shirt_size", s("M"))]),
            ],
        )
        .await);
        let df = docs_q(
            &c,
            &pipe,
            select_params(&["shirt_size"], r#"{"id": [4, 5]}"#),
        )
        .await;
        assert_eq!(strs(&df, "shirt_size"), vec!["L", "M"]);
        ok(sync_live(&c, &mut pipe, vec![doc(vec![("color", s("purple"))])]).await);
        let df = docs_q(&c, &pipe, select_params(&["color"], r#"{"id": 6}"#)).await;
        assert_eq!(df[0]["color"].py_str(), "purple");
        assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(5));
        c.delete(&pipe).await;
    }
);

flavor_tests!(test_add_primary_key_to_existing, |c: Ctx| async move {
    let mut pipe = c.pipe("test_sync", "primary_key", Some("later"), "{}");
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![("id", i(1)), ("color", s("red"))]),
            doc(vec![("id", i(2)), ("color", s("blue"))]),
        ],
    )
    .await);
    let df = docs_q(&c, &pipe, select(&["id"])).await;
    assert_eq!(ints(&df, "id"), vec![Some(1), Some(2)]);
    pipe.set_columns(serde_json::from_str(r#"{"primary": "id"}"#).unwrap())
        .unwrap();
    assert!(c.conn.create_indices(&pipe, None, None).await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("id", i(3)), ("color", s("green"))])],
    )
    .await);
    let cols_indices = c.conn.get_pipe_columns_indices(&pipe).await.unwrap();
    let id_types: Vec<String> = cols_indices["id"]
        .iter()
        .map(|d| d.type_.clone().unwrap_or_default())
        .collect();
    assert!(
        id_types.contains(&"PRIMARY KEY".to_string()),
        "{id_types:?}"
    );
    let df = docs_q(&c, &pipe, q_params(r#"{"id": 3}"#)).await;
    assert_eq!(df[0]["color"].py_str(), "green");
    c.delete(&pipe).await;
});

flavor_tests!(test_static_schema, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "static",
        None,
        r#"{"upsert": true, "static": true, "columns": {"datetime": "dt", "primary": "id"},
            "dtypes": {"id": "uuid", "dt": "datetime64[ns]", "num": "numeric", "val": "float", "meta": "json"}}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2024-01-01 01:02:03.456789")),
            ("id", s("e7a57f9b-da26-4c70-bffc-ed27cdd74565")),
            ("num", s("1.23")),
            ("val", s("2.34")),
            ("meta", s(r#"{"a": 1}"#)),
        ])],
    )
    .await);
    let res = sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2024-01-01 01:02:03.456789")),
            ("id", s("44be2c5a-3d42-4e5f-b118-93a56697ae14")),
            ("val", s("foo")),
            ("bar", i(1)),
        ])],
    )
    .await;
    assert!(!res.success, "{}", res.message);
    let types = c.conn.get_pipe_columns_types(&pipe).await.unwrap();
    assert!(!types.contains_key("bar"), "{types:?}");
    assert!(
        ["DOUBLE", "DOUBLE PRECISION", "FLOAT", "REAL"]
            .contains(&types["val"].to_uppercase().as_str()),
        "{types:?}"
    );
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(1));
    c.delete(&pipe).await;
});

flavor_tests!(test_create_drop_indices, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "indices",
        Some("drop"),
        r#"{"columns": {"primary": "Id", "datetime": "dt"}, "upsert": true}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![
                ("Id", i(1)),
                ("dt", s("2025-01-01")),
                ("val", fl(1.1)),
            ]),
            doc(vec![
                ("Id", i(2)),
                ("dt", s("2025-01-02")),
                ("val", fl(1.2)),
            ]),
        ],
    )
    .await);
    let og = c.conn.get_pipe_columns_indices(&pipe).await.unwrap();
    assert!(og.len() > 1, "{og:?}");
    ok(c.conn.drop_pipe_indices(&pipe, None).await);
    pipe.set_indices(Default::default());
    ok(c.conn.edit_pipe(&pipe, false).await);
    let after = c.conn.get_pipe_columns_indices(&pipe).await.unwrap();
    assert!(after.len() <= og.len());
    ok(c.conn.create_pipe_indices(&pipe, None).await);
    let rebuilt = c.conn.get_pipe_columns_indices(&pipe).await.unwrap();
    assert_eq!(rebuilt.len(), og.len(), "{rebuilt:?} vs {og:?}");
    c.delete(&pipe).await;
});

flavor_tests!(test_no_null_indices, |c: Ctx| async move {
    // The instance half of `test_no_null_indices` (the in-place `sql:` pipe is not ported).
    let mut pipe = c.pipe(
        "test",
        "null_indices",
        Some("false"),
        r#"{"columns": {"datetime": "dt", "id": "id"}, "null_indices": false}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![
                ("dt", s("2025-01-01")),
                ("id", i(1)),
                ("val", fl(1.1)),
            ]),
            doc(vec![
                ("dt", s("2025-01-01")),
                ("id", i(2)),
                ("val", fl(2.2)),
            ]),
        ],
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![
                ("dt", s("2025-01-02")),
                ("id", i(3)),
                ("val", fl(3.3)),
            ]),
            doc(vec![
                ("dt", s("2025-01-02")),
                ("id", i(4)),
                ("val", fl(4.4)),
            ]),
        ],
    )
    .await);
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![
            ("dt", s("2025-01-02")),
            ("id", i(4)),
            ("val", fl(5.5)),
        ])],
    )
    .await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(4));
    c.delete(&pipe).await;
});

flavor_tests!(test_autotime, |c: Ctx| async move {
    let dt_col = "timestamp";
    let mut pipe = c.pipe(
        "test",
        "autotime",
        None,
        &format!(
            r#"{{"autotime": true, "static": true, "null_indices": false, "enforce": false,
                "columns": {{"datetime": "{dt_col}", "id": "id"}}, "dtypes": {{"{dt_col}": "int"}}}}"#
        ),
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![("id", i(1)), ("val", fl(100.1))]),
            doc(vec![("id", i(2)), ("val", fl(200.2))]),
        ],
    )
    .await);
    let df = c.docs(&pipe).await;
    assert!(df[0].contains_key(dt_col));
    let ts_1 = df[0][dt_col].clone();
    // A different autotime value on the next sync.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![("id", i(1)), ("val", fl(100.2))]),
            doc(vec![("id", i(2)), ("val", fl(200.3))]),
        ],
    )
    .await);
    let df = c.docs(&pipe).await;
    assert_eq!(df.len(), 4);
    let mut dt_vals: Vec<String> = col(&df, dt_col).iter().map(|v| v.py_str()).collect();
    dt_vals.sort();
    dt_vals.dedup();
    assert_eq!(dt_vals.len(), 2, "{dt_vals:?}");
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("id", i(1)), ("val", fl(90.1)), (dt_col, ts_1)])],
    )
    .await);
    let df = docs_q(&c, &pipe, q_params(r#"{"id": 1}"#)).await;
    let vals: Vec<f64> = col(&df, "val")
        .iter()
        .filter_map(|v| match v {
            KValue::Float(f) => Some(*f),
            KValue::Decimal(d) => Some(d.to_f64()),
            _ => None,
        })
        .collect();
    assert!(vals.contains(&90.1), "{vals:?}");
    assert_eq!(df.len(), 2);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    ok(sync_live(
        &c,
        &mut pipe,
        vec![doc(vec![("id", i(1)), ("val", fl(100.2))])],
    )
    .await);
    let df = docs_q(&c, &pipe, q_params(r#"{"id": 1}"#)).await;
    assert_eq!(df.len(), 3);
    c.delete(&pipe).await;
});

flavor_tests!(test_parametrized_sync_time, |c: Ctx| async move {
    let mut pipe = c.pipe(
        "test",
        "sync_time",
        Some("params"),
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    ok(sync_live(
        &c,
        &mut pipe,
        vec![
            doc(vec![
                ("ts", udt(2024, 1, 1, 0, 0)),
                ("id", s("a")),
                ("val", i(1)),
            ]),
            doc(vec![
                ("ts", udt(2024, 6, 1, 0, 0)),
                ("id", s("b")),
                ("val", i(2)),
            ]),
        ],
    )
    .await);
    let ta = c
        .conn
        .get_sync_time(&pipe, Some(&common::params(r#"{"id": "a"}"#)), true, false)
        .await
        .unwrap();
    let tb = c
        .conn
        .get_sync_time(&pipe, Some(&common::params(r#"{"id": "b"}"#)), true, false)
        .await
        .unwrap();
    let (Some(DateBound::Datetime(a)), Some(DateBound::Datetime(b))) = (ta, tb) else {
        panic!("missing sync times");
    };
    assert!(a.utc < b.utc);
    assert_eq!(a.utc, naive(2024, 1, 1, 0, 0, 0));
    assert_eq!(b.utc, naive(2024, 6, 1, 0, 0, 0));
    c.delete(&pipe).await;
});
