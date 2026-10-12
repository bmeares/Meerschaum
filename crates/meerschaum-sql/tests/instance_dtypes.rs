//! `tests/test_pipes_dtypes.py` against the SQL instance connector (one test per flavor).

mod common;
use common::*;

use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::dtypes::are_dtypes_equal;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_kernel::{Frame, Geometry};
use meerschaum_sql::instance::fetch::FetchArgs;

/// `df[col].tolist()`.
fn col(df: &Frame, name: &str) -> Vec<KValue> {
    df.data(name)
        .unwrap_or_else(|_| panic!("missing column '{name}' in {:?}", df.names()))
        .values()
}

/// `str(df.dtypes[col])`.
fn dtype(df: &Frame, name: &str) -> String {
    df.dtypes().get(name).cloned().unwrap_or_default()
}

fn uuid(s: &str) -> KValue {
    KValue::Uuid(uuid::Uuid::parse_str(s).unwrap())
}

fn json_of(v: &KValue) -> serde_json::Value {
    match v {
        KValue::Json(j) => j.clone(),
        other => panic!("not JSON: {other:?}"),
    }
}

/// `datetime.strftime('%Y-%m-%d')`.
fn ymd(v: &KValue) -> String {
    match v {
        KValue::Datetime(dt) => dt.local().format("%Y-%m-%d").to_string(),
        other => panic!("not a datetime: {other:?}"),
    }
}

fn geometry(v: &KValue) -> Geometry {
    match v {
        KValue::Geometry(g) => g.clone(),
        other => panic!("not a geometry: {other:?}"),
    }
}

/// The pipe with its registered parameters (Python mutates `pipe.parameters` in place when a
/// sync captures new special columns; a Rust `Pipe` is a value).
async fn refreshed(c: &Ctx, pipe: &meerschaum_core::Pipe) -> meerschaum_core::Pipe {
    let reg = c.registered(pipe).await;
    let mut p = pipe.clone();
    if !reg.is_empty() {
        p.set_parameters(reg);
    }
    p
}

async fn all(c: &Ctx, pipe: &meerschaum_core::Pipe) -> Frame {
    let pipe = refreshed(c, pipe).await;
    c.get_data(&pipe, DataQuery::default()).await.expect("data")
}

/// `df[col][i] == Decimal(text)` (numeric equality, like Python's `Decimal.__eq__`).
fn dec_eq(v: &KValue, text: &str) -> bool {
    let want = meerschaum_kernel::Decimal::parse(text).unwrap();
    match v {
        KValue::Decimal(d) => d.numeric_cmp(&want) == Some(std::cmp::Ordering::Equal),
        _ => false,
    }
}

flavor_tests!(test_sync_change_columns_dtypes, |c: Ctx| async move {
    let pipe = c.pipe(
        "change",
        "cols",
        Some("dtypes"),
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", s("2022-01-01")),
            ("id", i(1)),
            ("a", i(10)),
        ])],
    )
    .await);
    assert_eq!(all(&c, &pipe).await.width(), 3);
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", s("2022-01-01")),
            ("id", i(1)),
            ("a", s("foo")),
        ])],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert_eq!(df.width(), 3);
    assert_eq!(df.height(), 1);
    assert!(
        are_dtypes_equal(&dtype(&df, "a"), "string"),
        "{}",
        dtype(&df, "a")
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_dtype_enforcement, |c: Ctx| async move {
    let pipe = c.pipe(
        "dtype",
        "enforcement",
        None,
        r#"{"static": false, "upsert": true, "enforce": true,
            "columns": {"datetime": "dt", "id": "id"},
            "dtypes": {"id": "int", "int": "int", "float": "float", "bool": "bool",
                       "object": "object", "json": "json", "numeric": "numeric", "str": "str",
                       "uuid": "uuid", "bytes": "bytes", "date": "date", "geometry": "geometry"}}"#,
    );
    let cases = [
        ("int", "1"),
        ("float", "1.0"),
        ("bool", "True"),
        ("object", "foo"),
        ("str", "bar"),
        ("json", r#"{"a": {"b": 1}}"#),
        ("numeric", "1"),
        ("uuid", "00000000-1234-5678-0000-000000000000"),
        ("bytes", "Zm9vIGJhcg=="),
        ("date", "2025-01-01"),
        ("geometry", "POINT (0 0)"),
    ];
    let mut msgs = Vec::new();
    for (col_name, val) in cases {
        let res = c
            .sync(
                &pipe,
                vec![doc(vec![
                    ("dt", s("2022-01-01")),
                    ("id", i(1)),
                    (col_name, s(val)),
                ])],
            )
            .await;
        if !res.success {
            msgs.push(format!("{col_name}: {}", res.message));
        }
    }
    assert!(msgs.is_empty(), "{}", msgs.join("\n"));
    let df = all(&c, &pipe).await;
    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), cases.len() + 2, "{:?}", df.names());
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let dtypes = c.conn.pipe_state(&sp).await.dtypes;
    for (name, typ) in df.dtypes() {
        let mut pipe_dtype = dtypes.get(&name).cloned().unwrap_or_default();
        let v = &col(&df, &name)[0];
        match pipe_dtype.as_str() {
            "json" => {
                assert!(
                    matches!(v, KValue::Json(serde_json::Value::Object(_))),
                    "{v:?}"
                );
                pipe_dtype = "object".into();
            }
            "numeric" => {
                assert!(matches!(v, KValue::Decimal(_)), "{v:?}");
                pipe_dtype = "object".into();
            }
            "uuid" => {
                assert!(matches!(v, KValue::Uuid(_)), "{v:?}");
                pipe_dtype = "object".into();
            }
            "bytes" => {
                assert!(matches!(v, KValue::Bytes(_)), "{v:?}");
                pipe_dtype = "object".into();
            }
            "str" => assert!(matches!(v, KValue::Str(_)), "{v:?}"),
            "geometry" => {
                assert_eq!(geometry(v).geom_type(), "Point");
                pipe_dtype = "object".into();
            }
            _ => {}
        }
        assert!(
            are_dtypes_equal(&pipe_dtype.to_lowercase(), &typ),
            "{name}: pipe dtype '{pipe_dtype}' vs frame dtype '{typ}'"
        );
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_infer_json_dtype, |c: Ctx| async move {
    let pipe = c.pipe("foo", "bar", Some("infer_json"), r#"{"columns": ["id"]}"#);
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("id", i(1)), ("a", j(r#"["b", "c"]"#))]),
            doc(vec![("id", i(2)), ("a", j(r#"{"b": 1}"#))]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert!(json_of(&a[0]).is_array(), "{a:?}");
    assert!(json_of(&a[1]).is_object(), "{a:?}");
    c.delete(&pipe).await;
});

flavor_tests!(test_force_json_dtype, |c: Ctx| async move {
    let pipe = c.pipe(
        "foo",
        "bar",
        Some("force_json"),
        r#"{"dtypes": {"a": "json"}, "columns": ["id"]}"#,
    );
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("id", i(1)), ("a", s(r#"["b", "c"]"#))]),
            doc(vec![("id", i(2)), ("a", s(r#"{"b": 1}"#))]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert!(json_of(&a[0]).is_array(), "{a:?}");
    assert!(json_of(&a[1]).is_object(), "{a:?}");
    c.delete(&pipe).await;
});

flavor_tests!(test_infer_numeric_dtype, |c: Ctx| async move {
    // `scale, precision = NUMERIC_PRECISION_FLAVORS.get(flavor, NUMERIC_PRECISION_FLAVORS['mssql'])`
    let (scale, precision): (usize, usize) = if c.is_mysql() { (38, 20) } else { (28, 10) };
    let mut digits: Vec<u32> = Vec::new();
    for _ in 0..4 {
        digits.extend((0..10).rev());
    }
    digits.truncate(scale);
    let decimal_digits = &digits[digits.len() - precision..];
    let numeric_digits = &digits[..digits.len() - precision];
    let numeric_str = format!(
        "{}.{}",
        numeric_digits
            .iter()
            .map(|d| d.to_string())
            .collect::<String>(),
        decimal_digits
            .iter()
            .map(|d| d.to_string())
            .collect::<String>()
    );
    let pipe = c.pipe("infer", "numeric", None, r#"{"columns": ["id"]}"#);
    let d = |t: &str| KValue::Decimal(meerschaum_kernel::Decimal::parse(t).unwrap());
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("id", i(1)), ("a", d("1"))]),
            doc(vec![("id", i(2)), ("a", d(&numeric_str))]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert!(dec_eq(&a[0], "1"), "{a:?}");
    assert!(dec_eq(&a[1], &numeric_str), "{a:?}");
    c.delete(&pipe).await;
});

flavor_tests!(test_infer_uuid_dtype, |c: Ctx| async move {
    let pipe = c.pipe("infer", "uuid", None, r#"{"columns": ["id"]}"#);
    let u = "e6f3a4ea-f1af-4e93-8da9-716b57672206";
    ok(c.sync(&pipe, vec![doc(vec![("id", i(1)), ("a", uuid(u))])])
        .await);
    let df = all(&c, &pipe).await;
    assert_eq!(col(&df, "a")[0], uuid(u));
    c.delete(&pipe).await;
});

flavor_tests!(test_infer_bytes_dtype, |c: Ctx| async move {
    let pipe = c.pipe("infer", "bytes", None, r#"{"columns": ["id"]}"#);
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("id", i(1)),
            ("a", KValue::Bytes(b"foo bar".to_vec())),
        ])],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert_eq!(col(&df, "a")[0], KValue::Bytes(b"foo bar".to_vec()));
    c.delete(&pipe).await;
});

flavor_tests!(test_infer_numeric_from_mixed_types, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "infer_numeric",
        Some("mixed"),
        r#"{"columns": ["id"]}"#,
    );
    ok(c.sync(&pipe, vec![doc(vec![("id", i(1)), ("a", i(1))])])
        .await);
    ok(c.sync(&pipe, vec![doc(vec![("id", i(2)), ("a", fl(2.1))])])
        .await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert!(dec_eq(&a[0], "1"), "{a:?}");
    assert!(dec_eq(&a[1], "2.1"), "{a:?}");
    c.delete(&pipe).await;
});

flavor_tests!(test_force_numeric_dtype, |c: Ctx| async move {
    let pipe = c.pipe(
        "foo",
        "bar",
        Some("force_numeric"),
        r#"{"dtypes": {"a": "numeric"}, "columns": ["id"]}"#,
    );
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("id", i(1)), ("a", s("1"))]),
            doc(vec![("id", i(2)), ("a", s("2.1"))]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert!(dec_eq(&a[0], "1"), "{a:?}");
    assert!(dec_eq(&a[1], "2.1"), "{a:?}");

    ok(c.sync(&pipe, vec![doc(vec![("a", KValue::Null)])]).await);
    let df = all(&c, &pipe).await;
    let a = col(&df, "a");
    assert_eq!(a.last().unwrap().py_str(), "NaN", "{a:?}");
    for v in &a {
        assert!(matches!(v, KValue::Decimal(_)), "{v:?}");
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_utc_offset_datetimes, |c: Ctx| async move {
    let pipe = c.pipe(
        "test_utc_offset",
        "datetimes",
        None,
        r#"{"columns": {"datetime": "dt"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("dt", ndt(2023, 1, 1, 0, 0))]),
            doc(vec![("dt", ndt(2023, 1, 1, 23, 0))]),
        ],
    )
    .await);
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("dt", s("2023-01-01 00:00:00+00:00"))]),
            doc(vec![("dt", s("2023-01-02 00:00:00+01:00"))]),
        ],
    )
    .await);
    let docs = c.docs(&pipe).await;
    assert_eq!(
        docs,
        vec![
            doc(vec![("dt", udt(2023, 1, 1, 0, 0))]),
            doc(vec![("dt", udt(2023, 1, 1, 23, 0))]),
        ]
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_explicit_utc_datetimes, |c: Ctx| async move {
    let pipe = c.pipe(
        "test_explicit",
        "datetimes",
        Some("utc"),
        r#"{"columns": {"datetime": "dt"}, "dtypes": {"dt": "datetime64[ns, UTC]"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("dt", s("2024-01-01 00:00:00+00:00"))]),
            doc(vec![("dt", s("2024-01-02 00:00:00+01:00"))]),
        ],
    )
    .await);
    assert_eq!(
        c.docs(&pipe).await,
        vec![
            doc(vec![("dt", udt(2024, 1, 1, 0, 0))]),
            doc(vec![("dt", udt(2024, 1, 1, 23, 0))]),
        ]
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_ignore_datetime_conversion, |c: Ctx| async move {
    let pipe = c.pipe(
        "test_utc_offset",
        "datetimes",
        Some("ignore"),
        r#"{"dtypes": {"dt": "str"}}"#,
    );
    let docs = vec![
        doc(vec![("dt", s("2023-01-01 00:00:00+00:00"))]),
        doc(vec![("dt", s("2023-01-02 00:00:00+01:00"))]),
    ];
    ok(c.sync(&pipe, docs.clone()).await);
    let df = c
        .get_data(
            &pipe,
            DataQuery {
                select_columns: Some(vec!["dt".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(meerschaum_connector::pipe_ops::frame_to_docs(&df), docs);
    c.delete(&pipe).await;
});

// `test_mixed_offset_datetimes_sql`: skipped — needs `sync_pipe_inplace` (not ported) and
// pandas `to_sql(if_exists='replace')` table creation.

flavor_tests!(
    test_no_indices_inferred_datetime_to_text,
    |c: Ctx| async move {
        let pipe = c.pipe("test_no_indices", "datetimes", Some("text"), "{}");
        assert!(!c.conn.pipe_exists(&pipe).await.unwrap());
        let row = |dt: &str, a: i64| doc(vec![("fake-dt", s(dt)), ("a", i(a))]);
        ok(c.sync(&pipe, vec![row("2023-01-01", 1)]).await);
        ok(
            c.sync(&pipe, vec![row("2023-01-01", 1), row("2023-01-02", 2)])
                .await,
        );
        assert_eq!(all(&c, &pipe).await.height(), 2);
        ok(c.sync(
            &pipe,
            vec![row("2023-01-01", 1), row("2023-01-02", 2), row("foo", 3)],
        )
        .await);
        assert_eq!(all(&c, &pipe).await.height(), 3);
        c.delete(&pipe).await;
    }
);

async fn bools_scenario(c: &Ctx, pipe: &meerschaum_core::Pipe) {
    let row = |dt: &str, b: bool| doc(vec![("dt", s(dt)), ("is_bool", KValue::Bool(b))]);
    let check = |df: &Frame, expected: &[(&str, bool)]| {
        assert!(
            dtype(df, "is_bool").contains("bool"),
            "{}",
            dtype(df, "is_bool")
        );
        let dts = col(df, "dt");
        let bools = col(df, "is_bool");
        let got: Vec<(String, KValue)> = dts
            .iter()
            .zip(bools.iter())
            .map(|(d, b)| (ymd(d), b.clone()))
            .collect();
        let want: Vec<(String, KValue)> = expected
            .iter()
            .map(|(d, b)| (d.to_string(), KValue::Bool(*b)))
            .collect();
        assert_eq!(got, want);
    };
    ok(c.sync(
        pipe,
        vec![row("2023-01-01", true), row("2023-01-02", false)],
    )
    .await);
    check(
        &all(c, pipe).await,
        &[("2023-01-01", true), ("2023-01-02", false)],
    );
    ok(c.sync(
        pipe,
        vec![row("2023-01-01", false), row("2023-01-02", true)],
    )
    .await);
    check(
        &all(c, pipe).await,
        &[("2023-01-01", false), ("2023-01-02", true)],
    );
}

flavor_tests!(test_sync_bools, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "bools",
        None,
        r#"{"columns": {"datetime": "dt"}, "dtypes": {"is_bool": "bool"}}"#,
    );
    bools_scenario(&c, &pipe).await;
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_bools_inferred, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "bools",
        Some("inferred"),
        r#"{"columns": {"datetime": "dt"}}"#,
    );
    bools_scenario(&c, &pipe).await;
    c.delete(&pipe).await;
});

/// The `*_inplace` tests through the regular (non-in-place) path: `fetch` the source pipe's
/// table with the SQL definition, then `sync` the result into the downstream pipe.
/// `sync_pipe_inplace` itself is not ported.
async fn fetch_sync(c: &Ctx, down: &meerschaum_core::Pipe) -> meerschaum_core::SuccessTuple {
    let df = c
        .conn
        .fetch(down, &FetchArgs::default())
        .await
        .expect("fetch")
        .expect("frame");
    c.sync_frame(down, df).await
}

fn table_of(c: &Ctx, pipe: &meerschaum_core::Pipe) -> String {
    let sp = c.conn.sql_pipe(pipe).unwrap();
    c.conn.pipe_table_name(&sp)
}

fn inplace_pipe(
    c: &Ctx,
    src: &meerschaum_core::Pipe,
    metric: &str,
    dtypes: &str,
) -> meerschaum_core::Pipe {
    let def = format!("SELECT * FROM {}", table_of(c, src));
    let params = format!(
        r#"{{"columns": {{"datetime": "dt", "id": "id"}}, "dtypes": {dtypes}, "fetch": {{"definition": {}}}}}"#,
        serde_json::to_string(&def).unwrap()
    );
    c.pipe(
        &format!("sql_{}", c.flavor),
        metric,
        Some("inplace"),
        &params,
    )
}

flavor_tests!(test_sync_bools_inplace, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "bools",
        Some("inplace"),
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    let down = inplace_pipe(&c, &pipe, "bools", r#"{"is_bool": "bool"}"#);
    let row =
        |dt: &str, id: i64, b: KValue| doc(vec![("dt", s(dt)), ("id", i(id)), ("is_bool", b)]);
    ok(c.sync(
        &pipe,
        vec![
            row("2023-01-01", 1, KValue::Bool(true)),
            row("2023-01-02", 2, KValue::Bool(false)),
            row("2023-01-03", 3, KValue::Null),
        ],
    )
    .await);
    ok(fetch_sync(&c, &down).await);
    let sp = c.conn.sql_pipe(&down).unwrap();
    assert!(c.conn.pipe_state(&sp).await.dtypes["is_bool"].contains("bool"));
    assert_eq!(c.rowcount(&down, DataQuery::default()).await, Some(3));

    ok(
        c.sync(&pipe, vec![row("2023-01-03", 3, KValue::Bool(true))])
            .await,
    );
    ok(fetch_sync(&c, &down).await);
    for p in [&pipe, &down] {
        let df = c.get_data(p, q_params(r#"{"id": 3}"#)).await.unwrap();
        assert_eq!(col(&df, "is_bool")[0], KValue::Bool(true));
    }
    ok(c.sync(&pipe, vec![row("2023-01-03", 3, KValue::Null)])
        .await);
    ok(fetch_sync(&c, &down).await);
    let df = c.get_data(&down, q_params(r#"{"id": 3}"#)).await.unwrap();
    assert!(col(&df, "is_bool")[0].is_na(), "{:?}", col(&df, "is_bool"));
    c.delete(&down).await;
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_uuids_inplace, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "uuid",
        Some("inplace"),
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    let down = inplace_pipe(&c, &pipe, "uuid", r#"{"uuid_col": "uuid"}"#);
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![
                ("dt", s("2023-01-01")),
                ("id", i(1)),
                ("uuid_col", uuid("77e704d2-7513-45c7-b806-7b5cb0badc37")),
            ]),
            doc(vec![
                ("dt", s("2023-01-02")),
                ("id", i(2)),
                ("uuid_col", uuid("2854eeed-2911-4641-8d67-6ecd217392cc")),
            ]),
            doc(vec![("dt", s("2023-01-03")), ("id", i(3))]),
        ],
    )
    .await);
    ok(fetch_sync(&c, &down).await);
    let sp = c.conn.sql_pipe(&down).unwrap();
    let state = c.conn.pipe_state(&sp).await;
    assert!(state.dtypes["uuid_col"].contains("uuid"));
    assert_eq!(c.rowcount(&down, DataQuery::default()).await, Some(3));
    let db_col = state.cols_types["uuid_col"].clone();
    let expected = meerschaum_sql::dtypes::get_db_type_from_pd_type("uuid", Some(c.conn.flavor()));
    assert_eq!(
        db_col.split('(').next().unwrap(),
        expected.split('(').next().unwrap()
    );
    let update = "7befa9bd-fbb3-404b-b8e1-6389db6b6e84";
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", s("2023-01-03")),
            ("id", i(3)),
            ("uuid_col", uuid(update)),
        ])],
    )
    .await);
    ok(fetch_sync(&c, &down).await);
    let df = c.get_data(&down, q_params(r#"{"id": 3}"#)).await.unwrap();
    assert_eq!(col(&df, "uuid_col")[0], uuid(update));
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", s("2023-01-03")),
            ("id", i(3)),
            ("uuid_col", KValue::Null),
        ])],
    )
    .await);
    ok(fetch_sync(&c, &down).await);
    let df = c.get_data(&down, q_params(r#"{"id": 3}"#)).await.unwrap();
    assert!(col(&df, "uuid_col")[0].is_na());
    c.delete(&down).await;
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_bytes_inplace, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "bytes",
        Some("inplace"),
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    let down = inplace_pipe(&c, &pipe, "bytes", r#"{"bytes_col": "bytes"}"#);
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![
                ("dt", s("2024-01-01")),
                ("id", i(1)),
                ("bytes_col", KValue::Bytes(b"foo bar".to_vec())),
            ]),
            doc(vec![
                ("dt", s("2024-01-02")),
                ("id", i(2)),
                ("bytes_col", KValue::Bytes(b"do re mi".to_vec())),
            ]),
            doc(vec![("dt", s("2024-01-03")), ("id", i(3))]),
        ],
    )
    .await);
    ok(fetch_sync(&c, &down).await);
    let sp = c.conn.sql_pipe(&down).unwrap();
    assert!(c.conn.pipe_state(&sp).await.dtypes["bytes_col"].contains("bytes"));
    assert_eq!(c.rowcount(&down, DataQuery::default()).await, Some(3));
    let df = all(&c, &down).await;
    let b = col(&df, "bytes_col");
    assert_eq!(b[0], KValue::Bytes(b"foo bar".to_vec()));
    assert_eq!(b[1], KValue::Bytes(b"do re mi".to_vec()));
    assert!(b[2].is_na());
    c.delete(&down).await;
    c.delete(&pipe).await;
});

flavor_tests!(test_sync_uuids_simple_upsert, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "uuids",
        Some("upsert"),
        r#"{"columns": ["datetime", "id"], "upsert": false}"#,
    );
    let row = |dt: &str, id: &str, val: &str| {
        doc(vec![
            ("datetime", s(dt)),
            ("id", uuid(id)),
            ("val", uuid(val)),
        ])
    };
    ok(c.sync(
        &pipe,
        vec![
            row(
                "2024-01-01",
                "7d78f4a7-8c0d-4cc3-9636-9516aa0c32ce",
                "603e2509-8c53-4942-8395-4c8775455df1",
            ),
            row(
                "2024-01-02",
                "07557810-5662-449d-b0da-8360fe6134fe",
                "de8b4f65-1bbb-41ac-9572-be637a647349",
            ),
        ],
    )
    .await);
    ok(c.sync(
        &pipe,
        vec![
            row(
                "2024-01-01",
                "7d78f4a7-8c0d-4cc3-9636-9516aa0c32ce",
                "603e2509-8c53-4942-8395-4c8775455df1",
            ),
            row(
                "2024-01-02",
                "07557810-5662-449d-b0da-8360fe6134fe",
                "d1cc1516-16e5-4471-8ab9-e969a1def655",
            ),
        ],
    )
    .await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
    let pipe = refreshed(&c, &pipe).await;
    let df = c
        .get_data(
            &pipe,
            q_params(r#"{"id": "07557810-5662-449d-b0da-8360fe6134fe"}"#),
        )
        .await
        .unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(
        col(&df, "val")[0],
        uuid("d1cc1516-16e5-4471-8ab9-e969a1def655")
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_upsert_null_special_dtypes, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "upsert",
        Some("null_special"),
        r#"{"columns": {"primary": "id"}, "upsert": true,
            "dtypes": {"id": "int", "j": "json", "n": "numeric", "u": "uuid", "t": "string"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("id", i(1)),
            ("j", j("[1, 2]")),
            (
                "n",
                KValue::Decimal(meerschaum_kernel::Decimal::parse("1.5").unwrap()),
            ),
            ("u", uuid("7d78f4a7-8c0d-4cc3-9636-9516aa0c32ce")),
            ("t", s("first")),
        ])],
    )
    .await);
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("id", i(1)),
            ("j", KValue::Null),
            ("n", KValue::Null),
            ("u", KValue::Null),
            ("t", s("second")),
        ])],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert_eq!(df.height(), 1);
    assert_eq!(col(&df, "t")[0], s("second"));
    for name in ["j", "n", "u"] {
        assert!(
            col(&df, name)[0].is_na(),
            "'{name}' is not null: {:?}",
            col(&df, name)[0]
        );
    }
    c.delete(&pipe).await;
});

flavor_tests!(test_mixed_timezone_aware_and_naive, |c: Ctx| async move {
    let target = format!("{}_test_timezone_mix", c.prefix);
    let params = format!(r#"{{"columns": {{"datetime": "ts"}}, "target": "{target}"}}"#);
    let pipe = c.pipe("test", "timezone", Some("aware_naive"), &params);
    // `conn.to_sql(pd.DataFrame([{'ts': datetime(2024, 1, 1), 'val': 2}]), target)`: a naive
    // timestamp column created outside of the pipe.
    let flavor = c.conn.flavor();
    let ts_type = meerschaum_sql::dtypes::get_db_type_from_pd_type("datetime64[ns]", Some(flavor));
    let int_type = meerschaum_sql::dtypes::get_db_type_from_pd_type("int64", Some(flavor));
    let q = |n: &str| meerschaum_sql::ident::sql_item_name(n, flavor, None);
    let table = table_of(&c, &pipe);
    let ts_lit = if c.is_pg() {
        "'2024-01-01 00:00:00'"
    } else {
        "'2024-01-01 00:00:00.000000'"
    };
    assert!(
        c.conn
            .exec(&format!(
                "CREATE TABLE {table} ({} {ts_type}, {} {int_type})",
                q("ts"),
                q("val")
            ))
            .await
            .is_some()
    );
    assert!(
        c.conn
            .exec(&format!("INSERT INTO {table} VALUES ({ts_lit}, 2)"))
            .await
            .is_some()
    );
    // `filter_existing(src_df)` finds nothing new: syncing the same row is a no-op.
    let res = ok(c
        .sync(
            &pipe,
            vec![doc(vec![("ts", ndt(2024, 1, 1, 0, 0)), ("val", i(2))])],
        )
        .await);
    assert_eq!(res.message, "Inserted 0, updated 0 rows.");

    ok(c.sync(
        &pipe,
        vec![doc(vec![("ts", s("2024-01-01 05:00:00")), ("val", i(3))])],
    )
    .await);
    let df = c
        .get_data(
            &pipe,
            DataQuery {
                begin: DateBound::parse("2024-01-01 05:00:00"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(as_int(&col(&df, "val")[0]), Some(3));

    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("ts", s("2024-01-01 05:00:00+00:00")),
            ("val", i(4)),
        ])],
    )
    .await);
    let df = c
        .get_data(
            &pipe,
            DataQuery {
                begin: DateBound::parse("2024-01-01 05:00:00+00:00"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(as_int(&col(&df, "val")[0]), Some(4));
    c.delete(&pipe).await;
});

flavor_tests!(test_parse_date_bounds, |c: Ctx| async move {
    let naive_begin = Some(nbound(2024, 1, 1, 0, 0));
    let naive_end = Some(nbound(2024, 1, 2, 0, 0));
    let aware_begin = Some(ubound(2024, 1, 1, 0, 0));
    let aware_end = Some(ubound(2024, 2, 1, 0, 0));
    let is_aware = |b: &Option<DateBound>| match b {
        Some(DateBound::Datetime(dt)) => dt.tz.is_some(),
        other => panic!("not a datetime: {other:?}"),
    };
    for (lk, params, aware) in [
        ("tz", r#"{"columns": {"datetime": "ts"}}"#, true),
        (
            "naive",
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "datetime64[ns]"}}"#,
            false,
        ),
    ] {
        let pipe = c.pipe("test", "parse_date_bounds", Some(lk), params);
        ok(c.sync(&pipe, vec![doc(vec![("ts", s("2024-01-01"))])])
            .await);
        ok(c.sync(&pipe, vec![doc(vec![("ts", s("2024-01-02"))])])
            .await);
        let sp = c.conn.sql_pipe(&pipe).unwrap();
        let dtypes = c.conn.pipe_state(&sp).await.dtypes;
        for (b, e) in [
            (&naive_begin, &naive_end),
            (&aware_begin, &aware_end),
            (&naive_begin, &aware_end),
        ] {
            let b = c.conn.parse_date_bound(&sp, &dtypes, b.clone()).unwrap();
            let e = c.conn.parse_date_bound(&sp, &dtypes, e.clone()).unwrap();
            assert_eq!(is_aware(&b), aware, "{lk}");
            assert_eq!(is_aware(&e), aware, "{lk}");
        }
        c.delete(&pipe).await;
    }
});

flavor_tests!(test_distant_datetimes, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "datetimes",
        Some("distant"),
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "datetime64[ms]"}, "enforce": true}"#,
    );
    ok(c.sync(&pipe, vec![doc(vec![("ts", ndt(1, 1, 1, 0, 0))])])
        .await);
    let df = all(&c, &pipe).await;
    let ts = as_naive_utc(&col(&df, "ts")[0]).expect("datetime");
    assert_eq!(chrono::Datelike::year(&ts), 1);
    c.delete(&pipe).await;
});

flavor_tests!(test_enforce_false, |c: Ctx| async move {
    if c.flavor != "timescaledb" {
        return;
    }
    let pipe = c.pipe(
        "test",
        "enforce",
        None,
        r#"{"enforce": false, "static": true, "dtypes": {"dt": "datetime64[ns]"}, "columns": {"datetime": "dt"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![("dt", ndt(2024, 12, 26, 0, 0)), ("num", i(1))])],
    )
    .await);
    let df = c
        .get_data(
            &pipe,
            DataQuery {
                select_columns: Some(vec!["num".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(as_int(&col(&df, "num")[0]), Some(1));
    ok(c.sync(
        &pipe,
        vec![doc(vec![("dt", ndt(2024, 12, 26, 0, 0)), ("num", i(2))])],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert_eq!(df.height(), 1);
    assert_eq!(as_int(&col(&df, "num")[0]), Some(2));
    c.delete(&pipe).await;
});

flavor_tests!(test_mixed_int_float_coersion, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "mixed",
        Some("float_int"),
        r#"{"columns": ["id"], "dtypes": {"i": "int"}, "static": false, "enforce": true, "upsert": true}"#,
    );
    ok(
        c.sync(&pipe, vec![doc(vec![("id", i(1)), ("i", fl(f64::NAN))])])
            .await,
    );
    c.delete(&pipe).await;
    ok(c.sync(
        &pipe,
        vec![doc(vec![("id", i(1)), ("i", fl(0.0000000001))])],
    )
    .await);
    c.delete(&pipe).await;
});

flavor_tests!(
    test_explicit_precision_scale_numeric_sql,
    |c: Ctx| async move {
        let pipe = c.pipe(
        "test",
        "explicit",
        Some("precision_scale"),
        r#"{"columns": {"primary": "id", "datetime": "dt"}, "dtypes": {"val": "numeric[5,2]"}}"#,
    );
        let _ = c
            .sync(
                &pipe,
                vec![doc(vec![
                    ("id", i(1)),
                    ("val", s("123.456")),
                    ("dt", s("2025-01-01")),
                ])],
            )
            .await;
        let df = all(&c, &pipe).await;
        assert_eq!(col(&df, "val")[0].py_str(), "123.46");
        c.delete(&pipe).await;
    }
);

const MULTILINE_HEX: &str = "01050000000200000001020000000200000000000000000000000000000000000000000000000000F03F00000000000000400102000000020000000000000000001040000000000000104000000000000014400000000000001840";

async fn geometry_scenario(
    c: &Ctx,
    pipe: &meerschaum_core::Pipe,
    column: &str,
    wkt: &str,
    hex: &str,
) {
    let expected = Geometry::from_wkt(wkt).unwrap();
    ok(
        c.sync(pipe, vec![doc(vec![("id", i(1)), (column, s(wkt))])])
            .await,
    );
    let df = all(c, pipe).await;
    let got = geometry(&col(&df, column)[0]);
    assert_eq!(got.shape, expected.shape, "{got:?}");
    ok(c.sync_frame(pipe, df).await);
    assert_eq!(c.rowcount(pipe, DataQuery::default()).await, Some(1));
    ok(
        c.sync(pipe, vec![doc(vec![("id", i(1)), (column, s(hex))])])
            .await,
    );
    assert_eq!(c.rowcount(pipe, DataQuery::default()).await, Some(1));
}

flavor_tests!(test_geometry_dtype, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "geometry",
        None,
        r#"{"columns": {"primary": "id"}, "dtypes": {"id": "int", "geom": "geometry"}}"#,
    );
    geometry_scenario(
        &c,
        &pipe,
        "geom",
        "MULTILINESTRING ((0 0, 1 2), (4 4, 5 6))",
        MULTILINE_HEX,
    )
    .await;
    c.delete(&pipe).await;
});

flavor_tests!(test_geography_dtype, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "geography",
        None,
        r#"{"columns": {"primary": "id"}, "dtypes": {"id": "int", "geog": "geography[SRID=4326]"}}"#,
    );
    geometry_scenario(
        &c,
        &pipe,
        "geog",
        "MULTILINESTRING ((0 0, 1 2), (4 4, 5 6))",
        MULTILINE_HEX,
    )
    .await;
    c.delete(&pipe).await;
});

flavor_tests!(test_geometry_custom_srid, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "geometry",
        Some("srid"),
        r#"{"columns": {"primary": "id"}, "dtypes": {"id": "int", "geom": "geometry[Point,4326]"}}"#,
    );
    geometry_scenario(
        &c,
        &pipe,
        "geom",
        "POINT (-82.3511 34.86965)",
        "01010000000A68226C789654C0B37BF2B0506F4140",
    )
    .await;
    c.delete(&pipe).await;
});

flavor_tests!(test_geometry_esri_srid, |c: Ctx| async move {
    // The GeoDataFrame `crs` check (and the `{{ Pipe }}` downstream pipe) live above the
    // instance; this checks the round trip and the dtype persisted with the pipe.
    let pipe = c.pipe(
        "test",
        "geometry",
        Some("esri"),
        r#"{"columns": {"primary": "id"}, "dtypes": {"id": "int", "geo": "geometry[esri:102003]"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![("id", i(1)), ("geo", s("POINT (0 0)"))])],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert_eq!(geometry(&col(&df, "geo")[0]).geom_type(), "Point");
    let reg = c.registered(&pipe).await;
    assert_eq!(
        reg["dtypes"].get("geo").map(|v| v.py_str()).as_deref(),
        Some("geometry[esri:102003]")
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_date_as_datetime, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "date",
        Some("datetime_column"),
        r#"{"columns": {"datetime": "day", "id": "id"}, "dtypes": {"day": "date"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![
                ("day", s("2025-07-22")),
                ("id", i(1)),
                ("val", fl(100.1)),
            ]),
            doc(vec![
                ("day", s("2025-07-22")),
                ("id", i(2)),
                ("val", fl(200.2)),
            ]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert!(
        dtype(&df, "day").contains("date32"),
        "{}",
        dtype(&df, "day")
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_date_inferred, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "date",
        Some("inferred"),
        r#"{"autotime": true, "columns": ["id"], "dtypes": {"ts": "int"}}"#,
    );
    let day = KValue::Date(chrono::NaiveDate::from_ymd_opt(2025, 7, 22).unwrap());
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("day", day.clone()), ("id", i(1))]),
            doc(vec![("day", day), ("id", i(2))]),
        ],
    )
    .await);
    let df = all(&c, &pipe).await;
    assert!(
        dtype(&df, "day").contains("date32"),
        "{}",
        dtype(&df, "day")
    );
    c.delete(&pipe).await;
});

// `test_json_index_column_is_rejected`: skipped — the json-index validation is in
// `Pipe.register()` / `Pipe.sync()` (`core/Pipe/_register.py:79`), above the instance.

flavor_tests!(
    test_json_column_not_indexed_syncs_repeatedly,
    |c: Ctx| async move {
        let pipe = c.pipe(
        "test",
        "json_value_col",
        None,
        r#"{"columns": {"datetime": "ts", "id": "sid"}, "dtypes": {"sid": "int", "attrs": "json"}}"#,
    );
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("ts", udt(2024, 1, 1, 0, 0)),
                ("sid", i(1)),
                ("attrs", j(r#"{"a": 1}"#)),
            ])],
        )
        .await);
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("ts", udt(2024, 1, 1, 0, 0)),
                ("sid", i(1)),
                ("attrs", j(r#"{"a": 2}"#)),
            ])],
        )
        .await);
        let df = all(&c, &pipe).await;
        assert_eq!(df.height(), 1);
        assert_eq!(json_of(&col(&df, "attrs")[0]), serde_json::json!({"a": 2}));
        c.delete(&pipe).await;
    }
);

flavor_tests!(
    test_json_column_allowed_under_indices,
    |c: Ctx| async move {
        let pipe = c.pipe(
        "test",
        "json_perf_index",
        None,
        r#"{"columns": {"datetime": "ts", "id": "sid"}, "indices": {"cfg": "attrs"}, "dtypes": {"sid": "int", "attrs": "json"}}"#,
    );
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("ts", udt(2024, 1, 1, 0, 0)),
                ("sid", i(1)),
                ("attrs", j(r#"{"a": 1}"#)),
            ])],
        )
        .await);
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("ts", udt(2024, 1, 1, 0, 0)),
                ("sid", i(1)),
                ("attrs", j(r#"{"a": 2}"#)),
            ])],
        )
        .await);
        c.delete(&pipe).await;
    }
);
