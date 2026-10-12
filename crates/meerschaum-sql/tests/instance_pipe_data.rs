//! `tests/test_pipe_data.py` against the SQL instance connector (one test per flavor).

mod common;
use common::*;

use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::Value as KValue;

/// `df[col].tolist()`.
fn col(df: &Frame, name: &str) -> Vec<KValue> {
    df.data(name).expect("column").values()
}

fn no_check() -> SyncOptions {
    SyncOptions {
        check_existing: false,
        ..Default::default()
    }
}

/// `datetime_to_int(datetime(2026, 5, day, tzinfo=utc), 'ms')`.
fn epoch_ms(day: u32) -> i64 {
    naive(2026, 5, day, 0, 0, 0).and_utc().timestamp_millis()
}

fn color_docs() -> Vec<Doc> {
    vec![
        doc(vec![("id", i(1)), ("color", s("red"))]),
        doc(vec![("id", i(2)), ("color", s("green"))]),
        doc(vec![("id", i(3)), ("color", s("blue"))]),
    ]
}

flavor_tests!(test_get_backtrack_data_limit, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "get_backtrack_data_limit",
        Some("foo"),
        r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}}"#,
    );
    ok(c.sync_with(&pipe, color_docs(), no_check()).await);
    let df = c
        .conn
        .get_backtrack_data(
            &pipe,
            None,
            &DataQuery {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(as_int(&col(&df, "id")[0]), Some(3));
    c.delete(&pipe).await;
});

flavor_tests!(test_get_data_order, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "get_data_order",
        Some("foo"),
        r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}}"#,
    );
    ok(c.sync_with(&pipe, color_docs(), no_check()).await);
    for (order, first) in [("asc", 1), ("desc", 3)] {
        let df = c
            .get_data(
                &pipe,
                DataQuery {
                    limit: Some(2),
                    order: Some(order.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(df.height(), 2, "{order}");
        assert_eq!(as_int(&col(&df, "id")[0]), Some(first), "{order}");
    }
    c.delete(&pipe).await;
});

fn dt_id_val_docs(n: usize) -> Vec<Doc> {
    (1..=n as i64)
        .map(|k| {
            doc(vec![
                ("dt", s(&format!("2024-01-0{k}"))),
                ("id", i(k)),
                ("val", fl(k as f64 * 10.0)),
            ])
        })
        .collect()
}

flavor_tests!(test_get_docs, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "get_docs",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    ok(c.sync_with(&pipe, dt_id_val_docs(3), no_check()).await);
    let docs = InstanceConnector::get_pipe_docs(&c.conn, &pipe, &DataQuery::default())
        .await
        .unwrap();
    assert_eq!(docs.len(), 3);
    assert!(docs[0].contains_key("val"));
    let empty = InstanceConnector::get_pipe_docs(
        &c.conn,
        &pipe,
        &DataQuery {
            begin: Some(nbound(2099, 1, 1, 0, 0)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(empty.is_empty());
    c.delete(&pipe).await;
});

flavor_tests!(test_get_docs_as_iterator, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "get_docs_iterator",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    ok(c.sync_with(&pipe, dt_id_val_docs(2), no_check()).await);
    let chunks = c
        .conn
        .get_pipe_data_chunks(&pipe, &DataQuery::default(), None)
        .await
        .unwrap();
    let all: usize = chunks
        .iter()
        .map(|f| meerschaum_connector::pipe_ops::frame_to_docs(f).len())
        .sum();
    assert_eq!(all, 2);
    c.delete(&pipe).await;
});

// `test_int_pipe_chunk_interval` exercises `Pipe.get_chunk_interval` / `get_backtrack_interval`
// only (no instance access); covered by `meerschaum-core` and `pipe::tests::chunk_interval`.

flavor_tests!(test_parse_date_bounds_epoch_int_axis, |c: Ctx| async move {
    let pipe = c.pipe(
        "demo",
        "bounds",
        Some("epoch"),
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "precision": "ms"}"#,
    );
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let dtypes = c.conn.pipe_state(&sp).await.dtypes;
    let parse = |b: Option<DateBound>| c.conn.parse_date_bound(&sp, &dtypes, b);
    let expected = epoch_ms(30);
    assert_eq!(
        parse(DateBound::parse("2026-05-30")).unwrap(),
        Some(DateBound::Int(expected))
    );
    assert_eq!(
        parse(Some(ubound(2026, 5, 30, 0, 0))).unwrap(),
        Some(DateBound::Int(expected))
    );
    assert_eq!(
        parse(Some(DateBound::Int(5))).unwrap(),
        Some(DateBound::Int(5))
    );
    assert_eq!(
        parse(DateBound::parse(&expected.to_string())).unwrap(),
        Some(DateBound::Int(expected))
    );
    assert_eq!(parse(None).unwrap(), None);
    assert_eq!(
        parse(DateBound::parse("2026-05-31")).unwrap(),
        Some(DateBound::Int(epoch_ms(31)))
    );
});

flavor_tests!(
    test_parse_date_bounds_non_epoch_int_axis_raises,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "demo",
            "bounds",
            Some("arbitrary"),
            r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#,
        );
        let sp = c.conn.sql_pipe(&pipe).unwrap();
        let dtypes = c.conn.pipe_state(&sp).await.dtypes;
        assert_eq!(
            c.conn
                .parse_date_bound(&sp, &dtypes, Some(DateBound::Int(5)))
                .unwrap(),
            Some(DateBound::Int(5))
        );
        assert_eq!(
            c.conn
                .parse_date_bound(&sp, &dtypes, DateBound::parse("5"))
                .unwrap(),
            Some(DateBound::Int(5))
        );
        assert!(
            c.conn
                .parse_date_bound(&sp, &dtypes, DateBound::parse("2026-05-30"))
                .is_err()
        );
    }
);

flavor_tests!(
    test_parse_date_bounds_datetime_axis_unchanged,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "demo",
            "bounds",
            Some("dt"),
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "datetime"}}"#,
        );
        let sp = c.conn.sql_pipe(&pipe).unwrap();
        let dtypes = c.conn.pipe_state(&sp).await.dtypes;
        let b = c
            .conn
            .parse_date_bound(&sp, &dtypes, DateBound::parse("2026-05-30"))
            .unwrap();
        assert!(matches!(b, Some(DateBound::Datetime(_))));
    }
);

flavor_tests!(test_epoch_int_axis_datetime_begin, |c: Ctx| async move {
    let pipe = c.pipe(
        "demo",
        "epoch_begin",
        Some("foo"),
        r#"{"columns": {"datetime": "ts", "id": "id"}, "dtypes": {"ts": "int", "id": "int"}, "precision": "ms"}"#,
    );
    let docs: Vec<Doc> = [28, 29, 30, 31]
        .iter()
        .map(|d| doc(vec![("ts", i(epoch_ms(*d))), ("id", i(*d as i64))]))
        .collect();
    ok(c.sync(&pipe, docs).await);
    let ids = |df: Frame| {
        let mut v: Vec<i64> = col(&df, "id").iter().filter_map(as_int).collect();
        v.sort();
        v
    };
    let filtered = c
        .get_data(
            &pipe,
            DataQuery {
                begin: DateBound::parse("2026-05-30"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(filtered), vec![30, 31]);
    ok(c.conn
        .clear_pipe(
            &pipe,
            &DataQuery {
                end: DateBound::parse("2026-05-30"),
                ..Default::default()
            },
        )
        .await);
    let remaining = c.get_data(&pipe, DataQuery::default()).await.unwrap();
    assert_eq!(ids(remaining), vec![30, 31]);
    c.delete(&pipe).await;
});
