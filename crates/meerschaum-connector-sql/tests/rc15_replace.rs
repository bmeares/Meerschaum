//! Replacing syncs (rc15 Q19, `SyncOptions::replace`): the rows of `[begin, end)` (matching
//! `params`) are deleted and the frame's rows of that window inserted in one transaction.
mod common;
use common::*;
use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_kernel::value::Value as KValue;

fn replace_opts(begin: Option<u32>, end: Option<u32>) -> SyncOptions {
    SyncOptions {
        begin: begin.map(|d| nbound(2024, 1, d, 0, 0)),
        end: end.map(|d| nbound(2024, 1, d, 0, 0)),
        replace: true,
        ..Default::default()
    }
}

async fn values(c: &Ctx, pipe: &meerschaum_core::Pipe) -> Vec<i64> {
    let mut v: Vec<i64> = c
        .docs(pipe)
        .await
        .iter()
        .filter_map(|d| as_int(&d["v"]))
        .collect();
    v.sort();
    v
}

flavor_tests!(replace_windows, |c: Ctx| async move {
    let pipe = c.pipe(
        "q19",
        "replace",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    let row = |d: u32, id: i64, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, d, 0, 0)),
            ("id", i(id)),
            ("v", i(v)),
        ])
    };
    ok(
        c.sync(&pipe, (1..=5).map(|d| row(d, 1, d as i64)).collect())
            .await,
    );

    // [2, 4) is replaced by one row; the row of day 9 is outside the window.
    let res = ok(c
        .sync_with(
            &pipe,
            vec![row(2, 2, 20), row(9, 1, 9)],
            replace_opts(Some(2), Some(4)),
        )
        .await);
    assert_eq!(
        res.message, "Replaced 2 rows with 1 row.\nSkipped 1 row outside the window.",
        "{}",
        c.flavor
    );
    assert_eq!(values(&c, &pipe).await, vec![1, 4, 5, 20]);

    // An empty frame clears the window.
    let res = ok(c
        .sync_with(&pipe, Vec::new(), replace_opts(Some(1), Some(3)))
        .await);
    assert!(
        res.message.starts_with("Replaced 2 rows with 0 rows."),
        "{}",
        res.message
    );
    assert_eq!(values(&c, &pipe).await, vec![4, 5]);

    // `params`: only the matching rows of the window are replaced.
    ok(c.sync(&pipe, vec![row(4, 2, 42)]).await);
    let mut opts = replace_opts(Some(4), Some(5));
    opts.params = Some(params(r#"{"id": 2}"#));
    ok(c.sync_with(&pipe, vec![row(4, 2, 43)], opts).await);
    assert_eq!(values(&c, &pipe).await, vec![4, 5, 43]);
    c.delete(&pipe).await;
});

// The first replacing sync of a pipe with nothing in its window (a `fetch: {replace: true}`
// pipe whose first fetch is empty) succeeds, like a merge, without creating the table.
flavor_tests!(
    replace_into_a_missing_table_without_rows,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "q19",
            "empty",
            None,
            r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
        );
        let res = ok(c
            .sync_with(&pipe, Vec::new(), replace_opts(Some(1), Some(3)))
            .await);
        assert_eq!(res.message, "Replaced 0 rows with 0 rows.", "{}", c.flavor);
        let row = doc(vec![
            ("ts", ndt(2024, 1, 9, 0, 0)),
            ("id", i(1)),
            ("v", i(9)),
        ]);
        // Rows outside the window only: still nothing to write.
        let res = ok(c
            .sync_with(&pipe, vec![row], replace_opts(Some(1), Some(3)))
            .await);
        assert!(
            res.message.starts_with("Replaced 0 rows with 0 rows."),
            "{}: {}",
            c.flavor,
            res.message
        );
        c.delete(&pipe).await;
    }
);

flavor_tests!(replace_failures_keep_the_old_rows, |c: Ctx| async move {
    let pipe = c.pipe(
        "q19",
        "crash",
        None,
        r#"{"columns": {"datetime": "ts", "primary": "pk"}}"#,
    );
    let row = |d: u32, pk: i64, v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, d, 0, 0)),
            ("pk", i(pk)),
            ("v", i(v)),
        ])
    };
    ok(
        c.sync(&pipe, (1..=3).map(|d| row(d, d as i64, d as i64)).collect())
            .await,
    );
    // Two rows with the same key (the datetime is part of a partitioned table's primary key):
    // the insert fails after the delete, which rolls back.
    let res = c
        .sync_with(
            &pipe,
            vec![row(1, 7, 70), row(1, 7, 71)],
            replace_opts(Some(1), Some(4)),
        )
        .await;
    assert!(!res.success, "{}: {}", c.flavor, res.message);
    assert_eq!(values(&c, &pipe).await, vec![1, 2, 3], "{}", c.flavor);
    c.delete(&pipe).await;
});

flavor_tests!(
    replace_whole_tables_without_datetimes,
    |c: Ctx| async move {
        let pipe = c.pipe("q19", "nodt", None, r#"{"columns": {"id": "id"}}"#);
        let row = |id: i64| doc(vec![("id", i(id)), ("v", i(id))]);
        ok(c.sync(&pipe, (0..3).map(row).collect()).await);
        let res = ok(c
            .sync_with(&pipe, vec![row(9)], replace_opts(None, None))
            .await);
        assert_eq!(res.message, "Replaced 3 rows with 1 row.");
        assert_eq!(values(&c, &pipe).await, vec![9]);
        // Bounds need a datetime column.
        let res = c
            .sync_with(&pipe, vec![row(1)], replace_opts(Some(1), Some(2)))
            .await;
        assert!(!res.success);
        c.delete(&pipe).await;
    }
);

// A window that ends a microsecond after its last row (what `pipe.sync(df, replace=True)`
// derives from the data) replaces that row: SQLite compares the exact stored text.
flavor_tests!(replace_sub_second_windows, |c: Ctx| async move {
    let pipe = c.pipe(
        "q19",
        "subsecond",
        None,
        r#"{"columns": {"datetime": "ts", "id": "id"}}"#,
    );
    let row = |v: i64| {
        doc(vec![
            ("ts", ndt(2024, 1, 4, 0, 0)),
            ("id", i(1)),
            ("v", i(v)),
        ])
    };
    ok(c.sync(&pipe, vec![row(4)]).await);
    let begin = nbound(2024, 1, 4, 0, 0);
    let end = match &begin {
        meerschaum_connector::DateBound::Datetime(d) => {
            let mut d = d.clone();
            d.utc += chrono::TimeDelta::microseconds(1);
            meerschaum_connector::DateBound::Datetime(d)
        }
        other => other.clone(),
    };
    let opts = SyncOptions {
        begin: Some(begin),
        end: Some(end),
        replace: true,
        ..Default::default()
    };
    let res = ok(c.sync_with(&pipe, vec![row(40)], opts).await);
    assert!(
        res.message.starts_with("Replaced 1 row with 1 row."),
        "{}: {}",
        c.flavor,
        res.message
    );
    assert_eq!(values(&c, &pipe).await, vec![40]);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(1));
    let _ = KValue::Null;
    c.delete(&pipe).await;
});
