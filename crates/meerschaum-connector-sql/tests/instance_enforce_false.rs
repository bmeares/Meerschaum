//! PARITY H9: pipes with `enforce: false` read the backtrack window and create their tables
//! like v4 (`Pipe.enforce_dtypes` with `enforce=False`, `get_data`, `filter_existing`):
//! a re-synced key is updated (not inserted twice), an integer datetime axis is a datetime
//! column, and a timezone-naive axis against a UTC table raises pandas' merge error.
//!
//! SQLite always; the PostgreSQL family (schema `w15q`) and the MySQL family when their URIs are
//! set. The MySQL family stores `datetime` without a timezone, so v4 reads the backtrack naive
//! and raises the merge error for UTC rows too: the v4 oracle cannot reach those containers from
//! the reference environment, so the `mysql_family` tests below are Rust-only expectations that
//! apply the pandas merge rules the SQLite/PostgreSQL fixtures pin (naive against naive merges,
//! aware against naive raises, with the delta as the left side) to a table whose datetimes come
//! back naive. Every test names its pipes with the harness' unique prefix and deletes them.

mod common;
use common::*;
use meerschaum_kernel::value::Value as KValue;

/// `flavor_tests!` with the PostgreSQL family inside schema `w15q`.
macro_rules! h9_tests {
    ($name:ident, $body:expr) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            async fn run(flavor: &'static str) {
                let Some(ctx) =
                    $crate::common::ctx_in_schema(flavor, stringify!($name), "w15q").await
                else {
                    return;
                };
                let f = $body;
                f(ctx).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn sqlite() {
                run("sqlite").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgresql() {
                run("postgresql").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn timescaledb() {
                run("timescaledb").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgis() {
                run("postgis").await
            }
        }
    };
}

/// `(inserted, updated)` from `"Inserted 1, updated 0 rows."`.
fn counts(message: &str) -> (i64, i64) {
    let nums: Vec<i64> = message
        .split(|c: char| !c.is_ascii_digit())
        .filter(|t| !t.is_empty())
        .map(|t| t.parse().unwrap())
        .collect();
    (nums[0], nums[1])
}

h9_tests!(
    aware_datetime_axis_updates_the_same_key,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "aware",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", udt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        let first = ok(c.sync(&pipe, vec![row(1)]).await);
        assert_eq!(counts(&first.message), (1, 0), "{}", first.message);
        let second = ok(c.sync(&pipe, vec![row(2)]).await);
        assert_eq!(counts(&second.message), (0, 1), "{}", second.message);
        let third = ok(c.sync(&pipe, vec![row(2)]).await);
        assert_eq!(counts(&third.message), (0, 0), "{}", third.message);
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
    }
);

h9_tests!(
    string_datetime_axis_updates_the_same_key,
    |c: Ctx| async move {
        // v4 parses the strings of the first sync; the second keeps them as text and `astype`s them.
        let pipe = c.pipe(
            "strdt",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", s("2026-01-01 00:00:00")),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
    }
);

h9_tests!(null_values_update_the_same_key, |c: Ctx| async move {
    let pipe = c.pipe(
        "nulls",
        "enf",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
    );
    let first = doc(vec![
        ("dt", udt(2026, 1, 1, 0, 0)),
        ("id", i(1)),
        ("v", KValue::Null),
    ]);
    let second = doc(vec![
        ("dt", udt(2026, 1, 1, 0, 0)),
        ("id", i(1)),
        ("v", i(4)),
    ]);
    assert_eq!(
        counts(&ok(c.sync(&pipe, vec![first]).await).message),
        (1, 0)
    );
    assert_eq!(
        counts(&ok(c.sync(&pipe, vec![second]).await).message),
        (0, 1)
    );
    assert_eq!(c.docs(&pipe).await.len(), 1);
    c.delete(&pipe).await;
});

h9_tests!(no_axis_pipe_updates_the_same_key, |c: Ctx| async move {
    let pipe = c.pipe(
        "noaxis",
        "enf",
        None,
        r#"{"columns": {"id": "id"}, "enforce": false}"#,
    );
    let row = |v: i64| doc(vec![("id", i(1)), ("v", i(v))]);
    assert_eq!(
        counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
        (1, 0)
    );
    assert_eq!(
        counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
        (0, 1)
    );
    let docs = c.docs(&pipe).await;
    assert_eq!(docs.len(), 1, "{docs:?}");
    assert_eq!(as_int(&docs[0]["v"]), Some(2));
    c.delete(&pipe).await;
});

h9_tests!(
    integer_axis_is_created_as_a_datetime_column,
    |c: Ctx| async move {
        // v4 parses the integers of the first sync as datetimes (a `TIMESTAMP` column) and casts the
        // second sync's integers to the table's `datetime` dtype as microsecond ticks.
        let pipe = c.pipe(
            "intaxis",
            "enf",
            None,
            r#"{"columns": {"datetime": "ts", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| doc(vec![("ts", i(1)), ("id", i(1)), ("v", i(v))]);
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        let types = c
            .conn
            .get_pipe_columns_types(&pipe)
            .await
            .expect("column types");
        let ts_type = types["ts"].to_uppercase();
        assert!(
            ts_type.contains("TIMESTAMP") || ts_type.contains("DATETIME"),
            "{types:?}"
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert!(matches!(docs[0]["ts"], KValue::Datetime(_)), "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
    }
);

h9_tests!(
    naive_datetime_axis_raises_the_pandas_merge_error,
    |c: Ctx| async move {
        // PARITY: v4's `filter_existing` merges a timezone-naive delta with the UTC-aware backtrack
        // and pandas raises; the façade turns the failed result into the same `ValueError`.
        let pipe = c.pipe(
            "naive",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", ndt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        let second = c.sync(&pipe, vec![row(2)]).await;
        assert!(!second.success);
        assert_eq!(
            second.message,
            "You are trying to merge on datetime64[us] and datetime64[us, UTC] columns for key 'dt'. \
         If you wish to proceed you should use pd.concat"
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(1));
        c.delete(&pipe).await;
    }
);

h9_tests!(
    enforcing_pipes_still_update_naive_rows,
    |c: Ctx| async move {
        // The same rows with `enforce` on are coerced to UTC and match.
        let pipe = c.pipe(
            "enforcing",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", ndt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        assert_eq!(c.docs(&pipe).await.len(), 1);
        c.delete(&pipe).await;
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_reads_the_datetimes_of_a_non_enforcing_pipe_as_utc() {
    // v4 reads the stored text and `astype`s it to `datetime64[us, UTC]`.
    let c = ctx("sqlite", "h9_read").await.expect("sqlite");
    let pipe = c.pipe(
        "read",
        "enf",
        None,
        r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![("dt", udt(2026, 1, 1, 0, 0)), ("id", i(1))])],
    )
    .await);
    let df = c.get_data(&pipe, Default::default()).await.expect("data");
    assert_eq!(df.dtypes()["dt"], "datetime64[us, UTC]");
    c.delete(&pipe).await;
}

/// The MySQL family (`mysql`, `mariadb`): `datetime` columns carry no timezone, so a
/// non-enforcing pipe reads its backtrack window naive. Rust-only expectations (module docs).
macro_rules! mysql_family_tests {
    ($name:ident, $body:expr) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            async fn run(flavor: &'static str) {
                let Some(ctx) = $crate::common::ctx(flavor, stringify!($name)).await else {
                    return;
                };
                let f = $body;
                f(ctx).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mysql() {
                run("mysql").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mariadb() {
                run("mariadb").await
            }
        }
    };
}

mysql_family_tests!(
    mysql_family_naive_axis_updates_the_same_key,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "naive",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", ndt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 0)
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
    }
);

mysql_family_tests!(
    mysql_family_aware_axis_raises_the_pandas_merge_error,
    |c: Ctx| async move {
        // PARITY: the delta is UTC-aware and the table's backtrack is naive: pandas refuses the merge
        // (the SQLite/PostgreSQL fixtures pin the mirrored message for a naive delta).
        let pipe = c.pipe(
            "aware",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", udt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        let second = c.sync(&pipe, vec![row(2)]).await;
        assert!(!second.success, "{}", second.message);
        assert_eq!(
            second.message,
            "You are trying to merge on datetime64[us, UTC] and datetime64[us] columns for key 'dt'. \
         If you wish to proceed you should use pd.concat"
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(1));
        c.delete(&pipe).await;
    }
);

mysql_family_tests!(
    mysql_family_null_values_update_the_same_key,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "nulls",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "enforce": false}"#,
        );
        let first = doc(vec![
            ("dt", ndt(2026, 1, 1, 0, 0)),
            ("id", i(1)),
            ("v", KValue::Null),
        ]);
        let second = doc(vec![
            ("dt", ndt(2026, 1, 1, 0, 0)),
            ("id", i(1)),
            ("v", i(4)),
        ]);
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![first]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![second]).await).message),
            (0, 1)
        );
        assert_eq!(c.docs(&pipe).await.len(), 1);
        c.delete(&pipe).await;
    }
);

mysql_family_tests!(
    mysql_family_no_axis_pipe_updates_the_same_key,
    |c: Ctx| async move {
        let pipe = c.pipe(
            "noaxis",
            "enf",
            None,
            r#"{"columns": {"id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| doc(vec![("id", i(1)), ("v", i(v))]);
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
    }
);

mysql_family_tests!(
    mysql_family_integer_axis_is_a_datetime_column,
    |c: Ctx| async move {
        // The integers of the first sync become datetimes (a `DATETIME` column). The second sync's
        // integers are cast to the pipe's `datetime` dtype, which is UTC-aware, while the backtrack
        // comes back naive: pandas refuses the merge (the same rule as the aware axis above).
        let pipe = c.pipe(
            "intaxis",
            "enf",
            None,
            r#"{"columns": {"datetime": "ts", "id": "id"}, "enforce": false}"#,
        );
        let row = |v: i64| doc(vec![("ts", i(1)), ("id", i(1)), ("v", i(v))]);
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        let second = c.sync(&pipe, vec![row(2)]).await;
        assert!(!second.success, "{}", second.message);
        assert_eq!(
            second.message,
            "You are trying to merge on datetime64[us, UTC] and datetime64[us] columns for key 'ts'. \
         If you wish to proceed you should use pd.concat"
        );
        let types = c
            .conn
            .get_pipe_columns_types(&pipe)
            .await
            .expect("column types");
        let ts_type = types["ts"].to_uppercase();
        assert!(
            ts_type.contains("TIMESTAMP") || ts_type.contains("DATETIME"),
            "{types:?}"
        );
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1, "{docs:?}");
        assert!(matches!(docs[0]["ts"], KValue::Datetime(_)), "{docs:?}");
        assert_eq!(as_int(&docs[0]["v"]), Some(1));
        c.delete(&pipe).await;
    }
);

mysql_family_tests!(
    mysql_family_enforcing_pipes_update_utc_rows,
    |c: Ctx| async move {
        // With `enforce` on the delta and the backtrack are both coerced to UTC and match.
        let pipe = c.pipe(
            "enforcing",
            "enf",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        let row = |v: i64| {
            doc(vec![
                ("dt", udt(2026, 1, 1, 0, 0)),
                ("id", i(1)),
                ("v", i(v)),
            ])
        };
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(1)]).await).message),
            (1, 0)
        );
        assert_eq!(
            counts(&ok(c.sync(&pipe, vec![row(2)]).await).message),
            (0, 1)
        );
        assert_eq!(c.docs(&pipe).await.len(), 1);
        c.delete(&pipe).await;
    }
);
