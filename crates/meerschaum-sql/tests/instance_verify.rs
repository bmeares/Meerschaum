//! `tests/test_verify.py` (data shapes): `Pipe.verify()` is engine-level, so the verification
//! loop is done by hand here with the instance pieces it uses: `get_chunk_bounds(align=True)`,
//! `get_pipe_rowcount` (local and remote), `fetch` and `sync`.
mod common;
mod fork_helpers;
use chrono::TimeDelta;
use common::*;
use fork_helpers::{row_dt, stress_docs};
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::Pipe;
use meerschaum_sql::instance::data::get_chunk_bounds;
use meerschaum_sql::instance::fetch::{FetchArgs, metadef_bound};
use meerschaum_sql::pipe::ChunkInterval;

/// The pipe's oldest and newest sync times as parsed bounds.
async fn sync_times(c: &Ctx, pipe: &Pipe) -> (Option<DateBound>, Option<DateBound>) {
    let sp = c.conn.sql_pipe(pipe).unwrap();
    let state = c.conn.pipe_state(&sp).await;
    let parse = |b| c.conn.parse_date_bound(&sp, &state.dtypes, b).unwrap();
    let oldest = parse(
        c.conn
            .get_sync_time(pipe, None, false, false)
            .await
            .unwrap(),
    );
    let newest = parse(c.conn.get_sync_time(pipe, None, true, false).await.unwrap());
    (oldest, newest)
}

fn in_chunk(d: &Doc, begin: &Option<DateBound>, end: &Option<DateBound>) -> bool {
    let dt = row_dt(d, "datetime");
    let pos = |b: &DateBound| match b {
        DateBound::Datetime(x) => x.utc,
        DateBound::Int(_) => unreachable!(),
    };
    begin.as_ref().is_none_or(|b| dt >= pos(b)) && end.as_ref().is_none_or(|e| dt < pos(e))
}

flavor_tests!(test_verify_backfill_simple, |c: Ctx| async move {
    let pipe = c.pipe(
        "plugin:stress",
        "test",
        Some("verify_backfill_simple"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}, "fetch": {"rows": 100, "id": 3}}"#,
    );
    let source = stress_docs(
        naive(2023, 1, 1, 0, 0, 0),
        naive(2023, 1, 2, 0, 0, 0),
        100,
        3,
    );
    ok(c.sync(&pipe, source.clone()).await);

    let clear = DataQuery {
        begin: Some(nbound(2023, 1, 1, 12, 0)),
        end: Some(nbound(2023, 1, 1, 14, 0)),
        ..Default::default()
    };
    let existing = c.get_data(&pipe, clear.clone()).await.unwrap();
    assert!(existing.height() > 0);
    ok(c.conn.clear_pipe(&pipe, &clear).await);
    assert_eq!(c.rowcount(&pipe, clear.clone()).await, Some(0));

    // `pipe.verify(chunk_interval=1 hour)`: re-sync the chunks whose rowcounts differ.
    let (oldest, newest) = sync_times(&c, &pipe).await;
    let bounds = get_chunk_bounds(
        None,
        None,
        true,
        ChunkInterval::Delta(TimeDelta::hours(1)),
        true,
        oldest,
        newest,
    )
    .unwrap();
    assert!(bounds.len() >= 20, "{bounds:?}");
    let mut resynced = 0;
    for (b, e) in &bounds {
        let remote: Vec<Doc> = source
            .iter()
            .filter(|d| in_chunk(d, b, e))
            .cloned()
            .collect();
        let local = c
            .rowcount(
                &pipe,
                DataQuery {
                    begin: b.clone(),
                    end: e.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or(0);
        if local != remote.len() as i64 {
            ok(c.sync(&pipe, remote).await);
            resynced += 1;
        }
    }
    assert_eq!(resynced, 2);

    let new = c.get_data(&pipe, clear).await.unwrap();
    assert_eq!(existing.height(), new.height());
    let docs = |f| meerschaum_connector::pipe_ops::frame_to_docs(f);
    let (a, b) = (docs(&existing), docs(&new));
    assert_eq!(row_dt(&a[0], "datetime"), row_dt(&b[0], "datetime"));
    assert_eq!(
        row_dt(a.last().unwrap(), "datetime"),
        row_dt(b.last().unwrap(), "datetime")
    );
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(100));
    c.delete(&pipe).await;
});

flavor_tests!(test_verify_backfill_inplace, |c: Ctx| async move {
    let source = c.pipe(
        "plugin:stress",
        "test",
        Some("verify_backfill_inplace"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}, "fetch": {"rows": 100, "id": 3}}"#,
    );
    let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
    let target = c.pipe(
        &c.conn.keys(),
        "test_verify",
        Some("backfill"),
        &format!(
            r#"{{"columns": {{"datetime": "datetime", "id": "id"}}, "fetch": {{"definition": "SELECT * FROM {}"}}}}"#,
            source_table.replace('"', "\\\"")
        ),
    );
    ok(c.sync(
        &source,
        stress_docs(
            naive(2023, 1, 1, 0, 0, 0),
            naive(2023, 1, 2, 0, 0, 0),
            100,
            3,
        ),
    )
    .await);

    // `target_pipe.sync()`: fetch the definition from the (same) source connector.
    let fetched = c
        .conn
        .fetch(
            &target,
            &FetchArgs {
                begin: metadef_bound(None),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.height(), 100);
    ok(c.sync_frame(&target, fetched).await);
    assert_eq!(
        c.rowcount(&source, DataQuery::default()).await,
        c.rowcount(&target, DataQuery::default()).await
    );

    // Backfill the source, then verify the target against the remote rowcounts.
    ok(c.sync(
        &source,
        stress_docs(
            naive(2022, 12, 24, 0, 0, 0),
            naive(2022, 12, 26, 0, 0, 0),
            100,
            3,
        ),
    )
    .await);
    let (oldest, newest) = sync_times(&c, &source).await;
    let sp = c.conn.sql_pipe(&target).unwrap();
    let bounds = get_chunk_bounds(
        None,
        None,
        true,
        sp.chunk_interval().unwrap(),
        true,
        oldest,
        newest,
    )
    .unwrap();
    for (b, e) in &bounds {
        let q = DataQuery {
            begin: b.clone(),
            end: e.clone(),
            ..Default::default()
        };
        let remote = c
            .rowcount(
                &target,
                DataQuery {
                    remote: true,
                    ..q.clone()
                },
            )
            .await;
        let local = c.rowcount(&target, q).await;
        if remote != local {
            let chunk = c
                .conn
                .fetch(
                    &target,
                    &FetchArgs {
                        begin: metadef_bound(b.as_ref()),
                        end: metadef_bound(e.as_ref()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            ok(c.sync_frame(&target, chunk).await);
        }
    }
    assert_eq!(c.rowcount(&source, DataQuery::default()).await, Some(200));
    assert_eq!(c.rowcount(&target, DataQuery::default()).await, Some(200));
    c.delete(&target).await;
    c.delete(&source).await;
});
