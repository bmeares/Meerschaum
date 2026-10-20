//! Wave 18 (PF): fewer round trips on a fresh `Pipe`, and the columns types a sync hands to the
//! next read. Table prefix `w5sql_` (the shared `Ctx` prefix).

mod common;
use common::*;
use meerschaum_connector::{DataQuery, DateBound};

fn row(day: u32, id: i64, val: i64) -> Doc {
    doc(vec![
        ("dt", ndt(2024, 1, day, 0, 0)),
        ("id", i(id)),
        ("val", i(val)),
    ])
}

const DEFINITION: &str = r#"{"columns": {"datetime": "dt", "id": "id"}}"#;

// `get_pipe_attributes` reads the row by the pipe's keys in one statement (v4 looked the id up
// first): the id, keys and parameters are the registered ones, with and without a location.
flavor_tests!(attributes_come_from_one_query, |c: Ctx| async move {
    let with_location = c.pipe("attrs", "loc", Some("here"), DEFINITION);
    let without_location = c.pipe("attrs", "noloc", None, DEFINITION);
    let unregistered = c.pipe("attrs", "missing", None, DEFINITION);
    assert!(c.conn.register_pipe(&with_location).await.success);
    assert!(c.conn.register_pipe(&without_location).await.success);

    for pipe in [&with_location, &without_location] {
        let id = c.conn.get_pipe_id(pipe).await.unwrap().expect("registered");
        c.conn.enable_query_log();
        let attrs = c.conn.get_pipe_attributes(pipe).await.unwrap();
        let log = c.conn.take_query_log();
        let reads: Vec<_> = log.iter().filter(|q| q.contains("mrsm_pipes")).collect();
        assert_eq!(
            reads.len(),
            1,
            "one statement, not an id lookup and a read: {log:?}"
        );
        assert_eq!(attrs["pipe_id"].as_i64(), Some(id));
        assert_eq!(attrs["connector_keys"].py_str(), pipe.connector_keys());
        assert_eq!(attrs["metric_key"].py_str(), pipe.metric_key());
        match pipe.location_key() {
            Some(lk) => assert_eq!(attrs["location_key"].py_str(), lk),
            None => assert!(attrs["location_key"].is_null(), "{attrs:?}"),
        }
        assert_eq!(
            attrs["parameters"].to_map()["columns"].to_map()["datetime"].py_str(),
            "dt"
        );
    }
    // The same keys with and without a location are different pipes.
    let a = c.conn.get_pipe_attributes(&with_location).await.unwrap();
    let b = c.conn.get_pipe_attributes(&without_location).await.unwrap();
    assert_ne!(a["pipe_id"], b["pipe_id"]);

    assert!(
        c.conn
            .get_pipe_attributes(&unregistered)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        c.conn
            .get_pipe_parameters(&unregistered)
            .await
            .unwrap()
            .is_none()
    );
    c.delete(&with_location).await;
    c.delete(&without_location).await;
});

// A sync that wrote into an existing table leaves the table's columns types for the façade to
// adopt (once); one that created the table, and one that was superseded, leave none; a sync that
// added a column leaves the layout with it.
flavor_tests!(
    a_sync_hands_out_the_table_layout_once,
    |c: Ctx| async move {
        let pipe = c.pipe("synced", "cols", None, DEFINITION);
        let other = c.pipe("synced", "other", None, DEFINITION);

        ok(c.sync(&pipe, vec![row(1, 1, 10), row(2, 2, 20)]).await);
        assert!(
            c.conn.take_synced_columns_types(&pipe).is_none(),
            "the sync that created the table reflects it on the next read"
        );

        ok(c.sync(&pipe, vec![row(3, 3, 30)]).await);
        let handed = c
            .conn
            .take_synced_columns_types(&pipe)
            .expect("a sync into an existing table hands its layout out");
        let reflected = c.conn.get_pipe_columns_types(&pipe).await.unwrap();
        assert_eq!(handed, reflected);
        assert!(handed.contains_key("val"));
        assert!(c.conn.take_synced_columns_types(&pipe).is_none(), "once");
        assert!(
            c.conn.take_synced_columns_types(&other).is_none(),
            "per table"
        );

        // A new column: the layout handed out is the one after the `ADD COLUMN`.
        let extra = doc(vec![
            ("dt", ndt(2024, 1, 4, 0, 0)),
            ("id", i(4)),
            ("val", i(40)),
            ("extra", s("x")),
        ]);
        ok(c.sync(&pipe, vec![extra]).await);
        let handed = c.conn.take_synced_columns_types(&pipe).expect("layout");
        assert!(handed.contains_key("extra"), "{handed:?}");
        assert_eq!(handed, c.conn.get_pipe_columns_types(&pipe).await.unwrap());

        // An untaken layout does not outlive the table: re-creating it clears the entry.
        ok(c.sync(&pipe, vec![row(5, 5, 50)]).await);
        assert!(c.conn.drop_pipe(&pipe).await.success);
        ok(c.sync(&pipe, vec![row(1, 1, 10)]).await);
        assert!(c.conn.take_synced_columns_types(&pipe).is_none());
        c.delete(&pipe).await;
    }
);

// `get_sync_time` and `get_rowcount` answer the same through the prepared-statement path (a
// pooled PostgreSQL connection reuses the statement) as after the table changed under it.
flavor_tests!(
    sync_time_and_rowcount_follow_the_table,
    |c: Ctx| async move {
        let pipe = c.pipe("stable", "text", None, DEFINITION);
        ok(c.sync(&pipe, vec![row(1, 1, 10), row(2, 2, 20)]).await);
        for _ in 0..3 {
            let Some(DateBound::Datetime(newest)) = c
                .conn
                .get_sync_time(&pipe, None, true, false)
                .await
                .unwrap()
            else {
                panic!("no sync time");
            };
            assert_eq!(newest.utc, naive(2024, 1, 2, 0, 0, 0));
            assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(2));
        }
        ok(c.sync(&pipe, vec![row(3, 3, 30)]).await);
        let Some(DateBound::Datetime(newest)) = c
            .conn
            .get_sync_time(&pipe, None, true, false)
            .await
            .unwrap()
        else {
            panic!("no sync time");
        };
        assert_eq!(newest.utc, naive(2024, 1, 3, 0, 0, 0));
        assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(3));

        // The table is dropped and rebuilt with another layout of the same name: a statement
        // prepared for the old one must not answer for the new one.
        assert!(c.conn.drop_pipe(&pipe).await.success);
        assert_eq!(
            c.conn
                .get_sync_time(&pipe, None, true, false)
                .await
                .unwrap(),
            None
        );
        let rebuilt = doc(vec![
            ("dt", ndt(2025, 6, 1, 0, 0)),
            ("id", s("text-id")),
            ("val", fl(1.5)),
            ("added", i(1)),
        ]);
        ok(c.sync(&pipe, vec![rebuilt]).await);
        let Some(DateBound::Datetime(newest)) = c
            .conn
            .get_sync_time(&pipe, None, true, false)
            .await
            .unwrap()
        else {
            panic!("no sync time");
        };
        assert_eq!(newest.utc, naive(2025, 6, 1, 0, 0, 0));
        assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(1));
        c.delete(&pipe).await;
    }
);
