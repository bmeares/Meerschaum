//! Confined verify segments and direct sync and verify of filtered rows.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confined_verify_delivers_each_sqlite_segment() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("direct", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom", "scopes":["actions:execute", "pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", "segmented-ticks")
        .instance("sql:main")
        .parameters(map(json!({
            "columns": {"datetime":"tick", "id":"tick"},
            "dtypes": {"tick":"int64"},
            "target": "segmented_ticks",
            "verify": {"chunk_range": 2},
            "fetch": {"definition": "SELECT 1 AS tick UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8"}
        })))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "direct").await;
    let (status, body) = response(
        client
            .post(format!("{origin}/actions/verify"))
            .bearer_auth(token)
            .json(&json!({
                "action": ["pipes"],
                "connector_keys": ["sql:main"],
                "metric_keys": ["segmented-ticks"],
                "location_keys": ["None"],
                "mrsm_instance": "sql:main",
                "begin": 2,
                "end": 8
            })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.get(0),
        Some(&json!(true)),
        "{body}{}",
        log_tails(root.path())
    );

    let data = instance
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let column = data.column("tick").unwrap();
    let mut ticks: Vec<_> = (0..data.height())
        .map(|index| match column.data.get(index) {
            meerschaum_kernel::value::Value::Int(value) => value,
            value => panic!("unexpected tick {value:?}"),
        })
        .collect();
    ticks.sort_unstable();
    assert_eq!(ticks, [2, 3, 4, 5, 6, 7]);

    let records = Catalog::new(root.path()).records().unwrap();
    let captures: Vec<_> = records
        .values()
        .filter(|record| {
            matches!(
                record.operation.as_ref(),
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            )
        })
        .collect();
    assert_eq!(captures.len(), 1);
    let record = captures[0];
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert!(!record.commit_journal.has_pending());
    // Each two-row segment must be delivered and acknowledged independently.
    let journal = serde_json::to_value(&record.commit_journal).unwrap();
    assert_eq!(journal["audit"]["acknowledged_commits"], 3, "{journal}");
    assert_eq!(journal["audit"]["affected_rows"], 6, "{journal}");
    let stream = &journal["batches"]["nodes"][pipe.pipe_ref()];
    assert_eq!(stream["retired_through"], 3, "{stream}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_sync_and_verify_filter_integer_station_rows() {
    use meerschaum_kernel::value::Value as Cell;
    use meerschaum_kernel::{Column, Frame};
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("direct", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom", "scopes":["actions:execute", "pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", "station-samples")
        .instance("sql:main")
        .parameters(map(json!({
            "columns":{"datetime":"tick","id":"station"},
            "dtypes":{"tick":"int64","station":"int64"},
            "target":"station_samples", "verify":{"chunk_range":2},
            "fetch":{"definition":"SELECT CAST(t AS INTEGER) AS tick, CAST(s AS INTEGER) AS station FROM (SELECT 1 AS t, 10 AS s UNION ALL SELECT 2, 10 UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 10 UNION ALL SELECT 3, 20 UNION ALL SELECT 4, 20) AS samples"}
        }))).build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let seed = Frame::new(vec![
        Column::object("tick", vec![Cell::Int(0)]),
        Column::object("station", vec![Cell::Int(20)]),
    ])
    .unwrap();
    let seeded = instance
        .sync(
            &pipe,
            meerschaum_kernel::enforce::PipeInput::Frame(seed),
            &Default::default(),
        )
        .await;
    assert!(seeded.success, "{}", seeded.message);
    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "direct").await;
    for (action, station, expected) in [
        ("sync", 10, vec![(0, 20), (2, 10), (3, 10)]),
        (
            "verify",
            20,
            vec![(0, 20), (2, 10), (2, 20), (3, 10), (3, 20), (4, 20)],
        ),
    ] {
        let (status, body) = response(client.post(format!("{origin}/actions/{action}")).bearer_auth(&token).json(&json!({
            "action":["pipes"], "connector_keys":["sql:main"], "metric_keys":["station-samples"], "location_keys":["None"], "mrsm_instance":"sql:main",
            "begin":2,"end":5, "params":{"station":station}
        }))).await;
        assert_eq!(status, 200, "{body}");
        let data = instance
            .get_pipe_data(&pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        let mut actual = Vec::new();
        for row in 0..data.height() {
            let (Cell::Int(tick), Cell::Int(station)) = (
                data.column("tick").unwrap().data.get(row),
                data.column("station").unwrap().data.get(row),
            ) else {
                panic!("Expected canonical integer sample columns");
            };
            actual.push((tick, station));
        }
        actual.sort_unstable();
        let records = Catalog::new(root.path()).records().unwrap();
        let record = records
            .values()
            .find(|record| {
                record
                    .operation
                    .as_ref()
                    .and_then(|operation| operation.endpoint())
                    .is_some_and(|endpoint| endpoint.verify == (action == "verify"))
            })
            .unwrap();
        let journal = serde_json::to_value(&record.commit_journal).unwrap();
        let stream = &journal["batches"]["nodes"][pipe.pipe_ref()];
        assert_eq!(
            body.get(0),
            Some(&json!(true)),
            "{action}: {body}; rows {actual:?}; audit {}; retired through {}; active phase {}; pending {}. Main log: {}{}",
            journal["audit"],
            stream["retired_through"],
            stream["active"]["phase"],
            record.commit_journal.has_pending(),
            file_tail(&root.path().join("main.log")),
            log_tails(root.path())
        );
        assert_eq!(actual, expected, "{action}");
        assert!(!record.commit_journal.has_pending());
        assert_eq!(
            journal["audit"]["affected_rows"],
            if action == "verify" { 3 } else { 2 }
        );
    }
}
