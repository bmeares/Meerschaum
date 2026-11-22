//! The native API client reads and records the same canonical numeric history as the server.

use super::*;

#[tokio::test]
async fn native_api_client_reads_and_records_the_same_canonical_numeric_history() {
    let fixture = fixture(true).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = APIConnector::new(
        Some("metrics_client"),
        map(json!({"host":"127.0.0.1", "port":port})),
    )
    .unwrap();
    let mut metric = SyncMetric::from_outcome(
        &fixture.public,
        &SyncOutcome::new(true, "", SyncSource::Python, None).with_counts(5, 0, 0),
    );
    metric.pipe_ref = "forged|keys|are|ignored".into();
    client.post_sync_metrics(&[metric]).await.unwrap();
    let result = client
        .get_remote_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.rows, 1007);
    assert!(
        result
            .history
            .iter()
            .all(|row| row.pipe_ref.starts_with("api:metrics_client|"))
    );
    // The native client records one logical sync, even when it posts multiple chunks.
    // An empty replacement must carry the same header or the server would add a duplicate.
    use meerschaum_connector::{DateBound, SyncOptions, pipe_ops, sync_status};
    use meerschaum_kernel::{
        enforce::{PipeInput, pipe_enforce_dtypes},
        value::Value as KValue,
    };
    let pipe = Pipe::builder("plugin:metrics", "protocol")
        .instance("api:metrics_client")
        .parameters(map(
            json!({"columns":{"datetime":"id"},"dtypes":{"id":"int"}}),
        ))
        .build()
        .unwrap();
    assert!(client.register_pipe(&pipe).await.success);
    InstanceConnector::get_pipe_id(&client, &pipe)
        .await
        .unwrap()
        .unwrap();
    let context = pipe_ops::dtypes_context(&client, &pipe, pipe.parameters())
        .await
        .unwrap();
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(vec![
            [("id".to_string(), KValue::Int(1))].into_iter().collect(),
            [("id".to_string(), KValue::Int(2))].into_iter().collect(),
        ]),
        &context,
    )
    .unwrap();
    let inserted = sync_status::recorded_scope(client.sync_pipe_counted(
        &pipe,
        frame.clone(),
        &SyncOptions {
            chunksize: Some(1),
            ..Default::default()
        },
    ))
    .await;
    assert!(inserted.success, "{}", inserted.message);
    let metric = SyncMetric::from_outcome(
        &pipe,
        &SyncOutcome::new(true, inserted.message, SyncSource::Python, None).with_counts(
            inserted.inserted,
            inserted.updated,
            inserted.upserted,
        ),
    );
    client
        .post_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    client
        .post_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    let replaced = sync_status::recorded_scope(client.sync_pipe_counted(
        &pipe,
        frame.take(&[]),
        &SyncOptions {
            replace: true,
            begin: Some(DateBound::Int(1)),
            end: Some(DateBound::Int(3)),
            ..Default::default()
        },
    ))
    .await;
    assert!(replaced.success, "{}", replaced.message);
    let mut outcome = SyncOutcome::new(true, replaced.message, SyncSource::Python, None)
        .with_counts(replaced.inserted, replaced.updated, replaced.upserted);
    outcome.deleted = replaced.deleted;
    client
        .post_sync_metrics(&[SyncMetric::from_outcome(&pipe, &outcome)])
        .await
        .unwrap();
    sync_status::flush(Duration::from_secs(4)).await;
    let result = client
        .get_remote_engine_metrics(&MetricsQuery {
            pipe_ref: Some(pipe.pipe_ref()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 2, "{result:?}");
    assert_eq!(result.summary.rows, 4);
    assert_eq!(result.summary.deleted, 2);

    let metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("api-daily"))
        .instance("sql:metrics")
        .parameters(map(
            json!({"pipeline":{"name":"api-daily", "id":"api-pipeline"}}),
        ))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&metadata).await.success);
    // Older hosts have an existing node table without the new registration columns.
    assert!(
        fixture
            .conn
            .exec("CREATE TABLE mrsm_engine_nodes (started TIMESTAMP, run_id TEXT, pipe_ref TEXT)")
            .await
            .is_some()
    );
    let started = (chrono::Utc::now() - chrono::TimeDelta::seconds(10)).to_rfc3339();
    let finished = (chrono::Utc::now() - chrono::TimeDelta::seconds(1)).to_rfc3339();
    let registration = meerschaum_connector::sync_metrics::observed_registration(&pipe).unwrap();
    for (run_id, reference, rows) in [
        ("api-run", pipe.pipe_ref(), 12),
        (
            "foreign-secret-run",
            "api:foreign|plugin:metrics|protocol|None".into(),
            900,
        ),
    ] {
        let runs = meerschaum_connector_api::engine_pipes::engine_frame("engine_runs",
            meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
                "run_id":run_id,"started":started,"finished":finished,"success":true,
                "targets":[reference],"num_nodes":1,
                "selection":json!({"pipeline_definitions":[{"name":"api-daily","id":"api-pipeline", "registry":"api:metrics_client"}]}).to_string()
            })])).unwrap();
        assert!(client.write_engine_pipe("engine_runs", &runs).await.success);
        let nodes = meerschaum_connector_api::engine_pipes::engine_frame(
            "engine_nodes",
            meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
                "run_id":run_id,"pipe_ref":reference,"registration_id":registration,
                "registration_ref":"sql:metrics|plugin:metrics|protocol|None",
                "started":started,"finished":finished,"status":"succeeded", "attempts":1,
                "inserted":rows
            })]),
        )
        .unwrap();
        let written = client.write_engine_pipe("engine_nodes", &nodes).await;
        assert!(written.success, "{}", written.message);
    }
    let result = client
        .get_remote_engine_metrics(&MetricsQuery {
            pipeline: Some("api-daily".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.pipelines.len(), 1, "{result:?}");
    assert_eq!(result.pipelines[0].summary.syncs, 1);
    assert_eq!(result.pipelines[0].summary.rows, 12);
    assert_eq!(result.pipelines[0].last_run_id.as_deref(), Some("api-run"));
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("foreign-secret")
    );
    let stale = meerschaum_connector_api::engine_pipes::engine_frame(
        "engine_nodes",
        meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
            "run_id":"api-run","pipe_ref":pipe.pipe_ref(),"registration_id":"old-registration",
            "started":started,"finished":finished,"status":"succeeded"
        })]),
    )
    .unwrap();
    assert!(
        !client
            .write_engine_pipe("engine_nodes", &stale)
            .await
            .success
    );
    let renamed = client
        .rename_pipeline(&metadata, "api-daily-renamed", None, Some("api-pipeline"))
        .await
        .unwrap();
    assert_eq!(
        renamed.get("id").and_then(Value::as_str),
        Some("api-pipeline")
    );
    assert_eq!(
        renamed.get("name").and_then(Value::as_str),
        Some("api-daily-renamed")
    );
    assert_eq!(
        renamed.get("registry").and_then(Value::as_str),
        Some("api:metrics_client")
    );
    assert_eq!(
        InstanceConnector::saved_pipeline_identity(&client, "api-daily-renamed")
            .await
            .unwrap(),
        Some(("api-pipeline".into(), "sql:metrics".into())),
        "history identity retains the exact server registry before public alias rebinding"
    );
    assert!(
        InstanceConnector::saved_pipeline_identity(&client, "missing-pipeline")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .conn
            .get_pipe_attributes(&metadata)
            .await
            .unwrap()
            .is_empty()
    );
    task.abort();
}
