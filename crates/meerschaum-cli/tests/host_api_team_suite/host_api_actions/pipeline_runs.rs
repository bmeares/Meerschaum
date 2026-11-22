//! Captured runs of saved Pipelines: revisions, explicit targets, replanning and watermarks.

use super::saved_jobs::saved_job_fixture;
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confined_pipeline_sync_uses_the_saved_revision_and_both_explicit_targets() {
    assert_captured_pipeline_run(PipelineRequest::Rest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_sync_pipeline_runs_the_saved_revision_and_both_explicit_targets() {
    assert_captured_pipeline_run(PipelineRequest::Mcp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_pipeline_requiring_a_new_plan_preserves_both_targets() {
    assert_captured_pipeline_run(PipelineRequest::Connected).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_records_its_prepared_replacement_watermark_after_delivery() {
    assert_captured_pipeline_run(PipelineRequest::Prepared).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_a_new_child_from_the_prepared_parent_watermark() {
    assert_captured_pipeline_run(PipelineRequest::PreparedChild).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_a_populated_child_after_parent_delivery() {
    assert_captured_pipeline_run(PipelineRequest::PopulatedChild).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_two_children_from_one_delivered_parent() {
    assert_captured_pipeline_run(PipelineRequest::SiblingChildren).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_bounds_a_child_by_both_committed_parents() {
    assert_captured_pipeline_run(PipelineRequest::TwoParents).await;
}

#[derive(Clone, Copy, PartialEq)]
enum PipelineRequest {
    Prepared,
    PreparedChild,
    PopulatedChild,
    SiblingChildren,
    TwoParents,
    Rest,
    Mcp,
    Connected,
}

impl PipelineRequest {
    fn dependent_children(self) -> bool {
        matches!(
            self,
            Self::PreparedChild | Self::PopulatedChild | Self::SiblingChildren | Self::TwoParents
        )
    }
    fn prepared_parent(self) -> bool {
        self == Self::Prepared || self.dependent_children()
    }
}

async fn assert_captured_pipeline_run(request: PipelineRequest) {
    let root = main_root();
    let (instance, mut first, _main, client, origin, token) =
        saved_job_fixture(root.path(), "pipeline-first").await;
    let instance = std::sync::Arc::new(instance);
    if request.prepared_parent() {
        first.update_parameters(map(json!({"fetch":{"replace":true}})));
        assert!(instance.edit_pipe(&first, false).await.success);
        let prior =
            meerschaum_kernel::frame::Frame::new(vec![meerschaum_kernel::frame::Column::object(
                "tick",
                vec![meerschaum_kernel::value::Value::Int(5)],
            )])
            .unwrap();
        let synced = instance.sync_pipe(&first, prior, &Default::default()).await;
        assert!(synced.success, "{}", synced.message);
    }
    if request == PipelineRequest::Mcp {
        // Earlier releases emitted this plain ID index when datetime and ID shared a column.
        for ddl in [
            r#"CREATE TABLE "pipeline_first" ("tick" BIGINT)"#,
            r#"CREATE INDEX None ON "pipeline_first" ("tick")"#,
        ] {
            assert!(instance.exec(ddl).await.is_some(), "{ddl}");
        }
    }
    let mut parameters = json!({"columns":{"datetime":"tick","id":"tick"}, "dtypes":{"tick":"int64"},
        "target":"pipeline_second", "fetch":{"definition":"SELECT 5 AS tick UNION ALL SELECT 7 UNION ALL SELECT 8"}});
    if request == PipelineRequest::Connected || request.dependent_children() {
        parameters["parents"] = json!([{"connector":"sql:main","metric":"pipeline-first"}]);
        parameters["fetch"]["definition"] = json!("SELECT tick FROM pipeline_first");
        if request == PipelineRequest::TwoParents {
            parameters["parents"]
                .as_array_mut()
                .unwrap()
                .push(json!({"connector":"sql:main","metric":"pipeline-third"}));
            parameters["fetch"]["definition"] =
                json!("SELECT tick FROM pipeline_first UNION SELECT tick FROM pipeline_third");
        }
    }
    let second = Pipe::builder("sql:main", "pipeline-second")
        .instance("sql:main")
        .parameters(map(parameters))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&second).await.success);
    if request == PipelineRequest::PopulatedChild {
        let existing =
            meerschaum_kernel::frame::Frame::new(vec![meerschaum_kernel::frame::Column::object(
                "tick",
                vec![meerschaum_kernel::value::Value::Int(4)],
            )])
            .unwrap();
        let result = instance
            .sync_pipe(&second, existing, &Default::default())
            .await;
        assert!(result.success, "{}", result.message);
    }
    let mut targets = vec![first.pipe_ref(), second.pipe_ref()];
    let third = if matches!(
        request,
        PipelineRequest::SiblingChildren | PipelineRequest::TwoParents
    ) {
        let mut parameters = if request == PipelineRequest::TwoParents {
            let mut parameters = first.raw_parameters().clone();
            parameters.insert("fetch".into(),map(json!({"replace":true, "definition":"SELECT 5 AS tick UNION ALL SELECT 7 AS tick"})).into());
            parameters
        } else {
            second.raw_parameters().clone()
        };
        parameters.insert("target".into(), "pipeline_third".into());
        let pipe = Pipe::builder("sql:main", "pipeline-third")
            .instance("sql:main")
            .parameters(parameters)
            .build()
            .unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
        if request == PipelineRequest::TwoParents {
            let seed = meerschaum_kernel::frame::Frame::new(vec![
                meerschaum_kernel::frame::Column::object(
                    "tick",
                    vec![meerschaum_kernel::value::Value::Int(5)],
                ),
            ])
            .unwrap();
            let synced = instance.sync_pipe(&pipe, seed, &Default::default()).await;
            assert!(synced.success, "{}", synced.message);
        }
        targets.push(pipe.pipe_ref());
        Some(pipe)
    } else {
        None
    };
    let api_name = "both-tick-streams";
    let definition = json!({"id":uuid::Uuid::new_v4().to_string(), "name":api_name,
        "registry":"sql:main", "revision":3, "schema_version":2,
        "graph":true, "targets":targets});
    let pipeline = Pipe::builder("mrsm", "pipeline")
        .location(Some(api_name))
        .instance("sql:main")
        .parameters(map(json!({"pipeline":definition})))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipeline).await.success);
    let reply = if request == PipelineRequest::Mcp {
        let message = tool(
            &client,
            &origin,
            &token,
            "sync_pipeline",
            json!({"name":api_name,"begin":5,"end":8}),
        )
        .await;
        assert!(tool_success(&message), "{message}");
        let result: Json =
            serde_json::from_str(message["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert!(result["run_id"].as_str().unwrap().contains("-api-"));
        result
    } else {
        let run_id = "saved-tick-streams-12";
        let (status, reply) = response(
            client
                .post(format!("{origin}/pipelines/{api_name}/sync"))
                .bearer_auth(&token)
                .json(&json!({"run_id":run_id,"begin":5,"end":8})),
        )
        .await;
        if request == PipelineRequest::Connected {
            assert!(status >= 400, "{reply}");
            assert!(
                reply.to_string().contains("entire parent window"),
                "{reply}"
            );
            assert!(!instance.pipe_exists(&first).await.unwrap());
            assert!(!instance.pipe_exists(&second).await.unwrap());
            return;
        }
        assert_eq!(
            status,
            200,
            "{reply} {}",
            file_tail(&root.path().join("main.log"))
        );
        assert_eq!(reply["run_id"], run_id);
        reply
    };
    let run_id = reply["run_id"].as_str().unwrap();
    let name = reply["job"].as_str().unwrap();
    use meerschaum_server::backends::{EngineBackend, RunsFilter};
    let history =
        meerschaum_server::engine_store::InstanceEngine::new(instance.clone(), "sql:main");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let (status, job) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(&token),
        )
        .await;
        assert_eq!(status, 200, "{job}");
        if job["status"] == "stopped" && job["completion_pending"] != true {
            if job["result"][0] != true {
                let rows = history
                    .get_runs(&RunsFilter {
                        run_id: Some(run_id.into()),
                        ..Default::default()
                    })
                    .await
                    .unwrap_or_default();
                for row in rows {
                    eprintln!(
                        "Pipeline node {:?}: {} {:?} {:?}",
                        row.pipe_ref,
                        row.status,
                        row.extra.get("message"),
                        row.extra.get("reason")
                    );
                }
                let record = Catalog::new(root.path()).get(name).unwrap().unwrap();
                let journal = serde_json::to_value(&record.commit_journal).unwrap();
                if let Some(completed) = journal["completed_replacements"].as_object() {
                    for (name, value) in completed {
                        eprintln!(
                            "Pipeline completed source {name}: watermark {}",
                            value["source"]["watermark"]
                        );
                    }
                }
            }
            assert_eq!(
                job["result"][0],
                true,
                "{job} {} {}",
                file_tail(&root.path().join("main.log")),
                log_tails(root.path())
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{job} {}",
            log_tails(root.path())
        );
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    let record = Catalog::new(root.path()).get(name).unwrap().unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert_eq!(
        record.sysargs,
        [
            "sync",
            "pipelines",
            api_name,
            "-i",
            "sql:main",
            "--run-id",
            run_id,
            "--begin",
            "5",
            "--end",
            "8"
        ]
    );
    let (saved, mut roots) = record
        .operation
        .as_ref()
        .unwrap()
        .pipeline_bootstrap("sql:main", api_name, run_id)
        .unwrap();
    assert_eq!(saved["id"], definition["id"]);
    assert_eq!(saved["revision"], 3);
    roots.sort();
    targets.sort();
    assert_eq!(roots, targets);
    assert!(!record.commit_journal.has_pending());
    let journal = serde_json::to_value(&record.commit_journal).unwrap();
    if request.prepared_parent() {
        let evidence = &journal["completed_replacements"][first.pipe_ref()]["source"];
        assert_eq!(evidence["watermark"], 6, "{journal}");
        assert_eq!(evidence["rows"], 1);
        assert_eq!(evidence["snapshot_sha256"].as_str().unwrap().len(), 64);
        assert!(
            journal["completed_replacements"]
                .get(second.pipe_ref())
                .is_none()
        );
    }
    let expected_second = if request.dependent_children() {
        let instructions = record
            .operation
            .as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .main_instructions()
            .unwrap();
        let child = instructions.planned_node(&second.pipe_ref()).unwrap();
        assert_eq!(child["action"], "incremental");
        assert_ne!(child["provisional"], true);
        assert_eq!(child["bounds"], json!({"begin":5,"end":7}));
        if request == PipelineRequest::PopulatedChild {
            assert_eq!(child["watermark"], 4);
            vec![4, 6]
        } else if request == PipelineRequest::TwoParents {
            vec![5, 6]
        } else {
            vec![6]
        }
    } else {
        vec![5, 7]
    };
    let mut expected_rows = vec![(&first, vec![6_i64]), (&second, expected_second)];
    if let Some(third) = &third {
        let instructions = record
            .operation
            .as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .main_instructions()
            .unwrap();
        let node = instructions.planned_node(&third.pipe_ref()).unwrap();
        assert_ne!(node["provisional"], true);
        if request == PipelineRequest::TwoParents {
            assert_eq!(node["action"], "root");
            assert_eq!(
                journal["completed_replacements"][third.pipe_ref()]["source"]["watermark"],
                7
            );
            expected_rows.push((third, vec![5, 7]));
        } else {
            assert_eq!(node["bounds"], json!({"begin":5,"end":7}));
            expected_rows.push((third, vec![6]));
        }
    }
    for (pipe, expected) in expected_rows {
        let data = instance
            .get_pipe_data(pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        let tick = &data.column("tick").unwrap().data;
        let mut actual: Vec<_> = (0..data.height())
            .map(|row| match tick.get(row) {
                meerschaum_kernel::value::Value::Int(value) => value,
                other => panic!("unexpected tick {other:?}"),
            })
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{}", pipe.pipe_ref());
        assert_eq!(
            journal["batches"]["nodes"][pipe.pipe_ref()]["retired_through"],
            1
        );
    }
    let records = history
        .get_runs(&RunsFilter {
            run_id: Some(run_id.into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let header = records
        .iter()
        .find(|row| row.pipe_ref.is_none())
        .expect("The saved run must publish a header under the requested run ID.");
    assert_eq!(header.run_id, run_id);
    assert_eq!(header.status, "success");
    let saved_selection = &header.extra["selection"];
    let selection: Json = match saved_selection {
        Value::Str(text) => serde_json::from_str(text).unwrap(),
        value => serde_json::to_value(value).unwrap(),
    };
    assert_eq!(selection["pipeline_definitions"][0]["id"], definition["id"]);
    assert_eq!(selection["pipeline_definitions"][0]["revision"], 3);
}
