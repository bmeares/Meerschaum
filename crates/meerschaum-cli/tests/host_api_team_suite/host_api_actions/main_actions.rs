//! Actions and pipe tools on the main API keep the caller's original scopes without running on
//! the main host.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_main_actions_and_pipe_tools_keep_original_scopes_without_main_execution() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let private = root.path().join("main-private.txt");
    std::fs::write(&private, "synthetic-main-file-private").unwrap();
    std::fs::write(plugins.join("boundregister.py"),format!(
        "import os\nfrom pathlib import Path\ndef register(pipe, **kw):\n    assert os.environ.get('MRSM_API_EXECUTION') == '1'\n    assert 'AWS_SECRET_ACCESS_KEY' not in os.environ\n    try:\n        Path({:?}).read_text()\n    except OSError:\n        pass\n    else:\n        raise RuntimeError('Main private file was readable')\n    return {{'columns':{{'id':'from_hook'}}, 'tags':['confined-register'], 'hook_pid':os.getpid()}}\ndef fetch(pipe, **kw):\n    return []\n",
        private.display().to_string())).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    for (name, scopes) in [
        ("action", vec!["actions:execute"]),
        ("direct", vec!["actions:execute", "pipes:write"]),
        ("writer", vec!["pipes:read", "pipes:write"]),
        ("writeonly", vec!["pipes:write"]),
        ("reader", vec!["pipes:read"]),
        ("registrar", vec!["pipes:register"]),
    ] {
        let mut user = User::new(name, Some("synthetic-scope-pass"));
        user.user_type = Some("regular".into());
        user.attributes = Some(map(json!({"role":"custom","scopes":scopes})));
        assert!(instance.register_user(&mut user).await.success);
    }
    let pipe = Pipe::builder("sql:main","scope-positive").instance("sql:main")
        .parameters(map(json!({"columns":{"id":"id"},"target":"scope_positive", "fetch":{"definition":"SELECT 1 AS id"}}))).build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let write_only = Pipe::builder("sql:main", "write-only").instance("sql:main")
        .parameters(map(json!({"columns":{"id":"id"},"dtypes":{"id":"int64"},"target":"write_only_destination", "fetch":{"definition":"SELECT 2 AS id, X'010203' AS payload"}}))).build().unwrap();
    assert!(instance.register_pipe(&write_only).await.success);
    let pipeline = Pipe::builder("mrsm","pipeline").location(Some("reader")).instance("sql:main")
        .parameters(map(json!({"pipeline":{"id":uuid::Uuid::new_v4().to_string(),"name":"reader","targets":[pipe.pipe_ref()],"graph":true}}))).build().unwrap();
    assert!(instance.register_pipe(&pipeline).await.success);
    let (main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let action = login(&client, &origin, "action").await;
    let writer = login(&client, &origin, "writer").await;
    let writeonly = login(&client, &origin, "writeonly").await;
    let reader = login(&client, &origin, "reader").await;
    let registrar = login(&client, &origin, "registrar").await;
    let (status, result) = response(
        client
            .post(format!("{origin}/actions/show"))
            .bearer_auth(&action)
            .json(&json!({"action":["version"]})),
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    let record = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|record| record.action_request)
        .unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert!(record.operation.is_none());
    let grant: Json = serde_json::from_slice(
        &std::fs::read(
            root.path()
                .join("executors/callbacks")
                .join(format!("{}.json", record.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grant["required"], json!(["actions:execute"]));
    assert!(
        !grant["permissions"]
            .as_array()
            .unwrap()
            .contains(&json!("jobs:execute"))
    );
    assert_eq!(
        response(
            client
                .post(format!("{origin}/jobs/forged"))
                .bearer_auth(&action)
                .json(&json!({"sysargs":["show","version"]}))
        )
        .await
        .0,
        403
    );
    let before = Catalog::new(root.path()).records().unwrap().len();
    let tables_before = meerschaum_connector::pipe_ops::frame_to_docs(
        &instance
            .read("SELECT name FROM sqlite_master ORDER BY name")
            .await
            .unwrap(),
    );
    let (status, planned) = response(
        client
            .post(format!("{origin}/pipelines/reader/plan"))
            .bearer_auth(&reader)
            .json(&json!({})),
    )
    .await;
    assert_eq!(status, 200, "{planned}");
    assert_eq!(planned["plan"]["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(
        Catalog::new(root.path()).records().unwrap().len(),
        before,
        "A reader plan created a job"
    );
    assert_eq!(
        meerschaum_connector::pipe_ops::frame_to_docs(
            &instance
                .read("SELECT name FROM sqlite_master ORDER BY name")
                .await
                .unwrap()
        ),
        tables_before,
        "Reader planning created state tables"
    );
    // Exercise MCP scope denial and registration independently of the pipe workers.
    assert_pipe_stats_denied(&client, &origin, &writeonly).await;
    let registered = tool(
        &client,
        &origin,
        &registrar,
        "register_pipe",
        json!({
            "connector_keys": "plugin:boundregister",
            "metric_key": "created",
            "columns": {"id": "caller_override"},
        }),
    )
    .await;
    assert!(
        tool_success(&registered),
        "A register-only source did not use its confined hook: {registered}{}",
        log_tails(root.path())
    );
    let registration = Pipe::builder("plugin:boundregister", "created")
        .instance("sql:main")
        .build()
        .unwrap();
    let attributes = instance.get_pipe_attributes(&registration).await.unwrap();
    let parameters = attributes["parameters"].as_map().unwrap();
    assert_eq!(
        parameters["columns"].get("id"),
        Some(&Value::Str("caller_override".into()))
    );
    assert_eq!(
        parameters["tags"],
        Value::List(vec!["confined-register".into()])
    );
    assert_ne!(
        parameters["hook_pid"].as_i64(),
        Some(main.child.id() as i64)
    );
    let canonical = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|record| record.connector_registration.is_some())
        .unwrap();
    assert_eq!(canonical.placement, JobPlacement::HostApi);
    let grant: Json = serde_json::from_slice(
        &std::fs::read(
            root.path()
                .join("executors/callbacks")
                .join(format!("{}.json", canonical.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grant["required"], json!(["pipes:register"]));
    for permission in ["actions:execute", "jobs:execute", "pipes:read"] {
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!(permission)),
            "Registration synthesized {permission}"
        );
    }

    for name in ["sync_pipe", "verify_pipe"] {
        let result = tool(
            &client,
            &origin,
            &writer,
            name,
            json!({"connector_keys":"sql:main","metric_key":"scope-positive"}),
        )
        .await;
        assert!(
            tool_success(&result),
            "{name}: {result}{}",
            log_tails(root.path())
        );
    }
    let operations: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|record| record.operation.is_some())
        .collect();
    assert_eq!(operations.len(), 2);
    for record in operations {
        assert_eq!(record.placement, JobPlacement::HostApi);
        assert!(!record.action_request);
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["pipes:write"]));
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("actions:execute"))
        );
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("jobs:execute"))
        );
    }
    for name in ["sync_pipe", "verify_pipe"] {
        let result = tool(
            &client,
            &origin,
            &writeonly,
            name,
            json!({"connector_keys":"sql:main","metric_key":"write-only"}),
        )
        .await;
        assert!(
            tool_success(&result),
            "Strict write-only {name}: {result}{}",
            log_tails(root.path())
        );
    }
    let strict: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|record| {
            record
                .operation
                .as_ref()
                .and_then(|operation| operation.endpoint())
                .is_some_and(|operation| operation.pipe_ref == write_only.pipe_ref())
        })
        .collect();
    assert_eq!(strict.len(), 2);
    let inferred = instance
        .get_pipe_parameters(&write_only)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inferred["dtypes"].as_map().unwrap().get("payload"),
        Some(&Value::Str("bytes".into())),
        "Main did not infer the actual new binary column"
    );
    for key in ["target", "fetch", "access"] {
        assert_eq!(
            inferred.get(key),
            write_only.raw_parameters().get(key),
            "Native inference changed protected {key}"
        );
    }
    let mut current = write_only.clone();
    current.set_parameters(inferred.clone());
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(
        &instance
            .get_pipe_data(&current, &Default::default())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        rows.len(),
        1,
        "Repeated sync/verify duplicated binary source data"
    );
    assert_eq!(
        rows[0].get("payload"),
        Some(&meerschaum_kernel::value::Value::Bytes(vec![1, 2, 3])),
        "Typed input was flattened into JSON text"
    );
    for (hints, expected) in [
        (json!({"payload":"sql:private"}), 400),
        (json!({"absent":"bytes"}), 400),
        (json!({"id":"bytes"}), 409),
    ] {
        let mut url =
            reqwest::Url::parse(&format!("{origin}/pipes/sql:main/write-only/None/data")).unwrap();
        url.query_pairs_mut()
            .append_pair("new_special_cols", &hints.to_string());
        let (status, body) = response(
            client
                .post(url)
                .bearer_auth(&writeonly)
                .json(&json!([{"id":3,"payload":"AQID"}])),
        )
        .await;
        assert_eq!(
            status, expected,
            "Invalid transport hints changed native metadata: {body}"
        );
    }
    assert_eq!(
        instance.get_pipe_parameters(&write_only).await.unwrap(),
        Some(inferred.clone()),
        "Refused hints adopted caller metadata"
    );
    for record in strict {
        assert!(
            record.operation.as_ref().unwrap().snapshot().is_some(),
            "Write-only work has no exact captured read capability"
        );
        let snapshot =
            serde_json::to_value(record.operation.as_ref().unwrap().snapshot().unwrap()).unwrap();
        assert_eq!(
            snapshot["members"][write_only.pipe_ref()]["parameters"],
            serde_json::to_value(Value::Map(inferred.clone())).unwrap(),
            "The canonical operation did not accept its own proven metadata transition"
        );
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["pipes:write"]));
        for permission in ["pipes:read", "sql:read", "actions:execute", "jobs:execute"] {
            assert!(
                !grant["permissions"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(permission)),
                "Write-only operation synthesized {permission}"
            );
        }
    }
    // The originating HTTP token cannot borrow the worker's private operation capability.
    for path in [
        "/pipes",
        "/pipes/sql:main/write-only/None/attributes",
        "/pipes/sql:main/write-only/None/data",
        "/pipes/sql:main/write-only/None/rowcount",
        "/pipes/sql:main/scope-positive/None/attributes",
    ] {
        assert_eq!(
            response(
                client
                    .get(format!("{origin}{path}"))
                    .bearer_auth(&writeonly)
            )
            .await
            .0,
            403,
            "Write-only token gained {path}"
        );
    }
    assert_pipe_stats_denied(&client, &origin, &writeonly).await;
    let denied = tool(
        &client,
        &origin,
        &writer,
        "execute_action",
        json!({"action":"show version"}),
    )
    .await;
    assert!(
        !tool_success(&denied),
        "Pipe scopes gained generic action authority: {denied}"
    );
    assert_eq!(
        response(
            client
                .post(format!("{origin}/jobs/forged-write"))
                .bearer_auth(&writer)
                .json(&json!({"sysargs":["show","version"],"properties":{"action_request":true}}))
        )
        .await
        .0,
        403
    );

    // Run after the registration case: this direct-action slice admits an actual
    // observed empty source set. The ordinary show/version action above used its original grant.
    std::fs::remove_file(plugins.join("boundregister.py")).unwrap();
    let selected = Pipe::builder("sql:main","direct-ticks").instance("sql:main")
        .parameters(map(json!({"columns":{"datetime":"tick","id":"tick"},
            "dtypes":{"tick":"int64"},"target":"direct_ticks",
            "fetch":{"definition":"SELECT 4 AS tick UNION ALL SELECT 6 AS tick UNION ALL SELECT 8 AS tick"}
        }))).build().unwrap();
    assert!(instance.register_pipe(&selected).await.success);
    let direct = login(&client, &origin, "direct").await;
    let args = json!({"action":["pipes"],"connector_keys":["sql:main"],"metric_keys":["direct-ticks"],
        "location_keys":["None"],"mrsm_instance":"sql:main","begin":5,"end":8});
    for operation in ["sync", "verify"] {
        let (status, body) = response(
            client
                .post(format!("{origin}/actions/{operation}"))
                .bearer_auth(&direct)
                .json(&args),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body.get(0),
            Some(&json!(true)),
            "{operation}: {body}{}",
            log_tails(root.path())
        );
    }
    let data = instance
        .get_pipe_data(&selected, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.height(), 1);
    assert_eq!(
        data.column("tick").unwrap().data.get(0),
        meerschaum_kernel::value::Value::Int(6)
    );
    let captured: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|r| {
            matches!(
                r.operation.as_ref(),
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            )
        })
        .collect();
    assert_eq!(captured.len(), 2);
    for record in captured {
        assert!(
            record.sysargs.contains(&"--begin=5".into())
                && record.sysargs.contains(&"--end=8".into())
        );
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["actions:execute", "pipes:write"]));
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("jobs:execute"))
        );
    }
    let (status, body) = response(
        client
            .post(format!("{origin}/actions/sync"))
            .bearer_auth(&action)
            .json(&args),
    )
    .await;
    assert!(
        status == 403 || (status == 200 && body.get(0) == Some(&json!(false))),
        "{body}"
    );
}
