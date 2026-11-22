//! Saved pipe jobs: their public command, each registered window and schedule rearming.

use super::*;

pub(super) async fn saved_job_fixture(
    root: &std::path::Path,
    metric: &str,
) -> (
    meerschaum_connector_sql::SqlConnector,
    Pipe,
    Main,
    reqwest::Client,
    String,
    String,
) {
    let plugins = root.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let database = root.join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("job-owner", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom","scopes":["actions:execute","jobs:read","jobs:write","jobs:execute","jobs:stop","jobs:pause","pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", metric).instance("sql:main")
        .parameters(map(json!({"columns":{"datetime":"tick","id":"tick"},"dtypes":{"tick":"int64"},
            "target":metric.replace('-', "_"),"fetch":{"definition":"SELECT 4 AS tick UNION ALL SELECT 6 AS tick UNION ALL SELECT 8 AS tick"}})))
        .build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let (main, client, origin) = start_main(root, &plugins, &database).await;
    let token = login(&client, &origin, "job-owner").await;
    (instance, pipe, main, client, origin, token)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipe_jobs_keep_their_public_command_and_run_each_registered_window() {
    let root = main_root();
    let (instance, pipe, _main, client, origin, token) =
        saved_job_fixture(root.path(), "saved-ticks").await;
    let catalog = Catalog::new(root.path());
    for action in ["sync", "verify"] {
        let name = format!("saved-{action}");
        let url = format!("{origin}/jobs/{name}");
        let words = [
            action,
            "pipes",
            "-c",
            "sql:main",
            "-m",
            "saved-ticks",
            "-l",
            "None",
            "-i",
            "sql:main",
            "--begin=5",
            "--end=8",
        ];
        let mut previous = None;
        for revision in 1..=2 {
            let request = if revision == 1 {
                client.post(&url).json(&words)
            } else {
                client.post(format!("{url}/start"))
            };
            let (status, body) = response(request.bearer_auth(&token)).await;
            assert_eq!(status, 200, "{action} launch {revision}: {body}");
            assert_eq!(body[0], true, "{action} launch {revision}: {body}");
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let (status, body) = response(client.get(&url).bearer_auth(&token)).await;
                assert_eq!(status, 200, "{body}");
                assert_eq!(body["sysargs"], json!(words));
                if body["status"] == "stopped" && body["completion_pending"] != true {
                    assert_eq!(
                        body["result"][0],
                        true,
                        "{action}: {body} {}",
                        log_tails(root.path())
                    );
                    break;
                }
                assert!(Instant::now() < deadline, "{action} remains {body}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let record = catalog.get(&name).unwrap().unwrap();
            assert_eq!(record.context_revision, revision);
            assert!(record.owner_binding.is_some());
            assert!(record.callback.is_some());
            assert_ne!(record.callback, previous);
            previous = record.callback;
            let grant: Json = serde_json::from_slice(
                &std::fs::read(
                    root.path()
                        .join("executors/callbacks")
                        .join(format!("{}.json", record.callback.unwrap())),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(grant["required"], json!(["jobs:execute", "pipes:write"]));
            let data = instance
                .get_pipe_data(&pipe, &Default::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(data.height(), 1);
            assert_eq!(
                data.column("tick").unwrap().data.get(0),
                meerschaum_kernel::value::Value::Int(6)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipe_schedule_fires_with_fresh_windows_after_manual_rearm() {
    let root = main_root();
    let (instance, scheduled_pipe, _main, client, origin, token) =
        saved_job_fixture(root.path(), "scheduled-ticks").await;
    let catalog = Catalog::new(root.path());
    let name = "scheduled-sync";
    let url = format!("{origin}/jobs/{name}");
    let words = [
        "sync",
        "pipes",
        "-c",
        "sql:main",
        "-m",
        "scheduled-ticks",
        "-l",
        "None",
        "-i",
        "sql:main",
        "--begin=5",
        "--end=8",
        "--schedule=every 10 seconds",
    ];
    let client_properties = json!({
        "sysargs": words,
        "properties": {
            "label": meerschaum_core::shlex::join(&words.map(str::to_owned)),
            "mrsm_execution": {"kind":"persistent_job","origin":"user_job"}
        }
    });
    let (status, body) = response(
        client
            .post(&url)
            .json(&client_properties)
            .bearer_auth(&token),
    )
    .await;
    assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
    let mut previous_revision = 1;
    for run in 0..2 {
        if run > 0 {
            let (status, body) =
                response(client.post(format!("{url}/start")).bearer_auth(&token)).await;
            assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let record = catalog.get(name).unwrap().unwrap();
            let (status, body) = response(client.get(&url).bearer_auth(&token)).await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["sysargs"], json!(words));
            assert!(
                body["schedule"]["error"].is_null(),
                "{body} {}",
                log_tails(root.path())
            );
            if record.context_revision > previous_revision
                && body["result"][0] == true
                && body["schedule"]["last_fire"].is_string()
                && body["schedule"]["firing"] == false
            {
                previous_revision = record.context_revision;
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Scheduled graph: {body} {}",
                log_tails(root.path())
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        let (control, expected_status) = if run == 0 {
            ("pause", "paused")
        } else {
            ("stop", "stopped")
        };
        let (status, body) =
            response(client.post(format!("{url}/{control}")).bearer_auth(&token)).await;
        assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
        let (_, body) = response(client.get(&url).bearer_auth(&token)).await;
        assert!(body["schedule"]["next_fire"].is_null(), "{body}");
        assert_eq!(body["status"], expected_status);
        let data = instance
            .get_pipe_data(&scheduled_pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data.height(), 1);
        assert_eq!(
            data.column("tick").unwrap().data.get(0),
            meerschaum_kernel::value::Value::Int(6)
        );
    }
}
