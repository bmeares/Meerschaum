//! A strict Host operation receives only its accepted, API-served source transports.
#![cfg(unix)]

use meerschaum_cli::executor_jobs::Catalog;
use meerschaum_core::{Map, Pipe, Value, users::User};
use serde_json::{Value as Json, json};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
struct Main {
    child: Child,
    log: PathBuf,
}
impl Drop for Main {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .status();
        let deadline = Instant::now() + Duration::from_secs(8);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn tail(path: &Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    let Ok(metadata) = file.metadata() else {
        return String::new();
    };
    let _ = file.seek(SeekFrom::Start(metadata.len().saturating_sub(64 * 1024)));
    let mut text = String::new();
    let _ = file.take(64 * 1024).read_to_string(&mut text);
    text
}
async fn response(request: reqwest::RequestBuilder) -> (u16, Json) {
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or_else(|_| json!(text)),
    )
}
fn successful(body: &Json) -> bool {
    body.get("error").is_none()
        && body["result"]["isError"] != true
        && body["result"]["structuredContent"]["success"] == true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_host_operation_uses_only_accepted_served_source_labels_without_connector_read() {
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("host-selected-source-")
        .tempdir_in(parent)
        .unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let database = root.path().join("main.db");
    let source_database = root.path().join("second.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let second = meerschaum_connector_sql::SqlConnector::from_uri(
        "second",
        &format!("sqlite:///{}", source_database.display()),
    )
    .await
    .unwrap();
    assert!(second.exec("CREATE TABLE source_rows(id INTEGER,payload BLOB); INSERT INTO source_rows VALUES(7,X'010203')").await.is_some());
    let mut user = User::new("writer", Some("synthetic-source-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(json!({"role":"custom","scopes":["pipes:write"]})));
    assert!(instance.register_user(&mut user).await.success);
    let selected=Pipe::builder("sql:second","selected").instance("sql:main")
        .parameters(map(json!({"target":"selected_destination","columns":{"id":"id"},"dtypes":{"id":"int64"},"fetch":{"definition":"SELECT id,payload FROM source_rows"}})))
        .build().unwrap();
    assert!(instance.register_pipe(&selected).await.success);
    let sibling = Pipe::builder("sql:sibling", "denied")
        .instance("sql:main")
        .parameters(map(
            json!({"target":"denied_destination","fetch":{"definition":"SELECT 1 AS id"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&sibling).await.success);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let origin = format!("http://127.0.0.1:{port}");
    let patch = json!({"meerschaum":{"instance":"sql:main","executor":"local","connectors":{"sql":{
        "main":{"flavor":"sqlite","database":database},
        "second":{"flavor":"sqlite","database":source_database,"password":"synthetic-source-native-secret"},
        "unselected":{"flavor":"sqlite","database":root.path().join("unselected.db")},
        "sibling":{"flavor":"sqlite","database":root.path().join("sibling.db"),"teams":["Other"]}
    }}},"api":{"execution":{"callback_url":origin},"permissions":{"actions":{"non_admin":true},"instances":{"allow_multiple_instances":true,"allowed_instance_keys":["sql:*"]}}},"system":{"api":{"permissions":{"actions":{"non_admin":true}}}},"jobs":{"startup":{"enabled":false}}});
    let log = root.path().join("main.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env("MRSM_CONFIG", patch.to_string())
        .env(
            "MRSM_PYTHON",
            std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "/usr/bin/python3".into()),
        )
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .args(["start", "api", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .args(["--no-dash", "--no-webterm", "-w", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(std::fs::File::create(&log).unwrap()))
        .stderr(Stdio::from(
            std::fs::OpenOptions::new().append(true).open(&log).unwrap(),
        ));
    if let Some(path) = std::env::var_os("PYTHONPATH") {
        command.env("PYTHONPATH", path);
    }
    let mut main = Main {
        child: command.spawn().unwrap(),
        log,
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        if client
            .get(format!("{origin}/healthcheck"))
            .send()
            .await
            .is_ok_and(|reply| reply.status().is_success())
        {
            break;
        }
        assert!(
            main.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            tail(&main.log)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (status, login) = response(
        client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("username=writer&password=synthetic-source-pass"),
    )
    .await;
    assert_eq!(status, 200, "{login}");
    let token = login["access_token"].as_str().unwrap();
    for path in [
        "/connectors",
        "/pipes",
        "/pipes/sql:second/selected/None/attributes",
    ] {
        let (status, body) =
            response(client.get(format!("{origin}{path}")).bearer_auth(token)).await;
        assert_eq!(status, 403, "{path}: {body}");
    }
    let before = Catalog::new(root.path()).records().unwrap().len();
    let (status,denied)=response(client.post(format!("{origin}/mcp")).bearer_auth(token).json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sync_pipe","arguments":{"connector_keys":"sql:sibling","metric_key":"denied"}}}))).await;
    assert_eq!(status, 200, "{denied}");
    assert!(
        !successful(&denied),
        "A sibling source was admitted: {denied}"
    );
    assert_eq!(
        Catalog::new(root.path()).records().unwrap().len(),
        before,
        "A denied sibling operation launched a worker"
    );
    let (status,result)=response(client.post(format!("{origin}/mcp")).bearer_auth(token).json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"sync_pipe","arguments":{"connector_keys":"sql:second","metric_key":"selected"}}}))).await;
    assert_eq!(status, 200, "{result}");
    assert!(
        successful(&result),
        "Selected nondefault source failed: {result}\n{}",
        tail(&main.log)
    );
    let record = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|record| {
            record
                .operation
                .as_ref()
                .and_then(|operation| operation.endpoint())
                .is_some_and(|pipe| pipe.pipe_ref == selected.pipe_ref())
        })
        .unwrap();
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
    for denied in [
        "pipes:read",
        "sql:read",
        "connectors:read",
        "actions:execute",
        "jobs:execute",
    ] {
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!(denied)),
            "Tracking synthesized {denied}"
        );
    }
    let mut current = selected.clone();
    current.set_parameters(
        instance
            .get_pipe_parameters(&selected)
            .await
            .unwrap()
            .unwrap(),
    );
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(
        &instance
            .get_pipe_data(&current, &Default::default())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("payload"),
        Some(&meerschaum_kernel::value::Value::Bytes(vec![1, 2, 3]))
    );
}
