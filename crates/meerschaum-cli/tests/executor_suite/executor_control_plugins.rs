//! The credentialed control process never imports, dlopens or dispatches a granted plugin.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::gateway::{CONFIGURATION_PATH, Configuration};
use serde_json::json;
use uuid::Uuid;

struct Signer {
    authority: Authority,
    revision: Mutex<String>,
}
impl RequestAuthentication for Signer {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        self.authority
            .issue(
                Actor {
                    owner: "operator".into(),
                    username: Some("operator".into()),
                    permissions: Default::default(),
                    administrator: false,
                },
                meerschaum_server::executor::control::Request {
                    method: request.method,
                    path: request.path,
                    body: request.body.ok_or_else(|| {
                        ConnectorError::Invalid("Unbuffered fixture request.".into())
                    })?,
                },
                &self.revision.lock().unwrap(),
                chrono::Utc::now().timestamp(),
            )
            .map(|token| format!("Bearer {token}"))
            .map_err(|_| ConnectorError::Invalid("Could not sign fixture request.".into()))
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = meerschaum_jobs::process::send_signal(
            self.0.id() as i32,
            meerschaum_jobs::process::Sig::Term,
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.0.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn core(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
fn no_import(root: &Path) {
    for marker in [
        "python-imported",
        "site-imported",
        "native-loaded",
        "inline-executed",
    ] {
        assert!(
            !root.join(marker).exists(),
            "Credentialed executor executed {marker}."
        );
    }
}
async fn configure(api: &APIConnector, signer: &Signer, configuration: &Configuration) -> u16 {
    *signer.revision.lock().unwrap() = configuration.fingerprint().unwrap();
    api.make_request(
        Method::Put,
        CONFIGURATION_PATH,
        RequestOptions {
            body: Body::Json(serde_json::to_string(configuration).unwrap()),
            timeout: Some(Duration::from_secs(20)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_configuration_and_inline_routes_never_load_granted_python_site_or_native_code() {
    let root = tempfile::tempdir().unwrap();
    let control = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    let site = root.path().join("hostile-site");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::create_dir(&site).unwrap();
    std::fs::write(
        site.join("sitecustomize.py"),
        format!(
            "from pathlib import Path\nPath({:?}).write_text('site')\n",
            root.path().join("site-imported")
        ),
    )
    .unwrap();
    std::fs::write(plugins.join("raising_probe.py"), format!("from pathlib import Path\nPath({:?}).write_text('imported')\nraise RuntimeError('synthetic import must never occur in control')\n", root.path().join("python-imported"))).unwrap();
    let native = plugins.join("native_probe");
    std::fs::create_dir(&native).unwrap();
    std::fs::write(
        native.join("plugin.toml"),
        "name='native_probe'\nversion='0.0.0'\n",
    )
    .unwrap();
    let source = root.path().join("native_probe.c");
    std::fs::write(&source, format!("#include <stdio.h>\n__attribute__((constructor)) static void probe(void) {{ FILE *f=fopen({:?}, \"w\"); if(f){{ fputs(\"loaded\",f); fclose(f); }} }}\nvoid *mrsm_plugin_v1(void) {{ return 0; }}\n", root.path().join("native-loaded").display().to_string())).unwrap();
    let compiled = Command::new("cc")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(native.join("libnative_probe.so"))
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    // Persisted host metadata must not start an extension host or load user routes either.
    std::fs::create_dir(root.path().join("cache")).unwrap();
    std::fs::write(root.path().join("cache/plugins_manifest.json"), json!({
        "api_plugins":["raising_probe"], "dash_plugins":["raising_probe"], "web_pages":["raising_probe"], "mcp_tools":["raising_probe"], "console":true
    }).to_string()).unwrap();
    let team_id = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let (authority, _) = Authority::generate(Uuid::new_v4(), team_id, generation).unwrap();
    let bootstrap = control.path().join("bootstrap.json");
    std::fs::write(
        &bootstrap,
        serde_json::to_vec(&authority.verifier()).unwrap(),
    )
    .unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let log_path = root.path().join("launcher.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    let python_path =
        std::env::join_paths([site, Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")]).unwrap();
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env("MRSM_EXECUTOR_BOOTSTRAP", &bootstrap)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("PYTHONPATH", python_path)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    if let Some(python) = std::env::var_os("MRSM_PYTHON") {
        command.env("MRSM_PYTHON", python);
    }
    command.args([
        "start",
        "executor",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--control-root",
        control.path().to_str().unwrap(),
    ]);
    drop(reservation);
    let mut process = Process(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            process.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    no_import(root.path());
    let mut configuration = Configuration {
        catalog_sha256: None,
        team_id,
        generation,
        team_name: "ControlFixture".into(),
        plugins: ["raising_probe".into(), "native_probe".into()]
            .into_iter()
            .collect(),
        config: core(
            json!({"meerschaum":{"instance":"sql:executor","api_instance":"sql:executor","connectors":{"sql":{"executor":{"flavor":"sqlite","database":root.path().join("executor.db")}}}},
            "system":{"packages":{"auto_install":false}},"api":{"no_dash":false}}),
        ),
    };
    let signer = Arc::new(Signer {
        authority,
        revision: Mutex::new(configuration.fingerprint().unwrap()),
    });
    let api = APIConnector::new(
        Some("control-fixture"),
        core(json!({"host":"127.0.0.1","port":port})),
    )
    .unwrap()
    .with_request_authentication(signer.clone(), 1024 * 1024)
    .unwrap();
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    no_import(root.path());
    let actions = api
        .get("/actions", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(actions.status, 503, "{}", actions.text());
    assert!(actions.text().contains("Confined plugin action metadata"));
    for action in ["show", "raising_probe"] {
        let response = api
            .post(
                &format!("/actions/{action}"),
                RequestOptions {
                    body: Body::Json(json!({"action":["version"]}).to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200, "{}", response.text());
        assert_eq!(response.json().unwrap()[0], false);
    }
    let shell = api.post("/actions/sh",RequestOptions {
        body:Body::Json(json!({"action":[format!("touch {}",root.path().join("inline-executed").display())]}).to_string()),..Default::default()
    }).await.unwrap();
    assert_eq!(shell.json().unwrap()[0], false);
    assert_eq!(
        api.get("/dash/", RequestOptions::default())
            .await
            .unwrap()
            .status,
        503
    );
    assert_eq!(
        api.get("/jobs", RequestOptions::default())
            .await
            .unwrap()
            .status,
        200
    );
    no_import(root.path());
    configuration.plugins.clear();
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    let core_actions = api
        .get("/actions", RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(core_actions.status, 200);
    assert!(
        core_actions
            .json()
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action == "show")
    );
    no_import(root.path());
    configuration.config.insert(
        "meerschaum".into(),
        Value::Map(core(
            json!({"instance":"raising_probe:main","api_instance":"raising_probe:main"}),
        )),
    );
    assert_eq!(configure(&api, &signer, &configuration).await, 503);
    no_import(root.path());
    drop(process);
    no_import(root.path());
}
