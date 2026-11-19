//! A real native Main, console session store, websocket, private tmux and API-only guardian.
//! This custom harness is also the compiled native entry point used by its confined children.
#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    fixture::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
mod common;

#[cfg(target_os = "linux")]
mod fixture {

    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitCode, Stdio};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures_util::{SinkExt, StreamExt};
    use meerschaum_cli::executor_client::Controller;
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::access::{Team, TeamRole};
    use meerschaum_core::config::ConfigOptions;
    use meerschaum_core::users::User;
    use meerschaum_core::{Map, SuccessTuple};
    use meerschaum_server::access::Access;
    use meerschaum_server::backends::{ActionTarget, ActionsBackend};
    use meerschaum_server::webterm::remote::Terminal;
    use meerschaum_web::sessions::SessionStore;
    use serde_json::{Value as Json, json};
    use tokio_tungstenite::tungstenite::Message;
    use uuid::Uuid;

    const SERVER: &str = "__host_api_terminal_fixture_server__";
    const TEST: &str = "real_host_api_terminals_keep_original_source_and_private_native_runtime";
    const PASSWORD: &str = "synthetic-private-terminal-password";
    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    pub(super) fn main() -> ExitCode {
        let arguments: Vec<_> = std::env::args().skip(1).collect();
        if arguments.first().is_some_and(|argument| argument == SERVER) {
            meerschaum_core::self_exe::register_native_current_executable().unwrap();
            runtime().block_on(serve(PathBuf::from(&arguments[1])));
            return ExitCode::SUCCESS;
        }
        if std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
            || arguments
                .first()
                .is_some_and(|argument| argument.starts_with("__mrsm_"))
        {
            return meerschaum_cli::run_main();
        }
        if arguments.iter().any(|argument| argument == "--list") {
            println!("{TEST}: test");
            return ExitCode::SUCCESS;
        }
        let exact = arguments.iter().any(|argument| argument == "--exact");
        let ignored = arguments.iter().any(|argument| argument == "--ignored")
            && !arguments
                .iter()
                .any(|argument| argument == "--include-ignored");
        let mut values = arguments.iter();
        let mut selected = true;
        while let Some(argument) = values.next() {
            if matches!(argument.as_str(), "--format" | "--test-threads" | "--color") {
                let _ = values.next();
            } else if !argument.starts_with('-') {
                selected &= if exact {
                    argument == TEST
                } else {
                    TEST.contains(argument)
                };
            }
        }
        if ignored || !selected {
            return ExitCode::SUCCESS;
        }
        runtime().block_on(acceptance());
        ExitCode::SUCCESS
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    use super::common::json_map as map;

    struct Actions(Arc<Controller>);
    #[async_trait::async_trait]
    impl ActionsBackend for Actions {
        fn api_execution(&self) -> meerschaum_server::backends::ApiExecution {
            meerschaum_server::backends::ApiExecution::Tracked { terminals: true }
        }
        fn list(&self) -> Vec<String> {
            vec!["show".into()]
        }
        async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
            SuccessTuple::fail(
                "This fixture only dispatches the private terminal's native commands.",
            )
        }
        async fn resolve(&self, step: &[String]) -> ActionTarget {
            let pair = step.iter().take(2).map(String::as_str).collect::<Vec<_>>();
            let plugin = match pair.as_slice() {
                ["terminal", "probe"] => Some("terminal_probe"),
                ["new", "probe"] => Some("new_probe"),
                _ => None,
            };
            if let Some(plugin) = plugin {
                ActionTarget::Action {
                    name: plugin.into(),
                    plugin: Some(plugin.into()),
                    sub: None,
                }
            } else if step.first().map(String::as_str) == Some("show") {
                ActionTarget::Action {
                    name: "show".into(),
                    plugin: None,
                    sub: step.get(1).cloned(),
                }
            } else {
                ActionTarget::Shell
            }
        }
        async fn host_api_terminal(
            &self,
            team: Option<Uuid>,
            caller: &Access,
        ) -> Result<Terminal, String> {
            self.0
                .host_terminal(team, caller)
                .await
                .inspect_err(|error| {
                    eprintln!("The owned host terminal fixture could not prepare: {error}");
                })
        }
        async fn revoke_host_api_terminal(
            &self,
            team: Option<Uuid>,
            owner: &str,
            credential: &str,
        ) {
            self.0.revoke_host_terminal(team, owner, credential).await;
        }
    }

    async fn serve(root: PathBuf) {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("mrsm::terminal_transport=debug")
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .try_init();
        let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        assert!(std::env::var_os("PYTHONPATH").is_some_and(|value| value == sdk));
        assert!(sdk.join("meerschaum/__init__.py").is_file());
        assert!(
            std::env::var("DB_PASSWORD").as_deref() == Ok("synthetic-main-terminal-env-secret")
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.join("main.db").display()),
            )
            .await
            .unwrap(),
        );
        let options = ConfigOptions::with_env(std::env::vars().collect());
        let controller = Arc::new(Controller::new(
            root.clone(),
            instance.clone(),
            options,
            Map::new(),
        ));
        controller.bind_host_terminals().unwrap();
        let mut config = meerschaum_server::ServerConfig::for_root("sql:main", &root);
        config.execution_callback_url = Some(format!("http://{address}"));
        config.actions_non_admin = true;
        config.no_dash = false;
        config.no_webterm = false;
        config.webterm_tmux_enabled = true;
        config.connectors = map(json!({"sql":{"main":{
            "flavor":"sqlite", "database":root.join("main.db"),
            "password":"synthetic-main-terminal-database-secret"
        }}}));
        let state = meerschaum_server::AppState::builder(config, instance.clone())
            .actions(Arc::new(Actions(controller.clone())))
            .build()
            .unwrap();
        controller.bind_state(&state).unwrap();
        tokio::spawn(controller.clone().maintain());
        let sessions = Arc::new(meerschaum_web::sessions::InstanceSessions::new(instance));
        let web = meerschaum_web::WebConfig {
            sessions,
            webterm: Some(meerschaum_web::WebtermSettings::new(
                meerschaum_server::webterm::ShellCommand::new("/nonexistent/ambient-host-fallback"),
            )),
            pages: Vec::new(),
        };
        let socket = meerschaum_server::webterm::restricted_socket(
            &meerschaum_server::webterm::tmux_socket_path(&state.config)
                .unwrap()
                .with_extension("host-api"),
        );
        let router = meerschaum_web::app(state.clone(), web).unwrap();
        meerschaum_core::team_scope::write_private(
            &root.join("ready.json"),
            &json!({"address":address.to_string(),"secret":state.secret(),"socket":socket})
                .to_string(),
        )
        .unwrap();
        axum::serve(listener, router).await.unwrap();
    }

    struct Owner {
        child: Child,
        socket: Option<PathBuf>,
        diagnostic: PathBuf,
    }
    impl Owner {
        fn kill(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            if std::thread::panicking() {
                if let Some(root) = self.diagnostic.parent() {
                    runtime_diagnostics(root);
                }
                if let Some(socket) = self.socket.as_ref() {
                    let listing = tmux(socket, &["list-panes", "-a", "-F", "#{pane_id}"]);
                    for pane in String::from_utf8_lossy(&listing.stdout).lines().take(8) {
                        if !pane.strip_prefix('%').is_some_and(|value| {
                            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                        }) {
                            continue;
                        }
                        let output = tmux(socket, &["capture-pane", "-p", "-t", pane, "-S", "-50"]);
                        let text = String::from_utf8_lossy(&output.stdout);
                        let tail = text
                            .chars()
                            .rev()
                            .take(4096)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect::<String>();
                        let tail =
                            meerschaum_cli::executor_jobs::LogSanitizer::default().push(&tail);
                        eprintln!(
                            "Owned terminal {pane} history before cleanup: {}",
                            meerschaum_server::redact::mask_uri_passwords(&tail)
                        );
                    }
                }
            }
            self.kill();
            if let Some(socket) = self.socket.as_ref() {
                let _ = tmux(socket, &["kill-server"]);
            }
            if std::thread::panicking()
                && let Ok(bytes) = std::fs::read(&self.diagnostic)
            {
                let tail = &bytes[bytes.len().saturating_sub(16 * 1024)..];
                eprintln!(
                    "Owned Main terminal diagnostics: {}",
                    String::from_utf8_lossy(tail)
                );
            }
        }
    }

    fn runtime_diagnostics(root: &Path) {
        let Ok(entries) = std::fs::read_dir(root.join("execution-contexts")) else {
            return;
        };
        for entry in entries.flatten().take(8) {
            let id = entry.file_name();
            if id
                .to_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .is_none()
            {
                continue;
            }
            let mut directories = vec![entry.path()];
            if let Ok(children) = std::fs::read_dir(entry.path()) {
                for child in children.flatten().take(8) {
                    if child
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("generation-"))
                        && child.file_type().is_ok_and(|kind| kind.is_dir())
                    {
                        directories.push(child.path());
                    }
                }
            }
            for directory in directories {
                if let Ok(bytes) = std::fs::read(directory.join("credential.json"))
                    && let Ok(value) = serde_json::from_slice::<Json>(&bytes)
                {
                    eprintln!(
                        "Owned terminal lease before cleanup: {}",
                        json!({"context":id.to_string_lossy(),"expires_at":value["expires_at"],"now":chrono::Utc::now().timestamp()})
                    );
                }
                let Ok(files) = std::fs::read_dir(directory) else {
                    continue;
                };
                for file in files.flatten().take(32) {
                    let name = file.file_name();
                    let Some(name) = name.to_str() else {
                        continue;
                    };
                    if !name.starts_with("terminal-") || !name.ends_with(".json") {
                        continue;
                    }
                    let Ok(metadata) = std::fs::symlink_metadata(file.path()) else {
                        continue;
                    };
                    if !metadata.is_file() || metadata.len() > 16 * 1024 {
                        continue;
                    }
                    let Ok(bytes) = std::fs::read(file.path()) else {
                        continue;
                    };
                    let Ok(value) = serde_json::from_slice::<Json>(&bytes) else {
                        continue;
                    };
                    let live = |key: &str| {
                        serde_json::from_value::<meerschaum_jobs::supervisor::LifecycleOwner>(
                            value[key].clone(),
                        )
                        .ok()
                        .map(|owner| owner.alive())
                    };
                    eprintln!(
                        "Owned terminal lifecycle before cleanup: {}",
                        json!({"context":id.to_string_lossy(),"kind":if name.starts_with("terminal-launch-"){"pending"}else{"guardian"},"owner":value["owner"],"guardian":value["guardian"],"child":value["child"],"cleanup_complete":value["cleanup_complete"],"owner_alive":live("owner"),"guardian_alive":live("guardian"),"child_alive":live("child")})
                    );
                }
            }
        }
    }

    fn tmux(socket: &Path, arguments: &[&str]) -> std::process::Output {
        Command::new("tmux")
            .arg("-S")
            .arg(socket)
            .args(arguments)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap()
    }

    fn panes(socket: &Path) -> Vec<(i32, chrono::NaiveDateTime)> {
        let output = tmux(socket, &["list-panes", "-a", "-F", "#{pane_pid}"]);
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| {
                let pid = line.parse().ok()?;
                Some((pid, meerschaum_jobs::process::process_start_time(pid)?))
            })
            .collect()
    }

    fn alive(identity: (i32, chrono::NaiveDateTime)) -> bool {
        meerschaum_jobs::process::process_start_time(identity.0) == Some(identity.1)
    }

    async fn wait_until(message: &str, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready() {
            assert!(Instant::now() < deadline, "{message}");
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    fn records(root: &Path) -> Json {
        serde_json::from_slice(
            &std::fs::read(root.join("executors/host-terminals/catalog.json")).unwrap(),
        )
        .unwrap()
    }

    fn record(root: &Path, name: &str) -> Json {
        records(root)
            .as_array()
            .unwrap()
            .iter()
            .find(|record| record["grant"]["owner"] == name)
            .unwrap()
            .clone()
    }

    fn has_terminal_record(root: &Path, id: &Json) -> bool {
        records(root)
            .as_array()
            .unwrap()
            .iter()
            .any(|record| record["grant"]["id"] == *id)
    }

    fn generation(root: &Path, record: &Json) -> PathBuf {
        let launch = Uuid::parse_str(record["grant"]["id"].as_str().unwrap()).unwrap();
        let base = root
            .join("execution-contexts")
            .join(launch.simple().to_string());
        let mut found = std::fs::read_dir(base)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("generation-")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            found.len(),
            1,
            "Initial terminal must select one sealed generation."
        );
        found.pop().unwrap()
    }

    async fn session(
        client: &reqwest::Client,
        origin: &str,
        secret: &[u8],
        sessions: &dyn SessionStore,
        name: &str,
        password: &str,
    ) -> String {
        let response = client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("username={name}&password={password}"))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let login: Json = response.json().await.unwrap();
        let claims = meerschaum_server::auth::jwt_decode(
            login["access_token"].as_str().unwrap(),
            secret,
            chrono::Utc::now(),
        )
        .unwrap();
        let id = Uuid::new_v4().to_string();
        sessions
            .set(
                &id,
                json!({"username":name,"credential":claims["mrsm_credential"]}),
                300,
            )
            .await
            .unwrap();
        id
    }

    async fn connect(address: &str, session: &str) -> Socket {
        let (socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/websocket/{session}"))
                .await
                .unwrap();
        socket
    }

    async fn send(socket: &mut Socket, value: Json, stage: &'static str) {
        if value[0] == "stdin" {
            shell_state(socket, stage).await;
        }
        socket
            .send(Message::Text(value.to_string().into()))
            .await
            .unwrap_or_else(|error| panic!("Terminal {stage}: sending input failed: {error}"));
    }

    async fn until_output(
        socket: &mut Socket,
        stage: &'static str,
        expected: impl Fn(&str) -> bool,
    ) -> String {
        let mut output = String::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let message = socket
                    .next()
                    .await
                    .unwrap_or_else(|| panic!("Terminal {stage}: the transport ended."))
                    .unwrap_or_else(|error| {
                        panic!("Terminal {stage}: receiving output failed: {error}")
                    });
                assert!(
                    !matches!(message, Message::Close(_)),
                    "Terminal {stage}: the transport closed while waiting for output."
                );
                if let Message::Text(raw) = message {
                    let value: Json = serde_json::from_str(&raw).unwrap();
                    if value[0] == "stdout" {
                        output.push_str(value[1].as_str().unwrap());
                    }
                    assert!(
                        output.len() < 512 * 1024,
                        "Terminal output exceeded the fixture bound."
                    );
                    if expected(&output) {
                        return;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("Terminal {stage}: expected output did not arrive: {output:?}"));
        output
    }

    async fn version(socket: &mut Socket, stage: &'static str) {
        eprintln!("Host terminal lifecycle stage: {stage}");
        send(socket, json!(["stdin", "show version\r"]), stage).await;
        until_output(socket, stage, |output| {
            output.contains(&format!("Meerschaum v{}", env!("CARGO_PKG_VERSION")))
        })
        .await;
        shell_state(socket, stage).await;
    }

    async fn shell_state(socket: &mut Socket, stage: &'static str) {
        let request = format!("host-native-state-{}", Uuid::new_v4().simple());
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = Json::Null;
        let mut startup = String::new();
        let diagnostic = |output: &str| {
            let output = meerschaum_cli::executor_jobs::LogSanitizer::default().push(output);
            meerschaum_server::redact::mask_uri_passwords(&output)
        };
        loop {
            // Keep only one readiness query outstanding. Startup can emit several stdout
            // frames before its reply; resending on every frame would flood the intentionally
            // bounded bridge queues rather than wait for the native shell's actual readiness.
            socket
                .send(Message::Text(
                    json!(["shell_state", request]).to_string().into(),
                ))
                .await
                .unwrap_or_else(|error| {
                    panic!("Terminal {stage}: sending readiness request failed: {error}; last readiness: {last}; startup: {:?}", diagnostic(&startup))
                });
            let query_deadline = Instant::now() + Duration::from_millis(150);
            loop {
                let received = tokio::time::timeout(
                    query_deadline.saturating_duration_since(Instant::now()),
                    socket.next(),
                )
                .await;
                let message = match received {
                    Err(_) => break,
                    Ok(None) => panic!(
                        "Terminal {stage}: readiness transport ended; last readiness: {last}; startup: {:?}",
                        diagnostic(&startup)
                    ),
                    Ok(Some(Err(error))) => panic!(
                        "Terminal {stage}: receiving readiness failed: {error}; last readiness: {last}; startup: {:?}",
                        diagnostic(&startup)
                    ),
                    Ok(Some(Ok(message))) => message,
                };
                assert!(
                    !matches!(message, Message::Close(_)),
                    "Terminal {stage}: the transport closed before readiness; last readiness: {last}; startup: {:?}",
                    diagnostic(&startup)
                );
                if let Message::Text(raw) = message {
                    let value: Json = serde_json::from_str(&raw).unwrap();
                    if value[0] == "stdout" {
                        startup.push_str(value[1].as_str().unwrap());
                        if startup.len() > 8192 {
                            startup = startup
                                .chars()
                                .rev()
                                .take(4096)
                                .collect::<String>()
                                .chars()
                                .rev()
                                .collect();
                        }
                    }
                    assert!(
                        value[0] != "disconnect",
                        "Terminal {stage}: native terminal disconnected before readiness; last readiness: {last}; startup: {:?}",
                        diagnostic(&startup)
                    );
                    if value[0] == "shell_state" && value[1] == request {
                        last = value[2].clone();
                        if last["idle"] == true && last["empty"] == true {
                            return;
                        }
                        break;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "Terminal {stage}: native shell did not become ready for input: {last}; startup: {:?}",
                diagnostic(&startup)
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn acceptance() {
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
        std::fs::create_dir_all(&parent).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("host-api-terminal-")
            .tempdir_in(parent)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("plugins")).unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.join("main.db").display()),
            )
            .await
            .unwrap(),
        );
        for (name, primary) in [
            ("everyone_operator", "Everyone"),
            ("team_operator", "HostTeam"),
            ("orphan_operator", "Everyone"),
            ("source_operator", "Everyone"),
        ] {
            let mut user = User::new(name, Some(PASSWORD));
            user.attributes = Some(map(json!({"role":"operator","primary_team":primary})));
            assert!(instance.register_user(&mut user).await.success);
        }
        let mut team = Team::new("HostTeam");
        team.members
            .insert("team_operator".into(), TeamRole::Member);
        assert!(instance.register_team(&mut team).await.success);
        let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        assert!(sdk.join("meerschaum/__init__.py").is_file());
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .env_clear()
            .env(
                "PATH",
                std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
            )
            .env("MRSM_ROOT_DIR", &root)
            .env("MRSM_PLUGINS_DIR", root.join("plugins"))
            .env(
                "MRSM_PYTHON",
                std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "/usr/bin/python3".into()),
            )
            .env("PYTHONPATH", &sdk)
            .env("DB_PASSWORD", "synthetic-main-terminal-env-secret")
            .arg(SERVER)
            .arg(&root)
            .stdin(Stdio::null())
            .stdout(Stdio::from(
                std::fs::File::create(root.join("main.stdout")).unwrap(),
            ))
            .stderr(Stdio::from(
                std::fs::File::create(root.join("main.stderr")).unwrap(),
            ));
        // This trusted native Main must capture its explicit candidate SDK as host input.
        // Its environment was cleared above; only confined child launches strip import
        // injection after recording the exact SDK paths in their sealed runtime descriptor.
        let mut owner = Owner {
            child: command.spawn().unwrap(),
            socket: None,
            diagnostic: root.join("main.stderr"),
        };
        wait_until("The owned native Main did not become ready.", || {
            assert!(
                owner.child.try_wait().unwrap().is_none(),
                "Main failed: {}",
                std::fs::read_to_string(root.join("main.stderr")).unwrap()
            );
            root.join("ready.json").is_file()
        })
        .await;
        let ready: Json =
            serde_json::from_slice(&std::fs::read(root.join("ready.json")).unwrap()).unwrap();
        let address = ready["address"].as_str().unwrap();
        let origin = format!("http://{address}");
        let secret: Vec<u8> = serde_json::from_value(ready["secret"].clone()).unwrap();
        let tmux_socket = PathBuf::from(ready["socket"].as_str().unwrap());
        owner.socket = Some(tmux_socket.clone());
        let plugin = format!(
            r#"
import errno,os,pathlib
from meerschaum.plugins import make_action
assert os.environ.get('MRSM_API_EXECUTION')=='1','Granted terminal sources must not import in control'
assert 'DB_PASSWORD' not in os.environ,'Main environment secret reached the terminal'
@make_action
def terminal_probe(action=None,**kw):
    state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
    assert state.name=='state'
    try:
        open({database}).read(1)
    except OSError as error:
        assert error.errno in (errno.EACCES,errno.EPERM)
    else:
        raise AssertionError('Host terminal read Main credentials/data')
    (state/'terminal-plugin-positive').write_text('confined')
    return True,'Confined Host terminal plugin completed.'
"#,
            database = json!(root.join("main.db"))
        );
        std::fs::write(root.join("plugins/terminal_probe.py"), &plugin).unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let sessions = meerschaum_web::sessions::InstanceSessions::new(instance.clone());
        let alice = session(
            &client,
            &origin,
            &secret,
            &sessions,
            "everyone_operator",
            PASSWORD,
        )
        .await;
        let mut socket = connect(address, &alice).await;
        version(&mut socket, "everyone initial").await;
        let original = record(&root, "everyone_operator");
        assert_eq!(
            original["source_ceiling"]["sources"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["terminal_probe"]
        );
        assert!(original["team_id"].is_null());
        assert_ne!(original["grant"]["id"], alice);
        let context = generation(&root, &original);
        let initial_spec: Json = serde_json::from_slice(
            &std::fs::read(context.join(meerschaum_core::team_scope::sandbox::SPEC_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            initial_spec["runtime"]["packages"]["meerschaum"],
            json!(sdk.join("meerschaum/__init__.py")),
            "The private terminal did not bind this candidate's SDK package: {}",
            initial_spec["runtime"]["packages"]
        );
        send(
            &mut socket,
            json!(["stdin", "terminal probe\r"]),
            "captured plugin A",
        )
        .await;
        until_output(&mut socket, "captured plugin A", |output| {
            output.contains("Confined Host terminal plugin completed")
        })
        .await;
        assert_eq!(
            std::fs::read_to_string(context.join("state/terminal-plugin-positive")).unwrap(),
            "confined"
        );
        std::fs::write(
            root.join("plugins/new_probe.py"),
            r#"
import os,pathlib
from meerschaum.plugins import make_action
assert os.environ.get('MRSM_API_EXECUTION')=='1'
@make_action
def new_probe(action=None,**kw):
    (pathlib.Path(os.environ['MRSM_ROOT_DIR'])/'ungranted-plugin-action').write_text('unsafe')
    return True,'A newly installed source must not enter an old terminal'
"#,
        )
        .unwrap();
        send(
            &mut socket,
            json!(["stdin", "new probe\r"]),
            "new plugin B refusal",
        )
        .await;
        until_output(&mut socket, "new plugin B refusal", |output| {
            output.contains("'sh' cannot be run by non-administrators")
        })
        .await;
        let current_credential: Json =
            serde_json::from_slice(&std::fs::read(context.join("credential.json")).unwrap())
                .unwrap();
        // The inner source view cannot know the newly installed action. Ask Main directly
        // with the original terminal callback too: its captured name ceiling must independently
        // allow A and reject raw B, before any shell fallback or argument rewriting.
        for (args, expected) in [
            (json!(["terminal", "probe"]), 200),
            (json!(["new", "probe"]), 403),
        ] {
            let response = client
                .post(format!("{origin}/execute/validate"))
                .bearer_auth(current_credential["token"].as_str().unwrap())
                .json(&json!({"sysargs": args}))
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body: Json = response.json().await.unwrap();
            assert_eq!(status, expected, "Raw terminal policy for {args}: {body}");
        }
        assert!(!context.join("state/ungranted-plugin-action").exists());
        assert!(!context.join("state/plugins/new_probe.py").exists());
        assert_eq!(
            record(&root, "everyone_operator")["source_ceiling"],
            original["source_ceiling"]
        );
        let configuration = std::fs::read_to_string(context.join("config.json")).unwrap();
        for forbidden in [
            "synthetic-main-terminal-database-secret",
            "synthetic-main-terminal-env-secret",
            "main.db",
            PASSWORD,
        ] {
            assert!(
                !configuration.contains(forbidden),
                "Private terminal configuration leaked {forbidden}."
            );
        }
        assert!(configuration.contains("mrsm_api_alias"));
        let spec: Json = serde_json::from_slice(
            &std::fs::read(context.join(meerschaum_core::team_scope::sandbox::SPEC_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(spec["mode"], "require_sockets");
        assert!(spec["runtime"].is_object());
        let first_panes = panes(&tmux_socket);
        assert_eq!(
            first_panes.len(),
            1,
            "A private tmux guardian must own the API shell."
        );
        let native_environment =
            std::fs::read(format!("/proc/{}/environ", first_panes[0].0)).unwrap();
        let native_environment = String::from_utf8_lossy(&native_environment);
        assert!(native_environment.contains("MRSM_API_EXECUTION=1"));
        assert!(native_environment.contains("MRSM_NATIVE_JOB_LIFECYCLE=terminal"));
        assert!(!native_environment.contains(PASSWORD));
        assert!(!native_environment.contains("synthetic-main-terminal-database-secret"));
        assert!(!native_environment.contains("DB_PASSWORD="));
        assert!(!native_environment.contains("synthetic-main-terminal-env-secret"));
        shell_state(&mut socket, "before raw shell refusal").await;
        let callback_before: Json =
            serde_json::from_slice(&std::fs::read(context.join("credential.json")).unwrap())
                .unwrap();
        send(
            &mut socket,
            json!([
                "stdin",
                "sh printf unsafe > forbidden-host-terminal-marker\r"
            ]),
            "raw shell refusal",
        )
        .await;
        until_output(&mut socket, "raw shell refusal", |output| {
            output.contains("not") || output.contains("refus") || output.contains("only")
        })
        .await;
        assert!(
            !context
                .join("state/forbidden-host-terminal-marker")
                .exists()
        );
        assert!(!root.join("forbidden-host-terminal-marker").exists());
        send(
            &mut socket,
            json!(["stdin", "show version --_mrsm-confined-python\r"]),
            "raw internal flag refusal",
        )
        .await;
        until_output(&mut socket, "raw internal flag refusal", |output| {
            output.contains("not user commands")
        })
        .await;
        send(
            &mut socket,
            json!(["set_size", 40, 160, 800, 1600]),
            "resize before new window",
        )
        .await;
        send(&mut socket, json!(["new_window"]), "create second window").await;
        wait_until("The private terminal did not add a guarded window.", || {
            panes(&tmux_socket).len() == 2
        })
        .await;
        version(&mut socket, "second window").await;
        let preserved = panes(&tmux_socket);
        socket.close(None).await.unwrap();
        drop(socket);
        let mut socket = connect(address, &alice).await;
        version(&mut socket, "everyone reconnect").await;
        shell_state(&mut socket, "everyone reconnect settled").await;
        let pending = std::fs::read_dir(&context)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("terminal-launch-"))
            })
            .map(|entry| {
                serde_json::from_slice::<Json>(&std::fs::read(entry.path()).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            pending.len(),
            preserved.len(),
            "Reattachment left a never-launched pending guardian."
        );
        assert!(
            pending
                .iter()
                .all(|proof| !proof["guardian"].is_null() && !proof["child"].is_null()),
            "Reattachment retained an unclaimed lifecycle nonce."
        );
        assert_eq!(
            record(&root, "everyone_operator")["grant"],
            original["grant"]
        );
        assert_eq!(
            panes(&tmux_socket),
            preserved,
            "Reconnect replaced a valid private tmux session."
        );
        wait_until(
            "The detached terminal's original callback lease was not rotated.",
            || {
                let current: Json = serde_json::from_slice(
                    &std::fs::read(context.join("credential.json")).unwrap(),
                )
                .unwrap();
                current["token"] != callback_before["token"]
            },
        )
        .await;
        version(&mut socket, "after callback rotation").await;
        let mut replacement = User::new(
            "everyone_operator",
            Some("synthetic-replacement-terminal-pass"),
        );
        replacement.attributes = Some(map(json!({"role":"operator"})));
        assert!(instance.edit_user(&replacement).await.success);
        wait_until(
            "Password revocation left the old terminal guardian alive.",
            || preserved.iter().all(|identity| !alive(*identity)),
        )
        .await;
        wait_until(
            "Password revocation did not finish exact terminal context retirement.",
            || {
                !context.exists()
                    && !root
                        .join("executors/callbacks")
                        .join(format!("{}.json", original["callback"].as_str().unwrap()))
                        .exists()
                    && !has_terminal_record(&root, &original["grant"]["id"])
            },
        )
        .await;
        drop(socket);
        let source_session = session(
            &client,
            &origin,
            &secret,
            &sessions,
            "source_operator",
            PASSWORD,
        )
        .await;
        let mut source_socket = connect(address, &source_session).await;
        // The WS handshake can finish before native provisioning. A finite buffered browser
        // burst must apply backpressure at the existing queue bound while output and fresh
        // source reauthorization continue; it must not close this healthy new connection.
        for index in 0..(meerschaum_server::webterm::remote::QUEUE + 8) {
            source_socket
                .send(Message::Text(
                    json!(["shell_state", format!("startup-burst-{index}")])
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
        version(&mut source_socket, "source revocation setup").await;
        let source_record = record(&root, "source_operator");
        let source_context = generation(&root, &source_record);
        let source_panes = panes(&tmux_socket);
        assert_eq!(source_panes.len(), 1);
        std::fs::write(
            root.join("plugins/terminal_probe.py"),
            format!("{plugin}\n# immutable source changed\n"),
        )
        .unwrap();
        wait_until(
            "Changed source content left the old Host terminal alive.",
            || source_panes.iter().all(|identity| !alive(*identity)),
        )
        .await;
        wait_until(
            "Source revocation did not finish exact terminal context retirement.",
            || {
                !source_context.exists()
                    && !root
                        .join("executors/callbacks")
                        .join(format!(
                            "{}.json",
                            source_record["callback"].as_str().unwrap()
                        ))
                        .exists()
                    && !has_terminal_record(&root, &source_record["grant"]["id"])
            },
        )
        .await;
        assert!(
            !root
                .join("executors/callbacks")
                .join(format!(
                    "{}.json",
                    source_record["callback"].as_str().unwrap()
                ))
                .exists()
        );
        drop(source_socket);
        let team_session = session(
            &client,
            &origin,
            &secret,
            &sessions,
            "team_operator",
            PASSWORD,
        )
        .await;
        let mut team_socket = connect(address, &team_session).await;
        version(&mut team_socket, "registered team initial").await;
        let team_record = record(&root, "team_operator");
        let team_context = generation(&root, &team_record);
        assert_eq!(team_record["team_id"], team.id.clone().unwrap());
        let team_panes = panes(&tmux_socket);
        assert_eq!(team_panes.len(), 1);
        team.members.clear();
        assert!(instance.edit_team(&team).await.success);
        wait_until(
            "Team membership revocation left its private terminal alive.",
            || team_panes.iter().all(|identity| !alive(*identity)),
        )
        .await;
        wait_until(
            "Membership revocation did not finish exact terminal context retirement.",
            || {
                !team_context.exists()
                    && !root
                        .join("executors/callbacks")
                        .join(format!(
                            "{}.json",
                            team_record["callback"].as_str().unwrap()
                        ))
                        .exists()
                    && !has_terminal_record(&root, &team_record["grant"]["id"])
            },
        )
        .await;
        drop(team_socket);
        let orphan = session(
            &client,
            &origin,
            &secret,
            &sessions,
            "orphan_operator",
            PASSWORD,
        )
        .await;
        let mut orphan_socket = connect(address, &orphan).await;
        version(&mut orphan_socket, "owner death setup").await;
        let orphan_panes = panes(&tmux_socket);
        assert_eq!(orphan_panes.len(), 1);
        orphan_socket.close(None).await.unwrap();
        drop(orphan_socket);
        owner.kill();
        wait_until(
            "Native Main owner death left its detached terminal alive.",
            || orphan_panes.iter().all(|identity| !alive(*identity)),
        )
        .await;
        println!(
            "Host API terminals: native websocket, confined actions, windows, reconnect, rotation, revocation and owner death passed."
        );
    }
}
