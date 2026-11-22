//! Actual Main HTTP extension discovery and native confined Api invocation.
use meerschaum_cli::executor::client::Controller;
use meerschaum_cli::executor::jobs::Catalog;
use meerschaum_cli::executor::registry::Registry;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::{environment, plugin_handlers, plugins};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
#[path = "../fixtures/executor_browser_hold.rs"]
mod browser_gate;
#[allow(unused_imports)]
use crate::common;
use crate::common::json_map as map;
use crate::common::process::{AbortOnDrop as Task, Subreaper};
struct Executor {
    child: Child,
    root: PathBuf,
}
impl Drop for Executor {
    fn drop(&mut self) {
        use meerschaum_jobs::process;
        let mut owned = BTreeMap::new();
        let mut parents = vec![self.child.id() as i32];
        // Only this private root's native detached supervisors are included.
        for (pid, args) in process::process_cmdlines() {
            if args.windows(2).any(|pair| {
                pair[0] == "--jobs-dir" && Path::new(&pair[1]) == self.root.join("jobs")
            }) && args
                .iter()
                .any(|arg| arg == meerschaum_jobs::supervisor::SUPERVISOR_ARG)
            {
                parents.push(pid);
            }
        }
        while let Some(pid) = parents.pop() {
            if owned.contains_key(&pid) {
                continue;
            }
            if let Some(birth) = process::process_start_time(pid) {
                owned.insert(pid, birth);
                parents.extend(process::children(pid).unwrap_or_default());
            }
        }
        let _ = process::send_signal(self.child.id() as i32, process::Sig::Int);
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until && self.child.try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        for (pid, birth) in owned {
            if process::process_start_time(pid) == Some(birth)
                && matches!(
                    process::proc_state(pid),
                    process::ProcState::Alive | process::ProcState::Stopped
                )
            {
                let _ = process::send_signal(pid, process::Sig::Kill);
            }
            if let Some(pid) = rustix::process::Pid::from_raw(pid) {
                let _ = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
            }
        }
    }
}
fn archive(path: &Path, name: &str, source: &str) {
    common::fs::write_plugin_archive(path, name, &format!("{name}.py"), source);
}
fn diagnostics(root: &Path) -> String {
    let mut text = String::new();
    let mut paths = vec![root.join("launcher.log"), root.join("daemon_errors.log")];
    for directory in [root.join("logs"), root.join("jobs")] {
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.flatten().take(12) {
                if entry.path().is_dir() {
                    paths.push(entry.path().join("properties.json"));
                } else {
                    paths.push(entry.path());
                }
            }
        }
    }
    for path in paths {
        if let Ok(metadata) = std::fs::symlink_metadata(&path)
            && metadata.is_file()
            && metadata.len() < 128 * 1024
            && let Ok(value) = std::fs::read_to_string(&path)
        {
            let value = meerschaum_server::redact::mask_uri_passwords(&value);
            text.push_str(&format!(
                "\n{}: {}",
                path.strip_prefix(root).unwrap_or(&path).display(),
                value
            ));
        }
    }
    text
}

struct Extensions {
    controller: Arc<Controller>,
    browser_sql: Option<Arc<meerschaum_connector_sql::SqlConnector>>,
}

impl Extensions {
    async fn track<T>(&self, stage: &str, future: impl std::future::Future<Output = T>) -> T {
        let Some(sql) = &self.browser_sql else {
            return future.await;
        };
        let started = Instant::now();
        let preceding = sql.take_query_log().len();
        eprintln!("EXTENSION_BACKEND_STAGE={stage}_started preceding_sql_statements={preceding}");
        let result = future.await;
        eprintln!(
            "EXTENSION_BACKEND_STAGE={stage}_finished elapsed_ms={} observed_sql_statements={}",
            started.elapsed().as_millis(),
            sql.take_query_log().len()
        );
        result
    }
}
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for Extensions {
    fn list(&self) -> Vec<String> {
        Vec::new()
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        panic!("Typed extension entered generic Main action dispatch")
    }
    async fn extension_catalog(&self) -> Result<Option<plugins::Validated>, String> {
        self.track("catalog", self.controller.extension_catalog())
            .await
    }
    async fn extension_invoke(
        &self,
        request: plugin_handlers::Invocation,
    ) -> Result<plugin_handlers::Reply, String> {
        self.track("invocation", self.controller.extension_invoke(request))
            .await
    }
}

fn marker(root: &Path, name: &str) -> Option<PathBuf> {
    let mut pending = vec![(root.to_path_buf(), 0)];
    let mut remaining = 4096usize;
    while let Some((directory, depth)) = pending.pop() {
        if depth > 9 {
            continue;
        }
        for entry in std::fs::read_dir(directory).ok()?.flatten() {
            remaining = remaining.checked_sub(1)?;
            let metadata = std::fs::symlink_metadata(entry.path()).ok()?;
            if metadata.is_symlink() {
                continue;
            }
            if metadata.is_file() && entry.file_name() == name {
                return Some(entry.path());
            }
            if metadata.is_dir() {
                pending.push((entry.path(), depth + 1));
            }
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_and_code_team_extensions_use_exact_private_handlers_and_recheck_a_waiting_source() {
    extension_fixture(None).await;
}

/// An isolated Firefox acceptance lane drives the printed private file protocol. No browser
/// is started here, and all native processes remain owned by this bounded fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated Firefox browser and bounded private control protocol"]
async fn opaque_extension_pages_render_callbacks_and_refuse_navigation() {
    extension_fixture(Some(&[
        "/_pages/extension-html",
        "/_pages/extension-dash",
        "/_pages/extension-active-figure",
        "/_pages/extension-entity-figure",
        "/_pages/extension-custom-js",
        "/_pages/extension-command-figure",
    ]))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Firefox to render a live chart and submit its authorized callback"]
async fn dash_extension_displays_a_graph_and_updates_it_from_the_team_callback() {
    extension_fixture(Some(&["/_pages/extension-dash"])).await;
}

async fn extension_fixture(browser_pages: Option<&[&str]>) {
    let browser = browser_pages.is_some();
    let fixture_started = Instant::now();
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
    let _subreaper = Subreaper::new();
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    std::fs::create_dir_all(main.join("plugins")).unwrap();
    std::fs::write(main.join("main.secret"), "extension-main-private-sentinel").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let mut reservations = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..2 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        ports.push(listener.local_addr().unwrap().port());
        reservations.push(listener);
    }
    let snapshot = map(
        json!({"meerschaum":{"instance":"sql:main","api_instance":"sql:main","connectors":{
        "sql":{"main":{"flavor":"sqlite","database":main.join("main.db")}},
        "api":{"remote_api":{"host":"127.0.0.1","port":ports[0]},"remote_code":{"host":"127.0.0.1","port":ports[1]},"data":{"uri":origin}}
    }},"api":{"no_dash":true,"execution":{"callback_url":origin}},"system":{"packages":{"auto_install":false}}}),
    );
    let options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), main.display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            main.join("plugins").display().to_string(),
        ),
    ]);
    let config = Config::new(options.clone()).unwrap();
    config.set_config(snapshot.clone());
    let session = meerschaum_cli::native::Session::with_runtime(tokio::runtime::Handle::current());
    session.set_python_worker(false);
    let native_instance =
        tokio::task::spawn_blocking(move || session.instance("sql:main", &config).unwrap())
            .await
            .unwrap();
    let instance = native_instance.conn();
    let mut teams = Vec::new();
    let mut users = Vec::new();
    for (name, username, code, connector) in [
        ("ExtensionApi", "extension_api", false, "api:remote_api"),
        ("ExtensionCode", "extension_code", true, "api:remote_code"),
    ]
    .into_iter()
    .take(if browser { 1 } else { 2 })
    {
        let mut user = User::new(username, Some("fixture-extension-password"));
        user.attributes = Some(map(if code {
            json!({"role":"operator","primary_team":name})
        } else {
            json!({"scopes":["plugins:read","pipes:read","users:read"],"primary_team":name})
        }));
        assert!(instance.register_user(&mut user).await.success);
        let mut team = Team::new(name);
        team.attributes = map(
            json!({"isolated":true,"plugins_write":code,"plugins":["extension_probe"],"instance":"api:data","connectors":["api:data"],"execution":{"mode":"api","connector":connector}}),
        );
        team.members.insert(username.into(), TeamRole::Member);
        assert!(instance.register_team(&mut team).await.success);
        users.push(user);
        teams.push(team);
    }
    let desired = Registry::new(&main).reconcile(&teams).unwrap();
    let mut executors = Vec::new();
    for (index, team) in teams.iter().enumerate() {
        let root = temp.path().join(format!("executor-{index}"));
        let control = temp.path().join(format!("factory-control-{index}"));
        std::fs::create_dir(&control).unwrap();
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
        let bootstrap = control.join("bootstrap.json");
        std::fs::write(
            &bootstrap,
            serde_json::to_vec(&desired.teams[&id].verifier).unwrap(),
        )
        .unwrap();
        let log = std::fs::File::create(control.join("launcher.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &root)
            .env("MRSM_ROOT_DIR", &root)
            .env("MRSM_PLUGINS_DIR", root.join("plugins"))
            .env("MRSM_EXECUTOR_BOOTSTRAP", &bootstrap)
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "PYTHONPATH",
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../..")
                    .canonicalize()
                    .unwrap(),
            )
            .env("AWS_SECRET_ACCESS_KEY", "extension-parent-secret")
            .env("DB_PASSWORD", "extension-parent-db-secret")
            .env(
                "MRSM_PYTHON",
                std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "python3".into()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .args([
                "start",
                "executor",
                "--host",
                "127.0.0.1",
                "--port",
                &ports[index].to_string(),
                "--control-root",
                control.to_str().unwrap(),
            ]);
        drop(reservations.remove(0));
        let mut executor = Executor {
            child: command.spawn().unwrap(),
            root: control,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while tokio::net::TcpStream::connect(("127.0.0.1", ports[index]))
            .await
            .is_err()
        {
            assert!(
                executor.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
                "{}",
                diagnostics(&executor.root)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        executors.push(executor);
    }
    let controller = Arc::new(Controller::new(
        main.clone(),
        instance.clone(),
        options,
        snapshot.clone(),
    ));
    let mut server = meerschaum_server::ServerConfig::for_root("sql:main", &main);
    server.no_dash = false;
    server.execution_callback_url = Some(origin.clone());
    server.connectors = snapshot
        .get("meerschaum")
        .unwrap()
        .get("connectors")
        .unwrap()
        .to_map();
    archive(
        &server.plugins_archives_path,
        "extension_probe",
        &(format!(
            r#"
import errno,json,os,pathlib,time
from meerschaum.plugins import api_plugin
from meerschaum.mcp import tool,resource,prompt
@tool('extension_tool','Exact private tool',input_schema={{'type':'object','properties':{{'text':{{'type':'string'}}}},'required':['text'],'additionalProperties':False}},scopes=['pipes:read'],read_only=True,idempotent=True,title='Confined tool')
def extension_tool(text):
    return {{'echo':text,'api':os.environ.get('MRSM_API_EXECUTION')=='1'}}
@tool('extension_changed_tool','Runtime declaration must match',input_schema={{'type':'object'}},scopes=['pipes:read'] if os.environ.get('MRSM_METADATA_EXECUTION')=='1' else ['users:write'])
def extension_changed_tool():
    return {{'should_not_run':True}}
@resource(uri='mrsm://extension/probe',name='extension_resource',description='Exact private resource',mime_type='text/plain',scopes=['pipes:read'],title='Confined resource')
def extension_resource():
    return 'private-resource'
@prompt('extension_prompt','Exact private prompt',arguments=[{{'name':'topic','description':'The topic','required':True}}],scopes=['pipes:read'],title='Confined prompt')
def extension_prompt(topic):
    return [{{'role':'user','content':{{'type':'text','text':topic}}}}]
@api_plugin
def api(app):
    from fastapi import Request,Depends
    from meerschaum.api import ScopedAuth,CurrentScopes,session_user,optional_session_user,manager
    @app.get('/extension-probe/scoped',openapi_extra={{'x-mrsm-scopes':['users:read']}})
    async def scoped(request:Request,user=Depends(ScopedAuth(['pipes:read'])),scopes=Depends(CurrentScopes),session=Depends(optional_session_user),direct=Depends(manager)):
        return {{'username':user.username,'scopes':scopes,'permissions':user.permissions,
            'direct_username':direct.username,'session_username':session.username if session else None,
            'password_absent':not user.password,
            'credential_headers_absent':all(key not in request.headers for key in ['authorization','cookie'])}}
    @app.get('/extension-probe/session')
    async def console_user(user=Depends(session_user)):
        return {{'username':user.username,'permissions':user.permissions,'password_absent':not user.password}}
    @app.get('/extension-probe/changed',openapi_extra={{'x-mrsm-scopes':['pipes:read'] if os.environ.get('MRSM_METADATA_EXECUTION')=='1' else ['users:write']}})
    def changed():
        return {{'should_not_run':True}}
    @app.post('/extension-probe/items/{{item}}',openapi_extra={{'x-mrsm-scopes':['pipes:read','users:read']}})
    async def item(item:str,request:Request):
        value=await request.json()
        root=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
        if value.get('wait'):
            (root/value['wait']).write_text('entered')
            deadline=time.monotonic()+15
            while not (root/(value['wait']+'-release')).exists():
                assert time.monotonic()<deadline,'fixture did not release exact handler'
                time.sleep(.02)
        denied=False
        try:pathlib.Path({secret}).read_text()
        except OSError as error:denied=error.errno in (errno.EACCES,errno.EPERM)
        import meerschaum as mrsm
        config=mrsm.get_config('meerschaum','connectors')
        callback=mrsm.get_connector('api','main_callback')
        response=callback.get('/healthcheck')
        users_response=callback.get('/users')
        teams_response=callback.get('/teams/'+value['team'])
        return {{'item':item,'api':os.environ.get('MRSM_API_EXECUTION')=='1','main_denied':denied,
            'main_sql_absent':config.get('sql',{{}}).get('main') is None,
            'parent_env_absent':all(name not in os.environ for name in ['AWS_SECRET_ACCESS_KEY','DB_PASSWORD']),
            'alias':bool(config['api']['data'].get('mrsm_api_alias')),
            'credential_headers_absent':all(key not in request.headers for key in ['authorization','cookie','proxy-authorization','x-mrsm-user']),
            'callback_ok':response.status_code==200,
            'callback_users_denied':users_response.status_code==403,
            'callback_teams_denied':teams_response.status_code==403}}
"#,
            secret = json!(main.join("main.secret"))
        ) + if browser {
            include_str!("../fixtures/executor_extension_pages.py")
        } else {
            ""
        }),
    );
    let state = meerschaum_server::AppState::builder(server, instance.clone())
        .actions(Arc::new(Extensions {
            controller: controller.clone(),
            browser_sql: browser.then(|| {
                let sql = native_instance.sql().unwrap().clone();
                sql.enable_query_log();
                sql
            }),
        }))
        .build()
        .unwrap();
    controller.bind_state(&state).unwrap();
    // Match the production Main lease owner throughout long HTTP sequences and the
    // manual browser hold. Dropping Task aborts the owned maintenance/subtasks.
    let _maintenance = Task(tokio::spawn(controller.clone().maintain()));
    let web = meerschaum_web::WebConfig::for_state(&state);
    let sessions = web.sessions.clone();
    let app = meerschaum_web::app(state.clone(), web).unwrap();
    let app = if browser {
        app.layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: axum::middleware::Next| async move {
            let stage = match request.uri().path() {
                path if path.starts_with("/_pages/") => Some("legacy_page"),
                path if path.starts_with("/plugins/extensions/pages/") => Some("dedicated_page"),
                "/plugins/extensions/page-callback" => Some("page_callback"),
                "/plugins/extensions/policy" => Some("page_policy"),
                _ => None,
            };
            let request_started = Instant::now();
            if let Some(stage) = stage {
                eprintln!("EXTENSION_HTTP_STAGE={stage}_started fixture_elapsed_ms={} utc_ms={}", fixture_started.elapsed().as_millis(),chrono::Utc::now().timestamp_millis());
            }
            let response = next.run(request).await;
            if let Some(stage) = stage {
                eprintln!("EXTENSION_HTTP_STAGE={stage}_finished status={} request_elapsed_ms={} fixture_elapsed_ms={}",response.status().as_u16(),request_started.elapsed().as_millis(),fixture_started.elapsed().as_millis());
            }
            response
        }))
    } else {
        app
    };
    let _main = Task(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (index, team) in teams.iter().enumerate() {
        let login: Json = client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!(
                "username={}&password=fixture-extension-password",
                users[index].username
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let bearer = login["access_token"].as_str().unwrap();
        let response = client
            .get(format!("{origin}/plugins/extensions"))
            .bearer_auth(bearer)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert_eq!(
            status,
            200,
            "{text}\n{}",
            diagnostics(&executors[index].root)
        );
        let catalog: plugins::Catalog = serde_json::from_str(&text).unwrap();
        if browser {
            eprintln!(
                "EXTENSION_BROWSER_STAGE=catalog_primed fixture_elapsed_ms={} utc_ms={}",
                fixture_started.elapsed().as_millis(),
                chrono::Utc::now().timestamp_millis()
            );
        }
        let registry = catalog.plugins["extension_probe"]
            .metadata
            .as_ref()
            .unwrap()
            .registry
            .as_ref()
            .unwrap();
        assert_eq!(
            registry.handlers().count(),
            if browser { 15 } else { 8 },
            "API registry: {:?}; owned metadata diagnostics: {}",
            registry.api,
            diagnostics(&executors[index].root)
        );
        let mut original_headers = axum::http::HeaderMap::new();
        original_headers.insert("authorization", format!("Bearer {bearer}").parse().unwrap());
        let caller = meerschaum_server::access::require_fresh(
            &state,
            &original_headers,
            &["plugins:read", "pipes:read", "users:read"],
        )
        .await
        .unwrap();
        if browser {
            assert!(team.is_isolated());
            assert_eq!(
                team.attributes.get("plugins_write"),
                Some(&Value::Bool(false))
            );
            assert!(!caller.has("actions:execute"));
            assert!(!caller.has("jobs:execute"));
            assert!(!caller.has("jobs:write"));
            let session_id = Uuid::new_v4().to_string();
            sessions
                .set(
                    &session_id,
                    json!({"username": users[index].username, "credential": caller.credential}),
                    600,
                )
                .await
                .unwrap();
            let browser_origin = format!(
                "http://tpr-{}.localhost:{}",
                Uuid::new_v4(),
                reqwest::Url::parse(&origin).unwrap().port().unwrap()
            );
            let mut gate = browser_gate::BrowserGate::open(
                &temp.path().join("browser-hold"),
                &browser_origin,
                &session_id,
                browser_pages.unwrap(),
            )
            .unwrap();
            eprintln!(
                "EXTENSION_BROWSER_READY={}",
                temp.path().join("browser-hold/ready.json").display()
            );
            eprintln!(
                "EXTENSION_BROWSER_STAGE=ready fixture_elapsed_ms={} utc_ms={}",
                fixture_started.elapsed().as_millis(),
                chrono::Utc::now().timestamp_millis()
            );
            let saved = team.clone();
            loop {
                let control = gate.next().await.unwrap();
                match control.action {
                    browser_gate::Action::RevokeSource => {
                        let mut revoked = saved.clone();
                        revoked
                            .attributes
                            .insert("plugins".into(), Value::List(vec![]));
                        assert!(instance.edit_team(&revoked).await.success);
                    }
                    browser_gate::Action::RestoreSource => {
                        assert!(instance.edit_team(&saved).await.success);
                    }
                    browser_gate::Action::CloseSession => {
                        sessions.delete(&session_id).await.unwrap();
                    }
                    browser_gate::Action::Release { passed } => {
                        gate.acknowledge(control.sequence, true).unwrap();
                        assert!(passed, "The actual Firefox acceptance failed");
                        sessions.delete(&session_id).await.unwrap();
                        return;
                    }
                }
                gate.acknowledge(control.sequence, true).unwrap();
            }
        }
        if index == 0 {
            assert!(!caller.has("actions:execute"));
            assert!(!caller.has("jobs:execute"));
            assert!(!caller.has("jobs:write"));
            let stored = instance
                .get_user_attributes(&users[index])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stored.get("scopes"),
                Some(&Value::List(vec![
                    "plugins:read".into(),
                    "pipes:read".into(),
                    "users:read".into()
                ]))
            );
        }
        let selected = registry.handlers().find(|handler|matches!(&handler.selector,plugin_handlers::Selector::Api {path,..} if path == "/extension-probe/items/{item}")).unwrap();
        let operation = meerschaum_server::executor::extensions::Operation {
            caller: None,
            deadline: chrono::Utc::now().timestamp() + 60,
            catalog: catalog.clone(),
            invocation: plugin_handlers::Invocation {
                catalog_sha256: registry.source_catalog_sha256.clone(),
                plugin: "extension_probe".into(),
                handler_id: selected.id.clone(),
                request: plugin_handlers::Request::Api {
                    method: "POST".into(),
                    path: "/extension-probe/items/owned".into(),
                    query: Vec::new(),
                    headers: Default::default(),
                    body: json!({"team":team.name}).to_string(),
                },
            },
        };
        operation.authorize(&caller, team).unwrap();
        // The original login's explicit JWT scope ceiling must survive an owner whose
        // stored role still holds users:read, including a callback-derived child.
        let mut claims =
            meerschaum_server::auth::jwt_decode(bearer, state.secret(), chrono::Utc::now())
                .unwrap();
        claims.insert("scopes".into(), json!(["plugins:read", "pipes:read"]));
        let narrow_bearer =
            meerschaum_server::auth::jwt_encode(&map(Json::Object(claims)), state.secret());
        let mut narrow_headers = axum::http::HeaderMap::new();
        narrow_headers.insert(
            "authorization",
            format!("Bearer {narrow_bearer}").parse().unwrap(),
        );
        let narrow =
            meerschaum_server::access::require_fresh(&state, &narrow_headers, &["plugins:read"])
                .await
                .unwrap();
        assert!(!narrow.has("users:read"));
        assert!(operation.authorize(&narrow, team).is_err());
        let required = operation.required().unwrap();
        let required_refs: Vec<_> = required.iter().map(String::as_str).collect();
        let ceiling =
            meerschaum_server::executor::source_ceiling::Ceiling::from_catalog(&catalog).unwrap();
        let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
        let parent = meerschaum_server::executor::callbacks::register_with_sources(
            &state,
            &narrow,
            Uuid::new_v4(),
            Some(team_id),
            &["plugins:read"],
            meerschaum_server::executor::callbacks::Purpose::Command,
            ceiling.clone(),
        )
        .await
        .unwrap();
        let parent_identity = meerschaum_server::executor::callbacks::load_identity(
            &state,
            parent.id,
            parent.launch_id,
        )
        .await
        .unwrap();
        assert!(parent_identity.source_access().has("users:read"));
        assert!(!parent_identity.caller().has("users:read"));
        assert!(
            operation
                .admit(
                    &state,
                    parent.id,
                    parent.launch_id,
                    Some(team_id),
                    &operation.sysargs()
                )
                .await
                .is_err(),
            "An exact handler recovered permissions outside its initiating JWT/parent"
        );
        assert!(
            meerschaum_server::executor::callbacks::register_with_sources(
                &state,
                &parent_identity.caller(),
                Uuid::new_v4(),
                Some(team_id),
                &required_refs,
                meerschaum_server::executor::callbacks::Purpose::Command,
                ceiling.clone(),
            )
            .await
            .is_err(),
            "A narrow parent minted an unheld handler scope"
        );
        meerschaum_server::executor::callbacks::revoke(&state, parent.id)
            .await
            .unwrap();
        let admitted = meerschaum_server::executor::callbacks::register_with_sources(
            &state,
            &caller,
            Uuid::new_v4(),
            Some(team_id),
            &required_refs,
            meerschaum_server::executor::callbacks::Purpose::Command,
            ceiling,
        )
        .await
        .unwrap();
        let admitted_identity = operation
            .admit(
                &state,
                admitted.id,
                admitted.launch_id,
                Some(team_id),
                &operation.sysargs(),
            )
            .await
            .unwrap();
        assert!(
            !admitted_identity.caller().has("users:read"),
            "Main-only handler admission expanded the actual callback principal"
        );
        assert!(!admitted_identity.grant.permissions.contains("users:read"));
        meerschaum_server::executor::callbacks::revoke(&state, admitted.id)
            .await
            .unwrap();
        let mut crossed = caller.clone();
        crossed.principal = crossed
            .principal
            .within_team("A different execution boundary");
        assert!(
            operation.authorize(&crossed, team).is_err(),
            "An explicit parent execution boundary was broadened"
        );
        let response = client
            .get(format!("{origin}/extension-probe/scoped"))
            .bearer_auth(bearer)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let scoped: Json = response.json().await.unwrap();
        assert_eq!(status, 200, "{scoped}");
        assert_eq!(scoped["username"], users[index].username);
        assert_eq!(scoped["direct_username"], users[index].username);
        assert!(
            scoped["session_username"].is_null(),
            "A bearer adopted a console session"
        );
        assert_eq!(scoped["password_absent"], true);
        assert_eq!(scoped["credential_headers_absent"], true);
        for key in ["scopes", "permissions"] {
            let actual: std::collections::BTreeSet<_> = scoped[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect();
            let expected: std::collections::BTreeSet<_> = caller
                .principal
                .permissions
                .iter()
                .map(String::as_str)
                .collect();
            assert_eq!(actual, expected, "{key}: {scoped}");
        }
        let response = client
            .get(format!("{origin}/extension-probe/session"))
            .bearer_auth(bearer)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.bytes().await.unwrap();
        let mut body = serde_json::from_slice::<Json>(&bytes)
            .unwrap_or_else(|_| json!({"unparsed_response_bytes":bytes.len()}));
        meerschaum_server::redact::redact_value(None, &mut body);
        let body = body.to_string().chars().take(2048).collect::<String>();
        assert!(
            !status.is_success(),
            "A plain bearer satisfied legacy session_user: status={status} body={body}"
        );
        assert_eq!(
            status,
            401,
            "The plain-bearer session check did not reach the expected authentication refusal: status={status} body={body} fixture_elapsed_ms={}",
            fixture_started.elapsed().as_millis(),
        );
        let session_id = Uuid::new_v4().to_string();
        sessions
            .set(
                &session_id,
                json!({"username":users[index].username,"credential":caller.credential}),
                3600,
            )
            .await
            .unwrap();
        let cookie = format!("mrsm-session-id={session_id}");
        let response = client
            .get(format!("{origin}/extension-probe/session"))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let console: Json = response.json().await.unwrap();
        assert_eq!(status, 200, "{console}");
        assert_eq!(console["username"], users[index].username);
        assert_eq!(console["password_absent"], true);
        sessions.delete(&session_id).await.unwrap();
        let before = Catalog::new(&main).records().unwrap().len();
        let response = client
            .get(format!("{origin}/extension-probe/session"))
            .header("cookie", &cookie)
            .header("x-mrsm-user", &users[index].username)
            .send()
            .await
            .unwrap();
        assert!(
            !response.status().is_success(),
            "A deleted session/forged user header entered a private handler"
        );
        assert_eq!(Catalog::new(&main).records().unwrap().len(), before);
        let response = client
            .get(format!("{origin}/extension-probe/changed"))
            .bearer_auth(bearer)
            .send()
            .await
            .unwrap();
        assert!(
            !response.status().is_success(),
            "A runtime-expanded API scope declaration was used"
        );
        let response = client
            .post(format!("{origin}/extension-probe/items/owned"))
            .bearer_auth(bearer)
            .header("cookie", "fixture-main-cookie=must-not-forward")
            .json(&json!({"team":team.name}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert_eq!(
            status,
            200,
            "{text}\n{}",
            diagnostics(&executors[index].root)
        );
        let proof: Json = serde_json::from_str(&text).unwrap();
        assert_eq!(proof["item"], "owned");
        for field in [
            "api",
            "main_denied",
            "main_sql_absent",
            "parent_env_absent",
            "alias",
            "credential_headers_absent",
            "callback_ok",
            "callback_users_denied",
            "callback_teams_denied",
        ] {
            assert_eq!(proof[field], true, "{field}: {proof}");
        }
        let records = Catalog::new(&main).records().unwrap();
        let extension = records
            .values()
            .find(|record| {
                record.extension.is_some()
                    && record.team_id.to_string() == team.id.as_deref().unwrap()
                    && record
                        .extension
                        .as_ref()
                        .is_some_and(|operation| operation.invocation.handler_id == selected.id)
            })
            .unwrap();
        assert_eq!(extension.profile, environment::Profile::Api);
        assert!(!extension.action_request);
        assert_eq!(
            extension.extension.as_ref().unwrap().invocation.handler_id,
            selected.id
        );
        let grant = meerschaum_server::executor::callbacks::load_identity(
            &state,
            extension.callback.unwrap(),
            Uuid::parse_str(extension.remote_name.strip_prefix("mrsm-").unwrap()).unwrap(),
        )
        .await;
        // Completed jobs may already be suspended; canonical required scopes remain exact
        // while any live source proof is available.
        if let Ok(grant) = grant {
            assert!(!grant.grant.required.contains("actions:execute"));
            assert!(!grant.grant.required.contains("jobs:execute"));
        }
        let mut stale = extension.extension.as_ref().unwrap().invocation.clone();
        stale.handler_id = "0".repeat(64);
        let response = client
            .post(format!("{origin}/plugins/extensions/invoke"))
            .bearer_auth(bearer)
            .json(&stale)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        let rpc = |id, method: &str, params: Json| json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let listed: Json = client
            .post(format!("{origin}/mcp"))
            .bearer_auth(bearer)
            .json(&rpc(1, "tools/list", json!({})))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let tool = listed["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("{listed}"))
            .iter()
            .find(|tool| tool["name"] == "extension_tool")
            .unwrap_or_else(|| panic!("{listed}"));
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["annotations"]["idempotentHint"], true);
        assert_eq!(tool["title"], "Confined tool");
        let called: Json = client
            .post(format!("{origin}/mcp"))
            .bearer_auth(bearer)
            .json(&rpc(
                2,
                "tools/call",
                json!({"name":"extension_tool","arguments":{"text":"typed"}}),
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_ne!(called["result"]["isError"], true, "{called}");
        assert!(called.to_string().contains("typed"), "{called}");
        let resource: Json = client
            .post(format!("{origin}/mcp"))
            .bearer_auth(bearer)
            .json(&rpc(
                3,
                "resources/read",
                json!({"uri":"mrsm://extension/probe"}),
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            resource["result"]["contents"][0]["text"], "private-resource",
            "{resource}"
        );
        assert_eq!(
            resource["result"]["contents"][0]["mimeType"], "text/plain",
            "{resource}"
        );
        let prompt: Json = client
            .post(format!("{origin}/mcp"))
            .bearer_auth(bearer)
            .json(&rpc(
                4,
                "prompts/get",
                json!({"name":"extension_prompt","arguments":{"topic":"confined topic"}}),
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            prompt["result"]["description"], "Exact private prompt",
            "{prompt}"
        );
        assert_eq!(
            prompt["result"]["messages"][0]["content"]["text"], "confined topic",
            "{prompt}"
        );
        let changed: Json = client
            .post(format!("{origin}/mcp"))
            .bearer_auth(bearer)
            .json(&rpc(
                5,
                "tools/call",
                json!({"name":"extension_changed_tool","arguments":{}}),
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            changed.get("error").is_some() || changed["result"]["isError"] == true,
            "A runtime-expanded MCP scope declaration was used: {changed}"
        );
        if index == 0 {
            let session_id = Uuid::new_v4().to_string();
            sessions
                .set(
                    &session_id,
                    json!({"username":users[index].username,"credential":caller.credential}),
                    3600,
                )
                .await
                .unwrap();
            let nonce = format!("session-handler-{}", Uuid::new_v4().simple());
            let request_nonce = nonce.clone();
            let request_team = team.name.clone();
            let url = format!("{origin}/extension-probe/items/waiting");
            let request_client = client.clone();
            let cookie = format!("mrsm-session-id={session_id}");
            let pending = tokio::spawn(async move {
                request_client
                    .post(url)
                    .header("cookie", cookie)
                    .json(&json!({"team":request_team,"wait":request_nonce}))
                    .send()
                    .await
                    .unwrap()
            });
            let deadline = Instant::now() + Duration::from_secs(20);
            let entered = loop {
                if let Some(path) = marker(&executors[index].root, &nonce) {
                    break path;
                }
                assert!(
                    Instant::now() < deadline,
                    "Console handler did not reach its owned barrier: {}",
                    diagnostics(&executors[index].root)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            sessions.delete(&session_id).await.unwrap();
            std::fs::write(
                entered.with_file_name(format!("{nonce}-release")),
                "release",
            )
            .unwrap();
            assert!(
                !pending.await.unwrap().status().is_success(),
                "A console session deleted during the actual handler wait published a result"
            );
            let token = Uuid::new_v4().simple().to_string();
            let nonce = format!("handler-{token}");
            let request_nonce = nonce.clone();
            let request_team = team.name.clone();
            let url = format!("{origin}/extension-probe/items/waiting");
            let request_client = client.clone();
            let request_bearer = bearer.to_string();
            let pending = tokio::spawn(async move {
                request_client
                    .post(url)
                    .bearer_auth(request_bearer)
                    .json(&json!({"team":request_team,"wait":request_nonce}))
                    .send()
                    .await
                    .unwrap()
            });
            let deadline = Instant::now() + Duration::from_secs(20);
            let entered = loop {
                if let Some(path) = marker(&executors[index].root, &nonce) {
                    break path;
                }
                assert!(
                    Instant::now() < deadline,
                    "Handler did not reach owned barrier: {}",
                    diagnostics(&executors[index].root)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            let mut narrowed = users[index].clone();
            // Preserve the exact password binding so this proves role revocation rather
            // than an incidental salt/password change invalidating the fixture's JWT.
            let password_hash = instance
                .get_user_password_hash(&users[index])
                .await
                .unwrap();
            narrowed.password_hash = password_hash.clone();
            narrowed.attributes = Some(map(
                json!({"scopes":["plugins:read"],"primary_team":team.name}),
            ));
            assert!(instance.edit_user(&narrowed).await.success);
            let stored = instance
                .get_user_attributes(&narrowed)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stored.get("scopes"),
                Some(&Value::List(vec!["plugins:read".into()]))
            );
            let mut original_headers = axum::http::HeaderMap::new();
            original_headers.insert("authorization", format!("Bearer {bearer}").parse().unwrap());
            let current = meerschaum_server::access::require_fresh(
                &state,
                &original_headers,
                &["plugins:read"],
            )
            .await
            .unwrap();
            assert!(
                !current.has("pipes:read"),
                "Fixture JWT remains valid but must observe the narrowed role"
            );
            std::fs::write(
                entered.with_file_name(format!("{nonce}-release")),
                "release",
            )
            .unwrap();
            let response = pending.await.unwrap();
            assert!(
                !response.status().is_success(),
                "A role revoked while the actual handler awaited was allowed to publish its result"
            );
            let mut restored = users[index].clone();
            restored.password_hash = password_hash;
            assert!(instance.edit_user(&restored).await.success);
        }
    }
}
