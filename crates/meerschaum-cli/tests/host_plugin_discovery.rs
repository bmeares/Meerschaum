//! Real credential-free Main-host discovery with explicit Everyone/registered Host identities.
#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    fixture::main()
}
#[cfg(not(target_os = "linux"))]
fn main() {}
#[cfg(target_os = "linux")]
mod fixture {
    use meerschaum_cli::{
        executor::client::Controller,
        executor::jobs::{Catalog, JobPlacement, Record},
    };
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::{
        Map, Pipe, SuccessTuple, Value,
        access::{Team, TeamRole},
        config::ConfigOptions,
        users::User,
    };
    use meerschaum_server::{
        AppState,
        access::{self, Access},
        auth::{self, Scopes},
        backends::{ActionsBackend, ApiExecution, PlanError, PlanOptions},
        executor::{
            callbacks, context, environment, plugin_host::Placement, plugins,
            source_ceiling::Ceiling,
        },
    };
    use serde_json::{Value as Json, json};
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::{Path, PathBuf},
        process::ExitCode,
        sync::Arc,
        time::{Duration, Instant},
    };
    use uuid::Uuid;
    const TEST: &str =
        "host_metadata_has_no_authority_uses_immutable_selected_files_and_fresh_whole_set_cache";
    pub(super) fn main() -> ExitCode {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
            || args.first().is_some_and(|a| a.starts_with("__mrsm_"))
        {
            return meerschaum_cli::run_main();
        }
        if args.iter().any(|a| a == "--list") {
            println!("{TEST}: test");
            return ExitCode::SUCCESS;
        }
        if args.iter().any(|a| a == "--ignored") && !args.iter().any(|a| a == "--include-ignored") {
            return ExitCode::SUCCESS;
        }
        let exact = args.iter().any(|argument| argument == "--exact");
        let mut selected = true;
        let mut values = args.iter();
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
        if !selected {
            return ExitCode::SUCCESS;
        }
        meerschaum_core::self_exe::register_native_current_executable().unwrap();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(acceptance());
        ExitCode::SUCCESS
    }
    fn map(value: Json) -> Map {
        serde_json::from_value::<Value>(value).unwrap().to_map()
    }
    struct Api;
    #[async_trait::async_trait]
    impl ActionsBackend for Api {
        fn api_execution(&self) -> ApiExecution {
            ApiExecution::Tracked { terminals: false }
        }
        fn list(&self) -> Vec<String> {
            vec!["show".into()]
        }
        async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
            SuccessTuple::fail("This fixture executes only canonical Metadata jobs.")
        }
    }
    struct Subreaper(Option<rustix::process::Pid>);
    impl Subreaper {
        fn new() -> Self {
            let previous = rustix::process::child_subreaper().unwrap();
            rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
            Self(previous)
        }
    }
    impl Drop for Subreaper {
        fn drop(&mut self) {
            let _ = rustix::process::set_child_subreaper(self.0);
        }
    }
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            use meerschaum_jobs::process;
            let mut identities = BTreeMap::new();
            let mut pending = Vec::new();
            for (pid, args) in process::process_cmdlines() {
                if args
                    .iter()
                    .any(|a| a == meerschaum_jobs::supervisor::SUPERVISOR_ARG)
                    && args.windows(2).any(|pair| {
                        pair[0] == "--jobs-dir" && Path::new(&pair[1]) == self.0.join("jobs")
                    })
                {
                    pending.push(pid);
                }
            }
            while let Some(pid) = pending.pop() {
                if identities.contains_key(&pid) {
                    continue;
                }
                if let Some(birth) = process::process_start_time(pid) {
                    identities.insert(pid, birth);
                    pending.extend(process::children(pid).unwrap_or_default());
                }
            }
            for (pid, birth) in identities {
                if process::process_start_time(pid) == Some(birth)
                    && matches!(
                        process::proc_state(pid),
                        process::ProcState::Alive | process::ProcState::Stopped
                    )
                {
                    let _ = process::send_signal(pid, process::Sig::Kill);
                }
                if let Some(pid) = rustix::process::Pid::from_raw(pid) {
                    let _ =
                        rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
                }
            }
        }
    }
    async fn caller(state: &AppState, name: &str) -> Access {
        let principal = auth::load_from_subject(state, name).await.unwrap().unwrap();
        let credential = auth::user_credential(state, &User::new(name, None))
            .await
            .unwrap()
            .unwrap();
        let mut caller = access::access_of_fresh(
            state,
            principal,
            &Scopes::List(vec!["actions:execute".into()]),
        )
        .await
        .unwrap();
        caller.credential = Some(credential);
        assert!(caller.has("actions:execute"));
        assert!(!caller.has("jobs:execute"));
        caller
    }
    async fn reader(state: &AppState, name: &str) -> Access {
        let principal = auth::load_from_subject(state, name).await.unwrap().unwrap();
        let mut caller =
            access::access_of_fresh(state, principal, &Scopes::List(vec!["pipes:read".into()]))
                .await
                .unwrap();
        caller.credential = auth::user_credential(state, &User::new(name, None))
            .await
            .unwrap();
        assert!(caller.has("pipes:read"));
        assert!(!caller.has("actions:execute") && !caller.has("jobs:execute"));
        caller
    }
    async fn saved_plan(
        instance: &dyn InstanceConnector,
        name: &str,
        source: &str,
        team: &str,
        owner: &str,
    ) {
        let pipe = Pipe::builder(source, name).instance("sql:main")
            .parameters(map(json!({"access":{"team":team,"owner":owner},"columns":{"datetime":"id"},"dtypes":{"id":"int64"}})))
            .build().unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
        let metadata = Pipe::builder("mrsm", "pipeline").location(Some(name)).instance("sql:main")
            .parameters(map(json!({"access":{"team":team,"owner":owner},"pipeline":{"id":Uuid::new_v4().to_string(),"name":name,"targets":[pipe.pipe_ref()],"graph":true}})))
            .build().unwrap();
        assert!(instance.register_pipe(&metadata).await.success);
    }
    async fn observe(controller: &Arc<Controller>, caller: &Access, name: &str) {
        let result = context::scope(
            Some(Arc::new(caller.clone())),
            controller.plan_metadata(name, "sql:main", &PlanOptions::default()),
        )
        .await
        .unwrap();
        let nodes = result["plan"]["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1, "{result}");
        assert_eq!(
            nodes[0]["action"], "observe",
            "A proven streaming source was classified as an ordinary batch: {result}"
        );
    }
    async fn unavailable_plan(controller: &Arc<Controller>, caller: &Access, name: &str) {
        let result = context::scope(
            Some(Arc::new(caller.clone())),
            controller.plan_metadata(name, "sql:main", &PlanOptions::default()),
        )
        .await;
        assert!(
            matches!(result, Err(PlanError::MetadataUnavailable(_))),
            "A reader launched discovery or guessed an unclassified source: {result:?}"
        );
    }
    fn private_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn walk(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
            let Ok(entries) = std::fs::read_dir(path) else {
                return;
            };
            for entry in entries {
                let entry = entry.unwrap();
                let metadata = entry.file_type().unwrap();
                assert!(!metadata.is_symlink());
                if metadata.is_dir() {
                    walk(root, &entry.path(), out)
                } else {
                    out.insert(
                        entry.path().strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(entry.path()).unwrap(),
                    );
                }
            }
        }
        let mut files = BTreeMap::new();
        for name in [
            "executor-plugin-files",
            "executor-plugin-metadata",
            "execution-contexts",
            "jobs",
        ] {
            walk(root, &root.join(name), &mut files)
        }
        files.insert(
            PathBuf::from("executor-host-identity"),
            std::fs::read(root.join("executor-host-identity")).unwrap(),
        );
        files
    }
    fn start(
        controller: &Arc<Controller>,
        caller: &Access,
        team: Option<Uuid>,
    ) -> tokio::task::JoinHandle<Result<plugins::Validated, String>> {
        let (controller, caller) = (controller.clone(), Arc::new(caller.clone()));
        tokio::spawn(async move {
            context::scope(
                Some(caller),
                context::action_request_scope(
                    controller.discover_host_plugin_catalog(
                        team,
                        BTreeSet::from(["manifest_probe".into()]),
                    ),
                ),
            )
            .await
        })
    }
    async fn ready(
        root: &Path,
        task: &mut tokio::task::JoinHandle<Result<plugins::Validated, String>>,
    ) -> (Record, PathBuf, PathBuf) {
        let until = Instant::now() + Duration::from_secs(25);
        loop {
            for record in Catalog::new(root)
                .records()
                .unwrap()
                .values()
                .filter(|r| r.discovery.is_some())
            {
                let id =
                    Uuid::parse_str(record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
                let directory = root
                    .join("execution-contexts")
                    .join(id.simple().to_string());
                if let Ok(entries) = std::fs::read_dir(&directory) {
                    for entry in entries.flatten() {
                        if entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("generation-")
                        {
                            let work = entry.path().join(".mrsm_team_work");
                            if work.join("plugin-ready").exists() {
                                return (record.clone(), work, entry.path());
                            }
                        }
                    }
                }
            }
            if task.is_finished() {
                let outcome = (&mut *task)
                    .await
                    .map(|result| result.map(|catalog| catalog.complete()));
                panic!("Host Metadata ended before its private readiness marker: {outcome:?}");
            }
            assert!(
                Instant::now() < until,
                "Host Metadata never reached its credential-free native worker."
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    async fn finish(
        task: tokio::task::JoinHandle<Result<plugins::Validated, String>>,
        work: &Path,
    ) -> plugins::Validated {
        std::fs::write(work.join("release-discovery"), "release").unwrap();
        tokio::time::timeout(Duration::from_secs(20), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }
    async fn acceptance() {
        let _subreaper = Subreaper::new();
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
        std::fs::create_dir_all(&parent).unwrap();
        let temp = tempfile::Builder::new()
            .prefix("host-plugin-discovery-")
            .tempdir_in(parent)
            .unwrap();
        let root = temp.path().canonicalize().unwrap();
        let _cleanup = Cleanup(root.clone());
        std::fs::create_dir(root.join("plugins")).unwrap();
        std::fs::write(root.join("main.secret"), "synthetic-host-metadata-secret").unwrap();
        let source = format!(
            r#"
import errno,json,os,pathlib,socket,stat,time,uuid
assert os.environ.get('MRSM_METADATA_EXECUTION')=='1','Main must not import Host sources'
assert os.environ.get('MRSM_API_EXECUTION')=='1'
state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
config=json.loads(pathlib.Path(os.environ['MRSM_CONFIG_FILE']).read_text())
text=json.dumps(config)
assert all(value not in text for value in ['synthetic-host-metadata-secret','synthetic-plugin-setting','sql:main','main.db','proxy-password'])
assert all(key not in os.environ for key in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','AWS_SECRET_ACCESS_KEY','DB_PASSWORD'])
assert os.environ.get('MRSM_NONINTERACTIVE')=='1'
assert stat.S_ISCHR(os.fstat(0).st_mode) and not os.isatty(0) and os.read(0,1)==b''
callback=config['meerschaum']['connectors']['api']['main_callback']
credential=json.loads(pathlib.Path(callback['credential_file']).read_text())
assert credential['token']=='mrsm-metadata-no-authority' and callback['uri']=='http://metadata.invalid'
for name,family,kind in [('tcp',socket.AF_INET,socket.SOCK_STREAM),('udp',socket.AF_INET6,socket.SOCK_DGRAM),('unix',socket.AF_UNIX,socket.SOCK_STREAM)]:
 try: socket.socket(family,kind)
 except OSError as error: assert error.errno in (errno.EACCES,errno.EPERM,errno.EAFNOSUPPORT)
 else: raise AssertionError('Metadata obtained network access: '+name)
for path in [{secret},{proc}]:
 try: open(path).read(1)
 except OSError as error: assert error.errno in (errno.EACCES,errno.EPERM)
 else: raise AssertionError('Metadata read Main-private state')
assert '/executor-plugin-files/objects/' in str(pathlib.Path(__file__).resolve())
assert not (state/'plugins/unselected.py').exists()
from meerschaum.plugins import make_action
@make_action
def host_catalog_probe(action=None,**kw):
 return True,'Discovered without invoking the action.'
host_catalog_probe.__doc__='Credential-free cache observation '+uuid.uuid4().hex
def subscribe(*args,**kw):
 raise AssertionError('Reader planning must never invoke a source function')
(state/'plugin-ready').write_text(json.dumps({{'source':str(pathlib.Path(__file__).resolve()),'no_authority':True}}))
until=time.monotonic()+25
while not (state/'release-discovery').exists():
 assert time.monotonic()<until,'Fixture did not release Host discovery'
 time.sleep(.02)
"#,
            secret = json!(root.join("main.secret")),
            proc = json!(format!("/proc/{}/environ", std::process::id()))
        );
        std::fs::write(root.join("plugins/manifest_probe.py"), &source).unwrap();
        std::fs::write(
            root.join("plugins/unselected.py"),
            "raise AssertionError('Unselected B must not be imported')\n",
        )
        .unwrap();
        let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let options = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.join("plugins").display().to_string(),
            ),
            (
                "MRSM_PYTHON".into(),
                std::env::var("MRSM_PYTHON").unwrap_or_else(|_| "/usr/bin/python3".into()),
            ),
            ("PYTHONPATH".into(), sdk.display().to_string()),
            ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
            ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
            (
                "HTTP_PROXY".into(),
                "http://owner:proxy-password@127.0.0.1:1".into(),
            ),
            (
                "AWS_SECRET_ACCESS_KEY".into(),
                "synthetic-host-metadata-secret".into(),
            ),
        ]);
        let snapshot = map(
            json!({"meerschaum":{"instance":"sql:main","connectors":{"sql":{"main":{"flavor":"sqlite","database":root.join("main.db"),"password":"synthetic-host-metadata-secret"}}}},"plugins":{"manifest_probe":{"setting":"synthetic-plugin-setting"}}}),
        );
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.join("main.db").display()),
            )
            .await
            .unwrap(),
        );
        for (name, primary) in [
            ("everyone_metadata", "Everyone"),
            ("team_metadata", "MetadataHost"),
        ] {
            let mut user = User::new(name, Some("synthetic-host-metadata-password"));
            user.attributes = Some(map(json!({"role":"operator","primary_team":primary})));
            assert!(instance.register_user(&mut user).await.success);
        }
        let mut team = Team::new("MetadataHost");
        team.attributes = map(
            json!({"isolated":true,"plugins":["manifest_probe"],"connectors":["sql:main"],"instance":"sql:main","execution":{"mode":"host"}}),
        );
        team.members
            .insert("team_metadata".into(), TeamRole::Member);
        let registered = instance.register_team(&mut team).await;
        assert!(registered.success, "{registered}");
        let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
        let controller = Arc::new(Controller::new(
            root.clone(),
            instance.clone(),
            options,
            snapshot.clone(),
        ));
        let mut server = meerschaum_server::ServerConfig::for_root("sql:main", &root);
        server.actions_non_admin = true;
        server.connectors = snapshot["meerschaum"].get("connectors").unwrap().to_map();
        let state = AppState::builder(server, instance.clone())
            .actions(Arc::new(Api))
            .build()
            .unwrap();
        controller.bind_state(&state).unwrap();
        controller
            .bind_host_jobs(meerschaum_jobs::JobsContext::new(
                meerschaum_jobs::config::JobPaths::for_root(&root, root.join("dot-config")),
                meerschaum_jobs::config::JobsConfig::default(),
                meerschaum_jobs::supervisor::Launcher {
                    supervisor: vec![
                        std::env::current_exe().unwrap().display().to_string(),
                        meerschaum_jobs::supervisor::SUPERVISOR_ARG.into(),
                    ],
                    program: vec![std::env::current_exe().unwrap().display().to_string()],
                },
            ))
            .unwrap();
        let everyone = caller(&state, "everyone_metadata").await;
        let mut first = start(&controller, &everyone, None);
        let (record, work, generation) = ready(&root, &mut first).await;
        assert_eq!(record.placement, JobPlacement::HostApi);
        assert_eq!(record.profile, environment::Profile::Metadata);
        assert!(record.team_id.is_nil());
        let deadline = record.discovery.as_ref().unwrap().deadline;
        let now = chrono::Utc::now().timestamp();
        assert!(deadline > now && deadline <= now + 60);
        let identity: Json = serde_json::from_slice(
            &std::fs::read(generation.parent().unwrap().join("identity.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(identity["identity"]["profile"], "metadata");
        assert_eq!(identity["identity"]["metadata_deadline"], deadline);
        let credential: Json =
            serde_json::from_slice(&std::fs::read(generation.join("credential.json")).unwrap())
                .unwrap();
        assert_eq!(credential["token"], environment::METADATA_TOKEN);
        assert!(credential["expires_at"].as_i64().unwrap() <= deadline);
        let complete = finish(first, &work).await;
        assert!(complete.complete());
        let observed = &complete.catalog().plugins["manifest_probe"]
            .metadata
            .as_ref()
            .unwrap()
            .actions;
        assert_eq!(observed.len(), 1);
        assert!(
            observed[0]
                .description
                .starts_with("Credential-free cache observation ")
        );
        assert_eq!(
            observed[0].description.trim().len(),
            "Credential-free cache observation ".len() + 32
        );
        let Placement::Host(host) = &complete.catalog().placement else {
            panic!("Host metadata claimed remote archives")
        };
        assert!(host.team_id.is_none());
        assert_eq!(
            complete
                .catalog()
                .plugins
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["manifest_probe"]
        );
        assert!(!generation.exists());
        let cached = start(&controller, &everyone, None).await.unwrap().unwrap();
        assert_eq!(
            cached.catalog(),
            complete.catalog(),
            "The exact fresh whole-set cache should retain the observed random action documentation."
        );
        let team_caller = caller(&state, "team_metadata").await;
        let mut request = start(&controller, &team_caller, Some(team_id));
        let (record, work, _) = ready(&root, &mut request).await;
        assert_eq!(record.team_id, team_id);
        let team_catalog = finish(request, &work).await;
        let Placement::Host(host) = &team_catalog.catalog().placement else {
            panic!("Host Team metadata claimed remote placement")
        };
        assert_eq!(host.team_id, Some(team_id));
        // Cache-only planning uses the actually attested credential-free observation above.
        // The reader has no action/job permission, and the selected source function panics
        // if anything attempts to invoke it rather than reading this inert classification.
        saved_plan(
            instance.as_ref(),
            "team-reader",
            "plugin:manifest_probe",
            "MetadataHost",
            "team_metadata",
        )
        .await;
        saved_plan(
            instance.as_ref(),
            "everyone-reader",
            "plugin:manifest_probe",
            "Everyone",
            "everyone_metadata",
        )
        .await;
        saved_plan(
            instance.as_ref(),
            "new-source-reader",
            "plugin:unselected",
            "Everyone",
            "everyone_metadata",
        )
        .await;
        let team_reader = reader(&state, "team_metadata").await;
        let everyone_reader = reader(&state, "everyone_metadata").await;
        let files_before = private_files(&root);
        let registrations_before = instance
            .registered_pipes(&Default::default())
            .await
            .unwrap();
        let schema_before = meerschaum_connector::pipe_ops::frame_to_docs(
            &instance
                .read("SELECT name, sql FROM sqlite_master ORDER BY name")
                .await
                .unwrap(),
        );
        observe(&controller, &team_reader, "team-reader").await;
        let cache_path = root.join("executor-plugin-metadata").join(format!(
            "v{}-{}.json",
            plugins::PRODUCER_SCHEMA,
            team_catalog.source_digest().unwrap()
        ));
        let original_cache = std::fs::read(&cache_path).unwrap();
        let mut forged: Json = serde_json::from_slice(&original_cache).unwrap();
        for profile in ["api", "team"] {
            forged["profile"] = json!(profile);
            std::fs::write(&cache_path, serde_json::to_vec(&forged).unwrap()).unwrap();
            unavailable_plan(&controller, &team_reader, "team-reader").await;
        }
        std::fs::write(&cache_path, &original_cache).unwrap();
        let reader_parent = callbacks::register_with_sources(
            &state,
            &everyone_reader,
            Uuid::new_v4(),
            None,
            &["pipes:read"],
            callbacks::Purpose::Command,
            Ceiling::from_catalog(complete.catalog()).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reader_parent.depth, 0,
            "This must exercise a first-generation callback, not only nested depth."
        );
        let parent_identity =
            callbacks::load_identity(&state, reader_parent.id, reader_parent.launch_id)
                .await
                .unwrap();
        let execution_reader = parent_identity.caller();
        observe(&controller, &execution_reader, "everyone-reader").await;
        unavailable_plan(&controller, &execution_reader, "new-source-reader").await;
        // A direct Everyone reader currently selects A+B. The A-only cache is not an exact
        // whole-source-set derivation for that request, and cannot trigger discovery.
        unavailable_plan(&controller, &everyone_reader, "everyone-reader").await;
        callbacks::revoke(&state, reader_parent.id).await.unwrap();
        assert_eq!(
            private_files(&root),
            files_before,
            "Inert reader planning changed runtime, source-store, cache or Host identity files."
        );
        assert!(Catalog::new(&root).records().unwrap().is_empty());
        assert_eq!(
            instance
                .registered_pipes(&Default::default())
                .await
                .unwrap(),
            registrations_before
        );
        assert_eq!(
            meerschaum_connector::pipe_ops::frame_to_docs(
                &instance
                    .read("SELECT name, sql FROM sqlite_master ORDER BY name")
                    .await
                    .unwrap()
            ),
            schema_before,
            "Reader classification created engine state."
        );
        let parent = callbacks::register_with_sources(
            &state,
            &everyone,
            Uuid::new_v4(),
            None,
            &["actions:execute"],
            callbacks::Purpose::Command,
            Ceiling::from_catalog(complete.catalog()).unwrap(),
        )
        .await
        .unwrap();
        let identity = callbacks::load_identity(&state, parent.id, parent.launch_id)
            .await
            .unwrap();
        let child = Arc::new(identity.caller());
        let refused = context::scope(
            Some(child),
            context::action_request_scope(
                controller
                    .discover_host_plugin_catalog(None, BTreeSet::from(["unselected".into()])),
            ),
        )
        .await;
        assert!(refused.is_err());
        callbacks::revoke(&state, parent.id).await.unwrap();
        std::fs::write(
            root.join("plugins/manifest_probe.py"),
            format!("{source}\n# new immutable view\n"),
        )
        .unwrap();
        let mut request = start(&controller, &everyone, None);
        let (record, work, _) = ready(&root, &mut request).await;
        std::fs::write(
            root.join("plugins/manifest_probe.py"),
            format!("{source}\n# changed during discovery\n"),
        )
        .unwrap();
        std::fs::write(work.join("release-discovery"), "release").unwrap();
        assert!(
            request.await.unwrap().is_err(),
            "A changed original source must not publish a stale catalog"
        );
        assert!(
            !root
                .join("executors/callbacks")
                .join(format!("{}.json", record.callback.unwrap()))
                .exists()
        );
        std::fs::write(
            root.join("plugins/manifest_probe.py"),
            format!("{source}\n# canceled driver\n"),
        )
        .unwrap();
        let mut canceled = start(&controller, &everyone, None);
        let (cancel_record, _, cancel_generation) = ready(&root, &mut canceled).await;
        let id = Uuid::parse_str(cancel_record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
        let lifecycle: Json = serde_json::from_slice(
            &std::fs::read(
                root.join("execution-contexts")
                    .join(id.simple().to_string())
                    .join("job-lifecycle-1.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let mut pending = vec![
            lifecycle["supervisor"]["pid"].as_i64().unwrap() as i32,
            lifecycle["worker"]["pid"].as_i64().unwrap() as i32,
        ];
        let mut owned = BTreeMap::new();
        while let Some(pid) = pending.pop() {
            if owned.contains_key(&pid) {
                continue;
            }
            if let Some(birth) = meerschaum_jobs::process::process_start_time(pid) {
                owned.insert(pid, birth);
                pending.extend(meerschaum_jobs::process::children(pid).unwrap_or_default());
            }
        }
        assert!(owned.len() >= 2);
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let stopped = owned.iter().all(|(pid, birth)| {
                meerschaum_jobs::process::process_start_time(*pid) != Some(*birth)
                    || !matches!(
                        meerschaum_jobs::process::proc_state(*pid),
                        meerschaum_jobs::process::ProcState::Alive
                            | meerschaum_jobs::process::ProcState::Stopped
                    )
            });
            let revoked = !root
                .join("executors/callbacks")
                .join(format!("{}.json", cancel_record.callback.unwrap()))
                .exists();
            if stopped && revoked && !cancel_generation.exists() {
                break;
            }
            assert!(
                Instant::now() < until,
                "Canceled Host metadata retained its own active child, source proof or generation."
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        // A request canceled while an owned blocking reservation is waiting must not leave
        // a later completed reservation or its already-created original-source callback.
        std::fs::write(
            root.join("plugins/manifest_probe.py"),
            format!("{source}\n# cancel reservation\n"),
        )
        .unwrap();
        let catalog_lock = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("executors/catalog.lock"))
            .unwrap();
        fs4::FileExt::lock(&catalog_lock).unwrap();
        let reservation = start(&controller, &everyone, None);
        let until = Instant::now() + Duration::from_secs(10);
        let callback_path = loop {
            let found = std::fs::read_dir(root.join("executors/callbacks"))
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| {
                    path.file_stem()
                        .and_then(|name| name.to_str())
                        .and_then(|name| Uuid::parse_str(name).ok())
                        .is_some()
                        && path.extension().is_some_and(|ext| ext == "json")
                });
            if let Some(path) = found {
                break path;
            }
            assert!(
                !reservation.is_finished() && Instant::now() < until,
                "Host metadata did not capture its source before reservation cancellation."
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        tokio::time::sleep(Duration::from_millis(150)).await;
        reservation.abort();
        assert!(reservation.await.unwrap_err().is_cancelled());
        drop(catalog_lock);
        let until = Instant::now() + Duration::from_secs(10);
        while callback_path.exists() || !Catalog::new(&root).records().unwrap().is_empty() {
            assert!(
                Instant::now() < until,
                "Canceled owned reservation leaked its callback or canonical row."
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut revoked = User::new("everyone_metadata", None);
        revoked.attributes = Some(map(json!({"role":"viewer","primary_team":"Everyone"})));
        assert!(instance.edit_user(&revoked).await.success);
        assert!(
            start(&controller, &everyone, None).await.unwrap().is_err(),
            "A retained original source must not regain removed action authority from cached metadata."
        );
        assert!(Catalog::new(&root).records().unwrap().is_empty());
        println!(
            "Host Metadata: immutable copied sources, no data/bearer/environment/network/stdin authority, genuine Everyone/Team identity, cache and fresh-source refusal passed."
        );
    }
}
