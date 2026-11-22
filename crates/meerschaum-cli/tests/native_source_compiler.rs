//! Actual offline native compilation inside a sealed Metadata generation, never in Main.
//! Explicit gate: supply MRSM_TEST_EXECUTOR_COMPILER_ROOT and
//! MRSM_TEST_EXECUTOR_COMPILER_SHA256, then run this target with --ignored.
#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    fixture::main()
}
#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
mod fixture {
    use meerschaum_cli::{executor::client::Controller, executor::jobs::Catalog};
    use meerschaum_core::{Map, Value, config::ConfigOptions, users::User};
    use meerschaum_server::{
        AppState, access, auth,
        executor::{context, plugin_handlers, plugins},
    };
    use serde_json::{Value as Json, json};
    use std::{collections::BTreeMap, path::PathBuf, process::ExitCode, sync::Arc, time::Duration};

    const TEST: &str = "owned_offline_native_metadata_compiles_inside_its_sealed_generation";

    pub(super) fn main() -> ExitCode {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
            || args.first().is_some_and(|arg| arg.starts_with("__mrsm_"))
        {
            return meerschaum_cli::run_main();
        }
        if args.iter().any(|arg| arg == "--list") {
            println!("{TEST}: test");
            return ExitCode::SUCCESS;
        }
        if !args
            .iter()
            .any(|arg| matches!(arg.as_str(), "--ignored" | "--include-ignored"))
        {
            println!("{TEST}: ignored (requires an explicitly owned pinned offline compiler)");
            return ExitCode::SUCCESS;
        }
        let exact = args.iter().any(|arg| arg == "--exact");
        let mut values = args.iter();
        while let Some(arg) = values.next() {
            if matches!(arg.as_str(), "--format" | "--test-threads" | "--color") {
                let _ = values.next();
            } else if !arg.starts_with('-')
                && if exact {
                    arg != TEST
                } else {
                    !TEST.contains(arg)
                }
            {
                return ExitCode::SUCCESS;
            }
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
    impl meerschaum_server::backends::ActionsBackend for Api {
        fn api_execution(&self) -> meerschaum_server::backends::ApiExecution {
            meerschaum_server::backends::ApiExecution::Tracked { terminals: false }
        }
        fn list(&self) -> Vec<String> {
            vec!["show".into()]
        }
        async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
            panic!("Main must never dispatch native source code")
        }
    }

    struct MainServer(tokio::task::JoinHandle<()>);
    impl Drop for MainServer {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    struct OwnedProcesses {
        root: PathBuf,
        previous_subreaper: Option<rustix::process::Pid>,
    }
    impl OwnedProcesses {
        fn new(root: PathBuf) -> Self {
            let previous_subreaper = rustix::process::child_subreaper().unwrap();
            rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
            Self {
                root,
                previous_subreaper,
            }
        }
    }
    impl Drop for OwnedProcesses {
        fn drop(&mut self) {
            use meerschaum_jobs::process;
            let mut identities = BTreeMap::new();
            let mut pending: Vec<_> = process::process_cmdlines()
                .into_iter()
                .filter(|(_, args)| {
                    args.iter()
                        .any(|arg| arg == meerschaum_jobs::supervisor::SUPERVISOR_ARG)
                        && args.windows(2).any(|pair| {
                            pair[0] == "--jobs-dir"
                                && std::path::Path::new(&pair[1]) == self.root.join("jobs")
                        })
                })
                .map(|(pid, _)| pid)
                .collect();
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
            let _ = rustix::process::set_child_subreaper(self.previous_subreaper);
        }
    }

    async fn acceptance() {
        let snapshot = PathBuf::from(
            std::env::var_os("MRSM_TEST_EXECUTOR_COMPILER_ROOT")
                .expect("provide the explicitly owned immutable compiler snapshot"),
        );
        let compiler_sha = std::env::var("MRSM_TEST_EXECUTOR_COMPILER_SHA256")
            .expect("provide its exact host-pinned manifest SHA256");
        assert!(snapshot.is_absolute());
        assert!(plugins::valid_digest(&compiler_sha));
        let parent =
            PathBuf::from(std::env::var_os("MRSM_ROOT_DIR").expect("use the isolated test runner"));
        for configured in [false, true] {
            let temporary = tempfile::Builder::new()
                .prefix("native-source-compiler-")
                .tempdir_in(&parent)
                .unwrap();
            let root = temporary.path().canonicalize().unwrap();
            let _owned = OwnedProcesses::new(root.clone());
            std::fs::create_dir(root.join("plugins")).unwrap();
            std::fs::write(
                root.join("main.secret"),
                "synthetic-native-compiler-private",
            )
            .unwrap();
            std::fs::write(
                root.join("plugins/native_probe.rs"),
                include_str!("fixtures/executor_extension_native.rs"),
            )
            .unwrap();
            let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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
                    std::env::var("MRSM_PYTHON").expect("use the prepared private SDK"),
                ),
                ("PYTHONPATH".into(), repository.display().to_string()),
                ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
                ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
                (
                    "AWS_SECRET_ACCESS_KEY".into(),
                    "synthetic-native-compiler-private".into(),
                ),
            ]);
            let instance = Arc::new(
                meerschaum_connector_sql::SqlConnector::from_uri(
                    "main",
                    &format!("sqlite:///{}", root.join("main.db").display()),
                )
                .await
                .unwrap(),
            );
            let mut user = User::new(
                "native_metadata",
                Some("synthetic-native-compiler-password"),
            );
            user.attributes = Some(map(json!({"role":"operator","primary_team":"Everyone"})));
            assert!(instance.register_user(&mut user).await.success);
            let mut reader = User::new("native_reader", Some("synthetic-native-reader-password"));
            reader.attributes = Some(meerschaum_core::access::attributes_with_permissions(
                &map(json!({"role":"operator","primary_team":"Everyone"})),
                &["plugins:read", "pipes:read", "users:read"],
            ));
            assert!(instance.register_user(&mut reader).await.success);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let controller = Arc::new(Controller::new(
                root.clone(),
                instance.clone(),
                options,
                map(
                    json!({"meerschaum":{"instance":"sql:main","connectors":{"sql":{"main":{
                        "flavor":"sqlite","database":root.join("main.db"),"password":"synthetic-native-compiler-private"
                    }}}},"api":{"execution":{"callback_url":origin}}}),
                ),
            ));
            let mut server = meerschaum_server::ServerConfig::for_root("sql:main", &root);
            server.connectors = map(json!({"sql":{"main":{
                "flavor":"sqlite", "database":root.join("main.db"),
                "password":"synthetic-native-compiler-private"
            }}}));
            server.actions_non_admin = true;
            server.execution_callback_url = Some(origin);
            let state = AppState::builder(server, instance.clone())
                .actions(Arc::new(Api))
                .build()
                .unwrap();
            controller.bind_state(&state).unwrap();
            let app = meerschaum_server::app(state.clone());
            let _server = MainServer(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
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
            if configured {
                assert!(
                    controller
                        .bind_host_source_compiler(
                            &snapshot,
                            &"0".repeat(64),
                            std::path::Path::new("/usr/lib")
                        )
                        .is_err()
                );
                controller
                    .bind_host_source_compiler(
                        &snapshot,
                        &compiler_sha,
                        std::path::Path::new("/usr/lib"),
                    )
                    .unwrap();
            }
            let principal = auth::load_from_subject(&state, "native_metadata")
                .await
                .unwrap()
                .unwrap();
            let mut caller = access::access_of_fresh(
                &state,
                principal,
                &auth::Scopes::List(vec!["actions:execute".into()]),
            )
            .await
            .unwrap();
            caller.credential = auth::user_credential(&state, &User::new("native_metadata", None))
                .await
                .unwrap();
            let observed = tokio::time::timeout(
                Duration::from_secs(65),
                context::scope(
                    Some(Arc::new(caller.clone())),
                    context::action_request_scope(controller.discover_host_plugin_catalog(
                        None,
                        std::collections::BTreeSet::from(["native_probe".into()]),
                    )),
                ),
            )
            .await
            .expect("the sealed native compilation exceeded its total owner budget");
            if !configured {
                let error = observed.expect_err("Rust discovery used an ambient compiler");
                assert!(
                    error.contains("host-selected attested offline compiler"),
                    "{error}"
                );
                assert!(Catalog::new(&root).records().unwrap().is_empty());
                continue;
            }
            let catalog =
                observed.unwrap_or_else(|error| panic!("actual confined Rust discovery: {error}"));
            assert!(catalog.complete());
            let plugin = &catalog.catalog().plugins["native_probe"];
            assert_eq!(plugin.kind, plugins::SourceKind::NativeSource);
            let registry = plugin.metadata.as_ref().unwrap().registry.as_ref().unwrap();
            assert_eq!(registry.handlers().count(), 5, "{registry:?}");
            let digest = catalog.source_digest().unwrap();
            let cache = root.join("executor-plugin-metadata").join(format!(
                "v{}-{digest}-compiler-{compiler_sha}.json",
                plugins::PRODUCER_SCHEMA
            ));
            let cached: Json = serde_json::from_slice(&std::fs::read(cache).unwrap()).unwrap();
            assert_eq!(cached["compiler_sha256"], compiler_sha);
            assert_eq!(cached["signed_source_sha256"], digest);
            assert!(Catalog::new(&root).records().unwrap().is_empty());
            println!(
                "Actual sealed Metadata compiled and loaded its pinned offline Rust API/MCP/page declarations; no compiler was invoked by Main."
            );
            let principal = auth::load_from_subject(&state, "native_reader")
                .await
                .unwrap()
                .unwrap();
            let mut reader = access::access_of_fresh(
                &state,
                principal,
                &auth::Scopes::List(vec![
                    "plugins:read".into(),
                    "pipes:read".into(),
                    "users:read".into(),
                ]),
            )
            .await
            .unwrap();
            reader.credential = auth::user_credential(&state, &User::new("native_reader", None))
                .await
                .unwrap();
            assert!(!reader.has("actions:execute") && !reader.has("jobs:execute"));
            for handler in registry.handlers() {
                let request = match &handler.selector {
                    plugin_handlers::Selector::Api { .. } => plugin_handlers::Request::Api {
                        method: "POST".into(),
                        path: "/native-extension-probe/owned".into(),
                        query: vec![(
                            "main_sentinel".into(),
                            root.join("main.secret").display().to_string(),
                        )],
                        headers: Default::default(),
                        body: "{}".into(),
                    },
                    plugin_handlers::Selector::McpTool { .. } => plugin_handlers::Request::Mcp {
                        arguments: json!({"text":"typed-native"}),
                    },
                    plugin_handlers::Selector::McpPrompt { .. } => plugin_handlers::Request::Mcp {
                        arguments: json!({"topic":"native-topic"}),
                    },
                    plugin_handlers::Selector::McpResource { .. } => {
                        plugin_handlers::Request::Mcp {
                            arguments: json!({"uri":"mrsm://native-extension/resource", "variables":{}}),
                        }
                    }
                    plugin_handlers::Selector::Page { .. } => {
                        plugin_handlers::Request::Page { query: Vec::new() }
                    }
                    other => panic!("unexpected native handler: {other:?}"),
                };
                let reply = context::scope(
                    Some(Arc::new(reader.clone())),
                    controller.extension_invoke(plugin_handlers::Invocation {
                        catalog_sha256: registry.source_catalog_sha256.clone(),
                        plugin: "native_probe".into(),
                        handler_id: handler.id.clone(),
                        request,
                    }),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("actual private native {:?}: {error}", handler.selector)
                });
                match (&handler.selector, reply) {
                    (
                        plugin_handlers::Selector::Api { .. },
                        plugin_handlers::Reply::Api { status, body },
                    ) => {
                        assert_eq!(status, 200);
                        let value: Json = serde_json::from_str(&body).unwrap();
                        assert_eq!(value["item"], "owned");
                        for key in [
                            "api",
                            "main_denied",
                            "main_sql_absent",
                            "parent_env_absent",
                            "credential_headers_absent",
                        ] {
                            assert_eq!(value[key], true, "{key}: {value}");
                        }
                    }
                    (
                        plugin_handlers::Selector::McpTool { .. },
                        plugin_handlers::Reply::Json { value },
                    ) => {
                        assert_eq!(value["text"], "typed-native");
                        assert_eq!(value["api"], true);
                    }
                    (plugin_handlers::Selector::McpResource { .. }, reply) => assert!(
                        serde_json::to_string(&reply)
                            .unwrap()
                            .contains("native confined resource")
                    ),
                    (plugin_handlers::Selector::McpPrompt { .. }, reply) => assert!(
                        serde_json::to_string(&reply)
                            .unwrap()
                            .contains("native-topic")
                    ),
                    (
                        plugin_handlers::Selector::Page { .. },
                        plugin_handlers::Reply::Page {
                            fragment, status, ..
                        },
                    ) => {
                        assert_eq!(status, 200);
                        assert!(fragment.contains("Main SQL absent: true"), "{fragment}");
                    }
                    (_, reply) => panic!("unexpected native reply: {reply:?}"),
                }
            }
            let records = Catalog::new(&root).records().unwrap();
            assert!(!records.is_empty());
            for (_, record) in records {
                assert_eq!(
                    record.placement,
                    meerschaum_cli::executor::jobs::JobPlacement::HostApi
                );
                assert_eq!(
                    record.profile,
                    meerschaum_server::executor::environment::Profile::Api
                );
                assert!(record.extension.is_some());
            }
            println!(
                "Actual Host/Everyone native API, MCP tool/resource/prompt and page calls used private Api jobs without action/job permissions."
            );
        }
    }
}
