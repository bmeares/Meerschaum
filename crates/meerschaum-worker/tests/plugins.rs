//! Plugin actions, sync hooks, `setup()`, plugin `sync()` with callbacks, and the manifest.

mod common;

use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value as CValue};
use meerschaum_sql::SqlConnector;
use meerschaum_worker::manifest::{ManifestSource, ensure_manifest, manifest_path, stale_plugins};
use meerschaum_worker::plugins::{
    ActionRequest, HookPhase, SyncHookContext, run_action, run_sync_hooks, setup_plugin,
};
use meerschaum_worker::sync::{PythonSyncOptions, sync_python_pipe};
use meerschaum_worker::{CallOptions, PythonConnector};

use common::{TestEnv, assert_same_data, v4_compare};

/// The `SuccessTuple` v4's CLI (`python -m meerschaum <sysargs>`) prints, each case in a fresh
/// interpreter.
///
/// PARITY (deliberate deviation): v4's CLI runs a module-named plugin action
/// (`def <plugin_name>()` in the plugin, registered by `load_plugins`) only while the plugin's
/// venv does not exist yet; once it exists, `mrsm wk hooks` falls through to `sh` (exit code
/// 127). The worker always runs the action; those cases are asserted separately below.
fn v4_entry(env: &TestEnv, cases: &[Vec<&str>]) -> Vec<SuccessTuple> {
    let code = r#"
import json, sys
import meerschaum.__main__ as cli
captured = []
cli._print_tuple = lambda t, **kw: captured.append(t)
try:
    cli.main(json.loads(sys.argv[1]))
except SystemExit:
    pass
success, msg = captured[0]
print('\n@@JSON@@' + json.dumps([success, msg]))
"#;
    cases
        .iter()
        .map(|case| {
            let mut cmd = env.python_command(code);
            cmd.arg(serde_json::to_string(case).unwrap());
            let out = cmd.output().unwrap();
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success(),
                "{stdout}\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let (success, message): (bool, String) =
                serde_json::from_str(stdout.rsplit("@@JSON@@").next().unwrap()).unwrap();
            SuccessTuple { success, message }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_actions_match_v4() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let cases: Vec<Vec<&str>> = vec![
        vec!["wk", "hello"],
        vec!["wk", "hello", "there", "--wk-flag", "5"],
        vec!["wk", "hello", "--debug"],
    ];
    let expected = v4_entry(&env, &cases);
    for (case, want) in cases.iter().zip(expected) {
        let got = run_action(
            &pool,
            &ActionRequest::Sysargs(case.iter().map(|s| s.to_string()).collect()),
            None,
            CallOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(got, want, "{case:?}");
    }

    // Module-named actions (see `v4_entry`).
    for (case, want) in [
        (vec!["wk", "hooks"], "module action"),
        (vec!["wk", "hooks", "sub"], "module action"),
    ] {
        let got = run_action(
            &pool,
            &ActionRequest::Sysargs(case.iter().map(|s| s.to_string()).collect()),
            None,
            CallOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(got, SuccessTuple::ok(want), "{case:?}");
    }

    // Already-parsed keyword arguments (`action` as words, as v4's parser produces).
    let mut kw = Map::new();
    kw.insert(
        "action".into(),
        CValue::List(vec![
            CValue::Str("wk".into()),
            CValue::Str("hello".into()),
            CValue::Str("x".into()),
        ]),
    );
    kw.insert("wk_flag".into(), CValue::Int(7));
    let got = run_action(
        &pool,
        &ActionRequest::Kwargs(kw),
        None,
        CallOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(got.message, "hello ['x'] 7");

    // Core actions are refused.
    let err = run_action(
        &pool,
        &ActionRequest::Sysargs(vec!["show".into(), "version".into()]),
        None,
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not a plugin action"), "{err}");
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_matches_v4() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let out =
        env.run_python("import meerschaum as mrsm\nprint(repr(mrsm.Plugin('wk_hooks').setup()))\n");
    let got = setup_plugin(&pool, "wk_hooks", &[], &Map::new())
        .await
        .unwrap();
    let want = out.trim().lines().last().unwrap().to_string();
    let got_repr = format!(
        "({}, '{}')",
        if got.success { "True" } else { "False" },
        got.message
    );
    assert!(
        want == got_repr || (want == "True" && got.success),
        "v4: {want}, worker: {got:?}"
    );
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_hooks_receive_v4_kwargs() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let pipe = Pipe::builder("plugin:wk_shapes", "docs")
        .instance("sql:main")
        .build()
        .unwrap();
    let now = chrono::Utc::now();

    let pre = run_sync_hooks(
        &pool,
        &pipe,
        HookPhase::Pre,
        &SyncHookContext {
            sync_timestamp: Some(now),
            ..Default::default()
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(pre.len(), 1, "{pre:?}");
    assert_eq!(pre[0].plugin.as_deref(), Some("wk_hooks"));
    assert_eq!(pre[0].hook, "wk_pre");
    assert_eq!(pre[0].result, SuccessTuple::ok("pre docs datetime"));

    let post = run_sync_hooks(
        &pool,
        &pipe,
        HookPhase::Post,
        &SyncHookContext {
            sync_timestamp: Some(now),
            success_tuple: Some(SuccessTuple::ok("Inserted 3 rows.")),
            sync_duration: Some(1.5),
            sync_complete_timestamp: Some(now),
            extra: Map::new(),
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(post.len(), 2, "{post:?}");
    assert_eq!(
        post[0].result,
        SuccessTuple::ok("post docs True Inserted 3 rows. float datetime")
    );
    assert_eq!(post[1].hook, "wk_post_broken");
    assert!(!post[1].result.success);
    assert!(
        post[1].result.message.starts_with(
            "Failed to execute sync hook 'wk_post_broken' from plugin 'wk_hooks':\nTraceback"
        ),
        "{}",
        post[1].result.message
    );
    assert!(post[1].result.message.contains("synthetic hook failure"));

    // Only the named plugins' hooks run.
    let none = run_sync_hooks(
        &pool,
        &pipe,
        HookPhase::Pre,
        &SyncHookContext::default(),
        Some(&["wk_shapes".to_string()]),
        None,
    )
    .await
    .unwrap();
    assert!(none.is_empty());
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_sync_callbacks_match_v4() {
    require_python!();
    let dir = tempfile::tempdir().unwrap();
    let rs_uri = format!("sqlite:///{}", dir.path().join("rs.db").display());
    let env = TestEnv::with_env(vec![
        ("MRSM_SQL_RS".into(), rs_uri.clone()),
        (
            "MRSM_SQL_V4REF".into(),
            format!("sqlite:///{}", dir.path().join("v4.db").display()),
        ),
    ]);
    let pool = env.pool(1);
    let source = PythonConnector::connect("plugin:wk_syncer", pool.clone())
        .await
        .unwrap();
    assert!(source.info().has_sync);
    let instance: Arc<dyn InstanceConnector> =
        Arc::new(SqlConnector::from_uri("rs", &rs_uri).await.unwrap());
    let mut pipe = Pipe::builder("plugin:wk_syncer", "batches")
        .instance("sql:rs")
        .build()
        .unwrap();
    let st = sync_python_pipe(&source, instance, &mut pipe, &PythonSyncOptions::default()).await;
    assert!(st.success, "{}", st.message);
    assert!(st.message.starts_with("synced twice: "), "{}", st.message);

    let result = v4_compare(
        &env,
        "plugin:wk_syncer",
        &["batches"],
        1,
        "sql:v4ref",
        "sql:rs",
    );
    assert_same_data(&result, &["batches"], "sql:v4ref", "sql:rs");
    assert_eq!(
        result["data"]["batches"]["sql:rs"]["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_is_built_cached_and_rebuilt_when_stale() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let path = manifest_path(env.root.path());
    let dirs = env.plugins_dirs();

    let (manifest, source) = ensure_manifest(&pool, &path, &dirs).await.unwrap();
    assert_eq!(source, ManifestSource::Built);
    assert!(path.exists());
    let spawned = pool.stats().spawned;

    // v4's `get_plugins_names()` order.
    let mut names: Vec<&str> = manifest.plugins.keys().map(String::as_str).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "wk_custom",
            "wk_hooks",
            "wk_shapes",
            "wk_streamer",
            "wk_syncer"
        ]
    );
    let hooks = &manifest.plugins["wk_hooks"];
    assert_eq!(hooks.version.as_deref(), Some("2.1.0"));
    assert_eq!(hooks.required, vec!["wk_dependency>=0.1"]);
    assert!(hooks.import_error.is_none(), "{:?}", hooks.import_error);
    let hello = hooks
        .actions
        .iter()
        .find(|a| a.name == "wk_hello")
        .expect("wk_hello");
    assert_eq!(hello.doc.as_deref(), Some("Say hello from a worker."));
    assert!(hooks.actions.iter().any(|a| a.name == "wk_crash"));
    let flag = hooks
        .arguments
        .iter()
        .find(|a| a.option_strings == vec!["--wk-flag"])
        .expect("--wk-flag");
    assert_eq!(flag.type_name.as_deref(), Some("int"));
    assert_eq!(flag.help.as_deref(), Some("A worker test flag."));
    assert_eq!(hooks.pre_sync_hooks, vec!["wk_pre"]);
    assert_eq!(hooks.post_sync_hooks, vec!["wk_post", "wk_post_broken"]);
    assert_eq!(hooks.api_plugins, vec!["wk_api"]);
    assert_eq!(hooks.dash_plugins, vec!["wk_dash"]);
    assert!(
        hooks.web_pages.iter().any(|p| p.path.ends_with("wk/top")),
        "{:?}",
        hooks.web_pages
    );
    let top = hooks
        .web_pages
        .iter()
        .find(|p| p.path.ends_with("wk/top"))
        .unwrap();
    assert_eq!(top.mode, "dash");
    assert_eq!(top.route.as_deref(), Some("/dash/wk_hooks/wk/top"));
    let html = hooks
        .web_pages
        .iter()
        .find(|p| p.path.ends_with("wk/html"))
        .unwrap();
    assert_eq!(html.mode, "html");
    assert_eq!(html.route.as_deref(), Some("/_pages/wk/html"));
    assert_eq!(hooks.mcp_tools.len(), 1);
    assert_eq!(hooks.mcp_tools[0].name, "wk_tool");
    assert_eq!(hooks.mcp_tools[0].scopes, vec!["pipes:read"]);
    assert_eq!(hooks.mcp_resources.len(), 1);
    assert_eq!(
        manifest.plugins["wk_shapes"].data_functions,
        vec!["fetch", "register"]
    );
    assert_eq!(
        manifest.plugins["wk_syncer"].data_functions,
        vec!["sync", "register"]
    );
    let wkmem = manifest.connector_type("wkmem").expect("wkmem");
    assert_eq!(wkmem.plugin, "wk_custom");
    assert!(wkmem.is_instance);
    assert!(wkmem.methods.contains(&"sync_pipe".to_string()));
    assert!(!manifest.connector_type("wkfeed").unwrap().is_instance);
    assert!(manifest.has_sync_hooks());
    assert!(manifest.needs_api_host());
    assert_eq!(manifest.api_plugins, vec!["wk_hooks"]);
    assert_eq!(manifest.mcp_tools, vec!["wk_hooks"]);
    assert!(manifest.action_plugin("wk_hello").map(|p| p.name.as_str()) == Some("wk_hooks"));
    assert!(
        manifest.load_errors.is_empty(),
        "{:?}",
        manifest.load_errors
    );

    // Fresh: served from the cache without starting Python.
    let (cached, source) = ensure_manifest(&pool, &path, &dirs).await.unwrap();
    assert_eq!(source, ManifestSource::Cache);
    assert_eq!(cached, manifest);
    assert_eq!(pool.stats().spawned, spawned);

    // Editing a plugin makes it stale.
    let file = env.plugins.path().join("wk_shapes.py");
    let mut text = std::fs::read_to_string(&file).unwrap();
    text.push_str("\n# edited\n");
    std::fs::write(&file, text).unwrap();
    let (stale, changed) = stale_plugins(&cached, &dirs);
    assert!(stale);
    assert_eq!(changed, vec!["wk_shapes"]);
    let (_, source) = ensure_manifest(&pool, &path, &dirs).await.unwrap();
    assert_eq!(source, ManifestSource::Built);

    // A new plugin also makes it stale.
    std::fs::write(
        env.plugins.path().join("wk_new.py"),
        "def fetch(pipe, **kw):\n    return []\n",
    )
    .unwrap();
    let (rebuilt, source) = ensure_manifest(&pool, &path, &dirs).await.unwrap();
    assert_eq!(source, ManifestSource::Built);
    assert_eq!(rebuilt.plugins["wk_new"].data_functions, vec!["fetch"]);
    pool.shutdown().await;
}
