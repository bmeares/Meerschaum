//! Isolated teams' pools (5.1): selection by team and configuration, retirement when the team
//! changes, the limit on live workers across team pools, the team scope every call carries and
//! the checks of callbacks.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_core::team_scope::TeamScope;
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_worker::callback::{
    CallbackGuard, CallbackHandler, CallbackRequest, GuardedHandler,
};
use meerschaum_worker::plugins::{ActionRequest, ping, run_action};
use meerschaum_worker::{CallOptions, PoolConfig, PoolScope, TeamPools, WorkerError, WorkerPool};

use crate::common::TestEnv;

fn scope(team: &str, connectors: &[&str]) -> Arc<TeamScope> {
    Arc::new(TeamScope {
        team: team.into(),
        team_id: Some(format!("id-{team}")),
        connectors: connectors.iter().map(|s| s.to_string()).collect(),
        ..TeamScope::default()
    })
}

fn pool_scope(team: &str, connectors: &[&str]) -> PoolScope {
    PoolScope {
        scope: scope(team, connectors),
        configured: Arc::new(
            ["sql:main", "sql:wetlands", "sql:drylands"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        ),
        guard: None,
    }
}

fn team_settings(env: &TestEnv, team: &str, live: meerschaum_worker::LiveLimit) -> PoolConfig {
    PoolConfig {
        size: 2,
        worker: env.worker_options(),
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_millis(100),
        scope: Some(pool_scope(team, &[&format!("sql:{team}")])),
        live: Some(live),
        ..PoolConfig::default()
    }
}

#[tokio::test]
async fn team_pools_are_lazy_reused_and_replaced_when_the_team_changes() {
    let env = TestEnv::without_init();
    let pools = TeamPools::new(2);
    let (first, lookup) = pools.get("id-wetlands", "v1", |live| {
        team_settings(&env, "wetlands", live)
    });
    assert_eq!(lookup, meerschaum_worker::team::Lookup::Created);
    assert_eq!(first.stats().spawned, 0, "no worker before the first call");
    let (again, lookup) = pools.get("id-wetlands", "v1", |_| unreachable!());
    assert_eq!(lookup, meerschaum_worker::team::Lookup::Warm);
    assert!(again.same(&first));
    // A grant changed: a new pool; the old one refuses new calls.
    let (replaced, lookup) = pools.get("id-wetlands", "v2", |live| {
        team_settings(&env, "wetlands", live)
    });
    assert_eq!(lookup, meerschaum_worker::team::Lookup::Replaced);
    assert!(!replaced.same(&first));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = first
        .call(
            "ping",
            meerschaum_worker::protocol::Msg::new(),
            Vec::new(),
            CallOptions::default(),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, WorkerError::ShuttingDown), "{err}");
    // A deleted team: retired.
    assert!(pools.retire("id-wetlands"));
    assert!(pools.keys().is_empty());
    // Other teams have their own pools.
    let (dry, _) = pools.get("id-drylands", "v1", |live| {
        team_settings(&env, "drylands", live)
    });
    assert!(!dry.same(&replaced));
}

#[test]
fn team_settings_have_a_shorter_idle_timeout_and_the_live_limit() {
    let root = tempfile::tempdir().unwrap();
    let config =
        meerschaum_core::Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![(
            "MRSM_ROOT_DIR".to_string(),
            root.path().display().to_string(),
        )]))
        .unwrap();
    let pools = TeamPools::new(meerschaum_worker::pool::team_workers_limit(&config));
    let settings = PoolConfig::for_team(&config, pool_scope("wetlands", &[]), pools.live_limit());
    assert_eq!(
        settings.idle_timeout,
        Some(Duration::from_secs(
            meerschaum_worker::pool::DEFAULT_TEAM_IDLE_SECONDS
        ))
    );
    assert!(settings.size <= pools.limit());
    assert_eq!(
        PoolConfig::from_config(&config).idle_timeout,
        Some(Duration::from_secs(
            meerschaum_worker::pool::DEFAULT_IDLE_SECONDS
        )),
        "the shared pool is unchanged"
    );
}

#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

#[meerschaum_connector::async_trait]
impl CallbackHandler for Recorder {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let pipe = request.pipe.map(|p| p.to_string()).unwrap_or_default();
        self.0.lock().unwrap().push(pipe);
        SuccessTuple::success()
    }
}

struct DenyAll;

#[meerschaum_connector::async_trait]
impl CallbackGuard for DenyAll {
    async fn check(&self, _request: &CallbackRequest) -> Result<(), String> {
        Err("You may not change it.".into())
    }
}

fn request(ck: &str, instance: &str) -> CallbackRequest {
    let pipe = Pipe::builder(ck, "temperature")
        .instance(instance)
        .build()
        .unwrap();
    CallbackRequest {
        op: "sync".into(),
        call_id: 1,
        pipe: Some(pipe),
        chunk: None,
        kwargs: Map::new(),
    }
}

#[tokio::test]
async fn callbacks_outside_the_team_are_refused_like_the_api_refuses_them() {
    let recorder = Arc::new(Recorder::default());
    let configured: BTreeSet<String> = ["sql:main", "sql:drylands", "sql:wetlands"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut wet = (*scope("wetlands", &["sql:wetlands"])).clone();
    wet.plugins.insert("noaa".into());
    let handler = GuardedHandler {
        inner: recorder.clone(),
        scope: Arc::new(wet),
        configured: Arc::new(configured.clone()),
        guard: None,
    };
    // Another team's instance, the server's own instance.
    for instance in ["sql:drylands", "sql:main"] {
        let refused = handler.handle(request("plugin:noaa", instance)).await;
        assert!(!refused.success);
        assert_eq!(
            refused.message,
            format!("The instance '{instance}' is not available to your teams.")
        );
    }
    // A source connector the team was not given, a plugin it was not granted.
    let refused = handler.handle(request("sql:main", "sql:wetlands")).await;
    assert_eq!(
        refused.message,
        "The connector 'sql:main' is not available to your teams."
    );
    let refused = handler
        .handle(request("plugin:other", "sql:wetlands"))
        .await;
    assert_eq!(
        refused.message,
        "The plugin 'other' is not available to your teams."
    );
    // Allowed: the team's own instance with a granted plugin or a plain label.
    assert!(
        handler
            .handle(request("plugin:noaa", "sql:wetlands"))
            .await
            .success
    );
    assert!(
        handler
            .handle(request("manual:gauge", "sql:wetlands"))
            .await
            .success
    );
    assert_eq!(recorder.0.lock().unwrap().len(), 2);
    // The host's own guard (a pipe's access) runs after the team's checks.
    let guarded = GuardedHandler {
        inner: recorder.clone(),
        scope: scope("wetlands", &["sql:wetlands"]),
        configured: Arc::new(configured),
        guard: Some(Arc::new(DenyAll)),
    };
    let refused = guarded
        .handle(request("manual:gauge", "sql:wetlands"))
        .await;
    assert_eq!(refused.message, "You may not change it.");
    assert_eq!(recorder.0.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_live_limit_stops_another_teams_idle_worker_then_queues() {
    require_python!();
    let env = TestEnv::new();
    let pools = TeamPools::new(1);
    let (wet, _) = pools.get("id-wetlands", "v1", |live| {
        team_settings(&env, "wetlands", live)
    });
    let (dry, _) = pools.get("id-drylands", "v1", |live| {
        team_settings(&env, "drylands", live)
    });
    ping(&wet).await.expect("wetlands pings");
    assert_eq!(wet.idle_count(), 1);
    assert_eq!(pools.live_workers(), 1);
    // Drylands needs a worker: wetlands' idle one makes way.
    ping(&dry).await.expect("drylands pings");
    assert_eq!(wet.idle_count(), 0);
    assert_eq!(dry.idle_count(), 1);
    assert!(pools.live_workers() <= 1);

    // Every place busy: a new team call waits until one is free.
    let busy = dry.clone();
    let sleeper = tokio::spawn(async move {
        run_action(
            &busy,
            &ActionRequest::Sysargs(vec!["wk".into(), "sleep".into()]),
            None,
            CallOptions {
                timeout: Some(Duration::from_secs(2)),
                ..CallOptions::default()
            },
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = Instant::now();
    ping(&wet).await.expect("wetlands pings after waiting");
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "waited {:?}",
        started.elapsed()
    );
    let _ = sleeper.await;
    assert!(pools.live_workers() <= 1);
    pools.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_team_workers_plugin_code_may_look_up_only_the_teams_connectors() {
    require_python!();
    let env = TestEnv::new();
    std::fs::write(
        env.plugins.path().join("tpr_lookup.py"),
        r#"
import json
__version__ = '0.0.1'

def tpr_lookup(action=None, **kwargs):
    import meerschaum as mrsm
    from meerschaum.config import get_config
    out = {}
    try:
        mrsm.get_connector('sql:main')
        out['main'] = 'built'
    except PermissionError as e:
        out['main'] = str(e)
    out['labels'] = sorted((get_config('meerschaum', 'connectors', 'sql') or {}).keys())
    return True, json.dumps(out)
"#,
    )
    .unwrap();
    let pools = TeamPools::new(2);
    let (wet, _) = pools.get("id-wetlands", "v1", |live| {
        team_settings(&env, "wetlands", live)
    });
    let result = run_action(
        &wet,
        &ActionRequest::Sysargs(vec!["tpr".into(), "lookup".into()]),
        None,
        CallOptions::default(),
    )
    .await
    .expect("the action runs");
    assert!(result.success, "{}", result.message);
    let out: serde_json::Value = serde_json::from_str(result.message.trim()).unwrap();
    assert_eq!(
        out["main"],
        "The connector 'sql:main' is not available to your teams."
    );
    let labels: Vec<String> = serde_json::from_value(out["labels"].clone()).unwrap();
    assert!(!labels.contains(&"main".to_string()), "{labels:?}");
    // The shared pool (no team) is unchanged.
    let shared: WorkerPool = env.pool(1);
    let result = run_action(
        &shared,
        &ActionRequest::Sysargs(vec!["tpr".into(), "lookup".into()]),
        None,
        CallOptions::default(),
    )
    .await
    .expect("the action runs");
    let out: serde_json::Value = serde_json::from_str(result.message.trim()).unwrap();
    assert_eq!(out["main"], "built");
    shared.shutdown().await;
    pools.shutdown().await;
}
