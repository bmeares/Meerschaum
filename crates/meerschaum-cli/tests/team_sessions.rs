//! An isolated team's session (5.1): its workers are started with the team's configuration only
//! and are retired when the team's connectors change.

mod python_support;

use std::path::Path;

use meerschaum_cli::native::{Session, TeamLookup};
use meerschaum_core::access::Team;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::team_scope::TeamScope;
use meerschaum_worker::CallOptions;
use meerschaum_worker::plugins::{ActionRequest, run_action};

const PLUGIN: &str = r#"
import json, os
__version__ = '0.0.1'

def tpr_probe(action=None, **kwargs):
    from meerschaum import _core
    from meerschaum.config import get_config
    raw = _core.get_config('meerschaum', 'connectors', 'sql', warn=False) or {}
    config_file = os.environ.get('MRSM_CONFIG_FILE', '')
    text = open(config_file).read() if config_file else ''
    return True, json.dumps({
        'pid': os.getpid(),
        'defined': sorted(label for label, attrs in raw.items() if attrs),
        'visible': sorted((get_config('meerschaum', 'connectors', 'sql') or {}).keys()),
        'env': sorted(k for k in os.environ if k.startswith('MRSM_SQL_') or k in (
            'MRSM_CONFIG', 'MRSM_PATCH')),
        'team': os.environ.get('MRSM_TEAM_SCOPE'),
        'sentinels': sorted(k for k in os.environ if k.startswith('TPR_')
            or k == 'MRSM_SQL_LISTED'),
        'file_has_others': any(s in text for s in ('d-pw', 'server-pw')),
        'file_has_own': 'w-pw' in text,
        'sandbox': __import__('meerschaum._internal.team_sandbox', fromlist=['report']).report(),
        'server_config': _attempt_read(os.path.join(os.environ['MRSM_ROOT_DIR'], 'config', 'meerschaum.yaml')),
    })

def _attempt_read(path):
    try:
        open(path).read()
        return 'read'
    except PermissionError:
        return 'denied'
    except Exception as e:
        return repr(e)
"#;

/// `Ok` when `python`'s `meerschaum` can run an isolated team's code (it reports the
/// `team_scope` capability); else why not.
fn team_capable(python: &Path) -> Result<(), String> {
    let output = std::process::Command::new(python)
        .args([
            "-c",
            "import meerschaum\n\
             try:\n    from meerschaum._internal.team_scope import CAPABILITY\n\
             except Exception:\n    CAPABILITY = ''\n\
             print(meerschaum.__version__, CAPABILITY)",
        ])
        .current_dir(std::env::temp_dir())
        .output()
        .map_err(|e| format!("`{}` could not run: {e}", python.display()))?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let mut words = text.split_whitespace();
    let version = words.next().unwrap_or("unknown").to_string();
    if words.next() != Some("team_scope") {
        return Err(format!(
            "the `meerschaum` package of `{}` is {version}, older than this workspace ({}): it \
             cannot run team-scoped workers.",
            python.display(),
            env!("CARGO_PKG_VERSION"),
        ));
    }
    Ok(())
}

fn root_with_teams(root: &Path, plugins: &Path) -> Config {
    let config_dir = root.join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let db = |name: &str| root.join(format!("{name}.db")).display().to_string();
    std::fs::write(
        config_dir.join("meerschaum.yaml"),
        format!(
            "connectors:\n  sql:\n    main:\n      flavor: sqlite\n      database: {}\n      password: server-pw\n    wetlands:\n      flavor: sqlite\n      database: {}\n      password: w-pw\n      teams: [wetlands]\n    drylands:\n      flavor: sqlite\n      database: {}\n      password: d-pw\n      teams: [drylands]\n    shared:\n      flavor: sqlite\n      database: {}\n",
            db("main"),
            db("wetlands"),
            db("drylands"),
            db("shared"),
        ),
    )
    .unwrap();
    // `TPR_PASSED` is passed on to team processes; `MRSM_SQL_LISTED` is refused although listed.
    std::fs::write(
        config_dir.join("system.yaml"),
        "engine:\n  python_team_env_passthrough: [TPR_PASSED, MRSM_SQL_LISTED]\n",
    )
    .unwrap();
    std::fs::write(plugins.join("tpr_probe.py"), PLUGIN).unwrap();
    Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        ("MRSM_PLUGINS_DIR".into(), plugins.display().to_string()),
    ]))
    .unwrap()
}

fn probe(session: &Session, config: &Config) -> serde_json::Value {
    let pool = session.pool(config);
    let result = session
        .block_on(run_action(
            &pool,
            &ActionRequest::Sysargs(vec!["tpr".into(), "probe".into()]),
            None,
            CallOptions::default(),
        ))
        .expect("the action runs");
    assert!(result.success, "{}", result.message);
    serde_json::from_str(result.message.trim()).unwrap()
}

#[test]
fn a_team_session_runs_python_with_the_teams_configuration_only() {
    let Some(python) = python_support::current("MRSM_PYTHON") else {
        eprintln!("Skipping: no Python with meerschaum.");
        return;
    };
    // Team-scoped workers need the Python package of this release (an older one is refused:
    // `meerschaum-worker`'s `team_handshake` tests); the same behavior is covered where the
    // extension is built by `tests/test_team_workers.py`.
    if let Err(reason) = team_capable(&python) {
        eprintln!("Skipping: {reason}");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config = root_with_teams(root.path(), plugins.path());
    // The server's environment holds a credential of its own (a cloud key): team workers start
    // from an allowlist and never see it. (This file's only test: nothing else reads the
    // environment meanwhile.)
    // SAFETY: no other thread of this test binary reads or writes the environment now.
    unsafe {
        std::env::set_var("TPR_SENTINEL_CLOUD_KEY", "cloud-secret");
        std::env::set_var("TPR_PASSED", "passed");
        std::env::set_var(
            "MRSM_SQL_LISTED",
            format!("sqlite:///{}", root.path().join("listed.db").display()),
        );
    }
    let session = Session::new();
    session.set_worker_python(&python.display().to_string());
    let connectors = config.get(&["meerschaum", "connectors"]).unwrap().to_map();
    let mut wetlands = Team::new("wetlands");
    wetlands.id = Some("id-wetlands".into());
    wetlands
        .attributes
        .insert("isolated".into(), meerschaum_core::Value::Bool(true));
    // The plugin is the team's (5.1: a team's sandbox lets it read its own plugins only).
    wetlands.set_names("plugins", &["tpr_probe".to_string()]);

    let scope = TeamScope::of(&wetlands, &connectors);
    let (team, lookup) = session.for_team(&scope, None, &config).unwrap();
    assert_eq!(lookup, TeamLookup::Created);
    let binding = team.team().unwrap().clone();
    let team_config = binding.new_config().unwrap();
    let first = probe(&team, &team_config);
    // The process itself holds the team's connector only (not merely hidden by the guard).
    assert_eq!(first["defined"], serde_json::json!(["wetlands"]), "{first}");
    assert_eq!(first["visible"], serde_json::json!(["default", "wetlands"]));
    assert_eq!(first["env"], serde_json::json!([]), "{first}");
    assert_eq!(first["team"], "wetlands");
    assert_eq!(
        first["sentinels"],
        serde_json::json!(["TPR_PASSED"]),
        "{first}"
    );
    assert_eq!(first["file_has_others"], false);
    assert_eq!(first["file_has_own"], true);
    // 5.1: the worker restricted itself before it imported the plugin (where Landlock exists).
    let support = meerschaum_core::team_scope::sandbox::kernel_support();
    if support.available() {
        assert_eq!(first["sandbox"]["enforced"], true, "{first}");
        assert_eq!(first["sandbox"]["canary"], "denied", "{first}");
        assert_eq!(first["server_config"], "denied", "{first}");
    } else {
        eprintln!(
            "No Landlock here ({}): the sandbox is not checked.",
            support.describe()
        );
        assert_eq!(first["server_config"], "read", "{first}");
    }
    // The Rust side of the team's session resolves the team's connectors only (also what
    // `{{ Pipe(..., instance='sql:drylands') }}` would name).
    assert!(team.instance("sql:wetlands", &team_config).is_ok());
    for other in ["sql:drylands", "sql:main"] {
        assert!(team.instance(other, &team_config).is_err(), "{other}");
    }

    // Unchanged: the warm session and worker.
    let (same, lookup) = session.for_team(&scope, None, &config).unwrap();
    assert_eq!(lookup, TeamLookup::Warm);
    assert_eq!(probe(&same, &team_config)["pid"], first["pid"]);

    // Granted a connector: the old workers are retired; the new ones have it.
    wetlands.set_names("connectors", &["sql:shared".to_string()]);
    let granted = TeamScope::of(&wetlands, &connectors);
    let (team, lookup) = session.for_team(&granted, None, &config).unwrap();
    assert_eq!(lookup, TeamLookup::Replaced);
    let team_config = team.team().unwrap().new_config().unwrap();
    let second = probe(&team, &team_config);
    assert_ne!(second["pid"], first["pid"]);
    assert_eq!(second["defined"], serde_json::json!(["shared", "wetlands"]));

    // Revoked: no warm worker keeps it.
    wetlands.set_names("connectors", &[]);
    let revoked = TeamScope::of(&wetlands, &connectors);
    let (team, lookup) = session.for_team(&revoked, None, &config).unwrap();
    assert_eq!(lookup, TeamLookup::Replaced);
    let team_config = team.team().unwrap().new_config().unwrap();
    let third = probe(&team, &team_config);
    assert_ne!(third["pid"], second["pid"]);
    assert_eq!(third["defined"], serde_json::json!(["wetlands"]));

    // A deleted team's session is retired with its workers.
    let retired = session.retire_teams(|_, _| false);
    assert_eq!(retired, vec!["id-wetlands".to_string()]);
    assert!(session.team_pools(&config).keys().is_empty());
    // 5.1: the team's temporary directory is under the system's: leave nothing behind.
    if let Ok(tmp) = meerschaum_core::team_scope::sandbox::team_tmp_dir(root.path(), &scope.key()) {
        let _ = std::fs::remove_dir_all(tmp);
    }

    // The server's own session is unchanged: every connector.
    let open = probe(&session, &config);
    assert_eq!(
        open["defined"],
        serde_json::json!([
            "drylands", "listed", "local", "main", "memory", "shared", "wetlands"
        ])
    );
    // ... and its whole environment.
    assert_eq!(
        open["sentinels"],
        serde_json::json!(["MRSM_SQL_LISTED", "TPR_PASSED", "TPR_SENTINEL_CLOUD_KEY"])
    );
    session.shutdown();
}
