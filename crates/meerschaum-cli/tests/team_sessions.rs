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
        'private_files': _private_files() if os.environ.get('MRSM_TEAM_SCOPE') else {},
        'venvs_dir': os.environ.get('MRSM_VENVS_DIR'),
        'writes': _writes() if os.environ.get('MRSM_TEAM_SCOPE') else {},
        'worker_sockets': _worker_sockets(),
    })

def _worker_sockets():
    """The bound Unix sockets of this test host's worker channels this process could see."""
    try:
        lines = open('/proc/self/net/unix').read().splitlines()[1:]
    except Exception as e:
        return [repr(e)]
    needle = f'mrsm-w-{os.getppid()}-'
    return sorted(line.split()[-1] for line in lines if needle in line.split()[-1])

def _attempt_write(path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write('planted = True\n')
        return 'written'
    except PermissionError:
        return 'denied'
    except Exception as e:
        return repr(e)

def _private_files():
    root = os.environ['MRSM_ROOT_DIR']
    return {
        'another job': _attempt_read(os.path.join(root, 'jobs', 'other', 'properties.json')),
        'another plugin': _attempt_read(os.path.join(root, '.internal', 'plugins', 'other.py')),
        'parent environment': _attempt_read(f'/proc/{os.getppid()}/environ'),
        'write root': _attempt_write(os.path.join(root, 'planted.py')),
    }

def _writes():
    root = os.environ['MRSM_ROOT_DIR']
    here = os.path.dirname(os.path.realpath(__file__))
    own = os.environ.get('MRSM_VENVS_DIR') or ''
    out = {
        "server's venv": _attempt_write(os.path.join(root, 'venvs', 'tpr_probe', 'planted.py')),
        'plugins dir': _attempt_write(os.path.join(here, 'planted.py')),
        'pycache': _attempt_write(os.path.join(here, '__pycache__', 'tpr_probe.cpython-314.pyc')),
        'own venv': _attempt_write(os.path.join(own, 'tpr_probe', 'planted.py')),
    }
    for key in ('id-wetlands', 'id-drylands'):
        if key not in own:
            out["other team's venv"] = _attempt_write(
                os.path.join(root, 'team_scopes', key, 'venvs', 'tpr_probe', 'planted.py')
            )
    return out

from meerschaum.plugins import make_action

@make_action
def tpr_runtime(action=None, **kwargs):
    import concurrent.futures, multiprocessing, socket, ssl, tempfile
    import meerschaum as mrsm
    from meerschaum._internal import team_scope, team_sandbox
    report = team_sandbox.report()
    assert team_scope.hello_report()['sandbox'] == report
    with tempfile.TemporaryFile() as stream:
        stream.write(b'team data')
        stream.seek(0)
        assert stream.read() == b'team data'
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        assert list(pool.map(abs, [-1, -2])) == [1, 2]
    with multiprocessing.get_context('spawn').Pool(2) as pool:
        assert pool.map(abs, [-1, -2]) == [1, 2]
    assert socket.getaddrinfo('localhost', 80)
    assert ssl.create_default_context().cert_store_stats() is not None
    conn = mrsm.get_connector('sql', 'tpr_memory', flavor='sqlite', database=':memory:')
    pipe = mrsm.Pipe('tpr', 'memory', instance=conn)
    success, message = pipe.sync([{'a': 1}, {'a': 2}])
    assert success, message
    return True, json.dumps({'rowcount': pipe.get_rowcount()})

@make_action
def tpr_install(action=None, **kwargs):
    """Install a local wheel into this plugin's virtual environment and import it."""
    import tempfile, zipfile
    from meerschaum.utils.packages import pip_install
    from meerschaum.utils.venv import Venv
    tmp = tempfile.mkdtemp()
    wheel = os.path.join(tmp, 'tpr_local_pkg-0.1-py3-none-any.whl')
    with zipfile.ZipFile(wheel, 'w') as z:
        z.writestr('tpr_local_pkg/__init__.py', "WHO = 'installed'\n")
        z.writestr('tpr_local_pkg-0.1.dist-info/METADATA',
                   'Metadata-Version: 2.1\nName: tpr-local-pkg\nVersion: 0.1\n')
        z.writestr('tpr_local_pkg-0.1.dist-info/WHEEL',
                   'Wheel-Version: 1.0\nGenerator: tpr\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
        z.writestr('tpr_local_pkg-0.1.dist-info/RECORD', '')
    installed = pip_install(
        wheel, venv='tpr_probe', args=['--no-deps', '--no-index'],
        check_update=False, check_pypi=False, check_wheel=False, silent=True,
    )
    try:
        with Venv('tpr_probe'):
            import tpr_local_pkg
        where = os.path.realpath(tpr_local_pkg.__file__)
    except Exception as e:
        where = repr(e)
    return True, json.dumps({'installed': bool(installed), 'where': where})

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
    let other_job = root.join("jobs/other");
    std::fs::create_dir_all(&other_job).unwrap();
    std::fs::write(other_job.join("properties.json"), "{}").unwrap();
    let shared_plugins = root.join(".internal/plugins");
    std::fs::create_dir_all(&shared_plugins).unwrap();
    std::fs::write(shared_plugins.join("other.py"), "VALUE = 1\n").unwrap();
    Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        ("MRSM_PLUGINS_DIR".into(), plugins.display().to_string()),
    ]))
    .unwrap()
}

fn probe(session: &Session, config: &Config) -> serde_json::Value {
    action(session, config, "probe")
}

/// Run the plugin's action `tpr <name>` and parse the JSON it returns.
fn action(session: &Session, config: &Config, name: &str) -> serde_json::Value {
    let pool = session.pool(config);
    let result = session
        .block_on(run_action(
            &pool,
            &ActionRequest::Sysargs(vec!["tpr".into(), name.into()]),
            None,
            CallOptions::default(),
        ))
        .expect("the action runs");
    assert!(result.success, "{}", result.message);
    serde_json::from_str(result.message.trim()).unwrap()
}

/// Whether a file or directory named `name` is anywhere under `dir`.
fn tree_has(dir: &Path, name: &str) -> bool {
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries.flatten().any(|e| {
            e.file_name() == name
                || e.file_type().is_ok_and(|t| t.is_dir() && !t.is_symlink())
                    && tree_has(&e.path(), name)
        })
    })
}

const TEST: &str = "a_team_session_runs_python_with_the_teams_configuration_only";

fn main() -> std::process::ExitCode {
    use std::process::ExitCode;
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
    if args.iter().any(|arg| arg == "--ignored")
        && !args.iter().any(|arg| arg == "--include-ignored")
    {
        return ExitCode::SUCCESS;
    }
    let exact = args.iter().any(|arg| arg == "--exact");
    let mut remaining = args.iter();
    while let Some(arg) = remaining.next() {
        if matches!(arg.as_str(), "--format" | "--test-threads" | "--color") {
            let _ = remaining.next();
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
    a_team_session_runs_python_with_the_teams_configuration_only();
    ExitCode::SUCCESS
}

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
    // The server's virtual environment of the plugin (the server, the open side and every team
    // granted it import from it).
    let shared_venv = root.path().join("venvs").join("tpr_probe");
    std::fs::create_dir_all(shared_venv.join("lib")).unwrap();
    std::fs::write(shared_venv.join("lib").join("marker.txt"), "server").unwrap();
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
    // The server's own workers run first (5.1: their channels are socket pairs, so a team's code
    // finds nothing of theirs to connect to).
    let _ = probe(&session, &config);
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
    assert_eq!(first["worker_sockets"], serde_json::json!([]), "{first}");
    assert_eq!(first["file_has_own"], true);
    // 5.1: the worker restricted itself before it imported the plugin (where Landlock exists).
    let support = meerschaum_core::team_scope::sandbox::kernel_support();
    if support.available() {
        assert_eq!(first["sandbox"]["enforced"], true, "{first}");
        assert_eq!(first["sandbox"]["canary"], "denied", "{first}");
        assert_eq!(first["server_config"], "denied", "{first}");
        for (label, value) in first["private_files"].as_object().unwrap() {
            assert_eq!(value, "denied", "{label}: {first}");
        }
        // 5.1: it writes nothing a process outside the team loads, only its own environments.
        let writes = &first["writes"];
        for target in [
            "server's venv",
            "plugins dir",
            "pycache",
            "other team's venv",
        ] {
            assert_eq!(writes[target], "denied", "{target}: {first}");
        }
        assert_eq!(writes["own venv"], "written", "{first}");
        assert!(!shared_venv.join("planted.py").exists());
        assert_eq!(action(&team, &team_config, "runtime")["rowcount"], 2);
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

    // Its own environment is a copy of the server's, and what it installs at run time lands
    // there only.
    let own_venv = std::path::PathBuf::from(first["venvs_dir"].as_str().unwrap()).join("tpr_probe");
    assert!(
        own_venv.starts_with(root.path().join("team_scopes")),
        "{first}"
    );
    assert_eq!(
        std::fs::read_to_string(own_venv.join("lib").join("marker.txt")).unwrap(),
        "server"
    );
    let installed = action(&team, &team_config, "install");
    assert_eq!(installed["installed"], true, "{installed}");
    assert!(
        installed["where"]
            .as_str()
            .is_some_and(|w| w.starts_with(&own_venv.display().to_string())),
        "{installed}"
    );
    assert!(!tree_has(&shared_venv, "tpr_local_pkg"));

    // Another isolated team granted the same plugin works with its own copy, which the first
    // team's install did not touch, and cannot write the first team's.
    let mut drylands = Team::new("drylands");
    drylands.id = Some("id-drylands".into());
    drylands
        .attributes
        .insert("isolated".into(), meerschaum_core::Value::Bool(true));
    drylands.set_names("plugins", &["tpr_probe".to_string()]);
    let dry_scope = TeamScope::of(&drylands, &connectors);
    let (dry, _) = session.for_team(&dry_scope, None, &config).unwrap();
    let dry_config = dry.team().unwrap().new_config().unwrap();
    let dry_probe = probe(&dry, &dry_config);
    assert_eq!(dry_probe["team"], "drylands");
    let dry_venv =
        std::path::PathBuf::from(dry_probe["venvs_dir"].as_str().unwrap()).join("tpr_probe");
    assert_ne!(dry_venv, own_venv);
    assert!(!tree_has(&dry_venv, "tpr_local_pkg"));
    if support.available() {
        assert_eq!(
            dry_probe["writes"]["other team's venv"], "denied",
            "{dry_probe}"
        );
    }
    let dry_retired = session.retire_teams(|key, _| key != "id-drylands");
    assert_eq!(dry_retired, vec!["id-drylands".to_string()]);
    if let Ok(tmp) =
        meerschaum_core::team_scope::sandbox::team_tmp_dir(root.path(), &dry_scope.key())
    {
        let _ = std::fs::remove_dir_all(tmp);
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
