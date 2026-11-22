//! Server flags and HTTPS options, console dependencies, shutdown signals, native provenance of a
//! fresh host process and MCP SQL reads.

use super::*;
use meerschaum_server::mcp::{McpBackend, SqlReadError};

#[cfg(target_os = "linux")]
#[test]
fn tracked_api_requires_native_provenance_in_a_fresh_host_process() {
    use meerschaum_server::backends::{ActionsBackend, ApiExecution};
    const CHILD: &str = "MRSM_TEST_TRACKED_API_CAPABILITY_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "server::tests::tracked_api_requires_native_provenance_in_a_fresh_host_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    assert!(meerschaum_core::self_exe::native_executable().is_none());
    meerschaum_core::self_exe::set_self_command(vec![
        "python3".into(),
        "-P".into(),
        "-m".into(),
        "meerschaum".into(),
    ]);
    let root = tempfile::tempdir().unwrap();
    let mut options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.path().join("plugins").display().to_string(),
        ),
    ]);
    options.home = root.path().into();
    options.allow_writes = false;
    let session = Session::new();
    let instance = session
        .block_on(meerschaum_connector_sql::SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", root.path().join("main.db").display()),
        ))
        .unwrap();
    let controller = Arc::new(crate::executor::client::Controller::new(
        root.path().into(),
        Arc::new(instance),
        options.clone(),
        Map::new(),
    ));
    let mut backend = NativeBackend {
        http: true,
        options,
        snapshot: Map::new(),
        session,
        plugin_actions: Vec::new(),
        executors: Some(controller),
    };
    assert_eq!(
        backend.api_execution(),
        ApiExecution::Unavailable,
        "A configured Python entry point claimed native confinement."
    );
    meerschaum_core::self_exe::register_native_current_executable().unwrap();
    assert_eq!(
        backend.api_execution(),
        ApiExecution::Tracked {
            terminals: meerschaum_server::executor::terminal::API_TERMINALS_AVAILABLE,
        }
    );
    backend.http = false;
    assert_eq!(backend.api_execution(), ApiExecution::Unavailable);
    backend.http = true;
    backend.executors = None;
    assert_eq!(backend.api_execution(), ApiExecution::Unavailable);
}

#[test]
fn console_repairs_only_missing_dependencies_with_python_package_present() {
    use meerschaum_server::host::ConsoleProbe;
    let probe = |names: &[&str]| ConsoleProbe {
        available: names.is_empty(),
        missing: names.iter().map(|s| (*s).into()).collect(),
    };
    assert_eq!(
        console_dependencies(&probe(&["dash", "jwt"])),
        vec!["dash>=4.1.0", "PyJWT>=2.15.0"]
    );
    assert!(console_dependencies(&probe(&["meerschaum", "dash"])).is_empty());
    assert!(console_dependencies(&probe(&["no Python interpreter"])).is_empty());
    assert!(console_dependencies(&probe(&[])).is_empty());
    for name in meerschaum_server::host::CONSOLE_MODULES
        .iter()
        .filter(|n| **n != "meerschaum")
    {
        assert_eq!(console_dependencies(&probe(&[name])).len(), 1, "{name}");
    }
}

#[cfg(unix)]
#[test]
fn shutdown_signals_are_registered_before_wait_is_polled() {
    const CHILD_SIGNAL: &str = "MRSM_TEST_SERVER_REGISTRATION_SIGNAL";
    const TEST: &str = "server::tests::shutdown_signals_are_registered_before_wait_is_polled";
    if let Some(signum) = std::env::var_os(CHILD_SIGNAL) {
        let signum: i32 = signum.to_str().unwrap().parse().unwrap();
        assert!(matches!(
            signum,
            signal_hook::consts::SIGINT | signal_hook::consts::SIGTERM
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut shutdown = ShutdownSignal::new().unwrap();
            // Deliver only to this owned child, before wait() has even been constructed.
            signal_hook::low_level::raise(signum).unwrap();
            tokio::time::timeout(Duration::from_secs(5), shutdown.wait())
                .await
                .expect("registered signal was lost before the first wait poll")
                .unwrap();
        });
        return;
    }
    for signum in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD_SIGNAL, signum.to_string())
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    assert!(
                        status.success(),
                        "registration child for {signum} exited {status}"
                    );
                    break;
                }
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                result => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("registration child for {signum} did not finish: {result:?}");
                }
            }
        }
    }
}

#[test]
fn server_flags_preserve_cli_values() {
    let args = ArgsMap::from_iter([
        ("port".into(), PyValue::Int(8123)),
        ("production".into(), PyValue::Bool(true)),
        ("secure".into(), PyValue::Bool(true)),
        ("no_dash".into(), PyValue::Bool(true)),
        ("mrsm_instance".into(), PyValue::Str("sql:wiring".into())),
    ]);
    let flags = flags(&args).unwrap();
    assert_eq!(flags.port, Some(8123));
    assert_eq!(flags.instance.as_deref(), Some("sql:wiring"));
    assert!(flags.production && flags.secure && flags.no_dash);
}

#[test]
fn invalid_port_is_rejected() {
    let args = ArgsMap::from_iter([("port".into(), PyValue::Int(65536))]);
    assert!(flags(&args).is_err());
}

/// `--keyfile` / `--certfile` (and the `ssl_*` keys under `api:server`, or 4.1's
/// `api:uvicorn`) serve HTTPS; half a pair or a bad file stops `start api` with the reason.
#[test]
fn https_options_prepare_or_refuse() {
    let root = tempfile::tempdir().unwrap();
    let config_with = |patch: &str| {
        let mut env = vec![("MRSM_ROOT_DIR".into(), root.path().display().to_string())];
        if !patch.is_empty() {
            env.push(("MRSM_CONFIG".into(), patch.to_string()));
        }
        Config::new(ConfigOptions::with_env(env)).unwrap()
    };
    let server =
        |args: &ArgsMap, config: &Config| ServerConfig::from_config(config, &flags(args).unwrap());
    let plain = config_with("");
    let mut cfg = server(&ArgsMap::new(), &plain);
    assert!(prepare_https(&mut cfg).unwrap().is_none());
    assert!(cfg.internal_addr.is_none());

    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = root.path().join("key.pem");
    let cert = root.path().join("cert.pem");
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    let path = |p: &std::path::Path| PyValue::Str(p.display().to_string());
    let both = ArgsMap::from_iter([
        ("keyfile".into(), path(&key)),
        ("certfile".into(), path(&cert)),
    ]);
    let mut cfg = server(&both, &plain);
    let https = prepare_https(&mut cfg).unwrap().expect("HTTPS");
    // The API host calls back on the loopback-only plain listener.
    let internal = cfg.internal_addr.expect("the internal listener");
    assert!(internal.ip().is_loopback());
    assert_eq!(https.internal_addr(), Some(internal));
    assert_eq!(
        meerschaum_server::tls::internal_url(&cfg),
        format!("http://{internal}")
    );

    let one = ArgsMap::from_iter([("certfile".into(), path(&cert))]);
    let text = prepare_https(&mut server(&one, &plain)).unwrap_err();
    assert!(
        text.contains("only the certificate is set") && text.contains("`--certfile`"),
        "{text}"
    );
    // The certificate as the key: the files are swapped.
    let swapped = ArgsMap::from_iter([
        ("keyfile".into(), path(&cert)),
        ("certfile".into(), path(&key)),
    ]);
    let text = prepare_https(&mut server(&swapped, &plain)).unwrap_err();
    assert!(text.contains("swapped"), "{text}");

    for section in ["server", "uvicorn"] {
        let config = config_with(&format!(
            r#"{{"api": {{"{section}": {{"ssl_keyfile": "/etc/ssl/key.pem"}}}}}}"#
        ));
        let mut cfg = server(&ArgsMap::new(), &config);
        let text = prepare_https(&mut cfg).unwrap_err();
        assert!(
            text.contains(&format!("`api:{section}:ssl_keyfile`")),
            "{text}"
        );
        assert_eq!(
            cfg.notices
                .iter()
                .any(|n| n.contains("'api:uvicorn:ssl_keyfile' is deprecated")),
            section == "uvicorn",
            "{:?}",
            cfg.notices
        );
    }
    let empty = config_with(r#"{"api": {"uvicorn": {"ssl_certfile": ""}}}"#);
    assert!(
        prepare_https(&mut server(&ArgsMap::new(), &empty))
            .unwrap()
            .is_none()
    );
    // Empty flags turn off the configured pair (`start gui` serves plain HTTP on loopback).
    let configured = config_with(
        &serde_json::json!({"api": {"server": {
            "ssl_keyfile": key.display().to_string(),
            "ssl_certfile": cert.display().to_string(),
        }}})
        .to_string(),
    );
    assert!(
        prepare_https(&mut server(&ArgsMap::new(), &configured))
            .unwrap()
            .is_some()
    );
    let off = ArgsMap::from_iter([
        ("keyfile".into(), PyValue::Str(String::new())),
        ("certfile".into(), PyValue::Str(String::new())),
    ]);
    assert!(
        prepare_https(&mut server(&off, &configured))
            .unwrap()
            .is_none()
    );
}

#[test]
fn mcp_reads_sqlite_with_row_limit() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.path().display().to_string(),
        ),
    ]);
    let config = Config::new(options.clone()).unwrap();
    let session = Session::new();
    let backend = NativeBackend {
        http: false,
        options,
        snapshot: config.snapshot(),
        session: session.clone(),
        plugin_actions: Vec::new(),
        executors: None,
    };
    let rows = session
        .block_on(backend.read_sql("sql:local", "SELECT 1 AS n UNION ALL SELECT 2 AS n", 1))
        .unwrap();
    assert_eq!(
        serde_json::to_value(rows).unwrap(),
        serde_json::json!([{ "n": 1 }])
    );
    let invalid =
        session.block_on(backend.read_sql("sql:local", "SELECT * FROM wiring_missing_table", 1));
    assert_eq!(invalid, Err(SqlReadError::Failed));
}
