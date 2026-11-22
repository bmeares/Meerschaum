//! Tests that run a sandboxed child process and check which paths it may open, write and
//! execute (Linux with Landlock).

use super::*;

#[cfg(target_os = "linux")]
#[test]
fn public_certificate_stores_do_not_expose_sibling_private_keys() {
    let l = layout();
    let system = l._temp.path().join("system");
    let public = [
        "ssl/certs/ca.pem",
        "ssl/cert.pem",
        "pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "pki/ca-trust/source/anchors/local-ca.pem",
        "pki/tls/certs/ca-bundle.crt",
        "pki/tls/cert.pem",
    ];
    let private = [
        "ssl/private/service.key",
        "pki/tls/private/service.key",
        "pki/nssdb/key4.db",
        "pki/ca-trust/private/service.key",
    ];
    for path in public.iter().chain(private.iter()) {
        let path = system.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "certificate or private canary").unwrap();
    }
    let mut spec = spec_of(&l, Mode::Require, &[]);
    // Exercise the actual system allowlist against synthetic readable certificate
    // and key trees, without writing into the host's /etc or reading real keys.
    for rule in &mut spec.rules {
        if let Ok(relative) = rule.path.strip_prefix("/etc") {
            rule.path = system.join(relative);
        }
    }
    let plan = ChildPlan {
        spec,
        read: public
            .iter()
            .chain(private.iter())
            .map(|path| (path.to_string(), system.join(path)))
            .collect(),
        ..Default::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.verdict().is_ok(), "{report:?}");
    for path in public {
        assert_eq!(results[path], "ok", "{path}: {results:?}");
    }
    for path in private {
        assert_eq!(results[path], "denied", "{path}: {results:?}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_sandboxed_process_reaches_its_own_paths_and_nothing_else() {
    // At the kernel's level, then (5.1) forced to "filesystem only", where the seccomp filter
    // denies Unix sockets: the acceptance list holds under the filter too.
    for max_abi in [None, Some(8)] {
        reaches_its_own_paths_and_nothing_else(max_abi);
    }
}

#[cfg(target_os = "linux")]
fn reaches_its_own_paths_and_nothing_else(max_abi: Option<u32>) {
    use std::os::linux::net::SocketAddrExt;
    let l = layout();
    let mut spec = spec_of(&l, Mode::Require, &["mine", "single"]);
    spec.max_abi = max_abi;
    // A pathname socket and an abstract socket held by this (unsandboxed) process.
    let sockets = tempfile::tempdir().unwrap();
    let outside = sockets.path().join("outside.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let inside = l.tmp.join("inside.sock");
    let _inside = std::os::unix::net::UnixListener::bind(&inside).unwrap();
    let name = format!(
        "mrsm-test-sandbox-{}-{}",
        std::process::id(),
        max_abi.unwrap_or(0)
    );
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
    let _abstract = std::os::unix::net::UnixListener::bind_addr(&addr).unwrap();
    let s = |v: &str| v.to_string();
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: vec![
            (s("server config"), l.root.join("config/meerschaum.yaml")),
            (
                s("grafana password"),
                l.root.join("api/.grafana_admin_password"),
            ),
            (
                s("other team's config"),
                l.root.join("team_scopes/key-b/config.json"),
            ),
            (s("other team's job"), l.other_job.join("properties.json")),
            (s("other plugin"), l.plugins_dir.join("theirs/__init__.py")),
            (s("other venv"), l.root.join("venvs/theirs/lib/site.py")),
            (s("ssh key"), l.home.join(".ssh/id_ed25519")),
            (s("home listing"), l.home.clone()),
            (s("root listing"), l.root.clone()),
            (
                s("parent environ"),
                PathBuf::from(format!("/proc/{}/environ", std::process::id())),
            ),
            (s("canary"), canary_path(&l.root)),
            (s("own config"), l.files.config_file.clone()),
            (s("own plugin"), l.plugins_dir.join("mine/__init__.py")),
            (s("own single-file plugin"), l.plugins_dir.join("single.py")),
            (s("own job"), l.job.join("properties.json")),
            (s("own venv"), l.venvs.join("mine/lib/site.py")),
            (
                s("server's venv of its plugin"),
                l.root.join("venvs/mine/lib/site.py"),
            ),
            (
                s("plugins package"),
                l.root.join(".internal/plugins/__init__.py"),
            ),
            (s("user names"), PathBuf::from("/etc/passwd")),
            (s("cpuinfo"), PathBuf::from("/proc/cpuinfo")),
        ],
        write: vec![
            (s("write root"), l.root.join("planted.txt")),
            (s("write own job properties"), l.job.join("properties.json")),
            (
                s("write other venv"),
                l.root.join("venvs/theirs/lib/planted.py"),
            ),
            (
                s("write own config dir"),
                l.files.config_dir.join("planted.yaml"),
            ),
            (s("write tmp"), l.tmp.join("scratch.txt")),
            (s("write work"), l.work.join(".result.json")),
            (s("write cache"), l.cache.join("x")),
            (s("write own venv"), l.venvs.join("mine/lib/installed.py")),
            (
                s("write server's venv of its plugin"),
                l.root.join("venvs/mine/lib/installed.py"),
            ),
            (s("write /dev/null"), PathBuf::from("/dev/null")),
        ],
        connect: vec![
            (s("outside socket"), outside.clone()),
            (s("inside socket"), inside.clone()),
        ],
        connect_abstract: vec![(s("abstract socket"), name.clone())],
        signal: Some(std::process::id() as i32),
        fifo: Vec::new(),
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    assert_eq!(report.canary, Canary::Denied, "{report:?}");
    assert!(report.verdict().is_ok());
    let support = kernel_support();
    let abi = max_abi.unwrap_or(NEWEST_ABI).min(support.abi());
    let get = |label: &str| results[label].as_str().unwrap().to_string();
    for label in [
        "server config",
        "grafana password",
        "other team's config",
        "other team's job",
        "other plugin",
        "other venv",
        "ssh key",
        "home listing",
        "root listing",
        "parent environ",
        "canary",
        "write root",
        "write own job properties",
        "write other venv",
        "write own config dir",
        "server's venv of its plugin",
        "write server's venv of its plugin",
    ] {
        assert_eq!(get(label), "denied", "{label}: {results:?}");
    }
    for label in [
        "own config",
        "own plugin",
        "own single-file plugin",
        "own job",
        "own venv",
        "plugins package",
        "user names",
        "cpuinfo",
        "write tmp",
        "write work",
        "write cache",
        "write own venv",
        "write /dev/null",
        "own /proc",
    ] {
        assert_eq!(get(label), "ok", "{label}: {results:?}");
    }
    if abi >= 6 {
        assert_eq!(get("signal"), "denied", "{results:?}");
        assert_eq!(get("abstract socket"), "denied", "{results:?}");
    } else {
        eprintln!(
            "ABI {}: signal and abstract socket scoping not checked",
            support.abi()
        );
    }
    // Unix sockets: Landlock denies those outside the rules (the team's own still work); the
    // seccomp filter, below ABI 9, refuses to create any.
    assert_eq!(get("outside socket"), "denied", "ABI {abi}: {results:?}");
    assert!(report.denies_unix_sockets(), "{report:?}");
    if abi >= SOCKETS_ABI {
        assert_eq!(report.unix_sockets, SOCKETS_BY_LANDLOCK, "{report:?}");
        assert_eq!(get("inside socket"), "ok", "{results:?}");
        assert!(report.features.contains(&"resolve_unix".to_string()));
    } else {
        assert_eq!(report.unix_sockets, SOCKETS_BY_SECCOMP, "{report:?}");
        assert_eq!(report.socket_probe, SocketProbe::Denied, "{report:?}");
        assert_eq!(get("inside socket"), "denied", "{results:?}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn an_interpreter_path_covering_the_root_is_dropped_in_the_child() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine"]);
    let plan = ChildPlan {
        spec,
        extra: vec![Rule::new(
            l.root.parent().unwrap(),
            Access::ReadExec,
            "a PYTHONPATH holding the root",
        )],
        read: vec![(
            "server config".into(),
            l.root.join("config/meerschaum.yaml"),
        )],
        write: Vec::new(),
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
        fifo: Vec::new(),
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert_eq!(results["server config"], "denied");
    assert_eq!(report.dropped.len(), 1, "{report:?}");
}

#[cfg(target_os = "linux")]
#[test]
fn the_kernel_support_probe_restricts_nothing() {
    let support = kernel_support();
    eprintln!("this system: {}", support.describe());
    // This process can still read what a sandbox would deny.
    let l = layout();
    assert!(std::fs::read(l.root.join("config/meerschaum.yaml")).is_ok());
    assert!(current().is_none());
}

/// 5.1: a team's process may write nothing that a process outside the team later executes,
/// imports or loads: not the virtual environment of a plugin it was granted (which the server,
/// the open side and other teams granted the same plugin import), the plugins directory, the
/// bytecode next to shared source, the plugins package's links or the lock files the server's
/// processes take.
#[cfg(target_os = "linux")]
#[test]
fn a_team_process_writes_nothing_that_others_load() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine", "single"]);
    std::fs::create_dir_all(l.root.join(".cache/venvs")).unwrap();
    std::fs::write(l.root.join(".cache/venvs/mine.mutation.lock"), "").unwrap();
    std::fs::create_dir_all(l.plugins_dir.join("mine/__pycache__")).unwrap();
    let s = |v: &str| v.to_string();
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: Vec::new(),
        write: vec![
            (
                s("shared venv of a granted plugin"),
                l.root.join("venvs/mine/lib/planted.py"),
            ),
            (
                s("plugins directory"),
                l.plugins_dir.join("mine/planted.py"),
            ),
            (
                s("bytecode next to shared source"),
                l.plugins_dir
                    .join("mine/__pycache__/__init__.cpython-314.pyc"),
            ),
            (
                s("plugins package links"),
                l.root.join(".internal/plugins/planted.py"),
            ),
            (
                s("a lock the server takes"),
                l.root.join(".cache/venvs/mine.mutation.lock"),
            ),
        ],
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
        fifo: Vec::new(),
        ..ChildPlan::default()
    };
    let mut plan = plan;
    plan.write
        .push((s("its own venv"), l.venvs.join("mine/lib/installed.py")));
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    for (label, _) in &plan.write {
        let expected = if label == "its own venv" {
            "ok"
        } else {
            "denied"
        };
        assert_eq!(results[label.as_str()], expected, "{label}: {results:?}");
    }
}

/// 5.1: a job's stdin is a FIFO in its directory, written by the server (`mrsm attach`, the
/// console's prompt answers) and read by the job. Opening a FIFO is opening a path, which the
/// filesystem rules (Landlock ABI 1) restrict on every kernel: a team's process cannot write to
/// another job's stdin, nor to its own job's (its job directory is read-only to it).
#[cfg(target_os = "linux")]
#[test]
fn a_team_process_cannot_write_to_a_jobs_stdin() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine"]);
    let other = l.other_job.join("b-job.stdin");
    let own = l.job.join("a-job.stdin");
    for fifo in [&other, &own] {
        nix_mkfifo(fifo);
    }
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: Vec::new(),
        write: Vec::new(),
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
        fifo: vec![
            ("another job's stdin".into(), other.clone()),
            ("its own job's stdin".into(), own.clone()),
        ],
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    for label in ["another job's stdin", "its own job's stdin"] {
        assert_eq!(results[label], "denied", "{label}: {results:?}");
    }
}
