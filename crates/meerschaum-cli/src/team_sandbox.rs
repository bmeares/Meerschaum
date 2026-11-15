//! A team job's `mrsm` restricts itself to the team's filesystem sandbox (5.1).
//!
//! The API server launches an isolated team's job with `MRSM_TEAM_SANDBOX` naming the rules it
//! wrote ([`meerschaum_core::team_scope::sandbox`]). The job's command (this program, started by
//! the job's supervisor) applies them first thing, before it runs the command, so the command
//! and every worker or program it starts inherit them. The interpreters its workers use are
//! asked for their prefixes and import path first (read and execute). In `require` mode, a
//! process whose sandbox is not enforced, or whose canary read succeeded, runs nothing.

use std::path::{Path, PathBuf};
use std::time::Duration;

use meerschaum_core::team_scope::sandbox::{self, Access, Rule};

/// Prints the prefixes and import path of the interpreter that runs it (standard library only,
/// imports nothing of Meerschaum's).
const PROBE: &str = r#"
import json, os, sys, importlib.util
paths = {sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix}
paths.update(p for p in sys.path if p and os.path.isabs(p))
if sys.executable:
    paths.add(os.path.dirname(os.path.realpath(sys.executable)))
spec = importlib.util.find_spec('meerschaum')
if spec is not None and spec.origin:
    package = os.path.dirname(spec.origin)
    paths.add(package)
    launcher = os.path.join(os.path.dirname(package), '_meerschaum_mrsm.py')
    if os.path.exists(launcher):
        paths.add(launcher)
print(json.dumps(sorted(p for p in paths if p)))
"#;

/// `program` found on `PATH` when it has no directory.
fn which(program: &Path) -> Option<PathBuf> {
    if program.components().count() > 1 {
        return Some(program.to_path_buf());
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(program))
            .find(|candidate| candidate.is_file())
    })
}

/// The paths `python` reads and executes (empty when it cannot be asked).
fn interpreter_paths(python: &Path) -> Vec<PathBuf> {
    let Ok(mut child) = std::process::Command::new(python)
        .args([
            meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
            "-c",
            PROBE,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    else {
        return Vec::new();
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Vec::new();
            }
        }
    }
    let Ok(out) = child.wait_with_output() else {
        return Vec::new();
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .last()
        .and_then(|line| serde_json::from_str::<Vec<String>>(line).ok())
        .unwrap_or_default()
        .into_iter()
        .map(PathBuf::from)
        .collect()
}

/// The interpreter-and-program rules of this process: this program itself and every
/// interpreter its workers may use (`MRSM_PYTHON`, else `python3`; this interpreter when the
/// command line runs inside Python).
fn process_rules() -> Vec<Rule> {
    let mut rules = Vec::new();
    let mut interpreters: Vec<PathBuf> = Vec::new();
    if let Some(python) = which(&meerschaum_worker::worker::default_python()) {
        interpreters.push(python);
    }
    if let Ok(exe) = std::env::current_exe() {
        let is_python = exe
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("python"));
        if is_python {
            interpreters.push(exe.clone());
        }
        rules.push(Rule::new(exe, Access::ReadExec, "this program"));
    }
    if let Some(bin) = std::env::var_os("MRSM_BIN").filter(|b| !b.is_empty()) {
        rules.push(Rule::new(
            PathBuf::from(bin),
            Access::ReadExec,
            "this program",
        ));
    }
    let mut seen: Vec<PathBuf> = Vec::new();
    for python in interpreters {
        let real = std::fs::canonicalize(&python).unwrap_or(python.clone());
        if seen.contains(&real) {
            continue;
        }
        seen.push(real);
        for path in interpreter_paths(&python) {
            rules.push(Rule::new(path, Access::ReadExec, "the interpreter"));
        }
    }
    rules
}

/// Apply the team's sandbox when this process was started for a team job
/// ([`sandbox::SPEC_VAR`]); `Some(exit code)` when the command must not run. A no-op in any
/// other process, and when this process already applied it.
pub fn enforce_if_team_process() -> Option<i32> {
    std::env::var_os(sandbox::SPEC_VAR).filter(|v| !v.is_empty())?;
    if sandbox::current().is_some() {
        return None;
    }
    let report = sandbox::enforce_from_env(&process_rules())?;
    let in_job = std::env::var_os(meerschaum_jobs::supervisor::RESULT_PATH_ENV).is_some();
    if in_job {
        eprintln!("Team sandbox: {}", report.describe());
    }
    let Err(why) = report.verdict() else {
        return None;
    };
    let message =
        format!("This job of the isolated team was stopped before running its command: {why}.");
    eprintln!("{message}");
    if let Some(path) = std::env::var_os(meerschaum_jobs::supervisor::RESULT_PATH_ENV) {
        let _ = std::fs::write(path, serde_json::json!([false, message]).to_string());
    }
    Some(1)
}

/// The Docker (or Podman) API socket this process's user can connect to, if any (a connection
/// only: nothing is sent).
pub fn container_socket_reachable() -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = vec![
        PathBuf::from("/var/run/docker.sock"),
        PathBuf::from("/run/docker.sock"),
        PathBuf::from("/run/podman/podman.sock"),
    ];
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
        let runtime = PathBuf::from(runtime);
        candidates.push(runtime.join("docker.sock"));
        candidates.push(runtime.join("podman").join("podman.sock"));
    }
    if let Ok(host) = std::env::var("DOCKER_HOST")
        && let Some(path) = host.strip_prefix("unix://")
    {
        candidates.insert(0, PathBuf::from(path));
    }
    #[cfg(unix)]
    {
        candidates
            .into_iter()
            .find(|path| std::os::unix::net::UnixStream::connect(path).is_ok())
    }
    #[cfg(not(unix))]
    {
        let _ = candidates;
        None
    }
}

/// What the API server says about isolated teams' sandbox when it starts (`true`: a warning):
/// the mode and what this system enforces, why it is not enforced (`auto` on a system without
/// Landlock), and a reachable Docker socket the sandbox does not stop. Empty when no isolated
/// team is recorded under `root`.
pub fn server_status(config: &meerschaum_core::Config, root: &Path) -> Vec<(bool, String)> {
    use sandbox::{Mode, Support};
    if !meerschaum_core::team_scope::isolated_teams_recorded(root) {
        return Vec::new();
    }
    let mode = sandbox::Mode::from_config(config);
    let support = sandbox::kernel_support();
    let mut lines = Vec::new();
    match (mode, &support) {
        (Mode::Off, _) => lines.push((
            true,
            "Isolated teams' processes run without the filesystem sandbox \
             (`system.engine.python_team_sandbox: off`): their code can read whatever this \
             server's operating-system user can read."
                .to_string(),
        )),
        (Mode::Auto, Support::Unsupported { reason }) => lines.push((
            true,
            format!(
                "Isolated teams' processes run without the filesystem sandbox: Landlock is \
                 unavailable here ({reason}). Their code can read whatever this server's \
                 operating-system user can read. Set `system.engine.python_team_sandbox: \
                 require` to refuse to run it instead."
            ),
        )),
        (Mode::Require, Support::Unsupported { reason }) => lines.push((
            true,
            format!(
                "Isolated teams' code will not run: the filesystem sandbox is required \
                 (`system.engine.python_team_sandbox: require`) but Landlock is unavailable here \
                 ({reason})."
            ),
        )),
        (_, Support::Available { .. }) => lines.push((
            false,
            format!("Team sandbox ({}): {}.", mode.as_str(), support.describe()),
        )),
    }
    let stops_sockets = mode != Mode::Off && support.abi() >= 9;
    if !stops_sockets && let Some(socket) = container_socket_reachable() {
        lines.push((
            true,
            format!(
                "This server's user can open the container socket `{}`, and isolated teams' \
                 code can too ({}): that is the same as root on this host. Run the API server \
                 as a user without Docker access while teams run code.",
                socket.display(),
                if mode == Mode::Off {
                    "the sandbox is off".to_string()
                } else if support.available() {
                    format!(
                        "Landlock ABI {} cannot restrict Unix socket connections before ABI 9, \
                         Linux 7.1",
                        support.abi()
                    )
                } else {
                    "no sandbox on this system".to_string()
                }
            ),
        ));
    }
    lines
}
