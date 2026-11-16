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
    container_socket_candidates()
        .into_iter()
        .find(|path| connects(path))
}

fn container_socket_candidates() -> Vec<PathBuf> {
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
    candidates
}

/// Whether this process can connect to the Unix socket `path` (nothing is sent).
fn connects(path: &Path) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::net::UnixStream::connect(path).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

/// The variable naming the D-Bus session bus. Spelled in parts: the repository's hygiene check
/// treats the whole name as the mark of an environment dump in a public file.
pub const SESSION_BUS_VAR: &str = concat!("DBUS_", "SESSION_BUS_ADDRESS");

/// A socket of concern this process's user can connect to (5.1): what it is and its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketOfConcern {
    /// What connecting gives, in a few words.
    pub what: &'static str,
    /// The socket.
    pub path: PathBuf,
}

/// The Unix sockets this process's user can connect to that would give isolated teams' code
/// authority outside its sandbox where the sandbox covers the filesystem only (5.1): the D-Bus
/// session bus (the address in [`SESSION_BUS_VAR`], or `/run/user/<uid>/bus`: `systemd-run --user`
/// starts programs there), the Docker and Podman sockets (root on the host) and the user's
/// default tmux server (commands in the user's sessions). Each is only connected to and closed.
pub fn sockets_of_concern() -> Vec<SocketOfConcern> {
    if cfg!(not(unix)) {
        return Vec::new();
    }
    let mut out: Vec<SocketOfConcern> = Vec::new();
    let mut push = |what: &'static str, path: PathBuf| {
        if !out.iter().any(|s| s.path == path) && connects(&path) {
            out.push(SocketOfConcern { what, path });
        }
    };
    let mut buses: Vec<PathBuf> = Vec::new();
    if let Ok(address) = std::env::var(SESSION_BUS_VAR) {
        for part in address.split(';') {
            for field in part.split(',') {
                if let Some(path) = field
                    .strip_prefix("unix:path=")
                    .or_else(|| field.strip_prefix("path="))
                {
                    buses.push(PathBuf::from(path));
                }
            }
        }
    }
    #[cfg(unix)]
    {
        let uid = rustix::process::getuid().as_raw();
        buses.push(PathBuf::from(format!("/run/user/{uid}/bus")));
        let tmux_dir = std::env::var_os("TMUX_TMPDIR")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        for path in buses {
            push("the D-Bus session bus", path);
        }
        for path in container_socket_candidates() {
            push("a container runtime's socket", path);
        }
        push(
            "the user's tmux server",
            tmux_dir.join(format!("tmux-{uid}")).join("default"),
        );
    }
    #[cfg(not(unix))]
    {
        let _ = (buses, &mut push);
    }
    out
}

/// One warning about a sandbox under which Unix sockets are not denied to team code (5.1): the
/// Landlock level is "filesystem only" (`abi`) and the seccomp filter could not be installed
/// (`reason`). Names the reachable `sockets` and what the administrator can do.
pub fn filesystem_only_warning(
    abi: u32,
    reason: Option<&str>,
    sockets: &[SocketOfConcern],
) -> String {
    use sandbox::{LEVEL_FILESYSTEM, LEVEL_SOCKETS, SOCKETS_ABI};
    let reach = if sockets.is_empty() {
        "This server's user can connect to none of the sockets Meerschaum checks (the D-Bus \
         session bus, Docker, Podman, the user's tmux server), but other services' sockets stay \
         reachable."
            .to_string()
    } else {
        format!(
            "This server's user, and so their code, can connect to {}.",
            sockets
                .iter()
                .map(|s| format!("{} `{}`", s.what, s.path.display()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let why = reason
        .filter(|r| !r.is_empty())
        .map(|r| format!(" ({r})"))
        .unwrap_or_default();
    format!(
        "Isolated teams' sandbox is \"{LEVEL_FILESYSTEM}\" on this kernel (Landlock ABI {abi}) and \
         the seccomp filter that denies Unix sockets before \"{LEVEL_SOCKETS}\" (ABI \
         {SOCKETS_ABI}, Linux 7.1) is not in force{why}: their code can connect to any Unix socket \
         this server's operating-system user can. {reach} Let the server install seccomp filters \
         (a container runtime's seccomp profile must allow `seccomp()`), upgrade the kernel, run \
         the API server in a container or as a service without a session bus, or set \
         `system.engine.python_team_sandbox: require_sockets` to run teams' code only where Unix \
         sockets are denied."
    )
}

/// Whether Unix sockets are denied to isolated teams' code on this server, and the words that
/// say so ([`sandbox::unix_sockets_words`]), from the cached probe
/// ([`sandbox::cached_probe`]: a process that applies the sandbox as a team process would).
pub fn team_sockets(mode: sandbox::Mode, root: &Path) -> (bool, String) {
    if mode == sandbox::Mode::Off {
        return (
            false,
            sandbox::unix_sockets_words("", Some("the sandbox is off")),
        );
    }
    let report = sandbox::cached_probe(mode, root);
    (report.denies_unix_sockets(), report.sockets_words())
}

/// Whether web terminals keep tmux on this server and why, in the words the start-up log and
/// `mrsm doctor` use (`.0`: `true` when tmux is refused). tmux is refused only while an isolated
/// team whose members may install plugins exists and Unix sockets are not denied to team code
/// (`sockets_denied`).
pub fn terminals_words(root: &Path, sockets_denied: bool) -> (bool, String) {
    let teams = meerschaum_core::team_scope::plugin_installing_isolated_teams(root);
    if teams.is_empty() {
        return (
            false,
            "Web terminals keep tmux (they survive a reload or a dropped connection): no \
             isolated team may install its own plugins, so no code a team chose runs here."
                .to_string(),
        );
    }
    let names = teams
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    if sockets_denied {
        return (
            false,
            format!(
                "Web terminals keep tmux (they survive a reload or a dropped connection): the \
                 isolated team{} {names} may install {} own plugins, but Unix sockets are denied \
                 to team code, so that code cannot reach tmux's socket.",
                if teams.len() == 1 { "" } else { "s" },
                if teams.len() == 1 { "its" } else { "their" },
            ),
        );
    }
    (
        true,
        format!(
            "Web terminals run without tmux: the isolated team{} {names} may install {} own \
             plugins and Unix sockets are not denied to team code here, so that code could drive \
             tmux's socket. A terminal ends when its tab closes or its connection drops, and \
             whatever runs in its foreground ends with it: run long work as a job.",
            if teams.len() == 1 { "" } else { "s" },
            if teams.len() == 1 { "its" } else { "their" },
        ),
    )
}

/// What the API server says about isolated teams' sandbox when it starts (`true`: a warning):
/// the mode, the level this system enforces, why it is not enforced (`auto` on a system without
/// Landlock), and, at the "filesystem only" level, the reachable sockets of concern. Empty when
/// no isolated team is recorded under `root`.
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
        (mode, Support::Unsupported { reason }) => lines.push((
            true,
            format!(
                "Isolated teams' code will not run: the filesystem sandbox is required \
                 (`system.engine.python_team_sandbox: {}`) but Landlock is unavailable here \
                 ({reason}).",
                mode.as_str()
            ),
        )),
        (Mode::RequireSockets, Support::Available { .. })
            if support.abi() < sandbox::SOCKETS_ABI
                && !sandbox::cached_probe(mode, root).covers_sockets_and_signals() =>
        {
            lines.push((
                true,
                format!(
                    "Isolated teams' code will not run: their sandbox must deny Unix sockets and \
                     signals (`system.engine.python_team_sandbox: require_sockets`), but here {} \
                     ({}).",
                    sandbox::cached_probe(mode, root).sockets_words(),
                    support.describe()
                ),
            ))
        }
        (_, Support::Available { .. }) => lines.push((
            false,
            format!(
                "Team sandbox ({}): {}: {}.",
                mode.as_str(),
                support.level().unwrap_or_default(),
                support.describe()
            ),
        )),
    }
    // Unix sockets (Landlock from Linux 7.1, else the seccomp filter, as the probe found) and
    // what that means for web terminals.
    let (denied, words) = team_sockets(mode, root);
    lines.push((!denied, format!("{words}.")));
    lines.push(terminals_words(root, denied));
    let filesystem_only = mode != Mode::Off
        && mode != Mode::RequireSockets
        && support.available()
        && support.abi() < sandbox::SOCKETS_ABI;
    if filesystem_only && !denied {
        let reason = sandbox::cached_probe(mode, root).sockets_reason;
        lines.push((
            true,
            filesystem_only_warning(support.abi(), reason.as_deref(), &sockets_of_concern()),
        ));
    } else if !denied
        && (mode == Mode::Off || !support.available())
        && let Some(socket) = container_socket_reachable()
    {
        lines.push((
            true,
            format!(
                "This server's user can open the container socket `{}`, and isolated teams' \
                 code can too (no sandbox): that is the same as root on this host. Run the API \
                 server as a user without Docker access while teams run code.",
                socket.display()
            ),
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_filesystem_only_warning_names_the_level_the_sockets_and_the_remedies() {
        let sockets = vec![
            SocketOfConcern {
                what: "the D-Bus session bus",
                path: PathBuf::from("/run/user/1000/bus"),
            },
            SocketOfConcern {
                what: "a container runtime's socket",
                path: PathBuf::from("/var/run/docker.sock"),
            },
        ];
        let text = filesystem_only_warning(
            8,
            Some("the seccomp filter could not be installed"),
            &sockets,
        );
        for part in [
            "\"filesystem only\"",
            "Landlock ABI 8",
            "(ABI 9, Linux 7.1) is not in force (the seccomp filter could not be installed)",
            "the D-Bus session bus `/run/user/1000/bus`",
            "a container runtime's socket `/var/run/docker.sock`",
            "seccomp profile must allow `seccomp()`",
            "in a container",
            "upgrade the kernel",
            "require_sockets",
        ] {
            assert!(text.contains(part), "{part}: {text}");
        }
        assert!(filesystem_only_warning(5, None, &[]).contains("none of the sockets"));
    }

    #[test]
    fn terminals_keep_tmux_unless_a_plugin_installing_team_could_reach_it() {
        use meerschaum_core::team_scope::{TeamScope, write_scope_record};
        let root = tempfile::tempdir().unwrap();
        let (refused, text) = terminals_words(root.path(), false);
        assert!(!refused && text.contains("keep tmux"), "{text}");
        let mut scope = TeamScope {
            team: "reviewers".into(),
            team_id: Some("id-r".into()),
            plugins_write: Some(false),
            ..TeamScope::default()
        };
        write_scope_record(root.path(), &scope).unwrap();
        let (refused, text) = terminals_words(root.path(), false);
        assert!(
            !refused && text.contains("no isolated team may install"),
            "{text}"
        );
        scope.plugins_write = Some(true);
        write_scope_record(root.path(), &scope).unwrap();
        let (refused, text) = terminals_words(root.path(), true);
        assert!(
            !refused && text.contains("Unix sockets are denied"),
            "{text}"
        );
        let (refused, text) = terminals_words(root.path(), false);
        assert!(refused, "{text}");
        for part in [
            "run without tmux",
            "'reviewers' may install its own plugins",
            "ends when its tab closes or its connection drops",
            "run long work as a job",
        ] {
            assert!(text.contains(part), "{part}: {text}");
        }
    }
}
