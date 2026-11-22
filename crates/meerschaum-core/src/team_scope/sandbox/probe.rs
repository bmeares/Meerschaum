//! The sandbox probe: a team process started only to apply its sandbox and report it, so a
//! server learns what its team processes get, and the cached result.

use super::*;

/// The first argument of the sandbox probe: this program started as
/// `<program> __mrsm_team_sandbox_probe__ <mode> <canary> [<max ABI>]` applies a team process's
/// sandbox to itself and prints its [`Report`] as JSON (see [`probe_team_process`]).
pub const PROBE_ARG: &str = "__mrsm_team_sandbox_probe__";

/// The probe's side ([`PROBE_ARG`]): `Some(exit code)` when `args` asks for it. Restricts this
/// process (irreversibly: hosts call it first thing, in the process started for it) with no
/// rules but `canary`, as a team process would at `mode` (and at most `max ABI`), then prints the
/// report on one line.
pub fn probe_if_requested(args: &[String]) -> Option<i32> {
    if args.first().map(String::as_str) != Some(PROBE_ARG) {
        return None;
    }
    let mode = args
        .get(1)
        .and_then(|m| Mode::parse(m))
        .unwrap_or(Mode::Require);
    let Some(canary) = args.get(2) else {
        eprintln!("The sandbox probe needs a canary file.");
        return Some(2);
    };
    let spec = Spec {
        mode,
        team: "probe".into(),
        canary: PathBuf::from(canary),
        max_abi: args.get(3).and_then(|a| a.parse().ok()),
        ..Spec::default()
    };
    let report = enforce(&spec, &[]);
    println!("{}", serde_json::to_string(&report).unwrap_or_default());
    Some(0)
}

/// What a team process would enforce here, found by starting this program as the probe
/// ([`PROBE_ARG`], [`crate::self_exe::self_command`]) with a fresh canary file: the server asks
/// before it lets the web terminal use tmux (an isolated team that may install plugins, on a
/// system where neither Landlock nor the seccomp filter denies Unix sockets, could drive it).
/// `max_abi` caps Landlock's ABI (tests). `Err` when the probe could not run or answer.
pub fn probe_team_process(mode: Mode, max_abi: Option<u32>) -> Result<Report, String> {
    if mode == Mode::Off {
        return Ok(Report::not_enforced(Mode::Off, "off"));
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let canary =
        std::env::temp_dir().join(format!("mrsm-sandbox-probe-{}-{nanos}", std::process::id()));
    std::fs::write(&canary, b"canary").map_err(|e| format!("cannot write the canary: {e}"))?;
    let result = (|| {
        let program = crate::self_exe::native_executable()
            .ok_or_else(|| "A sandbox probe needs the registered native mrsm host; Python embedding cannot establish pre-startup confinement.".to_string())?;
        let mut cmd = std::process::Command::new(&program);
        cmd.arg(PROBE_ARG)
            .arg(mode.as_str())
            .arg(&canary)
            .env_remove(SPEC_VAR)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(abi) = max_abi {
            cmd.arg(abi.to_string());
        }
        runtime::sanitize_command(&mut cmd);
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot start `{}`: {e}", program.display()))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("the probe did not answer in 20 s".to_string());
                }
            }
        }
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        stdout
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str::<Report>(line.trim()).ok())
            .ok_or_else(|| {
                format!(
                    "the probe gave no report ({}): {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim()
                )
            })
    })();
    let _ = std::fs::remove_file(&canary);
    result
}

/// [`probe_team_process`] once per mode and state of the isolated teams recorded under `root`
/// (the probe runs again after a team change): a failed probe counts as a sandbox that denies
/// nothing.
pub fn cached_probe(mode: Mode, root: &Path) -> Report {
    type Cached = (Mode, PathBuf, Option<std::time::SystemTime>, Report);
    static CACHE: std::sync::Mutex<Option<Cached>> = std::sync::Mutex::new(None);
    let stamp = std::fs::metadata(root.join(super::SCOPES_DIR).join(super::INDEX_FILE))
        .and_then(|m| m.modified())
        .ok();
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((m, r, t, report)) = cache.as_ref()
        && *m == mode
        && r == root
        && *t == stamp
    {
        return report.clone();
    }
    let report = probe_team_process(mode, None).unwrap_or_else(|why| {
        Report::not_enforced(mode, format!("the sandbox probe failed: {why}"))
    });
    *cache = Some((mode, root.to_path_buf(), stamp, report.clone()));
    report
}
