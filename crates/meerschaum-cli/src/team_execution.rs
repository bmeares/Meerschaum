//! Team execution's processes on the API server's host (5.1, `meerschaum_server::team_execution`).
//!
//! A process of team execution is started the way an isolated team's jobs are: from an empty
//! environment holding the allowlist ([`meerschaum_core::team_scope::team_env`]), the team's own
//! configuration (the files the server records under `team_scopes/<team>/`), and the team's
//! sandbox spec written in `require` mode
//! ([`meerschaum_core::team_scope::sandbox::prepare_with`]), which this program applies first
//! thing ([`crate::team_sandbox::enforce_if_team_process`]) before it runs anything else.
//!
//! - [`probe`]: such a process started with [`PROBE_ENV`]: it applies the sandbox, asks the
//!   Python package whether it applies team scoping (the handshake capability), prints both as
//!   JSON and exits ([`probe_if_requested`]).
//! - [`team_shell`]: the `mrsm` shell of a team terminal.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use meerschaum_core::Config;
use meerschaum_core::team_scope::{self as ts, sandbox};
use meerschaum_server::team_execution::{HostFacts, Probe};

/// Set (to `1`) in a team process started to report its sandbox and Python capability.
pub const PROBE_ENV: &str = "MRSM_TEAM_PROBE";

/// How long a probe may take (the interpreter is started once inside the sandbox).
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);

/// A team process to start: its complete environment and its working directory (the team's
/// temporary directory).
#[derive(Debug, Clone)]
pub struct TeamProcess {
    /// The environment (start from an empty one).
    pub env: Vec<(String, String)>,
    /// The working directory.
    pub cwd: PathBuf,
}

/// The environment of a process of team execution of `team` (by name), its spec file named
/// `<purpose>.json` in the team's scope directory. `Err` when the team's scope is not recorded,
/// the team is not isolated, or the sandbox cannot be required here.
pub fn team_process(config: &Config, team: &str, purpose: &str) -> Result<TeamProcess, String> {
    config.refresh_connectors();
    let paths = config.paths();
    let root = paths.root_dir.clone();
    let key = ts::scope_key_of(&root, team)
        .ok_or_else(|| format!("the scope of the team '{team}' is not recorded on this server"))?;
    let scope = ts::read_scope_record(&root, &key)
        .ok_or_else(|| format!("the scope of the team '{team}' is not recorded on this server"))?;
    if scope.open {
        return Err(format!("the team '{team}' is not isolated"));
    }
    let team_map = ts::team_config(config, &scope);
    let fingerprint = ts::fingerprint(&scope, &team_map);
    let files = ts::write_scope_files(&ts::scope_dir(&root, &key), &team_map)
        .map_err(|e| format!("the team's configuration could not be written: {e}"))?;
    let mut base: Vec<(String, String)> = meerschaum_core::process_env::vars()
        .into_iter()
        .filter(|(k, _)| k != "MRSM_ROOT_DIR" && k != "MRSM_PLUGINS_DIR")
        .collect();
    base.push(("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()));
    let passthrough = ts::Passthrough::from_config(config);
    let mut env = ts::team_env(&base, &[], &files, &scope, &fingerprint, &passthrough);
    let spec_file = files.dir.join(format!("{purpose}.json"));
    let prepared = sandbox::prepare_with(config, &scope, &files, None, &spec_file, true)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "the team sandbox is off".to_string())?;
    sandbox::apply_env(&mut env, &prepared);
    Ok(TeamProcess {
        env,
        cwd: prepared.tmp_dir.clone(),
    })
}

/// Start a process of `team` that reports its sandbox and the Python package's capability
/// ([`probe_if_requested`]), and judge nothing: [`HostFacts`] for
/// `meerschaum_server::team_execution::judge`.
pub fn probe(config: &Config, team: &str) -> HostFacts {
    HostFacts {
        mode: sandbox::Mode::from_config(config),
        probe: run_probe(config, team),
    }
}

fn run_probe(config: &Config, team: &str) -> Result<Probe, String> {
    let process = team_process(config, team, "probe")?;
    let program = meerschaum_core::self_exe::native_executable()
        .ok_or("Team probing needs the registered native mrsm host.")?;
    let args: Vec<String> = Vec::new();
    let mut child = std::process::Command::new(&program)
        .args(&args)
        .env_clear()
        .envs(process.env.iter().map(|(k, v)| (k, v)))
        .env(PROBE_ENV, "1")
        .current_dir(&process.cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("`{}` could not start: {e}", program.display()))?;
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("the team's process did not answer in time".to_string());
            }
        }
    }
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Probe>(line.trim()).ok())
        .ok_or_else(|| {
            let stderr = String::from_utf8_lossy(&output.stderr);
            format!(
                "the team's process exited with {} without a report{}",
                output.status,
                stderr
                    .lines()
                    .last()
                    .map(|l| format!(": {l}"))
                    .unwrap_or_default()
            )
        })
}

/// In a process started with [`PROBE_ENV`] for a team: apply the sandbox, ask the Python
/// package for the team-scope capability (inside the sandbox), print the [`Probe`] as one JSON
/// line and exit (`Some(0)`). `None` in any other process.
pub fn probe_if_requested() -> Option<i32> {
    std::env::var_os(PROBE_ENV).filter(|v| v == "1")?;
    std::env::var_os(sandbox::SPEC_VAR).filter(|v| !v.is_empty())?;
    let report = crate::team_sandbox::enforce_now()?;
    let python_error = if report.enforced {
        python_capability().err()
    } else {
        Some("not checked (no sandbox)".to_string())
    };
    let probe = Probe {
        sandbox: report,
        python_error,
    };
    println!("{}", serde_json::to_string(&probe).unwrap_or_default());
    Some(0)
}

/// `Ok` when the interpreter the team's workers use reports the team-scope capability.
fn python_capability() -> Result<(), String> {
    let python = meerschaum_worker::worker::default_python();
    let output = crate::team_sandbox::confined_python_command(&python)?
        // This is the interpreter query inside an already-enforced outer probe. Keeping
        // PROBE_ENV would recursively dispatch another probe before the Python trampoline.
        .env_remove(PROBE_ENV)
        .args([
            meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
            "-c",
            "import json, meerschaum\n\
             from meerschaum._internal import team_sandbox\n\
             try:\n    from meerschaum._internal.team_scope import CAPABILITY\n\
             except Exception:\n    CAPABILITY = ''\n\
             report = team_sandbox.report() or team_sandbox.boot() or {}\n\
             print('MRSM_TEAM_PYTHON_CAPABILITY:' + json.dumps({'version':str(meerschaum.__version__), 'capability':CAPABILITY, 'enforced':report.get('enforced',False), 'canary':report.get('canary','unknown')}, separators=(',',':')))",
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("`{}` could not run: {e}", python.display()))?;
    let report = python_capability_report(&output.stdout, output.status.success());
    if report.as_ref().is_some_and(PythonCapability::scoped) {
        return Ok(());
    }
    let version = report
        .map(|report| report.version)
        .unwrap_or_else(|| "unknown".into());
    Err(format!(
        "the `meerschaum` package of `{}` ({version}) does not apply team scoping; install \
         `meerschaum>={}` there or point MRSM_PYTHON at an interpreter that has it",
        python.display(),
        meerschaum_core::python_guard::expected_version()
    ))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonCapability {
    version: String,
    capability: String,
    enforced: bool,
    canary: String,
}

impl PythonCapability {
    fn scoped(&self) -> bool {
        self.capability == meerschaum_worker::worker::TEAM_SCOPE_CAPABILITY
            && self.enforced
            && self.canary == "denied"
    }
}

fn python_capability_report(stdout: &[u8], success: bool) -> Option<PythonCapability> {
    const PREFIX: &str = "MRSM_TEAM_PYTHON_CAPABILITY:";
    if !success || stdout.len() > 16 * 1024 {
        return None;
    }
    let text = std::str::from_utf8(stdout).ok()?;
    let mut records = text.lines().filter_map(|line| line.strip_prefix(PREFIX));
    let record = records.next()?;
    if record.len() > 1024 || records.next().is_some() {
        return None;
    }
    let report: PythonCapability = serde_json::from_str(record).ok()?;
    if report.version.is_empty()
        || report.version.len() > 64
        || !report
            .version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
        || report.capability.len() > 64
        || report.canary.len() > 32
    {
        return None;
    }
    Some(report)
}

/// The shell of a team terminal of `team`: this program's shell as a team process.
pub fn team_shell(
    config: &Config,
    team: &str,
) -> Result<meerschaum_server::webterm::ShellCommand, String> {
    let process = team_process(config, team, "terminal")?;
    let program = meerschaum_core::self_exe::native_executable()
        .ok_or("Team terminals need the registered native mrsm host.")?
        .to_string_lossy()
        .into_owned();
    let args = Vec::new();
    Ok(meerschaum_server::webterm::ShellCommand {
        program,
        args,
        env: process.env,
        clear_env: true,
        cwd: Some(process.cwd),
    })
}

/// The isolated teams recorded under the root whose members' commands may run as team
/// execution (their scope says they run code), by name.
pub fn code_running_teams(config: &Config) -> Vec<String> {
    let root = config.paths().root_dir;
    let index: std::collections::BTreeMap<String, String> =
        std::fs::read_to_string(root.join(ts::SCOPES_DIR).join(ts::INDEX_FILE))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
    let mut teams: Vec<String> = index
        .values()
        .filter_map(|key| ts::read_scope_record(&root, key))
        .filter(|scope| !scope.open && !scope.deleted && scope.runs_code)
        .map(|scope| scope.team)
        .collect();
    teams.sort();
    teams.dedup();
    teams
}

/// Whether team execution is available for the code-running team `team` on this host, judged as
/// the API server judges it (`meerschaum_server::team_execution::judge`; a process of the team is
/// started to prove the sandbox).
pub fn team_status(config: &Config, team: &str) -> Result<(), String> {
    use meerschaum_core::value::Value;
    let mut record = meerschaum_core::access::Team::new(team);
    record
        .attributes
        .insert("isolated".into(), Value::Bool(true));
    record
        .attributes
        .insert("plugins_write".into(), Value::Bool(true));
    let host = probe(config, team);
    let facts = meerschaum_server::team_execution::Facts {
        mode: host.mode,
        support: sandbox::kernel_support(),
        container_socket: sandbox::container_socket_reachable(),
        probe: host.probe,
    };
    meerschaum_server::team_execution::judge(&record, &facts)
}

/// One line per code-running team: `(false, "available")` or `(true, why not)`.
pub fn status_lines(config: &Config) -> Vec<(bool, String)> {
    code_running_teams(config)
        .into_iter()
        .map(|team| match team_status(config, &team) {
            Ok(()) => (
                false,
                format!(
                    "Team execution for '{team}': available (its members' commands and terminal \
                     run as its sandboxed processes)."
                ),
            ),
            Err(why) => (
                true,
                format!("Team execution for '{team}': not available: {why}."),
            ),
        })
        .collect()
}

#[cfg(test)]
mod python_capability_tests {
    use super::*;
    const RECORD: &str = "MRSM_TEAM_PYTHON_CAPABILITY:{\"version\":\"5.1.0rc1\",\"capability\":\"team_scope\",\"enforced\":true,\"canary\":\"denied\"}";
    #[test]
    fn capability_record_is_framed_and_requires_a_successful_query() {
        let stdout = format!("An incidental import notice\n{RECORD}\n");
        let report = python_capability_report(stdout.as_bytes(), true).unwrap();
        assert_eq!(report.version, "5.1.0rc1");
        assert!(report.enforced);
        assert_eq!(report.canary, "denied");
        assert!(report.scoped());
        for (bytes, success) in [
            (stdout.as_bytes(), false),
            (b"{\"sandbox\":{\"enforced\":true}}".as_slice(), true),
            (
                b"MRSM_TEAM_PYTHON_CAPABILITY:{\"version\":\"5.1.0\"}".as_slice(),
                true,
            ),
        ] {
            assert!(python_capability_report(bytes, success).is_none());
        }
        assert!(python_capability_report(format!("{RECORD}\n{RECORD}").as_bytes(), true).is_none());
        assert!(python_capability_report(&vec![b'x'; 16 * 1024 + 1], true).is_none());
    }

    #[test]
    fn a_python_report_without_the_actual_scope_and_denied_canary_cannot_pass() {
        for record in [
            RECORD.replace("team_scope", ""),
            RECORD.replace("\"enforced\":true", "\"enforced\":false"),
            RECORD.replace("\"canary\":\"denied\"", "\"canary\":\"readable\""),
        ] {
            assert!(
                !python_capability_report(record.as_bytes(), true)
                    .unwrap()
                    .scoped()
            );
        }
    }
}
