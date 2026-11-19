//! Team execution (5.1): commands of a member of a team that runs code, run only inside that
//! team's sandboxed processes.
//!
//! A team "runs code" when it is isolated and lets its members install, edit and upload plugins
//! (`plugins_write`, [`meerschaum_core::access::Team::may_write_plugins`]): its members can
//! already run any Python in the team's processes. So a caller who acts for such a team
//! ([`crate::access::Access::acting_team`]) may run commands too (`sh`, `!command`, `python`,
//! plugin actions, editors in the terminal), but only as a process started the way the team's
//! jobs are: the team's configuration, the environment allowlist and the filesystem sandbox
//! applied before the command runs, and only where that sandbox is enforced here:
//!
//! 1. the team sandbox mode is not `off`;
//! 2. the sandbox denies Unix sockets (Landlock ABI 9 or the verified seccomp filter) and
//!    scopes signals (Landlock ABI 6), so a command cannot reach the server's processes;
//! 3. no container-runtime socket the server's user can open is left uncovered by the sandbox;
//! 4. a process of that team, started for the purpose in [`sandbox::Mode::RequireSockets`]
//!    (what every process of team execution runs in), applied the sandbox at
//!    the required socket and signal coverage, failed to read the canary and found a Python
//!    `meerschaum` package that applies team scoping (the handshake
//!    capability).
//!
//! [`judge`] is the pure decision (every condition, the first failure named); [`status`] gathers
//! the facts (the probe through [`crate::backends::ActionsBackend::team_execution_probe`], kept
//! for [`PROBE_TTL`]) and is what
//! [`crate::execute_policy::may_run_system_commands`] asks. macOS, Windows and kernels whose
//! sandbox cannot cover sockets and signals answer no.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use meerschaum_core::access::Team;
use meerschaum_core::team_scope::sandbox::{self, Canary, Mode, Report, Support};
use serde::{Deserialize, Serialize};

/// How long a probe of a team's process is reused.
pub const PROBE_TTL: Duration = Duration::from_secs(30);

/// How long a probe that did not succeed is reused (the team's scope is recorded shortly after
/// it is created).
pub const FAILED_PROBE_TTL: Duration = Duration::from_secs(3);

/// What a process of the team found when it was started for the probe.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Probe {
    /// Its sandbox.
    pub sandbox: Report,
    /// Why the Python package cannot run the team's code (`None`: it reports the team-scope
    /// capability).
    #[serde(default)]
    pub python_error: Option<String>,
}

/// What the host of the actions knows about a team's processes: the sandbox mode and a probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFacts {
    /// `system.engine.python_team_sandbox`.
    pub mode: Mode,
    /// The probe of one of the team's processes, or why none ran.
    pub probe: Result<Probe, String>,
}

/// Everything [`judge`] decides from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// The sandbox mode.
    pub mode: Mode,
    /// What this kernel offers.
    pub support: Support,
    /// A container socket the server's user can open, if any.
    pub container_socket: Option<PathBuf>,
    /// The probe of one of the team's processes.
    pub probe: Result<Probe, String>,
}

/// `Ok` when team execution is available for `team` under `facts`, else the first condition
/// that fails, in words.
pub fn judge(team: &Team, facts: &Facts) -> Result<(), String> {
    if !team.is_isolated() {
        return Err(format!("the team '{}' is not isolated", team.name));
    }
    if !team.may_write_plugins() {
        return Err(format!(
            "the team '{}' does not let its members install, edit and upload plugins",
            team.name
        ));
    }
    if facts.mode == Mode::Off {
        return Err(
            "the team sandbox is off on this server (`system.engine.python_team_sandbox: off`)"
                .to_string(),
        );
    }
    if let Support::Unsupported { reason } = &facts.support {
        return Err(format!("Landlock is unavailable on this server: {reason}"));
    }
    if facts.support.abi() < 6 {
        return Err(format!(
            "the sandbox on this server is \"{}\" (Landlock ABI {}); team execution needs \
             signal scoping (Landlock ABI 6, Linux 6.12) and Unix sockets denied by Landlock \
             or the seccomp filter",
            facts.support.level().unwrap_or(sandbox::LEVEL_FILESYSTEM),
            facts.support.abi()
        ));
    }
    let probe = facts
        .probe
        .as_ref()
        .map_err(|e| format!("the team's sandbox could not be proven: {e}"))?;
    let report = &probe.sandbox;
    if !report.enforced {
        return Err(format!(
            "the team's sandbox is not enforced: {}",
            report.reason.as_deref().unwrap_or("no reason given")
        ));
    }
    if report.canary != Canary::Denied {
        return Err(
            "the team's sandbox could not be proven: its process did not fail to read the canary"
                .to_string(),
        );
    }
    if report.abi > facts.support.abi() {
        return Err(format!(
            "the host's sandbox is \"{}\" (Landlock ABI {}), but the team's probe claims ABI {}",
            facts.support.level().unwrap_or(sandbox::LEVEL_FILESYSTEM),
            facts.support.abi(),
            report.abi
        ));
    }
    if !report.covers_sockets_and_signals() {
        let socket = facts
            .container_socket
            .as_ref()
            .map(|path| format!("; the container socket `{}` is not covered", path.display()))
            .unwrap_or_default();
        return Err(format!(
            "the team's sandbox is \"{}\": {}; team execution needs Unix sockets denied and \
             signals scoped{socket}",
            report.level(),
            report.sockets_words()
        ));
    }
    // Judged as every process of team execution judges itself.
    let required = Report {
        mode: Mode::RequireSockets,
        ..report.clone()
    };
    if let Err(why) = required.verdict() {
        return Err(format!("the team's sandbox could not be proven: {why}"));
    }
    if let Some(error) = &probe.python_error {
        return Err(format!(
            "the Python package cannot run the team's code: {error}"
        ));
    }
    Ok(())
}

/// The answer for one team, as [`status`] gives it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Status {
    /// The team.
    pub team: String,
    /// Whether its members' commands run as team execution here.
    pub available: bool,
    /// Why not.
    pub reason: Option<String>,
}

/// The probes of one server, by team scope key.
#[derive(Default)]
pub struct ProbeCache {
    probes: Mutex<HashMap<String, (Instant, HostFacts)>>,
}

impl ProbeCache {
    fn get(&self, key: &str) -> Option<HostFacts> {
        let probes = self.probes.lock().unwrap_or_else(|e| e.into_inner());
        probes
            .get(key)
            .filter(|(at, facts)| {
                let ttl = if facts.probe.is_ok() {
                    PROBE_TTL
                } else {
                    FAILED_PROBE_TTL
                };
                at.elapsed() < ttl
            })
            .map(|(_, facts)| facts.clone())
    }

    fn put(&self, key: &str, facts: HostFacts) {
        self.probes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_string(), (Instant::now(), facts));
    }

    /// Forget every probe (a team changed).
    pub fn clear(&self) {
        self.probes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// Whether team execution is available for `team` on this server (the probe is reused for
/// [`PROBE_TTL`]; the team's own settings, the mode, the kernel and the container socket are
/// read every time).
pub async fn status(state: &crate::state::AppState, team: &Team) -> Status {
    let refuse = |reason: String| Status {
        team: team.name.clone(),
        available: false,
        reason: Some(reason),
    };
    // The team's own switches first: no process is started for a team that does not run code.
    if !team.is_isolated() || !team.may_write_plugins() {
        let facts = Facts {
            mode: Mode::Off,
            support: Support::Unsupported {
                reason: String::new(),
            },
            container_socket: None,
            probe: Err(String::new()),
        };
        return refuse(judge(team, &facts).err().unwrap_or_default());
    }
    match team.execution() {
        Err(error) => return refuse(error.to_string()),
        Ok(settings) if settings.mode.is_remote() => {
            return match state.actions.executor_ready(team).await {
                Ok(()) => Status {
                    team: team.name.clone(),
                    available: true,
                    reason: None,
                },
                Err(reason) => refuse(reason),
            };
        }
        Ok(_) => {}
    }
    let key = team
        .id
        .clone()
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| format!("name-{}", team.name.to_lowercase()));
    let host = match state.team_execution_probes.get(&key) {
        Some(facts) => Some(facts),
        None => {
            let facts = state.actions.team_execution_probe(team).await;
            if let Some(facts) = &facts {
                state.team_execution_probes.put(&key, facts.clone());
            }
            facts
        }
    };
    let Some(host) = host else {
        return refuse("this server cannot start the team's processes".to_string());
    };
    let facts = Facts {
        mode: host.mode,
        support: sandbox::kernel_support(),
        container_socket: sandbox::container_socket_reachable(),
        probe: host.probe,
    };
    match judge(team, &facts) {
        Ok(()) => Status {
            team: team.name.clone(),
            available: true,
            reason: None,
        },
        Err(reason) => refuse(reason),
    }
}

/// The team a caller would run commands for as team execution, if they act for a team that
/// runs code (`None`: they do not; administrators and the open side never do). The team must be
/// the caller's primary team, the team their jobs are recorded in.
pub fn code_running_team(caller: &crate::access::Access) -> Option<Team> {
    if caller.no_auth
        || caller.full_admin()
        || !caller.principal.is_isolated()
        || matches!(
            &caller.auth,
            Some(crate::auth::Principal::Execution(identity))
                if identity.grant.purpose == crate::executor::callbacks::Purpose::Command
        )
    {
        return None;
    }
    let team = caller.acting_team(None)?;
    let primary = meerschaum_core::access::same_team(Some(&team.name), Some(&caller.primary_team));
    (primary && team.is_isolated() && team.may_write_plugins()).then_some(team)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::value::Value;

    fn team(isolated: bool, plugins_write: bool) -> Team {
        let mut team = Team::new("Wet");
        team.attributes
            .insert("isolated".into(), Value::Bool(isolated));
        team.attributes
            .insert("plugins_write".into(), Value::Bool(plugins_write));
        team
    }

    fn good_report() -> Report {
        let (features, _) = sandbox::features_of(9);
        Report {
            mode: Mode::RequireSockets,
            enforced: true,
            abi: 9,
            kernel_abi: 11,
            features,
            canary: Canary::Denied,
            ..Report::default()
        }
    }

    fn good() -> Facts {
        Facts {
            mode: Mode::Auto,
            support: Support::Available { kernel_abi: 11 },
            container_socket: None,
            probe: Ok(Probe {
                sandbox: good_report(),
                python_error: None,
            }),
        }
    }

    #[test]
    fn every_condition_must_hold() {
        let wet = team(true, true);
        assert_eq!(judge(&wet, &good()), Ok(()));
        // A container socket is covered by ABI 9's Unix socket rules.
        let covered = Facts {
            container_socket: Some("/run/docker.sock".into()),
            ..good()
        };
        assert_eq!(judge(&wet, &covered), Ok(()));

        let reason = |t: &Team, f: Facts| judge(t, &f).unwrap_err();
        assert!(reason(&team(false, true), good()).contains("not isolated"));
        assert!(reason(&team(true, false), good()).contains("install, edit and upload"));
        let off = Facts {
            mode: Mode::Off,
            ..good()
        };
        assert!(reason(&wet, off).contains("sandbox is off"));
        let no_landlock = Facts {
            support: Support::Unsupported {
                reason: "Landlock exists on Linux only".into(),
            },
            ..good()
        };
        assert!(reason(&wet, no_landlock).contains("Linux only"));
        for abi in [1, 5, 6, 8] {
            let old = Facts {
                support: Support::Available { kernel_abi: abi },
                ..good()
            };
            let why = reason(&wet, old);
            assert!(why.contains(sandbox::LEVEL_FILESYSTEM), "{abi}: {why}");
        }
        let failed = Facts {
            probe: Err("the process exited with 1".into()),
            ..good()
        };
        assert!(reason(&wet, failed).contains("could not be proven"));
        let unenforced = Facts {
            probe: Ok(Probe {
                sandbox: Report::not_enforced(Mode::Require, "seccomp"),
                python_error: None,
            }),
            ..good()
        };
        assert!(reason(&wet, unenforced).contains("not enforced"));
        let readable = Facts {
            probe: Ok(Probe {
                sandbox: Report {
                    canary: Canary::Readable,
                    ..good_report()
                },
                python_error: None,
            }),
            ..good()
        };
        assert!(reason(&wet, readable).contains("canary"));
        let partial = Facts {
            probe: Ok(Probe {
                sandbox: Report {
                    abi: 8,
                    level: sandbox::LEVEL_FILESYSTEM.to_string(),
                    features: sandbox::features_of(8).0,
                    ..good_report()
                },
                python_error: None,
            }),
            ..good()
        };
        assert!(reason(&wet, partial).contains(sandbox::LEVEL_FILESYSTEM));
        let old_python = Facts {
            probe: Ok(Probe {
                sandbox: good_report(),
                python_error: Some("meerschaum 5.0.2 predates team scoping".into()),
            }),
            ..good()
        };
        assert!(reason(&wet, old_python).contains("5.0.2"));
    }

    #[test]
    fn verified_seccomp_with_signal_scoping_permits_team_execution() {
        for abi in [6, 8] {
            let mut facts = good();
            facts.support = Support::Available { kernel_abi: abi };
            facts.container_socket = Some("/run/docker.sock".into());
            let report = &mut facts.probe.as_mut().unwrap().sandbox;
            report.abi = abi;
            report.kernel_abi = abi;
            report.features = sandbox::features_of(abi).0;
            report.level = sandbox::LEVEL_FILESYSTEM.into();
            report.unix_sockets = sandbox::SOCKETS_BY_SECCOMP.into();
            report.socket_probe = sandbox::SocketProbe::Denied;
            assert_eq!(judge(&team(true, true), &facts), Ok(()));
            for proof in [
                sandbox::SocketProbe::Created,
                sandbox::SocketProbe::NotChecked,
            ] {
                facts.probe.as_mut().unwrap().sandbox.socket_probe = proof;
                assert!(
                    judge(&team(true, true), &facts)
                        .unwrap_err()
                        .contains("could still create")
                );
            }
        }
    }
}
