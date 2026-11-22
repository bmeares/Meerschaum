//! Enforcing a spec on the current process, irreversibly, and the [`Report`] it produces: the
//! filesystem rules, the network denials of API and metadata processes, and the verdict a host
//! checks.

use super::*;

/// What a team process reports about its sandbox (in its handshake, its job log, `doctor`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Report {
    /// The mode it ran in.
    pub mode: Mode,
    /// Whether the process restricted itself.
    pub enforced: bool,
    /// The ABI Meerschaum used.
    #[serde(default)]
    pub abi: u32,
    /// The kernel's ABI.
    #[serde(default)]
    pub kernel_abi: u32,
    /// The features in force.
    #[serde(default)]
    pub features: Vec<String>,
    /// The features the kernel lacks.
    #[serde(default)]
    pub unavailable: Vec<String>,
    /// The canary's result.
    #[serde(default)]
    pub canary: Canary,
    /// Why it is not enforced (or a caveat).
    #[serde(default)]
    pub reason: Option<String>,
    /// The rules added.
    #[serde(default)]
    pub rules: usize,
    /// Rules dropped because they would cover a protected path.
    #[serde(default)]
    pub dropped: Vec<String>,
    /// The level in force ([`LEVEL_FILESYSTEM`] or [`LEVEL_SOCKETS`]; empty when not enforced).
    #[serde(default)]
    pub level: String,
    /// How Unix sockets are denied to the process: [`SOCKETS_BY_LANDLOCK`],
    /// [`SOCKETS_BY_SECCOMP`], or empty (not denied; [`Report::sockets_reason`] says why).
    #[serde(default)]
    pub unix_sockets: String,
    /// The proof of [`SOCKETS_BY_SECCOMP`]: creating a Unix socket after the filter.
    #[serde(default)]
    pub socket_probe: SocketProbe,
    /// Why Unix sockets are not denied (no filter for this architecture, a container runtime
    /// refused it).
    #[serde(default)]
    pub sockets_reason: Option<String>,
    /// Applied host-team resource limits and egress policy (omitted when unrestricted).
    #[serde(default, skip_serializing_if = "LimitsReport::is_empty")]
    pub limits: LimitsReport,
}

impl Report {
    /// Judge an active report against mandatory filesystem, Unix-socket and signal protection,
    /// regardless of the mode recorded in an older spec or inherited process report.
    pub fn require_sockets_verdict(&self) -> Result<(), String> {
        let mut required = self.clone();
        required.mode = Mode::RequireSockets;
        required.verdict()
    }

    /// A process that was not sandboxed, and why.
    pub fn not_enforced(mode: Mode, reason: impl Into<String>) -> Report {
        Report {
            mode,
            reason: Some(reason.into()),
            ..Report::default()
        }
    }

    /// `Err` (why) when a process with this report must not run the team's code: in `require`
    /// mode unless the sandbox is enforced and the canary was denied; in `auto` mode when the
    /// sandbox claims to be enforced but the canary was not denied.
    pub fn verdict(&self) -> Result<(), String> {
        match self.mode {
            Mode::Off => self.limits.verdict(),
            Mode::Auto if !self.enforced => self.limits.verdict(),
            mode if mode.requires() && !self.enforced => Err(format!(
                "the filesystem sandbox is required (`system.engine.python_team_sandbox: {}`) \
                 but is not enforced: {}",
                mode.as_str(),
                self.reason.as_deref().unwrap_or("no reason given")
            )),
            Mode::RequireSockets if !self.covers_sockets_and_signals() => Err(format!(
                "the sandbox must deny Unix sockets and signals to team code \
                 (`system.engine.python_team_sandbox: require_sockets`), but here: {}{}",
                self.sockets_words(),
                if self.denies_unix_sockets() {
                    format!(
                        " and this kernel's Landlock ABI {} cannot scope signals (that needs \
                         ABI 6, Linux 6.12)",
                        self.abi
                    )
                } else {
                    String::new()
                }
            )),
            _ if self.unix_sockets == SOCKETS_BY_SECCOMP
                && self.socket_probe != SocketProbe::Denied =>
            {
                Err(format!(
                    "the sandbox reports Unix sockets denied by its seccomp filter, but the \
                     process could still create one ({})",
                    match &self.socket_probe {
                        SocketProbe::Created => "it was created".to_string(),
                        SocketProbe::NotChecked => "not checked".to_string(),
                        SocketProbe::Error(e) => e.clone(),
                        SocketProbe::Denied => String::new(),
                    }
                ))
            }
            _ => match &self.canary {
                Canary::Denied => self.limits.verdict(),
                Canary::Readable => Err(
                    "the filesystem sandbox reports itself enforced, but the process could still \
                     read the canary file it must not reach"
                        .to_string(),
                ),
                Canary::Missing => Err(
                    "the filesystem sandbox could not be proven: the canary file does not exist"
                        .to_string(),
                ),
                Canary::NotChecked => {
                    Err("the filesystem sandbox was not checked against its canary".to_string())
                }
                Canary::Error(e) => Err(format!(
                    "the filesystem sandbox could not be proven: opening the canary failed with \
                     {e}"
                )),
            },
        }
    }

    /// Whether Unix sockets are denied to the process (by Landlock or the seccomp filter).
    pub fn denies_unix_sockets(&self) -> bool {
        self.enforced
            && (self.unix_sockets == SOCKETS_BY_LANDLOCK
                || self.unix_sockets == SOCKETS_BY_SECCOMP
                || (self.unix_sockets.is_empty() && self.level() == LEVEL_SOCKETS))
    }

    /// What `require_sockets` asks for: Unix sockets denied (either mechanism) and signals
    /// scoped (Landlock ABI 6).
    pub fn covers_sockets_and_signals(&self) -> bool {
        self.denies_unix_sockets()
            && (self.level() == LEVEL_SOCKETS || self.features.iter().any(|f| f == "scope_signal"))
    }

    /// [`unix_sockets_words`] for this report.
    pub fn sockets_words(&self) -> String {
        let mechanism = if self.denies_unix_sockets() && self.unix_sockets.is_empty() {
            SOCKETS_BY_LANDLOCK
        } else if self.enforced {
            self.unix_sockets.as_str()
        } else {
            ""
        };
        let reason = if self.enforced {
            self.sockets_reason.as_deref()
        } else {
            Some("the sandbox is not enforced")
        };
        unix_sockets_words(mechanism, reason)
    }

    /// The level in force: [`Report::level`] as recorded, else from the features (a report of an
    /// older package); empty when not enforced.
    pub fn level(&self) -> &str {
        if !self.enforced {
            return "";
        }
        if !self.level.is_empty() {
            return &self.level;
        }
        let has = |f: &str| self.features.iter().any(|x| x == f);
        if has("resolve_unix") && has("scope_signal") && has("scope_abstract_unix") {
            LEVEL_SOCKETS
        } else {
            LEVEL_FILESYSTEM
        }
    }

    /// One line for logs and `doctor`.
    pub fn describe(&self) -> String {
        if !self.enforced {
            return format!(
                "not enforced ({}): {}; {}",
                self.mode.as_str(),
                self.reason.as_deref().unwrap_or("off"),
                self.sockets_words()
            );
        }
        let kernel = if self.kernel_abi != self.abi {
            format!(" (kernel ABI {})", self.kernel_abi)
        } else {
            String::new()
        };
        let mut text = format!(
            "enforced, {} ({}): Landlock ABI {}{kernel}, {} rules: {}",
            self.level(),
            self.mode.as_str(),
            self.abi,
            self.rules,
            self.features.join(", ")
        );
        if !self.unavailable.is_empty() {
            text.push_str(&format!("; unavailable: {}", self.unavailable.join(", ")));
        }
        text.push_str(match &self.canary {
            Canary::Denied => "; canary denied",
            Canary::Readable => "; CANARY READABLE",
            _ => "; canary not proven",
        });
        text.push_str(&format!("; {}", self.sockets_words()));
        if !self.limits.is_empty() {
            let summary = self.limits.describe();
            if !summary.is_empty() {
                text.push_str(&format!("; limits: {summary}"));
            }
        }
        if let Some(reason) = &self.reason {
            text.push_str(&format!("; {reason}"));
        }
        text
    }
}

/// Report and native profile captured together by the first irreversible enforcement.
struct Enforcement {
    report: Report,
    native_api: bool,
    // Not serialized or exposed through Python. This is selected once, before site hooks.
    api_transport: Option<Result<super::api_execution::Alias, String>>,
}
static ENFORCED: std::sync::OnceLock<Enforcement> = std::sync::OnceLock::new();

/// This process's report (`None` until it enforced its spec).
pub fn current() -> Option<Report> {
    ENFORCED.get().map(|enforced| enforced.report.clone())
}

/// Sticky native API provenance; removing environment variables cannot restore ambient
/// compiler policy after this process enforced its protected host runtime.
pub fn is_native_api() -> bool {
    ENFORCED.get().is_some_and(|enforced| enforced.native_api)
}

/// Build a native per-launch transport from the first mandatory enforcement.
/// Later configuration/environment changes and repeated enforcement cannot select another
/// callback or turn an ordinary Team process into API execution. No bearer is retained here.
pub fn with_api_transport<T>(
    build: impl FnOnce(&super::api_execution::Alias) -> Result<T, String>,
) -> Result<Option<T>, String> {
    let Some(enforced) = ENFORCED.get().filter(|enforced| enforced.native_api) else {
        return Ok(None);
    };
    enforced.report.require_sockets_verdict()?;
    let alias = enforced
        .api_transport
        .as_ref()
        .ok_or("The native API runtime has no immutable host-prepared graph transport.")?
        .as_ref()
        .map_err(|error| error.clone())?;
    build(alias).map(Some)
}

/// Irreversibly deny new network connections on every thread and descendant of an already
/// confined native process. Reserved for credential-free metadata discovery. Connected Unix
/// stream/seqpacket pairs remain available for the worker protocol; the launcher must pass no
/// other connected network descriptors. Unsupported platforms refuse before discovery.
pub fn deny_network() -> Result<(), String> {
    current()
        .ok_or("Network denial requires an active native sandbox.")?
        .require_sockets_verdict()?;
    #[cfg(target_os = "linux")]
    {
        imp::deny_network()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("Credential-free network denial requires supported Linux seccomp.".into())
    }
}

/// Deny inbound listeners in an API-only process on every thread and descendant. Outbound
/// connections and connected Unix worker pairs remain available. A successful native
/// filesystem/socket/signal sandbox must already be active.
pub fn deny_inbound_network() -> Result<(), String> {
    current()
        .ok_or("Inbound network denial requires an active native sandbox.")?
        .require_sockets_verdict()?;
    #[cfg(target_os = "linux")]
    {
        imp::deny_inbound_network()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("API-only listener denial requires supported Linux seccomp.".into())
    }
}

/// Restrict this process (and everything it starts) to `spec`'s rules plus `extra` (the
/// interpreter's paths; protected paths dropped), then try the canary. Irreversible: call it
/// in a process started for the team, before any of the team's code runs. Once per process:
/// a second call returns the first report. Never fails: a sandbox that could not be applied
/// is a report that is not enforced, which [`Report::verdict`] judges by mode.
pub fn enforce(spec: &Spec, extra: &[Rule]) -> Report {
    if let Some(enforced) = ENFORCED.get() {
        return enforced.report.clone();
    }
    let native_api = spec
        .runtime
        .as_ref()
        .is_some_and(|runtime| runtime.api_only)
        || std::env::var(super::api_execution::PROFILE_VAR).as_deref() == Ok("1");
    let report = if spec.mode == Mode::Off {
        let mut report = Report::not_enforced(Mode::Off, "off");
        if spec.limits.has_process_limits() {
            report.limits = LimitsReport::from_limits(&spec.limits);
            imp::apply_rlimits(&spec.limits, &mut report.limits);
            imp::enforce_egress(&spec.limits, &mut report.limits);
        }
        report
    } else {
        let mut rules = spec.rules.clone();
        rules.extend(extra.iter().cloned());
        let (rules, dropped) = spec.allowed(rules);
        let mut report = imp::restrict(spec.mode, &rules, spec.max_abi, &spec.limits);
        report.dropped = dropped
            .iter()
            .map(|r| r.path.display().to_string())
            .collect();
        if report.enforced {
            report.canary = probe_canary(&spec.canary);
            report.level = level_of(report.abi).to_string();
            // Unix sockets: Landlock denies those outside the rules from ABI 9; before, a
            // seccomp filter refuses to create any (installed after Landlock, which set
            // `no_new_privs`, and before any of the team's code runs).
            if report.abi >= SOCKETS_ABI {
                report.unix_sockets = SOCKETS_BY_LANDLOCK.to_string();
            } else {
                imp::deny_unix_sockets(&mut report);
            }
            if spec.limits.has_process_limits() {
                if report.limits.is_empty() {
                    report.limits = LimitsReport::from_limits(&spec.limits);
                }
                imp::apply_rlimits(&spec.limits, &mut report.limits);
                imp::enforce_egress(&spec.limits, &mut report.limits);
            }
        }
        report
    };
    let api_transport = native_api.then(|| {
        report.require_sockets_verdict()?;
        super::api_execution::transport_capture::from_spec(spec, extra)
    });
    ENFORCED
        .get_or_init(|| Enforcement {
            report,
            native_api,
            api_transport,
        })
        .report
        .clone()
}

/// [`enforce`] the spec [`SPEC_VAR`] names; `None` when it is unset (not a sandboxed team
/// process). A spec that cannot be read is a report that is not enforced, in `require` mode
/// (the server asked for a sandbox).
pub fn enforce_from_env(extra: &[Rule]) -> Option<Report> {
    if let Some(enforced) = ENFORCED.get() {
        return Some(enforced.report.clone());
    }
    let path = std::env::var_os(SPEC_VAR).filter(|v| !v.is_empty())?;
    match Spec::read(Path::new(&path)) {
        Ok(spec) => Some(enforce(&spec, extra)),
        Err(e) => Some(
            ENFORCED
                .get_or_init(|| Enforcement {
                    api_transport: None,
                    native_api: std::env::var(super::api_execution::PROFILE_VAR).as_deref()
                        == Ok("1"),
                    report: Report::not_enforced(
                        Mode::Require,
                        format!(
                            "the sandbox's rules `{}` could not be read: {e}",
                            Path::new(&path).display()
                        ),
                    ),
                })
                .report
                .clone(),
        ),
    }
}
