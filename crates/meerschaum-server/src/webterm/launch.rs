//! Unified tmux multiplexer lifecycle (`TmuxMux` + `TmuxProfile`), environment files, and launch.

use super::*;

/// The tmux session name for a terminal: tmux rejects `.` and `:` in names, so they are written
/// `_d` and `_c`, and `_` itself `__` (5.1: one user's session name is never another's, as
/// `a.b` and `a_b` both were `a_b`). The suffix (`api:webterm:tmux:session_suffix`, the same for
/// everyone) keeps the old replacement.
pub fn tmux_session_name(term_name: &str, suffix: &str) -> String {
    let mut name = String::with_capacity(term_name.len() + suffix.len());
    for c in term_name.chars() {
        match c {
            '_' => name.push_str("__"),
            '.' => name.push_str("_d"),
            ':' => name.push_str("_c"),
            c => name.push(c),
        }
    }
    name.push_str(&suffix.replace(['.', ':'], "_"));
    name
}

/// Which tmux multiplexer server and environment-scrubbing policy a terminal uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TmuxProfile {
    /// Administrators' dedicated tmux server (`tmux.sock`).
    Full,
    /// Restricted terminals' locked-down tmux server (`tmux-r.sock`).
    Restricted,
    /// Per-team locked-down tmux server (`<stem>-t-<hash>.sock`), always isolated with `env -i`.
    Team(String),
}

impl TmuxProfile {
    /// Whether this profile starts tmux commands and windows from a clean environment (`env -i`).
    pub(super) fn isolate_env(&self, shell: &ShellCommand) -> bool {
        matches!(self, Self::Team(_)) || shell.clear_env
    }

    /// Whether `key` must be omitted from the window's sourced environment file: a window that
    /// starts under `env -i` (which wipes tmux's `TERM`) keeps the shell's own `TERM`.
    fn skip_env_key(&self, shell: &ShellCommand, key: &str) -> bool {
        if self.isolate_env(shell) {
            ENV_SKIP_ISOLATED.contains(&key)
        } else {
            ENV_SKIP.contains(&key)
        }
    }

    /// Whether the tmux server itself may hold inherited job-context variables that must be scrubbed.
    fn needs_server_scrub(&self) -> bool {
        !matches!(self, Self::Team(_))
    }

    /// Derive the dedicated [`TmuxSettings`] for this profile from the base settings.
    pub(super) fn settings(&self, base: &TmuxSettings) -> Option<TmuxSettings> {
        match self {
            Self::Full => Some(base.clone()),
            Self::Restricted => restricted_tmux(base),
            Self::Team(team) => team::team_tmux(base, team),
        }
    }
}

/// Unified tmux multiplexer handle parameterized by [`TmuxProfile`].
///
/// Consolidates socket selection, environment scrubbing, atomic `0600` env-file generation,
/// `env -i` / `sh -c` wrapper construction, session probing, window counting, window creation,
/// and lockdown verification across `Full`, `Restricted`, and `Team` modes.
#[derive(Debug, Clone)]
pub(super) struct TmuxMux {
    pub(super) settings: TmuxSettings,
    pub(super) profile: TmuxProfile,
}

impl TmuxMux {
    pub(super) fn new(settings: TmuxSettings, profile: TmuxProfile) -> Self {
        Self { settings, profile }
    }

    pub(super) fn from_base(base: &TmuxSettings, profile: TmuxProfile) -> Option<Self> {
        let settings = profile.settings(base)?;
        Some(Self { settings, profile })
    }

    /// A tmux command targeting this mux's dedicated socket (never the default server),
    /// with the profile's environment isolation applied.
    pub(super) fn command(&self, shell: Option<&ShellCommand>) -> Option<std::process::Command> {
        let socket = self.settings.socket.as_ref()?;
        let mut command = std::process::Command::new(&self.settings.binary);
        command.arg("-S").arg(socket);
        self.apply_command_env(&mut command, shell);
        Some(command)
    }

    /// A tmux command for window/pane queries. `Restricted` and `Team` always require a dedicated
    /// socket; `Full` permits an optional socket for unit tests without a running server.
    fn window_command(&self, shell: &ShellCommand) -> Option<std::process::Command> {
        let mut command = std::process::Command::new(&self.settings.binary);
        match (&self.profile, &self.settings.socket) {
            (_, Some(socket)) => {
                command.arg("-S").arg(socket);
            }
            (TmuxProfile::Full, None) => {}
            (TmuxProfile::Restricted | TmuxProfile::Team(_), None) => return None,
        }
        self.apply_command_env(&mut command, Some(shell));
        Some(command)
    }

    fn apply_command_env(&self, command: &mut std::process::Command, shell: Option<&ShellCommand>) {
        let isolated = match shell {
            Some(shell) => self.profile.isolate_env(shell),
            None => matches!(self.profile, TmuxProfile::Team(_)),
        };
        if isolated {
            command.env_clear().envs(team::minimal_env());
        } else if let Some(shell) = shell {
            command.envs(shell.env.iter().map(|(key, value)| (key, value)));
        }
        command.env_remove("TMUX").env_remove("TMUX_PANE");
        for var in job_context_vars() {
            command.env_remove(var);
        }
    }

    /// Whether the tmux `session` (exact name) exists on this mux's socket.
    pub(super) fn has_session(&self, session: &str) -> bool {
        let Some(mut command) = self.command(None) else {
            return false;
        };
        command.args(["has-session", "-t", &format!("={session}")]);
        tmux_output(&mut command).is_some_and(|output| output.status.success())
    }

    /// End the tmux `session` (exact name) on this mux's socket, with every window and process.
    /// Nothing happens without a dedicated socket, a running server or such a session.
    pub(super) fn end_session(&self, session: &str) {
        if let Some(mut command) = self.command(None) {
            command.args(["kill-session", "-t", &format!("={session}")]);
            let _ = tmux_output(&mut command);
        }
    }

    /// How many windows the tmux `session` (exact name) has; `None` when tmux cannot say.
    pub(super) fn window_count(&self, shell: &ShellCommand, session: &str) -> Option<usize> {
        let mut command = self.window_command(shell)?;
        let output = tmux_output(command.args([
            "list-windows",
            "-t",
            &format!("={session}"),
            "-F",
            "#{window_id}",
        ]))?;
        output.status.success().then(|| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count()
        })
    }

    /// Read the active pane's PID for `session` (exact name) on this mux's socket.
    pub(super) fn pane_pid(&self, shell: &ShellCommand, session: &str) -> Option<u32> {
        let mut command = self.window_command(shell)?;
        // `=`: exactly this terminal's session, never another whose name starts with it.
        let target = format!("={session}:");
        let output =
            tmux_output(command.args(["display-message", "-p", "-t", &target, "#{pane_pid}"]))?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8(output.stdout).ok()?.trim().parse().ok()
    }

    /// Write the shell's environment file (`0600`, replaced atomically) and return its path.
    pub(super) fn write_env_file(&self, shell: &ShellCommand) -> std::io::Result<PathBuf> {
        let mut vars: std::collections::BTreeMap<String, String> =
            if self.profile.isolate_env(shell) {
                Default::default()
            } else {
                meerschaum_core::process_env::vars().into_iter().collect()
            };
        for (key, value) in &shell.env {
            vars.insert(key.clone(), value.clone());
        }
        let context = job_context_vars();
        let mut body = String::new();
        for (key, value) in &vars {
            if self.profile.skip_env_key(shell, key)
                || !valid_env_name(key)
                || context.contains(key)
            {
                continue;
            }
            body.push_str(&format!("{key}={}\n", sh_quote(value)));
        }
        create_private_dir(&self.settings.env_dir)?;
        remove_stale_env_files(&self.settings.env_dir);
        // One file per launch, which the window's `sh` removes once it has read it
        // (`shell_argv`): two terminals starting at once never read each other's (a
        // restricted terminal's carries its own grant), and none stays behind.
        static LAUNCHES: AtomicUsize = AtomicUsize::new(0);
        let prefix = match self.profile {
            TmuxProfile::Team(_) => "webterm.team",
            TmuxProfile::Full | TmuxProfile::Restricted => "webterm",
        };
        let stem = format!(
            "{prefix}.{}.{}",
            std::process::id(),
            LAUNCHES.fetch_add(1, Ordering::Relaxed)
        );
        let path = self.settings.env_dir.join(format!("{stem}.env"));
        let temporary = self.settings.env_dir.join(format!("{stem}.env.tmp"));
        write_private_file(&temporary, body.as_bytes())?;
        std::fs::rename(&temporary, &path)?;
        Ok(path)
    }

    /// The argv tmux runs in a window: `sh` sources the `0600` environment file and removes it,
    /// unsets the server's job context, then replaces itself with the shell. Isolated profiles
    /// start through `env -i /bin/sh` using the probed absolute `env` path.
    pub(super) fn shell_argv(&self, shell: &ShellCommand) -> std::io::Result<Vec<String>> {
        let env_file = self.write_env_file(shell)?;
        // The names are `valid_env_name`s: safe to write into the script as they are.
        let unset = job_context_vars().join(" ");
        let mut argv: Vec<String> = if self.profile.isolate_env(shell) {
            vec![env_program().to_string(), "-i".into(), "/bin/sh".into()]
        } else {
            vec!["sh".into()]
        };
        argv.extend([
            "-c".into(),
            format!(
                "set -a; . \"$0\"; set +a; command -p rm -f -- \"$0\"; unset {unset}; exec \"$@\""
            ),
            env_file.display().to_string(),
            shell.program.clone(),
        ]);
        argv.extend(shell.args.iter().cloned());
        Ok(argv)
    }

    /// Open a window running `shell` in `session` (exact name), unless it already has
    /// [`MAX_SESSION_WINDOWS`] (or its windows cannot be counted): `Ok(false)` then.
    pub(super) fn new_window(
        &self,
        shell: &ShellCommand,
        guardian: Option<&NativeGuardian>,
        session: &str,
    ) -> std::io::Result<bool> {
        // One at a time, so two tabs of a session cannot both pass the count.
        static OPENING: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _opening = OPENING.lock().unwrap_or_else(|e| e.into_inner());
        if self
            .window_count(shell, session)
            .is_none_or(|n| n >= MAX_SESSION_WINDOWS)
        {
            return Ok(false);
        }
        let prepared = match guardian {
            Some(guardian) => guardian.prepare_window_shell(shell)?,
            None => shell.clone(),
        };
        let argv = self.shell_argv(&prepared)?;
        if self.profile.needs_server_scrub() {
            self.scrub_server_env(Some(session));
        }
        let Some(mut command) = self.window_command(&prepared) else {
            return Ok(false);
        };
        // `=`: exactly this session, never another whose name starts with it.
        command
            .args([
                "new-window",
                "-t",
                &format!("={session}:"),
                "-n",
                TMUX_WINDOW_NAME,
            ])
            .args(&argv)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        Ok(tmux_output(&mut command).is_some_and(|output| output.status.success()))
    }

    /// Write the locked-down config, claim server ownership, and verify lockdown if already running.
    fn prepare_locked_server(&self, label: &str) -> std::io::Result<()> {
        if self.settings.socket.is_none() {
            return Err(std::io::Error::other(format!("no {label} tmux socket")));
        }
        write_restricted_conf(&self.settings)?;
        self.claim_server();
        if self.server_running() && !self.apply_restricted_conf() {
            return Err(std::io::Error::other(format!(
                "the {label} tmux configuration could not be applied"
            )));
        }
        Ok(())
    }

    /// Start a detached tmux session running `shell` on this mux's dedicated socket.
    fn start_detached_session(
        &self,
        session: &str,
        shell: &ShellCommand,
        cwd: Option<PathBuf>,
        label: &str,
    ) -> std::io::Result<()> {
        let argv = self.shell_argv(shell)?;
        let mut command = self
            .command(Some(shell))
            .ok_or_else(|| std::io::Error::other(format!("no {label} tmux socket")))?;
        command.arg("-u");
        if let Some(file) = &self.settings.config_file {
            command.args(["-f", file]);
        }
        command
            .args(["new-session", "-d", "-s", session, "-n", TMUX_WINDOW_NAME])
            .args(&argv)
            .env("TERM", "xterm-256color");
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        if tmux_output(&mut command).is_some_and(|output| output.status.success()) {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "tmux could not start the {label} session"
            )))
        }
    }

    /// Build the `attach-session` command for a locked-down server (`Restricted` or `Team`).
    fn attach_builder(&self, session: &str) -> CommandBuilder {
        let mut builder = CommandBuilder::new(&self.settings.binary);
        builder.arg("-S");
        builder.arg(self.settings.socket.as_ref().expect("dedicated socket"));
        builder.arg("-u");
        builder.args(["attach-session", "-t", &format!("={session}")]);
        builder
    }
}

/// A started terminal.
pub(super) struct Launched {
    pub(super) pty: Pty,
    /// The tmux session it attached to (the tmux path).
    pub(super) session: Option<String>,
    /// The tmux multiplexer that session lives on (carries both socket settings and profile).
    pub(super) mux: Option<TmuxMux>,
    /// The shell a new window runs (a restricted terminal's carries its grant).
    pub(super) shell: ShellCommand,
    /// A restricted terminal's own grant on the plain path (one shell per connection).
    pub(super) grant: Option<restricted::GrantGuard>,
    /// Validated native guardian context, constructed once at the seam for API-profile shells.
    pub(super) guardian: Option<NativeGuardian>,
}

/// The tmux settings of the restricted terminals next to `tmux` (the administrators'): their own
/// socket ([`restricted_socket`]) and Meerschaum's [`TMUX_RESTRICTED_CONF`] (never the
/// administrators' `api:webterm:tmux:config`). `None` without a dedicated socket: the default
/// tmux server is never used, so such a terminal runs without tmux.
pub(super) fn restricted_tmux(tmux: &TmuxSettings) -> Option<TmuxSettings> {
    let socket = tmux.socket.as_ref()?;
    Some(TmuxSettings {
        binary: tmux.binary.clone(),
        socket: Some(restricted_socket(socket)),
        session_suffix: tmux.session_suffix.clone(),
        env_dir: tmux.env_dir.clone(),
        config_file: Some(
            tmux.env_dir
                .join("tmux-restricted.conf")
                .display()
                .to_string(),
        ),
    })
}

/// `shell` in restricted mode: it asks `address` with `grant` before running anything.
pub(super) fn restricted_shell(shell: &ShellCommand, address: &str, grant: &str) -> ShellCommand {
    let mut shell = shell.clone();
    shell.env.retain(|(key, _)| {
        ![RESTRICTED_ENV, POLICY_ADDRESS_ENV, GRANT_ENV].contains(&key.as_str())
    });
    shell.env.push((RESTRICTED_ENV.into(), "1".into()));
    shell.env.push((POLICY_ADDRESS_ENV.into(), address.into()));
    shell.env.push((GRANT_ENV.into(), grant.into()));
    // The shell may be `python -m meerschaum`: never an interactive interpreter after it exits,
    // a startup script or a debugger.
    for (var, value) in RESTRICTED_PYTHON_VARS {
        shell.env.retain(|(key, _)| key != var);
        shell.env.push((var.to_string(), value.to_string()));
    }
    shell
}

/// What a restricted shell's Python variables are, whatever the server's environment says: they
/// would make a Python interpreter (the shell, when it runs as `python -m meerschaum`) stay
/// interactive, run a script of its own or open a debugger (an empty value is unset for Python;
/// `PYTHONBREAKPOINT=0` turns `breakpoint()` off).
pub(super) const RESTRICTED_PYTHON_VARS: &[(&str, &str)] = &[
    ("PYTHONINSPECT", ""),
    ("PYTHONSTARTUP", ""),
    ("PYTHONBREAKPOINT", "0"),
];

/// Retire all panes of the exact Factory lease on its Team, API-only or legacy socket.
/// Each socket belongs to this executor configuration. A dedicated base is required.
pub fn end_executor_terminal(base: &TmuxSettings, team_id: uuid::Uuid, name: &str) {
    let session = tmux_session_name(name, &base.session_suffix);
    for profile in [
        TmuxProfile::Team(team_id.to_string()),
        TmuxProfile::Restricted,
        TmuxProfile::Full,
    ] {
        if let Some(mux) = TmuxMux::from_base(base, profile) {
            mux.end_session(&session);
        }
    }
}

/// End the tmux `session` (exact name) on `tmux`'s socket, with every window and process in it,
/// for a caller that holds only the socket's settings (an administrators' server's).
/// Nothing happens without a dedicated socket, a running server or such a session.
pub fn end_tmux_session(tmux: &TmuxSettings, session: &str) {
    TmuxMux::new(tmux.clone(), TmuxProfile::Full).end_session(session);
}

/// Write [`TMUX_RESTRICTED_CONF`] (`0600`) where `tmux` (restricted settings) reads it.
pub(super) fn write_restricted_conf(tmux: &TmuxSettings) -> std::io::Result<()> {
    let Some(file) = &tmux.config_file else {
        return Err(std::io::Error::other(
            "no restricted tmux configuration path",
        ));
    };
    let path = Path::new(file);
    if std::fs::read_to_string(path).ok().as_deref() == Some(TMUX_RESTRICTED_CONF) {
        return Ok(());
    }
    create_private_dir(&tmux.env_dir)?;
    let temporary = tmux
        .env_dir
        .join(format!("tmux-restricted.conf.{}.tmp", std::process::id()));
    write_private_file(&temporary, TMUX_RESTRICTED_CONF.as_bytes())?;
    std::fs::rename(&temporary, path)
}

impl TmuxMux {
    /// Lock down this mux's locked-down tmux server (`Restricted` or `Team`): apply
    /// [`TMUX_RESTRICTED_CONF`] (`source-file`), empty every key table it does not keep
    /// ([`restricted::KEPT_TABLES`]: tmux has more tables than the ones the file names, and new
    /// releases add some), then check that tmux lists exactly the file's bindings and no prefix
    /// key. `true` only when all of that holds.
    pub(super) fn apply_restricted_conf(&self) -> bool {
        let Some(file) = &self.settings.config_file else {
            return false;
        };
        let run = |args: &[&str]| -> Option<std::process::Output> {
            let mut command = self.command(None)?;
            command.args(args);
            tmux_output(&mut command).filter(|output| output.status.success())
        };
        if run(&["source-file", file]).is_none() {
            return false;
        }
        let Some(listing) = run(&["list-keys"]) else {
            return false;
        };
        let mut tables: Vec<String> = String::from_utf8_lossy(&listing.stdout)
            .lines()
            .filter_map(|line| restricted::binding(line).map(|(table, _, _)| table))
            .filter(|table| !restricted::KEPT_TABLES.contains(&table.as_str()))
            .collect();
        tables.sort();
        tables.dedup();
        for table in &tables {
            if run(&["unbind-key", "-a", "-T", table]).is_none() {
                return false;
            }
        }
        let Some(listing) = run(&["list-keys"]) else {
            return false;
        };
        if let Err(error) =
            restricted::only_allowed_bindings(&String::from_utf8_lossy(&listing.stdout))
        {
            tracing::warn!("A restricted terminal's tmux server is not locked down: {error}");
            return false;
        }
        ["prefix", "prefix2"].iter().all(|option| {
            run(&["show-options", "-gv", option])
                .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "None")
        })
    }
}

/// Start a terminal's process (`Full` or `Restricted` mode), validating any API guardian once.
pub(super) fn launch(
    config: &WebtermConfig,
    term_name: &str,
    restricted: Option<&str>,
) -> std::io::Result<Launched> {
    let guardian = NativeGuardian::parse(&config.shell)?;
    launch_with_guardian(config, term_name, restricted, guardian)
}

/// Start a `Full` or `Restricted` terminal with a pre-validated [`NativeGuardian`].
pub(super) fn launch_with_guardian(
    config: &WebtermConfig,
    term_name: &str,
    restricted: Option<&str>,
    guardian: Option<NativeGuardian>,
) -> std::io::Result<Launched> {
    let cwd = config
        .shell
        .cwd
        .clone()
        .or_else(|| std::env::current_dir().ok());
    let mut builder;
    let mut session = None;
    let mut active_mux: Option<TmuxMux> = None;
    let mut grant = None;
    let mut shell = config.shell.clone();
    // 5.1: no tmux at all while isolated teams' sandbox does not refuse Unix sockets
    // (`WebtermConfig::active_tmux`), for restricted terminals too.
    match (config.active_tmux(), restricted) {
        (Some(tmux), None) => {
            let mux = TmuxMux::new(tmux.clone(), TmuxProfile::Full);
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // A user who may run system commands now has no use for a restricted session left
            // from before (a promotion): end it.
            if let Some(rmux) = TmuxMux::from_base(tmux, TmuxProfile::Restricted)
                && rmux.has_session(&name)
            {
                rmux.end_session(&name);
                config.policy.forget_session(&name);
            }
            let wrapped = mux.shell_argv(&config.shell)?;
            if mux.claim_server() {
                refresh_tmux_config(tmux);
            }
            mux.scrub_server_env(Some(&name));
            builder = CommandBuilder::new(&tmux.binary);
            if let Some(socket) = &tmux.socket {
                builder.arg("-S");
                builder.arg(socket);
            }
            // `-u`: the websocket is UTF-8 whatever the server's locale says.
            builder.arg("-u");
            if let Some(file) = &tmux.config_file {
                builder.args(["-f", file]);
            }
            // `-n`: the first window's tab label (ignored when the session exists already).
            builder.args(["new-session", "-A", "-s", &name, "-n", TMUX_WINDOW_NAME]);
            builder.args(&wrapped);
            session = Some(name);
            active_mux = Some(mux);
        }
        (Some(tmux), Some(address)) if restricted_tmux(tmux).is_some() => {
            let rmux = TmuxMux::from_base(tmux, TmuxProfile::Restricted).expect("checked");
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // One restricted launch at a time: two tabs opening at once must not both start the
            // session (the second one's grant would replace the first's).
            static RESTRICTED_LAUNCH: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let _launching = RESTRICTED_LAUNCH.lock().unwrap_or_else(|e| e.into_inner());
            // A user who may not run system commands never reattaches a session started when
            // they could (a demotion): end it, with whatever runs in it.
            TmuxMux::new(tmux.clone(), TmuxProfile::Full).end_session(&name);
            rmux.prepare_locked_server("restricted")?;
            rmux.scrub_server_env(Some(&name));
            // Reattach only a session whose shell this server started for this user (so its
            // grant is known); end any other.
            let token = match config.policy.session_grant(term_name, &name) {
                Some(token) if rmux.has_session(&name) => {
                    // This fresh prepared shell has not been handed to argv construction or
                    // any launcher. Reattachment keeps the existing pane's claimed proof.
                    if let Some(ref guardian) = guardian {
                        guardian.cancel_unspawned()?;
                    }
                    token
                }
                _ => {
                    rmux.end_session(&name);
                    let grant = config.policy.grant_for_session(term_name, &name);
                    let rshell = restricted_shell(&config.shell, address, &grant);
                    if let Err(error) =
                        rmux.start_detached_session(&name, &rshell, cwd.clone(), "restricted")
                    {
                        config.policy.forget_session(&name);
                        return Err(error);
                    }
                    grant
                }
            };
            // tmux applies all of it, or the terminal does not open.
            if !rmux.apply_restricted_conf() {
                rmux.end_session(&name);
                config.policy.forget_session(&name);
                return Err(std::io::Error::other(
                    "the restricted tmux configuration could not be applied",
                ));
            }
            shell = restricted_shell(&config.shell, address, &token);
            builder = rmux.attach_builder(&name);
            session = Some(name);
            active_mux = Some(rmux);
        }
        (_, Some(address)) => {
            // One restricted shell per connection, with a grant that ends with it.
            let guard = config.policy.grant_for_connection(term_name);
            shell = restricted_shell(&config.shell, address, guard.token());
            grant = Some(guard);
            builder = CommandBuilder::new(&shell.program);
            builder.args(&shell.args);
        }
        (None, None) => {
            builder = CommandBuilder::new(&config.shell.program);
            builder.args(&config.shell.args);
        }
    }
    if shell.clear_env {
        builder.env_clear();
    }
    if shell.clear_env && active_mux.is_some() {
        for (key, value) in team::minimal_env() {
            builder.env(key, value);
        }
    } else {
        for (key, value) in &shell.env {
            builder.env(key, value);
        }
    }
    // A caller's shell overrides must not reconnect a scratch or web terminal to the
    // enclosing user's tmux server. Strip these after applying the overrides too.
    builder.env_remove("TMUX");
    builder.env_remove("TMUX_PANE");
    // Nor run the shell as the job the server runs in (the tmux client's environment also becomes
    // a new tmux server's global environment).
    scrub_job_context(&mut builder);
    builder.env("TERM", "xterm-256color");
    builder.env("COLORTERM", "truecolor");
    // PARITY: v4's shell command pops `COLUMNS` and `LINES` so the PTY's size wins.
    builder.env_remove("COLUMNS");
    builder.env_remove("LINES");
    if let Some(cwd) = cwd {
        builder.cwd(cwd);
    }
    let state_dir = if active_mux.is_none() {
        config.shell_state_directory().map(PathBuf::from)
    } else {
        // The PTY owns a tmux client; the session's shell survives that client's exit.
        None
    };
    // A user who may now run commands this way never reattaches a team terminal of before.
    if let Some(name) = &session {
        end_team_sessions_of(name);
    }
    Ok(Launched {
        pty: spawn_pty(builder, state_dir)?,
        session,
        mux: active_mux,
        shell,
        grant,
        guardian,
    })
}

/// Start a team terminal for `term_name` of `team` running `shell` (complete: it starts from an
/// empty environment): on the team's tmux server when the terminal uses tmux, else the shell
/// itself on the PTY.
pub(super) fn launch_team(
    config: &WebtermConfig,
    term_name: &str,
    team_name: &str,
    shell: &ShellCommand,
    check_membership: bool,
) -> std::io::Result<Launched> {
    let guardian = NativeGuardian::parse(shell)?;
    let mut shell = shell.clone();
    for (name, value) in [("TERM", "xterm-256color"), ("COLORTERM", "truecolor")] {
        shell.env.retain(|(k, _)| k != name);
        shell.env.push((name.into(), value.into()));
    }
    // The public rule applies here too (`WebtermConfig::active_tmux`): no tmux while isolated
    // teams' sandbox does not refuse Unix sockets. A team terminal exists only where it does.
    let Some(base) = config.active_tmux() else {
        let mut builder = CommandBuilder::new(&shell.program);
        builder.args(&shell.args);
        builder.env_clear();
        for (key, value) in &shell.env {
            builder.env(key, value);
        }
        if let Some(cwd) = &shell.cwd {
            builder.cwd(cwd);
        }
        return Ok(Launched {
            pty: spawn_pty(builder, None)?,
            session: None,
            mux: None,
            shell,
            grant: None,
            guardian,
        });
    };
    let mux = TmuxMux::from_base(base, TmuxProfile::Team(team_name.to_string()))
        .ok_or_else(|| std::io::Error::other("no tmux socket for the team's terminals"))?;
    let socket = mux.settings.socket.clone().expect("a team socket");
    let name = tmux_session_name(term_name, &base.session_suffix);
    static TEAM_LAUNCH: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _launching = TEAM_LAUNCH.lock().unwrap_or_else(|e| e.into_inner());
    // Not a session of another mode, nor of another team's server.
    TmuxMux::new(base.clone(), TmuxProfile::Full).end_session(&name);
    if let Some(restricted) = TmuxMux::from_base(base, TmuxProfile::Restricted) {
        restricted.end_session(&name);
        config.policy.forget_session(&name);
    }
    team::end_team_sessions_except(&name, Some(&socket));
    mux.prepare_locked_server("team terminal's")?;
    if !mux.has_session(&name) {
        let cwd = Some(shell.cwd.clone().unwrap_or_else(|| PathBuf::from("/")));
        mux.start_detached_session(&name, &shell, cwd, "team terminal's")?;
    } else if let Some(ref guardian) = guardian {
        // No argv for this fresh shell has been constructed or handed to tmux. An API
        // guardian's unused nonce must not survive reattachment to its existing panes.
        guardian.cancel_unspawned()?;
    }
    // tmux applies all of the lockdown, or the terminal does not open.
    if !mux.apply_restricted_conf() {
        mux.end_session(&name);
        return Err(std::io::Error::other(
            "the team terminal's tmux configuration could not be applied",
        ));
    }
    if check_membership {
        team::record_team_session(&socket, team_name, &mux.settings, &name, term_name);
    }
    let mut builder = mux.attach_builder(&name);
    builder.env_clear();
    for (key, value) in team::minimal_env() {
        builder.env(key, value);
    }
    builder.env("TERM", "xterm-256color");
    Ok(Launched {
        pty: spawn_pty(builder, None)?,
        session: Some(name),
        mux: Some(mux),
        shell,
        grant: None,
        guardian,
    })
}

/// The environment variables the tmux path does not carry into inherited-env shells.
pub(super) const ENV_SKIP: &[&str] = &[
    "TMUX",
    "TMUX_PANE",
    "TERM",
    "COLUMNS",
    "LINES",
    "SHLVL",
    "_",
    "PWD",
    "OLDPWD",
];

/// Variables omitted from the env files of windows that start under `env -i` (Team, and shells
/// with `clear_env`): `TERM` stays, because `env -i` clears tmux's.
const ENV_SKIP_ISOLATED: &[&str] = &[
    "TMUX",
    "TMUX_PANE",
    "COLUMNS",
    "LINES",
    "SHLVL",
    "_",
    "PWD",
    "OLDPWD",
];

/// `env`, by its absolute path (isolated windows start from an empty environment, without `PATH`).
pub(super) fn env_program() -> &'static str {
    ["/usr/bin/env", "/bin/env"]
        .into_iter()
        .find(|p| Path::new(p).is_file())
        .unwrap_or("/usr/bin/env")
}

/// Single-quote a value for `sh`.
pub(super) fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(super) fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// How long an environment file may wait for its window before it counts as abandoned (tmux
/// never started the window).
pub(super) const STALE_ENV_FILE: Duration = Duration::from_secs(120);

/// Remove the environment files no window read: older than [`STALE_ENV_FILE`], and the shared
/// `webterm.env` of earlier releases.
pub(super) fn remove_stale_env_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("webterm.") || !(name.ends_with(".env") || name.ends_with(".tmp")) {
            continue;
        }
        let stale = name == "webterm.env"
            || entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > STALE_ENV_FILE);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

pub(super) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(super) fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)
}
