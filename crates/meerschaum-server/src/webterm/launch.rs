//! Terminal session launch, restricted tmux configuration, and environment files.

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

/// A started terminal.
pub(super) struct Launched {
    pub(super) pty: Pty,
    /// The tmux session it attached to (the tmux path).
    pub(super) session: Option<String>,
    /// The tmux server that session lives on (the restricted one for a restricted terminal).
    pub(super) tmux: Option<TmuxSettings>,
    /// The shell a new window runs (a restricted terminal's carries its grant).
    pub(super) shell: ShellCommand,
    /// A restricted terminal's own grant on the plain path (one shell per connection).
    pub(super) grant: Option<restricted::GrantGuard>,
    /// A team terminal (5.1): its windows run the team's shell from an empty environment.
    pub(super) team_window: bool,
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

/// A tmux command on `tmux`'s own socket (never the default server), without the caller's tmux
/// variables or the server's job context.
pub(super) fn tmux_command(tmux: &TmuxSettings) -> Option<std::process::Command> {
    let socket = tmux.socket.as_ref()?;
    let mut command = std::process::Command::new(&tmux.binary);
    command
        .arg("-S")
        .arg(socket)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE");
    for var in job_context_vars() {
        command.env_remove(var);
    }
    Some(command)
}

/// Whether the tmux `session` (exact name) exists on `tmux`'s socket.
pub(super) fn tmux_has_session(tmux: &TmuxSettings, session: &str) -> bool {
    let Some(mut command) = tmux_command(tmux) else {
        return false;
    };
    command.args(["has-session", "-t", &format!("={session}")]);
    tmux_output(&mut command).is_some_and(|output| output.status.success())
}

/// Retire all panes of the exact Factory lease on its Team, API-only or legacy socket.
/// Each socket belongs to this executor configuration. A dedicated base is required.
pub fn end_executor_terminal(base: &TmuxSettings, team_id: uuid::Uuid, name: &str) {
    let session = tmux_session_name(name, &base.session_suffix);
    for tmux in [
        team::team_tmux(base, &team_id.to_string()),
        restricted_tmux(base),
    ]
    .into_iter()
    .flatten()
    {
        end_tmux_session(&tmux, &session);
    }
    end_tmux_session(base, &session);
}

/// End the tmux `session` (exact name) on `tmux`'s socket, with every window and process in it.
/// Nothing happens without a dedicated socket, a running server or such a session.
pub fn end_tmux_session(tmux: &TmuxSettings, session: &str) {
    if let Some(mut command) = tmux_command(tmux) {
        command.args(["kill-session", "-t", &format!("={session}")]);
        let _ = tmux_output(&mut command);
    }
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

/// Lock down the restricted tmux server that runs on `tmux`'s socket: apply
/// [`TMUX_RESTRICTED_CONF`] (`source-file`), empty every key table it does not keep
/// ([`restricted::KEPT_TABLES`]: tmux has more tables than the ones the file names, and new
/// releases add some), then check that tmux lists exactly the file's bindings and no prefix
/// key. `true` only when all of that holds.
pub(super) fn apply_restricted_conf(tmux: &TmuxSettings) -> bool {
    let Some(file) = &tmux.config_file else {
        return false;
    };
    let run = |args: &[&str]| -> Option<std::process::Output> {
        let mut command = tmux_command(tmux)?;
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
    if let Err(error) = restricted::only_allowed_bindings(&String::from_utf8_lossy(&listing.stdout))
    {
        tracing::warn!("A restricted terminal's tmux server is not locked down: {error}");
        return false;
    }
    ["prefix", "prefix2"].iter().all(|option| {
        run(&["show-options", "-gv", option])
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "None")
    })
}

/// Start a restricted terminal's tmux session `name` for `term_name` on `tmux` (restricted
/// A complete shell environment is delivered in its private pane file. The tmux client
/// and server run as the terminal host and resolve their tools with the host environment.
pub(super) fn configure_tmux_environment(
    command: &mut std::process::Command,
    shell: &ShellCommand,
) {
    if shell.clear_env {
        command.env_clear().envs(team::minimal_env());
    } else {
        command.envs(shell.env.iter().map(|(key, value)| (key, value)));
    }
}

/// settings), detached; the terminal attaches to it afterwards. Its grant.
pub(super) fn new_restricted_session(
    config: &WebtermConfig,
    tmux: &TmuxSettings,
    term_name: &str,
    name: &str,
    address: &str,
) -> std::io::Result<String> {
    let grant = config.policy.grant_for_session(term_name, name);
    let shell = restricted_shell(&config.shell, address, &grant);
    let argv = tmux_shell_argv(tmux, &shell)?;
    let mut command =
        tmux_command(tmux).ok_or_else(|| std::io::Error::other("no restricted tmux socket"))?;
    configure_tmux_environment(&mut command, &shell);
    command.arg("-u");
    if let Some(file) = &tmux.config_file {
        command.args(["-f", file]);
    }
    command
        .args(["new-session", "-d", "-s", name, "-n", TMUX_WINDOW_NAME])
        .args(&argv)
        .env("TERM", "xterm-256color")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE");
    if let Some(cwd) = shell.cwd.clone().or_else(|| std::env::current_dir().ok()) {
        command.current_dir(cwd);
    }
    for var in job_context_vars() {
        command.env_remove(var);
    }
    if tmux_output(&mut command).is_some_and(|output| output.status.success()) {
        Ok(grant)
    } else {
        config.policy.forget_session(name);
        Err(std::io::Error::other(
            "tmux could not start the restricted session",
        ))
    }
}

/// Start a terminal's process: the shell itself, or tmux around it (`new-session -A`, or for a
/// restricted terminal a session on the restricted tmux server, then `attach-session`).
/// `restricted` is the policy socket's address when the terminal is restricted.
pub(super) fn launch(
    config: &WebtermConfig,
    term_name: &str,
    restricted: Option<&str>,
) -> std::io::Result<Launched> {
    let cwd = match &config.shell.cwd {
        Some(dir) => Some(dir.clone()),
        None => std::env::current_dir().ok(),
    };
    let mut builder;
    let mut session = None;
    let mut active_tmux = None;
    let mut grant = None;
    let mut shell = config.shell.clone();
    // 5.1: no tmux at all while isolated teams' sandbox does not refuse Unix sockets
    // (`WebtermConfig::active_tmux`), for restricted terminals too.
    match (config.active_tmux(), restricted) {
        (Some(tmux), None) => {
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // A user who may run system commands now has no use for a restricted session left
            // from before (a promotion): end it.
            if let Some(restricted_tmux) = restricted_tmux(tmux)
                && tmux_has_session(&restricted_tmux, &name)
            {
                end_tmux_session(&restricted_tmux, &name);
                config.policy.forget_session(&name);
            }
            let wrapped = tmux_shell_argv(tmux, &config.shell)?;
            if claim_tmux_server(tmux) {
                refresh_tmux_config(tmux);
            }
            scrub_tmux_server_env(tmux, Some(&name));
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
            active_tmux = Some(tmux.clone());
        }
        (Some(tmux), Some(address)) if restricted_tmux(tmux).is_some() => {
            let rtmux = restricted_tmux(tmux).expect("checked");
            let name = tmux_session_name(term_name, &tmux.session_suffix);
            // One restricted launch at a time: two tabs opening at once must not both start the
            // session (the second one's grant would replace the first's).
            static RESTRICTED_LAUNCH: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let _launching = RESTRICTED_LAUNCH.lock().unwrap_or_else(|e| e.into_inner());
            // A user who may not run system commands never reattaches a session started when
            // they could (a demotion): end it, with whatever runs in it.
            end_tmux_session(tmux, &name);
            write_restricted_conf(&rtmux)?;
            claim_tmux_server(&rtmux);
            // The server already runs: lock it down before anything else happens on it.
            if tmux_server_running(&rtmux, rtmux.socket.as_deref().expect("socket"))
                && !apply_restricted_conf(&rtmux)
            {
                return Err(std::io::Error::other(
                    "the restricted tmux configuration could not be applied",
                ));
            }
            scrub_tmux_server_env(&rtmux, Some(&name));
            // Reattach only a session whose shell this server started for this user (so its
            // grant is known); end any other.
            let token = match config.policy.session_grant(term_name, &name) {
                Some(token) if tmux_has_session(&rtmux, &name) => {
                    // This fresh prepared shell has not been handed to argv construction or
                    // any launcher. Reattachment keeps the existing pane's claimed proof.
                    native_pending::cancel_unspawned(&config.shell)?;
                    token
                }
                _ => {
                    end_tmux_session(&rtmux, &name);
                    new_restricted_session(config, &rtmux, term_name, &name, address)?
                }
            };
            // tmux applies all of it, or the terminal does not open.
            if !apply_restricted_conf(&rtmux) {
                end_tmux_session(&rtmux, &name);
                config.policy.forget_session(&name);
                return Err(std::io::Error::other(
                    "the restricted tmux configuration could not be applied",
                ));
            }
            shell = restricted_shell(&config.shell, address, &token);
            builder = CommandBuilder::new(&rtmux.binary);
            builder.arg("-S");
            builder.arg(rtmux.socket.as_ref().expect("socket"));
            builder.arg("-u");
            builder.args(["attach-session", "-t", &format!("={name}")]);
            session = Some(name);
            active_tmux = Some(rtmux);
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
    if shell.clear_env && active_tmux.is_some() {
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
    let state_dir = if active_tmux.is_none() {
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
        tmux: active_tmux,
        shell,
        grant,
        team_window: false,
    })
}

/// The environment variables the tmux path does not carry into the shell.
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

/// Write the shell's environment for the tmux path (`0600`, replaced atomically) and return its
/// path: the server's environment plus [`ShellCommand::env`].
pub(super) fn write_env_file(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
) -> std::io::Result<PathBuf> {
    let mut vars: std::collections::BTreeMap<String, String> = if shell.clear_env {
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
        if ENV_SKIP.contains(&key.as_str()) || !valid_env_name(key) || context.contains(key) {
            continue;
        }
        body.push_str(&format!("{key}={}\n", sh_quote(value)));
    }
    create_private_dir(&tmux.env_dir)?;
    remove_stale_env_files(&tmux.env_dir);
    // One file per launch, which the window's `sh` removes once it has read it
    // ([`tmux_shell_argv`]): two terminals starting at once never read each other's (a
    // restricted terminal's carries its own grant), and none stays behind.
    static LAUNCHES: AtomicUsize = AtomicUsize::new(0);
    let stem = format!(
        "webterm.{}.{}",
        std::process::id(),
        LAUNCHES.fetch_add(1, Ordering::Relaxed)
    );
    let path = tmux.env_dir.join(format!("{stem}.env"));
    let temporary = tmux.env_dir.join(format!("{stem}.env.tmp"));
    write_private_file(&temporary, body.as_bytes())?;
    std::fs::rename(&temporary, &path)?;
    Ok(path)
}

/// How long an environment file may wait for its window before it counts as abandoned (tmux
/// never started the window).
pub(super) const STALE_ENV_FILE: Duration = Duration::from_secs(120);

/// Remove the environment files ([`write_env_file`]) no window read: older than
/// [`STALE_ENV_FILE`], and the shared `webterm.env` of earlier releases.
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

/// The argv tmux runs in a window: `sh` sources the environment file and removes it, unsets the
/// server's job context (a tmux server started before, or from a job's environment, may hold it
/// in its global environment), then replaces itself with the shell. A file that cannot be read
/// ends `sh` there (`.` is a special built-in): the window closes, nothing else runs.
pub(super) fn tmux_shell_argv(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
) -> std::io::Result<Vec<String>> {
    let env_file = write_env_file(tmux, shell)?;
    // The names are `valid_env_name`s: safe to write into the script as they are.
    let unset = job_context_vars().join(" ");
    let mut argv: Vec<String> = if shell.clear_env {
        vec!["/usr/bin/env".into(), "-i".into()]
    } else {
        Vec::new()
    };
    argv.extend([
        "sh".to_string(),
        "-c".into(),
        format!("set -a; . \"$0\"; set +a; command -p rm -f -- \"$0\"; unset {unset}; exec \"$@\""),
        env_file.display().to_string(),
        shell.program.clone(),
    ]);
    argv.extend(shell.args.iter().cloned());
    Ok(argv)
}

/// How many windows the tmux `session` (exact name) has; `None` when tmux cannot say.
pub(super) fn tmux_window_count(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
    session: &str,
) -> Option<usize> {
    let mut command = std::process::Command::new(&tmux.binary);
    configure_tmux_environment(&mut command, shell);
    if let Some(socket) = &tmux.socket {
        command.arg("-S").arg(socket);
    }
    let output = tmux_output(
        command
            .args([
                "list-windows",
                "-t",
                &format!("={session}"),
                "-F",
                "#{window_id}",
            ])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE"),
    )?;
    output.status.success().then(|| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    })
}

/// Open a window running the shell in the user's tmux `session` (exact name), unless it already
/// has [`MAX_SESSION_WINDOWS`] (or its windows cannot be counted): `Ok(false)` then.
pub(super) fn tmux_new_window(
    tmux: &TmuxSettings,
    shell: &ShellCommand,
    session: &str,
) -> std::io::Result<bool> {
    // One at a time, so two tabs of a session cannot both pass the count.
    static OPENING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _opening = OPENING.lock().unwrap_or_else(|e| e.into_inner());
    if tmux_window_count(tmux, shell, session).is_none_or(|n| n >= MAX_SESSION_WINDOWS) {
        return Ok(false);
    }
    let shell = native_pending::window_shell(shell)?;
    let argv = tmux_shell_argv(tmux, &shell)?;
    scrub_tmux_server_env(tmux, Some(session));
    let mut command = std::process::Command::new(&tmux.binary);
    configure_tmux_environment(&mut command, &shell);
    if let Some(socket) = &tmux.socket {
        command.arg("-S").arg(socket);
    }
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
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for var in job_context_vars() {
        command.env_remove(var);
    }
    Ok(tmux_output(&mut command).is_some_and(|output| output.status.success()))
}
