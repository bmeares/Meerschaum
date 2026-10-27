//! Port of `meerschaum.jobs.systemd.SystemdExecutor`: user units with the same text as Python,
//! driven through `systemctl --user`.
//!
//! Every `systemctl` call goes through the context's [`crate::runner::ProcessRunner`], so the
//! executor is testable with [`crate::runner::FakeSystemctl`] and never touches the host's
//! services.
//!
//! **Units per root.** Every root may use `systemd` (v4 and 5.0 before rc15: only the default
//! root). The default root keeps v4's unit names (`mrsm-<job>.service`); any other root names its
//! units `mrsm-<scope>.<job>.service`, where the scope is the Compose project's name for a job
//! Compose started, else a short hash of the root's path. A root lists only its own units: the ones
//! whose files live under its `systemd/services/` directory, so the default root never sees
//! another root's jobs. Units of other roots export `MRSM_ROOT_DIR` so they come back after a
//! reboot, and every unit file is private (`0600`: it may carry the project's `MRSM_CONFIG`). Status, PID and timestamps come from `systemctl is-active` / `show`; the job's
//! properties live in the hidden "sister" local job (`jobs/<name>/`, externally managed); logs
//! are the service's rotating log file under `<root>/systemd/logs/`.
//!
//! The job-side runtime (what runs inside the unit: the rotating log, the result file, the stdin
//! FIFO, delete-after-completion) is [`crate::systemd_job`].

use std::path::PathBuf;
use std::time::{Duration, Instant};

use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::config::Seconds;
use crate::daemon::{JobsContext, Status};
use crate::executor::JobExecutor;
use crate::job::{
    Job, JobOptions, MonitorControl, MonitorOptions, MonitorSource, RESTART_FLAGS,
    monitor_log_stream,
};
use crate::process::ProcState;
use crate::rotating::{RotatingFile, RotatingFileOptions};
use crate::stdin::StdinFile;

/// The `systemd` executor.
#[derive(Debug, Clone)]
pub struct SystemdExecutor {
    ctx: JobsContext,
    process_env: Option<Vec<(String, String)>>,
    /// Warnings of the operations so far, for the caller to show (see
    /// [`JobExecutor::take_warnings`]).
    warnings: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// The unit names chosen for jobs being created, before their unit files exist.
    creating: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
}

impl SystemdExecutor {
    /// A systemd executor over a context.
    pub fn new(ctx: JobsContext) -> SystemdExecutor {
        SystemdExecutor {
            ctx,
            process_env: None,
            warnings: Default::default(),
            creating: Default::default(),
        }
    }

    /// Use this environment (instead of the process's) for the `Environment=` lines of the units
    /// `create_job` writes (`os.environ`); tests pin it.
    pub fn with_process_env(mut self, env: Vec<(String, String)>) -> SystemdExecutor {
        self.process_env = Some(env);
        self
    }

    /// `get_service_name(name)`: the name of the job's unit. An existing job keeps the name its
    /// unit file has; a new one gets [`SystemdExecutor::new_service_name`] (`mrsm-<name with
    /// spaces as dashes>.service` in the default root, as v4 names it).
    pub fn get_service_name(&self, name: &str) -> String {
        if let Some(service) = self.creating.lock().ok().and_then(|c| c.get(name).cloned()) {
            return service;
        }
        self.existing_service_name(name)
            .unwrap_or_else(|| self.new_service_name(name, None))
    }

    /// The unit file name in the job's directory under `systemd/services/`, if it has one.
    pub fn existing_service_name(&self, name: &str) -> Option<String> {
        if crate::names::invalid_job_name_reason(name).is_some() {
            return None;
        }
        let mut found: Vec<String> = std::fs::read_dir(self.get_service_job_path(name))
            .ok()?
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("mrsm-") && n.ends_with(".service"))
            .collect();
        found.sort();
        let preferred = self.new_service_name(name, None);
        if found.contains(&preferred) {
            return Some(preferred);
        }
        found.into_iter().next()
    }

    /// Whether this executor's root is the default root (`<config dir>/meerschaum`).
    pub fn is_default_root(&self) -> bool {
        crate::job::default_root_dir(&self.ctx).is_some_and(|d| same_path(&d, &self.ctx.paths.root))
    }

    /// The scope of this root's unit names: `None` for the default root (v4's names), else the
    /// Compose `project` that starts the job, or [`root_scope`] of the root.
    pub fn unit_scope(&self, project: Option<&str>) -> Option<String> {
        if self.is_default_root() {
            return None;
        }
        Some(
            project
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| root_scope(&self.ctx.paths.root)),
        )
    }

    /// The unit name a new job gets: `mrsm-<job>.service` in the default root,
    /// `mrsm-<scope>.<job>.service` elsewhere (see [`SystemdExecutor::unit_scope`]).
    pub fn new_service_name(&self, name: &str, project: Option<&str>) -> String {
        match self.unit_scope(project) {
            None => format!("mrsm-{}.service", name.replace(' ', "-")),
            Some(scope) => scoped_service_name(&scope, name),
        }
    }

    /// [`SystemdExecutor::new_service_name`], unless another root already installed a unit by
    /// that name (two copies of one Compose project): then the scope also carries this root's
    /// hash.
    fn unique_service_name(&self, name: &str, project: Option<&str>) -> String {
        let candidate = self.new_service_name(name, project);
        let link = self.ctx.paths.systemd_user_dir.join(&candidate);
        if link.symlink_metadata().is_err() || self.link_is_ours(name, &candidate) {
            return candidate;
        }
        match self.unit_scope(project) {
            None => candidate,
            Some(scope) => scoped_service_name(
                &format!("{scope}-{}", root_scope(&self.ctx.paths.root)),
                name,
            ),
        }
    }

    /// Whether the user unit `service` is this root's unit for the job `name`.
    fn link_is_ours(&self, name: &str, service: &str) -> bool {
        let link = self.ctx.paths.systemd_user_dir.join(service);
        let ours = self.get_service_job_path(name).join(service);
        match std::fs::read_link(&link) {
            Ok(target) => {
                let target = if target.is_absolute() {
                    target
                } else {
                    link.parent().map(|p| p.join(&target)).unwrap_or(target)
                };
                same_path(&target, &ours)
            }
            // Not a link (a unit file copied into place): it is ours if the job's file exists.
            Err(_) => link.is_file() && ours.is_file(),
        }
    }

    /// Whether the job `name` of this root has an installed unit: a unit file in its directory
    /// and the user unit linking to it.
    fn unit_is_installed(&self, name: &str) -> bool {
        let Some(service) = self.existing_service_name(name) else {
            return false;
        };
        self.ctx.paths.systemd_user_dir.join(&service).exists() && self.link_is_ours(name, &service)
    }

    /// `get_service_job_path(name)`: `{root}/systemd/services/<name>`.
    pub fn get_service_job_path(&self, name: &str) -> PathBuf {
        self.ctx.paths.systemd_jobs_dir.join(name)
    }

    /// `get_service_symlink_file_path(name)`: `~/.config/systemd/user/<service>`.
    pub fn get_service_symlink_file_path(&self, name: &str) -> PathBuf {
        self.ctx
            .paths
            .systemd_user_dir
            .join(self.get_service_name(name))
    }

    /// `get_service_file_path(name)`.
    pub fn get_service_file_path(&self, name: &str) -> PathBuf {
        self.get_service_job_path(name)
            .join(self.get_service_name(name))
    }

    /// `get_service_logs_path(name)`: `{root}/systemd/logs/<service>.log`.
    pub fn get_service_logs_path(&self, name: &str) -> PathBuf {
        self.ctx
            .paths
            .systemd_logs_dir
            .join(format!("{}.log", self.get_service_name(name)))
    }

    /// `get_socket_path(name)`: the stdin FIFO.
    pub fn get_socket_path(&self, name: &str) -> PathBuf {
        self.get_service_job_path(name)
            .join(format!("{}.stdin", self.get_service_name(name)))
    }

    /// `get_result_path(name)`.
    pub fn get_result_path(&self, name: &str) -> PathBuf {
        self.get_service_job_path(name)
            .join(format!("{}.result.json", self.get_service_name(name)))
    }

    /// The hidden "sister" local job storing the job's properties.
    pub fn get_hidden_job(
        &self,
        name: &str,
        sysargs: Option<Vec<String>>,
        properties: Option<Map>,
    ) -> Option<Job> {
        Job::new(
            name,
            sysargs,
            JobOptions {
                executor_keys: Some("local".into()),
                properties,
                externally_managed: true,
                ..Default::default()
            },
            &self.ctx,
        )
        .ok()
    }

    /// `get_service_file_text(name, sysargs)`.
    ///
    /// `python` is the interpreter for `ExecStart=` (Python uses `sys.executable`) and `process_env`
    /// the launching process's environment, in order (`os.environ`); only the variables named in
    /// `STATIC_CONFIG['environment']` are copied.
    pub fn get_service_file_text(
        &self,
        name: &str,
        sysargs: &[String],
        python: &str,
        process_env: &[(String, String)],
    ) -> String {
        let sc = meerschaum_core::static_config::static_config();
        let env_names: Vec<String> = meerschaum_core::static_config::static_config()
            .get(&["environment"])
            .and_then(Value::as_map)
            .map(|m| m.values().map(Value::py_str).collect())
            .unwrap_or_default();
        let mut vars: indexmap::IndexMap<String, String> = process_env
            .iter()
            .filter(|(k, _)| env_names.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // The unit runs the `mrsm` binary, which starts its Python workers with `MRSM_PYTHON`
        // else the first `python3` on the *unit's* PATH: pin the interpreter this process
        // resolves so the job runs plugins under the same environment it was created in.
        // (v4 pins it as `ExecStart=<sys.executable>`.)
        // Inside the Python extension the unit runs `<python> -P -m meerschaum`, so its workers
        // use that interpreter unless `MRSM_PYTHON` says otherwise.
        // `python` is `<interpreter> -P` there (`-P`: the unit's working directory stays off
        // `sys.path`), so the interpreter is its first word.
        let interpreter = crate::shlex::split(python)
            .ok()
            .and_then(|words| words.into_iter().next())
            .unwrap_or_else(|| python.to_string());
        let own_python =
            meerschaum_core::self_exe::ends_with_module_args(&self.ctx.launcher.program)
                && std::path::Path::new(&interpreter).is_absolute()
                && !process_env
                    .iter()
                    .any(|(k, v)| k == PYTHON_ENV && !v.trim().is_empty());
        if own_python {
            vars.insert(PYTHON_ENV.to_string(), interpreter);
        } else if let Some(python) = resolve_python(process_env) {
            vars.insert(PYTHON_ENV.to_string(), python);
        }
        let mut job = self.get_hidden_job(name, None, None);
        let delete = job.as_mut().is_some_and(|j| j.delete_after_completion());
        let posix = |p: PathBuf| p.to_string_lossy().replace('\\', "/");
        vars.insert(sc.environment.daemon_id.clone(), name.to_string());
        vars.insert(
            sc.environment.systemd_log_path.clone(),
            posix(self.get_service_logs_path(name)),
        );
        vars.insert(
            sc.environment.systemd_result_path.clone(),
            posix(self.get_result_path(name)),
        );
        vars.insert(
            sc.environment.systemd_stdin_path.clone(),
            posix(self.get_socket_path(name)),
        );
        vars.insert(
            sc.environment.systemd_delete_job.clone(),
            if delete { "1" } else { "0" }.to_string(),
        );
        if let Some(job) = job.as_mut() {
            for (k, v) in job.env() {
                vars.insert(k, v);
            }
        }
        // A unit of another root must find its root after a reboot, whatever started it.
        if !self.is_default_root() {
            vars.entry(sc.environment.root.clone())
                .or_insert_with(|| posix(self.ctx.paths.root.clone()));
        }
        let environment = vars
            .iter()
            .map(|(k, v)| format!("Environment={k}={}", crate::shlex::quote(v)))
            .collect::<Vec<_>>()
            .join("\n");
        let service_name = self.get_service_name(name);
        format!(
            "[Unit]\nDescription=Run the job '{name}'\n\n[Service]\nExecStart={python} -m meerschaum {sysargs}\nKillSignal=SIGTERM\nRestart=always\nRestartSec=5\nRestartPreventExitStatus=0\nSyslogIdentifier={service_name}\n{environment}\n\n[Install]\nWantedBy=default.target\n",
            sysargs = crate::shlex::join(sysargs),
        )
    }

    /// `get_socket_file_text(name)`.
    pub fn get_socket_file_text(&self, name: &str) -> String {
        format!(
            "[Unit]\nBindsTo={}\n\n[Socket]\nListenFIFO={}\nFileDescriptorName=stdin\nRemoveOnStop=true\nSocketMode=0660\n",
            self.get_service_name(name),
            self.get_socket_path(name)
                .to_string_lossy()
                .replace('\\', "/"),
        )
    }

    /// `get_job_sysargs(name)`: parse the `ExecStart=` line of the service file.
    pub fn get_job_sysargs(&self, name: &str) -> Vec<String> {
        std::fs::read_to_string(self.get_service_file_path(name))
            .map(|t| parse_exec_start_sysargs(&t))
            .unwrap_or_default()
    }

    /// `get_job_restart(name)`.
    pub fn get_job_restart(&self, name: &str) -> bool {
        let sysargs = self.get_job_sysargs(name);
        RESTART_FLAGS.iter().any(|f| sysargs.iter().any(|a| a == f))
    }

    /// `get_job_rotating_file(name)`.
    pub fn get_job_rotating_file(&self, name: &str) -> Option<RotatingFile> {
        RotatingFile::new(
            self.get_service_logs_path(name),
            RotatingFileOptions {
                num_files_to_keep: self.ctx.config.num_files_to_keep,
                max_file_size: self.ctx.config.max_file_size,
                write_timestamps: false,
                timestamp_format: self.ctx.config.timestamp_format.clone(),
            },
        )
        .ok()
    }
}

/// The warning for a unit created without a pinned interpreter (see [`resolve_python`]).
pub fn unpinned_python_warning(name: &str) -> String {
    format!(
        "No Python interpreter is pinned in the unit of job '{name}': neither `{PYTHON_ENV}` nor `python3` resolves. \
         The job will use the first `python3` on the unit's PATH for plugins; set `{PYTHON_ENV}` and create the job again to pin one."
    )
}

/// The variable which selects the Python interpreter of the workers.
pub const PYTHON_ENV: &str = "MRSM_PYTHON";

/// The absolute path of the interpreter a process with this environment starts its Python
/// workers with: `MRSM_PYTHON` (resolved against `PATH` when it is a bare command), else
/// `python3` on `PATH`. `None` when nothing resolves (the unit then pins nothing).
pub fn resolve_python(process_env: &[(String, String)]) -> Option<String> {
    let get = |key: &str| {
        process_env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.trim().is_empty())
    };
    let wanted = get(PYTHON_ENV).unwrap_or(if cfg!(windows) { "python" } else { "python3" });
    let path = std::path::Path::new(wanted);
    let found = if path.components().count() > 1 || path.is_absolute() {
        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().ok()?.join(path)
        };
        abs.is_file().then_some(abs)
    } else {
        get("PATH").and_then(|p| {
            std::env::split_paths(p)
                .map(|d| d.join(wanted))
                .find(|c| c.is_file())
        })
    };
    found.map(|p| p.to_string_lossy().into_owned())
}

impl SystemdExecutor {
    /// The program `ExecStart=` runs before ` -m meerschaum <sysargs>`: the job launcher's
    /// program (the `mrsm` binary, or the interpreter when the program is
    /// `python -P -m meerschaum`, which is v4's `ExecStart=<sys.executable> -m meerschaum` plus `-P`).
    pub fn exec_program(&self) -> String {
        crate::shlex::join(&meerschaum_core::self_exe::without_module_args(
            &self.ctx.launcher.program,
        ))
    }

    /// `run_command(args)`: `systemctl --user <args>` as a `SuccessTuple`.
    pub fn run_command(&self, args: &[&str]) -> SuccessTuple {
        let argv = Self::argv(args);
        match self.ctx.process.run("systemctl", &argv) {
            Ok(out) if out.success() => SuccessTuple::ok("Success"),
            Ok(_) => SuccessTuple::fail(format!(
                "Failed to execute command `{}`.",
                crate::shlex::join(&Self::full_command(&argv))
            )),
            Err(e) => SuccessTuple::fail(format!(
                "Failed to execute command `{}`.\n{e}",
                crate::shlex::join(&Self::full_command(&argv))
            )),
        }
    }

    /// `run_command(args, as_output=True)`: the stripped standard output (empty when the command
    /// cannot run).
    pub fn run_command_output(&self, args: &[&str]) -> String {
        self.ctx
            .process
            .run("systemctl", &Self::argv(args))
            .map(|o| o.stdout.trim().to_string())
            .unwrap_or_default()
    }

    fn argv(args: &[&str]) -> Vec<String> {
        let mut argv = vec!["--user".to_string()];
        argv.extend(args.iter().map(|a| a.to_string()));
        argv
    }

    fn full_command(argv: &[String]) -> Vec<String> {
        let mut full = vec!["systemctl".to_string()];
        full.extend(argv.iter().cloned());
        full
    }

    /// `get_job_pid(name)`: `systemctl show --property=MainPID` (`None` for 0).
    pub fn get_job_pid(&self, name: &str) -> Option<i32> {
        let service = self.get_service_name(name);
        let output = self.run_command_output(&["show", &service, "--property=MainPID"]);
        let pid = output.strip_prefix("MainPID=")?;
        if pid == "0" {
            return None;
        }
        meerschaum_core::pyfmt::parse_py_int(pid).map(|p| p as i32)
    }

    fn show_timestamp(&self, name: &str, property: &str) -> Option<String> {
        let service = self.get_service_name(name);
        let output =
            self.run_command_output(&["show", &service, &format!("--property={property}")]);
        if !output.starts_with(property) {
            return None;
        }
        let text = output.rsplit('=').next().unwrap_or("");
        if text.is_empty() {
            return None;
        }
        parse_systemd_timestamp(text)
    }

    /// `get_job_result(name)`: the service's result file, else `(False, "No result available.")`.
    pub fn read_job_result(&self, name: &str) -> SuccessTuple {
        let path = self.get_result_path(name);
        if !path.exists() {
            return SuccessTuple::fail("No result available.");
        }
        let parsed = std::fs::read_to_string(&path)
            .ok()
            .and_then(|t| Value::from_json_str(&t).ok());
        match parsed
            .as_ref()
            .and_then(crate::job::success_tuple_from_value)
        {
            Some(t) => t,
            None => SuccessTuple::fail(format!("Could not read result for Job '{name}'.")),
        }
    }

    fn stdin_file(&self, name: &str) -> StdinFile {
        let socket = self.get_socket_path(name);
        let mut block = socket.clone().into_os_string();
        block.push(".block");
        StdinFile::new(socket, Some(PathBuf::from(block)))
            .with_refresh(self.ctx.config.cli_refresh_seconds.duration())
    }

    fn timeout(&self) -> (Seconds, Seconds) {
        (
            self.ctx.config.timeout_seconds,
            self.ctx.config.check_timeout_interval_seconds,
        )
    }
}

/// `dateutil.parser.parse(text).astimezone(timezone.utc).isoformat()` for systemd's
/// `Tue 2000-01-04 10:00:00 UTC` timestamps. `UTC` / `GMT` are UTC; any other zone name is read
/// in the local zone (dateutil leaves unknown abbreviations naive and `astimezone` assumes
/// local time for naive values).
pub fn parse_systemd_timestamp(text: &str) -> Option<String> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
    let mut parts: Vec<&str> = text.split_whitespace().collect();
    if parts.len() >= 3 && parts[0].chars().all(|c| c.is_ascii_alphabetic()) {
        parts.remove(0);
    }
    if parts.len() < 2 {
        return None;
    }
    let naive = NaiveDateTime::parse_from_str(
        &format!("{} {}", parts[0], parts[1]),
        "%Y-%m-%d %H:%M:%S%.f",
    )
    .ok()?;
    let zone = parts.get(2).copied().unwrap_or("");
    let utc: DateTime<Utc> = match zone {
        "UTC" | "GMT" | "Z" => Utc.from_utc_datetime(&naive),
        z if (z.starts_with('+') || z.starts_with('-')) && z.len() >= 5 => {
            let sign = if z.starts_with('-') { -1 } else { 1 };
            let digits: String = z[1..].chars().filter(char::is_ascii_digit).collect();
            let hours: i32 = digits.get(0..2)?.parse().ok()?;
            let minutes: i32 = digits.get(2..4)?.parse().ok()?;
            let offset = chrono::FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))?;
            offset
                .from_local_datetime(&naive)
                .single()?
                .with_timezone(&Utc)
        }
        _ => chrono::Local
            .from_local_datetime(&naive)
            .earliest()?
            .with_timezone(&Utc),
    };
    Some(crate::pytime::aware_isoformat(utc))
}

/// The systemd service's view for [`monitor_log_stream`] (v4 hands the hidden job a
/// `_rotating_log`, a `_status_hook` and a `_stdin_file`).
struct SystemdSource<'a> {
    executor: &'a SystemdExecutor,
    name: &'a str,
}

impl MonitorSource for SystemdSource<'_> {
    fn rotating_log(&mut self) -> Option<RotatingFile> {
        self.executor.get_job_rotating_file(self.name)
    }

    fn lines_to_show(&mut self) -> i64 {
        let configured = self.executor.ctx.config.lines_to_show;
        self.executor
            .get_job_properties(self.name)
            .get("logs")
            .and_then(|l| l.get("lines_to_show"))
            .and_then(Value::as_i64)
            .unwrap_or(configured)
    }

    fn timestamp_format(&self) -> String {
        self.executor.ctx.config.timestamp_format.clone()
    }

    fn refresh_seconds(&self) -> Seconds {
        self.executor.ctx.config.cli_refresh_seconds
    }

    fn status(&mut self) -> Status {
        match self.executor.get_job_status(self.name).as_str() {
            "running" => Status::Running,
            "paused" => Status::Paused,
            _ => Status::Stopped,
        }
    }

    fn result(&mut self) -> SuccessTuple {
        self.executor.read_job_result(self.name)
    }

    fn is_blocking_on_stdin(&mut self) -> bool {
        self.status() == Status::Running && self.executor.get_job_is_blocking_on_stdin(self.name)
    }

    fn write_stdin(&mut self, data: &str) -> std::io::Result<()> {
        self.executor.stdin_file(self.name).write(data.as_bytes())
    }
}

/// `line.split(' -m meerschaum ')[-1].split('<')[0]`, `shlex.split` of the `ExecStart=` line.
pub fn parse_exec_start_sysargs(unit_text: &str) -> Vec<String> {
    for line in unit_text.split_inclusive('\n') {
        if line.starts_with("ExecStart=") {
            let after = line.rsplit(" -m meerschaum ").next().unwrap_or("");
            let before_lt = after.split('<').next().unwrap_or("");
            return crate::shlex::split(before_lt).unwrap_or_default();
        }
    }
    Vec::new()
}

impl SystemdExecutor {
    /// `create_job` once the unit's name is chosen.
    fn create_unit(
        &self,
        name: &str,
        sysargs: &[String],
        properties: Option<&Map>,
    ) -> SuccessTuple {
        let service_name = self.get_service_name(name);
        let service_file_path = self.get_service_file_path(name);
        let symlink_path = self.get_service_symlink_file_path(name);

        // A local job of the same name hands over its directory (and keeps its logs).
        match self.migrate_local_job(name) {
            Ok(true) => {
                if let Ok(mut warnings) = self.warnings.lock() {
                    warnings.push(format!(
                        "Moved the local job '{name}' to systemd (unit '{service_name}'); its logs were kept."
                    ));
                }
            }
            Ok(false) => {}
            Err(e) => return SuccessTuple::fail(e),
        }

        // `socket_stdin.file_handler`: the stdin FIFO exists once the job does.
        if let Err(e) = std::fs::create_dir_all(self.get_service_job_path(name))
            .and_then(|()| self.stdin_file(name).ensure_fifo())
        {
            return SuccessTuple::fail(e.to_string());
        }

        // The hidden job stores the properties (`externally_managed`, `env`, ...).
        let Some(mut job) = self.get_hidden_job(name, Some(sysargs.to_vec()), properties.cloned())
        else {
            return SuccessTuple::fail(format!("Invalid job name '{name}'."));
        };
        job.set_externally_managed();
        let written = job.daemon().setup(true);
        if !written.success {
            return written;
        }

        if let Err(e) =
            std::fs::create_dir_all(service_file_path.parent().unwrap_or(&service_file_path))
        {
            return SuccessTuple::fail(e.to_string());
        }
        let process_env: Vec<(String, String)> = self.process_env.clone().unwrap_or_else(|| {
            std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect()
        });
        if resolve_python(&process_env).is_none()
            && let Ok(mut warnings) = self.warnings.lock()
        {
            warnings.push(unpinned_python_warning(name));
        }
        let text = self.get_service_file_text(name, sysargs, &self.exec_program(), &process_env);
        if let Err(e) = write_private(&service_file_path, &text) {
            return SuccessTuple::fail(e.to_string());
        }

        let linked = make_symlink(&service_file_path, &symlink_path);
        if !linked.success {
            return linked;
        }

        let mut fails = 0;
        for command in [
            vec!["daemon-reload"],
            vec!["enable", service_name.as_str()],
            vec!["start", service_name.as_str()],
        ] {
            if !self.run_command(&command).success {
                fails += 1;
            }
        }
        if fails > 1 {
            return SuccessTuple::fail("Failed to reload systemd.");
        }
        SuccessTuple::ok(format!("Started job '{name}' via systemd."))
    }

    /// A `local` job of the same name in this root (Meerschaum before rc15 ran the jobs of
    /// every root but the default one locally) moves to `systemd`: its process is stopped, its
    /// logs move to the unit's log, and its directory makes way for the hidden sister job.
    fn migrate_local_job(&self, name: &str) -> Result<bool, String> {
        let dpaths = crate::config::DaemonPaths::new(&self.ctx.paths, name);
        if !dpaths.properties.exists() || dpaths.externally_managed.exists() {
            return Ok(false);
        }
        let Ok(mut local) = Job::open(name, &self.ctx) else {
            return Ok(false);
        };
        if local.status() != crate::daemon::Status::Stopped {
            let stopped = local.stop(None);
            if !stopped.success {
                return Err(format!(
                    "Could not stop the local job '{name}' to move it to systemd:\n{}",
                    stopped.message
                ));
            }
        }
        let old_log = local.daemon().log_path();
        let new_log = self.get_service_logs_path(name);
        move_rotating_logs(&old_log, &new_log);
        let _ = std::fs::remove_file(&dpaths.log_offset);
        let cleaned = local.daemon().cleanup(true);
        if !cleaned.success {
            return Err(cleaned.message);
        }
        Ok(true)
    }
}

impl JobExecutor for SystemdExecutor {
    fn executor_keys(&self) -> String {
        "systemd".into()
    }

    fn get_job_names(&self) -> Vec<String> {
        // This root's jobs: the directories under its `systemd/services/` whose unit is
        // installed. A root never lists the units another root installed (v4 listed every
        // `mrsm-*.service` of the user, which was right only while one root used systemd).
        // PARITY (divergence): the directory keeps the job's real name (`my job`); v4 listed the
        // normalized unit name (`my-job`), for which the hidden job does not exist.
        let Ok(entries) = std::fs::read_dir(&self.ctx.paths.systemd_jobs_dir) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| self.unit_is_installed(n))
            .collect();
        names.sort();
        names
    }

    fn take_warnings(&self) -> Vec<String> {
        self.warnings
            .lock()
            .map(|mut w| std::mem::take(&mut *w))
            .unwrap_or_default()
    }

    fn get_job_exists(&self, name: &str) -> bool {
        let service = self.get_service_name(name);
        self.get_job_names()
            .iter()
            .any(|n| n == name || self.get_service_name(n) == service)
    }

    fn get_job_metadata(&self, name: &str) -> Map {
        let mut meta = Map::new();
        meta.insert(
            "sysargs".into(),
            Value::List(
                self.get_job_sysargs(name)
                    .into_iter()
                    .map(Value::Str)
                    .collect(),
            ),
        );
        let result = self.read_job_result(name);
        meta.insert(
            "result".into(),
            Value::List(vec![
                Value::Bool(result.success),
                Value::Str(result.message),
            ]),
        );
        meta.insert("restart".into(), Value::Bool(self.get_job_restart(name)));
        let status = self.get_job_status(name);
        let mut daemon = Map::new();
        daemon.insert("status".into(), Value::Str(status));
        daemon.insert(
            "pid".into(),
            self.get_job_pid(name)
                .map(|p| Value::Int(p as i64))
                .unwrap_or(Value::Null),
        );
        daemon.insert(
            "properties".into(),
            Value::Map(self.get_job_properties(name)),
        );
        meta.insert("daemon".into(), Value::Map(daemon));
        meta
    }

    fn get_job_properties(&self, name: &str) -> Map {
        let mut props = self
            .get_hidden_job(name, None, None)
            .map(|mut j| j.daemon().properties())
            .unwrap_or_default();
        props.shift_remove("externally_managed");
        props
    }

    fn get_job_status(&self, name: &str) -> String {
        let service = self.get_service_name(name);
        match self.run_command_output(&["is-active", &service]).as_str() {
            "activating" => "running".into(),
            "active" => {
                let Some(pid) = self.get_job_pid(name) else {
                    return "stopped".into();
                };
                match self.ctx.process.process_state(pid) {
                    ProcState::Stopped => "paused".into(),
                    ProcState::Gone | ProcState::Zombie => "stopped".into(),
                    ProcState::Alive => "running".into(),
                }
            }
            _ => "stopped".into(),
        }
    }

    fn get_job_began(&self, name: &str) -> Option<String> {
        self.show_timestamp(name, "ActiveEnterTimestamp")
    }

    fn get_job_ended(&self, name: &str) -> Option<String> {
        self.show_timestamp(name, "InactiveEnterTimestamp")
    }

    fn get_job_paused(&self, name: &str) -> Option<String> {
        if self.get_job_status(name) != "paused" {
            return None;
        }
        let stop_time = self.get_hidden_job(name, None, None)?.stop_time()?;
        Some(stop_time.isoformat())
    }

    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        if let Some(reason) = crate::names::invalid_job_name_reason(name) {
            return SuccessTuple::fail(format!("Invalid name: {reason}."));
        }
        let compose_var = &meerschaum_core::static_config::static_config()
            .environment
            .compose_project;
        let project = properties
            .and_then(|p| p.get("env"))
            .and_then(|e| e.get(compose_var))
            .and_then(Value::as_str)
            .map(str::to_string);
        let service_name = self
            .existing_service_name(name)
            .unwrap_or_else(|| self.unique_service_name(name, project.as_deref()));
        if let Ok(mut creating) = self.creating.lock() {
            creating.insert(name.to_string(), service_name);
        }
        let result = self.create_unit(name, sysargs, properties);
        if let Ok(mut creating) = self.creating.lock() {
            creating.remove(name);
        }
        result
    }

    fn start_job(&self, name: &str) -> SuccessTuple {
        if let Some(mut job) = self.get_hidden_job(name, None, None) {
            let _ = job.daemon().remove_stop_file();
        }
        let service = self.get_service_name(name);
        if self.get_job_status(name) == "paused" {
            return self.run_command(&["kill", "-s", "SIGCONT", &service]);
        }
        self.run_command(&["start", &service])
    }

    fn stop_job(&self, name: &str) -> SuccessTuple {
        let service = self.get_service_name(name);
        if let Some(mut job) = self.get_hidden_job(name, None, None) {
            let _ = job.daemon().write_stop_file("quit");
        }
        let _ = self.run_command(&["kill", "-s", "SIGINT", &service]);
        let (timeout, interval) = self.timeout();
        let begin = Instant::now();
        while begin.elapsed() < timeout.duration() {
            if self.get_job_status(name) == "stopped" {
                return SuccessTuple::ok("Success");
            }
            std::thread::sleep(interval.duration().max(Duration::from_millis(1)));
        }
        self.run_command(&["stop", &service])
    }

    fn pause_job(&self, name: &str) -> SuccessTuple {
        let service = self.get_service_name(name);
        if let Some(mut job) = self.get_hidden_job(name, None, None) {
            let _ = job.daemon().write_stop_file("pause");
        }
        self.run_command(&["kill", "-s", "SIGSTOP", &service])
    }

    fn delete_job(&self, name: &str) -> SuccessTuple {
        if let Some(reason) = crate::names::invalid_job_name_reason(name) {
            return SuccessTuple::fail(format!("Invalid name: {reason}."));
        }
        let service = self.get_service_name(name);
        let mut job = self.get_hidden_job(name, None, None);
        let delete_after_completion = job.as_mut().is_some_and(|j| j.delete_after_completion());
        if !delete_after_completion {
            let _ = self.stop_job(name);
            let _ = self.run_command(&["disable", &service]);
        }

        let job_path = self.get_service_job_path(name);
        if job_path.exists()
            && let Err(e) = std::fs::remove_dir_all(&job_path)
        {
            return SuccessTuple::fail(e.to_string());
        }

        let logs_path = self.get_service_logs_path(name);
        let logs_prefix = format!(
            "{}.",
            logs_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default()
        );
        let mut file_paths = vec![
            self.get_service_file_path(name),
            self.get_service_symlink_file_path(name),
            self.get_socket_path(name),
            self.get_result_path(name),
        ];
        if let Ok(entries) = std::fs::read_dir(&self.ctx.paths.systemd_logs_dir) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&logs_prefix)
                {
                    file_paths.push(entry.path());
                }
            }
        }
        for path in file_paths {
            // PARITY: v4 tests `path.exists()`, which is false for the symlink once its target
            // is gone (the job directory was just removed), so v4 leaves a dangling
            // `mrsm-<name>.service` link behind and a later `create_job` fails with
            // `FileExistsError`. A dangling link is removed here (`symlink_metadata`).
            if path.symlink_metadata().is_ok()
                && let Err(e) = std::fs::remove_file(&path)
            {
                return SuccessTuple::fail(e.to_string());
            }
        }

        // v4 hands the hidden job a `_status_hook`, so `job.delete()` asks `systemctl is-active`
        // once more before it removes the sister job's directory.
        let _ = self.get_job_status(name);
        if let Some(mut job) = job {
            let _ = job.delete();
        }
        self.run_command(&["daemon-reload"])
    }

    fn get_logs(&self, name: &str) -> Option<String> {
        Some(self.get_job_rotating_file(name)?.read())
    }

    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime> {
        self.get_hidden_job(name, None, None)?.stop_time()
    }

    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        let mut source = SystemdSource {
            executor: self,
            name,
        };
        monitor_log_stream(&mut source, |line: &str| callback(line), opts)
    }

    fn get_job_pid(&self, name: &str) -> Option<i32> {
        SystemdExecutor::get_job_pid(self, name)
    }

    fn get_job_result(&self, name: &str) -> Option<SuccessTuple> {
        Some(self.read_job_result(name))
    }

    fn get_job_is_blocking_on_stdin(&self, name: &str) -> bool {
        let socket = self.get_socket_path(name);
        let mut block = socket.into_os_string();
        block.push(".block");
        PathBuf::from(block).exists()
    }

    fn get_job_prompt_kwargs(&self, name: &str) -> Map {
        self.get_hidden_job(name, None, None)
            .map(|mut j| j.get_prompt_kwargs())
            .unwrap_or_default()
    }
}

/// The scope of a non-default root's unit names when no Compose project names them: the first
/// eight hex digits of the SHA-256 of the root's canonical path (stable across runs).
pub fn root_scope(root: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let digest = Sha256::digest(canonical.to_string_lossy().as_bytes());
    digest[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// A word of a unit name: spaces become dashes (as v4 does) and anything systemd does not allow
/// in a unit name (`[A-Za-z0-9:_.-]`) an underscore.
pub fn unit_word(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            ' ' => '-',
            c if c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '.' | '-') => c,
            _ => '_',
        })
        .collect()
}

/// `mrsm-<scope>.<job>.service`, shortened (with a hash of the full name) to stay within
/// systemd's 255-byte unit names.
pub fn scoped_service_name(scope: &str, name: &str) -> String {
    let scope = unit_word(scope);
    let job = unit_word(name);
    let full = format!("mrsm-{scope}.{job}.service");
    if full.len() <= 255 {
        return full;
    }
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(full.as_bytes());
    let hash: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
    let budget = 255 - "mrsm-..service".len() - hash.len() - 1;
    let scope: String = scope.chars().take(budget / 3).collect();
    let job: String = job.chars().take(budget - scope.len()).collect();
    format!("mrsm-{scope}.{job}-{hash}.service")
}

/// Whether two paths name the same file (compared canonically when they exist).
fn same_path(a: &std::path::Path, b: &std::path::Path) -> bool {
    let canon = |p: &std::path::Path| p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
    canon(a) == canon(b)
}

/// Write a file only its owner can read (`0600` on Unix): a unit may carry credentials in its
/// `Environment=MRSM_CONFIG=...` line.
fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(text.as_bytes())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, text)
    }
}

/// Move a rotating log's subfiles (`<old>.<n>`) to `<new>.<n>`, unless `new` already has some:
/// the history of a local job continues in its unit's log.
fn move_rotating_logs(old: &std::path::Path, new: &std::path::Path) {
    let subfiles = |base: &std::path::Path| -> Vec<(u64, PathBuf)> {
        let (Some(dir), Some(stem)) = (base.parent(), base.file_name()) else {
            return Vec::new();
        };
        let prefix = format!("{}.", stem.to_string_lossy());
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        let file = e.file_name().to_string_lossy().into_owned();
                        let index = file.strip_prefix(&prefix)?.parse::<u64>().ok()?;
                        Some((index, e.path()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    if !subfiles(new).is_empty() {
        return;
    }
    let Some(new_dir) = new.parent() else {
        return;
    };
    if std::fs::create_dir_all(new_dir).is_err() {
        return;
    }
    let new_base = new
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    for (index, path) in subfiles(old) {
        let _ = std::fs::rename(&path, new_dir.join(format!("{new_base}.{index}")));
    }
}

/// `make_symlink(src_path, dest_path)`: a symlink at `dest` pointing to `src`.
fn make_symlink(src: &std::path::Path, dest: &std::path::Path) -> SuccessTuple {
    #[cfg(unix)]
    {
        let canonical = |p: &std::path::Path| std::fs::canonicalize(p).ok();
        if dest.exists() && canonical(dest) == canonical(src) {
            return SuccessTuple::ok("Symlink already exists.");
        }
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // PARITY: a dangling link at `dest` makes v4's `symlink_to` raise `FileExistsError`;
        // it is replaced here (see `delete_job`).
        if dest.symlink_metadata().is_ok() && !dest.exists() {
            let _ = std::fs::remove_file(dest);
        }
        match std::os::unix::fs::symlink(src, dest) {
            Ok(()) => SuccessTuple::ok("Success"),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (src, dest);
        SuccessTuple::fail("Cannot create systemd services on this platform.")
    }
}
