//! Port of `meerschaum.jobs.systemd.SystemdExecutor`: user units with the same text as Python.
//!
//! Implemented: the service / socket unit texts, every path, `get_job_names`, `get_job_exists`,
//! the `ExecStart=` sysargs parser, the restart check, properties, logs and the stdin block file.
//! Not implemented in this wave (they need `systemctl --user`): `create_job`, `start_job`,
//! `stop_job`, `pause_job`, `delete_job`, status / PID / timestamps and `monitor_logs`.

use std::path::PathBuf;

use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::daemon::JobsContext;
use crate::executor::{JobExecutor, not_implemented};
use crate::job::{Job, JobOptions, MonitorControl, MonitorOptions, RESTART_FLAGS};
use crate::rotating::{RotatingFile, RotatingFileOptions};

/// The `systemd` executor.
#[derive(Debug, Clone)]
pub struct SystemdExecutor {
    ctx: JobsContext,
}

impl SystemdExecutor {
    /// A systemd executor over a context.
    pub fn new(ctx: JobsContext) -> SystemdExecutor {
        SystemdExecutor { ctx }
    }

    /// `get_service_name(name)`: `mrsm-<name with spaces as dashes>.service`.
    pub fn get_service_name(&self, name: &str) -> String {
        format!("mrsm-{}.service", name.replace(' ', "-"))
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
        let environment = vars
            .iter()
            .map(|(k, v)| format!("Environment={k}={}", crate::shlex::quote(v)))
            .collect::<Vec<_>>()
            .join("\n");
        let service_name = self.get_service_name(name);
        format!(
            "[Unit]\nDescription=Run the job '{name}'\n\n[Service]\nExecStart={python} -m meerschaum {sysargs}\nKillSignal=SIGTERM\nRestart=always\nRestartPreventExitStatus=0\nSyslogIdentifier={service_name}\n{environment}\n\n[Install]\nWantedBy=default.target\n",
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

impl JobExecutor for SystemdExecutor {
    fn executor_keys(&self) -> String {
        "systemd".into()
    }

    fn get_job_names(&self) -> Vec<String> {
        let dir = &self.ctx.paths.systemd_user_dir;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("mrsm-") && n.ends_with(".service") && dir.join(n).exists())
            .map(|n| n["mrsm-".len()..n.len() - ".service".len()].to_string())
            .collect()
    }

    fn get_job_exists(&self, name: &str) -> bool {
        self.get_job_names().iter().any(|n| n == name)
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
        meta.insert("restart".into(), Value::Bool(self.get_job_restart(name)));
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

    fn get_job_status(&self, _name: &str) -> String {
        // `systemctl --user is-active` is not wired yet.
        "stopped".into()
    }

    fn get_job_began(&self, _name: &str) -> Option<String> {
        None
    }

    fn get_job_ended(&self, _name: &str) -> Option<String> {
        None
    }

    fn get_job_paused(&self, _name: &str) -> Option<String> {
        None
    }

    fn create_job(&self, _name: &str, _sysargs: &[String], _p: Option<&Map>) -> SuccessTuple {
        not_implemented("systemd", "create_job")
    }

    fn start_job(&self, _name: &str) -> SuccessTuple {
        not_implemented("systemd", "start_job")
    }

    fn stop_job(&self, _name: &str) -> SuccessTuple {
        not_implemented("systemd", "stop_job")
    }

    fn pause_job(&self, _name: &str) -> SuccessTuple {
        not_implemented("systemd", "pause_job")
    }

    fn delete_job(&self, _name: &str) -> SuccessTuple {
        not_implemented("systemd", "delete_job")
    }

    fn get_logs(&self, name: &str) -> Option<String> {
        Some(self.get_job_rotating_file(name)?.read())
    }

    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime> {
        self.get_hidden_job(name, None, None)?.stop_time()
    }

    fn monitor_logs(
        &self,
        _name: &str,
        _callback: &mut dyn FnMut(&str) -> MonitorControl,
        _opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        not_implemented("systemd", "monitor_logs")
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
