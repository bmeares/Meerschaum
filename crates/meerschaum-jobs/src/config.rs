//! The `jobs` configuration (`meerschaum/config/_jobs.py`) and the paths jobs live under.

use std::fmt;
use std::path::{Path, PathBuf};

use meerschaum_core::config::Config;
use meerschaum_core::value::Value;

/// A number of seconds as Python holds it: an `int` or a `float` (the distinction shows in
/// messages such as `"... within 4 seconds."` versus `"... within 0.5 seconds."`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Seconds {
    /// A Python `int`.
    Int(i64),
    /// A Python `float`.
    Float(f64),
}

impl Seconds {
    /// The value as `f64`.
    pub fn as_f64(self) -> f64 {
        match self {
            Seconds::Int(i) => i as f64,
            Seconds::Float(f) => f,
        }
    }

    /// Python truthiness (`if not timeout:`).
    pub fn is_truthy(self) -> bool {
        self.as_f64() != 0.0
    }

    /// The value as a [`std::time::Duration`] (negative values clamp to zero).
    pub fn duration(self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.as_f64().max(0.0))
    }

    /// The plural suffix Python adds: `'s' if timeout != 1 else ''`.
    pub fn plural(self) -> &'static str {
        if self.as_f64() != 1.0 { "s" } else { "" }
    }

    fn from_value(value: &Value) -> Option<Seconds> {
        match value {
            Value::Int(i) => Some(Seconds::Int(*i)),
            Value::Float(f) => Some(Seconds::Float(*f)),
            _ => None,
        }
    }
}

impl fmt::Display for Seconds {
    /// `str(timeout)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Seconds::Int(i) => write!(f, "{i}"),
            Seconds::Float(x) => write!(f, "{}", meerschaum_core::pyfmt::float_repr(*x)),
        }
    }
}

impl From<f64> for Seconds {
    fn from(f: f64) -> Self {
        Seconds::Float(f)
    }
}

impl From<i64> for Seconds {
    fn from(i: i64) -> Self {
        Seconds::Int(i)
    }
}

/// The resolved `jobs` (and `system.cli.refresh_seconds`) configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct JobsConfig {
    /// `jobs.timeout_seconds` (default 4).
    pub timeout_seconds: Seconds,
    /// `jobs.check_timeout_interval_seconds` (default 0.1).
    pub check_timeout_interval_seconds: Seconds,
    /// `jobs.terminal.lines` (default 40), exported as `LINES`.
    pub terminal_lines: i64,
    /// `jobs.terminal.columns` (default 70), exported as `COLUMNS`.
    pub terminal_columns: i64,
    /// `jobs.logs.timestamps.enabled` (default `True`).
    pub timestamps_enabled: bool,
    /// `jobs.logs.timestamps.format` (default `'%Y-%m-%d %H:%M'`).
    pub timestamp_format: String,
    /// `jobs.logs.timestamps.follow_format` (default `'%H:%M'`).
    pub follow_timestamp_format: String,
    /// `jobs.logs.num_files_to_keep` (default 5).
    pub num_files_to_keep: i64,
    /// `jobs.logs.max_file_size` in bytes (default 100,000).
    pub max_file_size: i64,
    /// `jobs.logs.lines_to_show` (default 30).
    pub lines_to_show: i64,
    /// `jobs.logs.refresh_files_seconds` (default 5).
    pub refresh_files_seconds: Seconds,
    /// `system.cli.refresh_seconds` (default 0.1).
    pub cli_refresh_seconds: Seconds,
    /// `meerschaum.executor`, if configured.
    pub executor: Option<String>,
    /// `jobs.detached.*`: how a job's process is searched for without its PID file.
    pub detached: crate::detached::Limits,
}

impl Default for JobsConfig {
    /// The values of `default_jobs_config`.
    fn default() -> Self {
        JobsConfig {
            timeout_seconds: Seconds::Int(4),
            check_timeout_interval_seconds: Seconds::Float(0.1),
            terminal_lines: 40,
            terminal_columns: 70,
            timestamps_enabled: true,
            timestamp_format: "%Y-%m-%d %H:%M".to_string(),
            follow_timestamp_format: "%H:%M".to_string(),
            num_files_to_keep: 5,
            max_file_size: 100_000,
            lines_to_show: 30,
            refresh_files_seconds: Seconds::Int(5),
            cli_refresh_seconds: Seconds::Float(0.1),
            executor: None,
            detached: crate::detached::Limits::default(),
        }
    }
}

impl JobsConfig {
    /// Read the values from a config registry (missing or ill-typed keys keep their defaults).
    pub fn from_config(config: &Config) -> JobsConfig {
        let mut out = JobsConfig::default();
        let get = |keys: &[&str]| -> Option<Value> {
            let opts = meerschaum_core::config::GetOptions {
                warn: false,
                ..Default::default()
            };
            let (valid, value) = config.get_with(keys, opts);
            valid.then_some(value)
        };
        if let Some(s) = get(&["jobs", "timeout_seconds"])
            .as_ref()
            .and_then(Seconds::from_value)
        {
            out.timeout_seconds = s;
        }
        if let Some(s) = get(&["jobs", "check_timeout_interval_seconds"])
            .as_ref()
            .and_then(Seconds::from_value)
        {
            out.check_timeout_interval_seconds = s;
        }
        if let Some(i) = get(&["jobs", "terminal", "lines"]).and_then(|v| v.as_i64()) {
            out.terminal_lines = i;
        }
        if let Some(i) = get(&["jobs", "terminal", "columns"]).and_then(|v| v.as_i64()) {
            out.terminal_columns = i;
        }
        if let Some(b) = get(&["jobs", "logs", "timestamps", "enabled"]) {
            out.timestamps_enabled = b.truthy();
        }
        if let Some(s) = get(&["jobs", "logs", "timestamps", "format"])
            && let Some(s) = s.as_str()
        {
            out.timestamp_format = s.to_string();
        }
        if let Some(s) = get(&["jobs", "logs", "timestamps", "follow_format"])
            && let Some(s) = s.as_str()
        {
            out.follow_timestamp_format = s.to_string();
        }
        if let Some(i) = get(&["jobs", "logs", "num_files_to_keep"]).and_then(|v| v.as_i64()) {
            out.num_files_to_keep = i;
        }
        if let Some(i) = get(&["jobs", "logs", "max_file_size"]).and_then(|v| v.as_i64()) {
            out.max_file_size = i;
        }
        if let Some(i) = get(&["jobs", "logs", "lines_to_show"]).and_then(|v| v.as_i64()) {
            out.lines_to_show = i;
        }
        if let Some(s) = get(&["jobs", "logs", "refresh_files_seconds"])
            .as_ref()
            .and_then(Seconds::from_value)
        {
            out.refresh_files_seconds = s;
        }
        if let Some(s) = get(&["system", "cli", "refresh_seconds"])
            .as_ref()
            .and_then(Seconds::from_value)
        {
            out.cli_refresh_seconds = s;
        }
        out.executor =
            get(&["meerschaum", "executor"]).and_then(|v| v.as_str().map(str::to_string));
        let secs = |key: &str| {
            get(&["jobs", "detached", key])
                .as_ref()
                .and_then(Seconds::from_value)
                .map(Seconds::duration)
        };
        if let Some(d) = secs("index_seconds") {
            out.detached.index_ttl = d;
        }
        if let Some(d) = secs("negative_seconds") {
            out.detached.negative_ttl = d;
        }
        if let Some(d) = secs("launch_grace_seconds") {
            out.detached.launch_grace = d;
        }
        if let Some(d) = secs("create_time_slack_seconds") {
            out.detached.create_slack = d;
        }
        out
    }
}

/// The directories jobs use (`meerschaum.config.paths`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobPaths {
    /// `ROOT_DIR_PATH`.
    pub root: PathBuf,
    /// `DAEMON_RESOURCES_PATH`: `{root}/jobs`.
    pub jobs_dir: PathBuf,
    /// `LOGS_RESOURCES_PATH`: `{root}/logs`.
    pub logs_dir: PathBuf,
    /// `DAEMON_ERROR_LOG_PATH`: `{root}/daemon_errors.log`.
    pub daemon_error_log: PathBuf,
    /// `CHECK_JOBS_LOCK_PATH`: `{root}/.internal/check-jobs.lock`.
    pub check_jobs_lock: PathBuf,
    /// `SYSTEMD_USER_RESOURCES_PATH`: `{DOT_CONFIG_DIR}/systemd/user`.
    pub systemd_user_dir: PathBuf,
    /// `SYSTEMD_JOBS_RESOURCES_PATH`: `{root}/systemd/services`.
    pub systemd_jobs_dir: PathBuf,
    /// `SYSTEMD_LOGS_RESOURCES_PATH`: `{root}/systemd/logs`.
    pub systemd_logs_dir: PathBuf,
}

impl JobPaths {
    /// The paths for a root directory, with `~/.config` (or `$XDG_CONFIG_HOME`) for systemd.
    pub fn for_root(root: impl Into<PathBuf>, dot_config_dir: impl Into<PathBuf>) -> JobPaths {
        let root = root.into();
        let dot_config = dot_config_dir.into();
        JobPaths {
            jobs_dir: root.join("jobs"),
            logs_dir: root.join("logs"),
            daemon_error_log: root.join("daemon_errors.log"),
            check_jobs_lock: root.join(".internal").join("check-jobs.lock"),
            systemd_user_dir: dot_config.join("systemd").join("user"),
            systemd_jobs_dir: root.join("systemd").join("services"),
            systemd_logs_dir: root.join("systemd").join("logs"),
            root,
        }
    }

    /// The paths from a config registry's resolved paths.
    pub fn from_config(config: &Config) -> JobPaths {
        let paths = config.paths();
        JobPaths {
            root: paths.path("ROOT_DIR_PATH"),
            jobs_dir: paths.path("DAEMON_RESOURCES_PATH"),
            logs_dir: paths.path("LOGS_RESOURCES_PATH"),
            daemon_error_log: paths.path("DAEMON_ERROR_LOG_PATH"),
            check_jobs_lock: paths.path("CHECK_JOBS_LOCK_PATH"),
            systemd_user_dir: paths.path("SYSTEMD_USER_RESOURCES_PATH"),
            systemd_jobs_dir: paths.path("SYSTEMD_JOBS_RESOURCES_PATH"),
            systemd_logs_dir: paths.path("SYSTEMD_LOGS_RESOURCES_PATH"),
        }
    }

    /// `Daemon._get_path_from_daemon_id`: `jobs/<daemon_id>`.
    pub fn daemon_dir(&self, daemon_id: &str) -> PathBuf {
        self.jobs_dir.join(daemon_id)
    }
}

/// Every file of a daemon's on-disk layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    /// `jobs/<id>/`.
    pub dir: PathBuf,
    /// `jobs/<id>/properties.json`.
    pub properties: PathBuf,
    /// `jobs/<id>/process.pid`.
    pub pid: PathBuf,
    /// `jobs/<id>/pickle.pkl` (written by Python only; never read by Rust).
    pub pickle: PathBuf,
    /// `jobs/<id>/input.stdin` (a FIFO).
    pub stdin: PathBuf,
    /// `jobs/<id>/input.stdin.block` (exists while the daemon blocks on input).
    pub stdin_block: PathBuf,
    /// `jobs/<id>/.stop.json`.
    pub stop: PathBuf,
    /// `jobs/<id>/prompt_kwargs.json`.
    pub prompt_kwargs: PathBuf,
    /// `jobs/<id>/.externally-managed`.
    pub externally_managed: PathBuf,
    /// `logs/<id>.log` (the rotating log's base path; subfiles are `<id>.log.<n>`).
    pub log: PathBuf,
    /// `logs/.<id>.log.offset`.
    pub log_offset: PathBuf,
}

impl DaemonPaths {
    /// The layout of a daemon under `paths`.
    pub fn new(paths: &JobPaths, daemon_id: &str) -> DaemonPaths {
        let dir = paths.daemon_dir(daemon_id);
        DaemonPaths {
            properties: dir.join("properties.json"),
            pid: dir.join("process.pid"),
            pickle: dir.join("pickle.pkl"),
            stdin: dir.join("input.stdin"),
            stdin_block: dir.join("input.stdin.block"),
            stop: dir.join(".stop.json"),
            prompt_kwargs: dir.join("prompt_kwargs.json"),
            externally_managed: dir.join(".externally-managed"),
            log: paths.logs_dir.join(format!("{daemon_id}.log")),
            log_offset: paths.logs_dir.join(format!(".{daemon_id}.log.offset")),
            dir,
        }
    }

    /// The log path, honoring `properties['logs']['path']`.
    pub fn log_path(&self, logs_path_override: Option<&Path>) -> PathBuf {
        logs_path_override
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.log.clone())
    }
}

/// The default `DOT_CONFIG_DIR_PATH`: `$XDG_CONFIG_HOME` or `~/.config`.
pub fn default_dot_config_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(xdg);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".config")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_format_like_python() {
        assert_eq!(Seconds::Int(4).to_string(), "4");
        assert_eq!(Seconds::Float(0.5).to_string(), "0.5");
        assert_eq!(Seconds::Float(4.0).to_string(), "4.0");
        assert_eq!(Seconds::Int(1).plural(), "");
        assert_eq!(Seconds::Float(1.0).plural(), "");
        assert_eq!(Seconds::Int(3).plural(), "s");
    }

    #[test]
    fn default_config_matches_python_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let env = vec![(
            "MRSM_ROOT_DIR".to_string(),
            dir.path().to_string_lossy().to_string(),
        )];
        let mut opts = meerschaum_core::config::ConfigOptions::with_env(env);
        opts.allow_writes = false;
        let config = Config::new(opts).unwrap();
        let jobs = JobsConfig::from_config(&config);
        assert_eq!(jobs, JobsConfig::default());
        let paths = JobPaths::from_config(&config);
        assert!(paths.jobs_dir.ends_with("jobs"));
        assert!(paths.logs_dir.ends_with("logs"));
    }
}
