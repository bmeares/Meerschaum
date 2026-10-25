//! A job executor `procs:<label>`: each job is a local `mrsm <sysargs>` process whose state
//! lives under `<jobs_dir>/<name>/` (`job.json`, `pid`, `output.log`). `jobs_dir` comes from the
//! connector configuration (`MRSM_PROCS_<LABEL>='{"jobs_dir": "..."}'`), else
//! `<root_dir>/procs_jobs/<label>` (the root the host sends as `context.root_dir`, e.g. a compose
//! project's root; `$MRSM_ROOT_DIR` without one). `pause_job` is deliberately left out (it answers "not
//! supported"). Unix only; all data is synthetic.
use meerschaum_plugin::{
    Error, Plugin, Request, Result, SuccessTuple,
    executor::{Executor, LogEvent, LogStream, MonitorLogs, mrsm_command},
    serde_json::{Value, json},
};
use std::{
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
struct Procs;

impl Plugin for Procs {
    const NAME: &'static str = "native_executor";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> Value {
        json!({"schema_version": meerschaum_plugin::MANIFEST_SCHEMA_VERSION,
            "meerschaum_version": "5.0.0-rc.12", "python_version": "", "fingerprint": null,
            "plugins": {"native_executor": {"name": "native_executor", "version": "1.0.0"}},
            "executor_types": {"procs": {"type": "procs", "plugin": "native_executor",
                "class_name": "ProcsExecutor",
                "methods": ["get_job_names", "get_job_metadata", "create_job", "start_job",
                    "stop_job", "delete_job", "get_logs", "get_job_stop_time", "monitor_logs"],
                "doc": "Run jobs as local processes."}}})
    }
    fn executor(&self, request: &Request) -> Option<Box<dyn Executor>> {
        if request.connector_type != "procs" {
            return None;
        }
        let dir = request.config["jobs_dir"]
            .as_str()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                request.context["root_dir"]
                    .as_str()
                    .map(PathBuf::from)
                    .or_else(|| std::env::var_os("MRSM_ROOT_DIR").map(PathBuf::from))
                    .unwrap_or_else(std::env::temp_dir)
                    .join("procs_jobs")
                    .join(&request.label)
            });
        Some(Box::new(ProcsExecutor { dir }))
    }
}

struct ProcsExecutor {
    dir: PathBuf,
}

fn fail(e: impl std::fmt::Display) -> Error {
    Error::Message(e.to_string())
}

/// An ISO timestamp (UTC) without a date library.
fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}+00:00",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn alive(pid: u32) -> bool {
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // The state follows the parenthesized command name; a zombie has exited.
        return stat
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| !rest.starts_with('Z'));
    }
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

impl ProcsExecutor {
    fn job_dir(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
    fn read(&self, name: &str) -> Option<Value> {
        let text = std::fs::read_to_string(self.job_dir(name).join("job.json")).ok()?;
        serde_json::from_str(&text).ok()
    }
    fn write(&self, name: &str, job: &Value) -> Result<()> {
        std::fs::write(self.job_dir(name).join("job.json"), job.to_string()).map_err(fail)
    }
    fn pid(&self, name: &str) -> Option<u32> {
        std::fs::read_to_string(self.job_dir(name).join("pid"))
            .ok()?
            .trim()
            .parse()
            .ok()
    }
    fn running_pid(&self, name: &str) -> Option<u32> {
        self.pid(name).filter(|pid| alive(*pid))
    }
    fn log_path(&self, name: &str) -> PathBuf {
        self.job_dir(name).join("output.log")
    }
}

impl Executor for ProcsExecutor {
    fn get_job_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| e.path().join("job.json").is_file())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }

    fn get_job_metadata(&self, name: &str) -> Result<Value> {
        let Some(job) = self.read(name) else {
            // A job directory whose `job.json` is unreadable is a failure, not a missing job.
            if self.job_dir(name).join("job.json").exists() {
                return Err(fail(format!("job '{name}' has a corrupt job.json")));
            }
            return Ok(Value::Null);
        };
        let pid = self.running_pid(name);
        Ok(json!({
            "sysargs": job["sysargs"],
            "result": job.get("result").cloned().unwrap_or(json!([true, "Success"])),
            "restart": false,
            "daemon": {
                "status": if pid.is_some() { "running" } else { "stopped" },
                "pid": pid,
                "properties": {
                    "process": {"began": job["began"], "ended": job["ended"], "paused": null},
                    "stop_time": job["stop_time"],
                },
            },
        }))
    }

    fn create_job(
        &self,
        name: &str,
        sysargs: &[String],
        properties: &Value,
    ) -> Result<SuccessTuple> {
        std::fs::create_dir_all(self.job_dir(name)).map_err(fail)?;
        self.write(
            name,
            &json!({"sysargs": sysargs, "properties": properties, "began": null,
                "ended": null, "stop_time": null}),
        )?;
        self.start_job(name)
    }

    fn start_job(&self, name: &str) -> Result<SuccessTuple> {
        let Some(mut job) = self.read(name) else {
            return Ok(SuccessTuple::fail(format!("Job '{name}' does not exist.")));
        };
        if self.running_pid(name).is_some() {
            return Ok(SuccessTuple::ok(format!(
                "Job '{name}' is already running."
            )));
        }
        let sysargs: Vec<String> = serde_json::from_value(job["sysargs"].clone()).map_err(fail)?;
        // `mrsm_command()` runs the host's Meerschaum (the binary, or `python -m meerschaum`
        // inside the Python package). `sh` detaches the job (it is reparented once `sh` exits)
        // and prints its PID.
        let output = Command::new("sh")
            .arg("-c")
            .arg(r#""$0" "$@" >>"$PROCS_LOG" 2>&1 </dev/null & echo $!"#)
            .args(mrsm_command())
            .args(&sysargs)
            .env("PROCS_LOG", self.log_path(name))
            .output()
            .map_err(fail)?;
        let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if pid.is_empty() {
            return Ok(SuccessTuple::fail(format!("Failed to start job '{name}'.")));
        }
        std::fs::write(self.job_dir(name).join("pid"), &pid).map_err(fail)?;
        job["began"] = json!(now_iso());
        job["ended"] = Value::Null;
        job["stop_time"] = Value::Null;
        self.write(name, &job)?;
        Ok(SuccessTuple::ok(format!(
            "Started job '{name}' (PID {pid})."
        )))
    }

    fn stop_job(&self, name: &str) -> Result<SuccessTuple> {
        let Some(mut job) = self.read(name) else {
            return Ok(SuccessTuple::fail(format!("Job '{name}' does not exist.")));
        };
        if let Some(pid) = self.running_pid(name) {
            let _ = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(10);
            while alive(pid) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            if alive(pid) {
                let _ = Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status();
            }
        }
        let now = now_iso();
        job["ended"] = json!(now);
        job["stop_time"] = json!(now);
        job["result"] = json!([true, "Stopped."]);
        self.write(name, &job)?;
        Ok(SuccessTuple::ok(format!("Stopped job '{name}'.")))
    }

    fn delete_job(&self, name: &str) -> Result<SuccessTuple> {
        if self.read(name).is_none() {
            return Ok(SuccessTuple::fail(format!("Job '{name}' does not exist.")));
        }
        let stopped = self.stop_job(name)?;
        if !stopped.success {
            return Ok(stopped);
        }
        std::fs::remove_dir_all(self.job_dir(name)).map_err(fail)?;
        Ok(SuccessTuple::ok(format!("Deleted job '{name}'.")))
    }

    fn get_logs(&self, name: &str) -> Result<Option<String>> {
        Ok(std::fs::read_to_string(self.log_path(name)).ok())
    }

    fn get_job_stop_time(&self, name: &str) -> Result<Option<String>> {
        Ok(self
            .read(name)
            .and_then(|job| job["stop_time"].as_str().map(str::to_string)))
    }

    fn monitor_logs(&self, name: &str, options: MonitorLogs) -> Result<Box<dyn LogStream>> {
        if self.read(name).is_none() {
            return Err(fail(format!("Job '{name}' does not exist.")));
        }
        Ok(Box::new(Tail {
            path: self.log_path(name),
            offset: 0,
            partial: String::new(),
            pid: self.pid(name),
            stop_on_exit: options.stop_on_exit,
        }))
    }
}

/// Follows `output.log` from the start: complete lines as they appear.
struct Tail {
    path: PathBuf,
    offset: u64,
    partial: String,
    pid: Option<u32>,
    stop_on_exit: bool,
}

impl Tail {
    fn read_new(&mut self) -> Vec<String> {
        let Ok(mut file) = std::fs::File::open(&self.path) else {
            return Vec::new();
        };
        let mut text = String::new();
        if file.seek(SeekFrom::Start(self.offset)).is_err()
            || file.read_to_string(&mut text).is_err()
        {
            return Vec::new();
        }
        self.offset += text.len() as u64;
        self.partial.push_str(&text);
        let mut lines: Vec<String> = Vec::new();
        while let Some(ix) = self.partial.find('\n') {
            lines.push(self.partial[..ix].to_string());
            self.partial.drain(..=ix);
        }
        lines
    }
}

impl LogStream for Tail {
    fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent> {
        let deadline = Instant::now() + timeout;
        loop {
            let lines = self.read_new();
            if !lines.is_empty() {
                return Ok(LogEvent::Lines(lines));
            }
            if self.stop_on_exit && !self.pid.is_some_and(alive) {
                return Ok(LogEvent::End(Some(SuccessTuple::ok("Job stopped."))));
            }
            if Instant::now() >= deadline {
                return Ok(LogEvent::Timeout);
            }
            std::thread::sleep(Duration::from_millis(25).min(deadline - Instant::now()));
        }
    }
}

meerschaum_plugin::export_plugin!(Procs);
