//! Child processes of this binary that run one command and report its result: a timed chain
//! step (`--timeout-seconds`) and an in-process Compose command.

use std::io::Write;

use super::{Runtime, entry, entry_with_patch, warn_manifest_error_unless_reported};
use crate::builtin::ActionError;
use crate::shlex;
use crate::value::{ArgsMap, PyValue};

use meerschaum_core::SuccessTuple;

/// The first argument of the child a chain with `--timeout-seconds` runs each step in.
pub const PIPELINE_CHILD_ARG: &str = "__mrsm_pipeline_child__";

const FENCE_BEGIN: &str = "<MRSM_RESULT>";
const FENCE_END: &str = "</MRSM_RESULT>";

/// One `start pipeline` step in a child process, killed after `seconds` (`poll_process` in
/// v4): its output is printed as it arrives and its result is read from the fenced JSON line.
/// `Ok(None)`: the child ended without a result (v4 keeps the previous one).
pub(super) fn timed_pipeline_step(
    rt: &mut Runtime<'_>,
    line: &str,
    patch_json: &str,
    seconds: f64,
    seconds_text: &str,
) -> Result<Option<SuccessTuple>, ActionError> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    // This program again: the `mrsm` binary, or `python -m meerschaum` in the extension.
    let (program, leading) = meerschaum_core::self_exe::self_program();
    let mut command = Command::new(program);
    command
        .args(leading)
        .arg(PIPELINE_CHILD_ARG)
        .arg(line)
        .arg(patch_json)
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // v4 starts the step with `os.setsid` and stops it with `os.killpg`: the step leads its own
    // session (and process group), so its children die with it and, having no controlling
    // terminal, a step that prompts is not stopped by SIGTTIN.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: the closure runs between `fork` and `exec` and only calls `setsid(2)`, which
        // is async-signal-safe; it allocates nothing and takes no locks.
        #[allow(unsafe_code)]
        unsafe {
            command.pre_exec(|| {
                rustix::process::setsid()
                    .map(|_| ())
                    .map_err(std::io::Error::from)
            });
        }
    }
    for (key, value) in (rt.vars)() {
        command.env(key, value);
    }
    // The step writes to a pipe this process relays: it styles its output when this process
    // does (a terminal, a job, `FORCE_COLOR`), so the steps look like the summary.
    if let Some((key, value)) = crate::format::relay_env(&rt.settings()) {
        command.env(key, value);
    }
    let mut child = command
        .spawn()
        .map_err(|e| ActionError::new("OSError", format!("Failed to start the pipeline: {e}")))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let (sender, receiver) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(seconds.max(0.0));
    let mut result: Option<SuccessTuple> = None;
    let handle = |text: String, rt: &mut Runtime<'_>, result: &mut Option<SuccessTuple>| {
        if let Some(begin) = text.find(FENCE_BEGIN) {
            let from = begin + FENCE_BEGIN.len();
            let end = text.find(FENCE_END).unwrap_or(text.len());
            if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(&text[from..end])
                && let (Some(success), Some(message)) = (
                    items.first().and_then(serde_json::Value::as_bool),
                    items.get(1).and_then(serde_json::Value::as_str),
                )
            {
                *result = Some(SuccessTuple {
                    success,
                    message: message.to_string(),
                });
            }
            return;
        }
        rt.print(&format!("{text}\n"));
    };
    loop {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        match receiver.recv_timeout(wait.min(std::time::Duration::from_millis(100))) {
            Ok(text) => handle(text, rt, &mut result),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if std::time::Instant::now() >= deadline {
                    // The step leads its own process group (`setsid`, unix only): SIGKILL the
                    // whole group. Elsewhere only the direct child is killed (below), and
                    // processes it started may outlive the timeout.
                    #[cfg(unix)]
                    if let Some(pgid) = rustix::process::Pid::from_raw(child.id() as i32) {
                        let _ = rustix::process::kill_process_group(
                            pgid,
                            rustix::process::Signal::KILL,
                        );
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                    result = Some(SuccessTuple::fail(format!(
                        "Failed to execute pipeline within {seconds_text} second{}.",
                        if seconds != 1.0 { "s" } else { "" }
                    )));
                    return Ok(result);
                }
            }
        }
    }
    let _ = child.wait();
    Ok(result)
}

/// The first argument of a child that runs an in-process command of Compose (v4's
/// `run_mrsm_command(..., _subprocess=False)`, which calls `entry(sysargs)` and returns the
/// result without printing it): `mrsm __mrsm_entry_child__ <result path> <sysargs...>`. It is
/// honored only when [`ENTRY_CHILD_ENV`] names the same path (the compose runner sets both).
pub const ENTRY_CHILD_ARG: &str = "__mrsm_entry_child__";

/// The environment marker of an [`ENTRY_CHILD_ARG`] child: the result path. The process entry
/// takes it out of the environment before running anything (`run::take_entry_child_marker`), so
/// the command's own children (jobs, workers) never see it.
pub const ENTRY_CHILD_ENV: &str = "MRSM__ENTRY_CHILD";

/// The child of [`ENTRY_CHILD_ARG`]: run `entry(sysargs)`, write the result as
/// `[success, message]` JSON to the result path (a new file, 0600 on Unix, in the private
/// directory the parent created) and print nothing of it. The exit code is 0 on success, else 1.
pub fn entry_child(args: &[String], rt: &mut Runtime<'_>) -> i32 {
    let Some((path, sysargs)) = args.split_first() else {
        return 1;
    };
    // A helper of the command that started it: never the job's lap heartbeats.
    meerschaum_jobs::health::disable();
    let manifest_error = crate::python::manifest(rt).err().map(|e| e.message);
    let result = entry(sysargs, rt);
    if let Some(message) = &manifest_error {
        warn_manifest_error_unless_reported(rt, message, &result);
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    if let Ok(mut file) = options.open(path) {
        let _ = file.write_all(fence_json(&result).as_bytes());
    }
    if result.success { 0 } else { 1 }
}

/// Read the result an [`entry_child`] wrote to `path` (`None` when it wrote none).
pub fn read_entry_child_result(path: &std::path::Path) -> Option<SuccessTuple> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let items = value.as_array()?;
    Some(SuccessTuple {
        success: items.first()?.as_bool()?,
        message: items.get(1)?.as_str()?.to_string(),
    })
}

/// The child of `timed_pipeline_step`: run `line` with the patch arguments and print the result
/// between the fences.
pub fn pipeline_child(args: &[String], rt: &mut Runtime<'_>) -> i32 {
    // One step of the parent's chain: its laps are the parent's to record.
    meerschaum_jobs::health::disable();
    let line = args.first().cloned().unwrap_or_default();
    let patch_json = args.get(1).cloned().unwrap_or_default();
    let sysargs = match shlex::split(&line) {
        Ok(sysargs) => sysargs,
        Err(e) => {
            println!(
                "{FENCE_BEGIN}{}{FENCE_END}",
                fence_json(&SuccessTuple::fail(e))
            );
            return 0;
        }
    };
    let patch: ArgsMap = match serde_json::from_str::<serde_json::Value>(&patch_json) {
        Ok(serde_json::Value::Object(map)) => {
            map.into_iter().map(|(k, v)| (k, json_to_py(&v))).collect()
        }
        _ => ArgsMap::new(),
    };
    let result = entry_with_patch(&sysargs, &patch, rt);
    println!("{FENCE_BEGIN}{}{FENCE_END}", fence_json(&result));
    0
}

fn fence_json(result: &SuccessTuple) -> String {
    serde_json::json!([result.success, result.message]).to_string()
}

fn json_to_py(value: &serde_json::Value) -> PyValue {
    match value {
        serde_json::Value::Null => PyValue::None,
        serde_json::Value::Bool(b) => PyValue::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => PyValue::Int(i128::from(i)),
            None => PyValue::Float(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => PyValue::Str(s.clone()),
        serde_json::Value::Array(items) => PyValue::List(items.iter().map(json_to_py).collect()),
        serde_json::Value::Object(map) => PyValue::Dict(
            map.iter()
                .map(|(k, v)| (PyValue::Str(k.clone()), json_to_py(v)))
                .collect(),
        ),
    }
}
