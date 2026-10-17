//! Job commands and ownership: a port of `meerschaum/compose/utils/jobs.py`.

use indexmap::IndexMap;
use meerschaum_core::pipe::Pipe;
use meerschaum_core::pyfmt::py_str;
use meerschaum_core::static_config::static_config;
use meerschaum_core::value::{Map, Value};

use crate::error::{ComposeError, Messages};
use crate::ops::JobInfo;
use crate::pipes::instance_pipes;
use crate::project::{get_project_name, py_type_name};
use crate::shlex;

/// Flags which don't identify a job's workload (`NON_IDENTIFYING_FLAGS`).
pub const NON_IDENTIFYING_FLAGS: &[&str] = &["-d", "--daemon", "--no-daemon", "--debug"];

fn compose_project_var() -> String {
    static_config()
        .get(&["environment", "compose_project"])
        .and_then(Value::as_str)
        .unwrap_or("MRSM__COMPOSE_PROJECT")
        .to_string()
}

/// `_get_explicit_job_name(command)`.
pub fn explicit_job_name(command: &[String]) -> Option<String> {
    for flag in ["--name", "--job-name"] {
        if let Some(ix) = command.iter().position(|a| a == flag) {
            return command.get(ix + 1).cloned();
        }
    }
    None
}

/// `compress_pipeline_sysargs(sysargs)` from `meerschaum/_internal/arguments/_parse_arguments.py`:
/// turn a recorded `start pipeline ... -P '{"sub_args_line": ...}'` back into its compound form.
pub fn compress_pipeline_sysargs(sysargs: &[String]) -> Vec<String> {
    if sysargs.len() < 2 || sysargs[0] != "start" || sysargs[1] != "pipeline" {
        return sysargs.to_vec();
    }
    let Some(params_ix) = sysargs.iter().position(|a| a == "-P") else {
        return sysargs.to_vec();
    };
    let pipeline_args = &sysargs[2..params_ix];
    let params_str = sysargs.last().cloned().unwrap_or_default();
    let Ok(Value::Map(params)) = Value::from_json_str(&params_str) else {
        return sysargs.to_vec();
    };
    let Some(line) = params
        .get("sub_args_line")
        .filter(|v| v.truthy())
        .map(py_str)
    else {
        return sysargs.to_vec();
    };
    let Ok(mut out) = shlex::split(&line) else {
        return sysargs.to_vec();
    };
    out.push(":".into());
    out.extend(pipeline_args.iter().cloned());
    out
}

/// `_command_signature(sysargs)`.
pub fn command_signature(sysargs: &[String]) -> Vec<String> {
    compress_pipeline_sysargs(sysargs)
        .into_iter()
        .filter(|a| !NON_IDENTIFYING_FLAGS.contains(&a.as_str()))
        .collect()
}

/// `get_job_project_name(job)`.
pub fn job_project_name(job: &JobInfo) -> Option<String> {
    if let Some(name) = job
        .env
        .get(&compose_project_var())
        .filter(|s| !s.is_empty())
    {
        return Some(name.clone());
    }
    let cfg = job
        .env
        .get("MRSM__COMPOSE_CONFIG")
        .cloned()
        .unwrap_or_else(|| "{}".into());
    match Value::from_json_str(&cfg) {
        Ok(Value::Map(m)) => m.get("project_name").filter(|v| v.truthy()).map(py_str),
        _ => None,
    }
}

/// `job_belongs_to_project(job, project_name, project_command)`.
pub fn job_belongs_to_project(
    job: &JobInfo,
    project_name: &str,
    project_command: Option<&[String]>,
) -> bool {
    if job.env.get(&compose_project_var()).map(String::as_str) == Some(project_name) {
        return true;
    }
    let cfg = job
        .env
        .get("MRSM__COMPOSE_CONFIG")
        .cloned()
        .unwrap_or_else(|| "{}".into());
    if let Ok(Value::Map(m)) = Value::from_json_str(&cfg) {
        if m.get("project_name").and_then(Value::as_str) == Some(project_name) {
            return true;
        }
    }
    for (ix, arg) in job.sysargs.iter().enumerate() {
        if arg != "-t" && arg != "--tags" {
            continue;
        }
        for tag in &job.sysargs[ix + 1..] {
            if tag.starts_with('-') {
                break;
            }
            if tag == project_name {
                return true;
            }
        }
    }
    if let Some(cmd) = project_command {
        if command_signature(&job.sysargs) == command_signature(cmd) {
            return true;
        }
    }
    false
}

fn sync_value<'a>(config: &'a Map, key: &str) -> Option<&'a Value> {
    config
        .get("sync")
        .and_then(Value::as_map)
        .and_then(|s| s.get(key))
}

/// `get_jobs_commands(compose_config)`: the jobs `compose up` starts, by name.
///
/// `pipes` are the project's defined pipes (only used when no explicit `jobs:` are configured).
pub fn get_jobs_commands(
    config: &Map,
    pipes: &[Pipe],
    msgs: &mut Messages,
) -> Result<IndexMap<String, Vec<String>>, ComposeError> {
    let project_name = py_str(&get_project_name(config, msgs)?);
    let mut jobs = IndexMap::new();
    if let Some(explicit) = config.get("jobs").filter(|v| v.truthy()) {
        let Value::Map(explicit) = explicit else {
            return Err(ComposeError::type_error(format!(
                "'{}' object is not subscriptable",
                py_type_name(explicit)
            )));
        };
        for (job_name, command) in explicit {
            let Value::Str(command) = command else {
                return Err(ComposeError::attribute(format!(
                    "'{}' object has no attribute 'read'",
                    py_type_name(command)
                )));
            };
            let mut command_list =
                shlex::split(command).map_err(|e| ComposeError::value(e.to_string()))?;
            if !command_list.iter().any(|a| a == "-t" || a == "--tags") {
                command_list.push("-t".into());
                command_list.push(project_name.clone());
            }
            let explicit_name = explicit_job_name(&command_list);
            let project_job_name = explicit_name.clone().unwrap_or_else(|| job_name.clone());
            if explicit_name.is_none() {
                command_list.push("--name".into());
                command_list.push(project_job_name.clone());
            }
            if !command_list.iter().any(|a| a == "-d" || a == "--daemon") {
                command_list.push("-d".into());
            }
            if !command_list.iter().any(|a| a == "-f" || a == "--force") {
                command_list.push("-f".into());
            }
            jobs.insert(project_job_name, command_list);
        }
        return Ok(jobs);
    }

    let groups = instance_pipes(pipes);
    let schedule = sync_value(config, "schedule").filter(|v| v.truthy());
    let min_seconds = sync_value(config, "min_seconds").filter(|v| !v.is_null());
    let timeout_seconds = sync_value(config, "timeout_seconds").filter(|v| !v.is_null());
    let args: Vec<String> = match sync_value(config, "args") {
        None => Vec::new(),
        Some(Value::Str(s)) => shlex::split(s).map_err(|e| ComposeError::value(e.to_string()))?,
        Some(Value::List(l)) => l.iter().map(py_str).collect(),
        Some(other) => vec![py_str(other)],
    };
    let has = |flag: &str| args.iter().any(|a| a == flag);
    let mut additional = args.clone();
    if let Some(schedule) = schedule {
        if !has("--schedule") && !has("-s") && !has("--cron") {
            additional.push("--schedule".into());
            additional.push(py_str(schedule));
        }
    } else if !has("--loop") {
        additional.push("--loop".into());
    }
    if let Some(min_seconds) = min_seconds {
        if !has("--min-seconds") && !has("--cooldown") {
            additional.push("--min-seconds".into());
            additional.push(py_str(min_seconds));
        }
    }
    if let Some(timeout) = timeout_seconds {
        if !has("--timeout-seconds") && !has("--timeout") {
            additional.push("--timeout-seconds".into());
            additional.push(py_str(timeout));
        }
    }
    for instance_keys in groups.keys() {
        let job_name = format!("{project_name} sync ({instance_keys})");
        let mut cmd: Vec<String> = [
            "sync",
            "pipes",
            "-i",
            instance_keys,
            "-t",
            &project_name,
            "--name",
            &job_name,
            "-f",
            "-d",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        cmd.extend(additional.iter().cloned());
        jobs.insert(job_name, cmd);
    }
    Ok(jobs)
}

/// `get_project_job_names(compose_config, jobs)`: the existing jobs this project may replace.
pub fn get_project_job_names(
    config: &Map,
    pipes: &[Pipe],
    jobs: &IndexMap<String, JobInfo>,
    msgs: &mut Messages,
) -> Result<Vec<String>, ComposeError> {
    let project_name = py_str(&get_project_name(config, msgs)?);
    let mut names: Vec<String> = jobs
        .iter()
        .filter(|(_, job)| job_belongs_to_project(job, &project_name, None))
        .map(|(name, _)| name.clone())
        .collect();
    if !config.get("jobs").is_some_and(Value::truthy) {
        return Ok(names);
    }
    for (configured, command) in get_jobs_commands(config, pipes, msgs)? {
        let Some(job) = jobs.get(&configured) else {
            continue;
        };
        if job_belongs_to_project(job, &project_name, Some(&command)) {
            names.push(configured);
        }
    }
    let mut unique = Vec::new();
    for n in names {
        if !unique.contains(&n) {
            unique.push(n);
        }
    }
    Ok(unique)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn job(sysargs: &[&str]) -> JobInfo {
        JobInfo {
            sysargs: s(sysargs),
            status: "stopped".into(),
            ..JobInfo::default()
        }
    }

    fn config(json: &str) -> Map {
        Value::from_json_str(json)
            .unwrap()
            .as_map()
            .unwrap()
            .clone()
    }

    #[test]
    fn explicit_jobs_keep_established_flags() {
        let cfg =
            config(r#"{"project_name": "test-project", "jobs": {"api": "start api --port 9000"}}"#);
        let cmds = get_jobs_commands(&cfg, &[], &mut Messages::default()).unwrap();
        assert_eq!(
            cmds["api"],
            s(&[
                "start",
                "api",
                "--port",
                "9000",
                "-t",
                "test-project",
                "--name",
                "api",
                "-d",
                "-f"
            ])
        );
    }

    #[test]
    fn ownership_rules() {
        let cfg = config(r#"{"project_name": "alpha", "jobs": {"api": "start api"}}"#);
        let mut jobs = IndexMap::new();
        jobs.insert("api".to_string(), job(&["start", "api", "-t", "alpha"]));
        jobs.insert("foreign".to_string(), job(&["start", "api", "-t", "beta"]));
        assert_eq!(
            get_project_job_names(&cfg, &[], &jobs, &mut Messages::default()).unwrap(),
            s(&["api"])
        );

        let cfg = config(
            r#"{"project_name": "alpha", "jobs": {"recompute": "sync pipes -c sql:alpha -t recompute --loop"}}"#,
        );
        let command =
            get_jobs_commands(&cfg, &[], &mut Messages::default()).unwrap()["recompute"].clone();
        let running: Vec<&str> = command
            .iter()
            .map(String::as_str)
            .filter(|a| *a != "-d")
            .collect();
        let mut jobs = IndexMap::new();
        jobs.insert("recompute".to_string(), job(&running));
        assert_eq!(
            get_project_job_names(&cfg, &[], &jobs, &mut Messages::default()).unwrap(),
            s(&["recompute"])
        );
        let mut with_no_daemon = running.clone();
        with_no_daemon.push("--no-daemon");
        jobs.insert("recompute".to_string(), job(&with_no_daemon));
        assert_eq!(
            get_project_job_names(&cfg, &[], &jobs, &mut Messages::default()).unwrap(),
            s(&["recompute"])
        );
    }

    #[test]
    fn compound_pipeline_jobs_are_adopted() {
        let cfg = config(
            r#"{"project_name": "maps", "jobs": {"maps": "sync pipes -c plugin:osm -t maps + sync pipes -c sql:acme -m segments -t maps : --loop --min-seconds 604800 -w 1"}}"#,
        );
        let params = r#"{"sub_args_line":"sync pipes -c plugin:osm -t maps + sync pipes -c sql:acme -m segments -t maps","patch_args":null}"#;
        let running = [
            "start",
            "pipeline",
            "--loop",
            "--min-seconds",
            "604800",
            "-w",
            "1",
            "--name",
            "maps",
            "-f",
            "-P",
            params,
        ];
        let mut jobs = IndexMap::new();
        jobs.insert("maps".to_string(), job(&running));
        assert_eq!(
            get_project_job_names(&cfg, &[], &jobs, &mut Messages::default()).unwrap(),
            s(&["maps"])
        );
    }
}
