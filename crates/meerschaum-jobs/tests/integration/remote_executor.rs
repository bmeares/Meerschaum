//! The `api:<label>` executor over an in-memory fake of a remote jobs endpoint: create / persist /
//! reopen, idempotent start, pause / resume / stop / delete, status and timestamps, listing and
//! ownership, and the failure modes (an unreachable or unauthorized endpoint is an error, never
//! "no jobs"). The real protocol is exercised by `meerschaum-cli/tests/executors_api.rs`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::executor::{get_filtered_jobs_for, get_jobs_for, make_executor};
use meerschaum_jobs::remote::{RemoteJobs, RemoteJobsResolver, RemoteLogSink, RemoteMonitor};
use meerschaum_jobs::{Job, JobOptions, JobsContext, MonitorControl, MonitorOptions};

#[derive(Debug, Clone)]
struct FakeJob {
    sysargs: Vec<String>,
    properties: Map,
    status: String,
    logs: String,
    result: Option<(bool, String)>,
}

#[derive(Debug, Default)]
struct Server {
    jobs: IndexMap<String, FakeJob>,
    down: Option<String>,
    log: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct FakeRemote {
    server: Arc<Mutex<Server>>,
}

impl FakeRemote {
    fn check(&self) -> Result<(), String> {
        match &self.server.lock().unwrap().down {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn metadata(name: &str, job: &FakeJob) -> Map {
        let _ = name;
        let mut daemon = Map::new();
        daemon.insert("status".into(), Value::Str(job.status.clone()));
        daemon.insert(
            "pid".into(),
            if job.status == "stopped" {
                Value::Null
            } else {
                Value::Int(4242)
            },
        );
        let mut props = job.properties.clone();
        let mut process = Map::new();
        process.insert(
            "began".into(),
            Value::Str("2026-10-16T10:00:00.000001".into()),
        );
        if job.status == "stopped" {
            process.insert(
                "ended".into(),
                Value::Str("2026-10-16T11:00:00.000001".into()),
            );
        }
        props.insert("process".into(), Value::Map(process));
        daemon.insert("properties".into(), Value::Map(props));
        let mut meta = Map::new();
        meta.insert(
            "sysargs".into(),
            Value::List(job.sysargs.iter().cloned().map(Value::Str).collect()),
        );
        let result = job
            .result
            .clone()
            .unwrap_or((false, "No result available.".into()));
        meta.insert(
            "result".into(),
            Value::List(vec![Value::Bool(result.0), Value::Str(result.1)]),
        );
        meta.insert(
            "restart".into(),
            Value::Bool(job.sysargs.iter().any(|a| a == "--loop")),
        );
        meta.insert("status".into(), Value::Str(job.status.clone()));
        meta.insert("daemon".into(), Value::Map(daemon));
        meta
    }

    fn mutate(
        &self,
        name: &str,
        what: &str,
        f: impl FnOnce(&mut FakeJob) -> SuccessTuple,
    ) -> SuccessTuple {
        if let Err(e) = self.check() {
            return SuccessTuple::fail(e);
        }
        let mut server = self.server.lock().unwrap();
        server.log.push(format!("{what} {name}"));
        match server.jobs.get_mut(name) {
            Some(job) => f(job),
            None => SuccessTuple::fail(format!("Job(\"{name}\") does not exist.")),
        }
    }
}

impl RemoteJobs for FakeRemote {
    fn keys(&self) -> String {
        "api:fake".into()
    }
    fn get_jobs(&self) -> Result<IndexMap<String, Map>, String> {
        self.check()?;
        let server = self.server.lock().unwrap();
        Ok(server
            .jobs
            .iter()
            .map(|(n, j)| (n.clone(), Self::metadata(n, j)))
            .collect())
    }
    fn get_job_metadata(&self, name: &str) -> Result<Option<Map>, String> {
        self.check()?;
        let server = self.server.lock().unwrap();
        Ok(server.jobs.get(name).map(|j| Self::metadata(name, j)))
    }
    fn get_job_exists(&self, name: &str) -> Result<bool, String> {
        self.check()?;
        Ok(self.server.lock().unwrap().jobs.contains_key(name))
    }
    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        if let Err(e) = self.check() {
            return SuccessTuple::fail(e);
        }
        let mut server = self.server.lock().unwrap();
        server.log.push(format!("create {name}"));
        if server.jobs.contains_key(name) {
            return SuccessTuple::fail(format!("Job(\"{name}\") already exists."));
        }
        server.jobs.insert(
            name.to_string(),
            FakeJob {
                sysargs: sysargs.to_vec(),
                properties: properties.cloned().unwrap_or_default(),
                status: "running".into(),
                logs: String::new(),
                result: None,
            },
        );
        SuccessTuple::ok(format!("Started Job(\"{name}\")."))
    }
    fn start_job(&self, name: &str) -> SuccessTuple {
        self.mutate(name, "start", |j| {
            if j.status == "running" {
                return SuccessTuple::ok("already running");
            }
            j.status = "running".into();
            SuccessTuple::ok("Started.")
        })
    }
    fn stop_job(&self, name: &str) -> SuccessTuple {
        self.mutate(name, "stop", |j| {
            j.status = "stopped".into();
            j.result = Some((true, "Stopped.".into()));
            SuccessTuple::ok("Stopped.")
        })
    }
    fn pause_job(&self, name: &str) -> SuccessTuple {
        self.mutate(name, "pause", |j| {
            j.status = "paused".into();
            SuccessTuple::ok("Paused.")
        })
    }
    fn delete_job(&self, name: &str) -> SuccessTuple {
        if let Err(e) = self.check() {
            return SuccessTuple::fail(e);
        }
        let mut server = self.server.lock().unwrap();
        server.log.push(format!("delete {name}"));
        server.jobs.shift_remove(name);
        SuccessTuple::ok("Deleted.")
    }
    fn get_logs(&self, name: &str) -> Result<String, String> {
        self.check()?;
        self.server
            .lock()
            .unwrap()
            .jobs
            .get(name)
            .map(|j| j.logs.clone())
            .ok_or_else(|| format!("Cannot fetch logs for job '{name}'."))
    }
    fn get_job_stop_time(&self, _name: &str) -> Result<Option<String>, String> {
        self.check()?;
        Ok(None)
    }
    fn get_job_is_blocking_on_stdin(&self, _name: &str) -> Result<bool, String> {
        self.check()?;
        Ok(false)
    }
    fn get_job_prompt_kwargs(&self, _name: &str) -> Result<Map, String> {
        self.check()?;
        Ok(Map::new())
    }
    fn monitor_logs(
        &self,
        name: &str,
        sink: &mut dyn RemoteLogSink,
        _opts: RemoteMonitor,
    ) -> Result<(), String> {
        self.check()?;
        let (logs, result) = {
            let server = self.server.lock().unwrap();
            let job = server.jobs.get(name).ok_or("no such job")?;
            (job.logs.clone(), job.result.clone())
        };
        for line in logs.split_inclusive('\n') {
            if !sink.on_line(line) {
                return Ok(());
            }
        }
        if let Some((ok, msg)) = result {
            sink.on_stop(SuccessTuple {
                success: ok,
                message: msg,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Resolver {
    remotes: Mutex<HashMap<String, Arc<FakeRemote>>>,
}

impl RemoteJobsResolver for Resolver {
    fn resolve(&self, keys: &str) -> Result<Arc<dyn RemoteJobs>, String> {
        self.remotes
            .lock()
            .unwrap()
            .get(keys)
            .cloned()
            .map(|r| r as Arc<dyn RemoteJobs>)
            .ok_or_else(|| format!("No connector for '{keys}'."))
    }
}

fn context(root: &std::path::Path, remote: &Arc<FakeRemote>) -> JobsContext {
    let mut remotes = HashMap::new();
    remotes.insert("api:fake".to_string(), remote.clone());
    JobsContext::for_root(root).with_remote(Arc::new(Resolver {
        remotes: Mutex::new(remotes),
    }))
}

fn remote_job(ctx: &JobsContext, name: &str, sysargs: &[&str]) -> Job {
    Job::new(
        name,
        Some(sysargs.iter().map(|s| s.to_string()).collect()),
        JobOptions {
            executor_keys: Some("api:fake".into()),
            ..Default::default()
        },
        ctx,
    )
    .unwrap()
}

#[test]
fn create_persist_reopen_and_idempotent_start() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    let mut job = remote_job(&ctx, "alpha", &["sync", "pipes", "--loop"]);
    assert!(!job.exists());
    let created = job.start();
    assert!(created.success, "{}", created.message);
    assert!(job.exists());

    // Nothing is created on disk locally.
    assert!(!ctx.paths.jobs_dir.join("alpha").exists());

    // Reopen by name only: sysargs, status and restart come from the remote.
    let mut reopened = Job::new(
        "alpha",
        None,
        JobOptions {
            executor_keys: Some("api:fake".into()),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    assert_eq!(reopened.sysargs(), vec!["sync", "pipes", "--loop"]);
    assert_eq!(reopened.status().as_str(), "running");
    assert!(reopened.restarts());
    assert_eq!(reopened.pid(), Some(4242));
    assert!(reopened.began().is_some());
    assert!(reopened.ended().is_none());

    // Starting again does not create a second job.
    let log_before = remote.server.lock().unwrap().log.len();
    assert!(reopened.start().success);
    let log = remote.server.lock().unwrap().log.clone();
    assert_eq!(log.len(), log_before + 1);
    assert_eq!(log.last().unwrap(), "start alpha");
    assert_eq!(log.iter().filter(|l| l.starts_with("create")).count(), 1);
}

#[test]
fn pause_resume_stop_delete() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    let mut job = remote_job(&ctx, "beta", &["show", "version"]);
    assert!(job.start().success);
    assert!(job.pause(None).success);
    assert_eq!(job.status().as_str(), "paused");
    assert!(job.resume(None).success);
    assert_eq!(job.status().as_str(), "running");
    assert!(job.stop(None).success);
    assert_eq!(job.status().as_str(), "stopped");
    assert!(job.ended().is_some());
    assert_eq!(job.result().message, "Stopped.");
    assert!(job.delete().success);
    assert!(!job.exists());
}

#[test]
fn listing_and_filtering_are_the_executors_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    for (name, args) in [("one", "show"), ("_hidden", "show"), ("two", "show")] {
        assert!(remote_job(&ctx, name, &[args, "version"]).start().success);
    }
    let listing = get_jobs_for(&ctx, Some("api:fake"), false, false).unwrap();
    assert_eq!(
        listing.jobs.keys().cloned().collect::<Vec<_>>(),
        vec!["one", "two"]
    );
    let all = get_jobs_for(&ctx, Some("api:fake"), true, false).unwrap();
    assert_eq!(all.jobs.len(), 3);
    for (_, job) in all.jobs.iter() {
        assert_eq!(job.executor_keys, "api:fake");
    }

    let filtered = get_filtered_jobs_for(
        &ctx,
        Some("api:fake"),
        &["two".into(), "missing".into()],
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        filtered.jobs.keys().cloned().collect::<Vec<_>>(),
        vec!["two"]
    );
    assert_eq!(filtered.missing, vec!["Job 'missing' does not exist."]);
    assert!(filtered.listing_warnings.is_empty());

    // A local listing never shows the remote's jobs (and the reverse).
    assert!(
        get_jobs_for(&ctx, Some("local"), true, false)
            .unwrap()
            .jobs
            .is_empty()
    );
}

#[test]
fn a_failed_endpoint_is_an_error_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    assert!(
        remote_job(&ctx, "gamma", &["show", "version"])
            .start()
            .success
    );
    remote.server.lock().unwrap().down = Some("Connection refused".into());

    let err = get_jobs_for(&ctx, Some("api:fake"), true, false).unwrap_err();
    assert_eq!(err, "Connection refused");
    assert!(get_filtered_jobs_for(&ctx, Some("api:fake"), &[], true, false).is_err());

    let mut job = remote_job(&ctx, "gamma", &["show", "version"]);
    // Neither "exists" nor "create": the start reports the failure and creates nothing.
    let started = job.start();
    assert!(!started.success);
    assert_eq!(started.message, "Connection refused");
    assert_eq!(
        remote
            .server
            .lock()
            .unwrap()
            .log
            .iter()
            .filter(|l| l.starts_with("create"))
            .count(),
        1
    );
    assert!(job.try_exists().is_err());
    assert!(job.try_get_logs().is_err());
    assert!(!job.stop(None).success);
    assert!(!job.delete().success);
    // The display-only getters degrade (and never panic).
    assert_eq!(job.status().as_str(), "stopped");

    // A context with no client at all.
    let bare = JobsContext::for_root(dir.path());
    let err = get_jobs_for(&bare, Some("api:fake"), true, false).unwrap_err();
    assert!(err.contains("No jobs client is available"), "{err}");
}

#[test]
fn logs_follow_and_stop_callbacks() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    assert!(
        remote_job(&ctx, "delta", &["show", "version"])
            .start()
            .success
    );
    {
        let mut server = remote.server.lock().unwrap();
        let job = server.jobs.get_mut("delta").unwrap();
        job.logs = "line one\nline two\n".into();
        job.status = "stopped".into();
        job.result = Some((true, "finished".into()));
    }
    let mut job = remote_job(&ctx, "delta", &["show", "version"]);
    assert_eq!(job.get_logs().unwrap(), "line one\nline two\n");

    let mut lines = Vec::new();
    let result = Mutex::new(None);
    let outcome = job.monitor_logs(
        |line: &str| {
            lines.push(line.to_string());
            MonitorControl::Continue
        },
        MonitorOptions {
            stop_on_exit: true,
            on_stop: Some(Box::new(|r| *result.lock().unwrap() = Some(r.clone()))),
            ..Default::default()
        },
    );
    assert!(outcome.success);
    assert_eq!(lines, vec!["line one\n", "line two\n"]);
    assert_eq!(result.lock().unwrap().clone().unwrap().message, "finished");

    // A callback can stop the monitor early.
    let mut seen = 0;
    let outcome = job.monitor_logs(
        |_| {
            seen += 1;
            MonitorControl::Stop
        },
        MonitorOptions::default(),
    );
    assert!(outcome.success);
    assert_eq!(seen, 1);

    // The executor built by `make_executor` is the same object model.
    let executor = make_executor("api:fake", &ctx);
    assert_eq!(executor.executor_keys(), "api:fake");
    assert!(executor.try_get_job_exists("delta").unwrap());
}

#[test]
fn project_style_ownership_survives_the_round_trip() {
    // Compose stamps jobs with `MRSM__COMPOSE_PROJECT` in `properties.env`; a remote listing must
    // hand it back through `Job::env()`.
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    let mut env = IndexMap::new();
    env.insert("MRSM__COMPOSE_PROJECT".to_string(), "alpha".to_string());
    let mut job = Job::new(
        "stamped",
        Some(vec!["show".into(), "version".into()]),
        JobOptions {
            executor_keys: Some("api:fake".into()),
            env: Some(env),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    assert!(job.start().success);
    let mut listed = get_jobs_for(&ctx, Some("api:fake"), true, false)
        .unwrap()
        .jobs;
    let env = listed.get_mut("stamped").unwrap().env();
    assert_eq!(
        env.get("MRSM__COMPOSE_PROJECT").map(String::as_str),
        Some("alpha")
    );
    // The job defaults are layered underneath, as for a local job.
    assert_eq!(env.get("PYTHONUNBUFFERED").map(String::as_str), Some("1"));
}

#[test]
fn an_api_job_belongs_to_whoever_its_server_recorded() {
    // rc15 (AC9): the server records the caller as the job's owner, whatever the request says;
    // `Job::access()` reports the server's record, not this client's own properties.
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(FakeRemote::default());
    let ctx = context(dir.path(), &remote);
    let claim: Map = Value::from_json_str(r#"{"owner": "admin", "team": "Ops"}"#)
        .unwrap()
        .to_map();
    let mut job = Job::new(
        "owned",
        Some(vec!["sync".into(), "pipes".into()]),
        JobOptions {
            executor_keys: Some("api:fake".into()),
            properties: Some(claim),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    // Not created yet: nobody's (Everyone's).
    assert_eq!(
        job.access(),
        meerschaum_core::job_access::JobAccess::default()
    );
    assert!(job.start().success);
    // What the server records (here: the caller `alice` in her primary team).
    {
        let mut server = remote.server.lock().unwrap();
        let props = &mut server.jobs.get_mut("owned").unwrap().properties;
        props.insert("owner".into(), Value::Str("alice".into()));
        props.insert("team".into(), Value::Str("Data".into()));
    }
    let access = job.access();
    assert_eq!(access.owner.as_deref(), Some("alice"));
    assert_eq!(access.team.as_deref(), Some("Data"));
    let mut reopened = remote_job(&ctx, "owned", &[]);
    assert_eq!(reopened.access(), access);
}
