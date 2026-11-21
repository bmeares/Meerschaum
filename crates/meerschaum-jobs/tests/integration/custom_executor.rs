//! Executor keys of other types (`<type>:<label>`) resolve through the context's
//! [`CustomExecutors`] (the CLI installs one over native plugin executors): jobs, listings and
//! filtered listings use it, a resolver error becomes a failing executor, and an unresolved type
//! still falls through to the `api` executor's rejection. The native plugin path itself is
//! exercised end to end by `meerschaum-cli/tests/native_executor_stream.rs`.

use std::sync::{Arc, Mutex};

use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::executor::{
    CustomExecutors, JobExecutor, get_filtered_jobs_for, get_jobs_for, make_executor,
};
use meerschaum_jobs::{Job, JobOptions, JobsContext, MonitorControl, MonitorOptions, Status};

#[derive(Debug, Default)]
struct Store {
    jobs: Vec<(String, Vec<String>, String)>,
}

#[derive(Debug, Clone)]
struct Memory {
    keys: String,
    store: Arc<Mutex<Store>>,
}

impl Memory {
    fn status(&self, name: &str) -> Option<String> {
        let store = self.store.lock().unwrap();
        store
            .jobs
            .iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, _, s)| s.clone())
    }
    fn set(&self, name: &str, status: &str) -> SuccessTuple {
        let mut store = self.store.lock().unwrap();
        match store.jobs.iter_mut().find(|(n, _, _)| n == name) {
            Some(job) => {
                job.2 = status.into();
                SuccessTuple::ok(format!("{name} {status}"))
            }
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }
}

impl JobExecutor for Memory {
    fn executor_keys(&self) -> String {
        self.keys.clone()
    }
    fn get_job_names(&self) -> Vec<String> {
        let store = self.store.lock().unwrap();
        store.jobs.iter().map(|(n, _, _)| n.clone()).collect()
    }
    fn get_job_exists(&self, name: &str) -> bool {
        self.status(name).is_some()
    }
    fn get_job_metadata(&self, name: &str) -> Map {
        let store = self.store.lock().unwrap();
        let Some((_, sysargs, status)) = store.jobs.iter().find(|(n, _, _)| n == name) else {
            return Map::new();
        };
        let mut daemon = Map::new();
        daemon.insert("status".into(), Value::Str(status.clone()));
        daemon.insert("pid".into(), Value::Null);
        daemon.insert("properties".into(), Value::Map(Map::new()));
        let mut meta = Map::new();
        meta.insert(
            "sysargs".into(),
            Value::List(sysargs.iter().cloned().map(Value::Str).collect()),
        );
        meta.insert(
            "result".into(),
            Value::List(vec![Value::Bool(true), Value::Str("ok".into())]),
        );
        meta.insert("restart".into(), Value::Bool(false));
        meta.insert("daemon".into(), Value::Map(daemon));
        meta
    }
    fn get_job_properties(&self, _name: &str) -> Map {
        Map::new()
    }
    fn get_job_status(&self, name: &str) -> String {
        self.status(name).unwrap_or_else(|| "stopped".into())
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
    fn create_job(&self, name: &str, sysargs: &[String], _: Option<&Map>) -> SuccessTuple {
        self.store
            .lock()
            .unwrap()
            .jobs
            .push((name.into(), sysargs.to_vec(), "running".into()));
        SuccessTuple::ok(format!("created {name}"))
    }
    fn start_job(&self, name: &str) -> SuccessTuple {
        self.set(name, "running")
    }
    fn stop_job(&self, name: &str) -> SuccessTuple {
        self.set(name, "stopped")
    }
    fn pause_job(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail("`pause_job` is not supported by the 'memory:test' executor.")
    }
    fn delete_job(&self, name: &str) -> SuccessTuple {
        self.store
            .lock()
            .unwrap()
            .jobs
            .retain(|(n, _, _)| n != name);
        SuccessTuple::ok(format!("deleted {name}"))
    }
    fn get_logs(&self, name: &str) -> Option<String> {
        Some(format!("log of {name}\n"))
    }
    fn get_job_stop_time(&self, _name: &str) -> Option<PyDateTime> {
        None
    }
    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        _opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        callback(&format!("log of {name}"));
        SuccessTuple::success()
    }
    fn get_job_is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    fn get_job_prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
}

#[derive(Debug, Default)]
struct Resolver {
    store: Arc<Mutex<Store>>,
}

impl CustomExecutors for Resolver {
    fn resolve(&self, executor_keys: &str) -> Option<Result<Box<dyn JobExecutor>, String>> {
        match executor_keys.split(':').next() {
            Some("memory") => Some(Ok(Box::new(Memory {
                keys: executor_keys.into(),
                store: self.store.clone(),
            }))),
            Some("broken") => Some(Err("the plugin could not be loaded".into())),
            _ => None,
        }
    }
}

fn context(root: &std::path::Path) -> JobsContext {
    JobsContext::for_root(root).with_executors(Arc::new(Resolver::default()))
}

fn job(ctx: &JobsContext, name: &str, keys: &str) -> Job {
    Job::new(
        name,
        Some(vec!["show".into(), "version".into()]),
        JobOptions {
            executor_keys: Some(keys.into()),
            ..Default::default()
        },
        ctx,
    )
    .unwrap()
}

#[test]
fn custom_executor_jobs_lifecycle_and_listing() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context(dir.path());
    assert_eq!(
        make_executor("memory:test", &ctx).executor_keys(),
        "memory:test"
    );

    let mut j = job(&ctx, "j1", "memory:test");
    assert!(!j.exists());
    let started = j.start();
    assert!(started.success, "{started:?}");
    assert_eq!(started.message, "created j1");
    assert_eq!(j.status(), Status::Running);
    assert!(j.start().success, "an existing job is started, not created");
    assert!(!j.pause(None).success);
    assert!(j.stop(None).success);
    assert_eq!(j.status(), Status::Stopped);
    assert_eq!(j.get_logs().as_deref(), Some("log of j1\n"));

    let listing = get_jobs_for(&ctx, Some("memory:test"), false, true).unwrap();
    assert_eq!(listing.jobs.keys().collect::<Vec<_>>(), vec!["j1"]);
    let filtered = get_filtered_jobs_for(
        &ctx,
        Some("memory:test"),
        &["j1".into(), "j2".into()],
        false,
        true,
    )
    .unwrap();
    assert_eq!(filtered.jobs.len(), 1);
    assert_eq!(filtered.missing, vec!["Job 'j2' does not exist."]);
    assert!(j.delete().success);
    assert!(
        get_jobs_for(&ctx, Some("memory:test"), false, true)
            .unwrap()
            .jobs
            .is_empty()
    );
    // The local executor is untouched.
    assert!(!dir.path().join("jobs").join("j1").exists());
}

#[test]
fn unavailable_and_unresolved_custom_executors_fail_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context(dir.path());
    let broken = make_executor("broken:x", &ctx);
    assert_eq!(broken.executor_keys(), "broken:x");
    assert_eq!(
        broken.try_get_job_names().unwrap_err(),
        "the plugin could not be loaded"
    );
    assert!(!job(&ctx, "j", "broken:x").start().success);
    assert!(get_jobs_for(&ctx, Some("broken:x"), false, true).is_err());

    // Not a custom type and no `api` resolver: the `api` executor's error, as before.
    let unknown = make_executor("nope:x", &ctx);
    assert!(unknown.try_get_job_names().is_err());
    // No custom resolver at all: also the `api` executor.
    let plain = JobsContext::for_root(dir.path());
    assert!(
        make_executor("memory:test", &plain)
            .try_get_job_names()
            .is_err()
    );
}
