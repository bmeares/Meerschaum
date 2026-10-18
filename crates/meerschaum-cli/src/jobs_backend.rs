//! `/jobs/*` and `/logs/*` of `mrsm start api`: [`JobsBackend`] over `meerschaum-jobs`
//! (`meerschaum/api/routes/_jobs.py`).
//!
//! The HTTP contract is `meerschaum-server`'s; this is the behavior behind it, the one a v4
//! server has: jobs run with the `local` executor, except that a job which exists under the
//! `systemd` executor is managed there (`_get_job`), and the listing combines both on a
//! systemd host. The `api:<label>` job executor of another `mrsm` is the client of these routes.
//!
//! Every method runs the blocking job code on a blocking thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::executor::get_jobs_for;
use meerschaum_jobs::{Job, JobOptions, JobsContext, MonitorControl, MonitorOptions};
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, LogMonitor};
use serde_json::Value as Json;
use tokio::sync::mpsc;

/// The jobs of the server's root.
#[derive(Debug, Clone)]
pub struct LocalJobsBackend {
    ctx: JobsContext,
}

impl LocalJobsBackend {
    /// A backend over a jobs context (the server's root, configuration and launcher).
    pub fn new(ctx: JobsContext) -> LocalJobsBackend {
        LocalJobsBackend { ctx }
    }
}

/// `_get_job(name, sysargs)`: the `systemd` job if it exists, else a `local` one.
fn open_job(ctx: &JobsContext, name: &str, sysargs: Option<Vec<String>>) -> Option<Job> {
    let make = |keys: &str| {
        Job::new(
            name,
            sysargs.clone(),
            JobOptions {
                executor_keys: Some(keys.to_string()),
                ..Default::default()
            },
            ctx,
        )
        .ok()
    };
    if let Some(mut systemd) = make("systemd")
        && systemd.exists()
    {
        return Some(systemd);
    }
    make("local")
}

fn to_json(value: &Value) -> Json {
    serde_json::to_value(value).unwrap_or(Json::Null)
}

/// `{sysargs, result, restart, status, daemon: {status, pid, properties}}`.
fn metadata(job: &mut Job) -> Json {
    let status = job.status().as_str().to_string();
    let result = job.result();
    let mut daemon = serde_json::Map::new();
    daemon.insert("status".into(), Json::String(status.clone()));
    daemon.insert(
        "pid".into(),
        job.pid()
            .map(|p| Json::from(p as i64))
            .unwrap_or(Json::Null),
    );
    daemon.insert("properties".into(), to_json(&Value::Map(job.properties())));
    let mut meta = serde_json::Map::new();
    meta.insert(
        "sysargs".into(),
        Json::Array(job.sysargs().into_iter().map(Json::String).collect()),
    );
    meta.insert(
        "result".into(),
        Json::Array(vec![
            Json::Bool(result.success),
            Json::String(result.message),
        ]),
    );
    meta.insert("restart".into(), Json::Bool(job.restarts()));
    meta.insert("status".into(), Json::String(status));
    meta.insert("daemon".into(), Json::Object(daemon));
    Json::Object(meta)
}

fn utc(dt: PyDateTime) -> DateTime<Utc> {
    match dt {
        PyDateTime::Aware(dt) => dt.with_timezone(&Utc),
        PyDateTime::Naive(dt) => dt.and_utc(),
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(f).await.ok()
}

#[async_trait]
impl JobsBackend for LocalJobsBackend {
    async fn list(&self) -> IndexMap<String, Json> {
        let ctx = self.ctx.clone();
        blocking(move || {
            let mut out = IndexMap::new();
            if let Ok(listing) = get_jobs_for(&ctx, Some("local"), false, true) {
                for (name, mut job) in listing.jobs {
                    out.insert(name, metadata(&mut job));
                }
            }
            out
        })
        .await
        .unwrap_or_default()
    }

    async fn get(&self, name: &str) -> Option<Json> {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            let mut job = open_job(&ctx, &name, None)?;
            job.exists().then(|| metadata(&mut job))
        })
        .await
        .flatten()
    }

    async fn exists(&self, name: &str) -> bool {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || open_job(&ctx, &name, None).is_some_and(|mut j| j.exists()))
            .await
            .unwrap_or(false)
    }

    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            // The job runs with the server's environment (v4: `env={NONINTERACTIVE: '1',
            // **os.environ}` replaces whatever `properties.env` the client sent). PARITY
            // (divergence): the one client variable kept is the Compose project stamp, so the
            // ownership of a Compose project's jobs on a shared executor survives the trip;
            // v4 drops it and every project then sees the other's jobs as unowned. Nothing
            // else the client sends reaches the server's process environment.
            let mut env: IndexMap<String, String> = IndexMap::new();
            env.insert(
                meerschaum_core::static_config::static_config()
                    .environment
                    .noninteractive
                    .clone(),
                "1".to_string(),
            );
            env.extend(
                std::env::vars_os()
                    .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
            );
            let stamp = &meerschaum_core::static_config::static_config()
                .environment
                .compose_project;
            if let Some(Value::Map(client_env)) = properties.as_ref().and_then(|p| p.get("env"))
                && let Some(project) = client_env.get(stamp.as_str())
            {
                env.insert(stamp.clone(), project.py_str());
            }
            let job = Job::new(
                &name,
                Some(sysargs),
                JobOptions {
                    executor_keys: Some("local".into()),
                    env: Some(env),
                    properties,
                    ..Default::default()
                },
                &ctx,
            );
            match job {
                Err(e) => CreateJob::Started(SuccessTuple::fail(e.to_string())),
                Ok(mut job) => {
                    if job.exists() {
                        CreateJob::Exists
                    } else {
                        CreateJob::Started(job.start())
                    }
                }
            }
        })
        .await
        .unwrap_or(CreateJob::Started(SuccessTuple::fail(
            "The job request was interrupted.",
        )))
    }

    async fn delete(&self, name: &str) -> SuccessTuple {
        self.run(name, |job| job.delete()).await
    }

    async fn start(&self, name: &str) -> SuccessTuple {
        self.run(name, |job| job.start()).await
    }

    async fn stop(&self, name: &str) -> SuccessTuple {
        self.run(name, |job| job.stop(None)).await
    }

    async fn pause(&self, name: &str) -> SuccessTuple {
        self.run(name, |job| job.pause(None)).await
    }

    async fn logs(&self, name: &str) -> Option<String> {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || open_job(&ctx, &name, None)?.get_logs())
            .await
            .flatten()
    }

    async fn stop_time(&self, name: &str) -> Option<DateTime<Utc>> {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || open_job(&ctx, &name, None)?.stop_time().map(utc))
            .await
            .flatten()
    }

    async fn is_blocking_on_stdin(&self, name: &str) -> bool {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || open_job(&ctx, &name, None).is_some_and(|mut j| j.is_blocking_on_stdin()))
            .await
            .unwrap_or(false)
    }

    async fn prompt_kwargs(&self, name: &str) -> Map {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            open_job(&ctx, &name, None)
                .map(|mut j| j.get_prompt_kwargs())
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
        let (events_tx, events_rx) = mpsc::channel::<LogEvent>(256);
        let (input_tx, mut input_rx) = mpsc::channel::<String>(16);
        let stop = Arc::new(AtomicBool::new(false));
        // Dropped when the follower thread ends, however it ends.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        {
            // The websocket going away (the receiver is dropped) stops the follower even while
            // the job is silent. This watcher must end with the follower: while it holds a
            // sender the channel never closes, and a follower which ends without a result
            // (the job finished without output) would leave the websocket waiting forever.
            let (closed, stop) = (events_tx.clone(), stop.clone());
            tokio::spawn(async move {
                tokio::select! {
                    () = closed.closed() => stop.store(true, Ordering::SeqCst),
                    _ = done_rx => {}
                }
            });
        }
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        std::thread::spawn(move || {
            let _done = done_tx;
            let Some(mut job) = open_job(&ctx, &name, None) else {
                return;
            };
            let line_tx = events_tx.clone();
            let stdin_tx = events_tx.clone();
            let stop_tx = events_tx;
            let _ = job.monitor_logs(
                |line: &str| {
                    if line_tx
                        .blocking_send(LogEvent::Line(line.to_string()))
                        .is_err()
                    {
                        return MonitorControl::Stop;
                    }
                    MonitorControl::Continue
                },
                MonitorOptions {
                    stop_on_exit: true,
                    accept_input: true,
                    stop: Some(stop),
                    input: Some(Box::new(move || {
                        stdin_tx.blocking_send(LogEvent::StdinRequested).ok()?;
                        input_rx.blocking_recv()
                    })),
                    on_stop: Some(Box::new(move |result: &SuccessTuple| {
                        let _ = stop_tx.blocking_send(LogEvent::Stopped(result.clone()));
                    })),
                    ..Default::default()
                },
            );
        });
        Some(LogMonitor {
            events: events_rx,
            input: input_tx,
        })
    }
}

impl LocalJobsBackend {
    async fn run(
        &self,
        name: &str,
        f: impl FnOnce(&mut Job) -> SuccessTuple + Send + 'static,
    ) -> SuccessTuple {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || match open_job(&ctx, &name, None) {
            Some(mut job) => f(&mut job),
            None => SuccessTuple::fail(format!("Invalid job name '{name}'.")),
        })
        .await
        .unwrap_or_else(|| SuccessTuple::fail("The job request was interrupted."))
    }
}
