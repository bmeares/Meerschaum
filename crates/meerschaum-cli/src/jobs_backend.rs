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
use meerschaum_server::access::Access;
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, LogMonitor};
use serde_json::{Value as Json, json};
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

/// Ordinary result publication from one current-launch properties document. This is not
/// a native attestation, data EOF or graph-readiness assertion.
pub(crate) fn committed_result(properties: &Map) -> Option<SuccessTuple> {
    if !meerschaum_jobs::detached::Timestamps::from_properties(properties).is_completed() {
        return None;
    }
    let values = properties.get("result")?.as_list()?;
    let [Value::Bool(success), Value::Str(message)] = values.as_slice() else {
        return None;
    };
    Some(SuccessTuple {
        success: *success,
        message: message.clone(),
    })
}

/// A monitor keeps the launch token it attached to, even if the name is restarted.
struct ResultPublication {
    launch: Option<String>,
    signed: Option<uuid::Uuid>,
}

impl ResultPublication {
    fn launch(properties: &Map) -> Option<&str> {
        properties
            .get("process")?
            .get(meerschaum_jobs::supervisor::LAUNCH_ID_KEY)?
            .as_str()
    }

    fn for_job(ctx: &JobsContext, name: &str, properties: &Map) -> Option<Self> {
        let resolver = ctx.launch_resolver.as_ref()?;
        let signed = if resolver.authoritative(name, properties) {
            Some(
                name.strip_prefix("mrsm-")
                    .and_then(|value| uuid::Uuid::parse_str(value).ok())
                    .filter(|id| !id.is_nil() && name == format!("mrsm-{}", id.simple()))?,
            )
        } else if resolver.tracks_unattributed_publication() {
            None
        } else {
            return None;
        };
        Some(Self {
            launch: Self::launch(properties).map(str::to_owned),
            signed,
        })
    }

    fn matches(&self, properties: &Map) -> bool {
        let marker = properties.get(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY);
        self.launch.is_some()
            && self.launch.as_deref() == Self::launch(properties)
            && match self.signed {
                Some(id) => {
                    marker
                        .and_then(Value::as_str)
                        .and_then(|value| uuid::Uuid::parse_str(value).ok())
                        == Some(id)
                }
                None => marker.is_none(),
            }
    }

    fn pending(&self, ctx: &JobsContext, name: &str, properties: &Map) -> bool {
        if !self.matches(properties) {
            return false;
        }
        if self.signed.is_some() {
            return ctx
                .launch_resolver
                .as_ref()
                .and_then(|resolver| {
                    resolver
                        .retained_generation(name, properties)
                        .ok()
                        .flatten()
                })
                .is_some_and(|generation| generation.finalization_pending().unwrap_or(false));
        }
        self.launch.as_deref().is_some_and(|launch| {
            meerschaum_jobs::supervisor::FactoryPublication::pending(
                &ctx.paths.daemon_dir(name),
                launch,
            )
        })
    }

    fn result(&self, properties: &Map) -> Option<SuccessTuple> {
        if !self.matches(properties) {
            return None;
        }
        committed_result(properties)
    }
}

fn metadata(job: &mut Job, ctx: &JobsContext) -> Json {
    let mut status = job.status().as_str().to_string();
    let mut properties = job.properties();
    let publication = ResultPublication::for_job(ctx, &job.name, &properties);
    let mut completion_pending = false;
    let mut completion_unavailable = false;
    let result = if let Some(publication) = &publication {
        if status != "stopped" {
            Json::Null
        } else {
            let pending = publication.pending(ctx, &job.name, &properties);
            properties = job.daemon().read_properties().unwrap_or_default();
            match publication.result(&properties) {
                Some(result) => json!([result.success, result.message]),
                None => {
                    completion_pending = pending && publication.matches(&properties);
                    completion_unavailable = !completion_pending;
                    if completion_unavailable {
                        status = "unavailable".into();
                    }
                    Json::Null
                }
            }
        }
    } else {
        let result = job.result();
        json!([result.success, result.message])
    };
    let mut daemon = serde_json::Map::new();
    daemon.insert("status".into(), Json::String(status.clone()));
    daemon.insert(
        "pid".into(),
        job.pid()
            .map(|p| Json::from(p as i64))
            .unwrap_or(Json::Null),
    );
    daemon.insert("properties".into(), to_json(&Value::Map(properties)));
    let mut meta = serde_json::Map::new();
    meta.insert(
        "sysargs".into(),
        Json::Array(job.sysargs().into_iter().map(Json::String).collect()),
    );
    meta.insert("result".into(), result);
    if publication.is_some() {
        meta.insert("completion_pending".into(), completion_pending.into());
        meta.insert(
            "completion_unavailable".into(),
            completion_unavailable.into(),
        );
    }
    meta.insert("restart".into(), Json::Bool(job.restarts()));
    meta.insert("status".into(), Json::String(status));
    meta.insert("daemon".into(), Json::Object(daemon));
    // v5: the job's health (`job.health`), so `api:` executors report it like local ones.
    let health = if completion_unavailable {
        json!({"state": "unknown", "message": "The native job's final result is unavailable."})
    } else {
        job.health().to_json()
    };
    meta.insert("health".into(), health);
    Json::Object(meta)
}

fn visible(caller: Option<&Access>, name: &str, job: &mut Job, ctx: &JobsContext) -> bool {
    caller.is_none_or(|caller| {
        meerschaum_server::routes::jobs::can_see(caller, name, Some(&metadata(job, ctx)))
    })
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
    async fn promote(&self, name: &str) -> SuccessTuple {
        self.run(name, |job| {
            let mut properties = job.daemon().properties();
            if let Some(Value::Map(execution)) = properties.get_mut("mrsm_execution") {
                execution.insert("kind".into(), Value::Str("persistent_job".into()));
            } else {
                let mut execution = Map::new();
                execution.insert("kind".into(), Value::Str("persistent_job".into()));
                properties.insert("mrsm_execution".into(), Value::Map(execution));
            }
            job.daemon().update_properties(&properties);
            job.daemon().write_properties()
        })
        .await
    }

    async fn list(&self) -> IndexMap<String, Json> {
        let ctx = self.ctx.clone();
        blocking(move || {
            let mut out = IndexMap::new();
            // Every `systemd` job's status, PID and times from one `systemctl show`.
            let ctx = meerschaum_jobs::summary::listing_context(&ctx, Some("local"), true);
            if let Ok(listing) = get_jobs_for(&ctx, Some("local"), false, true) {
                for (name, mut job) in listing.jobs {
                    out.insert(name, metadata(&mut job, &ctx));
                }
            }
            out
        })
        .await
        .unwrap_or_default()
    }

    async fn list_definitions(&self) -> IndexMap<String, Json> {
        let ctx = self.ctx.clone();
        blocking(move || {
            meerschaum_jobs::get_job_definitions(&ctx, false)
                .into_iter()
                .map(|(name, definition)| {
                    let meta = serde_json::json!({
                        "sysargs": definition.sysargs,
                        "daemon": {"properties": to_json(&Value::Map(definition.properties))},
                    });
                    (name, meta)
                })
                .collect()
        })
        .await
        .unwrap_or_default()
    }

    async fn get(&self, name: &str) -> Option<Json> {
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            let mut job = open_job(&ctx, &name, None)?;
            job.exists().then(|| metadata(&mut job, &ctx))
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
        // Tokio task locals do not enter spawn_blocking. Capture the accepted registration
        // now, and never let a client's serialized marker choose this job's historical owner.
        let mut properties = properties;
        if let Some(caller) = meerschaum_server::executor::context::current() {
            meerschaum_server::routes::jobs::stamp_registration(
                properties.get_or_insert_with(Map::new),
                &caller,
            );
        }
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            // The job runs with the server's environment (v4: `env={NONINTERACTIVE: '1',
            // **os.environ}` replaces whatever `properties.env` the client sent). PARITY
            // (divergence): the client variables kept are the Compose stamps (the project, and
            // the root compose file of a tree), so the ownership of a Compose project's jobs
            // on a shared executor survives the trip; v4 drops them and every project then
            // sees the other's jobs as unowned. Nothing else the client sends reaches the
            // server's process environment.
            let mut env: IndexMap<String, String> = IndexMap::new();
            env.insert(
                meerschaum_core::static_config::static_config()
                    .environment
                    .noninteractive
                    .clone(),
                "1".to_string(),
            );
            match ctx.environment_for(&name, &properties.clone().unwrap_or_default()) {
                Ok(environment) => env.extend(environment),
                Err(error) => return CreateJob::Unavailable(error.to_string()),
            }
            let stamps = &meerschaum_core::static_config::static_config().environment;
            if let Some(Value::Map(client_env)) = properties.as_ref().and_then(|p| p.get("env")) {
                for stamp in [&stamps.compose_project, &stamps.compose_root] {
                    if let Some(value) = client_env.get(stamp.as_str()) {
                        env.insert(stamp.clone(), value.py_str());
                    }
                }
            }
            // A job of an isolated team records only what its process may receive (an
            // allowlist and the administrator's passthrough names), never the rest of the
            // server's environment (its launch applies the same allowlist).
            let root = ctx.paths.root.clone();
            if let Some(props) = properties.as_ref()
                && meerschaum_jobs::config_files::job_team_scope(&root, props).is_some()
            {
                let mut options = meerschaum_core::config::ConfigOptions::with_env(
                    meerschaum_core::process_env::vars(),
                );
                options.env.retain(|(k, _)| k != "MRSM_ROOT_DIR");
                options
                    .env
                    .push(("MRSM_ROOT_DIR".into(), root.to_string_lossy().into()));
                options.allow_writes = false;
                let passthrough = meerschaum_core::Config::new(options)
                    .map(|config| meerschaum_core::team_scope::Passthrough::from_config(&config))
                    .unwrap_or_default();
                env.retain(|k, _| meerschaum_core::team_scope::may_record(k, &passthrough));
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
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            let mut job = open_job(&ctx, &name, None)?;
            visible(caller.as_deref(), &name, &mut job, &ctx)
                .then(|| job.get_logs())
                .flatten()
        })
        .await
        .flatten()
    }

    async fn stop_time(&self, name: &str) -> Option<DateTime<Utc>> {
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            let mut job = open_job(&ctx, &name, None)?;
            visible(caller.as_deref(), &name, &mut job, &ctx)
                .then(|| job.stop_time().map(utc))
                .flatten()
        })
        .await
        .flatten()
    }

    async fn completion_time(&self, name: &str) -> Option<DateTime<Utc>> {
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            let mut job = open_job(&ctx, &name, None)?;
            if !visible(caller.as_deref(), &name, &mut job, &ctx) {
                return None;
            }
            job.ended().or_else(|| job.stop_time()).map(utc)
        })
        .await
        .flatten()
    }

    async fn is_blocking_on_stdin(&self, name: &str) -> bool {
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            open_job(&ctx, &name, None).is_some_and(|mut job| {
                visible(caller.as_deref(), &name, &mut job, &ctx) && job.is_blocking_on_stdin()
            })
        })
        .await
        .unwrap_or(false)
    }

    async fn prompt_kwargs(&self, name: &str) -> Map {
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || {
            open_job(&ctx, &name, None)
                .and_then(|mut job| {
                    visible(caller.as_deref(), &name, &mut job, &ctx)
                        .then(|| job.get_prompt_kwargs())
                })
                .unwrap_or_default()
        })
        .await
        .unwrap_or_default()
    }

    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
        let caller = meerschaum_server::executor::context::current();
        let (check_ctx, check_name, check_caller) =
            (self.ctx.clone(), name.to_string(), caller.clone());
        if !blocking(move || {
            open_job(&check_ctx, &check_name, None).is_some_and(|mut job| {
                visible(check_caller.as_deref(), &check_name, &mut job, &check_ctx)
            })
        })
        .await
        .unwrap_or(false)
        {
            return None;
        }
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
            if !visible(caller.as_deref(), &name, &mut job, &ctx) {
                return;
            }
            // Asks whether the job still waits while the client has not answered: a job which
            // ended (or was answered by another client) is not waited for.
            let mut probe = open_job(&ctx, &name, None);
            let line_tx = events_tx.clone();
            let stdin_tx = events_tx.clone();
            let stop_tx = events_tx;
            let publication = ResultPublication::for_job(
                &ctx,
                &name,
                &job.daemon().read_properties().unwrap_or_default(),
            );
            let properties_path = job.daemon().paths().properties.clone();
            let result_stop = stop.clone();
            let mut stopped_result = None;
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
                    wait_if_stopped: publication.is_none(),
                    stop: Some(stop),
                    input: Some(Box::new(move || {
                        stdin_tx.blocking_send(LogEvent::StdinRequested).ok()?;
                        let mut last_check = std::time::Instant::now();
                        loop {
                            match input_rx.try_recv() {
                                Ok(text) => return Some(text),
                                Err(mpsc::error::TryRecvError::Disconnected) => return None,
                                Err(mpsc::error::TryRecvError::Empty) => {}
                            }
                            if last_check.elapsed() >= std::time::Duration::from_millis(250) {
                                last_check = std::time::Instant::now();
                                if !probe.as_mut().is_some_and(|j| j.is_blocking_on_stdin()) {
                                    return None;
                                }
                            }
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                    })),
                    on_stop: Some(Box::new(|result: &SuccessTuple| {
                        let Some(publication) = &publication else {
                            stopped_result = Some(result.clone());
                            return;
                        };
                        while !result_stop.load(Ordering::Acquire) {
                            let properties =
                                meerschaum_jobs::properties::read_properties(&properties_path)
                                    .unwrap_or_default();
                            let pending = publication.pending(&ctx, &name, &properties);
                            // The publisher may have exited after its atomic result write.
                            let latest =
                                meerschaum_jobs::properties::read_properties(&properties_path)
                                    .unwrap_or_default();
                            if let Some(result) = publication.result(&latest) {
                                stopped_result = Some(result);
                                break;
                            }
                            if !pending || !publication.matches(&latest) {
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                    })),
                    ..Default::default()
                },
            );
            // monitor_logs reads the final log bytes after its on_stop callback returns.
            // A websocket closes on Stopped, so enqueue it only after that read.
            if let Some(result) = stopped_result {
                let _ = stop_tx.blocking_send(LogEvent::Stopped(result));
            }
        });
        Some(LogMonitor {
            events: events_rx,
            input: input_tx,
        })
    }

    async fn team_jobs(&self, team: &str) -> Vec<String> {
        let (ctx, team) = (self.ctx.clone(), team.to_string());
        blocking(move || meerschaum_jobs::ownership::team_jobs(&ctx, &team))
            .await
            .unwrap_or_default()
    }

    async fn retag_team(&self, old: &str, new: Option<&str>) -> Vec<String> {
        let (ctx, old, new) = (self.ctx.clone(), old.to_string(), new.map(str::to_string));
        blocking(move || {
            let retagged = meerschaum_jobs::ownership::retag_team(&ctx, &old, new.as_deref());
            for (job, why) in &retagged.failed {
                tracing::warn!("Could not move job '{job}' off team '{old}': {why}");
            }
            retagged.jobs
        })
        .await
        .unwrap_or_default()
    }
}

impl LocalJobsBackend {
    async fn run(
        &self,
        name: &str,
        f: impl FnOnce(&mut Job) -> SuccessTuple + Send + 'static,
    ) -> SuccessTuple {
        let caller = meerschaum_server::executor::context::current();
        let (ctx, name) = (self.ctx.clone(), name.to_string());
        blocking(move || match open_job(&ctx, &name, None) {
            Some(mut job) => {
                if visible(caller.as_deref(), &name, &mut job, &ctx) {
                    f(&mut job)
                } else {
                    SuccessTuple::fail(format!("Invalid job name '{name}'."))
                }
            }
            None => SuccessTuple::fail(format!("Invalid job name '{name}'.")),
        })
        .await
        .unwrap_or_else(|| SuccessTuple::fail("The job request was interrupted."))
    }
}

#[cfg(test)]
mod factory_log_tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn factory_log_monitor_emits_the_published_tuple_and_closes_when_its_supervisor_dies() {
        #[derive(Debug)]
        struct Factory;
        impl meerschaum_jobs::daemon::LaunchEnvironment for Factory {
            fn authoritative(&self, _: &str, _: &Map) -> bool {
                false
            }
            fn tracks_unattributed_publication(&self) -> bool {
                true
            }
            fn environment(
                &self,
                _: &str,
                _: &Map,
                _: &[(String, String)],
            ) -> std::io::Result<Vec<(String, String)>> {
                unreachable!("a read-only log monitor cannot launch a job")
            }
        }

        let root = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(root.path())
            .with_process_runner(Arc::new(meerschaum_jobs::FakeSystemctl::new()))
            .with_launch_resolver(Arc::new(Factory));
        let backend = LocalJobsBackend::new(ctx.clone());
        for (name, publishes) in [("published", true), ("dead", false)] {
            let properties = Value::from_json_str(
                r#"{"process":{"launch_id":"first","began":"2026-01-01T00:00:00"},"result":null}"#,
            )
            .unwrap()
            .to_map();
            let mut job = Job::new(
                name,
                Some(vec!["show".into(), "version".into()]),
                JobOptions {
                    executor_keys: Some("local".into()),
                    properties: Some(properties.clone()),
                    ..Default::default()
                },
                &ctx,
            )
            .unwrap();
            assert!(job.daemon().write_properties().success);
            let paths = job.daemon().paths().clone();
            std::fs::create_dir_all(&ctx.paths.logs_dir).unwrap();
            job.daemon()
                .rotating_log()
                .unwrap()
                .write_raw(b"worker exited\n")
                .unwrap();
            // Hold the kernel lock just as the native supervisor does after worker exit.
            let publication = paths.dir.join(".factory-publication");
            std::fs::write(&publication, "first").unwrap();
            let supervisor = std::fs::File::options()
                .write(true)
                .open(publication)
                .unwrap();
            supervisor.try_lock().unwrap();
            let pending = backend.try_get(name).await.unwrap().unwrap();
            assert_eq!(pending["completion_pending"], true);
            let mut monitor = backend.monitor_logs(name).await.unwrap();
            let line =
                tokio::time::timeout(std::time::Duration::from_secs(5), monitor.events.recv())
                    .await
                    .unwrap();
            assert!(matches!(line, Some(LogEvent::Line(line)) if line.contains("worker exited")));
            if publishes {
                job.daemon()
                    .rotating_log()
                    .unwrap()
                    .write_raw(b"final worker output\n")
                    .unwrap();
                let mut finished = properties.clone();
                finished
                    .get_mut("process")
                    .unwrap()
                    .as_map_mut()
                    .unwrap()
                    .insert("ended".into(), "2026-01-01T00:00:01".into());
                finished.insert(
                    "result".into(),
                    Value::List(vec![false.into(), "worker verdict".into()]),
                );
                assert!(
                    meerschaum_jobs::properties::write_existing_properties(
                        &paths.properties,
                        &finished
                    )
                    .success
                );
            }
            drop(supervisor);
            if publishes {
                let line =
                    tokio::time::timeout(std::time::Duration::from_secs(5), monitor.events.recv())
                        .await
                        .unwrap();
                assert!(
                    matches!(line, Some(LogEvent::Line(line)) if line == "final worker output\n")
                );
            }
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), monitor.events.recv())
                    .await
                    .unwrap();
            if publishes {
                assert!(
                    matches!(event, Some(LogEvent::Stopped(result)) if !result.success && result.message == "worker verdict")
                );
            } else {
                assert!(event.is_none());
                assert_eq!(
                    backend.try_get(name).await.unwrap().unwrap()["completion_unavailable"],
                    true
                );
            }
        }
    }

    /// `GET /jobs` reads every `systemd` job's status, PID and times from one `systemctl show`
    /// (not a few calls per job), and lists exactly what each job's getters read.
    #[tokio::test]
    async fn the_listing_asks_systemctl_once_for_every_unit() {
        use meerschaum_jobs::executor::JobExecutor;
        use meerschaum_jobs::runner::FakeService;
        let root = tempfile::tempdir().unwrap();
        let fake = std::sync::Arc::new(meerschaum_jobs::FakeSystemctl::new());
        let mut ctx = JobsContext::for_root(root.path().join("root"));
        ctx.paths.systemd_user_dir = root.path().join("home/.config/systemd/user");
        ctx.config.executor = Some("systemd".into());
        let ctx = ctx
            .with_launcher(meerschaum_jobs::Launcher {
                supervisor: vec!["unused".into()],
                program: vec!["/usr/bin/python3".into()],
            })
            .with_process_runner(fake.clone())
            .with_process_env(vec![("PATH".into(), "/usr/bin:/bin".into())]);
        let executor = meerschaum_jobs::systemd::SystemdExecutor::new(ctx.clone());
        for (index, (name, state)) in [("one", "active"), ("two", "inactive"), ("three", "active")]
            .into_iter()
            .enumerate()
        {
            let sysargs = vec![
                "show".to_string(),
                "version".to_string(),
                "--loop".to_string(),
            ];
            assert!(executor.create_job(name, &sysargs, None).success);
            fake.set_service(
                &executor.get_service_name(name),
                FakeService {
                    active_state: state.into(),
                    main_pid: if state == "active" {
                        4_200_000 + index as i32
                    } else {
                        0
                    },
                    stopped: name == "three",
                    active_enter: "Tue 2000-01-04 10:00:00 UTC".into(),
                    ..FakeService::default()
                },
            );
        }
        fake.clear_calls();
        let listed = LocalJobsBackend::new(ctx.clone()).list().await;
        let queries: Vec<String> = fake
            .rendered_calls()
            .into_iter()
            .filter(|c| c.starts_with("show") || c.starts_with("is-active"))
            .collect();
        assert_eq!(queries.len(), 1, "{queries:?}");
        let mut expected = IndexMap::new();
        for (name, mut job) in get_jobs_for(&ctx, Some("local"), false, true).unwrap().jobs {
            expected.insert(name, metadata(&mut job, &ctx));
        }
        assert_eq!(listed, expected);
        assert_eq!(listed["one"]["status"], "running");
        assert_eq!(listed["two"]["status"], "stopped");
        assert_eq!(listed["three"]["status"], "paused");
    }

    #[tokio::test]
    async fn promotion_persists_kind_and_preserves_owner() {
        let root = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(root.path());
        let props = Value::from_json_str(r#"{"mrsm_owner":"alice","mrsm_execution":{"kind":"temporary_execution","run_id":"run-1"},"process":{"ended":"2026-10-01T12:00:00+00:00"}}"#).unwrap();
        let mut job = Job::new(
            "execute-retained",
            Some(vec!["show".into(), "version".into()]),
            JobOptions {
                executor_keys: Some("local".into()),
                properties: props.as_map().cloned(),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert!(job.daemon().write_properties().success);
        let backend = LocalJobsBackend::new(ctx);
        assert!(backend.completion_time("execute-retained").await.is_some());
        assert!(backend.stop_time("execute-retained").await.is_none());
        assert!(backend.promote("execute-retained").await.success);
        assert!(backend.promote("execute-retained").await.success);
        let meta = backend.get("execute-retained").await.unwrap();
        assert_eq!(
            meta["daemon"]["properties"]["mrsm_execution"]["kind"],
            "persistent_job"
        );
        assert_eq!(
            meta["daemon"]["properties"]["mrsm_execution"]["run_id"],
            "run-1"
        );
        assert_eq!(meta["daemon"]["properties"]["mrsm_owner"], "alice");
        assert!(!meerschaum_server::routes::execute::is_temporary(
            "execute-retained",
            &meta
        ));
    }

    /// A job the server creates keeps who started it (the job routes stamp `owner` / `team`).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_created_job_keeps_who_started_it() {
        let root = tempfile::tempdir().unwrap();
        // A supervisor that does nothing: the properties are written before it would start.
        let ctx = JobsContext::for_root(root.path()).with_launcher(meerschaum_jobs::Launcher {
            supervisor: vec!["true".into()],
            program: vec!["true".into()],
        });
        let backend = LocalJobsBackend::new(ctx);
        let mut props = Map::new();
        meerschaum_core::job_access::JobAccess::stamp(&mut props, Some("alice"), Some("Data"));
        let sysargs = vec!["show".to_string(), "version".to_string()];
        match backend.create("nightly", sysargs, Some(props)).await {
            CreateJob::Started(started) => assert!(started.success, "{started}"),
            CreateJob::Exists => panic!("the job existed"),
            CreateJob::Unavailable(error) => panic!("{error}"),
        }
        let meta = backend.get("nightly").await.unwrap();
        assert_eq!(meta["daemon"]["properties"]["owner"], "alice");
        assert_eq!(meta["daemon"]["properties"]["team"], "Data");
        assert_eq!(backend.team_jobs("Data").await, ["nightly"]);
        assert_eq!(backend.team_jobs("Everyone").await, Vec::<String>::new());
    }

    /// The jobs of a team, and a team rename or deletion through the server (`/teams` routes).
    #[tokio::test]
    async fn a_team_rename_or_deletion_moves_the_server_s_jobs() {
        let root = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(root.path());
        for (name, props) in [
            ("nightly", r#"{"owner": "alice", "team": "Data"}"#),
            ("hourly", r#"{"owner": "bob", "team": "Ops"}"#),
            ("local", r#"{}"#),
        ] {
            let mut job = Job::new(
                name,
                Some(vec!["show".into(), "version".into()]),
                JobOptions {
                    executor_keys: Some("local".into()),
                    properties: Value::from_json_str(props).unwrap().as_map().cloned(),
                    ..Default::default()
                },
                &ctx,
            )
            .unwrap();
            assert!(job.daemon().write_properties().success);
        }
        let backend = LocalJobsBackend::new(ctx);
        assert_eq!(backend.team_jobs("data").await, ["nightly"]);
        assert_eq!(backend.team_jobs("Everyone").await, ["local"]);
        assert_eq!(
            backend.retag_team("Data", Some("Analytics")).await,
            ["nightly"]
        );
        let meta = backend.get("nightly").await.unwrap();
        assert_eq!(meta["daemon"]["properties"]["team"], "Analytics");
        assert_eq!(meta["daemon"]["properties"]["owner"], "alice");
        assert_eq!(backend.retag_team("Analytics", None).await, ["nightly"]);
        let meta = backend.get("nightly").await.unwrap();
        assert!(meta["daemon"]["properties"]["team"].is_null());
        assert_eq!(backend.team_jobs("Ops").await, ["hourly"]);
    }
}
