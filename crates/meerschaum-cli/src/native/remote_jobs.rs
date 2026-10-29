//! The host side of the `api:<label>` job executor: [`RemoteJobsResolver`] over
//! `meerschaum_connector_api::APIConnector`.
//!
//! `meerschaum-jobs` owns the trait boundary (`crates/README.md`, "Executor / client injection");
//! this module implements it for the CLI. The [`Session`]'s Tokio runtime is the one the
//! blocking trait methods wait on, so there is one runtime per session and none per call. The
//! log follower runs on that runtime and hands lines to the calling thread through a channel,
//! because the callbacks of [`RemoteLogSink`] are not `Send`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::LogsMonitor;
use meerschaum_core::SuccessTuple;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_jobs::remote::{RemoteJobs, RemoteJobsResolver, RemoteLogSink, RemoteMonitor};

use super::Session;

/// What the server sends as the first websocket message when the token is not accepted.
const INVALID_CREDENTIALS: &str = "Invalid credentials.";

/// One remote jobs endpoint (`api:<label>`).
pub struct CliRemoteJobs {
    keys: String,
    api: Arc<APIConnector>,
    session: Session,
}

impl std::fmt::Debug for CliRemoteJobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CliRemoteJobs")
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

impl CliRemoteJobs {
    /// A client for `keys` over a connector.
    pub fn new(keys: &str, api: APIConnector, session: Session) -> CliRemoteJobs {
        CliRemoteJobs {
            keys: keys.to_string(),
            api: Arc::new(api),
            session,
        }
    }

    fn mutate(&self, fut: impl std::future::Future<Output = SuccessTuple>) -> SuccessTuple {
        let result = self.session.block_on(fut);
        // The metadata cache (5 s in v4) would otherwise answer the next status read with the
        // state from before this request.
        self.api.clear_job_metadata_cache();
        result
    }
}

fn text(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl RemoteJobs for CliRemoteJobs {
    fn keys(&self) -> String {
        self.keys.clone()
    }

    fn get_jobs(&self) -> Result<IndexMap<String, Map>, String> {
        self.session.block_on(self.api.try_get_jobs()).map_err(text)
    }

    fn get_job_metadata(&self, name: &str) -> Result<Option<Map>, String> {
        self.session
            .block_on(self.api.try_get_job_metadata(name))
            .map_err(text)
    }

    fn get_job_exists(&self, name: &str) -> Result<bool, String> {
        self.session
            .block_on(self.api.try_get_job_exists(name))
            .map_err(text)
    }

    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        self.mutate(self.api.create_job(name, sysargs, properties.cloned()))
    }

    fn start_job(&self, name: &str) -> SuccessTuple {
        self.mutate(self.api.start_job(name))
    }

    fn stop_job(&self, name: &str) -> SuccessTuple {
        self.mutate(self.api.stop_job(name))
    }

    fn pause_job(&self, name: &str) -> SuccessTuple {
        self.mutate(self.api.pause_job(name))
    }

    fn delete_job(&self, name: &str) -> SuccessTuple {
        self.mutate(self.api.delete_job(name))
    }

    fn get_logs(&self, name: &str) -> Result<String, String> {
        self.session.block_on(self.api.get_logs(name)).map_err(text)
    }

    fn get_job_stop_time(&self, name: &str) -> Result<Option<String>, String> {
        self.session
            .block_on(self.api.try_get_job_stop_time(name))
            .map_err(text)
    }

    fn get_job_is_blocking_on_stdin(&self, name: &str) -> Result<bool, String> {
        self.session
            .block_on(self.api.try_get_job_is_blocking_on_stdin(name))
            .map_err(text)
    }

    fn get_job_prompt_kwargs(&self, name: &str) -> Result<Map, String> {
        self.session
            .block_on(self.api.try_get_job_prompt_kwargs(name))
            .map_err(text)
    }

    fn monitor_logs(
        &self,
        name: &str,
        sink: &mut dyn RemoteLogSink,
        opts: RemoteMonitor,
    ) -> Result<(), String> {
        enum Event {
            Line(String),
            NeedInput,
            Stopped(SuccessTuple),
            Done(Result<(), String>),
        }
        let (tx, rx) = channel::<Event>();
        let (input_tx, input_rx) = channel::<Option<String>>();
        let cancelled = Arc::new(AtomicBool::new(false));

        let api = self.api.clone();
        let task_name = name.to_string();
        let handle = self.session.handle().spawn({
            let tx = tx.clone();
            let cancelled = cancelled.clone();
            async move {
                let line_tx = tx.clone();
                let stop_tx = tx.clone();
                let input_request = tx.clone();
                let monitor = LogsMonitor {
                    on_line: Box::new(move |line| {
                        let _ = line_tx.send(Event::Line(line));
                    }),
                    on_input: opts.accept_input.then(|| {
                        Box::new(move || {
                            let _ = input_request.send(Event::NeedInput);
                            // Blocks this task's worker thread until the caller answers (the
                            // runtime is multi-threaded); `None` / a closed channel sends nothing.
                            input_rx.recv().ok().flatten().unwrap_or_default()
                        }) as Box<dyn FnMut() -> String + Send>
                    }),
                    on_stop: Some(Box::new(move |result| {
                        let _ = stop_tx.send(Event::Stopped(result));
                    })),
                    stop_on_exit: opts.stop_on_exit,
                    strip_timestamps: opts.strip_timestamps,
                };
                let result = api.monitor_logs(&task_name, monitor).await.map_err(text);
                if !cancelled.load(Ordering::SeqCst) {
                    let _ = tx.send(Event::Done(result));
                }
            }
        });
        drop(tx);

        let mut received_any = false;
        let outcome = loop {
            if sink.should_stop() {
                break Ok(());
            }
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(Event::Line(line)) => {
                    if !received_any && line.trim_end() == INVALID_CREDENTIALS {
                        break Err(format!(
                            "Authentication failed monitoring job '{name}' on '{}': the server \
                             did not accept the credentials.",
                            self.keys
                        ));
                    }
                    received_any = true;
                    if !sink.on_line(&line) {
                        break Ok(());
                    }
                }
                Ok(Event::NeedInput) => {
                    let _ = input_tx.send(sink.on_input());
                }
                Ok(Event::Stopped(result)) => {
                    sink.on_stop(result);
                    if opts.stop_on_exit {
                        break Ok(());
                    }
                }
                Ok(Event::Done(result)) => break result,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break Ok(()),
            }
        };
        cancelled.store(true, Ordering::SeqCst);
        // Unblock a pending input request before aborting the task.
        drop(input_tx);
        handle.abort();
        outcome
    }
}

/// Resolves `api:<label>` keys over the `meerschaum:connectors:api` configuration, building each
/// client once per resolver (so its login token and metadata cache are reused).
pub struct CliRemoteResolver {
    session: Session,
    api_section: Map,
    clients: Mutex<HashMap<String, Arc<CliRemoteJobs>>>,
}

impl std::fmt::Debug for CliRemoteResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CliRemoteResolver").finish_non_exhaustive()
    }
}

impl CliRemoteResolver {
    /// A resolver over the config's `meerschaum:connectors:api` section.
    pub fn new(session: Session, api_section: Map) -> CliRemoteResolver {
        CliRemoteResolver {
            session,
            api_section,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// The `meerschaum:connectors:api` section of a config.
    pub fn section(config: &meerschaum_core::Config) -> Map {
        config
            .get(&["meerschaum", "connectors", "api"])
            .map(|v| v.to_map())
            .unwrap_or_default()
    }
}

impl RemoteJobsResolver for CliRemoteResolver {
    fn resolve(&self, executor_keys: &str) -> Result<Arc<dyn RemoteJobs>, String> {
        let Some(label) = executor_keys.strip_prefix("api:") else {
            return Err(format!(
                "Invalid executor keys '{executor_keys}'. Use 'local', 'systemd' or 'api:<label>'."
            ));
        };
        let mut clients = self.clients.lock().map_err(|e| e.to_string())?;
        if let Some(client) = clients.get(executor_keys) {
            return Ok(client.clone() as Arc<dyn RemoteJobs>);
        }
        let section = |key: &str| {
            self.api_section
                .get(key)
                .map(Value::to_map)
                .unwrap_or_default()
        };
        let attributes = apply_patch_maps(&section("default"), &section(label));
        let api = APIConnector::new(Some(label), attributes)
            .map_err(|e| format!("Cannot build the connector for '{executor_keys}': {e}"))?;
        let client = Arc::new(CliRemoteJobs::new(executor_keys, api, self.session.clone()));
        clients.insert(executor_keys.to_string(), client.clone());
        Ok(client as Arc<dyn RemoteJobs>)
    }
}
