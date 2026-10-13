//! Worker adapters for the native engine. Hosts share one pool and a startup manifest.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use meerschaum_connector::{Connector, DateBound, InstanceConnector, async_trait};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_worker::plugins::{HookPhase, SyncHookContext, run_sync_hooks};
use meerschaum_worker::{
    CallbackHandler, CallbackRequest, ConnectorInfo, Manifest, PythonConnector, PythonStream,
    StreamEvent, StreamMode, StreamOptions, WorkerPool,
};
use tokio::sync::oneshot;

use crate::stream::{AckToken, LegacyStream, StopToken, StreamMessage, StreamSender, StreamSource};
use crate::{
    ConnectorRegistry, Engine, EngineConfig, PluginSync, SyncArgs, SyncBegin, SyncHooks, SyncResult,
};

/// Adds manifest-declared Python connectors and hooks to a host's native registry.
/// A weak callback reference avoids a registry / connector ownership cycle.
pub struct PythonRegistry {
    native: Arc<dyn ConnectorRegistry>,
    pool: WorkerPool,
    manifest: Arc<Manifest>,
    config: EngineConfig,
    callbacks: Arc<NativeCallbacks>,
    legacy_streams: AtomicBool,
    stream_kwargs: RwLock<Map>,
}

impl PythonRegistry {
    /// Wrap native connectors with the process's shared worker pool.
    pub fn new(
        native: Arc<dyn ConnectorRegistry>,
        pool: WorkerPool,
        manifest: Arc<Manifest>,
        config: EngineConfig,
    ) -> Arc<Self> {
        Arc::new_cyclic(|registry| Self {
            native,
            pool,
            manifest,
            config,
            legacy_streams: AtomicBool::new(false),
            stream_kwargs: RwLock::new(Map::new()),
            callbacks: Arc::new(NativeCallbacks {
                registry: registry.clone(),
            }),
        })
    }

    /// Enable the v4 subscribe-and-return `sync()` pattern for `--listen`.
    /// Ordinary sync overrides stay batch sources unless listening was requested.
    pub fn enable_legacy_streams(&self) {
        self.legacy_streams.store(true, Ordering::Relaxed);
    }

    /// Set CLI / plugin keyword arguments for subscriptions created by this registry.
    pub fn set_stream_kwargs(&self, kwargs: Map) {
        *self
            .stream_kwargs
            .write()
            .unwrap_or_else(|e| e.into_inner()) = kwargs;
    }

    /// Native callback handler for plugin actions and other calls made by the host.
    pub fn callbacks(&self) -> Arc<dyn CallbackHandler> {
        self.callbacks.clone()
    }

    fn info(&self, keys: &str) -> Option<ConnectorInfo> {
        let (kind, label) = keys.split_once(':')?;
        let mut info = ConnectorInfo {
            connector_type: kind.into(),
            label: label.into(),
            ..Default::default()
        };
        if kind == "plugin" {
            let plugin = self.manifest.plugins.get(label)?;
            info.methods = plugin.data_functions.clone();
            info.is_streaming = info.methods.iter().any(|m| m == "subscribe");
        } else {
            let custom = self.manifest.connector_type(kind)?;
            info.methods = custom.methods.clone();
            info.class_name = custom.class_name.clone();
            info.is_instance = custom.is_instance;
            info.is_thread_safe = custom.is_thread_safe;
            info.is_streaming = custom.is_streaming;
        }
        info.has_fetch = info.has_method("fetch");
        info.has_sync = info.has_method("sync");
        info.has_register = info.has_method("register");
        info.has_subscribe = info.has_method("subscribe");
        Some(info)
    }

    fn connector(&self, keys: &str) -> Option<PythonConnector> {
        Some(
            PythonConnector::with_info(keys, self.pool.clone(), self.info(keys)?)
                .with_callbacks(self.callbacks()),
        )
    }

    fn stream(&self, keys: &str) -> PythonSource {
        PythonSource {
            pool: self.pool.clone(),
            keys: keys.into(),
            kwargs: self
                .stream_kwargs
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            callbacks: self.callbacks(),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl ConnectorRegistry for PythonRegistry {
    fn fetch_chunksize(&self, keys: &str) -> usize {
        self.native.fetch_chunksize(keys)
    }

    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        if self.info(keys).is_some_and(|i| i.is_instance) {
            return Ok(Arc::new(self.connector(keys).expect("manifest connector")));
        }
        self.native.instance(keys)
    }
    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        if let Some(c) = self.connector(keys) {
            return Ok(Some(Arc::new(c)));
        }
        self.native.source(keys)
    }
    fn source_for_sync(
        &self,
        keys: &str,
        args: &SyncArgs,
    ) -> Result<Option<Arc<dyn Connector>>, String> {
        if let Some(connector) = self.connector(keys) {
            return Ok(Some(Arc::new(connector.with_fetch_kwargs(kwargs(args)))));
        }
        self.native.source_for_sync(keys, args)
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        if self.info(keys).is_some_and(|i| i.has_sync) {
            return Some(Arc::new(PythonSync(self.connector(keys)?)));
        }
        self.native.plugin_sync(keys)
    }
    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
        if self.info(keys).is_some_and(|i| i.has_subscribe) {
            return Some(Arc::new(self.stream(keys)));
        }
        self.native.stream_source(keys)
    }
    fn legacy_stream(&self, keys: &str) -> Option<Arc<dyn LegacyStream>> {
        if self.info(keys).is_some_and(|i| {
            i.has_sync
                && !i.has_subscribe
                && (i.is_streaming || self.legacy_streams.load(Ordering::Relaxed))
        }) {
            return Some(Arc::new(self.stream(keys)));
        }
        self.native.legacy_stream(keys)
    }
    fn is_python(&self, keys: &str) -> bool {
        self.info(keys).is_some() || self.native.is_python(keys)
    }
    fn hooks(&self) -> Option<Arc<dyn SyncHooks>> {
        if self.manifest.has_sync_hooks() {
            Some(Arc::new(PythonHooks {
                pool: self.pool.clone(),
                callbacks: self.callbacks(),
                native: self.native.hooks(),
            }))
        } else {
            self.native.hooks()
        }
    }
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        if self.info(pipe.instance_keys()).is_some() {
            pipe.target(None).map_err(|e| e.to_string())
        } else {
            self.native.table_reference(pipe)
        }
    }
}

fn bound_value(bound: &DateBound) -> Value {
    match bound {
        DateBound::Int(i) => Value::Int(*i),
        DateBound::Datetime(_) => Value::Str(bound.isoformat()),
    }
}

fn kwargs(args: &SyncArgs) -> Map {
    let mut map = args.extra.clone();
    map.insert(
        "begin".into(),
        match &args.begin {
            SyncBegin::Default => Value::Str(String::new()),
            SyncBegin::Unbounded => Value::Null,
            SyncBegin::At(b) => bound_value(b),
        },
    );
    map.insert(
        "end".into(),
        args.end.as_ref().map(bound_value).unwrap_or(Value::Null),
    );
    map.insert(
        "params".into(),
        args.params.clone().map(Value::Map).unwrap_or(Value::Null),
    );
    map.insert("check_existing".into(), Value::Bool(args.check_existing));
    map.insert("enforce".into(), Value::Bool(args.enforce_dtypes));
    map.insert("force".into(), Value::Bool(args.force));
    if args.workers.is_some() || !map.contains_key("workers") {
        map.insert(
            "workers".into(),
            args.workers.map(Value::Int).unwrap_or(Value::Null),
        );
    }
    map.insert(
        "chunksize".into(),
        args.chunksize.map(Value::Int).unwrap_or(Value::Null),
    );
    map.insert("retries".into(), Value::Int(args.retries.into()));
    map.insert("min_seconds".into(), Value::Float(args.min_seconds));
    map
}

struct PythonSync(PythonConnector);
type CallbackReply = (CallbackRequest, oneshot::Sender<SuccessTuple>);
struct ScopedCallbacks(tokio::sync::mpsc::UnboundedSender<CallbackReply>);
#[async_trait]
impl CallbackHandler for ScopedCallbacks {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let (tx, rx) = oneshot::channel();
        if self.0.send((request, tx)).is_err() {
            return SuccessTuple::fail("The enclosing sync has stopped.");
        }
        rx.await
            .unwrap_or_else(|_| SuccessTuple::fail("The enclosing sync has stopped."))
    }
}

#[async_trait]
impl PluginSync for PythonSync {
    async fn sync_native(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        env: &crate::sync::SyncEnv<'_>,
        observer: Option<&dyn crate::sync::DeltaObserver>,
    ) -> SyncResult {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CallbackReply>();
        let connector = self.0.clone().with_callbacks(Arc::new(ScopedCallbacks(tx)));
        let kw = kwargs(args);
        let call = connector.sync(pipe, &kw);
        tokio::pin!(call);
        let mut totals = SyncResult::new(true, "");
        loop {
            tokio::select! {
                result = &mut call => {
                    let status = result.unwrap_or_else(|e| SuccessTuple::fail(e.to_string()));
                    totals.success = status.success;
                    totals.message = status.message;
                    return totals;
                },
                request = rx.recv() => {
                    let Some((request, reply)) = request else { continue };
                    let result = if let Some(callback_pipe) = request.pipe {
                        match request.chunk.map(|c| c.into_pipe_input()).transpose() {
                            Ok(input) => {
                                let same_pipe = crate::PipeRef::from_pipe(pipe) == crate::PipeRef::from_pipe(&callback_pipe);
                                let callback_observer = if same_pipe { observer } else { None };
                                let result = meerschaum_worker::pool::callback_scope(crate::sync::sync_pipe(env, &callback_pipe, input, &callback_args(&request.kwargs), callback_observer)).await;
                                if same_pipe {
                                    totals.inserted += result.inserted;
                                    totals.updated += result.updated;
                                    totals.upserted += result.upserted;
                                    totals.deleted += result.deleted;
                                }
                                result.into()
                            },
                            Err(e) => SuccessTuple::fail(e.to_string()),
                        }
                    } else { SuccessTuple::fail("No pipe was provided to sync.") };
                    let _ = reply.send(result);
                }
            }
        }
    }
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> SyncResult {
        self.0
            .sync(pipe, &kwargs(args))
            .await
            .unwrap_or_else(|e| SuccessTuple::fail(e.to_string()))
            .into()
    }
}

struct NativeCallbacks {
    registry: Weak<PythonRegistry>,
}
#[async_trait]
impl CallbackHandler for NativeCallbacks {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let Some(registry) = self.registry.upgrade() else {
            return SuccessTuple::fail("The engine has stopped.");
        };
        let Some(pipe) = request.pipe else {
            return SuccessTuple::fail("No pipe was provided to sync.");
        };
        let input = match request.chunk.map(|c| c.into_pipe_input()).transpose() {
            Ok(input) => input,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let args = callback_args(&request.kwargs);
        let config = registry.config.clone();
        Engine::new(registry, config)
            .sync_pipe(&pipe, input, &args)
            .await
            .into()
    }
}

fn callback_args(kw: &Map) -> SyncArgs {
    let mut args = SyncArgs {
        extra: kw.clone(),
        ..Default::default()
    };
    let bound = |v: &Value| DateBound::parse(&v.py_str());
    if let Some(v) = kw.get("begin") {
        args.begin = match v {
            Value::Null => SyncBegin::Unbounded,
            Value::Str(s) if s.is_empty() => SyncBegin::Default,
            _ => bound(v).map(SyncBegin::At).unwrap_or_default(),
        };
    }
    args.end = kw.get("end").and_then(bound);
    args.params = kw.get("params").and_then(Value::as_map).cloned();
    if let Some(v) = kw.get("check_existing").and_then(Value::as_bool) {
        args.check_existing = v;
    }
    if let Some(v) = kw.get("enforce").and_then(Value::as_bool) {
        args.enforce_dtypes = v;
    }
    if let Some(v) = kw.get("force").and_then(Value::as_bool) {
        args.force = v;
    }
    if let Some(v) = kw.get("workers") {
        args.workers = v.as_i64();
    }
    if let Some(v) = kw.get("chunksize") {
        args.chunksize = v.as_i64();
    }
    if let Some(v) = kw.get("retries").and_then(Value::as_i64) {
        args.retries = v.max(0) as u32;
    }
    if let Some(v) = kw.get("min_seconds").and_then(Value::as_f64) {
        args.min_seconds = v;
    }
    args
}

struct PythonHooks {
    pool: WorkerPool,
    callbacks: Arc<dyn CallbackHandler>,
    native: Option<Arc<dyn SyncHooks>>,
}
impl PythonHooks {
    async fn run(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        phase: HookPhase,
        result: Option<&SyncResult>,
    ) {
        let context = SyncHookContext {
            sync_timestamp: Some(args.sync_timestamp),
            success_tuple: result.cloned().map(Into::into),
            sync_duration: result.and_then(|r| r.duration).map(|d| d.as_secs_f64()),
            sync_complete_timestamp: result.map(|_| chrono::Utc::now()),
            extra: kwargs(args),
        };
        match run_sync_hooks(
            &self.pool,
            pipe,
            phase,
            &context,
            None,
            Some(self.callbacks.clone()),
        )
        .await
        {
            Ok(results) => {
                for hook in results {
                    if !hook.result.success {
                        tracing::warn!("{}: {}", hook.hook, hook.result.message);
                    }
                }
            }
            Err(e) => tracing::warn!("Sync hook failed: {e}"),
        }
    }
}
#[async_trait]
impl SyncHooks for PythonHooks {
    async fn pre_sync(&self, pipe: &Pipe, args: &SyncArgs) {
        if let Some(h) = &self.native {
            h.pre_sync(pipe, args).await;
        }
        self.run(pipe, args, HookPhase::Pre, None).await;
    }
    async fn post_sync(&self, pipe: &Pipe, args: &SyncArgs, result: &SyncResult) {
        if let Some(h) = &self.native {
            h.post_sync(pipe, args, result).await;
        }
        self.run(pipe, args, HookPhase::Post, Some(result)).await;
    }
}

type Pending = Arc<Mutex<HashMap<AckToken, oneshot::Sender<()>>>>;
struct PythonSource {
    pool: WorkerPool,
    keys: String,
    kwargs: Map,
    callbacks: Arc<dyn CallbackHandler>,
    pending: Pending,
    next: Arc<AtomicU64>,
}
struct PushCallbacks {
    native: Arc<dyn CallbackHandler>,
    sink: StreamSender,
    pending: Pending,
    next: Arc<AtomicU64>,
    stop: StopToken,
}
#[async_trait]
impl CallbackHandler for PushCallbacks {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        if request.op != "push" {
            return self.native.handle(request).await;
        }
        let Some(chunk) = request.chunk else {
            return SuccessTuple::fail("A stream push requires data.");
        };
        let frame = match chunk.into_frame() {
            Ok(f) => f,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let bytes = match meerschaum_worker::protocol::frame_to_ipc(&frame) {
            Ok(ipc) => ipc.len() as u64,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let docs = (0..frame.height())
            .map(|row| {
                frame
                    .columns()
                    .iter()
                    .map(|c| (c.name.clone(), c.data.get(row)))
                    .collect()
            })
            .collect();
        let token = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(token, tx);
        let message = StreamMessage {
            docs,
            bytes,
            ack: Some(token),
            redelivered: false,
        };
        let result = tokio::select! {
            _ = self.stop.stopped() => SuccessTuple::fail("Stream stopped before commit."),
            result = async {
                self.sink.send(message).await.map_err(|_| "Stream sink closed.")?;
                rx.await.map_err(|_| "Stream stopped before commit.")
            } => match result { Ok(()) => SuccessTuple::success(), Err(e) => SuccessTuple::fail(e) }
        };
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&token);
        result
    }
}
impl PythonSource {
    async fn run_stream(
        &self,
        pipe: &Pipe,
        stop: StopToken,
        mode: StreamMode,
        callbacks: Arc<dyn CallbackHandler>,
    ) -> Result<(), String> {
        // The engine owns restart policy; avoid nesting two independent supervisors.
        let (stream, mut events) = PythonStream::start(
            self.pool.clone(),
            &self.keys,
            pipe,
            &self.kwargs,
            mode,
            callbacks,
            StreamOptions {
                restart: false,
                ..Default::default()
            },
        );
        let result = loop {
            tokio::select! {
                _ = stop.stopped() => break Ok(()),
                event = events.recv() => match event {
                    Some(StreamEvent::Exited { error }) => break Err(error),
                    Some(StreamEvent::Returned(result)) if !result.success => break Err(result.message),
                    Some(StreamEvent::Stopped) | None => break Err("Python stream stopped.".into()),
                    _ => {}
                }
            }
        };
        stream.stop().await;
        result
    }
}
#[async_trait]
impl LegacyStream for PythonSource {
    async fn run(&self, pipe: &Pipe, stop: StopToken) -> Result<(), String> {
        self.run_stream(pipe, stop, StreamMode::V4Sync, self.callbacks.clone())
            .await
    }
}
#[async_trait]
impl StreamSource for PythonSource {
    async fn subscribe(
        &self,
        pipe: &Pipe,
        sink: StreamSender,
        stop: StopToken,
    ) -> Result<(), String> {
        let callbacks = Arc::new(PushCallbacks {
            native: self.callbacks.clone(),
            sink,
            pending: self.pending.clone(),
            next: self.next.clone(),
            stop: stop.clone(),
        });
        self.run_stream(pipe, stop, StreamMode::Subscribe, callbacks)
            .await
    }
    async fn ack(&self, tokens: Vec<AckToken>) -> Result<(), String> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        for token in tokens {
            if let Some(tx) = pending.remove(&token) {
                let _ = tx.send(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_worker::{FetchChunk, callback::NoCallbacks};

    #[test]
    fn sync_kwargs_round_trip_bounds_and_controls() {
        let original = SyncArgs {
            begin: SyncBegin::At(DateBound::Int(100)),
            end: Some(DateBound::Int(200)),
            check_existing: false,
            enforce_dtypes: false,
            force: true,
            workers: Some(2),
            chunksize: None,
            retries: 3,
            min_seconds: 0.25,
            ..Default::default()
        };
        let parsed = callback_args(&kwargs(&original));
        assert_eq!(parsed.begin, original.begin);
        assert_eq!(parsed.end, original.end);
        assert!(!parsed.check_existing);
        assert!(!parsed.enforce_dtypes);
        assert!(parsed.force);
        assert_eq!(parsed.workers, Some(2));
        assert_eq!(parsed.chunksize, None);
        assert_eq!(parsed.retries, 3);
        assert_eq!(parsed.min_seconds, 0.25);
    }

    #[tokio::test]
    async fn subscribe_callback_waits_for_commit_ack() {
        let (sink, mut messages) = tokio::sync::mpsc::channel(1);
        let pending: Pending = Default::default();
        let callbacks = Arc::new(PushCallbacks {
            native: Arc::new(NoCallbacks),
            sink,
            pending: pending.clone(),
            next: Arc::new(AtomicU64::new(1)),
            stop: StopToken::new(),
        });
        let request = CallbackRequest {
            op: "push".into(),
            call_id: 1,
            pipe: None,
            chunk: Some(FetchChunk::Docs(vec![
                [("id".into(), meerschaum_kernel::value::Value::Int(1))]
                    .into_iter()
                    .collect(),
            ])),
            kwargs: Map::new(),
        };
        let task = tokio::spawn(async move { callbacks.handle(request).await });
        let message = messages.recv().await.unwrap();
        assert_eq!(message.docs.len(), 1);
        assert!(
            !task.is_finished(),
            "push must not report success before commit"
        );
        pending
            .lock()
            .unwrap()
            .remove(&message.ack.unwrap())
            .unwrap()
            .send(())
            .unwrap();
        assert!(task.await.unwrap().success);
        assert!(pending.lock().unwrap().is_empty());
    }
}
