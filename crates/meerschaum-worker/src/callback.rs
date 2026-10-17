//! The callback channel: plugin code running in a worker calls back into the host.
//!
//! When plugin code calls `pipe.sync(df)` (a plugin `sync()`, a `chunk_hook`, or a v4-pattern
//! streaming connector's message callback), the worker sends a `callback` message with the data;
//! the host runs the sync natively and answers with a `cb_result`. `subscribe()` sinks send
//! `push` callbacks the same way.

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::{
    InstanceConnector, SyncOptions, async_trait,
    pipe_ops::{bool_param, dtypes_context},
};
use meerschaum_core::dtypes::is_dtype_special;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value as CValue};
use meerschaum_kernel::detect::get_special_cols;
use meerschaum_kernel::enforce::pipe_enforce_dtypes;

use crate::chunk::FetchChunk;

/// A request from plugin code to the host.
#[derive(Debug, Clone)]
pub struct CallbackRequest {
    /// `sync` (a forwarded `pipe.sync()`) or `push` (a `subscribe()` sink batch).
    pub op: String,
    /// The worker call that was running when the callback was made (`0` after the call
    /// returned, e.g. from a streaming connector's background thread).
    pub call_id: u64,
    /// The pipe, with the in-memory parameters plugin code saw.
    pub pipe: Option<Pipe>,
    /// The data; `None` for `pipe.sync()` without a DataFrame (infer fetch).
    pub chunk: Option<FetchChunk>,
    /// The keyword arguments plugin code passed (callables dropped).
    pub kwargs: Map,
}

/// Handles callbacks from a worker.
#[async_trait]
pub trait CallbackHandler: Send + Sync {
    /// Run the request and return the `SuccessTuple` plugin code receives.
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple;
}

/// Resolves instance keys to an instance connector.
pub trait InstanceResolver: Send + Sync {
    /// The connector for `instance_keys`, or `None` if it cannot be built.
    fn resolve(&self, instance_keys: &str) -> Option<Arc<dyn InstanceConnector>>;
}

impl<F> InstanceResolver for F
where
    F: Fn(&str) -> Option<Arc<dyn InstanceConnector>> + Send + Sync,
{
    fn resolve(&self, instance_keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self(instance_keys)
    }
}

/// The native sync path for forwarded `pipe.sync(df)` calls: enforce the pipe's dtypes on the
/// data with the kernel (`Pipe.enforce_dtypes`) and call the instance's `sync_pipe()`, which
/// diffs against existing rows (`check_existing`, default `True`).
///
/// This is the minimal `Pipe.sync()` the adapter needs until the engine's `SyncExecutor`
/// (§5.5) owns the whole flow; the engine can supply its own [`CallbackHandler`] instead.
pub struct InstanceSyncHandler {
    resolver: Box<dyn InstanceResolver>,
}

impl InstanceSyncHandler {
    /// A handler resolving instances with `resolver`.
    pub fn new(resolver: impl InstanceResolver + 'static) -> InstanceSyncHandler {
        InstanceSyncHandler {
            resolver: Box::new(resolver),
        }
    }

    /// A handler that syncs every pipe into `instance`, whatever its instance keys.
    pub fn single(instance: Arc<dyn InstanceConnector>) -> InstanceSyncHandler {
        InstanceSyncHandler::new(move |_: &str| Some(Arc::clone(&instance)))
    }
}

/// Sync one chunk into `instance` natively: register the pipe if needed, enforce dtypes,
/// persist newly detected special columns (`_persist_new_special_columns`), then `sync_pipe()`.
///
/// This covers the parts of `Pipe.sync()` that apply to data handed over by plugin code; the
/// engine's native sync (autotime, `filter_existing` observers, the ledger) replaces it when the
/// engine supplies its own [`CallbackHandler`].
pub async fn sync_chunk_native(
    instance: &dyn InstanceConnector,
    pipe: &Pipe,
    chunk: FetchChunk,
    kwargs: &Map,
) -> SuccessTuple {
    let mut pipe = pipe.clone();
    let registered = match instance.get_pipe_id(&pipe).await {
        Ok(Some(_)) => true,
        Ok(None) => {
            let res = instance.register_pipe(&pipe).await;
            if !res.success && !res.message.contains("already") {
                return res;
            }
            true
        }
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    if pipe.raw_parameters().is_empty() {
        // `Pipe.parameters` reads the registered parameters unless set in memory.
        if let Ok(attrs) = instance.get_pipe_attributes(&pipe).await {
            if let Some(CValue::Map(params)) = attrs.get("parameters") {
                pipe.set_parameters(params.clone());
            }
        }
    }
    let params = pipe.parameters().clone();
    let ctx = match dtypes_context(instance, &pipe, &params).await {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let input = match chunk.into_pipe_input() {
        Ok(i) => i,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let df = match pipe_enforce_dtypes(input, &ctx) {
        Ok(df) => df,
        Err(e) => {
            return SuccessTuple::fail(format!(
                "Unable to cast incoming data as a DataFrame...:\n{e}"
            ));
        }
    };

    // `_persist_new_special_columns` (core/Pipe/_sync.py).
    let new_special: IndexMap<String, String> = get_special_cols(&df)
        .into_iter()
        .filter(|(c, _)| !ctx.dtypes.get(c).is_some_and(|t| is_dtype_special(t)))
        .collect();
    let mut opts = SyncOptions {
        check_existing: bool_param(kwargs, "check_existing", true),
        ..Default::default()
    };
    if !new_special.is_empty() {
        let mut dtypes: Map = pipe
            .param("dtypes")
            .and_then(|d| d.as_map().cloned())
            .unwrap_or_default();
        for (c, t) in &new_special {
            dtypes.insert(c.clone(), CValue::Str(t.clone()));
        }
        let mut patch = Map::new();
        patch.insert("dtypes".into(), CValue::Map(dtypes));
        pipe.update_parameters(patch);
        if registered && !pipe.temporary {
            let res = instance.edit_pipe(&pipe, false).await;
            if !res.success {
                tracing::warn!(
                    "Failed to capture new special columns for {pipe}:\n{}",
                    res.message
                );
            }
        }
        opts.extra.insert(
            "new_special_cols".into(),
            CValue::Map(
                new_special
                    .iter()
                    .map(|(c, t)| (c.clone(), CValue::Str(t.clone())))
                    .collect(),
            ),
        );
    }
    instance.sync_pipe(&pipe, df, &opts).await
}

#[async_trait]
impl CallbackHandler for InstanceSyncHandler {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let Some(pipe) = request.pipe else {
            return SuccessTuple::fail("No pipe was provided to sync.");
        };
        let Some(chunk) = request.chunk else {
            return SuccessTuple::fail(format!(
                "Cannot infer fetching for {pipe} from inside a plugin's `sync()`."
            ));
        };
        let Some(instance) = self.resolver.resolve(pipe.instance_keys()) else {
            return SuccessTuple::fail(format!(
                "Could not build the instance connector '{}'.",
                pipe.instance_keys()
            ));
        };
        sync_chunk_native(instance.as_ref(), &pipe, chunk, &request.kwargs).await
    }
}

/// A handler that refuses every callback (used when the caller expects none).
pub struct NoCallbacks;

#[async_trait]
impl CallbackHandler for NoCallbacks {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        SuccessTuple::fail(format!(
            "The host does not accept `{}` callbacks for this call.",
            request.op
        ))
    }
}
