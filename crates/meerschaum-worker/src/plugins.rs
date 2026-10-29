//! Plugin-level operations that are not connector methods: plugin actions, sync hooks and
//! `setup()`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_kernel::value::DateTimeValue;
use rmpv::Value as Wire;

use crate::callback::CallbackHandler;
use crate::error::WorkerResult;
use crate::pool::WorkerPool;
use crate::protocol::{Msg, core_to_wire, ext, map_to_wire};
use crate::worker::CallOptions;

fn success_from(v: &Wire) -> SuccessTuple {
    match v {
        Wire::Array(items) if items.len() == 2 => SuccessTuple {
            success: items[0].as_bool().unwrap_or(false),
            message: items[1].as_str().unwrap_or_default().to_string(),
        },
        Wire::Boolean(true) => SuccessTuple::success(),
        Wire::Boolean(false) => SuccessTuple::fail("Failed"),
        other => SuccessTuple::fail(format!("Invalid SuccessTuple from Python: {other}")),
    }
}

/// How to invoke a plugin action.
#[derive(Debug, Clone)]
pub enum ActionRequest {
    /// Parse these sysargs with v4's `parse_arguments` (plugin arguments included).
    Sysargs(Vec<String>),
    /// Use these already-parsed keyword arguments (`action` is required).
    Kwargs(Map),
}

/// Run a plugin action in a worker exactly as v4's `entry_with_args()` does (the plugin's venv,
/// `filter_keywords`, the same failure messages). Core actions are refused.
pub async fn run_action(
    pool: &WorkerPool,
    request: &ActionRequest,
    handler: Option<Arc<dyn CallbackHandler>>,
    opts: CallOptions,
) -> WorkerResult<SuccessTuple> {
    let args = match request {
        ActionRequest::Sysargs(sysargs) => Msg::new().set(
            "sysargs",
            Wire::Array(sysargs.iter().map(|s| Wire::from(s.as_str())).collect()),
        ),
        ActionRequest::Kwargs(kwargs) => Msg::new().set("kwargs", map_to_wire(kwargs)),
    };
    let reply = pool.call("action", args, Vec::new(), opts, handler).await?;
    Ok(success_from(&reply.value))
}

/// `Plugin(name).setup(*args, **kwargs)`.
pub async fn setup_plugin(
    pool: &WorkerPool,
    plugin: &str,
    args: &[String],
    kwargs: &Map,
) -> WorkerResult<SuccessTuple> {
    let msg = Msg::new()
        .set("plugin", plugin)
        .set(
            "args",
            Wire::Array(args.iter().map(|s| Wire::from(s.as_str())).collect()),
        )
        .set("kwargs", map_to_wire(kwargs));
    let reply = pool
        .call("setup", msg, Vec::new(), CallOptions::default(), None)
        .await?;
    Ok(success_from(&reply.value))
}

/// Which hooks to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPhase {
    /// `@pre_sync_hook`.
    Pre,
    /// `@post_sync_hook`.
    Post,
}

/// The keyword arguments v4's `sync pipes` passes to hooks (`actions/sync.py`).
#[derive(Debug, Clone, Default)]
pub struct SyncHookContext {
    /// `sync_timestamp`: when the sync started (UTC).
    pub sync_timestamp: Option<DateTime<Utc>>,
    /// `success_tuple` (post-sync only).
    pub success_tuple: Option<SuccessTuple>,
    /// `sync_duration` in seconds (post-sync only).
    pub sync_duration: Option<f64>,
    /// `sync_complete_timestamp` (post-sync only).
    pub sync_complete_timestamp: Option<DateTime<Utc>>,
    /// The rest of the sync kwargs (`begin`, `end`, `blocking`, `force`, `debug`, `workers`,
    /// `min_seconds`, `bounded`, `chunk_interval`, ...).
    pub extra: Map,
}

fn utc_wire(dt: &DateTime<Utc>) -> Wire {
    Wire::Ext(
        ext::DATETIME,
        DateTimeValue::utc(dt.naive_utc()).isoformat().into_bytes(),
    )
}

impl SyncHookContext {
    fn to_wire(&self) -> Wire {
        let mut m = Msg::new();
        for (k, v) in &self.extra {
            m.insert(
                k,
                if matches!(k.as_str(), "begin" | "end") {
                    crate::connector::sync_bound_wire(v)
                } else {
                    core_to_wire(v)
                },
            );
        }
        if let Some(ts) = &self.sync_timestamp {
            m.insert("sync_timestamp", utc_wire(ts));
        }
        if let Some(st) = &self.success_tuple {
            m.insert(
                "success_tuple",
                Wire::Array(vec![
                    Wire::Boolean(st.success),
                    Wire::from(st.message.as_str()),
                ]),
            );
        }
        if let Some(d) = self.sync_duration {
            m.insert("sync_duration", d);
        }
        if let Some(ts) = &self.sync_complete_timestamp {
            m.insert("sync_complete_timestamp", utc_wire(ts));
        }
        Wire::from(m)
    }
}

/// One hook's result.
#[derive(Debug, Clone, PartialEq)]
pub struct HookResult {
    /// The plugin registering the hook.
    pub plugin: Option<String>,
    /// The hook function's name.
    pub hook: String,
    /// Its result (`(True, "Success")` for non-tuple returns).
    pub result: SuccessTuple,
}

/// Run every registered pre- or post-sync hook for `pipe` with v4's kwargs, optionally only
/// those of `plugins`.
///
/// PARITY: v4 runs hooks concurrently in a thread pool and prints the results in registration
/// order; the worker runs them sequentially in registration order (actions/sync.py:509).
pub async fn run_sync_hooks(
    pool: &WorkerPool,
    pipe: &Pipe,
    phase: HookPhase,
    context: &SyncHookContext,
    plugins: Option<&[String]>,
    handler: Option<Arc<dyn CallbackHandler>>,
) -> WorkerResult<Vec<HookResult>> {
    let op = match phase {
        HookPhase::Pre => "pre_sync_hook",
        HookPhase::Post => "post_sync_hook",
    };
    let mut args = Msg::new()
        .set("pipe", map_to_wire(&pipe.to_state()))
        .set("kwargs", context.to_wire());
    if let Some(plugins) = plugins {
        args.insert(
            "plugins",
            Wire::Array(plugins.iter().map(|s| Wire::from(s.as_str())).collect()),
        );
    }
    let reply = pool
        .call(op, args, Vec::new(), CallOptions::default(), handler)
        .await?;
    let items = reply.value.as_array().cloned().unwrap_or_default();
    Ok(items
        .iter()
        .map(|item| {
            let m = match item {
                Wire::Map(m) => Msg(m.clone()),
                _ => Msg::new(),
            };
            HookResult {
                plugin: m.str("plugin").map(String::from),
                hook: m.str("hook").unwrap_or_default().to_string(),
                result: SuccessTuple {
                    success: m.get("success").and_then(Wire::as_bool).unwrap_or(false),
                    message: m.str("message").unwrap_or_default().to_string(),
                },
            }
        })
        .collect())
}

/// Round-trip a `ping` through a pooled worker; returns the worker's PID.
pub async fn ping(pool: &WorkerPool) -> WorkerResult<u32> {
    let reply = pool
        .call("ping", Msg::new(), Vec::new(), CallOptions::default(), None)
        .await?;
    Ok(match &reply.value {
        Wire::Map(m) => Msg(m.clone()).u64("pid").unwrap_or(0) as u32,
        _ => 0,
    })
}
