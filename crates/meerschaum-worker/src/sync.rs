//! `Pipe.sync()` for pipes whose source connector is Python (a `plugin:` or custom connector)
//! and whose instance is native.
//!
//! This mirrors the v4 flow in `core/Pipe/_sync.py` for the parts the adapter owns: register
//! the pipe (merging `register()`'s parameters), call the connector's `sync()` if it has one
//! (forwarding its `pipe.sync(df)` calls to the native path), else `fetch()` from the sync time
//! minus the backtrack interval and sync each chunk natively (kernel dtype enforcement, then the
//! instance's `sync_pipe()`). The engine's `SyncExecutor` (§5.5) is expected to wrap this.

use std::sync::Arc;

use meerschaum_connector::pipe_ops::dtypes_context;
use meerschaum_connector::{DateBound, InstanceConnector};
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value as CValue};
use meerschaum_kernel::value::DateTimeValue;

use crate::callback::{CallbackHandler, InstanceSyncHandler, sync_chunk_native};
use crate::connector::{FetchOptions, PythonConnector};

/// The default `pipes:parameters:fetch:backtrack_minutes`.
pub const DEFAULT_BACKTRACK_MINUTES: i64 = 1440;

/// Options for [`sync_python_pipe`].
#[derive(Debug, Clone)]
pub struct PythonSyncOptions {
    /// `begin`: `None` means v4's `''` (sync time minus backtrack); `Some(None)` means no lower
    /// bound; `Some(Some(b))` is explicit.
    pub begin: Option<Option<DateBound>>,
    /// `end`.
    pub end: Option<DateBound>,
    /// `params`.
    pub params: Option<Map>,
    /// `check_existing`.
    pub check_existing: bool,
    /// The configured default backtrack (`pipes:parameters:fetch:backtrack_minutes`).
    pub default_backtrack_minutes: i64,
    /// Other keyword arguments for the plugin (`chunksize`, `debug`, ...).
    pub kwargs: Map,
}

impl Default for PythonSyncOptions {
    fn default() -> Self {
        PythonSyncOptions {
            begin: None,
            end: None,
            params: None,
            check_existing: true,
            default_backtrack_minutes: DEFAULT_BACKTRACK_MINUTES,
            kwargs: Map::new(),
        }
    }
}

fn backtrack_minutes(pipe: &Pipe, opts: &PythonSyncOptions) -> i64 {
    if !opts.check_existing {
        return 0;
    }
    pipe.parameters()
        .get("fetch")
        .and_then(|f| f.get("backtrack_minutes"))
        .and_then(CValue::as_i64)
        .unwrap_or(opts.default_backtrack_minutes)
}

fn minus_minutes(b: DateBound, minutes: i64) -> DateBound {
    match b {
        DateBound::Int(i) => DateBound::Int(i - minutes),
        DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - chrono::Duration::minutes(minutes),
            tz: dt.tz,
        }),
    }
}

/// `Pipe.parse_date_bounds(bound)` for a datetime from the instance: the instance traits return
/// raw values, so coerce to the axis' timezone (UTC-aware unless the datetime dtype is naive).
pub async fn parse_date_bound(
    instance: &dyn InstanceConnector,
    pipe: &Pipe,
    bound: DateBound,
) -> Result<DateBound, String> {
    let DateBound::Datetime(dt) = bound else {
        return Ok(bound);
    };
    let ctx = dtypes_context(instance, pipe, pipe.parameters())
        .await
        .map_err(|e| e.to_string())?;
    Ok(DateBound::Datetime(if ctx.tz_aware {
        DateTimeValue::utc(dt.utc)
    } else {
        DateTimeValue::naive(dt.utc)
    }))
}

/// The keyword arguments v4's `Pipe.sync()` hands to a plugin's `sync()` / `fetch()`
/// (`core/Pipe/_sync.py`: the signature's defaults merged into `kw`), with the caller's
/// `kwargs` taking precedence.
pub fn v4_sync_kwargs(opts: &PythonSyncOptions) -> Map {
    let mut kwargs = Map::new();
    let defaults = [
        ("force", CValue::Bool(false)),
        ("retries", CValue::Int(10)),
        ("min_seconds", CValue::Int(1)),
        ("check_existing", CValue::Bool(opts.check_existing)),
        ("blocking", CValue::Bool(true)),
        ("workers", CValue::Null),
        ("callback", CValue::Null),
        ("error_callback", CValue::Null),
        ("sync_chunks", CValue::Bool(true)),
        ("chunksize", CValue::Int(-1)),
        ("safe_copy", CValue::Bool(true)),
    ];
    for (k, v) in defaults {
        kwargs.insert(k.to_string(), v);
    }
    for (k, v) in &opts.kwargs {
        kwargs.insert(k.clone(), v.clone());
    }
    kwargs
}

/// Register `pipe` on `instance` if needed, applying the connector's `register()` parameters
/// under the pipe's own (v4: `apply_patch_to_config(params, self.parameters)`).
pub async fn register_python_pipe(
    source: &PythonConnector,
    instance: &dyn InstanceConnector,
    pipe: &mut Pipe,
) -> SuccessTuple {
    match instance.get_pipe_id(pipe).await {
        Ok(Some(_)) => return SuccessTuple::ok(format!("{pipe} is already registered.")),
        Ok(None) => {}
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    if source.info().has_register {
        match source.register(pipe).await {
            Ok(outcome) if outcome.valid => {
                let merged = apply_patch_maps(&outcome.parameters, pipe.parameters());
                pipe.set_parameters(merged);
            }
            Ok(outcome) => tracing::warn!(
                "Invalid parameters returned from `register()` in connector {}: {:?}",
                source.info().label,
                outcome.parameters
            ),
            Err(e) => tracing::warn!("`register()` failed for {pipe}: {e}"),
        }
    }
    instance.register_pipe(pipe).await
}

/// Sync `pipe` from its Python source connector into the native `instance`.
pub async fn sync_python_pipe(
    source: &PythonConnector,
    instance: Arc<dyn InstanceConnector>,
    pipe: &mut Pipe,
    opts: &PythonSyncOptions,
) -> SuccessTuple {
    let reg = register_python_pipe(source, instance.as_ref(), pipe).await;
    if !reg.success && !reg.message.contains("already") {
        return reg;
    }
    let handler: Arc<dyn CallbackHandler> =
        Arc::new(InstanceSyncHandler::single(Arc::clone(&instance)));
    let source = source.clone().with_callbacks(Arc::clone(&handler));

    let kwargs = v4_sync_kwargs(opts);

    if source.info().has_sync {
        let mut kw = kwargs.clone();
        if let Some(p) = &opts.params {
            kw.insert("params".into(), CValue::Map(p.clone()));
        }
        return match source.sync(pipe, &kw).await {
            Ok(st) => st,
            Err(e) => SuccessTuple::fail(format!("Failed to sync {pipe} with exception: '{e}'")),
        };
    }

    let begin = match &opts.begin {
        Some(b) => b.clone(),
        // PARITY: with `begin=''` and an `end`, v4 fetches with no lower bound
        // (`_fetch.py:_determine_begin`).
        None if opts.end.is_some() => None,
        None => match instance.get_sync_time(pipe, None, true).await {
            Ok(Some(st)) => {
                let st = match parse_date_bound(instance.as_ref(), pipe, st).await {
                    Ok(st) => st,
                    Err(e) => return SuccessTuple::fail(e),
                };
                Some(minus_minutes(st, backtrack_minutes(pipe, opts)))
            }
            Ok(None) => None,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        },
    };
    let outcome = match source
        .fetch_chunks(
            pipe,
            &FetchOptions {
                begin,
                end: opts.end.clone(),
                params: opts.params.clone(),
                kwargs: kwargs.clone(),
                sync_chunks: true,
            },
        )
        .await
    {
        Ok(o) => o,
        Err(e) => {
            return SuccessTuple::fail(format!(
                "No data were fetched for {pipe}.\nFailed to fetch data from {}:\n    {e}",
                source.info().label
            ));
        }
    };
    if outcome.returned_none {
        return SuccessTuple::fail(format!("No data were fetched for {pipe}."));
    }
    if outcome.in_parallel {
        return SuccessTuple::ok(format!("{pipe} is being synced in parallel."));
    }
    if let Some(results) = outcome.hook_results {
        if results.is_empty() {
            return SuccessTuple::ok(format!("No new rows were returned for {pipe}."));
        }
        return SuccessTuple {
            success: results.iter().all(|r| r.success),
            message: results
                .iter()
                .map(|r| r.message.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        };
    }
    let mut chunks = outcome.chunks;
    if chunks.is_empty() {
        return if outcome.generator {
            SuccessTuple::ok("Received an empty generator; nothing to do.")
        } else {
            SuccessTuple::ok(format!("No new rows were returned for {pipe}."))
        };
    }
    if chunks.len() == 1 && !outcome.generator {
        let chunk = chunks.remove(0);
        if matches!(&chunk, crate::chunk::FetchChunk::Docs(d) if d.is_empty()) {
            return SuccessTuple::ok(format!("No new rows were returned for {pipe}."));
        }
        return sync_chunk_native(instance.as_ref(), pipe, chunk, &kwargs).await;
    }
    let mut results = Vec::new();
    for chunk in chunks {
        let n = chunk.len();
        let st = sync_chunk_native(instance.as_ref(), pipe, chunk, &kwargs).await;
        let rows = if n == 1 {
            format!("{n} row")
        } else {
            format!("{n} rows")
        };
        let verb = if st.success {
            "Synced"
        } else {
            "Failed to sync"
        };
        results.push(SuccessTuple {
            success: st.success,
            message: format!("{verb} a chunk ({rows}) to {pipe}:\n{}", st.message),
        });
    }
    let n_ok = results.iter().filter(|r| r.success).count();
    let n_fail = results.len() - n_ok;
    SuccessTuple {
        success: n_fail == 0,
        message: format!(
            "Synced {} chunk{} to {pipe}\n({n_ok} succeeded, {n_fail} failed):\n\n{}",
            results.len(),
            if results.len() == 1 { "" } else { "s" },
            results
                .iter()
                .map(|r| r.message.as_str())
                .collect::<Vec<_>>()
                .join("\n\n")
        )
        .trim()
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v4_kwargs_defaults_and_overrides() {
        let mut opts = PythonSyncOptions {
            check_existing: false,
            ..Default::default()
        };
        opts.kwargs.insert("chunksize".into(), CValue::Int(100));
        let kw = v4_sync_kwargs(&opts);
        assert_eq!(kw.get("check_existing"), Some(&CValue::Bool(false)));
        assert_eq!(kw.get("chunksize"), Some(&CValue::Int(100)));
        assert_eq!(kw.get("retries"), Some(&CValue::Int(10)));
        assert_eq!(kw.get("sync_chunks"), Some(&CValue::Bool(true)));
        assert_eq!(backtrack_minutes(&Pipe::new("a:b", "c").unwrap(), &opts), 0);
    }
}
