//! `sync_pipe`: posting a frame to `POST /pipes/{keys}/data` in chunks and combining the row
//! counts the server reports for each chunk.

use std::time::Instant;

use meerschaum_core::pyfmt::{format_timespan, thousands};
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::serialize::GeometryFormat;

use crate::client::batch_sender::{BatchSender, Operation};
use crate::client::{APIConnector, Body, RequestOptions};
use crate::pipe_ops;
use crate::traits::{DateBound, SyncOptions};
use crate::wire::{QueryValue, ToJsonOptions, to_json};

use super::{bound_query, py_json, py_json_opt_map, query_value, tuple_from_json};

impl APIConnector {
    /// `sync_pipe(pipe, df, chunksize, **kw)`: post the frame in chunks
    /// (`to_json(chunk, orient='columns', geometry_format='wkb_hex')`), forwarding the keyword
    /// arguments as query parameters, then invalidate the server's pipe cache.
    pub async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        self.sync_pipe_counted(pipe, df, opts).await.into()
    }

    /// [`APIConnector::sync_pipe`] with the row counts the server reported for the chunks it
    /// synced (`Inserted 2, updated 1 rows.`, `Upserted 3 rows.`, `Replaced 1 row with 2
    /// rows.`), summed. The message is `sync_pipe`'s (v4's `It took ... to sync 3 rows across 2
    /// chunks ...`), which has none (`InstanceConnector::sync_pipe_with_counts`).
    pub async fn sync_pipe_counted(
        &self,
        pipe: &Pipe,
        df: Frame,
        opts: &SyncOptions,
    ) -> SyncResult {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "sync") {
            return refused.into();
        }
        if crate::engine_pipes::is_engine_pipe(pipe) {
            return self.write_engine_pipe(pipe.metric_key(), &df).await.into();
        }
        let begin = Instant::now();
        // A replacing sync: `opts.replace` with the window the engine resolved, or
        // `replace=True` among the keyword arguments of a direct `APIConnector.sync_pipe(...)`
        // call, whose `begin` / `end` (when given) go along as they are and whose missing
        // bounds the server takes from the posted rows (never the whole table).
        let replace = opts.replace || opts.extra.get("replace").is_some_and(Value::truthy);
        // PARITY: `chunksize=None` posts one row per request (`1 if chunksize is None`).
        // A replacing sync posts every row in one request: the server replaces the window
        // once, in one transaction.
        let chunksize = if replace {
            df.height()
        } else {
            match opts.chunksize {
                None => 1,
                Some(-1) => self.sync_chunksize,
                Some(n) => n,
            }
            .max(1) as usize
        }
        .max(1);

        let mut query: Vec<(String, QueryValue)> = opts
            .extra
            .iter()
            .filter(|(k, _)| {
                !opts.replace || !matches!(k.as_str(), "replace" | "begin" | "end" | "params")
            })
            .map(|(k, v)| {
                // This structured field decodes the typed JSON payload on Main. Generic
                // keyword parameters retain their historical Python str(value) encoding.
                let value = if k == "new_special_cols" && v.as_map().is_some() {
                    QueryValue::One(py_json(v))
                } else {
                    query_value(v)
                };
                (k.clone(), value)
            })
            .collect();
        if !opts.extra.contains_key("check_existing") && !opts.check_existing {
            query.push(("check_existing".into(), QueryValue::bool(false)));
        }
        if opts.replace {
            // The resolved window: an unbounded side is sent as `None` (not left out, which the
            // server would read as "the rows' own range").
            let window_bound = |b: &Option<DateBound>| match b {
                Some(_) => bound_query(b),
                None => QueryValue::One("None".into()),
            };
            query.push(("replace".into(), QueryValue::bool(true)));
            query.push(("begin".into(), window_bound(&opts.begin)));
            query.push(("end".into(), window_bound(&opts.end)));
            query.push(("params".into(), py_json_opt_map(&opts.params).into()));
        }
        let parameters = self.pipe_resolved_parameters(pipe).await;
        let mut merged = pipe.clone();
        merged.set_parameters(parameters);
        let columns = merged.columns();
        if !columns.is_empty() {
            query.push(("columns".into(), py_json(&Value::Map(columns)).into()));
        }
        query.push(self.instance_query(pipe, "instance_keys"));
        let r_url = Self::pipe_r_url(pipe) + "/data";

        let n = df.height();
        let mut sender = if n == 0 && !replace {
            BatchSender::ordinary(self)
        } else {
            match BatchSender::new(self, pipe).await {
                Ok(sender) => sender,
                Err(error) => return SyncResult::new(false, error.to_string()),
            }
        };
        let json_opts = ToJsonOptions {
            columns_orient: true,
            geometry_format: GeometryFormat::WkbHex,
            ..Default::default()
        };
        let mut rowcount = 0usize;
        let mut num_success_chunks = 0usize;
        // The server's counts for each chunk (`Inserted 2, updated 1 rows.`), summed.
        let mut counts = SyncResult::new(true, String::new());
        let mut start = 0usize;
        if replace && n == 0 {
            // No rows: the window is cleared.
            let response = match sender
                .post(
                    &r_url,
                    RequestOptions {
                        query: query.clone(),
                        body: Body::Text(String::new()),
                        headers: if meerschaum_connector::sync_status::in_recorded_sync() {
                            vec![(
                                meerschaum_connector::sync_status::CLIENT_RECORDS_HEADER.into(),
                                "client".into(),
                            )]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    },
                    Operation::Data,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => return SuccessTuple::fail(format!("Failed to sync {pipe}:\n{e}")).into(),
            };
            let _ = self.delete_pipe_cache(pipe).await;
            return match response.json() {
                Ok(j @ serde_json::Value::Array(_)) => {
                    let t = tuple_from_json(&response, &j);
                    pipe_ops::sync_result_from_message(t.success, t.message)
                }
                // A refused window (`400`: no rows and no bounds) explains itself.
                Ok(serde_json::Value::Object(o)) if o.contains_key("detail") => {
                    SuccessTuple::fail(match &o["detail"] {
                        serde_json::Value::String(s) => s.clone(),
                        other => crate::models::detail_repr(other),
                    })
                    .into()
                }
                _ => SuccessTuple::fail(response.text()).into(),
            };
        }
        while start < n {
            let end = (start + chunksize).min(n);
            let indices: Vec<usize> = (start..end).collect();
            start = end;
            if indices.is_empty() {
                continue;
            }
            let chunk = df.take(&indices);
            let json_str = to_json(&chunk, Some(&indices), &json_opts);
            let response = match sender
                .post(
                    &r_url,
                    RequestOptions {
                        query: query.clone(),
                        body: Body::Text(json_str),
                        // A sync path records this sync's outcome itself, through this client
                        // (`client/sync_status.rs`): the server must not record the posts.
                        headers: if meerschaum_connector::sync_status::in_recorded_sync() {
                            vec![(
                                meerschaum_connector::sync_status::CLIENT_RECORDS_HEADER.into(),
                                "client".into(),
                            )]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    },
                    Operation::Data,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("Failed to post a chunk to {pipe}:\n{e}");
                    super::super::warnings::warn_pipe_fmt!(pipe, "{msg}");
                    if sender.is_batched() {
                        // Only counts from prior fully delivered responses are known.
                        counts.success = false;
                        counts.message = msg;
                        return counts;
                    }
                    return SuccessTuple::fail(msg).into();
                }
            };
            if !response.ok() {
                return SuccessTuple::fail(format!("Failed to sync a chunk:\n{}", response.text()))
                    .into();
            }
            let j = match response.json() {
                Ok(j) => j,
                Err(e) => {
                    return SuccessTuple::fail(format!(
                        "Failed to parse response from syncing {pipe}:\n{e}"
                    ))
                    .into();
                }
            };
            if let Some(d) = j.as_object().and_then(|o| o.get("detail")) {
                return SuccessTuple::fail(match d {
                    serde_json::Value::String(s) => s.clone(),
                    other => crate::models::detail_repr(other),
                })
                .into();
            }
            let t = match &j {
                serde_json::Value::Array(_) => tuple_from_json(&response, &j),
                _ => return SuccessTuple::fail(response.text()).into(),
            };
            if !t.success {
                return t.into();
            }
            let chunk_counts = pipe_ops::sync_result_from_message(t.success, t.message);
            if replace {
                // The server's `Replaced {deleted} rows with {inserted} rows.` (the counts).
                let _ = self.delete_pipe_cache(pipe).await;
                return chunk_counts;
            }
            counts.inserted += chunk_counts.inserted;
            counts.updated += chunk_counts.updated;
            counts.upserted += chunk_counts.upserted;
            counts.deleted += chunk_counts.deleted;
            rowcount += indices.len();
            num_success_chunks += 1;
        }
        let _ = self.delete_pipe_cache(pipe).await;
        let elapsed = begin.elapsed().as_secs_f64();
        counts.message = format!(
            "It took {} to sync {} row{} across {} chunk{} to {pipe}.",
            format_timespan(elapsed),
            thousands(rowcount as u64),
            if rowcount != 1 { "s" } else { "" },
            thousands(num_success_chunks as u64),
            if num_success_chunks != 1 { "s" } else { "" },
        );
        counts
    }
}
