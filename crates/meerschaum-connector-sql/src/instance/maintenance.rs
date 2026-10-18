//! Maintenance operations (`_compress.py` and `_maintenance.py`, runtime side): `get_pipe_size`,
//! `compress_pipe`, `decompress_pipe`, `apply_compression_policy`, `set_integer_now_func`,
//! `vacuum_pipe` and `analyze_pipe`.

use meerschaum_core::{Pipe, SuccessTuple};

use crate::pipe::SqlPipe;
use crate::queries::compress::{
    get_columnstore_policy_query, get_columnstore_settings_query, get_compress_query_groups,
    get_decompress_query_groups, get_integer_now_func_queries, get_is_hypertable_query,
    get_pipe_size_queries,
};
use crate::queries::maintenance::{get_analyze_query, get_vacuum_queries};

use super::SqlConnector;
use super::values::format_bytes;

/// `_index_pipes_command(pipe)`.
fn index_pipes_command(pipe: &Pipe) -> String {
    let mut parts = vec![
        "index pipes".to_string(),
        "-c".into(),
        pipe.connector_keys().to_string(),
        "-m".into(),
        pipe.metric_key().to_string(),
    ];
    if let Some(lk) = pipe.location_key() {
        parts.push("-l".into());
        parts.push(lk.to_string());
    }
    parts.push("-i".into());
    parts.push(pipe.instance_keys().to_string());
    parts.join(" ")
}

/// `_not_a_hypertable_message(pipe)`.
fn not_a_hypertable_message(pipe: &Pipe) -> String {
    let mut msg =
        format!("{pipe} is not a hypertable; only hypertables support TimescaleDB compression.");
    if pipe.column("datetime").is_some() {
        msg.push_str(&format!(
            "\n    Run `{}` to convert its table into a hypertable\n    (ensure the pipe's 'hypertable' parameter is not set to false).",
            index_pipes_command(pipe)
        ));
    }
    msg
}

fn size_change_message(
    before: Option<i64>,
    after: Option<i64>,
    grew: &str,
    shrank: &str,
    default: String,
) -> String {
    let (Some(b), Some(a)) = (before, after) else {
        return default;
    };
    let change = format!("{} to {}", format_bytes(Some(b)), format_bytes(Some(a)));
    let delta = b - a;
    if delta > 0 {
        format!("{shrank} {} ({change}).", format_bytes(Some(delta)))
    } else if delta < 0 {
        format!("{grew} {} ({change}).", format_bytes(Some(-delta)))
    } else {
        format!("Size unchanged ({}).", format_bytes(Some(b)))
    }
}

impl SqlConnector {
    /// `get_pipe_size(pipe)`: the target table's on-disk size in bytes (`None` if unknown).
    pub async fn get_pipe_size(&self, pipe: &Pipe) -> Option<i64> {
        let sp = self.sql_pipe(pipe).ok()?;
        self.pipe_size(&sp).await
    }

    pub(crate) async fn pipe_size(&self, sp: &SqlPipe) -> Option<i64> {
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(sp).as_deref())
            .await
        {
            return None;
        }
        for query in get_pipe_size_queries(&self.ctx(), sp, self.database()) {
            if let Some(v) = self.value(&query).await.and_then(|v| v.as_i64()) {
                return Some(v);
            }
        }
        None
    }

    /// `_is_hypertable(pipe)`: whether the target is a TimescaleDB hypertable.
    pub async fn is_hypertable(&self, sp: &SqlPipe) -> bool {
        match get_is_hypertable_query(&self.ctx(), sp) {
            Some(q) => self.value(&q).await.is_some(),
            None => false,
        }
    }

    /// `set_integer_now_func(pipe)`: register an `integer_now` function on an integer-axis
    /// hypertable. `true` when nothing was needed or it was registered.
    pub async fn set_integer_now_func(&self, sp: &SqlPipe) -> bool {
        let queries = get_integer_now_func_queries(&self.ctx(), sp);
        if queries.is_empty() {
            return true;
        }
        self.exec_queries_all(&queries, true, true).await
    }

    /// `apply_compression_policy(pipe)`: idempotently enable the columnstore and install a
    /// columnstore policy (TimescaleDB only; a no-op success elsewhere).
    pub async fn apply_compression_policy(&self, pipe: &Pipe) -> SuccessTuple {
        if !self.flavor.is_timescaledb() {
            return SuccessTuple::ok("Compression policies are only supported for TimescaleDB.");
        }
        if !pipe.param("compress").is_some_and(|v| v.truthy()) {
            return SuccessTuple::ok("Compression is not enabled for this pipe.");
        }
        if pipe.upsert() && self.warn_compress_upsert_once(&pipe.to_string()) {
            tracing::warn!(
                "{pipe} declares both `upsert` and `compress`:\n    upserts into compressed chunks will fail with \"no unique or exclusion constraint matching the ON CONFLICT specification\".\n    Ensure `compress:after` exceeds how far back this pipe re-syncs history."
            );
        }
        let Ok(sp) = self.sql_pipe(pipe) else {
            return SuccessTuple::fail(format!("Failed to apply a compression policy to {pipe}."));
        };
        let exists = self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await;
        if !exists || !self.is_hypertable(&sp).await {
            return SuccessTuple::ok(format!(
                "{pipe} is not a hypertable; skipping compression policy."
            ));
        }
        if !self.set_integer_now_func(&sp).await {
            tracing::warn!(
                "Could not register an `integer_now` function for {pipe}; its columnstore policy will fail on every run."
            );
        }
        // Enable the columnstore and add the policy in SEPARATE transactions
        // (see timescale/timescaledb#8600).
        let settings_ok = self
            .exec_queries_all(
                &[get_columnstore_settings_query(&self.ctx(), &sp)],
                true,
                true,
            )
            .await;
        let policy_ok = self
            .exec_queries_all(
                &[get_columnstore_policy_query(&self.ctx(), &sp)],
                true,
                true,
            )
            .await;
        if !(settings_ok && policy_ok) {
            return SuccessTuple::fail(format!("Failed to apply a compression policy to {pipe}."));
        }
        SuccessTuple::ok(format!("Applied a compression policy to {pipe}."))
    }

    /// `compress_pipe(pipe, no_policy)`.
    pub async fn compress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        self.compress_or_decompress(pipe, no_policy, true).await
    }

    /// `decompress_pipe(pipe, no_policy)`.
    pub async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        self.compress_or_decompress(pipe, no_policy, false).await
    }

    async fn compress_or_decompress(
        &self,
        pipe: &Pipe,
        no_policy: bool,
        compress: bool,
    ) -> SuccessTuple {
        let verb = if compress { "compress" } else { "decompress" };
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await
        {
            return SuccessTuple::fail(format!("{pipe} does not exist; nothing to {verb}."));
        }
        if !self.flavor.compressible() {
            let noun = if compress {
                "Compression"
            } else {
                "Decompression"
            };
            return SuccessTuple::fail(format!(
                "{noun} is not supported for flavor '{}'.",
                self.flavor.as_str()
            ));
        }
        let size_before = self.pipe_size(&sp).await;
        let is_hypertable = if self.flavor.is_timescaledb() {
            self.is_hypertable(&sp).await
        } else {
            false
        };
        let groups = if compress {
            get_compress_query_groups(&self.ctx(), &sp, is_hypertable, no_policy)
        } else {
            get_decompress_query_groups(&self.ctx(), &sp, is_hypertable, no_policy)
        };
        let groups = match groups {
            Ok(g) => g,
            Err(e) if e == "not a hypertable" => {
                return SuccessTuple::fail(not_a_hypertable_message(pipe));
            }
            Err(e) => return SuccessTuple::fail(e),
        };
        for group in &groups {
            if !self.exec_queries_all(group, true, true).await {
                return SuccessTuple::fail(format!("Failed to {verb} {pipe}."));
            }
        }
        let size_after = self.pipe_size(&sp).await;
        let msg = if compress {
            size_change_message(
                size_before,
                size_after,
                "Size grew by",
                "Reclaimed",
                String::new(),
            )
        } else {
            // `decompress_pipe` reports growth as "Expanded by" and shrinkage as "Shrank by".
            match (size_before, size_after) {
                (Some(b), Some(a)) => {
                    let added = a - b;
                    let change = format!("{} to {}", format_bytes(Some(b)), format_bytes(Some(a)));
                    if added > 0 {
                        format!("Expanded by {} ({change}).", format_bytes(Some(added)))
                    } else if added < 0 {
                        format!("Shrank by {} ({change}).", format_bytes(Some(-added)))
                    } else {
                        format!("Size unchanged ({}).", format_bytes(Some(b)))
                    }
                }
                _ => String::new(),
            }
        };
        SuccessTuple::ok(msg)
    }

    /// `vacuum_pipe(pipe, full)`.
    pub async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await
        {
            return SuccessTuple::fail(format!("{pipe} does not exist; nothing to vacuum."));
        }
        let flavor = self.flavor;
        let unsupported = || {
            SuccessTuple::fail(format!(
                "Vacuuming is not supported for flavor '{}'.",
                flavor.as_str()
            ))
        };
        if !flavor.vacuumable() {
            return unsupported();
        }
        let pipe_name = self.pipe_table_name(&sp);
        let Some(queries) = get_vacuum_queries(flavor, &pipe_name, full).filter(|q| !q.is_empty())
        else {
            return unsupported();
        };
        let size_before = self.pipe_size(&sp).await;
        for q in &queries {
            self.log(q);
        }
        let success = if flavor.autocommit_vacuum() {
            self.driver()
                .exec_autocommit(&queries)
                .await
                .unwrap_or(false)
        } else {
            self.exec_queries_all(&queries, true, true).await
        };
        if !success {
            return SuccessTuple::fail(format!("Failed to vacuum {pipe}."));
        }
        // `VACUUM` may renumber the rowids the GeoPackage R-tree keys on; rebuild it.
        if flavor == crate::flavor::Flavor::Geopackage
            && let Some(col) = self.geopackage_geometry_column(&sp.target).await
        {
            let rtree = crate::instance::geopackage::rtree_name(&sp.target, &col);
            if self.table_exists(&rtree, None).await {
                let queries =
                    crate::instance::geopackage::get_rtree_create_queries(&sp.target, &col);
                if !self.exec_queries_all(&queries, true, true).await {
                    return SuccessTuple::fail(format!(
                        "Failed to rebuild the spatial index of {pipe}."
                    ));
                }
            }
        }
        let size_after = self.pipe_size(&sp).await;
        SuccessTuple::ok(size_change_message(
            size_before,
            size_after,
            "Size grew by",
            "Reclaimed",
            format!("Vacuumed {pipe}."),
        ))
    }

    /// `analyze_pipe(pipe)`.
    pub async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await
        {
            return SuccessTuple::fail(format!("{pipe} does not exist; nothing to analyze."));
        }
        let flavor = self.flavor;
        let unsupported = || {
            SuccessTuple::fail(format!(
                "Analyzing is not supported for flavor '{}'.",
                flavor.as_str()
            ))
        };
        if !flavor.analyzable() {
            return unsupported();
        }
        let pipe_name = self.pipe_table_name(&sp);
        let Some(query) = get_analyze_query(flavor, &pipe_name) else {
            return unsupported();
        };
        if !self.exec_queries_all(&[query], true, true).await {
            return SuccessTuple::fail(format!("Failed to analyze {pipe}."));
        }
        SuccessTuple::ok(format!("Analyzed {pipe}."))
    }
}
