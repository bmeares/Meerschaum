//! Removing pipe data: `drop_pipe` (the target table), `clear_pipe` (rows in a date window and
//! matching `params`) and `deduplicate_pipe`, with the queries they run.

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, DataQuery, DateBound};
use meerschaum_core::pyfmt::thousands;
use meerschaum_core::{Pipe, SuccessTuple, Value};

use crate::dateadd::{Number, dateadd_for_execution as dateadd_str};
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::instance::values::date_arg;
use crate::instance::{SqlConnector, be};
use crate::pipe::{SqlPipe, str_map};
use crate::queries::cte::{ColsToSelect, format_cte_subquery};
use crate::queries::table::{get_create_table_query, get_rename_table_queries};

use super::{PipeState, guess_datetime};

impl SqlConnector {
    /// `drop_pipe(pipe)`: drop the target table (keep the registration) and forget its Syncing
    /// Engine state on success.
    pub async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let res = self.drop_pipe_table(pipe).await;
        let provider = self.instance_provider();
        meerschaum_connector::pipe_ops::forget_engine_state_after(
            self,
            Some(provider.as_ref()),
            pipe,
            res,
        )
        .await
    }

    /// Drop the target table without forgetting Syncing Engine state (used by `partition_pipe`
    /// while rebuilding a table in place).
    pub(crate) async fn drop_pipe_table(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.ensure_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        if let Some(refused) = self.protected_refusal(pipe, "drop") {
            return refused;
        }
        if let Some(refused) = self
            .lock_refusal(pipe, meerschaum_core::access::LockOp::Drop)
            .await
        {
            return refused;
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let schema = self.get_pipe_schema(&sp);
        let target_name = self.pipe_table_name(&sp);
        // Ask the database itself: another process may have dropped the table already.
        self.forget_catalog_table(&sp.target);
        let mut success = true;
        if self.table_exists(&sp.target, schema.as_deref()).await {
            let if_exists = if self.flavor.drop_if_exists() {
                "IF EXISTS"
            } else {
                ""
            };
            success = self
                .exec(&format!("DROP TABLE {if_exists} {target_name}"))
                .await
                .is_some();
        }
        // Remove the table's GeoPackage metadata and spatial index (no-op otherwise).
        if success && self.flavor == crate::flavor::Flavor::Geopackage {
            let _ = self.drop_geopackage_metadata(&sp.target).await;
        }
        // Drop any MSSQL partition scheme + function the table referenced (no-op otherwise).
        if success {
            let cleanup =
                crate::queries::partition::get_partition_cleanup_queries(self.flavor, &sp);
            if !cleanup.is_empty() {
                let _ = self.exec_queries(&cleanup, false, false).await;
            }
        }
        if success {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to drop {pipe}."))
        }
    }

    /// `clear_pipe(pipe, begin, end, params)`: delete rows (`>= begin`, `< end`, matching
    /// `params`) without dropping the table. On an unbounded full clear, forgets Syncing Engine
    /// state on success.
    pub async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        self.clear_pipe_with_retention(pipe, query, false).await
    }

    pub(in crate::instance) async fn clear_pipe_with_retention(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        retention: bool,
    ) -> SuccessTuple {
        if let Err(e) = self.ensure_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        if let Some(refused) = self.protected_refusal(pipe, "clear") {
            return refused;
        }
        if let Some(refused) = self
            .lock_refusal(pipe, meerschaum_core::access::LockOp::Clear)
            .await
        {
            return refused;
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let res = match self.clear_pipe_inner(pipe, query, retention).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        };
        if !meerschaum_connector::pipe_ops::is_full_clear(query) {
            return res;
        }
        let provider = self.instance_provider();
        meerschaum_connector::pipe_ops::forget_engine_state_after(
            self,
            Some(provider.as_ref()),
            pipe,
            res,
        )
        .await
    }

    async fn clear_pipe_inner(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        retention: bool,
    ) -> ConnectorResult<SuccessTuple> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(SuccessTuple::ok(format!(
                "{pipe} does not exist, so nothing was cleared."
            )));
        }
        let sql = self.get_clear_pipe_query(&sp, &state, query)?;
        if let Some(drop_sql) = self.get_drop_chunks_query(&sp, &state, query)?
            && self.is_hypertable(&sp).await
        {
            let dropped = self.driver().exec(&drop_sql).await;
            if retention {
                return dropped.map(|_| SuccessTuple::success()).map_err(be);
            }
            if dropped.is_err() {
                tracing::debug!("drop_chunks skipped for {pipe}; falling back to DELETE.");
            }
        }
        Ok(if self.exec(&sql).await.is_some() {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to clear {pipe}."))
        })
    }

    /// Best-effort `public.drop_chunks(...)` pre-pass for an `end`-only `clear_pipe` on a
    /// TimescaleDB hypertable (such as engine history / metrics retention). Dropping whole
    /// expired chunks immediately frees OS disk space and avoids row-by-row deletes over
    /// compressed chunks; the subsequent `DELETE` removes any remaining rows in the boundary
    /// chunk straddling `end`.
    pub fn get_drop_chunks_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
    ) -> ConnectorResult<Option<String>> {
        if !self.flavor.is_timescaledb()
            || !sp.hypertable()
            || query.begin.is_some()
            || query.params.as_ref().is_some_and(|p| !p.is_empty())
        {
            return Ok(None);
        }
        let Some(dt_col) = sp.column("datetime") else {
            return Ok(None);
        };
        let Some(end) = self
            .parse_date_bound(sp, &state.dtypes, query.end.clone())
            .map_err(be)?
        else {
            return Ok(None);
        };
        let dt_typ = state
            .dtypes
            .get(&dt_col)
            .cloned()
            .unwrap_or_else(|| "datetime".into());
        let dt_db_type = get_db_type_from_pd_type(&dt_typ, Some(self.flavor));
        let bound = dateadd_str(
            Some(self.flavor),
            Some("day"),
            Number::Int(0),
            &date_arg(Some(&end)),
            Some(&dt_db_type),
        )
        .map_err(be)?;
        if bound.is_empty() {
            return Ok(None);
        }
        let older_than = if matches!(end, DateBound::Int(_))
            || meerschaum_core::dtypes::are_dtypes_equal(&dt_typ, "int")
        {
            format!("CAST({bound} AS BIGINT)")
        } else {
            bound
        };
        let pipe_name = self.pipe_table_name(sp).replace('\'', "''");
        Ok(Some(format!(
            "SELECT public.drop_chunks('{pipe_name}', older_than => {older_than})"
        )))
    }

    /// The `DELETE` query of `clear_pipe(pipe, begin, end, params)`.
    pub fn get_clear_pipe_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
    ) -> ConnectorResult<String> {
        let pipe = sp.pipe();
        let flavor = self.flavor;
        let pipe_name = self.pipe_table_name(sp);
        let dt_cfg = sp.column("datetime");
        let dt_typ = dt_cfg.as_ref().map(|c| {
            state
                .dtypes
                .get(c)
                .cloned()
                .unwrap_or_else(|| "datetime".into())
        });
        let dt_db_type = dt_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let (dt_col, is_guess) = match &dt_cfg {
            None => (guess_datetime(sp, &state.dtypes), true),
            Some(c) => (Some(c.clone()), false),
        };
        let dt_name = dt_col
            .as_deref()
            .map(|c| sql_item_name(c, flavor, None))
            .unwrap_or_default();
        // `Pipe.clear()` parses the bounds first (`_clear.py:61`).
        let mut begin = self
            .parse_date_bound(sp, &state.dtypes, query.begin.clone())
            .map_err(be)?;
        let mut end = self
            .parse_date_bound(sp, &state.dtypes, query.end.clone())
            .map_err(be)?;
        if (begin.is_some() || end.is_some()) && is_guess {
            match &dt_col {
                None => {
                    tracing::warn!(
                        "No datetime could be determined for {pipe}.\n    Ignoring datetime bounds..."
                    );
                    begin = None;
                    end = None;
                }
                Some(c) => tracing::warn!(
                    "A datetime wasn't specified for {pipe}.\n    Using column \"{c}\" for datetime bounds..."
                ),
            }
        }
        let valid: IndexMap<String, Value> = query
            .params
            .as_ref()
            .map(|p| {
                p.iter()
                    .filter(|(k, _)| state.cols_types.contains_key(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let mut sql = format!("DELETE FROM {pipe_name}\nWHERE 1 = 1\n");
        if !valid.is_empty() {
            sql.push_str("\n    AND ");
            let literal = super::super::sync::null_marker_columns(sp, state);
            sql.push_str(&crate::where_clause::build_where_literal(
                &valid,
                flavor,
                false,
                Default::default(),
                &literal,
            ));
        }
        if let Some(b) = &begin {
            sql.push_str(&format!(
                "\n    AND {dt_name} >= {}",
                dateadd_str(
                    Some(flavor),
                    Some("day"),
                    Number::Int(0),
                    &date_arg(Some(b)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        if let Some(e) = &end {
            sql.push_str(&format!(
                "\n    AND {dt_name} <  {}",
                dateadd_str(
                    Some(flavor),
                    Some("day"),
                    Number::Int(0),
                    &date_arg(Some(e)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        Ok(sql)
    }

    /// `deduplicate_pipe(pipe, begin, end, params)`: rebuild the table keeping one row per
    /// index (the newest by the datetime axis).
    ///
    /// PARITY: like v4, `begin`, `end` and `params` are accepted but the whole table is
    /// deduplicated (`_pipes.py:3995`).
    pub async fn deduplicate_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let _ = query;
        if let Err(e) = self.ensure_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        if let Some(refused) = self.protected_refusal(pipe, "deduplicate") {
            return refused;
        }
        if let Some(refused) = self
            .lock_refusal(pipe, meerschaum_core::access::LockOp::Deduplicate)
            .await
        {
            return refused;
        }
        let pipe = &self.prepare_pipe(pipe).await;
        match self.deduplicate_pipe_inner(pipe).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn deduplicate_pipe_inner(&self, pipe: &Pipe) -> ConnectorResult<SuccessTuple> {
        let sp = self.sql_pipe(pipe)?;
        let flavor = self.flavor;
        let q = |c: &str| sql_item_name(c, flavor, None);
        let pipe_table_name = self.pipe_table_name(&sp);
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(SuccessTuple::fail(format!(
                "Table {pipe_table_name} does not exist."
            )));
        }
        let dt_col = sp.column("datetime");
        let cols_types = &state.cols_types;
        let get_rowcount_query = format!("SELECT COUNT(*) FROM {pipe_table_name}");
        let Some(old_rowcount) = self
            .value(&get_rowcount_query)
            .await
            .and_then(|v| v.as_i64())
        else {
            return Ok(SuccessTuple::fail(format!(
                "Failed to get rowcount for table {pipe_table_name}."
            )));
        };
        // Non-datetime indices that in fact exist.
        let indices: Vec<String> = str_map(&sp.columns())
            .into_iter()
            .map(|(_, c)| c)
            .filter(|c| !c.is_empty() && Some(c) != dt_col.as_ref() && cols_types.contains_key(c))
            .collect();
        let indices_names: Vec<String> = indices.iter().map(|c| q(c)).collect();
        let existing_cols_names: Vec<String> = cols_types.keys().map(|c| q(c)).collect();
        let duplicate_row_number_name = q("dup_row_num");
        let previous_row_number_name = q("prev_row_num");
        let mut index_list_str = dt_col.as_deref().map(q).unwrap_or_default();
        let mut index_list_str_ordered = dt_col
            .as_deref()
            .map(|c| format!("{} DESC", q(c)))
            .unwrap_or_default();
        if !indices.is_empty() {
            index_list_str.push_str(&format!(", {}", indices_names.join(", ")));
            index_list_str_ordered.push_str(&format!(", {}", indices_names.join(", ")));
        }
        if index_list_str.starts_with(',') {
            index_list_str = index_list_str
                .trim_start_matches(',')
                .trim_start()
                .to_string();
        }
        if index_list_str_ordered.starts_with(',') {
            index_list_str_ordered = index_list_str_ordered
                .trim_start_matches(',')
                .trim_start()
                .to_string();
        }
        let cols_list_str = existing_cols_names.join(", ");
        let is_old_mysql = if flavor.mysql_partition() {
            self.db_version()
                .await
                .and_then(|v| v.split('.').next().and_then(|m| m.parse::<i64>().ok()))
                .is_some_and(|major| major < 8)
        } else {
            false
        };
        let src_query = format!(
            "\n        SELECT\n            {cols_list_str},\n            ROW_NUMBER() OVER (\n                PARTITION BY\n                {index_list_str}\n                ORDER BY {index_list_str_ordered}\n            ) AS {duplicate_row_number_name}\n        FROM {pipe_table_name}\n    "
        );
        let mut duplicates_cte_subquery = format_cte_subquery(
            &src_query,
            flavor,
            "src",
            &ColsToSelect::Raw(cols_list_str.clone()),
        )
        .map_err(be)?
            + &format!("\n        WHERE {duplicate_row_number_name} = 1\n        ");
        if is_old_mysql {
            let plus = index_list_str.replace(", ", " + ");
            let null_str = crate::queries::update::get_null_replacement("str", Some(Flavor::Mysql));
            duplicates_cte_subquery = format!(
                "\n        SELECT\n            {index_list_str}\n        FROM (\n          SELECT\n            {index_list_str},\n            IF(\n                @{previous_row_number_name} <> {plus},\n                @{duplicate_row_number_name} := 0,\n                @{duplicate_row_number_name}\n            ),\n            @{previous_row_number_name} := {plus},\n            @{duplicate_row_number_name} := @{duplicate_row_number_name} + 1 AS {duplicate_row_number_name}\n          FROM\n            {pipe_table_name},\n            (\n                SELECT @{duplicate_row_number_name} := 0\n            ) AS {duplicate_row_number_name},\n            (\n                SELECT @{previous_row_number_name} := '{null_str}'\n            ) AS {previous_row_number_name}\n          ORDER BY {index_list_str_ordered}\n        ) AS t\n        WHERE {duplicate_row_number_name} = 1\n        "
            );
        }
        let session_id =
            meerschaum_core::users::generate_password(3).unwrap_or_else(|_| "000".into());
        let dedup_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("dedup"));
        let temp_old_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("old"));
        let schema = self.get_pipe_schema(&sp);
        let temp_old_table_name = sql_item_name(&temp_old_table, flavor, schema.as_deref());
        let create_temporary_table_query = get_create_table_query(
            &crate::queries::table::QueryOrDtypes::Query(duplicates_cte_subquery),
            &dedup_table,
            flavor,
            None,
        )
        .map_err(be)?
            + &format!("\n    ORDER BY {index_list_str_ordered}\n    ");
        let if_exists = if flavor.drop_if_exists() {
            "IF EXISTS"
        } else {
            ""
        };
        let mut alter_queries: Vec<String> = Vec::new();
        alter_queries.extend(
            get_rename_table_queries(&sp.target, &temp_old_table, flavor, schema.as_deref(), None)
                .map_err(be)?,
        );
        alter_queries.extend(
            get_rename_table_queries(&dedup_table, &sp.target, flavor, None, schema.as_deref())
                .map_err(be)?,
        );
        alter_queries.push(format!("DROP TABLE {if_exists} {temp_old_table_name}"));

        let _ = self
            .log_temporary_tables_creation(
                std::slice::from_ref(&temp_old_table),
                false,
                !sp.temporary,
            )
            .await;
        if self.exec(&create_temporary_table_query).await.is_none() {
            return Ok(SuccessTuple::fail(format!(
                "Failed to deduplicate table {pipe_table_name}."
            )));
        }
        let results = self.exec_queries(&alter_queries, true, true).await;
        let mut fail_query = None;
        for (i, query) in alter_queries.iter().enumerate() {
            if results.get(i).is_none_or(Option::is_none) {
                fail_query = Some(query.clone());
                break;
            }
        }
        if let Some(fq) = fail_query {
            return Ok(SuccessTuple::fail(format!(
                "Failed to execute query:\n{fq}"
            )));
        }
        let new_rowcount = self
            .value(&get_rowcount_query)
            .await
            .and_then(|v| v.as_i64());
        let mut msg = format!("Successfully deduplicated table {pipe_table_name}");
        if Some(old_rowcount) != new_rowcount {
            msg.push_str(&format!(
                "\nfrom {} to {} rows",
                thousands(old_rowcount),
                new_rowcount.map(thousands).unwrap_or_else(|| "None".into())
            ));
        }
        msg.push('.');
        Ok(SuccessTuple::ok(msg))
    }
}
