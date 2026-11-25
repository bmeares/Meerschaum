//! Reconcile a user's declared TimescaleDB settings after the data transaction releases its locks.

use super::*;
use crate::driver::SqlValue;
use crate::instance::be;
use crate::pipe::ChunkInterval;
use crate::queries::compress::{
    columnstore_after_clause, dt_axis_is_integer, parse_pd_timedelta_seconds,
};

fn declared_width(pipe: &Pipe) -> bool {
    pipe.param("verify").is_some_and(|v| {
        [
            "chunk_range",
            "chunk_minutes",
            "chunk_hours",
            "chunk_days",
            "chunk_weeks",
            "chunk_years",
            "chunk_seconds",
        ]
        .iter()
        .any(|key| v.get(key).is_some_and(|value| !value.is_null()))
    })
}

impl SqlConnector {
    pub(in crate::instance) async fn check_declared_hypertable(
        &self,
        pipe: &Pipe,
        message: &mut String,
    ) {
        if !self.flavor.is_timescaledb() || pipe.temporary || pipe.connector_keys() == "mrsm" {
            return;
        }
        let compress = pipe.param("compress").is_some_and(|v| v.truthy());
        if !compress && !declared_width(pipe) {
            return;
        }
        // Keep the catalog work off the in-place sync future's stack.
        let result = Box::pin(async {
            let sp = self.bind_pipe(pipe).await?;
            if !self.reconcile_hypertable(&sp, compress).await?
                && sp.hypertable()
                && sp.column("datetime").is_some()
            {
                return Err(be(format!("Table {} is not a hypertable", sp.target)));
            }
            Ok(())
        })
        .await;
        if let Err(error) = result {
            let warning = format!(
                "Could not apply the declared TimescaleDB settings to {pipe}: {error}. The next sync will retry."
            );
            tracing::warn!("{warning}");
            message.push_str(&format!("\n{warning}"));
        }
    }

    pub(super) async fn reconcile_hypertable(
        &self,
        sp: &SqlPipe,
        compress: bool,
    ) -> ConnectorResult<bool> {
        let table = self.pipe_table_name(sp).replace('\'', "''");
        let schema = self
            .get_pipe_schema(sp)
            .unwrap_or_else(|| "public".into())
            .replace('\'', "''");
        let name = sp.target.replace('\'', "''");
        let filter = format!("hypertable_schema = '{schema}' AND hypertable_name = '{name}'");
        let enabled = self
            .driver
            .value(&format!(
                "SELECT compression_enabled FROM timescaledb_information.hypertables WHERE {filter}"
            ))
            .await?;
        let Some(SqlValue::Bool(enabled)) = enabled else {
            return Ok(false);
        };
        if declared_width(sp.pipe())
            && let Some(axis) = sp.column("datetime")
        {
            let axis = axis.replace('\'', "''");
            let (field, desired) = match sp.chunk_interval()? {
                ChunkInterval::Int(n) => ("integer_interval", n.to_string()),
                ChunkInterval::Delta(delta) => (
                    "time_interval",
                    format!("INTERVAL '{} seconds'", delta.num_seconds()),
                ),
            };
            let mismatch = format!(
                "SELECT 1 FROM timescaledb_information.dimensions WHERE {filter} AND column_name = '{axis}' AND {field} <> {desired}"
            );
            if self.driver.value(&mismatch).await?.is_some() {
                self.policy_statement(&format!("SELECT public.set_chunk_time_interval('{table}', {desired}) FROM timescaledb_information.dimensions WHERE {filter} AND column_name = '{axis}' AND {field} <> {desired}")).await?;
            }
        }
        if !compress {
            return Ok(true);
        }
        let integer = dt_axis_is_integer(sp);
        if integer
            && let Some(after) = sp
                .param("compress")
                .and_then(|v| v.get("after"))
                .and_then(|v| v.as_str())
            && after.trim().parse::<i64>().is_err()
            && parse_pd_timedelta_seconds(after).is_none()
        {
            return Err(be(format!(
                "Invalid integer-axis compress.after '{after}'. Supply an integer or a duration in the pipe's precision."
            )));
        }
        if !enabled {
            self.policy_statement(&get_columnstore_settings_query(&self.ctx(), sp))
                .await?;
        }
        let jobs = format!(
            "timescaledb_information.jobs WHERE {filter} AND proc_name = 'policy_compression'"
        );
        let after = columnstore_after_clause(&self.ctx(), sp);
        let typ = if integer { "bigint" } else { "interval" };
        let query = format!("SELECT (config->>'compress_after')::{typ} = {after} FROM {jobs}");
        match self.driver.value(&query).await? {
            Some(SqlValue::Bool(true)) => {}
            Some(_) => {
                self.policy_statement(&format!("SELECT public.alter_job(job_id, config => jsonb_set(config, '{{compress_after}}', to_jsonb(({after})::{typ}))) FROM {jobs} AND (config->>'compress_after')::{typ} IS DISTINCT FROM {after}")).await?;
            }
            None => {
                for query in get_integer_now_func_queries(&self.ctx(), sp) {
                    self.policy_statement(&query).await?;
                }
                self.policy_statement(&get_columnstore_policy_query(&self.ctx(), sp))
                    .await?;
            }
        }
        Ok(true)
    }

    // Timescale requires enabling the columnstore and installing its policy in separate
    // transactions. Bound lock acquisition so a hot writer can finish and the next lap can retry.
    async fn policy_statement(&self, query: &str) -> ConnectorResult<()> {
        let mut tx = self.driver.begin_write().await?;
        tx.exec("SET LOCAL lock_timeout = '1s'").await?;
        tx.exec(query).await?;
        tx.commit().await?;
        Ok(())
    }
}
