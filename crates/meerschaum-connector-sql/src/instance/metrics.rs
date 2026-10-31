//! Window aggregates stay in the database; detailed history has an independent row cap.

use meerschaum_connector::{
    ConnectorResult, DataQuery,
    sync_metrics::{MAX_SUMMARY_GROUPS, MetricAggregate},
};

use super::data::DataQueryOptions;
use super::{SqlConnector, be};
use crate::flavor::{DriverFamily, Flavor};
use crate::ident::sql_item_name;

impl SqlConnector {
    pub(super) async fn aggregate_sync_metrics_query(
        &self,
        query: &DataQuery,
        begin_micros: i64,
        bucket_width_micros: i64,
        max_groups: usize,
    ) -> ConnectorResult<Option<(Vec<MetricAggregate>, bool)>> {
        let Some(pipe) = self.engine_state_pipe(&self.keys(), "engine_metrics") else {
            return Ok(None);
        };
        let sp = self.sql_pipe(&pipe)?;
        let state = self.pipe_state_checked(&sp).await?;
        if !state.exists {
            return Ok(Some((Vec::new(), false)));
        }
        let columns = [
            "pipe_key",
            "registration_id",
            "ts_us",
            "success",
            "duration_seconds",
            "inserted",
            "updated",
            "upserted",
            "deleted",
            "attempts",
            "chunks",
        ];
        if columns
            .iter()
            .any(|name| !state.cols_types.contains_key(*name))
        {
            return Ok(None);
        }
        let query = DataQuery {
            select_columns: Some(columns.iter().map(|name| (*name).to_string()).collect()),
            limit: None,
            order: None,
            ..query.clone()
        };
        let filtered = self
            .build_data_query(
                &sp,
                &state,
                &query,
                &DataQueryOptions {
                    no_order: true,
                    ..Default::default()
                },
            )
            .await?;
        let q = |name: &str| sql_item_name(name, self.flavor, None);
        let ts = q("ts_us");
        let width = bucket_width_micros.max(1);
        let bucket = match self.flavor.driver_family() {
            DriverFamily::Mysql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                format!("FLOOR(({ts} - {begin_micros}) / {width}.0)")
            }
            _ => format!("(({ts} - {begin_micros}) / {width})"),
        };
        let key = |name: &str| {
            if self.flavor == Flavor::Oracle {
                format!("CAST({} AS VARCHAR2(64))", q(name))
            } else {
                q(name)
            }
        };
        let success = if matches!(
            self.flavor.driver_family(),
            DriverFamily::Postgres | DriverFamily::Duckdb
        ) {
            "TRUE"
        } else {
            "1"
        };
        let count = if self.flavor == Flavor::Mssql {
            "COUNT_BIG"
        } else {
            "COUNT"
        };
        let condition = format!("{} = {success}", q("success"));
        let duration = q("duration_seconds");
        let max_duration = meerschaum_connector::sync_metrics::RETENTION_DAYS * 86400;
        let mut fields = vec![
            format!("{} AS {}", key("pipe_key"), q("pipe_key")),
            format!("{} AS {}", key("registration_id"), q("registration_id")),
            format!("{bucket} AS {}", q("bucket")),
            format!("{count}(*) AS {}", q("syncs")),
            format!(
                "{count}(CASE WHEN {condition} THEN 1 END) AS {}",
                q("succeeded")
            ),
            format!(
                "{count}(CASE WHEN {condition} THEN NULL ELSE 1 END) AS {}",
                q("failed")
            ),
            format!(
                "SUM(CASE WHEN {duration} >= 0 AND {duration} <= {max_duration} THEN {duration} ELSE 0 END) AS {duration}"
            ),
            format!("MAX({ts}) AS {}", q("last_sync_us")),
            format!(
                "MAX(CASE WHEN {condition} THEN {ts} END) AS {}",
                q("last_success_us")
            ),
            format!(
                "MAX(CASE WHEN {condition} THEN NULL ELSE {ts} END) AS {}",
                q("last_failure_us")
            ),
        ];
        for column in ["inserted", "updated", "upserted", "deleted", "chunks"] {
            let col = q(column);
            fields.push(format!(
                "SUM(CASE WHEN {col} > 0 THEN {col} ELSE 0 END) AS {}",
                q(column)
            ));
        }
        let attempts = q("attempts");
        fields.push(format!(
            "SUM(CASE WHEN {attempts} > 0 THEN {attempts} ELSE 1 END) AS {attempts}"
        ));
        fields.push(format!(
            "SUM(CASE WHEN {attempts} > 1 THEN {attempts} - 1 ELSE 0 END) AS {}",
            q("retries")
        ));
        let mut sql = format!(
            "SELECT {} FROM (\n{filtered}\n) mrsm_metrics_window\n\
            GROUP BY {}, {}, {bucket}\nORDER BY {} DESC, {}",
            fields.join(", "),
            key("pipe_key"),
            key("registration_id"),
            q("bucket"),
            q("pipe_key")
        );
        let max_groups = max_groups.clamp(1, MAX_SUMMARY_GROUPS);
        let take = max_groups + 1;
        match self.flavor {
            Flavor::Mssql => sql = format!("SELECT TOP {take} {}", &sql[7..]),
            Flavor::Oracle => sql = format!("SELECT * FROM ({sql}) WHERE ROWNUM <= {take}"),
            _ => sql.push_str(&format!("\nLIMIT {take}")),
        }
        let docs =
            meerschaum_connector::pipe_ops::frame_to_docs(&self.read(&sql).await.map_err(be)?);
        let mut capped = docs.len() > max_groups;
        let mut aggregates = Vec::with_capacity(docs.len().min(max_groups));
        for doc in docs.iter().take(max_groups) {
            match MetricAggregate::from_doc(doc) {
                Some(group) => aggregates.push(group),
                None => capped = true,
            }
        }
        Ok(Some((aggregates, capped)))
    }
}
