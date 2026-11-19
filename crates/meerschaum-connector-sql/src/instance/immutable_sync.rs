//! Native SQL operations used by the immutable sync planner.

use std::collections::BTreeMap;

use meerschaum_connector::{ConnectorError, ConnectorResult, DataQuery, DateBound};
use meerschaum_core::Pipe;
use meerschaum_kernel::{Frame, value::Value};

use crate::driver::params::{SqlParam, placeholder};
use crate::flavor::DriverFamily;
use crate::ident::sql_item_name;
use crate::queries::cte::wrap_query_with_cte;

use super::data::DataQueryOptions;
use super::fetch::FetchArgs;
use super::values::sync_time_from_sql;
use super::{SqlConnector, be};

fn column(pipe: &Pipe, kind: &str) -> ConnectorResult<String> {
    pipe.column(kind).ok_or_else(|| {
        ConnectorError::Invalid(format!(
            "Immutable sync requires a configured {kind} column."
        ))
    })
}

fn values<'a>(frame: &'a Frame, name: &str) -> ConnectorResult<std::borrow::Cow<'a, [Value]>> {
    Ok(frame.data(name).map_err(be)?.values_ref())
}

fn axis_value(value: &Value) -> ConnectorResult<DateBound> {
    DateBound::from_value(value)
        .or_else(|| DateBound::parse(&value.py_str()))
        .ok_or_else(|| ConnectorError::Invalid("Unsupported immutable sync axis value.".into()))
}

impl SqlConnector {
    fn immutable_sql(&self, source: &str, query: &str) -> ConnectorResult<String> {
        wrap_query_with_cte(source, query, self.flavor, "mrsm_immutable_rows").map_err(be)
    }

    pub(super) async fn immutable_remote_bound(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let dt = sql_item_name(&column(pipe, "datetime")?, self.flavor, None);
        let agg = if newest { "MAX" } else { "MIN" };
        let source = self.fetch_query(pipe, args).await?;
        let sql = self.immutable_sql(
            &source,
            &format!("SELECT {agg}({dt}) FROM mrsm_immutable_rows"),
        )?;
        self.value_checked(&sql)
            .await?
            .map(|value| {
                sync_time_from_sql(&value).ok_or_else(|| {
                    ConnectorError::Invalid("Unsupported immutable source timestamp.".into())
                })
            })
            .transpose()
    }

    pub(super) async fn immutable_remote_count(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
    ) -> ConnectorResult<Option<i64>> {
        let source = self.fetch_query(pipe, args).await?;
        let sql = self.immutable_sql(&source, "SELECT COUNT(*) FROM mrsm_immutable_rows")?;
        Ok(self
            .value_checked(&sql)
            .await?
            .and_then(|value| value.as_i64()))
    }

    pub(super) async fn immutable_target_query(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<String> {
        self.get_pipe_data_query(
            pipe,
            query,
            &DataQueryOptions {
                no_order: true,
                ..Default::default()
            },
        )
        .await
    }

    pub(super) async fn immutable_daily_counts(
        &self,
        pipe: &Pipe,
        table: &str,
    ) -> ConnectorResult<BTreeMap<i64, i64>> {
        let dt = sql_item_name(&column(pipe, "datetime")?, self.flavor, None);
        let day = match self.flavor.driver_family() {
            DriverFamily::Postgres | DriverFamily::Duckdb => {
                format!("CAST(FLOOR(EXTRACT(EPOCH FROM {dt}) / 86400) AS BIGINT)")
            }
            DriverFamily::Sqlite => {
                format!("CAST(julianday(date({dt})) - 2440587.5 AS INTEGER)")
            }
            _ => {
                return Err(ConnectorError::NotImplemented(
                    "Daily row counts require PostgreSQL, SQLite, or DuckDB.".into(),
                ));
            }
        };
        let sql = self.immutable_sql(
            table,
            &format!(
                "SELECT {day} AS mrsm_day, COUNT(*) AS mrsm_count FROM mrsm_immutable_rows \
             WHERE {dt} IS NOT NULL GROUP BY {day}"
            ),
        )?;
        let frame = self.read(&sql).await.map_err(be)?;
        values(&frame, "mrsm_day")?
            .iter()
            .zip(values(&frame, "mrsm_count")?.iter())
            .map(|(day, count)| match (day, count) {
                (Value::Int(day), Value::Int(count)) => Ok((*day, *count)),
                _ => Err(ConnectorError::Invalid(
                    "Expected an integer UTC day and row count.".into(),
                )),
            })
            .collect()
    }

    pub(super) async fn immutable_target_watermarks(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Vec<(String, DateBound)>> {
        require_postgres(self)?;
        let target = self.immutable_target_query(pipe, query).await?;
        let dt = sql_item_name(&column(pipe, "datetime")?, self.flavor, None);
        let id = sql_item_name(&column(pipe, "id")?, self.flavor, None);
        let sql = self.immutable_sql(
            &target,
            &format!(
                "SELECT CAST({id} AS TEXT) AS mrsm_id, MAX({dt}) AS mrsm_bound \
             FROM mrsm_immutable_rows WHERE {id} IS NOT NULL AND {dt} IS NOT NULL \
             GROUP BY CAST({id} AS TEXT) LIMIT 257"
            ),
        )?;
        let frame = self.read(&sql).await.map_err(be)?;
        if frame.height() > 256 {
            return Err(ConnectorError::Invalid(
                "This per-ID sync strategy supports at most 256 IDs in the destination scope."
                    .into(),
            ));
        }
        values(&frame, "mrsm_id")?
            .iter()
            .zip(values(&frame, "mrsm_bound")?.iter())
            .map(|(id, bound)| Ok((id.py_str(), axis_value(bound)?)))
            .collect()
    }

    pub(super) async fn immutable_fetch_by_id(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
        watermarks: &[(String, DateBound)],
        joined: bool,
    ) -> ConnectorResult<Option<Frame>> {
        require_postgres(self)?;
        if watermarks.is_empty() {
            return self.fetch(pipe, args).await;
        }
        let source = self.fetch_query(pipe, args).await?;
        let dt = sql_item_name(&column(pipe, "datetime")?, self.flavor, None);
        let id = sql_item_name(&column(pipe, "id")?, self.flavor, None);
        let mut params = Vec::with_capacity(watermarks.len() * 2);
        let mut pairs = Vec::with_capacity(watermarks.len());
        let mut selects = Vec::with_capacity(watermarks.len() + 1);
        let (axis, ty) = match &watermarks[0].1 {
            DateBound::Int(_) => (format!("src.{dt}"), "BIGINT"),
            DateBound::Datetime(dt_value) if dt_value.tz.is_some() => {
                (format!("src.{dt}"), "TIMESTAMP WITH TIME ZONE")
            }
            DateBound::Datetime(_) => (format!("src.{dt}"), "TIMESTAMP WITHOUT TIME ZONE"),
        };
        for (key, bound) in watermarks {
            params.push(SqlParam::Text(key.clone()));
            let p_id = placeholder(self.flavor, params.len());
            params.push(match bound {
                DateBound::Int(value) => SqlParam::Int(*value),
                DateBound::Datetime(value) if value.tz.is_some() => {
                    SqlParam::TimestampTz(value.utc.and_utc())
                }
                DateBound::Datetime(value) => SqlParam::Timestamp(value.utc),
            });
            let p_bound = placeholder(self.flavor, params.len());
            pairs.push(format!("(CAST({p_id} AS TEXT), CAST({p_bound} AS {ty}))"));
            if !joined {
                selects.push(format!(
                    "SELECT src.* FROM mrsm_immutable_rows AS src \
                     WHERE CAST(src.{id} AS TEXT) = {p_id} AND {axis} >= {p_bound}"
                ));
            }
        }
        let pairs = pairs.join(", ");
        let body = if joined {
            format!(
                "SELECT src.* FROM mrsm_immutable_rows AS src \
                 LEFT JOIN (VALUES {pairs}) AS wm(id, bound) ON CAST(src.{id} AS TEXT) = wm.id \
                 WHERE wm.id IS NULL OR {axis} >= wm.bound"
            )
        } else {
            selects.push(format!(
                "SELECT src.* FROM mrsm_immutable_rows AS src WHERE NOT EXISTS \
                 (SELECT 1 FROM (VALUES {pairs}) AS wm(id, bound) WHERE CAST(src.{id} AS TEXT) = wm.id)"
            ));
            selects.join("\nUNION ALL\n")
        };
        let sql = self.immutable_sql(&source, &body)?;
        Ok(Some(self.read_params(&sql, &params).await.map_err(be)?))
    }
}

fn require_postgres(connector: &SqlConnector) -> ConnectorResult<()> {
    if connector.flavor.driver_family() != DriverFamily::Postgres {
        return Err(ConnectorError::NotImplemented(
            "Per-ID immutable sync requires PostgreSQL-compatible SQL connectors.".into(),
        ));
    }
    Ok(())
}

impl SqlConnector {
    fn cpi_columns(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<(String, String)> {
        use meerschaum_connector::CPI_FIRST_POINT;

        require_postgres(self)?;
        let (Some(DateBound::Datetime(begin)), Some(DateBound::Datetime(end))) =
            (&query.begin, &query.end)
        else {
            return Err(ConnectorError::Invalid(
                "CPISync requires a bounded UTC day.".into(),
            ));
        };
        let epoch = begin.utc.and_utc().timestamp();
        if epoch % 86_400 != 0
            || (end.utc - begin.utc).num_microseconds() != Some(CPI_FIRST_POINT - 1)
        {
            return Err(ConnectorError::Invalid(
                "CPISync requires a bounded UTC day.".into(),
            ));
        }
        let time = sql_item_name(&column(pipe, "datetime")?, self.flavor, None);
        let series = pipe.column("id").map_or_else(
            || "NULL::TEXT".to_owned(),
            |id| format!("CAST({} AS TEXT)", sql_item_name(&id, self.flavor, None)),
        );
        Ok((
            series,
            format!("((EXTRACT(EPOCH FROM {time}) - ({epoch})) * 1000000)::BIGINT"),
        ))
    }

    pub(super) async fn immutable_cpi_samples(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        table: &str,
        points: i64,
    ) -> ConnectorResult<meerschaum_connector::CpiSeries> {
        use meerschaum_connector::{CPI_FIRST_POINT, CPI_PRIME, CpiSamples, CpiSeries};

        if !(2..=17).contains(&points) {
            return Err(ConnectorError::Invalid(
                "CPISync supports up to 17 sample points.".into(),
            ));
        }
        let (series, offset) = self.cpi_columns(pipe, query)?;
        let last = CPI_FIRST_POINT + points - 1;
        let sql = self.immutable_sql(table, &format!(
            "SELECT series, n, distinct_n, CAST(sample AS BIGINT) AS sample FROM (\n\
             WITH RECURSIVE positions AS (\n\
                 SELECT {series} AS series, {offset} AS k FROM mrsm_immutable_rows\n\
             ), numbered AS (\n\
                 SELECT series, k, ROW_NUMBER() OVER (PARTITION BY series ORDER BY k) AS pos FROM positions\n\
             ), sizes AS (\n\
                 SELECT series, COUNT(*) AS n, COUNT(DISTINCT k) AS distinct_n FROM positions GROUP BY series\n\
             ), products AS (\n\
                 SELECT series, n, distinct_n, point, 0::BIGINT AS pos, 1::NUMERIC AS sample\n\
                 FROM sizes CROSS JOIN GENERATE_SERIES({CPI_FIRST_POINT}, {last}) AS point\n\
                 WHERE n <= 8192\n\
                 UNION ALL\n\
                 SELECT p.series, p.n, p.distinct_n, p.point, t.pos, MOD(p.sample * (p.point - t.k), {CPI_PRIME})\n\
                 FROM products AS p JOIN numbered AS t ON p.series IS NOT DISTINCT FROM t.series AND t.pos = p.pos + 1\n\
             )\n\
             SELECT series, n, distinct_n, point, sample FROM products WHERE pos = n\n\
             ) AS samples ORDER BY series, point"
        ))?;
        let frame = self.read(&sql).await.map_err(be)?;
        let mut groups = CpiSeries::new();
        for (((series, n), distinct), sample) in values(&frame, "series")?
            .iter()
            .zip(values(&frame, "n")?.iter())
            .zip(values(&frame, "distinct_n")?.iter())
            .zip(values(&frame, "sample")?.iter())
        {
            let (Value::Int(n), Value::Int(distinct), Value::Int(sample)) = (n, distinct, sample)
            else {
                return Err(ConnectorError::Invalid(
                    "Unexpected CPISync sample result.".into(),
                ));
            };
            let id = (!series.is_na()).then(|| series.py_str());
            groups
                .entry(id)
                .or_insert_with(|| CpiSamples {
                    rows: *n,
                    distinct_timestamps: *distinct,
                    values: Vec::new(),
                })
                .values
                .push(*sample);
        }
        Ok(groups)
    }

    pub(super) async fn immutable_fetch_cpi(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        args: &FetchArgs,
        polynomials: &meerschaum_connector::CpiPolynomials,
    ) -> ConnectorResult<Frame> {
        use meerschaum_connector::CPI_PRIME;

        let (series, offset) = self.cpi_columns(pipe, query)?;
        let source = self.fetch_query(pipe, args).await?;
        let mut conditions = Vec::with_capacity(polynomials.len());
        let mut params = Vec::with_capacity(polynomials.len());
        for (id, coefficients) in polynomials {
            let mut polynomial = String::from("0::NUMERIC");
            for coefficient in coefficients.iter().rev() {
                polynomial =
                    format!("MOD(({polynomial}) * mrsm_cpi_offset.k + {coefficient}, {CPI_PRIME})");
            }
            params.push(SqlParam::opt_text(id.as_deref()));
            let param = placeholder(self.flavor, params.len());
            conditions.push(format!(
                "({series} IS NOT DISTINCT FROM CAST({param} AS TEXT) AND {polynomial} = 0)"
            ));
        }
        if conditions.is_empty() {
            return Ok(Frame::empty());
        }
        let sql = self.immutable_sql(
            &source,
            &format!(
                "SELECT mrsm_immutable_rows.* FROM mrsm_immutable_rows\n\
             CROSS JOIN LATERAL (SELECT {offset} AS k) AS mrsm_cpi_offset WHERE {}",
                conditions.join(" OR ")
            ),
        )?;
        self.read_params(&sql, &params).await.map_err(be)
    }
}
