//! Instance leases use a database clock and a unique key, shared by independent pools.
use std::time::Duration;

use meerschaum_connector::ConnectorResult;
use meerschaum_core::Pipe;
use sha2::{Digest, Sha256};

use super::{SqlConnector, be};
use crate::driver::{SqlParam, params::placeholder};
use crate::flavor::DriverFamily;
use crate::ident::sql_item_name;

impl SqlConnector {
    fn lease_table(&self) -> String {
        sql_item_name("mrsm_pipe_leases", self.flavor, self.internal_schema())
    }

    async fn ensure_leases(&self) -> ConnectorResult<()> {
        if self.state.lock().expect("state").leases_ready {
            return Ok(());
        }
        for q in crate::queries::table::get_create_schema_if_not_exists_queries(
            self.internal_schema(),
            self.flavor,
        )? {
            // Concurrent CREATE SCHEMA can race on PostgreSQL; table creation below verifies it.
            let _ = self.driver.exec(&q).await;
        }
        let table = self.lease_table();
        let ddl = format!(
            "CREATE TABLE {table} (pipe_ref VARCHAR(64) PRIMARY KEY, holder VARCHAR(256) NOT NULL, expires_us DECIMAL(20,0) NOT NULL)"
        );
        if let Err(e) = self.driver.exec(&ddl).await {
            // Only tolerate a concurrent/already completed creation, never a permission error.
            self.driver
                .read_frame(&format!(
                    "SELECT pipe_ref, holder, expires_us FROM {table} WHERE 1=0"
                ))
                .await
                .map_err(|_| be(e))?;
        }
        self.state.lock().expect("state").leases_ready = true;
        Ok(())
    }

    fn lease_clock(&self) -> &'static str {
        match self.flavor.driver_family() {
            DriverFamily::Postgres => {
                "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000 AS BIGINT)"
            }
            DriverFamily::Mysql => {
                "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS DECIMAL(20,0))"
            }
            DriverFamily::Sqlite => "CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)",
            DriverFamily::Mssql => "DATEDIFF_BIG(MICROSECOND, '19700101', SYSUTCDATETIME())",
            DriverFamily::Duckdb => "epoch_us(current_timestamp)",
            DriverFamily::Oracle => {
                "((TRUNC(CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE)) - DATE '1970-01-01') * 86400000000 + EXTRACT(HOUR FROM SYS_EXTRACT_UTC(SYSTIMESTAMP)) * 3600000000 + EXTRACT(MINUTE FROM SYS_EXTRACT_UTC(SYSTIMESTAMP)) * 60000000 + EXTRACT(SECOND FROM SYS_EXTRACT_UTC(SYSTIMESTAMP)) * 1000000)"
            }
        }
    }

    /// Acquire or renew a lease. Expired rows are replaced atomically by a conditional UPDATE;
    /// absent rows compete through the primary key. No client clock participates in expiry.
    pub async fn acquire_pipe_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: Duration,
    ) -> ConnectorResult<bool> {
        if holder.is_empty() || holder.len() > 256 || ttl.is_zero() {
            return Err(be(
                "A lease requires a nonempty holder (at most 256 bytes) and positive TTL.",
            ));
        }
        self.ensure_leases().await?;
        // Hash holders to keep identity case-sensitive even on case-insensitive collations.
        let holder = format!("{:x}", Sha256::digest(holder.as_bytes()));
        // Length-delimited JSON avoids ambiguous pipe keys and ignores connector label aliases.
        let key = serde_json::to_vec(&(
            self.schema(),
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        ))
        .map_err(be)?;
        let key = format!("{:x}", Sha256::digest(key));
        let ttl = i64::try_from(ttl.as_micros().max(1)).map_err(be)?;
        let p = |n| placeholder(self.flavor, n);
        let table = self.lease_table();
        let now = self.lease_clock();
        let sql = format!(
            "UPDATE {table} SET holder={}, expires_us={now}+{} WHERE pipe_ref={} AND (holder={} OR expires_us <= {now})",
            p(1),
            p(2),
            p(3),
            p(4)
        );
        let params = [
            SqlParam::Text(holder.clone()),
            SqlParam::Int(ttl),
            SqlParam::Text(key.clone()),
            SqlParam::Text(holder.clone()),
        ];
        match self.driver.exec_with(&sql, &params).await {
            Ok(changed) if changed > 0 => return Ok(true),
            Ok(_) => {}
            Err(e)
                if self.flavor.driver_family() == DriverFamily::Duckdb
                    && e.to_string().contains("Conflict on") =>
            {
                // DuckDB uses optimistic transactions rather than waiting on a row lock.
                // A concurrent takeover is a busy lease, not a connector failure.
                let query = format!(
                    "SELECT holder FROM {table} WHERE pipe_ref={} AND expires_us > {now}",
                    p(1)
                );
                let owner = self
                    .driver
                    .value_with(&query, &[SqlParam::Text(key.clone())])
                    .await
                    .map_err(be)?;
                return Ok(
                    matches!(owner, Some(crate::driver::SqlValue::Text(owner)) if owner == holder),
                );
            }
            Err(e) => return Err(be(e)),
        }
        let sql = format!(
            "INSERT INTO {table} (pipe_ref, holder, expires_us) VALUES ({}, {}, {now}+{})",
            p(1),
            p(2),
            p(3)
        );
        match self
            .driver
            .exec_with(
                &sql,
                &[
                    SqlParam::Text(key.clone()),
                    SqlParam::Text(holder.clone()),
                    SqlParam::Int(ttl),
                ],
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                // Confirm contention with a live row, so unrelated database failures propagate.
                let sql = format!(
                    "SELECT holder FROM {table} WHERE pipe_ref={} AND expires_us > {now}",
                    p(1)
                );
                match self
                    .driver
                    .value_with(&sql, &[SqlParam::Text(key)])
                    .await
                    .map_err(be)?
                {
                    Some(crate::driver::SqlValue::Text(owner)) => Ok(owner == holder),
                    _ => Err(be(e)),
                }
            }
        }
    }

    /// Release only this holder's lease; another holder's replacement cannot be removed.
    pub async fn release_pipe_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        self.ensure_leases().await?;
        // Hash holders to keep identity case-sensitive even on case-insensitive collations.
        let holder = format!("{:x}", Sha256::digest(holder.as_bytes()));
        let key = serde_json::to_vec(&(
            self.schema(),
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        ))
        .map_err(be)?;
        let key = format!("{:x}", Sha256::digest(key));
        let sql = format!(
            "DELETE FROM {} WHERE pipe_ref={} AND holder={} AND expires_us > {}",
            self.lease_table(),
            placeholder(self.flavor, 1),
            placeholder(self.flavor, 2),
            self.lease_clock()
        );
        Ok(self
            .driver
            .exec_with(&sql, &[SqlParam::Text(key), SqlParam::Text(holder.clone())])
            .await
            .map_err(be)?
            > 0)
    }
}
