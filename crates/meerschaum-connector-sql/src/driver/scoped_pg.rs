//! Draft host-only PostgreSQL writes with exact destination locks.
//!
//! This module is registered only in tests pending native operation integration. A caller
//! must derive its relation set and SQL from the accepted canonical operation, not from a
//! request flag, raw SQL target parser, environment variable or serialized job property.
//! Existing unscoped PostgreSQL operations are unchanged.
//!
//! The first supported storage contract is deliberately explicit: existing permanent plain
//! heap tables, built-in scalar types and ordinary built-in btree indexes. Executable table
//! hooks, foreign tables, inheritance, partition routing, defaults/identity sequences,
//! expression indexes and DDL need separate typed adapters. A transaction rollback cannot
//! promise to undo arbitrary trigger, FDW, sequence or external function side effects.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::mutation::{self, MutationCheck};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row};
use tokio::time::Instant;

use super::{SqlParam, params};
use crate::error::{Result, SqlError};

const MAX_RELATIONS: usize = 32;
const MAX_SQL_BYTES: usize = 256 * 1024;
const MAX_COPY_BYTES: usize = 64 * 1024 * 1024;

fn bounded_values(values: &[SqlParam]) -> bool {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
                std::io::Error::other("Native PostgreSQL parameter byte bound exceeded")
            })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut budget = Budget(MAX_COPY_BYTES);
    for value in values {
        let bytes = match value {
            SqlParam::Text(value) => value.len(),
            SqlParam::Bytes(value) => value.len(),
            SqlParam::Json(value) => {
                if serde_json::to_writer(&mut budget, value).is_err() {
                    return false;
                }
                continue;
            }
            _ => 32,
        };
        let Some(remaining) = budget.0.checked_sub(bytes) else {
            return false;
        };
        budget.0 = remaining;
    }
    true
}

fn refuse(message: impl Into<String>) -> SqlError {
    SqlError::InvalidArgument(message.into())
}

async fn bounded<T>(
    deadline: Instant,
    future: impl Future<Output = std::result::Result<T, sqlx::Error>>,
) -> Result<T> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| refuse("The scoped PostgreSQL statement deadline expired."))?
        .map_err(Into::into)
}

/// Host-selected bounds. No serialized/public configuration can relax this contract.
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) acquire: Duration,
    pub(super) lock: Duration,
    pub(super) statement: Duration,
    pub(super) transaction: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            acquire: Duration::from_secs(5),
            lock: Duration::from_secs(5),
            statement: Duration::from_secs(15),
            transaction: Duration::from_secs(30),
        }
    }
}

impl Limits {
    fn validate(self) -> Result<Self> {
        for duration in [self.acquire, self.lock, self.statement, self.transaction] {
            if duration < Duration::from_millis(10) || duration > Duration::from_secs(60) {
                return Err(refuse("Invalid host PostgreSQL write deadline."));
            }
        }
        if self.lock > self.statement || self.statement > self.transaction {
            return Err(refuse("Inconsistent host PostgreSQL write deadlines."));
        }
        Ok(self)
    }
}

/// Exact identifiers from the trusted native destination planner. Always schema-qualified.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct RelationName {
    schema: String,
    table: String,
}

fn identifier(name: &str) -> Result<String> {
    if name.is_empty() || name.len() > 63 || name.chars().any(char::is_control) {
        return Err(refuse("Invalid canonical PostgreSQL relation identifier."));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

impl RelationName {
    pub(super) fn new(schema: &str, table: &str) -> Result<Self> {
        identifier(schema)?;
        identifier(table)?;
        if schema == "information_schema" || schema.starts_with("pg_") {
            return Err(refuse(
                "A scoped write cannot target a PostgreSQL system schema.",
            ));
        }
        Ok(Self {
            schema: schema.into(),
            table: table.into(),
        })
    }

    fn quoted(&self) -> Result<String> {
        Ok(format!(
            "{}.{}",
            identifier(&self.schema)?,
            identifier(&self.table)?
        ))
    }
}

#[derive(Clone)]
struct Relation {
    name: RelationName,
    oid: i64,
}

/// In-memory provenance only. Cloning retains the original driver; there is no decoder.
#[derive(Clone)]
pub(super) struct Plan {
    lineage: Arc<()>,
    database: i64,
    relations: Vec<Relation>,
}

/// Bounded writers never consume the ordinary pool used for fresh canonical/auth reads.
#[derive(Clone)]
pub(super) struct Writer {
    pool: PgPool,
    lineage: Arc<()>,
    limits: Limits,
}

impl Writer {
    pub(super) fn lazy(
        options: PgConnectOptions,
        lineage: Arc<()>,
        limits: Limits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        // Apply native deadlines at startup too: even bounded catalog capture and BEGIN
        // must not inherit an unlimited host/libpq statement setting.
        let options = options.options([
            (
                "statement_timeout",
                format!("{}ms", limits.statement.as_millis()),
            ),
            ("lock_timeout", format!("{}ms", limits.lock.as_millis())),
            (
                "idle_in_transaction_session_timeout",
                format!("{}ms", limits.transaction.as_millis()),
            ),
        ]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .acquire_timeout(limits.acquire)
            .connect_lazy_with(options);
        Ok(Self {
            pool,
            lineage,
            limits,
        })
    }

    /// Capture only existing destinations chosen by the canonical native operation. This
    /// read is not admission: every OID is checked again after its transaction lock is held.
    pub(super) async fn capture(&self, names: &[RelationName]) -> Result<Plan> {
        tokio::time::timeout(self.limits.transaction, self.capture_inner(names))
            .await
            .map_err(|_| refuse("The PostgreSQL destination capture deadline expired."))?
    }

    async fn capture_inner(&self, names: &[RelationName]) -> Result<Plan> {
        if names.is_empty() || names.len() > MAX_RELATIONS {
            return Err(refuse(
                "A PostgreSQL write has an invalid destination count.",
            ));
        }
        let names: BTreeSet<_> = names.iter().cloned().collect();
        let mut connection = self.pool.acquire().await?;
        let database: i64 = sqlx::query_scalar(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()",
        )
        .fetch_one(&mut *connection)
        .await?;
        let mut relations = Vec::with_capacity(names.len());
        for name in names {
            let row = sqlx::query(
                "SELECT c.oid::bigint AS oid, c.relkind::text AS kind \
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
            )
            .bind(&name.schema)
            .bind(&name.table)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(|| refuse("The selected PostgreSQL write destination no longer exists."))?;
            if row.try_get::<String, _>("kind")? != "r" {
                return Err(refuse(
                    "This scoped PostgreSQL destination needs a storage-specific write adapter.",
                ));
            }
            relations.push(Relation {
                name,
                oid: row.try_get("oid")?,
            });
        }
        Ok(Plan {
            lineage: self.lineage.clone(),
            database,
            relations,
        })
    }

    pub(super) async fn begin(
        &self,
        plan: Plan,
        authority: Arc<dyn MutationCheck>,
    ) -> Result<Transaction> {
        if !Arc::ptr_eq(&plan.lineage, &self.lineage) {
            return Err(refuse(
                "The PostgreSQL destination plan belongs to another native driver.",
            ));
        }
        let deadline = Instant::now() + self.limits.transaction;
        let tx = tokio::time::timeout_at(deadline, self.pool.begin())
            .await
            .map_err(|_| refuse("The scoped PostgreSQL writer acquisition expired."))??;
        let mut transaction = Transaction {
            tx,
            authority,
            plan,
            deadline,
            limits: self.limits,
            refused: false,
        };
        let admitted = transaction.acquire().await;
        if let Err(error) = admitted {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        Ok(transaction)
    }
}

/// No mutable raw connection escapes this wrapper. An error permanently refuses commit;
/// dropping a cancelled wrapper retains SQLx's transaction rollback-before-pool-reuse path.
pub(super) struct Transaction {
    tx: sqlx::Transaction<'static, Postgres>,
    authority: Arc<dyn MutationCheck>,
    plan: Plan,
    deadline: Instant,
    limits: Limits,
    refused: bool,
}

impl Transaction {
    async fn acquire(&mut self) -> Result<()> {
        bounded(
            self.deadline,
            sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(&mut *self.tx),
        )
        .await?;
        self.refresh_deadline().await?;
        bounded(
            self.deadline,
            sqlx::query("SET LOCAL search_path = pg_catalog").execute(&mut *self.tx),
        )
        .await?;
        bounded(
            self.deadline,
            sqlx::query("SET LOCAL row_security = off").execute(&mut *self.tx),
        )
        .await?;
        // EXCLUSIVE conflicts with ROW SHARE (SELECT FOR UPDATE/SHARE), every writer and
        // DDL, while fresh ordinary SELECTs on a separate auth pool remain possible.
        // ONLY prevents implicit inheritance traversal for a still-ordinary destination.
        for relation in self.plan.relations.clone() {
            self.refresh_deadline().await?;
            let sql = format!(
                "LOCK TABLE ONLY {} IN EXCLUSIVE MODE",
                relation.name.quoted()?
            );
            bounded(
                self.deadline,
                sqlx::query(AssertSqlSafe(sql.as_str())).execute(&mut *self.tx),
            )
            .await?;
        }
        let database: i64 = bounded(self.deadline, sqlx::query_scalar(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()",
        ).fetch_one(&mut *self.tx)).await?;
        if database != self.plan.database {
            return Err(refuse(
                "The selected PostgreSQL database changed before write admission.",
            ));
        }
        for relation in self.plan.relations.clone() {
            self.refresh_deadline().await?;
            let row = bounded(
                self.deadline,
                sqlx::query(CONTROLLED_DESTINATION)
                    .bind(&relation.name.schema)
                    .bind(&relation.name.table)
                    .fetch_optional(&mut *self.tx),
            )
            .await?
            .ok_or_else(|| {
                refuse("The selected PostgreSQL destination disappeared while waiting.")
            })?;
            if row.try_get::<i64, _>("oid")? != relation.oid {
                return Err(refuse(
                    "The selected PostgreSQL destination was replaced while waiting.",
                ));
            }
            if let Some(reason) = row.try_get::<Option<String>, _>("unsupported")? {
                return Err(refuse(format!(
                    "This scoped PostgreSQL destination needs a storage-specific write adapter ({reason}). \
                     An administrator may use an authorized unscoped operation; no scoped write was performed."
                )));
            }
        }
        self.check().await
    }

    async fn refresh_deadline(&mut self) -> Result<()> {
        if self.refused {
            return Err(refuse(
                "This scoped PostgreSQL transaction was already refused.",
            ));
        }
        let Some(remaining) = self.deadline.checked_duration_since(Instant::now()) else {
            self.refused = true;
            return Err(refuse(
                "The scoped PostgreSQL transaction deadline expired.",
            ));
        };
        let statement = remaining.min(self.limits.statement).as_millis().max(1);
        let lock = remaining.min(self.limits.lock).as_millis().max(1);
        let idle = remaining.as_millis().max(1);
        bounded(
            self.deadline,
            sqlx::query(
                "SELECT pg_catalog.set_config('statement_timeout', $1, true), \
             pg_catalog.set_config('lock_timeout', $2, true), \
             pg_catalog.set_config('idle_in_transaction_session_timeout', $3, true)",
            )
            .bind(format!("{statement}ms"))
            .bind(format!("{lock}ms"))
            .bind(format!("{idle}ms"))
            .execute(&mut *self.tx),
        )
        .await?;
        Ok(())
    }

    async fn check(&mut self) -> Result<()> {
        if self.refused {
            return Err(refuse(
                "This scoped PostgreSQL transaction was already refused.",
            ));
        }
        let checked =
            tokio::time::timeout_at(self.deadline, mutation::recheck(&self.authority)).await;
        let result = match checked {
            Ok(result) => result.map_err(refuse),
            Err(_) => Err(refuse("Fresh PostgreSQL write authorization expired.")),
        };
        if result.is_err() {
            self.refused = true;
        }
        result
    }

    /// Only the native operation compiler may supply this statement. This does not accept
    /// public arbitrary SQL, source expressions or statements which acquire other targets,
    /// execute functions/DDL/transaction control, alter security settings or release locks.
    /// The native adapter must prove that every affected relation is in this captured plan.
    pub(super) async fn execute_native(&mut self, sql: &str, values: &[SqlParam]) -> Result<u64> {
        if sql.is_empty()
            || sql.len() > MAX_SQL_BYTES
            || values.len() > 60_000
            || !bounded_values(values)
        {
            self.refused = true;
            return Err(refuse(
                "The native PostgreSQL write exceeds its statement bound.",
            ));
        }
        self.refresh_deadline().await?;
        self.check().await?;
        let mut query = sqlx::query(AssertSqlSafe(sql));
        for value in values {
            query = params::bind_pg(query, value);
        }
        let result = bounded(self.deadline, query.execute(&mut *self.tx)).await;
        let count = match result {
            Ok(result) => result.rows_affected(),
            Err(error) => {
                self.refused = true;
                return Err(error);
            }
        };
        // A late refusal never publishes even an otherwise-successful statement.
        self.check().await?;
        Ok(count)
    }

    /// A bounded COPY from this host's payload into one already-locked destination. The
    /// statement is constructed here; PROGRAM, server files, queries and FREEZE are absent.
    pub(super) async fn copy_csv(
        &mut self,
        relation: &RelationName,
        columns: &[String],
        data: &[u8],
    ) -> Result<u64> {
        if data.len() > MAX_COPY_BYTES
            || columns.is_empty()
            || columns.len() > 1600
            || !self
                .plan
                .relations
                .iter()
                .any(|selected| selected.name == *relation)
        {
            self.refused = true;
            return Err(refuse(
                "Invalid bounded PostgreSQL COPY destination or payload.",
            ));
        }
        let columns = match columns
            .iter()
            .map(|name| identifier(name))
            .collect::<Result<Vec<_>>>()
        {
            Ok(columns) => columns,
            Err(error) => {
                self.refused = true;
                return Err(error);
            }
        };
        let sql = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT CSV)",
            relation.quoted()?,
            columns.join(", ")
        );
        self.refresh_deadline().await?;
        self.check().await?;
        let result = bounded(self.deadline, async {
            let mut copy = self.tx.copy_in_raw(sql.as_str()).await?;
            if let Err(error) = copy.send(data).await {
                let _ = copy.abort("The bounded native COPY payload failed.").await;
                return Err(error);
            }
            copy.finish().await
        })
        .await;
        let count = match result {
            Ok(count) => count,
            Err(error) => {
                self.refused = true;
                return Err(error);
            }
        };
        // COPY is not committed by finish; original authorization is refreshed first.
        self.check().await?;
        Ok(count)
    }

    pub(super) async fn commit(mut self) -> Result<()> {
        self.refresh_deadline().await?;
        if let Err(error) = self.check().await {
            let _ = self.tx.rollback().await;
            return Err(error);
        }
        // Do not cancel an in-flight COMMIT and label its outcome "rolled back". Server
        // deadlines remain armed, but connection loss while committing is an unknown
        // outcome and must be surfaced by the caller rather than automatically replayed.
        self.tx.commit().await?;
        Ok(())
    }

    pub(super) async fn rollback(self) -> Result<()> {
        self.tx.rollback().await?;
        Ok(())
    }
}

// One bounded catalog row is returned after the exact destination locks. Catalog names are
// qualified and no policy/trigger/default expression is evaluated during classification.
const CONTROLLED_DESTINATION: &str = "SELECT c.oid::bigint AS oid, CASE \
 WHEN c.relkind <> 'r' OR c.relpersistence <> 'p' OR c.relispartition OR a.amname IS DISTINCT FROM 'heap' \
   THEN 'non-plain storage' \
 WHEN c.relrowsecurity OR c.relforcerowsecurity THEN 'row security policy' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid) \
   THEN 'inheritance or partitions' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid) THEN 'database triggers' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid) THEN 'rewrite rules' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_attribute p \
   JOIN pg_catalog.pg_type t ON t.oid=p.atttypid \
   JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace \
   LEFT JOIN pg_catalog.pg_collation co ON co.oid=p.attcollation \
   LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=co.collnamespace \
   WHERE p.attrelid=c.oid AND p.attnum>0 AND NOT p.attisdropped \
   AND (p.atthasdef OR p.attidentity<>'' OR p.attgenerated<>'' OR n.nspname<>'pg_catalog' \
     OR t.typtype<>'b' OR t.typname NOT IN \
      ('bool','int2','int4','int8','float4','float8','numeric','bytea','text','varchar','bpchar',\
       'json','jsonb','uuid','timestamp','timestamptz','date','time','timetz','interval') \
     OR (p.attcollation<>0 AND cn.nspname IS DISTINCT FROM 'pg_catalog'))) \
   THEN 'defaults, generated values or custom column types' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid \
   AND (k.contype NOT IN ('p','u','n') OR k.condeferrable OR NOT k.convalidated)) \
   THEN 'executable, foreign or deferred constraints' \
 WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i \
   JOIN pg_catalog.pg_class ix ON ix.oid=i.indexrelid \
   JOIN pg_catalog.pg_am am ON am.oid=ix.relam \
   WHERE i.indrelid=c.oid AND (am.amname<>'btree' OR i.indexprs IS NOT NULL \
    OR i.indpred IS NOT NULL OR NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive \
    OR EXISTS (SELECT 1 FROM pg_catalog.unnest(i.indclass::oid[]) cls(oid) \
      JOIN pg_catalog.pg_opclass op ON op.oid=cls.oid \
      JOIN pg_catalog.pg_namespace n ON n.oid=op.opcnamespace WHERE n.nspname<>'pg_catalog') \
    OR EXISTS (SELECT 1 FROM pg_catalog.unnest(i.indcollation::oid[]) coll(oid) \
      JOIN pg_catalog.pg_collation co ON co.oid=coll.oid \
      JOIN pg_catalog.pg_namespace n ON n.oid=co.collnamespace WHERE n.nspname<>'pg_catalog'))) \
   THEN 'custom, expression or incomplete indexes' \
 ELSE NULL END::text AS unsupported \
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace \
 LEFT JOIN pg_catalog.pg_am a ON a.oid=c.relam WHERE n.nspname=$1 AND c.relname=$2";

#[cfg(test)]
#[path = "scoped_pg_tests.rs"]
mod tests;
