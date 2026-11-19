//! Native, guarded append of already typed rows into one existing InnoDB pipe.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use meerschaum_connector::SyncOptions;
use meerschaum_connector::mutation::{
    self, DataWindow, MutationCheck, NativeBatch, NativeDataEffect,
};
use meerschaum_core::{Pipe, Value};
use sqlx::{AssertSqlSafe, Executor, MySqlConnection};

use super::{
    Pool, SqlDriver, SqlParam, SqlTransaction, TxInner, backend, data_commit::NativeCommitGuard,
    mysql::MySqlBackend, mysql_authority,
};
use crate::Result;

pub(crate) const MAX_ROWS: usize = 512;

fn refused() -> crate::SqlError {
    crate::SqlError::InvalidArgument(
        "The registered InnoDB append is unavailable for this shape.".into(),
    )
}

pub(crate) fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn quote(value: &str) -> String {
    assert!(name(value));
    format!("`{value}`")
}

pub(crate) struct Append {
    pub(crate) database: String,
    pub(crate) target: String,
    pub(crate) id: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<SqlParam>>,
    pub(crate) registration: i64,
    pub(crate) pipe: Pipe,
    pub(crate) window: DataWindow,
    pub(crate) options: SyncOptions,
}

struct Guard {
    append: Append,
    deadline: Instant,
    authorization: Arc<dyn MutationCheck>,
    cancelled: Arc<AtomicBool>,
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Guard {
    async fn pin(&self, tx: &mut MySqlConnection, table: &str) -> Result<()> {
        let qualified = format!("{}.{}", quote(&self.append.database), quote(table));
        // Open the named table in this transaction and retain its MDL through COMMIT.
        let sql = format!("SELECT 1 FROM {qualified} LIMIT 0");
        sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut *tx).await?;
        let engine: Option<String> = sqlx::query_scalar(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'",
        ).bind(&self.append.database).bind(table).fetch_optional(tx).await?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(refused());
        }
        Ok(())
    }

    async fn registration(&self, tx: &mut MySqlConnection) -> Result<()> {
        let name = format!("{}.`mrsm_pipes`", quote(&self.append.database));
        let sql = format!(
            "SELECT CAST(parameters AS CHAR) FROM {name} WHERE pipe_id=? AND connector_keys=? AND metric_key=? AND location_key <=> ? FOR UPDATE"
        );
        let raw: Option<String> = sqlx::query_scalar(AssertSqlSafe(sql))
            .bind(self.append.registration)
            .bind(self.append.pipe.connector_keys())
            .bind(self.append.pipe.metric_key())
            .bind(self.append.pipe.location_key())
            .fetch_optional(tx)
            .await?;
        let parsed = raw
            .as_deref()
            .filter(|raw| raw.len() <= 256 * 1024)
            .and_then(|raw| Value::from_json_str(raw).ok());
        if parsed.as_ref().and_then(Value::as_map) != Some(self.append.pipe.raw_parameters()) {
            return Err(refused());
        }
        Ok(())
    }

    async fn schema(&self, tx: &mut MySqlConnection) -> Result<()> {
        // A database-wide direct grant makes the trigger catalog check authoritative even
        // when a narrower INSERT-only account would silently see an empty catalog.
        let visible: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES
             WHERE TABLE_SCHEMA IN (?, ?) AND PRIVILEGE_TYPE='TRIGGER'
               AND GRANTEE=CONCAT(QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',1)),'@',
                                  QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',-1)))",
        )
        .bind(&self.append.database)
        .bind(self.append.database.replace('_', "\\_"))
        .fetch_one(&mut *tx)
        .await?;
        if visible == 0 {
            return Err(refused());
        }
        for sql in [
            "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND EVENT_OBJECT_TABLE=?",
            "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_TYPE IN ('FOREIGN KEY', 'CHECK')",
            "SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND PARTITION_NAME IS NOT NULL",
        ] {
            let count: i64 = sqlx::query_scalar(sql)
                .bind(&self.append.database)
                .bind(&self.append.target)
                .fetch_one(&mut *tx)
                .await?;
            if count != 0 {
                return Err(refused());
            }
        }
        let physical = sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, EXTRA, COLUMN_KEY
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
             ORDER BY ORDINAL_POSITION LIMIT 33",
        )
        .bind(&self.append.database)
        .bind(&self.append.target)
        .fetch_all(&mut *tx)
        .await?;
        if physical.len() != self.append.columns.len() {
            return Err(refused());
        }
        let mut ids = 0;
        for (column, dtype, full_type, extra, key) in physical {
            let position = self
                .append
                .columns
                .iter()
                .position(|name| *name == column)
                .ok_or_else(refused)?;
            if !extra.is_empty()
                || (!matches!(
                    dtype.as_str(),
                    "bigint" | "blob" | "mediumblob" | "longblob"
                ) || full_type.contains("unsigned"))
            {
                return Err(refused());
            }
            if key == "PRI" {
                if column != self.append.id || dtype != "bigint" {
                    return Err(refused());
                }
                ids += 1;
            }
            if !self.append.rows.iter().all(|row| {
                if dtype == "bigint" {
                    matches!(row[position], SqlParam::Int(_))
                } else {
                    matches!(row[position], SqlParam::Bytes(_))
                }
            }) {
                return Err(refused());
            }
        }
        if ids != 1 {
            return Err(refused());
        }
        Ok(())
    }

    fn active(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            return Err(refused());
        }
        Ok(())
    }

    async fn append(&self, transaction: &mut SqlTransaction) -> Result<NativeDataEffect> {
        let TxInner::Mysql(tx, Some(_)) = &mut transaction.inner else {
            return Err(refused());
        };
        tokio::time::timeout_at(self.deadline.into(), async {
            for table in mysql_authority::TABLES {
                self.pin(tx, table).await?;
            }
            self.pin(tx, &self.append.target).await?;
            self.registration(tx).await?;
            self.schema(tx).await
        })
        .await
        .map_err(|_| refused())??;
        self.active()?;
        transaction.check_write().await?;
        let window = transaction
            .with_held_authorization_reads(mutation::current_window(&self.authorization))
            .await?
            .map_err(crate::SqlError::InvalidArgument)?;
        if window.as_ref() != Some(&self.append.window) {
            return Err(refused());
        }
        let TxInner::Mysql(tx, Some(_)) = &mut transaction.inner else {
            return Err(refused());
        };
        let columns = self
            .append
            .columns
            .iter()
            .map(|name| quote(name))
            .collect::<Vec<_>>()
            .join(", ");
        let binds = vec!["?"; self.append.columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {}.{} ({columns}) VALUES ({binds})",
            quote(&self.append.database),
            quote(&self.append.target),
        );
        let mut affected = Vec::with_capacity(self.append.rows.len());
        for row in &self.append.rows {
            self.active()?;
            let count = tokio::time::timeout_at(
                self.deadline.into(),
                backend::bound::<MySqlBackend>(&sql, row).execute(&mut **tx),
            )
            .await
            .map_err(|_| refused())??
            .rows_affected();
            if count != 1 {
                return Err(refused());
            }
            affected.push(count);
        }
        NativeDataEffect::from_native(
            &self.append.pipe,
            Value::Int(self.append.registration),
            &self.append.window,
            &self.append.options,
            affected,
        )
        .map_err(crate::SqlError::InvalidArgument)
    }
}

#[async_trait::async_trait]
impl NativeCommitGuard for Guard {
    async fn recheck(&self, transaction: &mut SqlTransaction) -> Result<()> {
        self.active()?;
        let TxInner::Mysql(tx, Some(_)) = &mut transaction.inner else {
            return Err(refused());
        };
        self.registration(tx).await?;
        self.active()
    }
}

impl SqlDriver {
    pub(crate) async fn mysql_append(&self, append: Append) -> Result<u64> {
        let Some(authority) = mutation::current().filter(|check| check.same_store_innodb_reads())
        else {
            return Err(refused());
        };
        let native_batch = authority.native_batch().ok_or_else(refused)?;
        let driver = self.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        // The owned task observes rollback/commit after caller cancellation.
        tokio::spawn(async move {
            mutation::scope(
                authority.clone(),
                driver.mysql_append_owned(append, authority, native_batch, cancelled),
            )
            .await
        })
        .await
        .map_err(|_| refused())?
    }

    async fn mysql_append_owned(
        &self,
        append: Append,
        authority: Arc<dyn MutationCheck>,
        batch: NativeBatch,
        cancelled: Arc<AtomicBool>,
    ) -> Result<u64> {
        if !name(&append.database)
            || !name(&append.target)
            || !name(&append.id)
            || append.columns.iter().any(|column| !name(column))
            || mysql_authority::TABLES
                .iter()
                .any(|table| table.eq_ignore_ascii_case(&append.target))
        {
            return Err(refused());
        }
        let Pool::Mysql(pool) = &self.pool else {
            return Err(refused());
        };
        let dedicated = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(|connection, _| Box::pin(async move {
                connection.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE").await?;
                connection.execute("SET SESSION innodb_lock_wait_timeout=3, lock_wait_timeout=3, sql_mode='STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'").await?;
                Ok(())
            }))
            .connect_lazy_with((*pool.connect_options()).clone());
        let deadline = Instant::now() + Duration::from_secs(60);
        let native = dedicated.begin_with("START TRANSACTION READ WRITE").await?;
        let policy = mysql_authority::Policy {
            lineage: self.lineage.clone(),
            deadline,
        };
        let mut transaction = SqlTransaction {
            authorization_pin: None,
            authorization_read: None,
            inner: TxInner::Mysql(native, Some(policy)),
            mutation_check: Some(authority.clone()),
            write_lock: true,
            refused: false,
        };
        if let Err(reason) = batch.begin_target() {
            transaction.rollback().await?;
            return Err(crate::SqlError::InvalidArgument(reason));
        }
        let guard = Guard {
            append,
            deadline,
            authorization: authority,
            cancelled,
        };
        // Only individual SQL awaits are timed out. Keep the owned authorization loan
        // alive so failure can explicitly roll back its real transaction.
        let result = guard.append(&mut transaction).await;
        let effect = match result {
            Ok(effect) => effect,
            Err(error) => {
                transaction.rollback_target(Some(&batch)).await?;
                return Err(error);
            }
        };
        let count = effect.affected().iter().sum();
        let receipt = transaction
            .commit_with_effect(effect, &guard)
            .await?
            .ok_or_else(refused)?;
        receipt
            .acknowledge()
            .await
            .map_err(crate::SqlError::InvalidArgument)?;
        Ok(count)
    }
}
