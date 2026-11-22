//! [`SqlTransaction`]: an open transaction on one pooled connection, its write checks and held
//! authorization reads, and its statements, commit and rollback.

use std::fmt;

use meerschaum_kernel::Frame;

use crate::error::Result;
use crate::flavor::Flavor;

#[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
use super::native;
use super::{
    AuthorizationReadScope, SqlParam, authorization_reads, backend, batches_to_frame,
    mysql_authority,
};

/// An open transaction (SQLAlchemy `Session`): statements see each other's uncommitted
/// effects until [`SqlTransaction::commit`]. Dropping it without committing rolls back.
pub struct SqlTransaction {
    // Drop the capability before SQLx schedules rollback / returns a connection to its pool.
    pub(super) authorization_pin: Option<authorization_reads::Owner>,
    pub(super) authorization_read: Option<authorization_reads::Handle>,
    pub(super) inner: TxInner,
    pub(super) mutation_check:
        Option<std::sync::Arc<dyn meerschaum_connector::mutation::MutationCheck>>,
    pub(super) write_lock: bool,
    pub(super) refused: bool,
}

pub(super) enum TxInner {
    PinnedRead,
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    Mysql(
        sqlx::Transaction<'static, sqlx::MySql>,
        Option<mysql_authority::Policy>,
    ),
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
    Native(native::NativeTx),
}

impl fmt::Debug for SqlTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTransaction").finish_non_exhaustive()
    }
}

impl SqlTransaction {
    /// The host's frame-source callbacks remain under this exact still-held transaction.
    pub fn authorization_read_scope(&self) -> Option<AuthorizationReadScope> {
        self.authorization_pin
            .as_ref()
            .map(authorization_reads::Owner::handle)
    }

    pub(super) async fn with_held_authorization_reads<F: std::future::Future>(
        &mut self,
        future: F,
    ) -> Result<F::Output> {
        if matches!(self.inner, TxInner::Mysql(_, Some(_))) {
            let TxInner::Mysql(tx, Some(policy)) =
                std::mem::replace(&mut self.inner, TxInner::PinnedRead)
            else {
                unreachable!("verified InnoDB transaction")
            };
            let held = mysql_authority::Held::new(tx, policy.clone());
            let result = held.scope(future).await;
            self.inner = TxInner::Mysql(held.take().await?, Some(policy));
            return result;
        }
        Ok(match self.authorization_read_scope() {
            Some(scope) => scope.scope(future).await,
            None => future.await,
        })
    }

    pub(super) async fn check_write(&mut self) -> Result<()> {
        let result = Box::pin(self.check_write_inner()).await;
        if result.is_err() {
            self.refused = true;
            self.authorization_pin.take();
        }
        result
    }

    async fn check_write_inner(&mut self) -> Result<()> {
        let Some(check) = self.mutation_check.clone() else {
            return Ok(());
        };
        if self.refused || !self.write_lock {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(
                "This scoped transaction has no admitted write lock.".into(),
            ));
        }
        if self.authorization_pin.is_none() {
            return Box::pin(
                self.with_held_authorization_reads(meerschaum_connector::mutation::recheck(&check)),
            )
            .await?
            .map_err(crate::SqlError::InvalidArgument);
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(
                "This write has no native SQLite lock proof.".into(),
            ));
        };
        let identity = Box::pin(authorization_reads::identity(transaction, true)).await?;
        let pin = self.authorization_pin.as_ref().ok_or_else(|| {
            crate::SqlError::InvalidArgument(
                "This write has no lock-backed authorization snapshot.".into(),
            )
        })?;
        pin.verify(&identity)?;
        let handle = pin.handle();
        if let Err(reason) =
            Box::pin(handle.scope(meerschaum_connector::mutation::recheck(&check))).await
        {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(reason));
        }
        let identity = Box::pin(authorization_reads::identity(transaction, true)).await?;
        pin.verify(&identity)?;
        pin.freeze();
        Ok(())
    }

    /// Install a host-owned SQLite VM budget for a bounded native read. The caller must retain
    /// this transaction through cancellation, clear the handler, and roll back before pool
    /// reuse. Returning false from SQLx's safe callback interrupts sqlite3_step itself.
    pub async fn set_sqlite_read_budget(
        &mut self,
        deadline: std::time::Instant,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        if matches!(self.inner, TxInner::PinnedRead) {
            return Ok(());
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "A SQLite read budget requires a SQLite transaction.".into(),
            ));
        };
        transaction
            .lock_handle()
            .await?
            .set_progress_handler(1000, move || {
                !cancelled.load(std::sync::atomic::Ordering::Relaxed)
                    && std::time::Instant::now() < deadline
            });
        Ok(())
    }

    /// Remove a read budget before rollback/pool reuse, including every error and cancellation
    /// path. A handler is never left armed on a shared connector's next operation.
    pub async fn clear_sqlite_read_budget(&mut self) -> Result<()> {
        if matches!(self.inner, TxInner::PinnedRead) {
            return Ok(());
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "A SQLite read budget requires a SQLite transaction.".into(),
            ));
        };
        transaction.lock_handle().await?.remove_progress_handler();
        Ok(())
    }

    /// Stream one SQLite read statement through bounded frames while retaining this exact
    /// transaction. The caller owns cancellation, progress-handler cleanup and rollback.
    pub async fn for_each_sqlite_batch<F>(
        &mut self,
        sql: &str,
        chunksize: usize,
        max_bytes: usize,
        accept: F,
    ) -> Result<()>
    where
        F: FnMut(Frame) -> Result<()>,
    {
        if chunksize == 0 || chunksize > 100_000 || max_bytes == 0 || max_bytes > 8 * 1024 * 1024 {
            return Err(crate::SqlError::InvalidArgument(
                "Invalid SQLite batch size.".into(),
            ));
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "Snapshot batches require a SQLite transaction.".into(),
            ));
        };
        backend::for_each_sqlite_batch(transaction, sql, chunksize, max_bytes, accept).await
    }

    /// Execute one or more statements, returning the rows affected.
    pub async fn exec(&mut self, sql: &str) -> Result<u64> {
        self.check_write().await?;
        debug_sql!(sql);
        match &mut self.inner {
            TxInner::PinnedRead => Err(crate::SqlError::InvalidArgument(
                "An authorization snapshot cannot write.".into(),
            )),
            TxInner::Postgres(tx) => backend::pg_tx::tx_exec(tx, sql).await,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_exec(tx, sql).await,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_exec(tx, sql).await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.exec(sql).await,
        }
    }

    /// Run a query and collect the result as a kernel [`Frame`].
    pub async fn read_frame(&mut self, sql: &str) -> Result<Frame> {
        debug_sql!(sql);
        if let Some(handle) = &self.authorization_read {
            if let Some(authorization_reads::Output::Batches(batches)) =
                handle.get(sql, &[], authorization_reads::Mode::Batches)?
            {
                return batches_to_frame(&batches);
            }
            let TxInner::Sqlite(transaction) = &mut self.inner else {
                return Err(crate::SqlError::InvalidArgument(
                    "The authorization read has no live snapshot.".into(),
                ));
            };
            let (output, bytes) = backend::authorization_read(
                transaction,
                sql,
                &[],
                authorization_reads::Mode::Batches,
            )
            .await?;
            let authorization_reads::Output::Batches(batches) =
                handle.store(sql, &[], authorization_reads::Mode::Batches, output, bytes)?
            else {
                unreachable!()
            };
            return batches_to_frame(&batches);
        }
        let batches = match &mut self.inner {
            TxInner::PinnedRead => {
                return Err(crate::SqlError::InvalidArgument(
                    "Invalid authorization snapshot.".into(),
                ));
            }
            TxInner::Postgres(tx) => backend::pg_tx::tx_read(tx, sql).await?,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_read(tx, sql).await?,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_read(tx, sql).await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.read(sql).await?,
        };
        batches_to_frame(&batches)
    }

    /// Execute one parameterized statement inside this transaction, returning the rows affected.
    pub async fn exec_with(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        if params.is_empty() {
            return self.exec(sql).await;
        }
        self.check_write().await?;
        debug_sql!(sql);
        match &mut self.inner {
            TxInner::PinnedRead => Err(crate::SqlError::InvalidArgument(
                "An authorization snapshot cannot write.".into(),
            )),
            TxInner::Postgres(tx) => backend::pg_tx::tx_exec_with(tx, sql, params).await,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_exec_with(tx, sql, params).await,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_exec_with(tx, sql, params).await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.exec_with(sql, params).await,
        }
    }

    /// Run a parameterized query inside this transaction and collect the result as a kernel
    /// [`Frame`].
    pub async fn read_frame_with(&mut self, sql: &str, params: &[SqlParam]) -> Result<Frame> {
        if params.is_empty() {
            return self.read_frame(sql).await;
        }
        debug_sql!(sql);
        let batches = match &mut self.inner {
            TxInner::PinnedRead => {
                return Err(crate::SqlError::InvalidArgument(
                    "Invalid authorization snapshot.".into(),
                ));
            }
            TxInner::Postgres(tx) => backend::pg_tx::tx_read_with(tx, sql, params).await?,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_read_with(tx, sql, params).await?,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_read_with(tx, sql, params).await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.read_with(sql, params).await?,
        };
        batches_to_frame(&batches)
    }

    /// Acquire a transaction-scoped advisory lock for `key` (released automatically on commit or
    /// rollback).
    ///
    /// - PostgreSQL family (except CockroachDB): hashes `key` with SHA-256 (domain-separated) and
    ///   calls `SELECT pg_advisory_xact_lock($1)`.
    /// - SQLite / GeoPackage: when opened via [`SqlDriver::begin_write`](super::SqlDriver::begin_write), `BEGIN IMMEDIATE`
    ///   already holds the database write lock for the transaction's lifetime.
    /// - Flavors without transaction-scoped advisory locks return [`crate::SqlError::InvalidArgument`].
    pub async fn advisory_xact_lock(&mut self, flavor: Flavor, key: &str) -> Result<()> {
        match &mut self.inner {
            TxInner::Postgres(tx) => {
                if flavor == Flavor::Cockroachdb {
                    return Err(crate::SqlError::InvalidArgument(
                        "CockroachDB does not support transaction-scoped advisory locks.".into(),
                    ));
                }
                use sha2::{Digest, Sha256};
                let mut digest = Sha256::new();
                digest.update(b"meerschaum:plugin-tx-lock:");
                digest.update(key.as_bytes());
                let lock_id =
                    i64::from_be_bytes(digest.finalize()[..8].try_into().expect("eight bytes"));
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(lock_id)
                    .execute(&mut **tx)
                    .await?;
                Ok(())
            }
            TxInner::Sqlite(_) => {
                // `SqlDriver::begin_write()` opens SQLite transactions with `BEGIN IMMEDIATE`,
                // which acquires the database write lock up front (waiting up to `busy_timeout`).
                Ok(())
            }
            _ => Err(crate::SqlError::InvalidArgument(format!(
                "Flavor '{flavor}' does not support transaction-scoped advisory locks."
            ))),
        }
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        if self.mutation_check.is_some()
            && self.write_lock
            && let Err(error) = self.check_write().await
        {
            Box::pin(self.rollback()).await?;
            return Err(error);
        }
        self.authorization_pin.take();
        match self.inner {
            TxInner::PinnedRead => {
                return Err(crate::SqlError::InvalidArgument(
                    "An authorization snapshot cannot commit.".into(),
                ));
            }
            TxInner::Postgres(tx) => tx.commit().await?,
            TxInner::Mysql(tx, _) => tx.commit().await?,
            TxInner::Sqlite(tx) => tx.commit().await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.commit().await?,
        }
        Ok(())
    }

    /// Roll the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        let check = self
            .write_lock
            .then(|| self.mutation_check.clone())
            .flatten();
        self.authorization_pin.take();
        let result = match self.inner {
            TxInner::PinnedRead => Ok(()),
            TxInner::Postgres(tx) => tx.rollback().await.map_err(crate::SqlError::from),
            TxInner::Mysql(tx, _) => tx.rollback().await.map_err(crate::SqlError::from),
            TxInner::Sqlite(tx) => tx.rollback().await.map_err(crate::SqlError::from),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.rollback().await,
        };
        if let Some(check) = check {
            check.rollback_completed(result.is_ok());
        }
        result
    }
}
