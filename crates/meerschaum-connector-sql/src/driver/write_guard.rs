//! Native target guard runner. A separate one-slot SQLite writer retains
//! the existing captured authority/pin and cleans VM handlers before pool reuse.
//! It owns cancellation, VM-handler cleanup and the held writer through actual COMMIT.

use super::*;
use crate::SqlError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

const MAX_NATIVE_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_NATIVE_STATEMENTS: usize = 4096;
const MAX_NATIVE_PARAMS: usize = 1_000_000;

/// Bound native work before cloning/spawning or acquiring its writer. JSON is
/// measured through a counting writer rather than another unbounded encoded Vec.
pub(crate) fn bounded_inputs<'a>(
    statements: impl IntoIterator<Item = &'a (String, Vec<SqlParam>)>,
) -> Result<()> {
    struct Count<'a>(&'a mut usize);
    impl std::io::Write for Count<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            *self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|total| *total <= MAX_NATIVE_INPUT_BYTES)
                .ok_or_else(|| {
                    std::io::Error::other("Native window input exceeds its byte bound.")
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let (mut count, mut parameters, mut bytes) = (0usize, 0usize, 0usize);
    for (sql, params) in statements {
        count = count
            .checked_add(1)
            .filter(|count| *count <= MAX_NATIVE_STATEMENTS)
            .ok_or_else(refused)?;
        parameters = parameters
            .checked_add(params.len())
            .filter(|count| *count <= MAX_NATIVE_PARAMS)
            .ok_or_else(refused)?;
        if sql.len() > 1024 * 1024 {
            return Err(refused());
        }
        std::io::Write::write_all(&mut Count(&mut bytes), sql.as_bytes()).map_err(|_| refused())?;
        for param in params {
            let size = match param {
                SqlParam::Text(value) => value.len(),
                SqlParam::Bytes(value) => value.len(),
                SqlParam::Json(value) => {
                    serde_json::to_writer(Count(&mut bytes), value).map_err(|_| refused())?;
                    0
                }
                _ => 64,
            };
            bytes = bytes
                .checked_add(size)
                .filter(|bytes| *bytes <= MAX_NATIVE_INPUT_BYTES)
                .ok_or_else(refused)?;
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct TargetBinding {
    lineage: Arc<()>,
    file: authorization_reads::FileIdentity,
}
impl TargetBinding {
    fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lineage, &other.lineage) && self.file == other.file
    }
}
pub(crate) struct HeldWriter {
    binding: TargetBinding,
}
impl HeldWriter {
    pub(crate) fn belongs_to(&self, binding: &TargetBinding) -> bool {
        self.binding.same(binding)
    }
    async fn verify(&self, transaction: &mut SqlTransaction) -> Result<()> {
        let TxInner::Sqlite(connection) = &mut transaction.inner else {
            return Err(refused());
        };
        if authorization_reads::identity(connection, true).await? != self.binding.file {
            return Err(refused());
        }
        Ok(())
    }
    /// Only repo-owned native staging builders use this. It retains the exact
    /// held writer and checks original authority before each TEMP-only statement.
    pub(crate) async fn execute_native(
        &self,
        transaction: &mut SqlTransaction,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<u64> {
        self.verify(transaction).await?;
        transaction.check_write().await?;
        transaction.raw_execute(sql, params).await
    }
}
fn refused() -> SqlError {
    SqlError::InvalidArgument("The exact native window writer/file proof is unavailable; no unguarded target fallback is allowed.".into())
}

#[async_trait::async_trait]
pub(crate) trait TargetGuard: Send + Sync {
    fn binding(&self) -> &TargetBinding;
    /// Must use this transaction for all schema/OLD-row reads. No recursive write.
    async fn validate(&self, transaction: &mut SqlTransaction, writer: &HeldWriter) -> Result<()>;
    /// Repo-owned staging or a sealed, Main-guarded structural plan. Persistent
    /// changes must enter data_effect() and the actual native commit protocol.
    async fn prepare(&self, _: &mut SqlTransaction, _: &HeldWriter) -> Result<()> {
        Ok(())
    }
    async fn after(&self, transaction: &mut SqlTransaction, writer: &HeldWriter) -> Result<()> {
        self.validate(transaction, writer).await
    }
    fn statements(&self, _: &[(String, Vec<SqlParam>)]) -> Result<()> {
        Err(refused())
    }
    fn data_effect(
        &self,
        _counts: &[Option<u64>],
    ) -> Result<meerschaum_connector::mutation::NativeDataEffect> {
        Err(refused())
    }
    /// Final held-writer check after durable Main I/O. Target DML is complete;
    /// this cannot depend on staging retired by its own after() method.
    async fn commit_recheck(
        &self,
        transaction: &mut SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<()> {
        self.validate(transaction, writer).await
    }
}

struct CommitGuard<'a> {
    guard: &'a dyn TargetGuard,
    writer: &'a HeldWriter,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
}
#[async_trait::async_trait]
impl super::data_commit::NativeCommitGuard for CommitGuard<'_> {
    async fn recheck(&self, transaction: &mut SqlTransaction) -> Result<()> {
        if self.cancel.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            return Err(refused());
        }
        transaction
            .set_sqlite_read_budget(self.deadline, self.cancel.clone())
            .await?;
        let scope = transaction.authorization_read_scope();
        let work = async {
            self.writer.verify(transaction).await?;
            self.guard.commit_recheck(transaction, self.writer).await?;
            if self.cancel.load(Ordering::Acquire) || Instant::now() >= self.deadline {
                return Err(refused());
            }
            Ok(())
        };
        let checked = match scope {
            Some(scope) => scope.scope(work).await,
            None => work.await,
        };
        let cleared = transaction.clear_sqlite_read_budget().await;
        cleared?;
        checked
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release)
    }
}
type GuardedWrites = (
    Vec<Option<u64>>,
    Option<meerschaum_connector::mutation::NativeCommitReceipt>,
);
enum Output {
    Writes(
        Vec<Option<u64>>,
        Option<Box<meerschaum_connector::mutation::NativeCommitReceipt>>,
    ),
    Probe(Vec<Frame>),
}
struct Probe(TargetBinding);
#[async_trait::async_trait]
impl TargetGuard for Probe {
    fn binding(&self) -> &TargetBinding {
        &self.0
    }
    async fn validate(&self, _: &mut SqlTransaction, _: &HeldWriter) -> Result<()> {
        Ok(())
    }
    fn statements(&self, _: &[(String, Vec<SqlParam>)]) -> Result<()> {
        Ok(())
    }
}

impl SqlDriver {
    /// Finish an already owned, guarded native snapshot transaction. The receipt
    /// remains unacknowledged until its native metadata publication is accepted.
    pub(crate) async fn commit_windowed(
        &self,
        transaction: SqlTransaction,
        guard: &dyn TargetGuard,
        writer: &HeldWriter,
        effect: meerschaum_connector::mutation::NativeDataEffect,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
    ) -> Result<Option<meerschaum_connector::mutation::NativeCommitReceipt>> {
        transaction
            .commit_with_effect(
                effect,
                &CommitGuard {
                    guard,
                    writer,
                    deadline,
                    cancel,
                },
            )
            .await
    }
    /// Adapt an already-owned native snapshot transaction without changing its
    /// lifetime, read-pin capability or caller's truthful inference receipts.
    pub(crate) async fn held_window_writer(
        &self,
        transaction: &mut SqlTransaction,
        binding: &TargetBinding,
    ) -> Result<HeldWriter> {
        if !Arc::ptr_eq(&self.lineage, &binding.lineage) {
            return Err(refused());
        }
        let TxInner::Sqlite(connection) = &mut transaction.inner else {
            return Err(refused());
        };
        let file = authorization_reads::identity(connection, true).await?;
        let writer = HeldWriter {
            binding: TargetBinding {
                lineage: self.lineage.clone(),
                file,
            },
        };
        if !writer.belongs_to(binding) {
            return Err(refused());
        }
        Ok(writer)
    }
    pub(crate) async fn bind_window_target(&self) -> Result<TargetBinding> {
        self.ready().await?;
        if self.flavor != Flavor::Sqlite {
            return Err(refused());
        }
        let Pool::Sqlite(pool) = &self.pool else {
            return Err(refused());
        };
        let mut connection = pool.acquire().await?;
        let file = authorization_reads::identity(&mut connection, false).await?;
        Ok(TargetBinding {
            lineage: self.lineage.clone(),
            file,
        })
    }
    async fn window_writer_pool(&self) -> Result<&sqlx::SqlitePool> {
        let Pool::Sqlite(pool) = &self.pool else {
            return Err(refused());
        };
        Ok(self
            .windowed_pool
            .get_or_init(|| async {
                sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(5))
                    .after_release(|connection, _| {
                        Box::pin(async move {
                            // SQLx queues rollback before returning a dropped transaction. Taking
                            // its safe handle waits for queued work before removing the deadline.
                            // A failed clear evicts this connection instead of lending it again.
                            match connection.lock_handle().await {
                                Ok(mut locked) => {
                                    locked.remove_progress_handler();
                                    Ok(true)
                                }
                                Err(_) => Ok(false),
                            }
                        })
                    })
                    .connect_lazy_with((*pool.connect_options()).clone())
            })
            .await)
    }

    /// Own the cleanup task: dropping/cancelling the caller signals cancellation but never
    /// abandons an armed handler/held writer. The actor was captured BEFORE spawning.
    pub(crate) fn exec_many_guarded(
        &self,
        statements: Vec<(String, Vec<SqlParam>)>,
        guard: Arc<dyn TargetGuard>,
        budget: Duration,
    ) -> futures_util::future::BoxFuture<'_, Result<GuardedWrites>> {
        Box::pin(async move {
            match self.windowed_task(statements, guard, budget, false).await? {
                Output::Writes(counts, receipt) => Ok((counts, receipt.map(|value| *value))),
                Output::Probe(_) => Err(refused()),
            }
        })
    }
    pub(crate) fn probe_window_target(
        &self,
        queries: Vec<(String, Vec<SqlParam>)>,
        binding: TargetBinding,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Frame>>> + Send + '_>> {
        Box::pin(async move {
            match self
                .windowed_task(
                    queries,
                    Arc::new(Probe(binding)),
                    Duration::from_secs(15),
                    true,
                )
                .await?
            {
                Output::Probe(frames) => Ok(frames),
                Output::Writes(_, _) => Err(refused()),
            }
        })
    }
    fn windowed_task(
        &self,
        statements: Vec<(String, Vec<SqlParam>)>,
        guard: Arc<dyn TargetGuard>,
        budget: Duration,
        probe: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Output>> + Send + '_>> {
        Box::pin(async move {
            if budget.is_zero()
                || budget > Duration::from_secs(60)
                || statements.len() > 4096
                || self.flavor != Flavor::Sqlite
                || !Arc::ptr_eq(&self.lineage, &guard.binding().lineage)
            {
                return Err(refused());
            }
            bounded_inputs(&statements)?;
            guard.statements(&statements)?;
            let check = meerschaum_connector::mutation::current().ok_or_else(refused)?;
            let cancel = Arc::new(AtomicBool::new(false));
            let cancel_on_drop = CancelOnDrop(cancel.clone());
            let driver = self.clone();
            let result = tokio::spawn(async move {
                meerschaum_connector::mutation::scope(check.clone(), async move {
                    driver
                        .exec_many_guarded_inner(statements, guard, check, cancel, budget, probe)
                        .await
                })
                .await
            })
            .await
            .map_err(|_| refused())?;
            drop(cancel_on_drop);
            result
        })
    }

    async fn exec_many_guarded_inner(
        &self,
        statements: Vec<(String, Vec<SqlParam>)>,
        guard: Arc<dyn TargetGuard>,
        check: Arc<dyn meerschaum_connector::mutation::MutationCheck>,
        cancel: Arc<AtomicBool>,
        budget: Duration,
        probe: bool,
    ) -> Result<Output> {
        let deadline = Instant::now() + budget;
        let pool = self.window_writer_pool().await?;
        let native_batch = if probe { None } else { check.native_batch() };
        let transaction =
            tokio::time::timeout_at(deadline.into(), pool.begin_with("BEGIN IMMEDIATE"))
                .await
                .map_err(|_| refused())??;
        let mut transaction = SqlTransaction {
            authorization_pin: None,
            authorization_read: None,
            inner: TxInner::Sqlite(transaction),
            mutation_check: Some(check.clone()),
            write_lock: true,
            refused: false,
        };
        // The writer has its database lock. Handoff precedes every target statement.
        // A failed lock acquisition therefore leaves the owned claim unstarted.
        if let Some(batch) = &native_batch
            && let Err(reason) = batch.begin_target()
        {
            transaction.rollback().await?;
            return Err(SqlError::InvalidArgument(reason));
        }
        let writer = match self
            .held_window_writer(&mut transaction, guard.binding())
            .await
        {
            Ok(writer) => writer,
            Err(error) => {
                if probe {
                    transaction.mutation_check = None;
                }
                transaction.rollback_target(native_batch.as_ref()).await?;
                return Err(error);
            }
        };
        if check.lock_snapshot_reads() {
            transaction.authorization_pin =
                Some(authorization_reads::Owner::new(writer.binding.file.clone()));
        }
        let scope = transaction.authorization_read_scope();
        let work = async {
            transaction
                .set_sqlite_read_budget(deadline, cancel.clone())
                .await?;
            transaction.check_write().await?; // Fresh original source AFTER native lock.
            writer.verify(&mut transaction).await?;
            guard.prepare(&mut transaction, &writer).await?;
            guard.validate(&mut transaction, &writer).await?;
            writer.verify(&mut transaction).await?;
            let mut counts = Vec::with_capacity(statements.len());
            let mut frames = Vec::new();
            let mut bytes = 0usize;
            for (sql, params) in statements {
                if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
                    return Err(refused());
                }
                if probe {
                    let TxInner::Sqlite(connection) = &mut transaction.inner else {
                        return Err(refused());
                    };
                    authorization_reads::read_only_statement(connection, &sql).await?;
                    let batch = transaction.raw_query(&sql, &params).await?;
                    bytes = bytes.saturating_add(batch.get_array_memory_size());
                    if batch.num_rows() > 4097 || bytes > 8 * 1024 * 1024 {
                        return Err(refused());
                    }
                    frames.push(Frame::from_record_batches(&[batch])?);
                } else {
                    counts.push(Some(transaction.raw_execute(&sql, &params).await?));
                }
            }
            // No peer writer can change OLD rows/schema between these checks; native
            // statements and incoming normalized values must stay within this one plan.
            guard.after(&mut transaction, &writer).await?;
            writer.verify(&mut transaction).await?;
            transaction.check_write().await?;
            Ok(if probe {
                Output::Probe(frames)
            } else {
                Output::Writes(counts, None)
            })
        };
        let work = async {
            match scope {
                Some(scope) => scope.scope(work).await,
                None => work.await,
            }
        };
        let result = tokio::time::timeout_at(deadline.into(), work)
            .await
            .map_err(|_| refused())
            .and_then(|result| result);
        let clear = transaction.clear_sqlite_read_budget().await;
        if result.is_err() || clear.is_err() || cancel.load(Ordering::Acquire) {
            if probe {
                transaction.mutation_check = None;
            }
            let rollback = transaction.rollback_target(native_batch.as_ref()).await;
            if clear.is_err() {
                pool.close().await;
            }
            rollback?;
            return result.and_then(|_| Err(refused()));
        }
        // commit rechecks the actor again; the native lock remains held throughout.
        if probe {
            // A read-only native schema probe is not a target rollback receipt.
            transaction.mutation_check = None;
            transaction.rollback().await?;
        } else {
            let Output::Writes(counts, _) = result? else {
                return Err(refused());
            };
            let effect = match guard.data_effect(&counts) {
                Ok(effect) => effect,
                Err(error) => {
                    transaction.rollback_target(native_batch.as_ref()).await?;
                    return Err(error);
                }
            };
            let receipt = transaction
                .commit_with_effect(
                    effect,
                    &CommitGuard {
                        guard: guard.as_ref(),
                        writer: &writer,
                        deadline,
                        cancel,
                    },
                )
                .await?;
            return Ok(Output::Writes(counts, receipt.map(Box::new)));
        }
        result
    }
}
