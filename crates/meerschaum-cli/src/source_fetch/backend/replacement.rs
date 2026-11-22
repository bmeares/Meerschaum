//! Main-owned two-phase SQLite source replacement. This is a child of the native source
//! backend; no executor or public caller can supply SQL, spool paths or mutation authority.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use meerschaum_connector::mutation::MutationCheck;
use meerschaum_connector::{ConnectorError, ConnectorResult, SyncOptions, async_trait};
use meerschaum_connector_api::{source_fetch, source_replace};
use meerschaum_connector_sql::instance::sync::snapshot::{Frames, Plan as SqlPlan};
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_kernel::Frame;
use meerschaum_server::backends::SourceFetchError;
use meerschaum_server::routes::source_fetch::ReplacementPlan;

use super::{
    CancelOnDrop, NativeSqlSources, SnapshotIdentity, capture_snapshot, configuration,
    source_schema, source_statement,
};
use crate::source_fetch::snapshot::Snapshot;

struct Ready {
    plan: SqlPlan,
    request: String,
}

#[derive(Clone)]
struct Authority {
    main: Arc<dyn MutationCheck>,
    receipt: Arc<dyn MutationCheck>,
    snapshot: Arc<Snapshot>,
    source: Arc<SqlConnector>,
    config: Arc<meerschaum_core::Config>,
    statement: String,
    cancelled: Arc<AtomicBool>,
    refused: Arc<AtomicBool>,
    timeout: Duration,
}

impl Authority {
    async fn validate(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::Relaxed) || self.snapshot.remaining().is_zero() {
            return Err("The source replacement was cancelled or expired.".into());
        }
        self.main
            .check()
            .await
            .map_err(|error| format!("Before source proof: {error}"))?;
        if configuration(&self.config) != self.snapshot.configuration() {
            return Err("The trusted source configuration changed.".into());
        }
        let mut transaction = self
            .source
            .driver()
            .begin_source_snapshot()
            .await
            .map_err(|_| "The canonical source is unavailable.")?;
        let proof = async {
            transaction
                .set_sqlite_read_budget(
                    Instant::now() + self.timeout.min(self.snapshot.remaining()),
                    self.cancelled.clone(),
                )
                .await
                .map_err(|_| "The canonical source is unavailable.")?;
            source_schema(&mut transaction, &self.statement, self.source.flavor())
                .await
                .map_err(|_| "The canonical source proof changed.")
        }
        .await;
        let cleared = transaction.clear_sqlite_read_budget().await;
        let rollback = transaction.rollback().await;
        if cleared.is_err() {
            self.source.driver().close_source_snapshots().await;
            return Err("The canonical source read could not be cleaned up.".into());
        }
        rollback.map_err(|_| "The canonical source read could not be cleaned up.")?;
        if proof? != self.snapshot.schema() {
            return Err("The canonical source relation changed.".into());
        }
        if self.cancelled.load(Ordering::Relaxed) || self.snapshot.remaining().is_zero() {
            return Err("The source replacement was cancelled or expired.".into());
        }
        // Source pool/catalog waits do not carry stale caller/source/destination authority
        // into the very next native mutation or page read.
        self.main
            .check()
            .await
            .map_err(|error| format!("After source proof: {error}"))
    }
}
#[async_trait]
impl MutationCheck for Authority {
    fn native_batch(&self) -> Option<meerschaum_connector::mutation::NativeBatch> {
        self.main.native_batch()
    }
    async fn write_window(
        &self,
    ) -> Result<Option<meerschaum_connector::mutation::DataWindow>, String> {
        self.validate().await?;
        meerschaum_connector::mutation::current_window(&self.main).await
    }
    async fn prepare_data_commit(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<Option<Arc<dyn meerschaum_connector::mutation::PendingDataCommit>>, String> {
        self.validate().await?;
        Ok(self.main.prepare_data_commit(effect).await?.map(|inner| {
            Arc::new(SourcePending {
                authority: self.clone(),
                inner,
            }) as Arc<dyn meerschaum_connector::mutation::PendingDataCommit>
        }))
    }
    fn rollback_completed(&self, succeeded: bool) {
        self.main.rollback_completed(succeeded);
    }
    fn tracks_metadata_inference(&self) -> bool {
        self.main.tracks_metadata_inference()
    }

    async fn metadata_inferred(
        &self,
        change: meerschaum_connector::mutation::MetadataInference,
    ) -> Result<(), String> {
        self.main.metadata_inferred(change).await
    }

    fn lock_snapshot_reads(&self) -> bool {
        true
    }

    async fn check(&self) -> Result<(), String> {
        let result = self.validate().await;
        if let Err(error) = &result
            && !self.refused.swap(true, Ordering::Relaxed)
        {
            let message = meerschaum_core::debug::redact_text(error)
                .chars()
                .take(256)
                .collect::<String>();
            eprintln!("Native source replacement admission refused: {message}");
        }
        result
    }
}
struct SourcePending {
    authority: Authority,
    inner: Arc<dyn meerschaum_connector::mutation::PendingDataCommit>,
}
#[async_trait]
impl meerschaum_connector::mutation::PendingDataCommit for SourcePending {
    async fn before_commit(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        self.authority.validate().await?;
        self.inner.before_commit(effect).await
    }
    async fn committed(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        self.inner.committed(effect).await
    }
    async fn acknowledge(
        &self,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<(), String> {
        let mut receipt = self.authority.clone();
        receipt.main = receipt.receipt.clone();
        receipt.validate().await?;
        self.inner.acknowledge(effect).await
    }
    async fn rolled_back(&self) -> Result<(), String> {
        self.inner.rolled_back().await
    }
}

struct Reader {
    snapshot: Arc<Snapshot>,
    authority: Arc<Authority>,
    next: usize,
    eof: bool,
    snapshot_sha256: Option<String>,
}
#[async_trait]
impl Frames for Reader {
    async fn next(&mut self) -> ConnectorResult<Option<Frame>> {
        // As with the driver's statement checks, canonical authorization reads must not
        // inherit the outer writer scope and recursively acquire its one writer slot.
        let captured: Arc<dyn MutationCheck> = self.authority.clone();
        meerschaum_connector::mutation::recheck(&captured)
            .await
            .map_err(ConnectorError::Backend)?;
        if self.next == self.snapshot.pages() {
            self.eof = true;
            return Ok(None);
        }
        let response = self.snapshot.read(self.next).map_err(|_| {
            ConnectorError::Backend("The frozen source page is unavailable.".into())
        })?;
        self.next += 1;
        decode(response).map(Some)
    }
    fn completed_source(&self) -> Option<&str> {
        self.snapshot_sha256.as_deref().filter(|_| self.eof)
    }
    async fn rewind(&mut self) -> ConnectorResult<()> {
        self.eof = false;
        self.next = 0;
        Ok(())
    }
}

fn decode(response: source_fetch::Response) -> ConnectorResult<Frame> {
    let docs = response
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(name, value)| (name, meerschaum_kernel::value::Value::from(value)))
                .collect()
        })
        .collect::<Vec<meerschaum_connector::pipe_ops::Doc>>();
    let frame = meerschaum_kernel::docs::frame_from_docs(&docs)
        .map_err(|_| ConnectorError::Invalid("The frozen source records are invalid.".into()))?;
    meerschaum_kernel::enforce::enforce_dtypes(&frame, &response.dtypes, &Default::default())
        .map_err(|_| ConnectorError::Invalid("The frozen source types are invalid.".into()))
}
fn summary(plan: &SqlPlan) -> source_replace::Summary {
    plan.summary().into()
}

impl NativeSqlSources {
    pub(super) async fn prepare_registered_replacement(
        &self,
        plan: ReplacementPlan,
    ) -> Result<source_replace::Response, SourceFetchError> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SourceFetchError::Unavailable)?;
        let source = self.sql(&plan.source().request().source_keys).await?;
        let target = self.sql(plan.destination().instance_keys()).await?;
        if source.flavor() != Flavor::Sqlite || target.flavor() != Flavor::Sqlite {
            return Err(SourceFetchError::Unsupported);
        }
        let store = self.snapshots.clone();
        let session = self.session.clone();
        let config = self.config.clone();
        let timeout = self.timeout;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = CancelOnDrop(cancelled.clone());
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            session
                .block_on_uninterruptible(plan.authority().check())
                .map_err(|_| SourceFetchError::Forbidden)?;
            let config_proof = configuration(&config);
            let mut snapshot = capture_snapshot(
                &session,
                &source,
                plan.source(),
                &store,
                SnapshotIdentity {
                    binding: plan.binding(),
                    configuration: &config_proof,
                },
                timeout,
                cancelled.clone(),
            )?;
            snapshot.reserve_receipt()?;
            let snapshot = Arc::new(snapshot);
            let (_, definition, statement) = source_statement(&source, plan.source())?;
            if definition != snapshot.query() {
                return Err(SourceFetchError::Stale);
            }
            let refused = Arc::new(AtomicBool::new(false));
            let authority = Arc::new(Authority {
                main: plan.authority(),
                receipt: plan.receipt_authority(),
                snapshot: snapshot.clone(),
                source,
                config,
                statement,
                cancelled: cancelled.clone(),
                refused: refused.clone(),
                timeout,
            });
            let mut reader = Reader {
                snapshot: snapshot.clone(),
                authority: authority.clone(),
                next: 0,
                eof: false,
                snapshot_sha256: None,
            };
            let query = plan
                .source()
                .request()
                .data_query()
                .map_err(|_| SourceFetchError::Failed)?;
            let options = SyncOptions {
                replace: true,
                begin: query.begin,
                end: query.end,
                params: query.params,
                ..Default::default()
            };
            let sql_plan = session
                .block_on_uninterruptible(meerschaum_connector::mutation::scope(
                    authority.clone(),
                    target.plan_snapshot_replace(
                        plan.destination(),
                        plan.source().pipe(),
                        &mut reader,
                        &options,
                        plan.enforce_dtypes(),
                    ),
                ))
                .map_err(|_| {
                    if refused.load(Ordering::Relaxed) {
                        SourceFetchError::Forbidden
                    } else {
                        SourceFetchError::Failed
                    }
                })?;
            session
                .block_on_uninterruptible(authority.check())
                .map_err(|_| SourceFetchError::Forbidden)?;
            let summary = summary(&sql_plan);
            drop(reader);
            drop(authority);
            let mut snapshot =
                Arc::try_unwrap(snapshot).map_err(|_| SourceFetchError::Unavailable)?;
            snapshot.set_prepared(Ready {
                plan: sql_plan,
                request: plan.request_identity().to_string(),
            });
            let id = snapshot.publish().id;
            Ok(source_replace::Response::Prepared { id, summary })
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
        drop(cancellation);
        result
    }

    pub(super) async fn commit_registered_replacement(
        &self,
        plan: ReplacementPlan,
        id: &str,
    ) -> Result<meerschaum_core::success::SyncResult, SourceFetchError> {
        let config_proof = configuration(&self.config);
        // A matching completed receipt returns its original result, never another DELETE.
        if let Some(result) = self
            .snapshots
            .completed(id, plan.binding(), plan.request_identity(), &config_proof)
            .await?
        {
            plan.receipt_authority()
                .check()
                .await
                .map_err(|_| SourceFetchError::Forbidden)?;
            return Ok(result);
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = CancelOnDrop(cancelled.clone());
        let snapshot = match self.snapshots.take(
            id,
            plan.binding(),
            plan.request_identity(),
            cancelled.clone(),
        ) {
            Ok(snapshot) => snapshot,
            Err(SourceFetchError::Closed) => {
                if let Some(result) = self
                    .snapshots
                    .completed(id, plan.binding(), plan.request_identity(), &config_proof)
                    .await?
                {
                    plan.receipt_authority()
                        .check()
                        .await
                        .map_err(|_| SourceFetchError::Forbidden)?;
                    return Ok(result);
                }
                return Err(SourceFetchError::Uncertain);
            }
            Err(error) => return Err(error),
        };
        let ready = snapshot
            .prepared::<Ready>()
            .ok_or(SourceFetchError::Stale)?;
        if ready.request != plan.request_identity() {
            return Err(SourceFetchError::Stale);
        }
        let sql_plan = ready.plan.clone();
        let snapshot = Arc::new(snapshot);
        let source = self.sql(&plan.source().request().source_keys).await?;
        let target = self.sql(plan.destination().instance_keys()).await?;
        if source.flavor() != Flavor::Sqlite || target.flavor() != Flavor::Sqlite {
            return Err(SourceFetchError::Unsupported);
        }
        let (_, definition, statement) = source_statement(&source, plan.source())?;
        if definition != snapshot.query() || configuration(&self.config) != snapshot.configuration()
        {
            return Err(SourceFetchError::Stale);
        }
        let session = self.session.clone();
        let config = self.config.clone();
        let timeout = self.timeout;
        let result = tokio::task::spawn_blocking(move || {
            let refused = Arc::new(AtomicBool::new(false));
            let authority = Arc::new(Authority {
                main: plan.authority(),
                receipt: plan.receipt_authority(),
                snapshot: snapshot.clone(),
                source,
                config,
                statement,
                cancelled: cancelled.clone(),
                refused: refused.clone(),
                timeout,
            });
            let request = plan.source().request();
            let snapshot_sha256 = if request.source_keys == request.instance_keys
                && request.select_columns.is_none()
                && request.omit_columns.is_none()
                && request
                    .params
                    .as_ref()
                    .is_none_or(|params| params.is_empty())
                && request.limit.is_none()
                && plan.enforce_dtypes()
            {
                Some(meerschaum_server::executor::plugins::sha256(
                    format!("{}:{}", snapshot.id(), plan.request_identity()).as_bytes(),
                ))
            } else {
                None
            };
            let mut reader = Reader {
                snapshot: snapshot.clone(),
                authority: authority.clone(),
                next: 0,
                eof: false,
                snapshot_sha256,
            };
            let budget = snapshot.remaining().min(Duration::from_secs(300));
            let result = session
                .block_on_uninterruptible(meerschaum_connector::mutation::scope(
                    authority,
                    target.apply_snapshot_replace(&sql_plan, &mut reader, cancelled, budget),
                ))
                .map_err(|_| {
                    if refused.load(Ordering::Relaxed) {
                        SourceFetchError::Forbidden
                    } else {
                        SourceFetchError::Failed
                    }
                })?;
            snapshot.complete(plan.request_identity(), result.clone());
            Ok(result)
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
        drop(cancellation);
        result
    }
}
