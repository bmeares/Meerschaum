//! Native SQL data kernel for the main canonical-source broker. No discovery or hooks.
//! `replacement` is the main-owned two-phase SQLite source replacement, and `table` adapts the
//! connector's typed registered table read.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
mod replacement;
mod table;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::source_fetch::{self, Response};
use meerschaum_core::{Config, Map, Value};
use meerschaum_kernel::Frame;
use meerschaum_server::backends::{SourceFetchBackend, SourceFetchError, SourcePage};
use meerschaum_server::routes::source_fetch::{Binding, Plan};

use crate::native::session::Session;

type CachedConnectors = BTreeMap<String, (String, Arc<meerschaum_connector_sql::SqlConnector>)>;

/// The main host explicitly supplies its native session and config. SQL key validation always
/// precedes Session::instance, so no Python/native plugin or API transport can be selected.
pub struct NativeSqlSources {
    session: Session,
    config: Arc<Config>,
    permits: Arc<tokio::sync::Semaphore>,
    timeout: Duration,
    snapshots: Arc<crate::source_fetch::snapshot::Store>,
    initial_configuration: String,
    connectors: Arc<Mutex<CachedConnectors>>,
}

impl NativeSqlSources {
    /// Inject on the main API only. Executor runtimes retain the refusing default backend.
    pub fn new(session: Session, config: Arc<Config>) -> Self {
        let snapshots = crate::source_fetch::snapshot::Store::new(
            config.paths().root_dir.join("executors/source-snapshots"),
            session.handle(),
        );
        let initial_configuration = configuration(&config);
        Self {
            session,
            config,
            permits: Arc::new(tokio::sync::Semaphore::new(8)),
            timeout: Duration::from_secs(20),
            snapshots,
            initial_configuration,
            connectors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// A host may choose a shorter VM budget; an HTTP caller cannot extend the 20-second cap.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, SourceFetchError> {
        if timeout.is_zero() || timeout > Duration::from_secs(20) {
            return Err(SourceFetchError::Unsupported);
        }
        self.timeout = timeout;
        Ok(self)
    }

    async fn sql(
        &self,
        keys: &str,
    ) -> Result<Arc<meerschaum_connector_sql::SqlConnector>, SourceFetchError> {
        native_keys(keys)?;
        let session = self.session.clone();
        let config = self.config.clone();
        let keys = keys.to_string();
        let initial = self.initial_configuration.clone();
        let connectors = self.connectors.clone();
        tokio::task::spawn_blocking(move || {
            // Session registrations are reused only while their host-owned configuration is
            // unchanged. A reconfigured source must never retain an earlier URI/credential.
            let signature = configuration(&config);
            let mut cache = connectors.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((cached, sql)) = cache.get(&keys)
                && cached == &signature
            {
                return Ok(sql.clone());
            }
            let sql = if signature == initial {
                let instance = session
                    .instance(&keys, &config)
                    .map_err(|_| SourceFetchError::Unavailable)?;
                instance.sql().ok_or(SourceFetchError::Unsupported)?.clone()
            } else {
                let (_, label) = keys.split_once(':').ok_or(SourceFetchError::Unsupported)?;
                Arc::new(
                    session
                        .sql_connector(label, &config)
                        .map_err(|_| SourceFetchError::Unavailable)?,
                )
            };
            if sql.keys() != keys {
                return Err(SourceFetchError::Unsupported);
            }
            if !matches!(
                sql.flavor(),
                meerschaum_connector_sql::flavor::Flavor::Sqlite
                    | meerschaum_connector_sql::flavor::Flavor::Geopackage
                    | meerschaum_connector_sql::flavor::Flavor::Postgresql
            ) {
                // Other dialects require their own same-snapshot relation/catalog proof.
                return Err(SourceFetchError::Unsupported);
            }
            // Only the latest connector per configured label is retained; active operations
            // retain their own Arc until cleanup has completed.
            if cache.len() >= 64 && !cache.contains_key(&keys) {
                cache.clear();
            }
            cache.insert(keys, (signature, sql.clone()));
            Ok(sql)
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?
    }
}

fn source_statement(
    sql: &meerschaum_connector_sql::SqlConnector,
    plan: &Plan,
) -> Result<(meerschaum_connector::DataQuery, String, String), SourceFetchError> {
    let request = plan.request();
    let query = request.data_query().map_err(|_| SourceFetchError::Failed)?;
    let args = meerschaum_connector_sql::instance::fetch::FetchArgs {
        begin: meerschaum_connector_sql::instance::fetch::metadef_bound(query.begin.as_ref()),
        end: meerschaum_connector_sql::instance::fetch::metadef_bound(query.end.as_ref()),
        check_existing: false,
        params: query.params.clone(),
        ..Default::default()
    };
    let definition = sql
        .get_pipe_metadef_for_sync(plan.pipe(), &args)
        .map_err(|_| SourceFetchError::Failed)?
        .ok_or(SourceFetchError::Failed)?;
    let statement = bounded_query(
        sql.flavor(),
        &definition,
        plan.pipe(),
        query.order.as_deref(),
        request.limit.unwrap_or(i64::MAX as usize),
    )?;
    plan.check_sql(&statement)
        .map_err(|_| SourceFetchError::Forbidden)?;
    Ok((query, definition, statement))
}

struct SnapshotIdentity<'a> {
    binding: &'a Binding,
    configuration: &'a str,
}

fn capture_snapshot(
    session: &Session,
    sql: &meerschaum_connector_sql::SqlConnector,
    plan: &Plan,
    store: &Arc<crate::source_fetch::snapshot::Store>,
    identity: SnapshotIdentity<'_>,
    timeout: Duration,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
) -> Result<crate::source_fetch::snapshot::Snapshot, SourceFetchError> {
    let request = plan.request();
    let (query, definition, statement) = source_statement(sql, plan)?;
    let mut writer = store.writer()?;
    let deadline = std::time::Instant::now() + timeout;
    let schema = session.block_on_uninterruptible(async {
        let mut transaction = sql
            .driver()
            .begin_source_snapshot()
            .await
            .map_err(|_| SourceFetchError::Failed)?;
        let result = async {
            transaction
                .set_sqlite_read_budget(deadline, cancelled.clone())
                .await
                .map_err(|_| SourceFetchError::Unavailable)?;
            let schema = source_schema(&mut transaction, &statement, sql.flavor()).await?;
            let mut error = None;
            let read = transaction
                .for_each_sqlite_batch(
                    &statement,
                    source_fetch::PAGE_ROWS.min(request.row_limit),
                    source_fetch::MAX_PAGE_BYTES - 1024,
                    |frame| {
                        let capture = (|| {
                            if cancelled.load(std::sync::atomic::Ordering::Relaxed)
                                || std::time::Instant::now() >= deadline
                            {
                                return Err(SourceFetchError::Unavailable);
                            }
                            let mut selected = query
                                .select_columns
                                .clone()
                                .unwrap_or_else(|| frame.names());
                            if selected.iter().any(|column| column == "*") {
                                selected = frame.names();
                            }
                            if let Some(omit) = &query.omit_columns {
                                selected.retain(|column| !omit.contains(column));
                            }
                            let frame = frame
                                .select(&selected)
                                .map_err(|_| SourceFetchError::Failed)?;
                            writer.push(&response(&frame, true)?)
                        })();
                        capture.map_err(|failure| {
                            error = Some(failure);
                            meerschaum_connector_sql::SqlError::InvalidArgument(
                                "Source snapshot refused.".into(),
                            )
                        })
                    },
                )
                .await;
            read.map_err(|_| error.unwrap_or(SourceFetchError::Failed))?;
            if writer.is_empty() {
                writer.push(&Response {
                    version: source_fetch::VERSION,
                    dtypes: Default::default(),
                    rows: Vec::new(),
                    complete: true,
                })?;
            }
            Ok(schema)
        }
        .await;
        let cleared = transaction.clear_sqlite_read_budget().await;
        let rollback = transaction.rollback().await;
        if cleared.is_err() {
            sql.driver().close_source_snapshots().await;
            return Err(SourceFetchError::Unavailable);
        }
        rollback.map_err(|_| SourceFetchError::Unavailable)?;
        result
    })?;
    // No source transaction survives publication: native sync may write its
    // first page to this very same SQLite file without a read/write deadlock.
    if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(SourceFetchError::Unavailable);
    }
    Ok(writer.finish(
        identity.binding,
        schema,
        identity.configuration.to_string(),
        definition,
    ))
}

fn configuration(config: &Config) -> String {
    // This private signature never enters a response. Include inherited defaults/settings,
    // so connector rotation cannot reuse an old physical pool under the same logical keys.
    serde_json::to_string(&(
        config.get(&["meerschaum", "connectors", "sql"]),
        config.get(&["system", "connectors", "sql"]),
    ))
    .unwrap_or_default()
}

/// Physical definitions of the exact source relations, from the same read transaction.
/// Unrelated destination DDL may proceed between pages of the frozen snapshot.
async fn source_schema(
    transaction: &mut meerschaum_connector_sql::driver::SqlTransaction,
    statement: &str,
    flavor: meerschaum_connector_sql::Flavor,
) -> Result<String, SourceFetchError> {
    let catalog = transaction
        .read_frame("PRAGMA table_list")
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    let tables = Catalog::from_sqlite(&catalog, flavor)?.relations(statement)?;
    if tables.len() > 256 {
        return Err(SourceFetchError::TooLarge);
    }
    if tables.is_empty() {
        return Ok("[]".into());
    }
    let names = tables
        .iter()
        .map(|name| format!("'{}'", name.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let definitions = transaction.read_frame(&format!(
        "SELECT name, sql FROM main.sqlite_schema WHERE type = 'table' AND lower(name) IN ({names})"))
        .await.map_err(|_| SourceFetchError::Unavailable)?;
    if definitions.height() != tables.len() {
        return Err(SourceFetchError::Stale);
    }
    let names = definitions
        .column("name")
        .ok_or(SourceFetchError::Unavailable)?;
    let sql = definitions
        .column("sql")
        .ok_or(SourceFetchError::Unavailable)?;
    let mut proof = BTreeMap::new();
    let mut size = 0usize;
    for index in 0..definitions.height() {
        let name = names.data.get(index).py_str().to_ascii_lowercase();
        let definition = sql.data.get(index).py_str();
        size = size
            .saturating_add(name.len())
            .saturating_add(definition.len());
        if size > 4 * 1024 * 1024 {
            return Err(SourceFetchError::TooLarge);
        }
        if !tables.contains(&name) {
            return Err(SourceFetchError::Stale);
        }
        proof.insert(name, definition);
    }
    serde_json::to_string(&proof).map_err(|_| SourceFetchError::Unavailable)
}

fn native_keys(keys: &str) -> Result<(), SourceFetchError> {
    let (typ, _) = meerschaum_core::team_scope::api_execution::split_keys(keys)
        .map_err(|_| SourceFetchError::Unsupported)?;
    if typ != "sql" {
        return Err(SourceFetchError::Unsupported);
    }
    Ok(())
}

#[async_trait::async_trait]
impl SourceFetchBackend for NativeSqlSources {
    async fn native_instance(
        &self,
        keys: &str,
    ) -> Result<Arc<dyn InstanceConnector>, SourceFetchError> {
        Ok(self.sql(keys).await?)
    }

    async fn close_snapshot(
        &self,
        cursor: &source_fetch::Cursor,
        owner: Option<&str>,
        credential: &str,
    ) -> Result<(), SourceFetchError> {
        let store = self.snapshots.clone();
        let cursor = cursor.clone();
        let owner = owner.map(str::to_string);
        let credential = credential.to_string();
        tokio::task::spawn_blocking(move || store.close(&cursor, owner.as_deref(), &credential))
            .await
            .map_err(|_| SourceFetchError::Unavailable)?
    }

    async fn page(
        &self,
        plan: Plan,
        cursor: Option<source_fetch::Cursor>,
    ) -> Result<SourcePage, SourceFetchError> {
        let lease = self.snapshots.page_permit()?;
        let binding = plan.binding().clone();
        let sql = self.sql(&plan.request().source_keys).await?;
        let configuration = configuration(&self.config);
        let store = self.snapshots.clone();
        let session = self.session.clone();
        let timeout = self.timeout;
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancellation = CancelOnDrop(cancelled.clone());
        let build_permit = if cursor.is_none() {
            Some(
                self.permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| SourceFetchError::Unavailable)?,
            )
        } else {
            None
        };
        let result = tokio::task::spawn_blocking(move || {
            let _permit = build_permit;
            let table_source = plan.registered_table().is_some();
            let (definition, statement) = if table_source {
                (table::snapshot_key(&plan)?, None)
            } else {
                let (_, definition, statement) = source_statement(&sql, &plan)?;
                (definition, Some(statement))
            };
            let identity = SnapshotIdentity {
                binding: &binding,
                configuration: &configuration,
            };
            let cursor = match cursor {
                Some(cursor) => cursor,
                None => match table_source {
                    true => session.block_on_uninterruptible(table::capture(
                        &sql, &plan, &store, identity, timeout, &cancelled,
                    )),
                    false => capture_snapshot(
                        &session,
                        &sql,
                        &plan,
                        &store,
                        identity,
                        timeout,
                        cancelled.clone(),
                    ),
                }?
                .publish(),
            };
            let schema = match statement {
                None => {
                    session.block_on_uninterruptible(table::proof(&sql, &plan, timeout, &cancelled))
                }
                Some(statement) => session.block_on_uninterruptible(async {
                    let mut transaction = sql
                        .driver()
                        .begin_source_snapshot()
                        .await
                        .map_err(|_| SourceFetchError::Unavailable)?;
                    let result = async {
                        transaction
                            .set_sqlite_read_budget(
                                std::time::Instant::now() + timeout,
                                cancelled.clone(),
                            )
                            .await
                            .map_err(|_| SourceFetchError::Unavailable)?;
                        source_schema(&mut transaction, &statement, sql.flavor()).await
                    }
                    .await;
                    let cleared = transaction.clear_sqlite_read_budget().await;
                    let rollback = transaction.rollback().await;
                    if cleared.is_err() {
                        sql.driver().close_source_snapshots().await;
                        return Err(SourceFetchError::Unavailable);
                    }
                    rollback.map_err(|_| SourceFetchError::Unavailable)?;
                    result
                }),
            };
            let result = schema.and_then(|schema| {
                if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(SourceFetchError::Unavailable);
                }
                store.read(&cursor, &binding, &schema, &configuration, &definition)
            });
            if result.is_err() {
                let _ = store.close(&cursor, Some(&binding.owner), &binding.credential);
            }
            result
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
        drop(cancellation);
        result.map(|response| SourcePage {
            response,
            lease: Box::new(lease),
        })
    }

    async fn prepare_replacement(
        &self,
        plan: meerschaum_server::routes::source_fetch::ReplacementPlan,
    ) -> Result<meerschaum_connector_api::source_replace::Response, SourceFetchError> {
        self.prepare_registered_replacement(plan).await
    }
    async fn commit_replacement(
        &self,
        plan: meerschaum_server::routes::source_fetch::ReplacementPlan,
        id: &str,
    ) -> Result<meerschaum_core::success::SyncResult, SourceFetchError> {
        self.commit_registered_replacement(plan, id).await
    }

    async fn fetch(&self, plan: Plan) -> Result<Response, SourceFetchError> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SourceFetchError::Unavailable)?;
        let sql = self.sql(&plan.request().source_keys).await?;
        let session = self.session.clone();
        let timeout = self.timeout;
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancellation = CancelOnDrop(cancelled.clone());
        if plan.registered_table().is_some() {
            return table::read(&sql, &plan, timeout, &cancelled).await;
        }
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let request = plan.request();
            let query = request.data_query().map_err(|_| SourceFetchError::Failed)?;
            let args = meerschaum_connector_sql::instance::fetch::FetchArgs {
                begin: meerschaum_connector_sql::instance::fetch::metadef_bound(
                    query.begin.as_ref(),
                ),
                end: meerschaum_connector_sql::instance::fetch::metadef_bound(query.end.as_ref()),
                check_existing: false,
                params: query.params.clone(),
                ..Default::default()
            };
            // Direct query builder: never prepare_pipe/fetch, which may consult an unrestricted
            // instance provider or resolve a parent after route-level snapshot authorization.
            let definition = sql
                .get_pipe_metadef_for_sync(plan.pipe(), &args)
                .map_err(|_| SourceFetchError::Failed)?
                .ok_or(SourceFetchError::Failed)?;
            let count = request.limit.map_or(request.row_limit + 1, |limit| {
                limit.min(request.row_limit + 1)
            });
            let statement = bounded_query(
                sql.flavor(),
                &definition,
                plan.pipe(),
                query.order.as_deref(),
                count,
            )?;
            plan.check_sql(&statement)
                .map_err(|_| SourceFetchError::Forbidden)?;
            session.block_on_uninterruptible(async {
                let mut transaction = sql
                    .driver()
                    .begin_source_snapshot()
                    .await
                    .map_err(|_| SourceFetchError::Failed)?;
                let result = async {
                    transaction
                        .set_sqlite_read_budget(
                            std::time::Instant::now() + timeout,
                            cancelled.clone(),
                        )
                        .await
                        .map_err(|_| SourceFetchError::Unavailable)?;
                    let catalog = transaction
                        .read_frame("PRAGMA table_list")
                        .await
                        .map_err(|_| SourceFetchError::Unavailable)?;
                    Catalog::from_sqlite(&catalog, sql.flavor())?.check(&statement)?;
                    let mut output = Response {
                        version: source_fetch::VERSION,
                        dtypes: Default::default(),
                        rows: Vec::new(),
                        complete: true,
                    };
                    let mut encoded = EncodedBudget {
                        bytes: 128,
                        limit: source_fetch::MAX_RESPONSE_BYTES,
                    };
                    let mut seen = 0usize;
                    let mut error = None;
                    let read = transaction
                        .for_each_sqlite_batch(
                            &statement,
                            source_fetch::PAGE_ROWS.min(request.row_limit),
                            source_fetch::MAX_PAGE_BYTES - 1024,
                            |frame| {
                                let captured = (|| {
                                    if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                                        return Err(SourceFetchError::Unavailable);
                                    }
                                    seen = seen.saturating_add(frame.height());
                                    if seen > count {
                                        return Err(SourceFetchError::Failed);
                                    }
                                    let keep = frame
                                        .height()
                                        .min(request.row_limit.saturating_sub(output.rows.len()));
                                    let frame = if keep == frame.height() {
                                        frame
                                    } else {
                                        frame.take(&(0..keep).collect::<Vec<_>>())
                                    };
                                    let mut selected = query
                                        .select_columns
                                        .clone()
                                        .unwrap_or_else(|| frame.names());
                                    if selected.iter().any(|column| column == "*") {
                                        selected = frame.names();
                                    }
                                    if let Some(omit) = &query.omit_columns {
                                        selected.retain(|column| !omit.contains(column));
                                    }
                                    let frame = frame
                                        .select(&selected)
                                        .map_err(|_| SourceFetchError::Failed)?;
                                    let page = response(&frame, true)?;
                                    if output.dtypes.is_empty() {
                                        serde_json::to_writer(&mut encoded, &page.dtypes)
                                            .map_err(|_| SourceFetchError::TooLarge)?;
                                        output.dtypes = page.dtypes;
                                    } else if output.dtypes != page.dtypes {
                                        return Err(SourceFetchError::Failed);
                                    }
                                    for row in page.rows {
                                        // Charge actual JSON keys/escaping before retaining each row.
                                        // At most one bounded native batch exists beside this aggregate.
                                        serde_json::to_writer(&mut encoded, &row)
                                            .map_err(|_| SourceFetchError::TooLarge)?;
                                        std::io::Write::write_all(&mut encoded, b",")
                                            .map_err(|_| SourceFetchError::TooLarge)?;
                                        output.rows.push(row);
                                    }
                                    output.complete = seen <= request.row_limit;
                                    Ok(())
                                })();
                                captured.map_err(|failure| {
                                    error = Some(failure);
                                    meerschaum_connector_sql::SqlError::InvalidArgument(
                                        "Complete source response refused.".into(),
                                    )
                                })
                            },
                        )
                        .await;
                    read.map_err(|_| error.unwrap_or(SourceFetchError::Failed))?;
                    Ok(output)
                }
                .await;
                // The owned worker finishes handler cleanup and rollback even after the
                // original HTTP request is dropped. The ordinary pools are never altered.
                let cleared = transaction.clear_sqlite_read_budget().await;
                let rollback = transaction.rollback().await;
                if cleared.is_err() {
                    sql.driver().close_source_snapshots().await;
                    return Err(SourceFetchError::Unavailable);
                }
                rollback.map_err(|_| SourceFetchError::Unavailable)?;
                result
            })
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
        drop(cancellation);
        result
    }
}

struct EncodedBudget {
    bytes: usize,
    limit: usize,
}
impl std::io::Write for EncodedBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("source response bound"))?;
        if self.bytes > self.limit {
            return Err(std::io::Error::other("source response bound"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct CancelOnDrop(Arc<std::sync::atomic::AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A real-table allowlist from the exact transaction that executes the source statement.
/// Views, virtual/eponymous tables, attached schemas and temporary shadows cannot turn a safe
/// looking name into a read of internal/private tables or files.
struct Catalog {
    tables: BTreeSet<String>,
    shadowed: BTreeSet<String>,
}

impl Catalog {
    fn from_sqlite(
        frame: &Frame,
        flavor: meerschaum_connector_sql::Flavor,
    ) -> Result<Self, SourceFetchError> {
        if frame.height() > 4096 {
            return Err(SourceFetchError::TooLarge);
        }
        let schema = frame
            .column("schema")
            .ok_or(SourceFetchError::Unavailable)?;
        let name = frame.column("name").ok_or(SourceFetchError::Unavailable)?;
        let kind = frame.column("type").ok_or(SourceFetchError::Unavailable)?;
        let mut catalog = Self {
            tables: BTreeSet::new(),
            shadowed: BTreeSet::new(),
        };
        for index in 0..frame.height() {
            let schema = schema.data.get(index).py_str().to_ascii_lowercase();
            let name = name.data.get(index).py_str().to_ascii_lowercase();
            let kind = kind.data.get(index).py_str();
            let geopackage_catalog = flavor == meerschaum_connector_sql::Flavor::Geopackage
                && meerschaum_connector_sql::instance::geopackage::is_system_target(&name);
            if schema == "main"
                && kind == "table"
                && !name.starts_with("sqlite_")
                && !geopackage_catalog
            {
                catalog.tables.insert(name);
            } else {
                catalog.shadowed.insert(name);
            }
        }
        Ok(catalog)
    }

    fn check(&self, sql: &str) -> Result<(), SourceFetchError> {
        self.relations(sql).map(|_| ())
    }

    fn relations(&self, sql: &str) -> Result<BTreeSet<String>, SourceFetchError> {
        use sqlparser::ast::{Statement, Visit};
        let statements =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::SQLiteDialect {}, sql)
                .map_err(|_| SourceFetchError::Forbidden)?;
        if statements.len() != 1 || !matches!(&statements[0], Statement::Query(_)) {
            return Err(SourceFetchError::Forbidden);
        }
        let mut visitor = Relations {
            catalog: self,
            scopes: Vec::new(),
            visited: 0,
            used: BTreeSet::new(),
        };
        match statements.visit(&mut visitor) {
            ControlFlow::Continue(()) => Ok(visitor.used),
            ControlFlow::Break(()) => Err(SourceFetchError::Forbidden),
        }
    }
}

struct Relations<'a> {
    catalog: &'a Catalog,
    scopes: Vec<BTreeSet<String>>,
    visited: usize,
    used: BTreeSet<String>,
}

impl sqlparser::ast::Visitor for Relations<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<()> {
        self.visited += 1;
        if self.visited > 256 || self.scopes.len() >= 32 || !query.locks.is_empty() {
            return ControlFlow::Break(());
        }
        let names = query
            .with
            .as_ref()
            .map(|with| {
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();
        self.scopes.push(names);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &sqlparser::ast::Select) -> ControlFlow<()> {
        if select.into.is_some() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn pre_visit_table_factor(&mut self, table: &sqlparser::ast::TableFactor) -> ControlFlow<()> {
        use sqlparser::ast::TableFactor;
        match table {
            TableFactor::Table {
                args: None,
                version: None,
                sample: None,
                json_path: None,
                ..
            }
            | TableFactor::Derived { lateral: false, .. }
            | TableFactor::NestedJoin { .. } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }

    fn pre_visit_relation(&mut self, name: &sqlparser::ast::ObjectName) -> ControlFlow<()> {
        let parts: Option<Vec<String>> = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|ident| ident.value.to_ascii_lowercase())
            })
            .collect();
        let Some(parts) = parts else {
            return ControlFlow::Break(());
        };
        match parts.as_slice() {
            [table] if self.scopes.iter().rev().any(|scope| scope.contains(table)) => {
                ControlFlow::Continue(())
            }
            [table]
                if self.catalog.tables.contains(table)
                    && !self.catalog.shadowed.contains(table) =>
            {
                self.used.insert(table.clone());
                ControlFlow::Continue(())
            }
            [schema, table] if schema == "main" && self.catalog.tables.contains(table) => {
                self.used.insert(table.clone());
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Break(()),
        }
    }
}

pub(crate) fn bounded_query(
    flavor: meerschaum_connector_sql::flavor::Flavor,
    definition: &str,
    pipe: &meerschaum_core::Pipe,
    order: Option<&str>,
    count: usize,
) -> Result<String, SourceFetchError> {
    use meerschaum_connector_sql::flavor::Flavor;
    use meerschaum_connector_sql::ident::sql_item_name;
    let name = format!("source_rows_{}", uuid::Uuid::new_v4().simple());
    let quoted = sql_item_name(&name, flavor, None);
    let mut parent = match flavor {
        Flavor::Mssql => format!("SELECT TOP {count} * FROM {quoted}"),
        _ => format!("SELECT * FROM {quoted}"),
    };
    if let Some(order) = order {
        if !matches!(order, "asc" | "desc") {
            return Err(SourceFetchError::Failed);
        }
        let column = pipe
            .column("datetime")
            .or_else(|| pipe.column("id"))
            .or_else(|| pipe.column("primary"))
            .ok_or(SourceFetchError::Unsupported)?;
        parent.push_str(&format!(
            " ORDER BY {} {order}",
            sql_item_name(&column, flavor, None)
        ));
    }
    match flavor {
        Flavor::Mssql => {}
        Flavor::Oracle => parent.push_str(&format!(" FETCH FIRST {count} ROWS ONLY")),
        _ => parent.push_str(&format!(" LIMIT {count}")),
    }
    meerschaum_connector_sql::queries::cte::wrap_query_with_cte(definition, &parent, flavor, &name)
        .map_err(|_| SourceFetchError::Failed)
}

pub(crate) fn response(frame: &Frame, complete: bool) -> Result<Response, SourceFetchError> {
    if frame.width() > source_fetch::MAX_COLUMNS || frame.height() > source_fetch::MAX_ROWS {
        return Err(SourceFetchError::TooLarge);
    }
    // Avoid serializing/cloning an already oversized native column. These bounds cover the
    // response; database-side query memory/CPU still follows its own role/resource policy.
    let mut estimate = 0usize;
    for column in frame.columns() {
        if let meerschaum_kernel::frame::ColumnData::Native(series) = &column.data {
            estimate = estimate.saturating_add(series.estimated_size());
        }
    }
    if estimate > source_fetch::MAX_RESPONSE_BYTES {
        return Err(SourceFetchError::TooLarge);
    }
    let names = frame.names();
    if names.iter().collect::<BTreeSet<_>>().len() != frame.width() {
        return Err(SourceFetchError::Failed);
    }
    let options = meerschaum_connector_api::wire::ToJsonOptions {
        geometry_format: meerschaum_kernel::serialize::GeometryFormat::WkbHex,
        ..Default::default()
    };
    let mut bytes = 0usize;
    let mut rows = Vec::with_capacity(frame.height());
    for index in 0..frame.height() {
        let mut row = Map::new();
        for (name, value) in names.iter().zip(frame.row(index)) {
            let encoded = meerschaum_connector_api::wire::to_json_value(&value, &options);
            bytes = bytes
                .saturating_add(encoded.len())
                .saturating_add(name.len())
                .saturating_add(8);
            if bytes > source_fetch::MAX_RESPONSE_BYTES {
                return Err(SourceFetchError::TooLarge);
            }
            row.insert(
                name.clone(),
                Value::from_json_str(&encoded).map_err(|_| SourceFetchError::Failed)?,
            );
        }
        rows.push(row);
    }
    let mut dtypes = frame.dtypes();
    dtypes.extend(frame.source_dtypes());
    Ok(Response {
        version: source_fetch::VERSION,
        dtypes,
        rows,
        complete,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_exact_native_sql_keys_can_reach_the_session() {
        for keys in [
            "api:main",
            "plugin:unsafe",
            "sql:main:extra",
            "sql:default",
            "sql:../file",
        ] {
            assert!(native_keys(keys).is_err(), "{keys}");
        }
        native_keys("sql:main").unwrap();
    }
}
