//! PostgreSQL canonical source broker with pinned native relation identities.
//!
//! This is a separately injected backend; the main backend's SQLite selection is unchanged.
//! PostgreSQL admission requires all of these native checks:
//!
//! 1. BEGIN READ ONLY with READ COMMITTED catalogs, pin namespace bindings with a SHARE lock
//!    on pg_namespace, then pin relation DDL with SHARE UPDATE EXCLUSIVE locks and revalidate
//!    the fresh catalog/OID bindings before reading data.
//!    A Rust timeout alone does not cancel server work: use native statement/lock timeouts.
//! 2. Preserve the actual schema search order while putting pg_catalog first. Resolve bare
//!    table names through pg_table_is_visible in that same transaction, never a global cache.
//! 3. Use this physical-relation/inheritance catalog proof; reject views, materialized views,
//!    foreign relations, unsafe inheritance components and unproved object types/policies.
//! 4. The dialect AST pass qualifies admitted pure functions AND overloadable operators to
//!    pg_catalog. Search-path order alone does not prevent a better matching user overload.
//!    Reject explicit user/schema functions, custom operators, user casts/collations, and
//!    non-pg_catalog types. SQL's intrinsic COALESCE/NULLIF/CAST syntax needs separate handling.
//! 5. Preserve quoted identifier case exactly; fold only ASCII unquoted identifiers. Initially
//!    refuse unquoted non-ASCII identifiers rather than assume the server locale's folding.
//! 6. SQLx 0.9 has no public PG cancel token in this workspace. statement_timeout provides real
//!    server cancellation at the budget; immediate request-drop cancellation needs a separate
//!    proved driver-owned mechanism. Keep the transaction owned until rollback completes.
//! 7. Opt-in owned PG acceptance covers views/matviews, inheritance/foreign descendants,
//!    function/operator/cast paths, typed bounds, joins/templates, quoted Unicode names and
//!    native lock/statement cancellation with pool reuse.
//!
//! Initial unsupported cases must remain explicit: other dialects; extension/custom types
//! (including PostGIS until trusted-extension proof); virtual generated columns; RLS; expression
//! or partial indexes and non-catalog operator classes; USING/NATURAL and unproved SQL forms.
//! This intermediate proof requires privileged catalog-lock access (usually superuser), blocks
//! global schema DDL while the bounded read transaction is active, and refuses SELECT-only roles
//! and hot standbys. It is not general PostgreSQL source compatibility. The native budget starts
//! before bounded pool acquisition; transport/rollback time is not an end-to-end drop guarantee.
//! These are explicit compatibility limits, with no raw-query fallback. The
//! complete-only wire also still needs real large-source paging.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::source_fetch::Response;
use meerschaum_core::Config;
use meerschaum_kernel::Frame;
use meerschaum_server::backends::{SourceFetchBackend, SourceFetchError};
use meerschaum_server::routes::source_fetch::Plan;
use sqlparser::ast::{self, VisitMut};

use crate::native::Session;

const MAX_RELATIONS: usize = 16_384;
const MAX_INHERITANCE: usize = 32_768;

/// Run after BEGIN READ ONLY, before any snapshot-setting read. Time values are fixed host
/// integers, never caller text. A failed policy statement means no source query is admitted.
pub(crate) fn transaction_policy(timeout_ms: u64) -> Result<Vec<String>, SourceFetchError> {
    if timeout_ms == 0 || timeout_ms > 20_000 {
        return Err(SourceFetchError::Unsupported);
    }
    Ok(vec![
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED".into(),
        format!("SET LOCAL statement_timeout = '{timeout_ms}ms'"),
        format!("SET LOCAL lock_timeout = '{}ms'", timeout_ms.min(2_000)),
    ])
}

/// Capture the configured schema order before prepending pg_catalog. Quote each returned
/// schema as an identifier when building SET LOCAL search_path; never interpolate raw names.
pub(crate) const SCHEMAS_SQL: &str = "SELECT schema_name::pg_catalog.text AS schema_name, ordinal::pg_catalog.int8 AS ordinal \
    FROM pg_catalog.unnest(pg_catalog.current_schemas(false)) WITH ORDINALITY AS s(schema_name, ordinal) \
    ORDER BY ordinal LIMIT 65";

/// Catalog values are plain data, never a VIEW definition to execute or trust as authorized.
/// The extra row detects a catalog that cannot fit the fixed metadata allowance.
pub(crate) const RELATIONS_SQL: &str = "SELECT c.oid::pg_catalog.text AS oid, n.nspname::pg_catalog.text AS schema_name, \
    c.relname::pg_catalog.text AS table_name, c.relkind::pg_catalog.text AS kind, \
    pg_catalog.pg_table_is_visible(c.oid) AS visible, c.relrowsecurity AS rls, \
    EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a \
      JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) a.atttypid \
      JOIN pg_catalog.pg_namespace tn ON tn.oid OPERATOR(pg_catalog.=) t.typnamespace \
      WHERE a.attrelid OPERATOR(pg_catalog.=) c.oid AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped \
      AND (tn.nspname::pg_catalog.text OPERATOR(pg_catalog.<>) 'pg_catalog' \
        OR t.typtype::pg_catalog.text OPERATOR(pg_catalog.<>) 'b' \
        OR a.attgenerated::pg_catalog.text OPERATOR(pg_catalog.=) 'v' \
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_opclass oc2 \
          JOIN pg_catalog.pg_namespace ons2 ON ons2.oid OPERATOR(pg_catalog.=) oc2.opcnamespace \
          WHERE oc2.opcintype OPERATOR(pg_catalog.=) t.oid AND oc2.opcdefault \
            AND ons2.nspname::pg_catalog.text OPERATOR(pg_catalog.<>) 'pg_catalog'))) AS unsafe_columns, \
    EXISTS (SELECT 1 FROM pg_catalog.pg_index i \
      CROSS JOIN LATERAL pg_catalog.unnest(i.indclass) AS op(opclass_oid) \
      JOIN pg_catalog.pg_opclass oc ON oc.oid OPERATOR(pg_catalog.=) op.opclass_oid \
      JOIN pg_catalog.pg_namespace onsp ON onsp.oid OPERATOR(pg_catalog.=) oc.opcnamespace \
      WHERE i.indrelid OPERATOR(pg_catalog.=) c.oid AND (i.indexprs IS NOT NULL OR i.indpred IS NOT NULL \
        OR onsp.nspname::pg_catalog.text OPERATOR(pg_catalog.<>) 'pg_catalog')) AS unsafe_indexes \
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace \
    WHERE n.nspname::pg_catalog.text OPERATOR(pg_catalog.<>) 'pg_catalog' \
      AND n.nspname::pg_catalog.text OPERATOR(pg_catalog.<>) 'information_schema' \
      AND n.nspname::pg_catalog.text OPERATOR(pg_catalog.!~~) 'pg_toast%' LIMIT 16385";

/// Hidden parents also hide their physical descendants; a readable parent cannot bypass a
/// hidden child or route through a foreign partition. Consider the entire connected component.
pub(crate) const INHERITANCE_SQL: &str = "SELECT inhparent::pg_catalog.text AS parent_oid, inhrelid::pg_catalog.text AS child_oid \
    FROM pg_catalog.pg_inherits LIMIT 32769";

#[derive(Debug)]
struct Relation {
    schema: String,
    name: String,
    visible: bool,
    safe: bool,
}

/// An allowlist of physical source identities, rebuilt after schema locks in the transaction
/// that will execute a separately proved dialect query. It is insufficient alone for admission.
pub(crate) struct Catalog {
    relations: BTreeMap<u64, Relation>,
    denied: BTreeSet<u64>,
    adjacent: BTreeMap<u64, Vec<u64>>,
}

fn text(frame: &Frame, column: &str, row: usize) -> Result<String, SourceFetchError> {
    let value = frame
        .column(column)
        .ok_or(SourceFetchError::Unavailable)?
        .data
        .get(row)
        .py_str();
    if value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(SourceFetchError::TooLarge);
    }
    Ok(value)
}

fn boolean(frame: &Frame, column: &str, row: usize) -> Result<bool, SourceFetchError> {
    match text(frame, column, row)?.as_str() {
        "True" | "true" => Ok(true),
        "False" | "false" => Ok(false),
        _ => Err(SourceFetchError::Unavailable),
    }
}

impl Catalog {
    /// `visible_to_caller` must use the fresh main-owned hidden/internal-target guard, including
    /// physical target aliases. It is called only on quoted exact catalog identifiers.
    pub(crate) fn from_frames(
        relations: &Frame,
        inheritance: &Frame,
        mut visible_to_caller: impl FnMut(&str, &str) -> bool,
    ) -> Result<Self, SourceFetchError> {
        if relations.height() > MAX_RELATIONS || inheritance.height() > MAX_INHERITANCE {
            return Err(SourceFetchError::TooLarge);
        }
        let mut catalog = Self {
            relations: BTreeMap::new(),
            denied: BTreeSet::new(),
            adjacent: BTreeMap::new(),
        };
        for row in 0..relations.height() {
            let oid = text(relations, "oid", row)?
                .parse()
                .map_err(|_| SourceFetchError::Unavailable)?;
            let schema = text(relations, "schema_name", row)?;
            let name = text(relations, "table_name", row)?;
            let kind = text(relations, "kind", row)?;
            let safe = matches!(kind.as_str(), "r" | "p")
                && !schema.starts_with("pg_")
                && !boolean(relations, "rls", row)?
                && !boolean(relations, "unsafe_columns", row)?
                && !boolean(relations, "unsafe_indexes", row)?;
            if !safe || !visible_to_caller(&schema, &name) {
                catalog.denied.insert(oid);
            }
            if catalog
                .relations
                .insert(
                    oid,
                    Relation {
                        schema,
                        name,
                        visible: boolean(relations, "visible", row)?,
                        safe,
                    },
                )
                .is_some()
            {
                return Err(SourceFetchError::Unavailable);
            }
        }
        let mut edges = Vec::with_capacity(inheritance.height());
        for row in 0..inheritance.height() {
            let parent = text(inheritance, "parent_oid", row)?
                .parse()
                .map_err(|_| SourceFetchError::Unavailable)?;
            let child = text(inheritance, "child_oid", row)?
                .parse()
                .map_err(|_| SourceFetchError::Unavailable)?;
            if !catalog.relations.contains_key(&parent) || !catalog.relations.contains_key(&child) {
                // A missing catalog identity cannot be taken as an uninteresting partition.
                catalog.denied.insert(parent);
                catalog.denied.insert(child);
            }
            edges.push((parent, child));
        }
        for (parent, child) in edges {
            catalog.adjacent.entry(parent).or_default().push(child);
            catalog.adjacent.entry(child).or_default().push(parent);
        }
        let mut queue: VecDeque<u64> = catalog.denied.iter().copied().collect();
        while let Some(oid) = queue.pop_front() {
            for neighbor in catalog.adjacent.get(&oid).into_iter().flatten() {
                if catalog.denied.insert(*neighbor) {
                    queue.push_back(*neighbor);
                }
            }
        }
        Ok(catalog)
    }

    /// Quoted names retain exact spelling. The AST pass must supply already normalized
    /// identifiers, using PostgreSQL folding rules rather than SQLite's ASCII comparisons.
    fn resolve(&self, schema: Option<&str>, name: &str) -> Option<(u64, &Relation)> {
        self.relations
            .iter()
            .find(|(oid, relation)| {
                !self.denied.contains(oid)
                    && relation.safe
                    && relation.name == name
                    && schema.map_or(relation.visible, |schema| schema == relation.schema)
            })
            .map(|(oid, relation)| (*oid, relation))
    }

    fn component(&self, roots: &BTreeSet<u64>) -> Result<BTreeSet<u64>, SourceFetchError> {
        let mut out = roots.clone();
        let mut queue: VecDeque<u64> = roots.iter().copied().collect();
        while let Some(oid) = queue.pop_front() {
            if self.denied.contains(&oid) || out.len() > 256 {
                return Err(SourceFetchError::Forbidden);
            }
            for child in self.adjacent.get(&oid).into_iter().flatten() {
                if out.insert(*child) {
                    queue.push_back(*child);
                }
            }
        }
        Ok(out)
    }
}

/// Separate native PG implementation. Main production selection remains explicit; constructing
/// this backend does not change SQLite admission or grant any command permissions.
pub struct PostgresSources {
    session: Session,
    config: Arc<Config>,
    permits: Arc<tokio::sync::Semaphore>,
    timeout: Duration,
}

impl PostgresSources {
    /// A trusted host supplies its session/config; source queries are PG only.
    pub fn new(session: Session, config: Arc<Config>) -> Self {
        Self {
            session,
            config,
            permits: Arc::new(tokio::sync::Semaphore::new(8)),
            timeout: Duration::from_secs(20),
        }
    }

    /// Host-only shorter budget (including tests); HTTP callers cannot increase it.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, SourceFetchError> {
        if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(20) {
            return Err(SourceFetchError::Unsupported);
        }
        self.timeout = timeout;
        Ok(self)
    }

    async fn sql(
        &self,
        keys: &str,
    ) -> Result<Arc<meerschaum_connector_sql::SqlConnector>, SourceFetchError> {
        let (typ, _) = meerschaum_core::team_scope::api_execution::split_keys(keys)
            .map_err(|_| SourceFetchError::Unsupported)?;
        if typ != "sql" {
            return Err(SourceFetchError::Unsupported);
        }
        let session = self.session.clone();
        let config = self.config.clone();
        let keys = keys.to_string();
        tokio::task::spawn_blocking(move || {
            let instance = session
                .instance(&keys, &config)
                .map_err(|_| SourceFetchError::Unavailable)?;
            let sql = instance.sql().ok_or(SourceFetchError::Unsupported)?.clone();
            if sql.keys() != keys {
                return Err(SourceFetchError::Unsupported);
            }
            Ok(sql)
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?
    }
}

#[async_trait::async_trait]
impl SourceFetchBackend for PostgresSources {
    async fn native_instance(
        &self,
        keys: &str,
    ) -> Result<Arc<dyn InstanceConnector>, SourceFetchError> {
        Ok(self.sql(keys).await?)
    }

    async fn fetch(&self, plan: Plan) -> Result<Response, SourceFetchError> {
        use meerschaum_connector_sql::flavor::DriverFamily;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SourceFetchError::Unavailable)?;
        let sql = self.sql(&plan.request().source_keys).await?;
        if sql.flavor().driver_family() != DriverFamily::Postgres {
            return Err(SourceFetchError::Unsupported);
        }
        let session = self.session.clone();
        let timeout = self.timeout;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = CancelOnDrop(cancelled.clone());
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
            let definition = sql
                .get_pipe_metadef_for_sync(plan.pipe(), &args)
                .map_err(|_| SourceFetchError::Failed)?
                .ok_or(SourceFetchError::Failed)?;
            let count = request.limit.map_or(request.row_limit + 1, |limit| {
                limit.min(request.row_limit + 1)
            });
            let statement = crate::source_fetch_backend::bounded_query(
                sql.flavor(),
                &definition,
                plan.pipe(),
                query.order.as_deref(),
                count,
            )?;
            plan.check_sql(&statement)
                .map_err(|_| SourceFetchError::Forbidden)?;
            let frame = session.block_on(async {
                let deadline = Instant::now() + timeout;
                // SQLx owns pooled BEGIN cancellation cleanup. Bound waiting for a connection
                // too; the native remaining budget starts before acquisition, not after it.
                let mut tx = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    sql.driver().begin_read_only(),
                )
                .await
                .map_err(|_| SourceFetchError::Unavailable)?
                .map_err(|_| SourceFetchError::Unavailable)?;
                let result = read(&mut tx, &plan, &statement, deadline, &cancelled).await;
                // Request-drop cancellation is bounded by the native remaining statement budget;
                // this owned worker retains the transaction until its rollback has completed.
                let rollback = tx.rollback().await;
                rollback.map_err(|_| SourceFetchError::Unavailable)?;
                result
            })?;
            if frame.height() > count {
                return Err(SourceFetchError::Failed);
            }
            let complete = frame.height() <= request.row_limit;
            let frame = if complete {
                frame
            } else {
                frame.take(&(0..request.row_limit).collect::<Vec<_>>())
            };
            let mut selected = query.select_columns.unwrap_or_else(|| frame.names());
            if selected.iter().any(|name| name == "*") {
                selected = frame.names();
            }
            if let Some(omit) = query.omit_columns {
                selected.retain(|name| !omit.contains(name));
            }
            let frame = frame
                .select(&selected)
                .map_err(|_| SourceFetchError::Failed)?;
            crate::source_fetch_backend::response(&frame, complete)
        })
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
        drop(cancellation);
        result
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn budget(
    tx: &mut meerschaum_connector_sql::driver::SqlTransaction,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<(), SourceFetchError> {
    if cancelled.load(Ordering::Relaxed) || Instant::now() >= deadline {
        return Err(SourceFetchError::Unavailable);
    }
    let millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .clamp(1, 20_000);
    tx.exec(&format!("SET LOCAL statement_timeout = '{millis}ms'"))
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    Ok(())
}

async fn catalog(
    tx: &mut meerschaum_connector_sql::driver::SqlTransaction,
    plan: &Plan,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Catalog, SourceFetchError> {
    budget(tx, deadline, cancelled).await?;
    let relations = tx
        .read_frame(RELATIONS_SQL)
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    budget(tx, deadline, cancelled).await?;
    let inheritance = tx
        .read_frame(INHERITANCE_SQL)
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    Catalog::from_frames(&relations, &inheritance, |schema, name| {
        plan.check_sql(&format!("SELECT * FROM {}", qualified(schema, name)))
            .is_ok()
    })
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quoted(schema), quoted(name))
}

async fn read(
    tx: &mut meerschaum_connector_sql::driver::SqlTransaction,
    plan: &Plan,
    statement: &str,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Frame, SourceFetchError> {
    let millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .clamp(1, 20_000) as u64;
    for policy in transaction_policy(millis)? {
        tx.exec(&policy)
            .await
            .map_err(|_| SourceFetchError::Unavailable)?;
    }
    // Relation locks alone cannot pin a qualified schema name: ALTER SCHEMA can rename its
    // namespace without altering the relation. This deliberately privileged intermediate proof
    // pins pg_namespace against its ROW EXCLUSIVE writers until rollback. Never fall back if
    // the role or standby cannot provide this capability.
    budget(tx, deadline, cancelled).await?;
    tx.exec("LOCK TABLE ONLY pg_catalog.pg_namespace IN SHARE MODE")
        .await
        .map_err(|_| SourceFetchError::Unsupported)?;
    budget(tx, deadline, cancelled).await?;
    let schemas = tx
        .read_frame(SCHEMAS_SQL)
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    if schemas.height() > 64 {
        return Err(SourceFetchError::TooLarge);
    }
    let mut names = vec![quoted("pg_catalog")];
    for row in 0..schemas.height() {
        let schema = text(&schemas, "schema_name", row)?;
        if schema != "pg_catalog" {
            names.push(quoted(&schema));
        }
    }
    tx.exec(&format!("SET LOCAL search_path = {}", names.join(", ")))
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    let initial = catalog(tx, plan, deadline, cancelled).await?;
    let prepared = prepare_query(statement, &initial)?;
    let locked = initial.component(&prepared.roots)?;
    for oid in &locked {
        let relation = initial
            .relations
            .get(oid)
            .ok_or(SourceFetchError::Forbidden)?;
        budget(tx, deadline, cancelled).await?;
        tx.exec(&format!(
            "LOCK TABLE ONLY {} IN SHARE UPDATE EXCLUSIVE MODE",
            qualified(&relation.schema, &relation.name)
        ))
        .await
        .map_err(|_| SourceFetchError::Unavailable)?;
    }
    let current = catalog(tx, plan, deadline, cancelled).await?;
    if current.component(&prepared.roots)? != locked {
        return Err(SourceFetchError::Unavailable);
    }
    for oid in &locked {
        let old = initial
            .relations
            .get(oid)
            .ok_or(SourceFetchError::Forbidden)?;
        let (actual, _) = current
            .resolve(Some(&old.schema), &old.name)
            .ok_or(SourceFetchError::Forbidden)?;
        if actual != *oid {
            return Err(SourceFetchError::Unavailable);
        }
    }
    budget(tx, deadline, cancelled).await?;
    tx.read_frame(&prepared.sql)
        .await
        .map_err(|_| SourceFetchError::Failed)
}

struct PreparedQuery {
    sql: String,
    roots: BTreeSet<u64>,
}

fn identifier(ident: &ast::Ident) -> Result<String, ()> {
    if ident.value.len() > 1024 || ident.value.chars().any(char::is_control) {
        return Err(());
    }
    match ident.quote_style {
        Some('"') => Ok(ident.value.clone()),
        None if ident.value.is_ascii() => Ok(ident.value.to_ascii_lowercase()),
        _ => Err(()),
    }
}

fn object(parts: &[&str]) -> ast::ObjectName {
    ast::ObjectName(
        parts
            .iter()
            .map(|part| ast::ObjectNamePart::Identifier(ast::Ident::with_quote('"', *part)))
            .collect(),
    )
}

fn prepare_query(sql: &str, catalog: &Catalog) -> Result<PreparedQuery, SourceFetchError> {
    let mut statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)
            .map_err(|_| SourceFetchError::Forbidden)?;
    if statements.len() != 1 || !matches!(&statements[0], ast::Statement::Query(_)) {
        return Err(SourceFetchError::Forbidden);
    }
    let mut proof = Rewrite {
        catalog,
        scopes: Vec::new(),
        traversed_with: Vec::new(),
        roots: BTreeSet::new(),
        queries: 0,
        expressions: 0,
    };
    if let ControlFlow::Break(()) = statements.visit(&mut proof) {
        return Err(SourceFetchError::Forbidden);
    }
    Ok(PreparedQuery {
        sql: statements[0].to_string(),
        roots: proof.roots,
    })
}

struct Rewrite<'a> {
    catalog: &'a Catalog,
    scopes: Vec<BTreeSet<String>>,
    traversed_with: Vec<Option<ast::With>>,
    roots: BTreeSet<u64>,
    queries: usize,
    expressions: usize,
}

impl ast::VisitorMut for Rewrite<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<()> {
        self.queries += 1;
        if self.queries > 256
            || self.scopes.len() >= 32
            || !query.locks.is_empty()
            || query.settings.is_some()
            || query.for_clause.is_some()
            || !query.pipe_operators.is_empty()
            || !read_only_set(&query.body)
        {
            return ControlFlow::Break(());
        }
        let mut names = BTreeSet::new();
        let mut with = query.with.take();
        if let Some(with) = &with {
            for cte in &with.cte_tables {
                let Ok(name) = identifier(&cte.alias.name) else {
                    return ControlFlow::Break(());
                };
                if cte.from.is_some() || !names.insert(name) {
                    return ControlFlow::Break(());
                }
            }
        }
        // PostgreSQL exposes all recursive aliases to their definitions. Without RECURSIVE,
        // each definition sees only preceding aliases: a self/forward name may resolve to an
        // actual table or view and MUST pass physical catalog proof. Visit WITH bodies exactly
        // once here, then restore them after the derived visitor traverses the query body.
        self.scopes
            .push(if with.as_ref().is_some_and(|with| with.recursive) {
                names
            } else {
                BTreeSet::new()
            });
        if let Some(with) = &mut with {
            for cte in &mut with.cte_tables {
                if let ControlFlow::Break(()) = cte.query.visit(self) {
                    return ControlFlow::Break(());
                }
                let Ok(name) = identifier(&cte.alias.name) else {
                    return ControlFlow::Break(());
                };
                self.scopes.last_mut().expect("query scope").insert(name);
            }
        }
        self.traversed_with.push(with);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<()> {
        query.with = self.traversed_with.pop().expect("query WITH traversal");
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut ast::Select) -> ControlFlow<()> {
        if select.into.is_some() {
            return ControlFlow::Break(());
        }
        for from in &select.from {
            for join in &from.joins {
                use ast::JoinOperator as Join;
                let constraint = match &join.join_operator {
                    Join::Join(c)
                    | Join::Inner(c)
                    | Join::Left(c)
                    | Join::LeftOuter(c)
                    | Join::Right(c)
                    | Join::RightOuter(c)
                    | Join::FullOuter(c)
                    | Join::CrossJoin(c) => c,
                    _ => return ControlFlow::Break(()),
                };
                // USING/NATURAL introduce implicit overloadable equality. An ON expression is
                // lowered explicitly below; preserving merged USING columns needs a later pass.
                if !matches!(
                    constraint,
                    ast::JoinConstraint::On(_) | ast::JoinConstraint::None
                ) {
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &mut ast::TableFactor) -> ControlFlow<()> {
        match table {
            ast::TableFactor::Table {
                args: None,
                version: None,
                sample: None,
                json_path: None,
                with_ordinality: false,
                with_hints,
                partitions,
                index_hints,
                ..
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
                ControlFlow::Continue(())
            }
            ast::TableFactor::Derived {
                lateral: false,
                sample: None,
                ..
            }
            | ast::TableFactor::NestedJoin { .. } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }

    fn pre_visit_relation(&mut self, name: &mut ast::ObjectName) -> ControlFlow<()> {
        let parts: Result<Vec<_>, _> = name
            .0
            .iter()
            .map(|part| part.as_ident().ok_or(()).and_then(identifier))
            .collect();
        let Ok(parts) = parts else {
            return ControlFlow::Break(());
        };
        if let [table] = parts.as_slice()
            && self.scopes.iter().rev().any(|scope| scope.contains(table))
        {
            return ControlFlow::Continue(());
        }
        let found = match parts.as_slice() {
            [table] => self.catalog.resolve(None, table),
            [schema, table] => self.catalog.resolve(Some(schema), table),
            _ => None,
        };
        let Some((oid, relation)) = found else {
            return ControlFlow::Break(());
        };
        self.roots.insert(oid);
        if self.roots.len() > 64 {
            return ControlFlow::Break(());
        }
        *name = object(&[&relation.schema, &relation.name]);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<()> {
        self.expressions += 1;
        if self.expressions > 16_384 || rewrite_expression(expr).is_err() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

fn read_only_set(body: &ast::SetExpr) -> bool {
    match body {
        ast::SetExpr::Select(_) | ast::SetExpr::Query(_) | ast::SetExpr::Values(_) => true,
        ast::SetExpr::SetOperation { left, right, .. } => {
            read_only_set(left) && read_only_set(right)
        }
        // TABLE stores its name outside relation visitor hooks; DML query bodies are never
        // admitted even when a native READ ONLY transaction would later reject execution.
        _ => false,
    }
}

fn literal(value: ast::Value) -> ast::Expr {
    ast::Expr::Value(value.into())
}
fn binary(left: ast::Expr, operator: &str, right: ast::Expr) -> ast::Expr {
    ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
        left: Box::new(left),
        op: ast::BinaryOperator::PGCustomBinaryOperator(vec!["pg_catalog".into(), operator.into()]),
        right: Box::new(right),
    }))
}
fn logical(left: ast::Expr, operator: ast::BinaryOperator, right: ast::Expr) -> ast::Expr {
    ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
        left: Box::new(left),
        op: operator,
        right: Box::new(right),
    }))
}
fn not(expr: ast::Expr, negated: bool) -> ast::Expr {
    if negated {
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr: Box::new(expr),
        }
    } else {
        expr
    }
}

fn rewrite_expression(expr: &mut ast::Expr) -> Result<(), ()> {
    use ast::{BinaryOperator as Op, Expr, UnaryOperator as Unary};
    let insensitive = matches!(expr, Expr::ILike { .. });
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::Nested(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_)
        | Expr::Exists { .. }
        | Expr::Subquery(_)
        | Expr::Interval(_)
        | Expr::Extract { .. }
        | Expr::Trim { .. }
        | Expr::Substring { .. } => {}
        Expr::BinaryOp { op, .. } => {
            if !matches!(op, Op::And | Op::Or) {
                if !matches!(
                    op,
                    Op::Plus
                        | Op::Minus
                        | Op::Multiply
                        | Op::Divide
                        | Op::Modulo
                        | Op::StringConcat
                        | Op::Gt
                        | Op::Lt
                        | Op::GtEq
                        | Op::LtEq
                        | Op::Eq
                        | Op::NotEq
                ) {
                    return Err(());
                }
                *op = Op::PGCustomBinaryOperator(vec!["pg_catalog".into(), op.to_string()]);
                // OPERATOR(schema.op) uses PostgreSQL's generic operator precedence. Preserve
                // the parsed tree explicitly rather than change `a + b * c` to `(a+b)*c`.
                *expr = Expr::Nested(Box::new(expr.clone()));
            }
        }
        Expr::UnaryOp { op, expr: operand } => match op {
            Unary::Not => {}
            Unary::Minus | Unary::Plus => {
                *expr = binary(
                    literal(ast::Value::Number("0".into(), false)),
                    if *op == Unary::Minus { "-" } else { "+" },
                    *operand.clone(),
                );
            }
            _ => return Err(()),
        },
        Expr::Between {
            expr: value,
            negated,
            low,
            high,
        } => {
            *expr = not(
                logical(
                    binary(*value.clone(), ">=", *low.clone()),
                    Op::And,
                    binary(*value.clone(), "<=", *high.clone()),
                ),
                *negated,
            );
        }
        Expr::InList {
            expr: value,
            list,
            negated,
        } => {
            if list.len() > 256 {
                return Err(());
            }
            let mut items = list.iter().cloned();
            let result = match items.next() {
                None => literal(ast::Value::Boolean(false)),
                Some(first) => items.fold(binary(*value.clone(), "=", first), |out, next| {
                    logical(out, Op::Or, binary(*value.clone(), "=", next))
                }),
            };
            *expr = not(result, *negated);
        }
        Expr::Like {
            negated,
            any: false,
            expr: value,
            pattern,
            escape_char: None,
        }
        | Expr::ILike {
            negated,
            any: false,
            expr: value,
            pattern,
            escape_char: None,
        } => {
            let (value, pattern, negated) = (*value.clone(), *pattern.clone(), *negated);
            *expr = not(
                binary(value, if insensitive { "~~*" } else { "~~" }, pattern),
                negated,
            );
        }
        Expr::Case {
            operand,
            conditions,
            ..
        } => {
            if let Some(value) = operand.take() {
                for when in conditions {
                    when.condition = binary(*value.clone(), "=", when.condition.clone());
                }
            }
        }
        Expr::Cast {
            data_type,
            kind,
            array: false,
            format: None,
            ..
        } => {
            if !matches!(kind, ast::CastKind::Cast | ast::CastKind::DoubleColon) {
                return Err(());
            }
            *data_type = native_type(data_type)?;
        }
        Expr::TypedString(typed) => {
            let data_type = native_type(&typed.data_type)?;
            *expr = Expr::Cast {
                kind: ast::CastKind::Cast,
                expr: Box::new(Expr::Value(typed.value.clone())),
                data_type,
                array: false,
                format: None,
            };
        }
        Expr::Function(function) => {
            if !matches!(function.parameters, ast::FunctionArguments::None) {
                return Err(());
            }
            let [name] = function.name.0.as_slice() else {
                return Err(());
            };
            let name = name.as_ident().ok_or(())?;
            if name.quote_style.is_some() || !name.value.is_ascii() {
                return Err(());
            }
            let name = name.value.to_ascii_lowercase();
            const PURE: &[&str] = &[
                "abs",
                "avg",
                "ceil",
                "ceiling",
                "char_length",
                "concat",
                "concat_ws",
                "count",
                "dense_rank",
                "floor",
                "lag",
                "lead",
                "length",
                "lower",
                "ltrim",
                "max",
                "min",
                "rank",
                "replace",
                "round",
                "row_number",
                "rtrim",
                "substr",
                "substring",
                "sum",
                "upper",
            ];
            if name == "coalesce" {
                // COALESCE is PostgreSQL grammar, not a schema-resolved function call.
                function.name = ast::ObjectName(vec![ast::ObjectNamePart::Identifier(
                    ast::Ident::new("coalesce"),
                )]);
            } else {
                if !PURE.contains(&name.as_str()) {
                    return Err(());
                }
                function.name = object(&["pg_catalog", &name]);
            }
        }
        _ => return Err(()),
    }
    Ok(())
}

fn native_type(data_type: &ast::DataType) -> Result<ast::DataType, ()> {
    let text = data_type.to_string().to_ascii_lowercase();
    if !text.is_ascii() || text.contains(['\'', '"', '.', '[', ']']) {
        return Err(());
    }
    let mut base = text.clone();
    let mut modifiers = Vec::new();
    if let Some(start) = text.find('(') {
        let end = text.find(')').ok_or(())?;
        if text[end + 1..].contains(['(', ')']) {
            return Err(());
        }
        for item in text[start + 1..end].split(',') {
            let value: u32 = item.trim().parse().map_err(|_| ())?;
            if value > 16_384 {
                return Err(());
            }
            modifiers.push(value.to_string());
        }
        base.replace_range(start..=end, "");
    }
    let base = base.split_whitespace().collect::<Vec<_>>().join(" ");
    let name = match base.as_str() {
        "int" | "integer" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "boolean" | "bool" => "bool",
        "real" | "float4" => "float4",
        "double precision" | "float8" => "float8",
        "numeric" | "decimal" | "dec" => "numeric",
        "text" => "text",
        "varchar" | "character varying" => "varchar",
        "char" | "character" => "bpchar",
        "date" => "date",
        "timestamp" | "timestamp without time zone" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "time" | "time without time zone" => "time",
        "timetz" | "time with time zone" => "timetz",
        "uuid" => "uuid",
        "bytea" => "bytea",
        "json" => "json",
        "jsonb" => "jsonb",
        "interval" => "interval",
        _ => return Err(()),
    };
    Ok(ast::DataType::Custom(
        object(&["pg_catalog", name]),
        modifiers,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Catalog {
        Catalog {
            relations: BTreeMap::from([
                (
                    1,
                    Relation {
                        schema: "fixture".into(),
                        name: "rows".into(),
                        visible: true,
                        safe: true,
                    },
                ),
                (
                    2,
                    Relation {
                        schema: "fixture".into(),
                        name: "other".into(),
                        visible: true,
                        safe: true,
                    },
                ),
                (
                    3,
                    Relation {
                        schema: "fixture".into(),
                        name: "view".into(),
                        visible: true,
                        safe: false,
                    },
                ),
                (
                    4,
                    Relation {
                        schema: "fixture".into(),
                        name: "ΔRows".into(),
                        visible: false,
                        safe: true,
                    },
                ),
            ]),
            denied: BTreeSet::from([3]),
            adjacent: BTreeMap::new(),
        }
    }

    #[test]
    fn pg_query_pins_tables_functions_operators_and_builtin_casts() {
        let query = prepare_query("SELECT lower(a.value), CAST(a.id AS BIGINT), a.id + 2 * a.id FROM rows a JOIN other b ON a.id = b.id WHERE a.id IN (1, 2) AND a.created >= TIMESTAMP '2026-01-01'", &catalog()).unwrap();
        assert!(query.sql.contains("\"fixture\".\"rows\""));
        assert!(query.sql.contains("\"pg_catalog\".\"lower\""));
        assert!(query.sql.contains("OPERATOR(pg_catalog.=)"));
        assert!(query.sql.contains("OPERATOR(pg_catalog.*)"));
        assert!(query.sql.contains("\"pg_catalog\".\"int8\""));
        assert_eq!(query.roots, BTreeSet::from([1, 2]));
    }

    #[test]
    fn pg_query_never_promotes_views_or_custom_operations() {
        for sql in [
            "SELECT * FROM view",
            "SELECT * FROM fixture.view",
            "WITH view AS (SELECT 1) SELECT * FROM fixture.view",
            "WITH view AS (SELECT * FROM view) SELECT * FROM view",
            "WITH first AS (SELECT * FROM view), view AS (SELECT 1) SELECT * FROM first",
            "WITH changed AS (DELETE FROM rows RETURNING *) SELECT * FROM changed",
            "TABLE rows",
            "SELECT evil(id) FROM rows",
            "SELECT fixture.lower(value) FROM rows",
            "SELECT CAST(value AS fixture.secret_type) FROM rows",
            "SELECT id OPERATOR(fixture.+) id FROM rows",
            "SELECT * FROM rows NATURAL JOIN other",
            "SELECT * FROM rows JOIN other USING (id)",
        ] {
            assert!(prepare_query(sql, &catalog()).is_err(), "{sql}");
        }
        assert!(prepare_query("SELECT * FROM fixture.ΔRows", &catalog()).is_err());
        assert!(prepare_query("SELECT * FROM fixture.\"ΔRows\"", &catalog()).is_ok());
    }

    #[test]
    fn pg_cte_scope_preserves_sequential_and_recursive_bindings() {
        let query = prepare_query(
            "WITH rows AS (SELECT * FROM rows), next_rows AS (SELECT * FROM rows) SELECT * FROM next_rows",
            &catalog(),
        )
        .unwrap();
        assert_eq!(query.roots, BTreeSet::from([1]));
        assert_eq!(query.sql.matches("\"fixture\".\"rows\"").count(), 1);
        let recursive = prepare_query(
            "WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < 3) SELECT * FROM n",
            &catalog(),
        )
        .unwrap();
        assert!(recursive.roots.is_empty());
    }

    #[test]
    fn pg_operator_rewrite_keeps_arithmetic_precedence() {
        let query = prepare_query("SELECT 1 + 2 * 3 FROM rows", &catalog()).unwrap();
        assert!(
            query
                .sql
                .contains("(1 OPERATOR(pg_catalog.+) (2 OPERATOR(pg_catalog.*) 3))"),
            "{}",
            query.sql
        );
    }
}
