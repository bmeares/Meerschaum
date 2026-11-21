//! A per-connector cache of catalog reads: whether a table exists, its columns types (both the
//! `information_schema` and the reflected forms), its indices and an upsert's conflict target.
//!
//! v4's `Pipe` caches `get_columns_types()` / `get_columns_indices()` for
//! `pipes:dtypes:columns_types_cache_seconds` and `exists()` for `pipes:sync:exists_cache_seconds`
//! (60 s each), and a static pipe never asks for its columns at all while syncing. v5 reads the
//! catalog from many places in one sync (the table state, the add / alter checks, the unique key
//! of an upsert), so the connector keeps the answers instead, for the same time:
//!
//! - only positive answers are kept (a table that exists, a non-empty column or index list, a
//!   conflict target that was found), so a table created elsewhere is seen at once;
//! - any `CREATE` / `ALTER` / `DROP` / `RENAME` this connector runs forgets the tables it names
//!   (before it runs and again after), and a statement that fails forgets the tables it names
//!   (as does a failed sync its pipe's table): a table changed by another process within the
//!   window shows up as a failed statement, and the next read asks the database again;
//! - a read that started before an invalidation is never stored (generation counter), so a
//!   concurrent DDL cannot leave its old layout behind.
//!
//! Two more kinds of entry live here, with their own lifetimes:
//!
//! - the readiness of internal `mrsm:*` tables: set once a write into one succeeded, kept until
//!   a statement names that table, a write into it fails or it is dropped (never by a clear of
//!   the whole cache: an unrelated procedural DDL block must not send every internal write back
//!   through initialization);
//! - the layout a sync into an existing table ended with, handed out once to the caller of that
//!   sync ([`SqlConnector::take_synced_columns_types`](super::SqlConnector::take_synced_columns_types)).
//!
//! The lock is never held across an `await`, and the connector is rebuilt after a fork (the
//! Python façade), so no lock is inherited.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use indexmap::IndexMap;

use crate::flavor::Flavor;
use crate::instance::data::PipeState;
use crate::queries::table::IndexDoc;

/// The most entries of each kind held at once (temporary tables are looked up too: the maps are
/// cleared rather than grown without bound).
const MAX_ENTRIES: usize = 2048;

/// The key of a table: its schema as the caller passed it and its name.
type TableKey = (Option<String>, String);

/// An upsert's conflict target (its key expressions, whether NULL keys are equal) by table and
/// join columns.
type ConflictMap = HashMap<(TableKey, Vec<String>), (Instant, (String, bool))>;

#[derive(Debug, Default)]
pub(crate) struct CatalogCache {
    /// Bumped by every invalidation (see [`CatalogCache::generation`]).
    generation: u64,
    exists: HashMap<TableKey, Instant>,
    cols_types: HashMap<TableKey, (Instant, IndexMap<String, String>)>,
    reflected: HashMap<TableKey, (Instant, IndexMap<String, String>)>,
    indices: HashMap<TableKey, (Instant, IndexMap<String, Vec<IndexDoc>>)>,
    conflict: ConflictMap,
    /// The columns types a static pipe's definition stages (by the pipe's table and a digest of
    /// its parameters), see [`CatalogCache::staged`].
    staged: HashMap<(TableKey, u64), (Instant, IndexMap<String, String>)>,
    /// Internal `mrsm:*` tables that completed initialization (table, indices/constraints, and
    /// MySQL history precision upgrade) in this process, with the table state their last write
    /// left (see [`CatalogCache::internal_ready`]).
    internal_ready: HashMap<TableKey, PipeState>,
    /// The layout the last successful sync of a pipe into its existing table ended with, by the
    /// pipe's keys (see [`CatalogCache::take_synced`]).
    synced: HashMap<String, (TableKey, IndexMap<String, String>)>,
}

fn key(table: &str, schema: Option<&str>) -> TableKey {
    (schema.map(str::to_string), table.to_string())
}

fn fresh(at: Instant, ttl: Duration) -> bool {
    !crate::driver::authorization_reads::active() && at.elapsed() < ttl
}

/// A cached answer was used instead of a catalog query: with debug output on, say so, so a
/// `debug=True` call still shows what it did.
fn served(what: &str, table: &str, schema: Option<&str>, at: Instant) {
    meerschaum_core::dprint!(
        "Using the catalog cache for the {what} of {} ({:.2} seconds old).",
        match schema {
            Some(schema) => format!("{schema}.{table}"),
            None => table.to_string(),
        },
        at.elapsed().as_secs_f64()
    );
}

fn bounded<K, V>(map: &mut HashMap<K, V>) -> &mut HashMap<K, V> {
    if map.len() >= MAX_ENTRIES {
        map.clear();
    }
    map
}

impl CatalogCache {
    /// The generation a read starts at: an answer is stored only if no invalidation happened
    /// while it was read.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn exists(&self, table: &str, schema: Option<&str>, ttl: Duration) -> bool {
        let hit = self
            .exists
            .get(&key(table, schema))
            .filter(|at| fresh(**at, ttl));
        if let Some(at) = hit {
            served("existence", table, schema, *at);
        }
        hit.is_some()
    }

    pub(crate) fn put_exists(&mut self, table: &str, schema: Option<&str>, generation: u64) {
        if generation == self.generation {
            bounded(&mut self.exists).insert(key(table, schema), Instant::now());
        }
    }

    pub(crate) fn cols_types(
        &self,
        table: &str,
        schema: Option<&str>,
        reflected: bool,
        ttl: Duration,
    ) -> Option<IndexMap<String, String>> {
        let map = if reflected {
            &self.reflected
        } else {
            &self.cols_types
        };
        let (at, cols_types) = map
            .get(&key(table, schema))
            .filter(|(at, _)| fresh(*at, ttl))?;
        served("columns types", table, schema, *at);
        Some(cols_types.clone())
    }

    pub(crate) fn put_cols_types(
        &mut self,
        table: &str,
        schema: Option<&str>,
        reflected: bool,
        cols_types: &IndexMap<String, String>,
        generation: u64,
    ) {
        if cols_types.is_empty() || generation != self.generation {
            return;
        }
        // Columns imply the table: its existence is known too.
        bounded(&mut self.exists).insert(key(table, schema), Instant::now());
        let map = if reflected {
            &mut self.reflected
        } else {
            &mut self.cols_types
        };
        bounded(map).insert(key(table, schema), (Instant::now(), cols_types.clone()));
    }

    pub(crate) fn indices(
        &self,
        table: &str,
        schema: Option<&str>,
        ttl: Duration,
    ) -> Option<IndexMap<String, Vec<IndexDoc>>> {
        let (at, indices) = self
            .indices
            .get(&key(table, schema))
            .filter(|(at, _)| fresh(*at, ttl))?;
        served("indices", table, schema, *at);
        Some(indices.clone())
    }

    pub(crate) fn put_indices(
        &mut self,
        table: &str,
        schema: Option<&str>,
        indices: &IndexMap<String, Vec<IndexDoc>>,
        generation: u64,
    ) {
        if indices.is_empty() || generation != self.generation {
            return;
        }
        bounded(&mut self.indices).insert(key(table, schema), (Instant::now(), indices.clone()));
    }

    pub(crate) fn conflict(
        &self,
        table: &str,
        schema: Option<&str>,
        join_cols: &[String],
        ttl: Duration,
    ) -> Option<(String, bool)> {
        let (at, target) = self
            .conflict
            .get(&(key(table, schema), join_cols.to_vec()))
            .filter(|(at, _)| fresh(*at, ttl))?;
        served("upsert conflict target", table, schema, *at);
        Some(target.clone())
    }

    pub(crate) fn put_conflict(
        &mut self,
        table: &str,
        schema: Option<&str>,
        join_cols: &[String],
        target: &(String, bool),
        generation: u64,
    ) {
        if generation != self.generation {
            return;
        }
        bounded(&mut self.conflict).insert(
            (key(table, schema), join_cols.to_vec()),
            (Instant::now(), target.clone()),
        );
    }

    /// The columns types the definition of a static pipe stages for its table (`table`): a
    /// static table never changes, so its definition yields the same columns sync after sync.
    /// `parameters` is a digest of the pipe's parameters: another definition is another entry.
    pub(crate) fn staged(
        &self,
        table: &str,
        schema: Option<&str>,
        parameters: u64,
        ttl: Duration,
    ) -> Option<IndexMap<String, String>> {
        let (at, cols_types) = self
            .staged
            .get(&(key(table, schema), parameters))
            .filter(|(at, _)| fresh(*at, ttl))?;
        served("staged columns types", table, schema, *at);
        Some(cols_types.clone())
    }

    pub(crate) fn put_staged(
        &mut self,
        table: &str,
        schema: Option<&str>,
        parameters: u64,
        cols_types: &IndexMap<String, String>,
        generation: u64,
    ) {
        if cols_types.is_empty() || generation != self.generation {
            return;
        }
        bounded(&mut self.staged).insert(
            (key(table, schema), parameters),
            (Instant::now(), cols_types.clone()),
        );
    }

    /// The table state of an internal `mrsm:*` table that completed initialization in this
    /// process, as its last successful write left it (`None`: the next write initializes it).
    pub(crate) fn internal_ready(&self, table: &str, schema: Option<&str>) -> Option<PipeState> {
        self.internal_ready.get(&key(table, schema)).cloned()
    }

    /// Record that an internal `mrsm:*` table completed initialization with `state`.
    pub(crate) fn put_internal_ready(
        &mut self,
        table: &str,
        schema: Option<&str>,
        state: &PipeState,
    ) {
        bounded(&mut self.internal_ready).insert(key(table, schema), state.clone());
    }

    /// Keep the layout a sync of the pipe `pipe_key` into its existing table ended with, until
    /// [`take_synced`](Self::take_synced) hands it out (or the table is invalidated).
    pub(crate) fn put_synced(
        &mut self,
        pipe_key: &str,
        table: &str,
        schema: Option<&str>,
        cols_types: &IndexMap<String, String>,
    ) {
        if cols_types.is_empty() {
            return;
        }
        bounded(&mut self.synced).insert(
            pipe_key.to_string(),
            (key(table, schema), cols_types.clone()),
        );
    }

    /// The layout the last successful sync of the pipe `pipe_key` left, handed out once.
    pub(crate) fn take_synced(&mut self, pipe_key: &str) -> Option<IndexMap<String, String>> {
        self.synced
            .remove(pipe_key)
            .map(|(_, cols_types)| cols_types)
    }

    /// Drop what a sync of the pipe `pipe_key` left (a new sync of it started).
    pub(crate) fn forget_synced(&mut self, pipe_key: &str) {
        self.synced.remove(pipe_key);
    }

    /// Forget every catalog answer (a statement that may have changed any table). The readiness
    /// of internal tables is kept: it is forgotten per table only (a statement naming the table,
    /// a failed write into it, a drop), so a DDL block elsewhere does not send every internal
    /// write back through initialization.
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.exists.clear();
        self.cols_types.clear();
        self.reflected.clear();
        self.indices.clear();
        self.conflict.clear();
        self.staged.clear();
        self.synced.clear();
    }

    /// Forget what `sql` may change, if it changes the schema of a table (see [`ddl_scope`]).
    pub(crate) fn note_statement(&mut self, sql: &str, flavor: Flavor) {
        match ddl_scope(sql) {
            DdlScope::None => {}
            DdlScope::All => self.clear(),
            DdlScope::Tables(text) => self.forget_named_in(text, flavor),
        }
    }

    /// Forget every table named in `text` (an identifier token, case-insensitively, in full or
    /// truncated to the flavor's identifier length). The generation moves on regardless: a read
    /// of a table not cached yet may be in flight.
    pub(crate) fn forget_named_in(&mut self, text: &str, flavor: Flavor) {
        self.generation = self.generation.wrapping_add(1);
        let upper = text.to_uppercase();
        let named = |(_, table): &TableKey| {
            names_table(&upper, &table.to_uppercase())
                || names_table(
                    &upper,
                    &crate::ident::truncate_item_name(table, flavor).to_uppercase(),
                )
        };
        self.exists.retain(|k, _| !named(k));
        self.cols_types.retain(|k, _| !named(k));
        self.reflected.retain(|k, _| !named(k));
        self.indices.retain(|k, _| !named(k));
        self.conflict.retain(|(k, _), _| !named(k));
        self.staged.retain(|(k, _), _| !named(k));
        self.internal_ready.retain(|k, _| !named(k));
        self.synced.retain(|_, (k, _)| !named(k));
    }

    /// Forget one table.
    pub(crate) fn forget_table(&mut self, table: &str) {
        let upper = table.to_uppercase();
        self.generation = self.generation.wrapping_add(1);
        let named = |(_, t): &TableKey| t.to_uppercase() == upper;
        self.exists.retain(|k, _| !named(k));
        self.cols_types.retain(|k, _| !named(k));
        self.reflected.retain(|k, _| !named(k));
        self.indices.retain(|k, _| !named(k));
        self.conflict.retain(|(k, _), _| !named(k));
        self.staged.retain(|(k, _), _| !named(k));
        self.internal_ready.retain(|k, _| !named(k));
        self.synced.retain(|_, (k, _)| !named(k));
    }
}

/// What a statement may change in the catalog.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DdlScope<'a> {
    /// Nothing (a query or a data change).
    None,
    /// The tables named in this text.
    Tables(&'a str),
    /// Anything (a procedural block or an index dropped by name).
    All,
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '$' | '#' | '@')
}

/// Whether `text` names `table` as a whole identifier (both upper-cased).
fn names_table(text: &str, table: &str) -> bool {
    if table.is_empty() {
        return false;
    }
    let mut from = 0;
    while let Some(pos) = text[from..].find(table) {
        let start = from + pos;
        let end = start + table.len();
        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        if !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char) {
            return true;
        }
        from = start + table.chars().next().map_or(1, char::len_utf8);
    }
    false
}

/// The statement without leading whitespace and comments.
fn strip_leading_comments(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        if let Some(rest) = sql.strip_prefix("--") {
            sql = rest.split_once('\n').map_or("", |(_, r)| r);
        } else if let Some(rest) = sql.strip_prefix("/*") {
            sql = rest.split_once("*/").map_or("", |(_, r)| r);
        } else {
            return sql;
        }
    }
}

/// The first `n` words of `sql`, upper-cased.
fn leading_words(sql: &str, n: usize) -> Vec<String> {
    sql.split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .filter(|w| !w.is_empty())
        .take(n)
        .map(str::to_uppercase)
        .collect()
}

/// The end of a `CREATE` statement's header: before its column list, `AS <query>` (a
/// `CREATE TABLE ... AS SELECT` names its source tables after `AS`, and they do not change) or
/// `PARTITION OF <parent>` (a new partition changes neither its parent's columns nor its
/// indices).
fn create_header(sql: &str) -> &str {
    let upper = sql.to_uppercase();
    let paren = sql.find('(');
    let mut as_kw = None;
    let mut from = 0;
    while let Some(pos) = upper[from..].find("AS") {
        let start = from + pos;
        let end = start + 2;
        let before = upper[..start].chars().next_back();
        let after = upper[end..].chars().next();
        if before.is_some_and(char::is_whitespace) && !after.is_some_and(is_ident_char) {
            as_kw = Some(start);
            break;
        }
        from = end;
    }
    let partition_of = upper.find(" PARTITION OF ");
    let cut = [paren, as_kw, partition_of]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(sql.len());
    &sql[..cut]
}

/// The statements a procedural block runs with `EXECUTE IMMEDIATE '<statement>';` (Oracle's
/// guarded DDL), when they are all the DDL it can run: each one a constant literal, and no
/// `CREATE` / `ALTER` / `DROP` / `RENAME` / `TRUNCATE` word outside them. `None` when the block
/// runs no such statement, builds one at run time (`'DROP TABLE ' || name`, `USING`) or runs
/// DDL of its own.
fn execute_immediate_statements(sql: &str) -> Option<Vec<String>> {
    const KEYWORD: &str = "EXECUTE IMMEDIATE";
    let upper = sql.to_ascii_uppercase();
    let bytes = sql.as_bytes();
    let mut outside = String::new();
    let mut statements = Vec::new();
    let mut from = 0;
    while let Some(pos) = upper[from..].find(KEYWORD) {
        let start = from + pos;
        outside.push_str(&upper[from..start]);
        let mut i = start + KEYWORD.len();
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if bytes.get(i) != Some(&b'\'') {
            return None;
        }
        let literal_start = i + 1;
        i = literal_start;
        let literal_end = loop {
            match bytes.get(i)? {
                b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
                b'\'' => break i,
                _ => i += 1,
            }
        };
        i = literal_end + 1;
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if bytes.get(i) != Some(&b';') {
            return None;
        }
        statements.push(sql[literal_start..literal_end].replace("''", "'"));
        from = i;
    }
    outside.push_str(&upper[from..]);
    let ddl_outside = ["CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE"]
        .iter()
        .any(|word| names_table(&outside, word));
    (!statements.is_empty() && !ddl_outside).then_some(statements)
}

/// What `sql` may change in the catalog.
pub(crate) fn ddl_scope(sql: &str) -> DdlScope<'_> {
    let sql = strip_leading_comments(sql);
    let words = leading_words(sql, 3);
    let Some(first) = words.first() else {
        return DdlScope::None;
    };
    let second = words.get(1).map(String::as_str).unwrap_or_default();
    // Case-insensitive without upper-casing every statement (most are short-lived queries).
    let mentions = |needles: &[&str]| {
        needles
            .iter()
            .any(|n| sql.contains(n) || sql.contains(&n.to_lowercase()))
    };
    match first.as_str() {
        "CREATE" => DdlScope::Tables(create_header(sql)),
        "ALTER" | "DROP" if matches!(second, "INDEX" | "SCHEMA" | "DATABASE") => DdlScope::All,
        "ALTER" | "DROP" | "RENAME" => DdlScope::Tables(sql),
        // Procedural blocks (MSSQL `IF ... CREATE`, Oracle `BEGIN EXECUTE IMMEDIATE`, `DO $$`).
        "EXEC" | "EXECUTE" | "CALL" | "DO" | "IF" | "BEGIN" | "DECLARE" => {
            if !mentions(&["CREATE", "ALTER", "DROP", "RENAME"]) {
                return DdlScope::None;
            }
            // Oracle's guarded DDL runs constant statements: they name what they change.
            let Some(statements) = execute_immediate_statements(sql) else {
                return DdlScope::All;
            };
            let mut changes = false;
            for statement in &statements {
                match ddl_scope(statement) {
                    DdlScope::All => return DdlScope::All,
                    DdlScope::Tables(_) => changes = true,
                    DdlScope::None => {}
                }
            }
            if changes {
                DdlScope::Tables(sql)
            } else {
                DdlScope::None
            }
        }
        // TimescaleDB functions that create indices or change a table's layout.
        "SELECT" | "PERFORM"
            if mentions(&["CREATE_HYPERTABLE", "ADD_DIMENSION", "CREATE_DISTRIBUTED"]) =>
        {
            DdlScope::Tables(sql)
        }
        _ if mentions(&["SP_RENAME"]) => DdlScope::All,
        _ => DdlScope::None,
    }
}

impl super::SqlConnector {
    /// How long catalog answers are kept (`None`: not at all).
    pub(crate) fn catalog_ttl(&self) -> Option<Duration> {
        let secs = self.settings.catalog_cache_seconds;
        (secs.is_finite() && secs > 0.0).then(|| Duration::from_secs_f64(secs))
    }

    pub(crate) fn catalog(&self) -> std::sync::MutexGuard<'_, CatalogCache> {
        self.catalog.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Forget what a statement this connector runs may change (called before and after it runs).
    pub(crate) fn note_catalog_statement(&self, sql: &str) {
        if ddl_scope(sql) != DdlScope::None {
            self.catalog().note_statement(sql, self.flavor);
        }
    }

    /// Forget the catalog answers about the tables `sql` names: it failed, and a table it reads
    /// or writes may have changed elsewhere (a statement that fails as a matter of course, such
    /// as a read of an optional table, costs nothing else).
    pub(crate) fn forget_catalog_failed(&self, sql: &str) {
        self.catalog().forget_named_in(sql, self.flavor);
    }

    /// A statement run on this connector's database outside its own methods (raw SQL from the
    /// Python façade or a plugin): what it may change in the catalog is forgotten. Call it before
    /// the statement runs and again after (`failed`: it failed, so the tables it names are
    /// forgotten as well).
    pub fn note_raw_statement(&self, sql: &str, failed: bool) {
        self.note_catalog_statement(sql);
        if failed {
            self.forget_catalog_failed(sql);
        }
    }

    /// Forget the catalog answers about one table.
    pub(crate) fn forget_catalog_table(&self, table: &str) {
        self.catalog().forget_table(table);
    }

    /// The columns types `{column: DB TYPE}` of the pipe's table as the last successful
    /// `sync_pipe` of this connector left them, handed out once (`None` when that sync created
    /// the table, did not finish, was given the layout by its caller, or was already taken, and
    /// when a statement or a failure invalidated the table since).
    ///
    /// A sync reflects the table's columns before it writes, and again after any DDL of its own,
    /// so what it holds at the end is the table's layout. The Python façade adopts it as the
    /// pipe's cached `get_columns_types()` instead of reflecting the table a second time on the
    /// next read.
    pub fn take_synced_columns_types(
        &self,
        pipe: &meerschaum_core::Pipe,
    ) -> Option<IndexMap<String, String>> {
        self.catalog().take_synced(&synced_key(pipe))
    }
}

/// The key of a pipe's post-sync layout: its keys, which a caller has without resolving the
/// pipe's parameters.
pub(crate) fn synced_key(pipe: &meerschaum_core::Pipe) -> String {
    format!(
        "{:?}",
        (
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key()
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes() {
        assert_eq!(ddl_scope("SELECT 1"), DdlScope::None);
        assert_eq!(ddl_scope("INSERT INTO \"t\" VALUES (1)"), DdlScope::None);
        assert_eq!(
            ddl_scope("  -- note\nALTER TABLE \"public\".\"t\" ADD COLUMN \"a\" INT"),
            DdlScope::Tables("ALTER TABLE \"public\".\"t\" ADD COLUMN \"a\" INT")
        );
        assert_eq!(
            ddl_scope("CREATE TABLE \"_t_ab_update\" AS SELECT * FROM \"t\""),
            DdlScope::Tables("CREATE TABLE \"_t_ab_update\" ")
        );
        assert_eq!(
            ddl_scope("CREATE INDEX \"IX_t_id\" ON \"public\".\"t\" (\"id\")"),
            DdlScope::Tables("CREATE INDEX \"IX_t_id\" ON \"public\".\"t\" ")
        );
        assert_eq!(ddl_scope("DROP INDEX \"IX_t_id\""), DdlScope::All);
        assert_eq!(
            ddl_scope(
                "CREATE TABLE IF NOT EXISTS \"public\".\"t_p_2025\" PARTITION OF \"public\".\"t\"\nFOR VALUES FROM ('2025') TO ('2026')"
            ),
            DdlScope::Tables("CREATE TABLE IF NOT EXISTS \"public\".\"t_p_2025\"")
        );
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'CREATE TABLE x (a INT)'; END;"),
            DdlScope::Tables("BEGIN EXECUTE IMMEDIATE 'CREATE TABLE x (a INT)'; END;")
        );
        // A statement built at run time, DDL outside the literal, or an index dropped by name
        // may change anything.
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'DROP TABLE ' || v_name; END;"),
            DdlScope::All
        );
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'SELECT 1 FROM dual'; DROP TABLE x; END;"),
            DdlScope::All
        );
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'DROP INDEX \"IX_t\"'; END;"),
            DdlScope::All
        );
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'ALTER SESSION SET x = ''y'''; END;"),
            DdlScope::Tables("BEGIN EXECUTE IMMEDIATE 'ALTER SESSION SET x = ''y'''; END;")
        );
        assert_eq!(
            ddl_scope("BEGIN EXECUTE IMMEDIATE 'CREATE TABLE x (a INT)' USING y; END;"),
            DdlScope::All
        );
        // Procedural blocks with no `EXECUTE IMMEDIATE` (MSSQL's guarded DDL) stay `All`.
        assert_eq!(
            ddl_scope("IF NOT EXISTS (SELECT 1) CREATE INDEX \"IX_t\" ON \"t\" (\"a\")"),
            DdlScope::All
        );
        assert_eq!(ddl_scope("BEGIN"), DdlScope::None);
        assert!(matches!(
            ddl_scope("SELECT create_hypertable('\"public\".\"t\"', 'ts')"),
            DdlScope::Tables(_)
        ));
    }

    #[test]
    fn identifier_tokens() {
        assert!(names_table(
            "DROP TABLE \"PUBLIC\".\"COMMANDS\"",
            "COMMANDS"
        ));
        assert!(!names_table(
            "CREATE TABLE \"_COMMANDS_AB12_UPDATE\"",
            "COMMANDS"
        ));
        assert!(!names_table(
            "ALTER TABLE COMMANDS_RESPONSES ADD X INT",
            "COMMANDS"
        ));
        assert!(names_table(
            "ALTER TABLE [DBO].[COMMANDS] ADD X INT",
            "COMMANDS"
        ));
    }

    #[test]
    fn ddl_forgets_named_tables_only() {
        let ttl = Duration::from_secs(60);
        let mut cache = CatalogCache::default();
        let cols: IndexMap<String, String> = [("a".to_string(), "INT".to_string())].into();
        let g = cache.generation();
        cache.put_cols_types("commands", Some("public"), false, &cols, g);
        cache.put_cols_types("responses", Some("public"), false, &cols, g);
        assert!(cache.exists("commands", Some("public"), ttl));
        // Staging a temporary table from the definition changes neither table.
        cache.note_statement(
            "CREATE TABLE \"_mrsm_internal\".\"_commands_ab12_update\" AS SELECT * FROM \"public\".\"responses\"",
            Flavor::Postgresql,
        );
        assert!(
            cache
                .cols_types("commands", Some("public"), false, ttl)
                .is_some()
        );
        assert!(
            cache
                .cols_types("responses", Some("public"), false, ttl)
                .is_some()
        );
        cache.note_statement(
            "ALTER TABLE \"public\".\"commands\" ADD COLUMN \"b\" TEXT",
            Flavor::Postgresql,
        );
        assert!(
            cache
                .cols_types("commands", Some("public"), false, ttl)
                .is_none()
        );
        assert!(!cache.exists("commands", Some("public"), ttl));
        assert!(
            cache
                .cols_types("responses", Some("public"), false, ttl)
                .is_some()
        );
    }

    #[test]
    fn reads_racing_an_invalidation_are_not_stored() {
        let ttl = Duration::from_secs(60);
        let mut cache = CatalogCache::default();
        let cols: IndexMap<String, String> = [("a".to_string(), "INT".to_string())].into();
        // A read that started before a DDL of its table is never stored.
        let started = cache.generation();
        cache.note_statement("DROP TABLE \"t\"", Flavor::Sqlite);
        cache.put_cols_types("t", None, false, &cols, started);
        assert!(cache.cols_types("t", None, false, ttl).is_none());
        // A data change does not invalidate anything.
        let started = cache.generation();
        cache.note_statement("INSERT INTO \"t\" VALUES (1)", Flavor::Sqlite);
        cache.put_cols_types("t", None, false, &cols, started);
        assert!(cache.cols_types("t", None, false, ttl).is_some());
        // A failure forgets everything.
        let g = cache.generation();
        cache.put_exists("t", None, g);
        cache.clear();
        assert!(!cache.exists("t", None, ttl));
        assert!(cache.cols_types("t", None, false, Duration::ZERO).is_none());
    }

    fn ready_state() -> PipeState {
        PipeState {
            exists: true,
            cols_types: [("a".to_string(), "INT".to_string())].into(),
            ..Default::default()
        }
    }

    #[test]
    fn internal_readiness_survives_unrelated_clears() {
        let mut cache = CatalogCache::default();
        let state = ready_state();
        cache.put_internal_ready("mrsm_engine_metrics", Some("mrsm"), &state);
        cache.put_internal_ready("mrsm_engine_runs", Some("mrsm"), &state);
        // A procedural block that may change anything forgets the catalog, not readiness.
        cache.note_statement(
            "DECLARE x INT; BEGIN EXECUTE IMMEDIATE 'DROP TABLE ' || v_name; END;",
            Flavor::Oracle,
        );
        cache.clear();
        assert_eq!(
            cache.internal_ready("mrsm_engine_metrics", Some("mrsm")),
            Some(state.clone())
        );
        // A statement naming the table, and a failed write (`forget_table`), forget it.
        cache.note_statement(
            "ALTER TABLE \"mrsm\".\"mrsm_engine_metrics\" ADD \"b\" INT",
            Flavor::Oracle,
        );
        assert!(
            cache
                .internal_ready("mrsm_engine_metrics", Some("mrsm"))
                .is_none()
        );
        assert!(
            cache
                .internal_ready("mrsm_engine_runs", Some("mrsm"))
                .is_some()
        );
        cache.forget_table("mrsm_engine_runs");
        assert!(
            cache
                .internal_ready("mrsm_engine_runs", Some("mrsm"))
                .is_none()
        );
    }

    #[test]
    fn oracle_guarded_index_forgets_only_its_table() {
        let ttl = Duration::from_secs(60);
        let mut cache = CatalogCache::default();
        let state = ready_state();
        let cols = state.cols_types.clone();
        let g = cache.generation();
        cache.put_cols_types("mrsm_engine_runs", Some("MRSM"), false, &cols, g);
        cache.put_cols_types("other", Some("MRSM"), false, &cols, g);
        cache.put_internal_ready("mrsm_engine_metrics", Some("MRSM"), &state);
        // The shape `guard_oracle_internal_indices` emits for an internal table's index.
        let guarded = "DECLARE matching_objects PLS_INTEGER;\nBEGIN\n    EXECUTE IMMEDIATE \
            'CREATE INDEX \"IX_mrsm_engine_runs_run_id\" ON \"MRSM\".\"mrsm_engine_runs\" (\"run_id\")';\n\
            EXCEPTION WHEN OTHERS THEN\n    IF SQLCODE NOT IN (-955, -1408) THEN RAISE; END IF;\n\
            SELECT COUNT(*) INTO matching_objects FROM all_indexes existing\n\
            WHERE existing.owner = 'MRSM' AND existing.table_name = 'mrsm_engine_runs'\n\
            AND existing.index_name = 'IX_mrsm_engine_runs_run_id';\n\
            IF matching_objects != 1 THEN RAISE; END IF;\nEND;";
        assert_eq!(ddl_scope(guarded), DdlScope::Tables(guarded));
        cache.note_statement(guarded, Flavor::Oracle);
        assert!(
            cache
                .cols_types("mrsm_engine_runs", Some("MRSM"), false, ttl)
                .is_none()
        );
        assert!(
            cache
                .cols_types("other", Some("MRSM"), false, ttl)
                .is_some()
        );
        assert!(
            cache
                .internal_ready("mrsm_engine_metrics", Some("MRSM"))
                .is_some()
        );
    }

    #[test]
    fn synced_layout_is_handed_out_once() {
        let mut cache = CatalogCache::default();
        let cols: IndexMap<String, String> = [("a".to_string(), "INT".to_string())].into();
        cache.put_synced("p", "t", None, &cols);
        assert_eq!(cache.take_synced("p"), Some(cols.clone()));
        assert_eq!(cache.take_synced("p"), None);
        // Invalidating its table, or a whole clear, drops an untaken layout.
        cache.put_synced("p", "t", None, &cols);
        cache.note_statement("ALTER TABLE \"t\" ADD \"b\" INT", Flavor::Sqlite);
        assert_eq!(cache.take_synced("p"), None);
        cache.put_synced("p", "t", None, &cols);
        cache.clear();
        assert_eq!(cache.take_synced("p"), None);
        cache.put_synced("p", "t", None, &cols);
        cache.forget_synced("p");
        assert_eq!(cache.take_synced("p"), None);
    }
}
