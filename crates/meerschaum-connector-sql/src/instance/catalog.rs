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
//! The lock is never held across an `await`, and the connector is rebuilt after a fork (the
//! Python façade), so no lock is inherited.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use indexmap::IndexMap;

use crate::flavor::Flavor;
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
}

fn key(table: &str, schema: Option<&str>) -> TableKey {
    (schema.map(str::to_string), table.to_string())
}

fn fresh(at: Instant, ttl: Duration) -> bool {
    at.elapsed() < ttl
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
        self.exists
            .get(&key(table, schema))
            .is_some_and(|at| fresh(*at, ttl))
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
        map.get(&key(table, schema))
            .filter(|(at, _)| fresh(*at, ttl))
            .map(|(_, v)| v.clone())
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
        self.indices
            .get(&key(table, schema))
            .filter(|(at, _)| fresh(*at, ttl))
            .map(|(_, v)| v.clone())
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
        self.conflict
            .get(&(key(table, schema), join_cols.to_vec()))
            .filter(|(at, _)| fresh(*at, ttl))
            .map(|(_, v)| v.clone())
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
        self.staged
            .get(&(key(table, schema), parameters))
            .filter(|(at, _)| fresh(*at, ttl))
            .map(|(_, v)| v.clone())
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

    /// Forget everything.
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.exists.clear();
        self.cols_types.clear();
        self.reflected.clear();
        self.indices.clear();
        self.conflict.clear();
        self.staged.clear();
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
            if mentions(&["CREATE", "ALTER", "DROP", "RENAME"]) {
                DdlScope::All
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
}
