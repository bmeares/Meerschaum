//! The SQL guard: one scanner and one set of query policies for pipe definitions, MCP
//! `read_sql`, the canonical SQL source broker and native plugins' SQL host calls.
//!
//! A pipe's SQL runs with the source connector's database credentials, not the caller's, so
//! visibility alone cannot keep a definition or query from reading another team's table or
//! Meerschaum's internal tables. [`check_sql`] enforces one [`SqlPolicy`]:
//!
//! - **Pipe SQL definitions** ([`SqlPolicy::PipeDefinition`]): no `MRSM{...}` config symlinks,
//!   no Meerschaum internal tables (`mrsm_*`, `_mrsm_*`, `sqlite_dbpage`, protected instance
//!   tables) or functions that run query text, no tables of pipes the caller cannot see (bare,
//!   quoted, schema-qualified, or in string literals), and no catalog reads while hidden pipes
//!   exist.
//! - **Read-only API queries** ([`SqlPolicy::ReadOnlyApiQuery`]): one read-only statement (or a
//!   bare table name), v4's protected-table refusal, the internal-table refusal, plus the hidden
//!   table and catalog checks for non-administrators.
//! - **Read-only native plugin queries** ([`SqlPolicy::ReadOnlyHostQuery`]).
//! - **Read-only stdio queries** ([`SqlPolicy::ReadOnlyStdioQuery`]): v4's read-only check and
//!   protected-table refusal for local-trust callers.
//! - **Canonical SQL source-broker queries** ([`SqlPolicy::SourceBrokerQuery`]): read-only, no
//!   internal or hidden tables, only unqualified pure SQL functions, and no `FROM 'file'`.
//! - **Raw API SQL statements** ([`SqlPolicy::RawExec`]): native plugin `exec_sql` calls.
//!
//! The guard does not know the database flavor, and flavors disagree about what is a comment or
//! where a string ends: `#` starts a comment on MySQL but is an operator on PostgreSQL and a
//! temporary table on SQL Server; MySQL needs whitespace after `--`; PostgreSQL and SQL Server
//! nest block comments, the others close them at the first `*/`; MySQL reads `\` escapes in every
//! string, PostgreSQL only in `E'...'`. So every text is read the way each of those dialects
//! reads it, and every reading must pass. Names of Meerschaum's internal tables are refused
//! anywhere in the raw text, comments included.
//!
//! This is best-effort keyword/token scanning, not a full SQL parser: true isolation between
//! teams needs separate connectors (database users) per team, and untrusted read-only callers
//! should use a read-only database role.
//!
//! The submodules are private: `lexer` reads a text the way each SQL dialect does, `policy`
//! holds the checks each [`SqlPolicy`] runs, and this module exposes the interface.

mod lexer;
mod policy;
#[cfg(test)]
mod tests;

use std::fmt;

use meerschaum_core::{Map, Value};

use lexer::{Readings, identifier_chains};
use policy::{
    Step, check_config_reference, check_hidden_tables, check_internal_tables,
    check_protected_tables, check_read_only, check_source_broker,
};

/// The parameter keys of a pipe's SQL definition, in the order `get_pipe_query` reads them
/// (`fetch.definition` first).
pub(crate) const DEFINITION_KEYS: &[&[&str]] = &[
    &["fetch", "definition"],
    &["definition"],
    &["query"],
    &["sql"],
];

/// The target-name parameters of a pipe.
pub(crate) const TARGET_KEYS: &[&str] =
    &["target", "target_name", "target_table", "target_table_name"];

/// v4's `PROTECTED_TABLES`: the instance tables `read_sql` and the pipe tools refuse.
pub(crate) const PROTECTED_TABLES: &[&str] =
    &["mrsm_pipes", "mrsm_plugins", "mrsm_tokens", "mrsm_users"];

/// v4's `_READ_ONLY_STARTS`: the first words of a read-only statement.
pub(crate) const READ_ONLY_STARTS: &[&str] = &[
    "desc", "describe", "explain", "select", "show", "table", "values", "with",
];

/// v4's `_MUTATING_KEYWORDS`.
pub(crate) const MUTATING_KEYWORDS: &[&str] = &[
    "alter",
    "analyze",
    "attach",
    "backup",
    "begin",
    "bulk",
    "call",
    "checkpoint",
    "cluster",
    "comment",
    "commit",
    "copy",
    "create",
    "deallocate",
    "delete",
    "detach",
    "discard",
    "do",
    "drop",
    "dumpfile",
    "exec",
    "execute",
    "export",
    "grant",
    "import",
    "insert",
    "into",
    "kill",
    "listen",
    "load",
    "lock",
    "merge",
    "notify",
    "opendatasource",
    "openquery",
    "openrowset",
    "outfile",
    "pragma",
    "prepare",
    "refresh",
    "reindex",
    "rename",
    "replace",
    "reset",
    "restore",
    "revoke",
    "rollback",
    "savepoint",
    "set",
    "shutdown",
    "start",
    "truncate",
    "unlock",
    "update",
    "upsert",
    "vacuum",
];

/// v4's `_MUTATING_FUNCTIONS`.
pub(crate) const MUTATING_FUNCTIONS: &[&str] = &[
    "dblink",
    "dblink_connect",
    "dblink_connect_u",
    "dblink_exec",
    "fsync",
    "lo_export",
    "lo_import",
    "lo_unlink",
    "load_extension",
    "load_file",
    "nextval",
    "pg_cancel_backend",
    "pg_create_logical_replication_slot",
    "pg_create_physical_replication_slot",
    "pg_create_restore_point",
    "pg_drop_replication_slot",
    "pg_logical_slot_get_changes",
    "pg_logical_slot_peek_changes",
    "pg_ls_dir",
    "pg_promote",
    "pg_read_binary_file",
    "pg_read_file",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_stat_file",
    "pg_switch_wal",
    "pg_terminate_backend",
    "readfile",
    "setval",
    "writefile",
];

/// v4's `_MUTATING_WORD_PREFIXES`: procedural packages.
pub(crate) const MUTATING_WORD_PREFIXES: &[&str] = &["xp_", "sp_", "dbms_", "utl_"];

/// Why a SQL policy refused a query or definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    /// Malformed, empty, multi-statement, or non-read-only query (`ValueError` in MCP).
    InvalidQuery,
    /// Access or security refusal: protected/internal/hidden table, catalog, `MRSM{}`, or
    /// disallowed source-broker construct (`PermissionError` in MCP, `403` in HTTP API).
    Forbidden,
}

/// A refusal produced by [`check_sql`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    /// Whether this is an invalid-query error or a permission/policy refusal.
    pub kind: SqlErrorKind,
    /// Human-readable refusal message (preserving v4 parity wording where frozen).
    pub message: String,
}

impl SqlError {
    fn invalid_query(message: impl Into<String>) -> Self {
        Self {
            kind: SqlErrorKind::InvalidQuery,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            kind: SqlErrorKind::Forbidden,
            message: message.into(),
        }
    }

    /// Consume the error and return its message string.
    pub fn into_message(self) -> String {
        self.message
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SqlError {}

/// Which SQL policy profile to enforce in [`check_sql`].
#[derive(Debug, Clone, Copy)]
pub enum SqlPolicy<'a> {
    /// A pipe's SQL definition (raw or resolved) authored by a non-administrator.
    PipeDefinition {
        /// Target tables of pipes the caller cannot see.
        hidden: &'a HiddenTables,
    },
    /// A read-only SQL query from an HTTP MCP caller (`read_sql` over HTTP). `hidden` is `None`
    /// for administrators and `Some(...)` for non-administrators; keeps v4 MCP's protected-table
    /// refusal wording.
    ReadOnlyApiQuery {
        /// Hidden tables for non-administrators (`None` skips hidden-table / catalog / `MRSM{}`
        /// checks while still enforcing read-only, protected tables, and internal tables).
        hidden: Option<&'a HiddenTables>,
    },
    /// A read-only SQL query from a native plugin host call (`read_sql` / `read_sql_with` /
    /// `tx_read_sql`) for an API caller.
    ReadOnlyHostQuery {
        /// Target tables of pipes the caller cannot see.
        hidden: &'a HiddenTables,
    },
    /// A read-only SQL query from a trusted local stdio caller (v4 MCP parity).
    ReadOnlyStdioQuery,
    /// A canonical SQL source-broker query (`routes/source_fetch`).
    SourceBrokerQuery {
        /// Target tables of pipes the caller cannot see on the source instance.
        hidden: &'a HiddenTables,
    },
    /// A raw SQL statement executed by a native plugin for an API caller (`exec_sql`).
    RawExec {
        /// Target tables of pipes the caller cannot see.
        hidden: &'a HiddenTables,
    },
}

/// The definition texts of `parameters` (the non-empty strings at the definition keys).
pub fn definitions(parameters: &Map) -> Vec<String> {
    let mut out = Vec::new();
    for path in DEFINITION_KEYS {
        let mut cur = parameters.get(path[0]);
        for key in &path[1..] {
            cur = cur.and_then(|v| v.get(key));
        }
        if let Some(text) = cur.filter(|v| !v.is_null()).map(Value::py_str)
            && !text.trim().is_empty()
        {
            out.push(text);
        }
    }
    out
}

fn at<'a>(parameters: Option<&'a Map>, path: &[&str]) -> Option<&'a Value> {
    let mut cur = parameters?.get(path[0])?;
    for key in &path[1..] {
        cur = cur.get(key)?;
    }
    Some(cur).filter(|v| !v.is_null())
}

/// Whether `new` sets or changes the SQL definition of a pipe of `connector_keys` (compared with
/// the stored `old`, `None` for a new pipe): a definition key, or (for `sql:` connectors) any
/// `fetch` parameter. Removing a definition is not setting one.
pub fn changes_definition(connector_keys: &str, old: Option<&Map>, new: &Map) -> bool {
    for path in DEFINITION_KEYS {
        if let Some(value) = at(Some(new), path)
            && at(old, path) != Some(value)
        {
            return true;
        }
    }
    let sql_connector = connector_keys.split(':').next() == Some("sql");
    if sql_connector && let Some(fetch) = at(Some(new), &["fetch"]) {
        return at(old, &["fetch"]) != Some(fetch);
    }
    false
}

/// The names (lower case) a definition or query must not mention, besides Meerschaum's internal
/// tables: the target tables of the pipes the caller cannot see.
#[derive(Debug, Clone, Default)]
pub struct HiddenTables {
    names: Vec<String>,
    identifier_byte_limit: Option<usize>,
}

impl HiddenTables {
    /// From the hidden pipes' target names (any case; duplicates and empty names dropped).
    pub fn new<I, S>(names: I) -> HiddenTables
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut out: Vec<String> = Vec::new();
        for name in names {
            let name = name.as_ref().trim().to_lowercase();
            if !name.is_empty() && !out.contains(&name) {
                out.push(name);
            }
        }
        HiddenTables {
            names: out,
            identifier_byte_limit: None,
        }
    }

    /// Normalize query identifiers as the database does, so a long spelling cannot alias a
    /// hidden physical table through implicit byte clipping.
    pub fn with_identifier_byte_limit(mut self, limit: Option<usize>) -> Self {
        self.identifier_byte_limit = limit;
        self
    }

    fn normalize_identifier(&self, identifier: &str) -> String {
        let mut end = self
            .identifier_byte_limit
            .unwrap_or(identifier.len())
            .min(identifier.len());
        while !identifier.is_char_boundary(end) {
            end -= 1;
        }
        identifier[..end].to_lowercase()
    }

    fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// Check `sql` against a [`SqlPolicy`].
pub fn check_sql(sql: &str, policy: &SqlPolicy<'_>) -> Result<(), SqlError> {
    let readings = Readings::new(sql);
    let phrasing = policy.phrasing();
    for step in policy.steps() {
        match step {
            Step::Lexing => {
                if let Some(reason) = readings.lexing_error(false) {
                    return Err(SqlError::forbidden(format!(
                        "Refusing {}: {reason}",
                        phrasing.subject
                    )));
                }
            }
            Step::ReadOnly => check_read_only(sql, &readings)?,
            Step::ProtectedTables => check_protected_tables(&readings)?,
            Step::InternalTables => {
                check_internal_tables(sql, &readings, phrasing.internal_context)?;
            }
            Step::ConfigReference => check_config_reference(sql, phrasing)?,
            Step::HiddenTables => {
                if let Some(hidden) = policy.hidden() {
                    check_hidden_tables(&readings, hidden, phrasing)?;
                }
            }
            Step::SourceBroker => check_source_broker(&readings)?,
        }
    }
    Ok(())
}

/// Check a target table name (`target`, `target_name`, `target_table`, `target_table_name`) a
/// caller who is not an administrator gives a pipe: never an internal table, never the table of
/// a pipe the caller cannot see.
pub fn check_target(target: &str, hidden: &HiddenTables) -> Result<(), String> {
    if target.contains("MRSM{") {
        return Err(
            "Refusing a target with an `MRSM{...}` config reference: only administrators may \
             read the server's configuration into pipe parameters."
                .into(),
        );
    }
    let lower = target.trim().to_lowercase();
    if lower.is_empty() {
        return Ok(());
    }
    let last = lower
        .rsplit('.')
        .next()
        .unwrap_or(&lower)
        .trim_matches(['"', '`', '[', ']']);
    if last.starts_with("mrsm_") || last.starts_with("_mrsm_") {
        return Err(format!(
            "Refusing to target '{target}': it is one of Meerschaum's internal tables."
        ));
    }
    let readings = Readings::new(target);
    let named = |h: &String| {
        *h == lower
            || h == last
            || readings.tokens().any(|tokens| {
                identifier_chains(tokens, |name| hidden.normalize_identifier(name))
                    .iter()
                    .any(|chain| chain.last() == Some(h))
            })
    };
    if hidden.names.iter().any(named) {
        return Err(format!(
            "Refusing to target '{target}': it is the table of a pipe you cannot see."
        ));
    }
    Ok(())
}
