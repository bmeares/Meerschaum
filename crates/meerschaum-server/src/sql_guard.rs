//! The SQL guard: one scanner and one set of query policies for pipe definitions (access model
//! AC8), MCP `read_sql`, the canonical SQL source broker and native plugins' SQL host calls.
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

use std::fmt;

use meerschaum_core::{Map, Value};

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

/// Catalog schemas: a qualified name starting with one of these reads the catalog.
const CATALOG_SCHEMAS: &[&str] = &[
    "information_schema",
    "pg_catalog",
    "sys",
    "mysql",
    "performance_schema",
    "sqlite_temp_schema",
];

/// Catalog tables, views and functions (any qualification).
const CATALOG_NAMES: &[&str] = &[
    "all_objects",
    "all_tab_columns",
    "all_tables",
    "all_views",
    "dba_objects",
    "dba_tables",
    "duckdb_columns",
    "duckdb_tables",
    "duckdb_views",
    "pg_attribute",
    "pg_class",
    "pg_namespace",
    "pg_stat_all_tables",
    "pg_stat_user_tables",
    "pg_statistic",
    "pg_tables",
    "pg_views",
    "pragma_table_info",
    "pragma_table_list",
    "sqlite_master",
    "sqlite_schema",
    "sqlite_temp_master",
    "syscolumns",
    "sysobjects",
    "user_objects",
    "user_tab_columns",
    "user_tables",
    "user_views",
];

/// Functions that run the query text they are given, read tables named in text, export whole
/// tables, schemas or databases, or read the database's files, so they could reach Meerschaum's
/// internal tables without naming them: PostgreSQL's XML exports, `ts_stat`, `ts_rewrite`,
/// `pg_read_file` and `pg_read_binary_file` (and the `dblink` and `tablefunc` extensions'
/// `dblink` and `crosstab`), Oracle's `DBMS_XMLGEN` / `DBMS_XMLQUERY` (`getxml`, `getxmltype`,
/// `newcontext`) and XQuery (`xmlquery`, `xmltable`, whose `fn:collection` reads tables by name),
/// and DuckDB's `query` and `query_table`. Each is refused wherever its name appears, except
/// `query` (a common column name), which is refused when it is called.
const TEXT_QUERY_FUNCTIONS: &[&str] = &[
    "crosstab",
    "cursor_to_xml",
    "cursor_to_xmlschema",
    "database_to_xml",
    "database_to_xml_and_xmlschema",
    "database_to_xmlschema",
    "dblink",
    "dblink_exec",
    "dblink_open",
    "dblink_send_query",
    "getxml",
    "getxmltype",
    "newcontext",
    "pg_read_binary_file",
    "pg_read_file",
    "query",
    "query_table",
    "query_to_xml",
    "query_to_xml_and_xmlschema",
    "query_to_xmlschema",
    "schema_to_xml",
    "schema_to_xml_and_xmlschema",
    "schema_to_xmlschema",
    "table_to_xml",
    "table_to_xml_and_xmlschema",
    "table_to_xmlschema",
    "ts_rewrite",
    "ts_stat",
    "xmlquery",
    "xmltable",
];

/// Ordinary pure SQL functions, syntax keywords and parameterized type names the source broker
/// permits. Database file/network readers and user-defined functions can escape table-name
/// guards, so the source broker deliberately allows only this small unqualified set.
const SOURCE_BROKER_PURE_FUNCTIONS: &[&str] = &[
    "abs",
    "avg",
    "cast",
    "ceil",
    "ceiling",
    "char_length",
    "coalesce",
    "concat",
    "concat_ws",
    "convert",
    "count",
    "date",
    "datetime",
    "dense_rank",
    "extract",
    "floor",
    "ifnull",
    "julianday",
    "lag",
    "lead",
    "length",
    "lower",
    "ltrim",
    "max",
    "min",
    "nullif",
    "rank",
    "replace",
    "round",
    "row_number",
    "rtrim",
    "strftime",
    "substr",
    "substring",
    "sum",
    "time",
    "trim",
    "unixepoch",
    "upper",
    "in",
    "exists",
    "as",
    "over",
    "filter",
    "when",
    "not",
    "by",
    "values",
    "from",
    "where",
    "select",
    "with",
    "and",
    "or",
    "top",
    "char",
    "varchar",
    "nvarchar",
    "nchar",
    "decimal",
    "numeric",
    "number",
    "varbinary",
    "binary",
    "timestamp",
    "timestamptz",
    "timetz",
    "datetime2",
    "datetimeoffset",
];

/// One token of a SQL text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// An identifier: bare (`t`) or quoted (`"t"`, `` `t` ``, `[t]`), without its quotes.
    Ident {
        /// The name.
        name: String,
        /// Whether it was quoted.
        quoted: bool,
    },
    /// A string literal's contents (`'...'`, `E'...'`, `$tag$...$tag$`).
    Str(String),
    /// Any other character that is not whitespace.
    Punct(char),
}

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

/// One check of a policy, run in the policy's order.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// Refuse executable comments (`/*! ... */`) under any reading.
    Lexing,
    /// One read-only statement under every reading (or a bare table name); includes `Lexing`.
    ReadOnly,
    /// v4's protected-table refusal on unquoted words.
    ProtectedTables,
    /// Meerschaum's internal tables and the functions that run query text.
    InternalTables,
    /// `MRSM{...}` config references.
    ConfigReference,
    /// The tables of hidden pipes, and the catalog while there are any.
    HiddenTables,
    /// The source broker's pure-function allowlist and file-table refusal.
    SourceBroker,
}

#[derive(Debug, Clone, Copy)]
struct Phrasing {
    subject: &'static str,
    internal_context: &'static str,
}

impl Phrasing {
    const DEFINITION: Self = Self {
        subject: "a SQL definition",
        internal_context: "in a pipe's definition",
    };
    const API_QUERY: Self = Self {
        subject: "a query",
        internal_context: "for an API request",
    };
    const API_STATEMENT: Self = Self {
        subject: "a SQL statement",
        internal_context: "for an API request",
    };
}

impl SqlPolicy<'_> {
    fn steps(&self) -> &'static [Step] {
        use Step::*;
        match self {
            SqlPolicy::ReadOnlyStdioQuery => &[ReadOnly, ProtectedTables],
            SqlPolicy::ReadOnlyApiQuery { hidden: None } => {
                &[ReadOnly, ProtectedTables, InternalTables]
            }
            SqlPolicy::ReadOnlyApiQuery { hidden: Some(_) } => &[
                ReadOnly,
                ProtectedTables,
                InternalTables,
                ConfigReference,
                HiddenTables,
            ],
            SqlPolicy::ReadOnlyHostQuery { .. } => {
                &[ReadOnly, InternalTables, ConfigReference, HiddenTables]
            }
            SqlPolicy::PipeDefinition { .. } => {
                &[ConfigReference, Lexing, InternalTables, HiddenTables]
            }
            SqlPolicy::SourceBrokerQuery { .. } => &[
                ReadOnly,
                ConfigReference,
                InternalTables,
                HiddenTables,
                SourceBroker,
            ],
            SqlPolicy::RawExec { .. } => &[Lexing, InternalTables, ConfigReference, HiddenTables],
        }
    }

    fn hidden(&self) -> Option<&HiddenTables> {
        match *self {
            SqlPolicy::ReadOnlyStdioQuery => None,
            SqlPolicy::ReadOnlyApiQuery { hidden } => hidden,
            SqlPolicy::ReadOnlyHostQuery { hidden }
            | SqlPolicy::PipeDefinition { hidden }
            | SqlPolicy::SourceBrokerQuery { hidden }
            | SqlPolicy::RawExec { hidden } => Some(hidden),
        }
    }

    fn phrasing(&self) -> Phrasing {
        match self {
            SqlPolicy::PipeDefinition { .. } | SqlPolicy::SourceBrokerQuery { .. } => {
                Phrasing::DEFINITION
            }
            SqlPolicy::RawExec { .. } => Phrasing::API_STATEMENT,
            SqlPolicy::ReadOnlyApiQuery { .. }
            | SqlPolicy::ReadOnlyHostQuery { .. }
            | SqlPolicy::ReadOnlyStdioQuery => Phrasing::API_QUERY,
        }
    }
}

/// How one SQL dialect delimits comments, strings and quoted identifiers.
#[derive(Debug, Clone, Copy)]
struct Dialect {
    /// `#` starts a line comment (MySQL, MariaDB).
    hash_comments: bool,
    /// `--` starts a comment only before whitespace, a control character or the end (MySQL).
    dash_comments_need_space: bool,
    /// `/* /* */ */` is one comment (PostgreSQL, DuckDB, SQL Server).
    nested_comments: bool,
    /// A carriage return ends a line comment as a newline does (PostgreSQL, SQL Server).
    carriage_return_ends_comments: bool,
    /// `\` escapes the next character in `'...'` and `"..."` (MySQL; PostgreSQL with
    /// `standard_conforming_strings` off).
    backslash_escapes: bool,
    /// `E'...'` reads `\` escapes (PostgreSQL, DuckDB).
    escape_strings: bool,
    /// `E'...'`, `N'...'`, `B'...'` and `X'...'` are strings, prefix included (v4's statement
    /// scanner read the prefix as a word).
    string_prefixes: bool,
    /// `$$...$$` and `$tag$...$tag$` are strings (PostgreSQL, DuckDB).
    dollar_quotes: bool,
    /// `$` is part of an identifier, so a dollar quote cannot start right after a letter or
    /// digit (v4's statement scanner opened one after any character).
    dollar_in_identifiers: bool,
}

/// The readings every SQL text must pass.
const DIALECTS: &[Dialect] = &[
    // v4's statement scanner (`strip_sql_noise`): the read-only check refuses whatever this
    // reading cannot lex, exactly as v4 did.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: false,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: false,
        dollar_quotes: true,
        dollar_in_identifiers: false,
    },
    // v4's table-name scanner.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // PostgreSQL, TimescaleDB, DuckDB.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: false,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // PostgreSQL with `standard_conforming_strings` off.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: true,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // MySQL, MariaDB.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: true,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: true,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // MySQL, MariaDB with `NO_BACKSLASH_ESCAPES`.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: true,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // SQLite, Oracle.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // SQL Server.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
];

/// The reading whose lexing errors the read-only check refuses (v4's statement scanner).
const STATEMENT_DIALECT: usize = 0;

const EXECUTABLE_COMMENT: &str =
    "Refusing an executable comment (`/*! ... */`), which MySQL runs as SQL.";

/// One reading of a SQL text.
struct Reading {
    tokens: Vec<Token>,
    /// Why this reading could not lex the text (its tokens then run to the end of the text).
    error: Option<&'static str>,
    /// Whether this reading saw an executable comment (`/*!` or MariaDB's `/*M!`).
    executable_comment: bool,
}

/// Every reading of one SQL text, in [`DIALECTS`] order.
struct Readings(Vec<Reading>);

impl Readings {
    fn new(sql: &str) -> Self {
        let chars: Vec<char> = sql.chars().collect();
        Self(DIALECTS.iter().map(|d| lex(&chars, d)).collect())
    }

    fn tokens(&self) -> impl Iterator<Item = &[Token]> {
        self.0.iter().map(|r| r.tokens.as_slice())
    }

    /// Refuse an executable comment under any reading and, when `statements` is set, whatever
    /// the statement reading cannot lex.
    fn lexing_error(&self, statements: bool) -> Option<&'static str> {
        if self.0.iter().any(|r| r.executable_comment) {
            return Some(EXECUTABLE_COMMENT);
        }
        if statements {
            return self.0[STATEMENT_DIALECT].error;
        }
        None
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// The length (in chars) of a dollar-quote tag at `chars[i..]` (`$$` or `$tag$`).
fn dollar_tag_len(chars: &[char], i: usize) -> Option<usize> {
    if chars.get(i) != Some(&'$') {
        return None;
    }
    if chars.get(i + 1) == Some(&'$') {
        return Some(2);
    }
    let first = *chars.get(i + 1)?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    let mut j = i + 2;
    while let Some(c) = chars.get(j) {
        if *c == '$' {
            return Some(j + 1 - i);
        }
        if !(c.is_ascii_alphanumeric() || *c == '_') {
            return None;
        }
        j += 1;
    }
    None
}

/// The index after the block comment that opens at `chars[i..]`, or `None` when it is
/// unterminated.
fn block_comment_end(chars: &[char], i: usize, nested: bool) -> Option<usize> {
    let n = chars.len();
    let mut depth = 1usize;
    let mut j = i + 2;
    while j < n {
        if nested && chars[j] == '/' && chars.get(j + 1) == Some(&'*') {
            depth += 1;
            j += 2;
        } else if chars[j] == '*' && chars.get(j + 1) == Some(&'/') {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return Some(j);
            }
        } else {
            j += 1;
        }
    }
    None
}

/// Tokenize `chars` as `dialect` reads it: comments are dropped; string literals and quoted
/// identifiers are read whole, and an unterminated one runs to the end of the text (recorded in
/// [`Reading::error`]).
fn lex(chars: &[char], dialect: &Dialect) -> Reading {
    let n = chars.len();
    let ident_char = |c: char| is_ident_char(c) && (dialect.dollar_in_identifiers || c != '$');
    let mut out = Vec::new();
    let mut error = None;
    let mut executable_comment = false;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let dash_comment = c == '-'
            && chars.get(i + 1) == Some(&'-')
            && (!dialect.dash_comments_need_space
                || chars
                    .get(i + 2)
                    .is_none_or(|c| c.is_whitespace() || c.is_control()));
        if dash_comment || (c == '#' && dialect.hash_comments) {
            while i < n
                && chars[i] != '\n'
                && !(dialect.carriage_return_ends_comments && chars[i] == '\r')
            {
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            if chars.get(i + 2) == Some(&'!')
                || (chars.get(i + 2) == Some(&'M') && chars.get(i + 3) == Some(&'!'))
            {
                executable_comment = true;
            }
            match block_comment_end(chars, i, dialect.nested_comments) {
                Some(end) => i = end,
                None => {
                    error.get_or_insert("Unterminated block comment.");
                    i = n;
                }
            }
            continue;
        }
        // String literals, with an `E` / `N` / `B` / `X` prefix read as part of them.
        let prefixed = dialect.string_prefixes
            && matches!(c, 'e' | 'E' | 'n' | 'N' | 'b' | 'B' | 'x' | 'X')
            && chars.get(i + 1) == Some(&'\'')
            && (i == 0 || !ident_char(chars[i - 1]));
        if c == '\'' || prefixed {
            let escapes = dialect.backslash_escapes
                || (dialect.escape_strings && prefixed && matches!(c, 'e' | 'E'));
            i += if prefixed { 2 } else { 1 };
            let (text, end) = quoted(chars, i, '\'', escapes);
            if end.is_none() {
                error.get_or_insert("Unterminated string literal.");
            }
            out.push(Token::Str(text));
            i = end.unwrap_or(n);
            continue;
        }
        // Dollar-quoted strings: `$$...$$` or `$tag$...$tag$`.
        if dialect.dollar_quotes
            && let Some(len) = dollar_tag_len(chars, i)
            && (!dialect.dollar_in_identifiers || i == 0 || !ident_char(chars[i - 1]))
        {
            let tag = &chars[i..i + len];
            let start = i + len;
            let end = (start..=n.saturating_sub(len))
                .find(|&j| j + len <= n && &chars[j..j + len] == tag);
            match end {
                Some(end) => {
                    out.push(Token::Str(chars[start..end].iter().collect()));
                    i = end + len;
                }
                None => {
                    error.get_or_insert("Unterminated dollar-quoted string.");
                    out.push(Token::Str(chars[start.min(n)..].iter().collect()));
                    i = n;
                }
            }
            continue;
        }
        // Quoted identifiers: "..." (`""` doubles a quote), `...` (`` `` `` doubles one), [...].
        if c == '"' || c == '`' || c == '[' {
            let (name, end) = if c == '[' {
                match chars[i + 1..].iter().position(|&ch| ch == ']') {
                    Some(p) => (chars[i + 1..i + 1 + p].iter().collect(), Some(i + 2 + p)),
                    None => (chars[i + 1..].iter().collect(), None),
                }
            } else {
                quoted(chars, i + 1, c, c == '"' && dialect.backslash_escapes)
            };
            if end.is_none() {
                error.get_or_insert("Unterminated quoted identifier.");
            }
            out.push(Token::Ident { name, quoted: true });
            i = end.unwrap_or(n);
            continue;
        }
        if is_ident_start(c) {
            let start = i;
            i += 1;
            while i < n && ident_char(chars[i]) {
                i += 1;
            }
            out.push(Token::Ident {
                name: chars[start..i].iter().collect(),
                quoted: false,
            });
            continue;
        }
        out.push(Token::Punct(c));
        i += 1;
    }
    Reading {
        tokens: out,
        error,
        executable_comment,
    }
}

/// The contents of a literal opened before `chars[start]` and closed by `close` (doubled, it
/// stands for itself; with `escapes`, `\` escapes the next character), and the index after it
/// (`None`: unterminated).
fn quoted(chars: &[char], start: usize, close: char, escapes: bool) -> (String, Option<usize>) {
    let n = chars.len();
    let mut text = String::new();
    let mut i = start;
    while i < n {
        let ch = chars[i];
        if escapes && ch == '\\' {
            if i + 1 < n {
                text.push(chars[i + 1]);
                i += 2;
                continue;
            }
            return (text, None);
        }
        if ch == close {
            if chars.get(i + 1) == Some(&close) {
                text.push(close);
                i += 2;
                continue;
            }
            return (text, Some(i + 1));
        }
        text.push(ch);
        i += 1;
    }
    (text, None)
}

fn identifier_chains(toks: &[Token], normalize: impl Fn(&str) -> String) -> Vec<Vec<String>> {
    let mut chains: Vec<Vec<String>> = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        if let Token::Ident { name, .. } = &toks[i] {
            let mut chain = vec![normalize(name)];
            let mut j = i + 1;
            while j + 1 < toks.len()
                && toks[j] == Token::Punct('.')
                && let Token::Ident { name, .. } = &toks[j + 1]
            {
                chain.push(normalize(name));
                j += 2;
            }
            chains.push(chain);
            i = j;
            continue;
        }
        i += 1;
    }
    chains
}

/// The ASCII words (`[A-Za-z_][A-Za-z0-9_]*`) of `text`, as `(start, end)` byte offsets.
fn ascii_word_spans(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            out.push((start, i));
        } else {
            i += 1;
        }
    }
    out
}

/// The ASCII words of a reading's unquoted identifiers, as v4's statement check reads them.
fn unquoted_words(tokens: &[Token]) -> impl Iterator<Item = &str> {
    tokens.iter().flat_map(|t| match t {
        Token::Ident {
            name,
            quoted: false,
        } => ascii_word_spans(name)
            .into_iter()
            .map(|(start, end)| &name[start..end])
            .collect(),
        _ => Vec::new(),
    })
}

/// Whether `text` contains `word` with no identifier character on either side.
fn contains_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let mut from = 0;
    while let Some(pos) = text[from..].find(word) {
        let start = from + pos;
        let end = start + word.len();
        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        if !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char) {
            return true;
        }
        from = start + word.chars().next().map_or(1, char::len_utf8);
    }
    false
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

/// One identifier part of a bare table name: bare, `"..."`, `` `...` `` or `[...]`.
fn identifier_part(s: &str) -> Option<&str> {
    let first = s.chars().next()?;
    let end = match first {
        '"' | '`' => 1 + s[1..].find(first)? + 1,
        '[' => 1 + s[1..].find(']')? + 1,
        c if c.is_ascii_alphabetic() || c == '_' => s
            .char_indices()
            .find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '$'))
            .map(|(i, _)| i)
            .unwrap_or(s.len()),
        _ => return None,
    };
    // Quoted parts need at least one character inside.
    if matches!(first, '"' | '`' | '[') && end <= 2 {
        return None;
    }
    Some(&s[end..])
}

/// v4's `_BARE_IDENTIFIER_PATTERN`: a table name, optionally schema- and database-qualified.
fn is_bare_identifier(sql: &str) -> bool {
    let mut rest = sql.trim().trim_end_matches(';').trim();
    for part in 0..3 {
        let Some(after) = identifier_part(rest) else {
            return false;
        };
        if after.is_empty() {
            return true;
        }
        if part == 2 {
            return false;
        }
        let Some(next) = after.strip_prefix('.') else {
            return false;
        };
        rest = next;
    }
    false
}

/// v4's read-only statement check, applied to every reading.
fn check_read_only(sql: &str, readings: &Readings) -> Result<(), SqlError> {
    if sql.trim().is_empty() {
        return Err(SqlError::invalid_query(
            "Refusing to execute an empty query.",
        ));
    }
    if is_bare_identifier(sql) {
        return Ok(());
    }
    if let Some(reason) = readings.lexing_error(true) {
        return Err(SqlError::invalid_query(format!(
            "Refusing to execute the query: {reason}"
        )));
    }
    readings.tokens().try_for_each(check_statement)
}

/// One reading of a query must be one read-only statement.
fn check_statement(tokens: &[Token]) -> Result<(), SqlError> {
    let statements: Vec<&[Token]> = tokens
        .split(|t| *t == Token::Punct(';'))
        .filter(|stmt| !stmt.is_empty())
        .collect();
    if statements.len() > 1 {
        return Err(SqlError::invalid_query(
            "Refusing to execute multiple statements. \
             Submit one read-only query at a time.",
        ));
    }
    let Some(statement) = statements.first() else {
        return Err(SqlError::invalid_query(
            "Refusing to execute an empty query.",
        ));
    };
    let words: Vec<String> = unquoted_words(statement)
        .map(str::to_ascii_lowercase)
        .collect();
    let Some(first) = words.first() else {
        return Err(SqlError::invalid_query(
            "Could not parse a statement from the query.",
        ));
    };
    if !READ_ONLY_STARTS.contains(&first.as_str()) {
        return Err(SqlError::invalid_query(format!(
            "Refusing to execute a '{}' statement. `read_sql` only accepts read-only queries ({}).",
            first.to_uppercase(),
            READ_ONLY_STARTS.join(", ")
        )));
    }
    for word in &words {
        if MUTATING_KEYWORDS.contains(&word.as_str()) || MUTATING_FUNCTIONS.contains(&word.as_str())
        {
            return Err(SqlError::invalid_query(format!(
                "Refusing to execute a query containing '{}'. `read_sql` only accepts read-only \
                 queries; use a dedicated write tool or the `execute_action` tool instead.",
                word.to_uppercase()
            )));
        }
        if MUTATING_WORD_PREFIXES.iter().any(|p| word.starts_with(p)) {
            return Err(SqlError::invalid_query(format!(
                "Refusing to execute a query containing '{word}': procedural packages are not \
                 reachable from `read_sql`."
            )));
        }
    }
    Ok(())
}

/// v4's protected-table check on the unquoted words of every reading.
fn check_protected_tables(readings: &Readings) -> Result<(), SqlError> {
    for tokens in readings.tokens() {
        if let Some(word) = unquoted_words(tokens)
            .find(|w| PROTECTED_TABLES.contains(&w.to_ascii_lowercase().as_str()))
        {
            return Err(SqlError::forbidden(format!(
                "Refusing to query protected table '{word}'."
            )));
        }
    }
    Ok(())
}

/// Whether `rest`, the text after a name, calls it: `(` after the closing quote of a quoted name,
/// whitespace and comments (block comments nest, as in PostgreSQL and DuckDB).
fn opens_call(rest: &str) -> bool {
    let mut rest = rest.strip_prefix(['"', '`', ']']).unwrap_or(rest);
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, next)| next);
        } else if rest.starts_with("/*") {
            let bytes = rest.as_bytes();
            let (mut depth, mut j, mut end) = (0usize, 0usize, rest.len());
            while j + 1 < bytes.len() {
                match &bytes[j..j + 2] {
                    b"/*" => {
                        depth += 1;
                        j += 2;
                    }
                    b"*/" => {
                        depth -= 1;
                        j += 2;
                        if depth == 0 {
                            end = j;
                            break;
                        }
                    }
                    _ => j += 1,
                }
            }
            rest = &rest[end..];
        } else {
            return rest.starts_with('(');
        }
    }
}

/// The refusal of a name of Meerschaum's internal tables (`mrsm_*`, `_mrsm_*`,
/// `sqlite_dbpage`, the protected instance tables) or of a function that runs query text,
/// phrased for `context`.
fn check_internal_word(raw: &str, called: bool, context: &str) -> Result<(), SqlError> {
    let lower = raw.to_ascii_lowercase();
    if lower.starts_with("mrsm_")
        || lower.starts_with("_mrsm_")
        || lower == "sqlite_dbpage"
        || PROTECTED_TABLES.contains(&lower.as_str())
    {
        return Err(SqlError::forbidden(format!(
            "Refusing SQL that names Meerschaum's internal table '{raw}' {context}."
        )));
    }
    if TEXT_QUERY_FUNCTIONS.contains(&lower.as_str()) && (lower != "query" || called) {
        return Err(SqlError::forbidden(format!(
            "Refusing SQL that calls `{lower}()` {context}: it reads tables it does not name."
        )));
    }
    Ok(())
}

/// Meerschaum's internal tables and the functions that run query text: anywhere in the raw text
/// (comments, strings and quoted names included), then `query(...)` calls under every reading.
fn check_internal_tables(sql: &str, readings: &Readings, context: &str) -> Result<(), SqlError> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("u&\"") || lower.contains("u&'") {
        return Err(SqlError::forbidden(format!(
            "Refusing SQL with Unicode-escaped names (`U&\"...\"`) {context}."
        )));
    }
    for (start, end) in ascii_word_spans(sql) {
        check_internal_word(&sql[start..end], opens_call(&lower[end..]), context)?;
    }
    for tokens in readings.tokens() {
        for (idx, token) in tokens.iter().enumerate() {
            if let Token::Ident { name, .. } = token
                && let Some(&(start, end)) = ascii_word_spans(name).last()
            {
                let called = matches!(tokens.get(idx + 1), Some(Token::Punct('(')));
                check_internal_word(&name[start..end], called, context)?;
            }
        }
    }
    Ok(())
}

/// `MRSM{...}` config references.
fn check_config_reference(sql: &str, phrasing: Phrasing) -> Result<(), SqlError> {
    if sql.contains("MRSM{") {
        return Err(SqlError::forbidden(format!(
            "Refusing {} with an `MRSM{{...}}` config reference: it would read the \
             server's configuration (connector credentials included) when the pipe syncs. Ask an \
             administrator to set it.",
            phrasing.subject
        )));
    }
    Ok(())
}

/// The tables of hidden pipes (identifier chains and string literals) and, while there are any,
/// the catalog, under every reading.
fn check_hidden_tables(
    readings: &Readings,
    hidden: &HiddenTables,
    phrasing: Phrasing,
) -> Result<(), SqlError> {
    for tokens in readings.tokens() {
        let chains = identifier_chains(tokens, |name| hidden.normalize_identifier(name));
        for part in chains.iter().flatten() {
            if hidden.names.iter().any(|h| h == part) {
                return Err(SqlError::forbidden(format!(
                    "Refusing {} that names '{part}': it is the table of a pipe \
                     you cannot see.",
                    phrasing.subject
                )));
            }
        }
        for token in tokens {
            if let Token::Str(raw) = token {
                let literal = raw.to_lowercase();
                if let Some(name) = hidden.names.iter().find(|h| contains_word(&literal, h)) {
                    return Err(SqlError::forbidden(format!(
                        "Refusing {} that names '{name}' in a string: it is the table of \
                         a pipe you cannot see.",
                        phrasing.subject
                    )));
                }
            }
        }
        if hidden.is_empty() {
            continue;
        }
        for chain in &chains {
            let schema = chain.len() > 1 && CATALOG_SCHEMAS.contains(&chain[0].as_str());
            if let Some(name) = chain
                .iter()
                .find(|p| CATALOG_NAMES.contains(&p.as_str()))
                .or(schema.then(|| &chain[0]))
            {
                return Err(SqlError::forbidden(format!(
                    "Refusing {} that reads the database's catalog ('{name}') \
                     while this instance has pipes you cannot see.",
                    phrasing.subject
                )));
            }
        }
    }
    Ok(())
}

/// The source broker's pure-function allowlist and `FROM 'file'` / `JOIN 'file'` refusal, under
/// every reading.
fn check_source_broker(readings: &Readings) -> Result<(), SqlError> {
    for tokens in readings.tokens() {
        for (index, token) in tokens.iter().enumerate() {
            let Token::Ident { name, .. } = token else {
                continue;
            };
            let lower = name.to_ascii_lowercase();
            if matches!(tokens.get(index + 1), Some(Token::Punct('(')))
                && (!SOURCE_BROKER_PURE_FUNCTIONS.contains(&lower.as_str())
                    || matches!(
                        index.checked_sub(1).and_then(|i| tokens.get(i)),
                        Some(Token::Punct('.'))
                    ))
            {
                return Err(SqlError::forbidden(
                    "The source broker does not allow this SQL function or table function.",
                ));
            }
            if matches!(lower.as_str(), "from" | "join")
                && matches!(tokens.get(index + 1), Some(Token::Str(_)))
            {
                return Err(SqlError::forbidden(
                    "The source broker does not read file paths as SQL tables.",
                ));
            }
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn hidden() -> HiddenTables {
        HiddenTables::new(["secret_counts", "Team-B Weather"])
    }

    fn definition(sql: &str, hidden: &HiddenTables) -> Result<(), String> {
        check_sql(sql, &SqlPolicy::PipeDefinition { hidden }).map_err(SqlError::into_message)
    }

    fn api_query(sql: &str) -> Result<(), SqlError> {
        check_sql(sql, &SqlPolicy::ReadOnlyApiQuery { hidden: None })
    }

    /// The identifier chains and string literals v4's table-name scanner reads.
    fn v4_tokens(sql: &str) -> Vec<Token> {
        lex(&sql.chars().collect::<Vec<_>>(), &DIALECTS[1]).tokens
    }

    #[test]
    fn identifiers_are_read_quoted_qualified_and_without_comments() {
        let chains = identifier_chains(
            &v4_tokens(
                "SELECT a.x, \"B\".\"Y z\" FROM [dbo].[t1] -- secret\n JOIN `db`.`t2` /* c /* d */ e */",
            ),
            str::to_lowercase,
        );
        assert!(chains.contains(&vec!["a".to_string(), "x".to_string()]));
        assert!(chains.contains(&vec!["b".to_string(), "y z".to_string()]));
        assert!(chains.contains(&vec!["dbo".to_string(), "t1".to_string()]));
        assert!(chains.contains(&vec!["db".to_string(), "t2".to_string()]));
        assert!(!chains.iter().flatten().any(|p| p == "secret" || p == "e"));
        let literals: Vec<Token> = v4_tokens("SELECT 'it''s', E'a\\'b', $x$ $$ $x$")
            .into_iter()
            .filter(|t| matches!(t, Token::Str(_)))
            .collect();
        assert_eq!(
            literals,
            ["it's", "a'b", " $$ "].map(|s| Token::Str(s.into()))
        );
    }

    #[test]
    fn the_tokenizer_reads_doubled_quotes_escapes_and_dollar_quotes() {
        let stdio = SqlPolicy::ReadOnlyStdioQuery;
        // A block comment is accepted whether or not the database nests it.
        assert!(check_sql("SELECT /* outer /* inner */ still outer */ 1", &stdio).is_ok());
        assert!(check_sql("SELECT /* outer /* inner */ 1", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT 1 /* open", &stdio).unwrap_err(),
            SqlError::invalid_query("Refusing to execute the query: Unterminated block comment.")
        );
        // Executable MySQL and MariaDB comments are rejected.
        assert_eq!(
            check_sql("SELECT /*! 1 */ 2", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute the query: Refusing an executable comment (`/*! ... */`), \
                 which MySQL runs as SQL."
            )
        );
        assert!(check_sql("SELECT /*M! 1 */ 2", &stdio).is_err());
        // Doubled quotes inside quoted identifiers stay one identifier (no false semicolon split).
        assert!(check_sql("SELECT \"a\"\"b; DROP TABLE t\" FROM x", &stdio).is_ok());
        assert!(check_sql("SELECT `a``b; DELETE FROM t` FROM x", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT \"unterminated\"\"ident FROM x", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute the query: Unterminated quoted identifier."
            )
        );
        assert!(check_sql("SELECT 'a''b; DROP TABLE t'", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT E'trailing\\", &stdio).unwrap_err(),
            SqlError::invalid_query("Refusing to execute the query: Unterminated string literal.")
        );
        // Dollar-quoted strings.
        assert!(check_sql("SELECT $$x$$", &stdio).is_ok());
        assert!(check_sql("SELECT $tag$x y$tag$", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT $tag$x y", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute the query: Unterminated dollar-quoted string."
            )
        );
    }

    #[test]
    fn read_only_policy_enforces_exact_v4_parity_wording() {
        let stdio = SqlPolicy::ReadOnlyStdioQuery;
        for ok in [
            "SELECT 1",
            "pipes",
            "\"mrsm\".\"pipes\";",
            "[dbo].[pipes]",
            "SELECT 'a;b' AS x;",
            "WITH a AS (SELECT 1) SELECT * FROM a",
            "SELECT 1 -- ; DROP TABLE t",
            "SELECT /* x */ 1",
        ] {
            assert!(check_sql(ok, &stdio).is_ok(), "{ok}");
        }
        assert_eq!(
            check_sql("   ", &stdio).unwrap_err(),
            SqlError::invalid_query("Refusing to execute an empty query.")
        );
        assert_eq!(
            check_sql("DELETE FROM t", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute a 'DELETE' statement. `read_sql` only accepts read-only \
                 queries (desc, describe, explain, select, show, table, values, with)."
            )
        );
        assert_eq!(
            check_sql("SELECT 1; SELECT 2", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute multiple statements. Submit one read-only query at a time."
            )
        );
        assert_eq!(
            check_sql("SELECT * INTO t2 FROM t", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute a query containing 'INTO'. `read_sql` only accepts read-only \
                 queries; use a dedicated write tool or the `execute_action` tool instead."
            )
        );
        assert_eq!(
            check_sql("SELECT pg_read_file('/etc/passwd')", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute a query containing 'PG_READ_FILE'. `read_sql` only accepts \
                 read-only queries; use a dedicated write tool or the `execute_action` tool instead."
            )
        );
        assert_eq!(
            check_sql("SELECT xp_cmdshell", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute a query containing 'xp_cmdshell': procedural packages are \
                 not reachable from `read_sql`."
            )
        );
        assert_eq!(
            check_sql("SELECT * FROM mrsm_users", &stdio).unwrap_err(),
            SqlError::forbidden("Refusing to query protected table 'mrsm_users'.")
        );
        // Bare quoted mrsm_users is allowed under stdio (v4 local trust), refused under API.
        assert!(check_sql("SELECT * FROM \"mrsm_users\"", &stdio).is_ok());
        assert_eq!(
            api_query("SELECT * FROM \"mrsm_users\"").unwrap_err(),
            SqlError::forbidden(
                "Refusing SQL that names Meerschaum's internal table 'mrsm_users' for an API request."
            )
        );
        // Unquoted mrsm_users keeps v4's message for API callers too (`PARITY.md`).
        assert_eq!(
            api_query("SELECT * FROM mrsm_users").unwrap_err(),
            SqlError::forbidden("Refusing to query protected table 'mrsm_users'.")
        );
    }

    #[test]
    fn internal_tables_are_refused_however_a_query_reaches_them() {
        let none = HiddenTables::default();
        let exec = |sql: &str| check_sql(sql, &SqlPolicy::RawExec { hidden: &none });
        for query in [
            "SELECT * FROM \"mrsm_users\"",
            "SELECT query_to_xml('select * from ' || 'mrsm' || '_users', true, true, '')",
            // Between a function's name and its arguments: comments (PostgreSQL nests them) and
            // the closing quote of a quoted name.
            "SELECT query_to_xml/**/(E'select * from mrsm\\x5fusers', true, true, '')",
            "SELECT query_to_xml /* a /* nested */ b */ ('select 1', true, true, '')",
            "SELECT \"query_to_xml\"(E'select * from mrsm\\x5fusers', true, true, '')",
            "SELECT * FROM query -- c\n ('select * from ' || 'mrsm' || '_users')",
            "SELECT * FROM query # c\n ('select 1')",
            "SELECT * FROM \"query\" ('select 1')",
            "SELECT * FROM \"query\" /* a /* b */ c */ ('select 1')",
            // Other functions that run the query text they are given, or read files.
            "SELECT * FROM ts_stat(E'select to_tsvector(password_hash) from mrsm\\x5fusers')",
            "SELECT ts_rewrite('a'::tsquery, 'select ' || 'x')",
            "SELECT * FROM dblink('dbname=x', 'select 1') AS t(a int)",
            "SELECT * FROM crosstab('select 1')",
            "SELECT DBMS_XMLGEN.GETXML('SELECT * FROM ' || 'MRSM' || '_USERS') FROM DUAL",
            "SELECT XMLQUERY('fn:collection(\"oradb:/S/T\")' RETURNING CONTENT) FROM DUAL",
            "SELECT pg_read_binary_file('base/1/2')",
            "SELECT * FROM U&\"mrsm\\005fusers\"",
            "SELECT * FROM sqlite_dbpage",
            "SELECT 'mrsm_' || 'users'",
            // In a comment, however a database reads comments.
            "SELECT 1 -- mrsm_users",
            "SELECT 1 /* mrsm_users */",
        ] {
            assert!(exec(query).is_err(), "{query}");
        }
        // Original case is preserved in the internal-table refusal.
        assert_eq!(
            exec("SELECT * FROM \"MRSM_USERS\"").unwrap_err().message,
            "Refusing SQL that names Meerschaum's internal table 'MRSM_USERS' for an API request."
        );
        for query in [
            "SELECT 1",
            "SELECT 'users' AS name, query FROM t",
            "SELECT query, count(*) FROM logs GROUP BY query",
            "SELECT \"query\" FROM t",
            "SELECT count(query) FROM t WHERE query = 'x'",
        ] {
            assert!(exec(query).is_ok(), "{query}");
        }
    }

    #[test]
    fn hidden_tables_are_refused_however_they_are_spelled() {
        let hidden = hidden();
        for sql in [
            "SELECT * FROM secret_counts",
            "SELECT * FROM SECRET_COUNTS",
            "SELECT * FROM \"secret_counts\"",
            "SELECT * FROM public.\"Secret_Counts\"",
            "SELECT * FROM [dbo].[secret_counts]",
            "SELECT * FROM `secret_counts`",
            "SELECT * FROM a, /* x */ secret_counts",
            "SELECT * FROM \"Team-B Weather\"",
            "SELECT * FROM (SELECT * FROM\nsecret_counts) s",
            "SELECT * FROM OPENQUERY(x, 'SELECT * FROM secret_counts')",
            "WITH s AS (SELECT 1) SELECT * FROM s JOIN \"secret_counts\" USING (id)",
            "SELECT /*! * FROM secret_counts */ 1",
            // Hidden behind what only some databases read as a comment.
            "SELECT 1 # 2 FROM secret_counts",
            "SELECT * FROM #t JOIN secret_counts ON 1=1",
            "SELECT 1--1 FROM secret_counts",
            "SELECT /* /* */ * FROM secret_counts -- */",
            "SELECT 1 -- x\r FROM secret_counts",
        ] {
            assert!(definition(sql, &hidden).is_err(), "{sql}");
        }
        for sql in [
            "SELECT * FROM my_counts",
            "SELECT secret_counts_2 FROM t",
            "SELECT 'secret_countsx' AS a FROM t",
            "SELECT * FROM t -- secret_counts\n",
            "SELECT * FROM t /* \"secret_counts\" */",
        ] {
            assert!(definition(sql, &hidden).is_ok(), "{sql}");
        }
    }

    #[test]
    fn phrasing_is_parameterized_by_policy_profile() {
        let hidden = hidden();
        assert_eq!(
            definition("SELECT * FROM secret_counts", &hidden).unwrap_err(),
            "Refusing a SQL definition that names 'secret_counts': it is the table of a pipe you cannot see."
        );
        let api_err = check_sql(
            "SELECT * FROM secret_counts",
            &SqlPolicy::ReadOnlyApiQuery {
                hidden: Some(&hidden),
            },
        )
        .unwrap_err();
        assert_eq!(
            api_err.message,
            "Refusing a query that names 'secret_counts': it is the table of a pipe you cannot see."
        );
        assert_eq!(
            definition("SELECT * FROM mrsm_users", &hidden).unwrap_err(),
            "Refusing SQL that names Meerschaum's internal table 'mrsm_users' in a pipe's definition."
        );
        let exec_ok = check_sql(
            "INSERT INTO my_table (id) VALUES (1)",
            &SqlPolicy::RawExec { hidden: &hidden },
        );
        assert!(exec_ok.is_ok());
        let exec_err = check_sql(
            "UPDATE secret_counts SET x = 1",
            &SqlPolicy::RawExec { hidden: &hidden },
        )
        .unwrap_err();
        assert_eq!(
            exec_err.message,
            "Refusing a SQL statement that names 'secret_counts': it is the table of a pipe you cannot see."
        );
    }

    #[test]
    fn source_broker_policy_enforces_pure_functions_and_no_file_tables() {
        let hidden = HiddenTables::new(["sibling_private"]);
        let policy = SqlPolicy::SourceBrokerQuery { hidden: &hidden };
        for query in [
            "SELECT * FROM mrsm_users",
            "SELECT * FROM sibling_private",
            "SELECT readfile('/secret')",
            "SELECT * FROM read_csv('/secret')",
            "SELECT * FROM '/secret.csv'",
            "SELECT * FROM safe JOIN '/secret.parquet' ON 1=1",
            "SELECT dangerous_user_function()",
            "SELECT public.count(*) FROM safe",
            "DELETE FROM safe",
            "SELECT 1 # x\n, read_csv('/secret')",
        ] {
            assert!(check_sql(query, &policy).is_err(), "{query}");
        }
        check_sql(
            "SELECT count(*), max(id) FROM safe WHERE id IN (1, 2)",
            &policy,
        )
        .unwrap();
        check_sql(
            "WITH definition AS (SELECT id, created FROM safe) SELECT * FROM definition \
             WHERE created >= CAST('2026-01-02 00:00:00+00:00' AS TIMESTAMPTZ(9)) \
             AND created < CAST('2026-01-04 00:00:00+00:00' AS TIMESTAMPTZ(9)) \
             AND id IN ('2', '3')",
            &policy,
        )
        .unwrap();
        check_sql("SELECT \"id\" FROM \"Safe Table\"", &policy).unwrap();
    }

    #[test]
    fn internal_tables_config_references_and_the_catalog_are_refused() {
        let none = HiddenTables::default();
        for sql in [
            "SELECT * FROM mrsm_users",
            "SELECT * FROM \"mrsm_pipes\"",
            "SELECT * FROM _mrsm_internal.mrsm_engine_runs",
            "SELECT query_to_xml('select 1', true, true, '')",
            "SELECT 'MRSM{meerschaum:connectors:sql:main:password}' AS p",
        ] {
            assert!(definition(sql, &none).is_err(), "{sql}");
        }
        // The catalog is refused only while the caller has hidden pipes.
        let catalog = [
            "SELECT table_name FROM information_schema.tables",
            "SELECT relname FROM pg_class",
            "SELECT name FROM sqlite_master",
            "SELECT name FROM sys.tables",
            "SELECT * FROM \"pg_catalog\".\"pg_tables\"",
            "SELECT 1 # 2 FROM pg_class",
        ];
        for sql in catalog {
            assert!(definition(sql, &none).is_ok(), "{sql}");
            assert!(definition(sql, &hidden()).is_err(), "{sql}");
        }
        // A column named like a catalog schema is not a catalog read.
        assert!(definition("SELECT sys FROM t", &hidden()).is_ok());
    }

    /// Each row is one SQL text and whether it is accepted as an administrator's `read_sql`
    /// query (MCP and native plugins) and as a pipe definition or `exec_sql` statement. Every
    /// row v4's guard (5.1.1) refused stays refused; rows marked "beyond v4" were accepted by it
    /// but some database reads them as several statements, a write, or an internal-table read.
    #[test]
    fn every_reading_of_a_query_must_pass() {
        let cases: &[(&str, bool, bool)] = &[
            // `#` is an operator on PostgreSQL and a temporary table on SQL Server.
            ("SELECT 1 # 2 FROM mrsm_users", false, false),
            (
                "SELECT 1 # 2, query_to_xml('select * from mrsm_users', true, true, '')",
                false,
                false,
            ),
            ("SELECT * FROM #t JOIN mrsm_users ON 1=1", false, false),
            // MySQL needs whitespace after `--`.
            ("SELECT 1--1 FROM mrsm_users", false, false),
            // Beyond v4: `--1` is two minus signs on MySQL.
            ("SELECT 1 --1 INTO OUTFILE '/tmp/x'", false, true),
            // MySQL, SQLite and Oracle close a block comment at the first `*/`.
            ("SELECT /* /* */ * FROM mrsm_users -- */", false, false),
            ("SELECT /* /* */ 1; DELETE FROM t -- */", false, true),
            ("SELECT /* /* */ 1 FROM mrsm_tokens", false, false),
            // `E` is not a string prefix on SQLite or SQL Server.
            ("SELECT E'\\'; DELETE FROM t; --'", false, true),
            // Beyond v4: PostgreSQL ends a line comment at a carriage return.
            ("SELECT 1 -- x\r; DELETE FROM t", false, true),
            // Beyond v4: MySQL reads `\` escapes, so these strings close before the `;`.
            ("SELECT 'a\\''; DELETE FROM t; -- '", false, true),
            ("SELECT \"a\\\"\"; DELETE FROM t; -- \"", false, true),
            // Beyond v4: only PostgreSQL and DuckDB read dollar quotes.
            (
                "SELECT 1 AS $$x; DELETE FROM t; SELECT 1 AS $$",
                false,
                true,
            ),
            // Executable comments (definitions: beyond v4); `/*M!` is MariaDB's.
            ("SELECT /*! 1 */ 2", false, false),
            ("SELECT /*M! 1 */ 2", false, false),
            ("SELECT 1; SELECT 2", false, true),
            ("SELECT 1 /* open", false, true),
            // PostgreSQL, TimescaleDB, DuckDB.
            ("SELECT 5 # 3", true, true),
            ("SELECT E'it\\'s' AS x", false, true),
            ("SELECT 'C:\\' AS path", true, true),
            ("SELECT $$x y$$ AS s", true, true),
            ("SELECT /* a /* b */ c */ 1", true, true),
            ("SELECT x::text FROM t WHERE y ~ '^a' LIMIT 5", true, true),
            ("SELECT * FROM \"public\".\"my table\"", true, true),
            // MySQL, MariaDB.
            ("SELECT 1 # note", true, true),
            ("SELECT 1 -- note", true, true),
            ("SELECT 1 --note", true, true),
            ("SELECT `a` FROM `db`.`t` WHERE b = 'x''y'", true, true),
            ("SELECT /* a /* b */ 1", true, true),
            ("SELECT 'it\\'s' AS x", false, true),
            // SQLite, Oracle.
            ("SELECT * FROM dual", true, true),
            ("SELECT strftime('%s', ts) FROM t", true, true),
            ("SELECT a || '-' || b FROM t", true, true),
            // SQL Server.
            ("SELECT * FROM #temp", true, true),
            ("SELECT TOP 5 [col] FROM [dbo].[t]", true, true),
            ("SELECT 1 -- note: update later\n", true, true),
            // Bare table names.
            ("pipes", true, true),
            ("\"my schema\".\"my table\";", true, true),
        ];
        let none = HiddenTables::default();
        for (sql, read, define) in cases {
            assert_eq!(api_query(sql).is_ok(), *read, "read_sql: {sql:?}");
            assert_eq!(
                check_sql(sql, &SqlPolicy::ReadOnlyHostQuery { hidden: &none }).is_ok(),
                *read,
                "native read_sql: {sql:?}"
            );
            assert_eq!(
                definition(sql, &none).is_ok(),
                *define,
                "definition: {sql:?}"
            );
            assert_eq!(
                check_sql(sql, &SqlPolicy::RawExec { hidden: &none }).is_ok(),
                *define,
                "exec_sql: {sql:?}"
            );
        }
    }

    #[test]
    fn definition_changes() {
        let map = |json: &str| Value::from_json_str(json).unwrap().to_map();
        let old = map(r#"{"fetch": {"definition": "SELECT 1", "backtrack_minutes": 5}}"#);
        assert!(!changes_definition("sql:a", Some(&old), &old));
        assert!(changes_definition(
            "sql:a",
            Some(&old),
            &map(r#"{"fetch": {"definition": "SELECT 2", "backtrack_minutes": 5}}"#)
        ));
        // Any `fetch` parameter of a `sql:` connector; not of other connectors.
        let backtrack = map(r#"{"fetch": {"definition": "SELECT 1", "backtrack_minutes": 9}}"#);
        assert!(changes_definition("sql:a", Some(&old), &backtrack));
        assert!(!changes_definition("plugin:a", Some(&old), &backtrack));
        assert!(changes_definition(
            "plugin:a",
            None,
            &map(r#"{"sql": "SELECT 1"}"#)
        ));
        assert!(changes_definition("api:a", None, &map(r#"{"query": "x"}"#)));
        // Removing a definition is not setting one.
        assert!(!changes_definition("sql:a", Some(&old), &map("{}")));
        assert_eq!(
            definitions(&map(
                r#"{"fetch": {"definition": "a"}, "sql": "b", "query": ""}"#
            )),
            vec!["a", "b"]
        );
    }

    #[test]
    fn targets() {
        let hidden = hidden();
        assert!(check_target("my_table", &hidden).is_ok());
        assert!(check_target("secret_counts", &hidden).is_err());
        assert!(check_target("public.secret_counts", &hidden).is_err());
        assert!(check_target("mrsm_users", &hidden).is_err());
        assert!(check_target("\"mrsm_tokens\"", &hidden).is_err());
        assert!(check_target("", &hidden).is_ok());
    }
}
