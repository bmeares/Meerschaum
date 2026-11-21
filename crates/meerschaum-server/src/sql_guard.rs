//! Unified SQL tokenizer and query policy guard (access model AC8, MCP `read_sql`,
//! canonical SQL source broker, native plugin SQL host calls).
//!
//! A pipe's SQL runs with the source connector's database credentials, not the caller's, so
//! visibility alone cannot keep a definition or query from reading another team's table or
//! Meerschaum's internal tables. This module is the single authoritative SQL scanner and
//! policy checker for:
//!
//! - **Pipe SQL definitions** ([`SqlPolicy::PipeDefinition`]): no `MRSM{...}` config symlinks,
//!   no Meerschaum internal tables (`mrsm_*`, `_mrsm_*`, `sqlite_dbpage`, protected instance
//!   tables) or text-query functions ([`TEXT_QUERY_FUNCTIONS`]), no tables of pipes the caller
//!   cannot see (bare, quoted, schema-qualified, or in string literals), and no catalog reads
//!   while hidden pipes exist.
//! - **Read-only API queries** ([`SqlPolicy::ReadOnlyApiQuery`]): single read-only statement
//!   (or bare table name), v4 protected-table refusal, RC15-H-002 internal-table / text-query
//!   function / `U&"..."` refusal, plus hidden-table and catalog checks for non-administrators.
//! - **Read-only stdio queries** ([`SqlPolicy::ReadOnlyStdioQuery`]): v4 parity read-only check
//!   and protected-table refusal for local-trust stdio callers.
//! - **Canonical SQL source-broker queries** ([`SqlPolicy::SourceBrokerQuery`]): read-only,
//!   no internal/hidden tables, only unqualified pure SQL functions
//!   ([`SOURCE_BROKER_PURE_FUNCTIONS`]), and no `FROM 'file'` / `JOIN 'file'` file readers.
//! - **Raw API SQL statements** ([`SqlPolicy::RawExec`]): native plugin `exec_sql` calls for
//!   API callers (internal tables, hidden tables, catalog, and `MRSM{...}` refused without
//!   requiring a read-only statement).
//!
//! All policies share one internal strict SQL tokenizer supporting line comments (`--`, `#`),
//! nested block comments (`/* a /* b */ c */`), `E'...'` backslash escapes, doubled quotes
//! inside quoted identifiers (`"a""b"`), and dollar-quoted strings (`$$...$$`, `$tag$...$tag$`),
//! while rejecting executable comments (`/*! ... */`) and unterminated comments/literals.
//!
//! This is best-effort keyword/token scanning, not a full SQL parser: true isolation between
//! teams needs separate connectors (database users) per team, and untrusted read-only callers
//! should use a read-only database role.

use std::fmt;

use meerschaum_core::{Map, Value};

use crate::mcp::registry::builtin;

/// The parameter keys of a pipe's SQL definition, in the order `get_pipe_query` reads them
/// (`fetch.definition` first).
pub const DEFINITION_KEYS: &[&[&str]] = &[
    &["fetch", "definition"],
    &["definition"],
    &["query"],
    &["sql"],
];

/// The target-name parameters of a pipe.
pub const TARGET_KEYS: &[&str] = &["target", "target_name", "target_table", "target_table_name"];

/// Catalog schemas: a qualified name starting with one of these reads the catalog.
pub const CATALOG_SCHEMAS: &[&str] = &[
    "information_schema",
    "pg_catalog",
    "sys",
    "mysql",
    "performance_schema",
    "sqlite_temp_schema",
];

/// Catalog tables, views and functions (any qualification).
pub const CATALOG_NAMES: &[&str] = &[
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
pub const TEXT_QUERY_FUNCTIONS: &[&str] = &[
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

/// Ordinary pure SQL functions, syntax keywords and parameterized type names permitted by
/// [`SqlPolicy::SourceBrokerQuery`]. Database file/network readers and user-defined functions
/// can escape table-name guards, so the source broker deliberately allows only this small
/// unqualified set.
pub const SOURCE_BROKER_PURE_FUNCTIONS: &[&str] = &[
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
pub enum Token {
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
    /// An invalid-query refusal.
    pub fn invalid_query(message: impl Into<String>) -> Self {
        Self {
            kind: SqlErrorKind::InvalidQuery,
            message: message.into(),
        }
    }

    /// A permission/policy refusal.
    pub fn forbidden(message: impl Into<String>) -> Self {
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

impl From<SqlError> for String {
    fn from(err: SqlError) -> Self {
        err.message
    }
}

/// Which SQL policy profile to enforce in [`check_sql`].
#[derive(Debug, Clone, Copy)]
pub enum SqlPolicy<'a> {
    /// A pipe's SQL definition (raw or resolved) authored by a non-administrator.
    PipeDefinition {
        /// Target tables of pipes the caller cannot see.
        hidden: &'a HiddenTables,
    },
    /// A read-only SQL query from an HTTP MCP caller (`read_sql` over HTTP). `hidden` is `None`
    /// for administrators and `Some(...)` for non-administrators; preserves v4 MCP protected-table
    /// refusal wording (`PARITY.md` RC15-H-002).
    ReadOnlyApiQuery {
        /// Hidden tables for non-administrators (`None` skips hidden-table / catalog / `MRSM{}`
        /// checks while still enforcing read-only, protected tables, and internal tables).
        hidden: Option<&'a HiddenTables>,
    },
    /// A read-only SQL query from a native plugin host call (`read_sql` / `read_sql_with` /
    /// `tx_read_sql`) for an API caller. Refuses internal tables directly with internal-table
    /// phrasing rather than v4 MCP's protected-table wording.
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

/// Internal result of tokenizing SQL with metadata about comments and whitespace.
struct Lexed {
    tokens: Vec<Token>,
    has_comments: bool,
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

/// Single authoritative SQL tokenizer.
///
/// Handles `--` and `#` line comments, nested `/* ... */` block comments (refusing MySQL's
/// `/*! ... */` executable comments when `strict`), standard `'...'` strings with `''` doubled
/// quotes, prefixed `E'...'` strings with `\` escapes (plus `N'...'`/`B'...'`/`X'...'`),
/// dollar-quoted strings (`$$...$$`, `$tag$...$tag$`), and quoted identifiers (`"..."` with
/// `""` doubled quotes, `` `...` `` with `` `` `` doubled quotes, and `[...]`).
fn lex(sql: &str, strict: bool) -> Result<Lexed, &'static str> {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut out = Vec::new();
    let mut has_comments = false;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // Line comments (`--`, `#`).
        if c == '#' || (c == '-' && chars.get(i + 1) == Some(&'-')) {
            has_comments = true;
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // Block comments (nested, as PostgreSQL and DuckDB read them).
        // `/*!` is executed by MySQL, so refuse it in strict mode.
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            if strict && chars.get(i + 2) == Some(&'!') {
                return Err(
                    "Refusing an executable comment (`/*! ... */`), which MySQL runs as SQL.",
                );
            }
            has_comments = true;
            let mut depth = 0usize;
            while i < n {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth = depth.saturating_sub(1);
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if depth > 0 && strict {
                return Err("Unterminated block comment.");
            }
            continue;
        }
        // String literals, with an `E` / `N` / `B` / `X` prefix read as part of them.
        let prefixed = matches!(c, 'e' | 'E' | 'n' | 'N' | 'b' | 'B' | 'x' | 'X')
            && chars.get(i + 1) == Some(&'\'')
            && (i == 0 || !is_ident_char(chars[i - 1]));
        if c == '\'' || prefixed {
            let escapes = prefixed && matches!(c, 'e' | 'E');
            i += if prefixed { 2 } else { 1 };
            let mut text = String::new();
            let mut closed = false;
            while i < n {
                let ch = chars[i];
                if escapes && ch == '\\' {
                    if i + 1 < n {
                        text.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                if ch == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        text.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    closed = true;
                    break;
                }
                text.push(ch);
                i += 1;
            }
            if !closed && strict {
                return Err("Unterminated string literal.");
            }
            out.push(Token::Str(text));
            continue;
        }
        // PostgreSQL dollar-quoted string: `$$...$$` or `$tag$...$tag$`.
        if let Some(len) = dollar_tag_len(&chars, i)
            && (i == 0 || !is_ident_char(chars[i - 1]))
        {
            let tag: Vec<char> = chars[i..i + len].to_vec();
            let start = i + len;
            let mut j = start;
            let mut end = None;
            while j + len <= n {
                if chars[j..j + len] == tag[..] {
                    end = Some(j);
                    break;
                }
                j += 1;
            }
            let Some(end) = end else {
                if strict {
                    return Err("Unterminated dollar-quoted string.");
                }
                out.push(Token::Str(chars[start..n].iter().collect()));
                break;
            };
            out.push(Token::Str(chars[start..end].iter().collect()));
            i = end + len;
            continue;
        }
        // Quoted identifiers: "..." (`""` doubles a quote), `...` (`` `` `` doubles a quote), [...].
        if c == '"' || c == '`' || c == '[' {
            let closing = if c == '[' { ']' } else { c };
            i += 1;
            let mut name = String::new();
            let mut closed = false;
            while i < n {
                if chars[i] == closing {
                    if closing != ']' && chars.get(i + 1) == Some(&closing) {
                        name.push(closing);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    closed = true;
                    break;
                }
                name.push(chars[i]);
                i += 1;
            }
            if !closed && strict {
                return Err("Unterminated quoted identifier.");
            }
            out.push(Token::Ident { name, quoted: true });
            continue;
        }
        if is_ident_start(c) {
            let start = i;
            while i < n && is_ident_char(chars[i]) {
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
    Ok(Lexed {
        tokens: out,
        has_comments,
    })
}

/// Tokenize `sql`: comments are dropped (`--`, `#`, nested `/* */`), string literals and quoted
/// identifiers are read whole (an unterminated one runs to the end of the text). Never fails.
pub fn tokens(sql: &str) -> Vec<Token> {
    lex(sql, false).expect("non-strict lex never fails").tokens
}

fn identifier_chains_from_tokens(
    toks: &[Token],
    normalize: impl Fn(&str) -> String,
) -> Vec<Vec<String>> {
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

/// The identifier chains of `sql` (`a`, `s.t`, `db.s.t`, quoted parts included), lower case.
pub fn identifier_chains(sql: &str) -> Vec<Vec<String>> {
    let toks = tokens(sql);
    identifier_chains_from_tokens(&toks, str::to_lowercase)
}

/// The string literals of `sql`, lower case.
pub fn string_literals(sql: &str) -> Vec<String> {
    tokens(sql)
        .into_iter()
        .filter_map(|t| match t {
            Token::Str(s) => Some(s.to_lowercase()),
            _ => None,
        })
        .collect()
}

/// Extract ASCII identifier words (`[A-Za-z_][A-Za-z0-9_]*`) from `text`, yielding `(start, end)`
/// byte offsets so callers can preserve original casing.
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

/// The definition texts of `parameters` (the non-empty strings at [`DEFINITION_KEYS`]).
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

    /// Whether there is nothing to hide.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// The names.
    pub fn names(&self) -> &[String] {
        &self.names
    }
}

/// Whether `lexed` represents a bare table identifier (`t`, `s.t`, `db.s.t`, quoted or unquoted,
/// with optional trailing `;`), matching v4's `_BARE_IDENTIFIER_PATTERN`.
fn is_bare_identifier_tokens(lexed: &Lexed) -> bool {
    if lexed.has_comments {
        return false;
    }
    let mut slice = lexed.tokens.as_slice();
    while matches!(slice.last(), Some(Token::Punct(';'))) {
        slice = &slice[..slice.len() - 1];
    }
    // 1, 2, or 3 non-empty identifier parts separated by `.` -> token count is 1, 3, or 5.
    if !matches!(slice.len(), 1 | 3 | 5) {
        return false;
    }
    for (idx, tok) in slice.iter().enumerate() {
        if idx % 2 == 0 {
            match tok {
                Token::Ident { name, .. } if !name.is_empty() => {}
                _ => return false,
            }
        } else if *tok != Token::Punct('.') {
            return false;
        }
    }
    true
}

/// Enforce the v4 read-only statement check on an already-lexed strict token stream.
fn check_read_only_tokens(lexed: &Lexed) -> Result<(), SqlError> {
    if lexed.tokens.is_empty() {
        return Err(SqlError::invalid_query(
            "Refusing to execute an empty query.",
        ));
    }
    if is_bare_identifier_tokens(lexed) {
        return Ok(());
    }
    let statements: Vec<&[Token]> = lexed
        .tokens
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
    // Unquoted identifier words only (quoted identifiers and string literals are excluded,
    // matching v4's `strip_sql_noise` + `words`).
    let words: Vec<String> = statement
        .iter()
        .filter_map(|t| match t {
            Token::Ident {
                name,
                quoted: false,
            } => Some(name.to_lowercase()),
            _ => None,
        })
        .collect();
    let Some(first) = words.first() else {
        return Err(SqlError::invalid_query(
            "Could not parse a statement from the query.",
        ));
    };
    let b = builtin();
    if !b.read_only_starts.contains(first) {
        let mut starts = b.read_only_starts.clone();
        starts.sort();
        return Err(SqlError::invalid_query(format!(
            "Refusing to execute a '{}' statement. `read_sql` only accepts read-only queries ({}).",
            first.to_uppercase(),
            starts.join(", ")
        )));
    }
    for word in &words {
        if b.mutating_keywords.contains(word) || b.mutating_functions.contains(word) {
            return Err(SqlError::invalid_query(format!(
                "Refusing to execute a query containing '{}'. `read_sql` only accepts read-only \
                 queries; use a dedicated write tool or the `execute_action` tool instead.",
                word.to_uppercase()
            )));
        }
        if b.mutating_word_prefixes
            .iter()
            .any(|p| word.starts_with(p.as_str()))
        {
            return Err(SqlError::invalid_query(format!(
                "Refusing to execute a query containing '{word}': procedural packages are not \
                 reachable from `read_sql`."
            )));
        }
    }
    Ok(())
}

/// V4 protected-table check on unquoted identifiers (`Refusing to query protected table '...'`).
fn check_v4_protected_tables(tokens: &[Token]) -> Result<(), SqlError> {
    let protected = &builtin().protected_tables;
    for token in tokens {
        if let Token::Ident {
            name,
            quoted: false,
        } = token
        {
            for (start, end) in ascii_word_spans(name) {
                let raw = &name[start..end];
                let lower = raw.to_ascii_lowercase();
                if protected.iter().any(|p| p == &lower) {
                    return Err(SqlError::forbidden(format!(
                        "Refusing to query protected table '{raw}'."
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Check `tokens` and raw `sql` for Meerschaum's internal tables (`mrsm_*`, `_mrsm_*`,
/// `sqlite_dbpage`, protected instance tables), text-query functions ([`TEXT_QUERY_FUNCTIONS`]),
/// and Unicode-escaped names (`U&"..."`, `U&'...'`), parameterized by `context` (e.g.
/// `"for an API request"` or `"in a pipe's definition"`).
fn check_internal_tables_tokens(
    sql: &str,
    tokens: &[Token],
    context: &str,
) -> Result<(), SqlError> {
    let lower_raw = sql.to_ascii_lowercase();
    if lower_raw.contains("u&\"") || lower_raw.contains("u&'") {
        return Err(SqlError::forbidden(format!(
            "Refusing SQL with Unicode-escaped names (`U&\"...\"`) {context}."
        )));
    }
    let protected = &builtin().protected_tables;
    let check_word = |raw: &str, next_opens_call: bool| -> Result<(), SqlError> {
        let lower = raw.to_ascii_lowercase();
        if lower.starts_with("mrsm_")
            || lower.starts_with("_mrsm_")
            || lower == "sqlite_dbpage"
            || protected.iter().any(|t| t == &lower)
        {
            return Err(SqlError::forbidden(format!(
                "Refusing SQL that names Meerschaum's internal table '{raw}' {context}."
            )));
        }
        if TEXT_QUERY_FUNCTIONS.contains(&lower.as_str()) && (lower != "query" || next_opens_call) {
            return Err(SqlError::forbidden(format!(
                "Refusing SQL that calls `{lower}()` {context}: it reads tables it does not name."
            )));
        }
        Ok(())
    };
    for (idx, token) in tokens.iter().enumerate() {
        match token {
            Token::Ident { name, .. } => {
                let called = matches!(tokens.get(idx + 1), Some(Token::Punct('(')));
                let spans = ascii_word_spans(name);
                for (span_idx, (start, end)) in spans.iter().enumerate() {
                    let is_last = span_idx + 1 == spans.len();
                    let next_opens = if is_last {
                        called
                    } else {
                        name[*end..].trim_start().starts_with('(')
                    };
                    check_word(&name[*start..*end], next_opens)?;
                }
            }
            Token::Str(text) => {
                for (start, end) in ascii_word_spans(text) {
                    let next_opens = text[end..].trim_start().starts_with('(');
                    check_word(&text[start..end], next_opens)?;
                }
            }
            Token::Punct(_) => {}
        }
    }
    Ok(())
}

/// Check `sql` for Meerschaum's internal tables, text-query functions, and Unicode-escaped names,
/// with the refusal phrased for `context` (for example `"for an API request"`).
pub fn check_internal_tables(sql: &str, context: &str) -> Result<(), SqlError> {
    let lexed = lex(sql, true)
        .map_err(|r| SqlError::invalid_query(format!("Refusing to execute the query: {r}")))?;
    check_internal_tables_tokens(sql, &lexed.tokens, context)
}

/// Check `MRSM{...}` config references, hidden tables (identifier chains and string literals),
/// and catalog schemas/tables when `!hidden.is_empty()`.
fn check_hidden_and_catalog(
    sql: &str,
    tokens: &[Token],
    hidden: &HiddenTables,
    phrasing: Phrasing,
) -> Result<(), SqlError> {
    if sql.contains("MRSM{") {
        return Err(SqlError::forbidden(format!(
            "Refusing {} with an `MRSM{{...}}` config reference: it would read the \
             server's configuration (connector credentials included) when the pipe syncs. Ask an \
             administrator to set it.",
            phrasing.subject
        )));
    }
    let chains = identifier_chains_from_tokens(tokens, |name| hidden.normalize_identifier(name));
    for chain in &chains {
        for part in chain {
            if hidden.names.iter().any(|h| h == part) {
                return Err(SqlError::forbidden(format!(
                    "Refusing {} that names '{part}': it is the table of a pipe \
                     you cannot see.",
                    phrasing.subject
                )));
            }
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
    if !hidden.is_empty() {
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

/// Enforce the canonical SQL source broker's pure-function allowlist and `FROM 'file'` /
/// `JOIN 'file'` rejection on an already-lexed token stream.
fn check_source_broker_tokens(tokens: &[Token]) -> Result<(), SqlError> {
    for (index, token) in tokens.iter().enumerate() {
        if let Token::Ident { name, .. } = token {
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

/// Check `sql` against a [`SqlPolicy`] profile in a single tokenization pass.
pub fn check_sql(sql: &str, policy: &SqlPolicy<'_>) -> Result<(), SqlError> {
    match policy {
        SqlPolicy::ReadOnlyStdioQuery => {
            if sql.trim().is_empty() {
                return Err(SqlError::invalid_query(
                    "Refusing to execute an empty query.",
                ));
            }
            let lexed = lex(sql, true).map_err(|r| {
                SqlError::invalid_query(format!("Refusing to execute the query: {r}"))
            })?;
            check_read_only_tokens(&lexed)?;
            check_v4_protected_tables(&lexed.tokens)?;
            Ok(())
        }
        SqlPolicy::ReadOnlyApiQuery { hidden } => {
            if sql.trim().is_empty() {
                return Err(SqlError::invalid_query(
                    "Refusing to execute an empty query.",
                ));
            }
            let lexed = lex(sql, true).map_err(|r| {
                SqlError::invalid_query(format!("Refusing to execute the query: {r}"))
            })?;
            check_read_only_tokens(&lexed)?;
            check_v4_protected_tables(&lexed.tokens)?;
            check_internal_tables_tokens(sql, &lexed.tokens, Phrasing::API_QUERY.internal_context)?;
            if let Some(hidden) = hidden {
                check_hidden_and_catalog(sql, &lexed.tokens, hidden, Phrasing::API_QUERY)?;
            }
            Ok(())
        }
        SqlPolicy::ReadOnlyHostQuery { hidden } => {
            if sql.trim().is_empty() {
                return Err(SqlError::invalid_query(
                    "Refusing to execute an empty query.",
                ));
            }
            let lexed = lex(sql, true).map_err(|r| {
                SqlError::invalid_query(format!("Refusing to execute the query: {r}"))
            })?;
            check_internal_tables_tokens(sql, &lexed.tokens, Phrasing::API_QUERY.internal_context)?;
            check_read_only_tokens(&lexed)?;
            check_hidden_and_catalog(sql, &lexed.tokens, hidden, Phrasing::API_QUERY)?;
            Ok(())
        }
        SqlPolicy::PipeDefinition { hidden } => {
            if sql.contains("MRSM{") {
                return Err(SqlError::forbidden(
                    "Refusing a SQL definition with an `MRSM{...}` config reference: it would \
                     read the server's configuration (connector credentials included) when the \
                     pipe syncs. Ask an administrator to set it.",
                ));
            }
            let lexed = lex(sql, true)
                .map_err(|r| SqlError::forbidden(format!("Refusing a SQL definition: {r}")))?;
            check_internal_tables_tokens(
                sql,
                &lexed.tokens,
                Phrasing::DEFINITION.internal_context,
            )?;
            check_hidden_and_catalog(sql, &lexed.tokens, hidden, Phrasing::DEFINITION)?;
            Ok(())
        }
        SqlPolicy::SourceBrokerQuery { hidden } => {
            if sql.trim().is_empty() {
                return Err(SqlError::invalid_query(
                    "Refusing to execute an empty query.",
                ));
            }
            let lexed = lex(sql, true).map_err(|r| {
                SqlError::invalid_query(format!("Refusing to execute the query: {r}"))
            })?;
            check_read_only_tokens(&lexed)?;
            check_internal_tables_tokens(
                sql,
                &lexed.tokens,
                Phrasing::DEFINITION.internal_context,
            )?;
            check_hidden_and_catalog(sql, &lexed.tokens, hidden, Phrasing::DEFINITION)?;
            check_source_broker_tokens(&lexed.tokens)?;
            Ok(())
        }
        SqlPolicy::RawExec { hidden } => {
            let lexed = lex(sql, true)
                .map_err(|r| SqlError::forbidden(format!("Refusing a SQL statement: {r}")))?;
            check_internal_tables_tokens(
                sql,
                &lexed.tokens,
                Phrasing::API_STATEMENT.internal_context,
            )?;
            check_hidden_and_catalog(sql, &lexed.tokens, hidden, Phrasing::API_STATEMENT)?;
            Ok(())
        }
    }
}

/// Check one SQL definition text (raw or resolved) of a caller who is not an administrator.
/// `Err` is the refusal (the message of a `403`).
pub fn check_definition(sql: &str, hidden: &HiddenTables) -> Result<(), String> {
    check_sql(sql, &SqlPolicy::PipeDefinition { hidden }).map_err(SqlError::into_message)
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
    let toks = tokens(target);
    let normalized = identifier_chains_from_tokens(&toks, |name| hidden.normalize_identifier(name));
    if hidden
        .names
        .iter()
        .any(|h| *h == lower || h == last || normalized.iter().any(|chain| chain.last() == Some(h)))
    {
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

    #[test]
    fn identifiers_are_read_quoted_qualified_and_without_comments() {
        let chains = identifier_chains(
            "SELECT a.x, \"B\".\"Y z\" FROM [dbo].[t1] -- secret\n JOIN `db`.`t2` /* c /* d */ e */",
        );
        assert!(chains.contains(&vec!["a".to_string(), "x".to_string()]));
        assert!(chains.contains(&vec!["b".to_string(), "y z".to_string()]));
        assert!(chains.contains(&vec!["dbo".to_string(), "t1".to_string()]));
        assert!(chains.contains(&vec!["db".to_string(), "t2".to_string()]));
        assert!(!chains.iter().flatten().any(|p| p == "secret" || p == "e"));
        assert_eq!(
            string_literals("SELECT 'it''s', E'a\\'b', $x$ $$ $x$"),
            vec!["it's", "a'b", " $$ "]
        );
    }

    #[test]
    fn strict_tokenizer_handles_nested_comments_doubled_quotes_and_dollar_quotes() {
        let stdio = SqlPolicy::ReadOnlyStdioQuery;
        // Nested block comments are accepted when balanced, rejected when unbalanced.
        assert!(check_sql("SELECT /* outer /* inner */ still outer */ 1", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT /* outer /* inner */ 1", &stdio).unwrap_err(),
            SqlError::invalid_query("Refusing to execute the query: Unterminated block comment.")
        );
        // Executable MySQL comments are rejected.
        assert_eq!(
            check_sql("SELECT /*! 1 */ 2", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute the query: Refusing an executable comment (`/*! ... */`), \
                 which MySQL runs as SQL."
            )
        );
        // Doubled quotes inside quoted identifiers stay one identifier (no false semicolon split).
        assert!(check_sql("SELECT \"a\"\"b; DROP TABLE t\" FROM x", &stdio).is_ok());
        assert!(check_sql("SELECT `a``b; DELETE FROM t` FROM x", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT \"unterminated\"\"ident FROM x", &stdio).unwrap_err(),
            SqlError::invalid_query(
                "Refusing to execute the query: Unterminated quoted identifier."
            )
        );
        // E'...' escapes and standard '' doubled quotes.
        assert!(check_sql("SELECT E'a\\'b; DROP TABLE t'", &stdio).is_ok());
        assert!(check_sql("SELECT 'a''b; DROP TABLE t'", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT E'trailing\\", &stdio).unwrap_err(),
            SqlError::invalid_query("Refusing to execute the query: Unterminated string literal.")
        );
        // Dollar-quoted strings.
        assert!(check_sql("SELECT $$x; DROP TABLE t$$", &stdio).is_ok());
        assert!(check_sql("SELECT $tag$x; DELETE FROM t$tag$", &stdio).is_ok());
        assert_eq!(
            check_sql("SELECT $tag$x;y", &stdio).unwrap_err(),
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
            "SELECT $$x;y$$",
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
            check_sql(
                "SELECT * FROM \"mrsm_users\"",
                &SqlPolicy::ReadOnlyApiQuery { hidden: None }
            )
            .unwrap_err(),
            SqlError::forbidden(
                "Refusing SQL that names Meerschaum's internal table 'mrsm_users' for an API request."
            )
        );
        // Unquoted mrsm_users still keeps v4's message even on API callers (RC15-H-002).
        assert_eq!(
            check_sql(
                "SELECT * FROM mrsm_users",
                &SqlPolicy::ReadOnlyApiQuery { hidden: None }
            )
            .unwrap_err(),
            SqlError::forbidden("Refusing to query protected table 'mrsm_users'.")
        );
    }

    #[test]
    fn internal_tables_are_refused_however_a_query_reaches_them() {
        for query in [
            "SELECT * FROM \"mrsm_users\"",
            "SELECT query_to_xml('select * from ' || 'mrsm' || '_users', true, true, '')",
            // Between a function's name and its arguments: comments (PostgreSQL nests them) and
            // the closing quote of a quoted name.
            "SELECT query_to_xml/**/(E'select * from mrsm\\x5fusers', true, true, '')",
            "SELECT query_to_xml /* a /* nested */ b */ ('select 1', true, true, '')",
            "SELECT \"query_to_xml\"(E'select * from mrsm\\x5fusers', true, true, '')",
            "SELECT * FROM query -- c\n ('select * from ' || 'mrsm' || '_users')",
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
        ] {
            assert!(
                check_internal_tables(query, "for an API request").is_err(),
                "{query}"
            );
        }
        // Original case is preserved in the internal-table refusal.
        assert_eq!(
            check_internal_tables("SELECT * FROM \"MRSM_USERS\"", "for an API request")
                .unwrap_err()
                .message,
            "Refusing SQL that names Meerschaum's internal table 'MRSM_USERS' for an API request."
        );
        for query in [
            "SELECT 1",
            "SELECT 'users' AS name, query FROM t",
            "SELECT query, count(*) FROM logs GROUP BY query",
            "SELECT \"query\" FROM t",
            "SELECT count(query) FROM t WHERE query = 'x'",
        ] {
            assert!(
                check_internal_tables(query, "for an API request").is_ok(),
                "{query}"
            );
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
        ] {
            assert!(check_definition(sql, &hidden).is_err(), "{sql}");
        }
        for sql in [
            "SELECT * FROM my_counts",
            "SELECT secret_counts_2 FROM t",
            "SELECT 'secret_countsx' AS a FROM t",
            "SELECT * FROM t -- secret_counts\n",
            "SELECT * FROM t /* \"secret_counts\" */",
        ] {
            assert!(check_definition(sql, &hidden).is_ok(), "{sql}");
        }
    }

    #[test]
    fn phrasing_is_parameterized_by_policy_profile() {
        let hidden = hidden();
        let def_err = check_sql(
            "SELECT * FROM secret_counts",
            &SqlPolicy::PipeDefinition { hidden: &hidden },
        )
        .unwrap_err();
        assert_eq!(
            def_err.message,
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
        let def_internal = check_sql(
            "SELECT * FROM mrsm_users",
            &SqlPolicy::PipeDefinition { hidden: &hidden },
        )
        .unwrap_err();
        assert_eq!(
            def_internal.message,
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
            assert!(check_definition(sql, &none).is_err(), "{sql}");
        }
        // The catalog is refused only while the caller has hidden pipes.
        let catalog = [
            "SELECT table_name FROM information_schema.tables",
            "SELECT relname FROM pg_class",
            "SELECT name FROM sqlite_master",
            "SELECT name FROM sys.tables",
            "SELECT * FROM \"pg_catalog\".\"pg_tables\"",
        ];
        for sql in catalog {
            assert!(check_definition(sql, &none).is_ok(), "{sql}");
            assert!(check_definition(sql, &hidden()).is_err(), "{sql}");
        }
        // A column named like a catalog schema is not a catalog read.
        assert!(check_definition("SELECT sys FROM t", &hidden()).is_ok());
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
