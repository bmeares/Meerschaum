//! The checks a [`SqlPolicy`] runs, in its order: v4's read-only statement and protected-table
//! checks, Meerschaum's internal tables and the functions that run query text, `MRSM{...}`
//! config references, the tables of hidden pipes and the catalog, and the source broker's
//! function allowlist; with the keyword and catalog lists they read.

use super::lexer::{
    Readings, Token, ascii_word_spans, contains_word, identifier_chains, opens_call, unquoted_words,
};
use super::{
    HiddenTables, MUTATING_FUNCTIONS, MUTATING_KEYWORDS, MUTATING_WORD_PREFIXES, PROTECTED_TABLES,
    READ_ONLY_STARTS, SqlError, SqlPolicy,
};

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

/// One check of a policy, run in the policy's order.
#[derive(Debug, Clone, Copy)]
pub(super) enum Step {
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
pub(super) struct Phrasing {
    pub(super) subject: &'static str,
    pub(super) internal_context: &'static str,
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
    pub(super) fn steps(&self) -> &'static [Step] {
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

    pub(super) fn hidden(&self) -> Option<&HiddenTables> {
        match *self {
            SqlPolicy::ReadOnlyStdioQuery => None,
            SqlPolicy::ReadOnlyApiQuery { hidden } => hidden,
            SqlPolicy::ReadOnlyHostQuery { hidden }
            | SqlPolicy::PipeDefinition { hidden }
            | SqlPolicy::SourceBrokerQuery { hidden }
            | SqlPolicy::RawExec { hidden } => Some(hidden),
        }
    }

    pub(super) fn phrasing(&self) -> Phrasing {
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
pub(super) fn check_read_only(sql: &str, readings: &Readings) -> Result<(), SqlError> {
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
pub(super) fn check_protected_tables(readings: &Readings) -> Result<(), SqlError> {
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
pub(super) fn check_internal_tables(
    sql: &str,
    readings: &Readings,
    context: &str,
) -> Result<(), SqlError> {
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
pub(super) fn check_config_reference(sql: &str, phrasing: Phrasing) -> Result<(), SqlError> {
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
pub(super) fn check_hidden_tables(
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
pub(super) fn check_source_broker(readings: &Readings) -> Result<(), SqlError> {
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
