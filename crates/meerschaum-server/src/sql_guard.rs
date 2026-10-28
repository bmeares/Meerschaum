//! The SQL guard (access model, AC8): what a caller who is not an administrator may write into a
//! pipe's SQL definition through the API, MCP and the console.
//!
//! A pipe's SQL runs with the source connector's database credentials, not the caller's, so
//! visibility alone cannot keep a definition from reading another team's table. Through the API
//! (every `api:` client, MCP and the console), the server therefore:
//!
//! - asks for the `pipes:define_sql` permission to set or change a definition
//!   ([`changes_definition`]: `fetch.definition`, `definition`, `query`, `sql`, and the `fetch`
//!   parameters of `sql:` connectors);
//! - refuses a definition that names Meerschaum's internal tables (`mrsm_*`, the engine's state
//!   tables, the `_mrsm_internal` schema) or calls a function that runs query text
//!   ([`crate::mcp::security::refuse_internal_tables`]);
//! - refuses a definition that names the table of a pipe the caller cannot see, as a bare,
//!   quoted (`"t"`, `` `t` ``, `[t]`) or schema-qualified identifier, or inside a string literal
//!   ([`check_definition`]), before and after its `{{ Pipe() }}` templates are resolved;
//! - refuses `MRSM{...}` config symlinks in a definition (they resolve to the server's
//!   configuration, connector passwords included, when the pipe syncs);
//! - and, while the caller has pipes it cannot see on the instance, refuses definitions that read
//!   the database's catalog (`information_schema`, `pg_catalog`, `sqlite_master`, `sys.*`, ...),
//!   which would list the hidden tables' names.
//!
//! This is best-effort parsing (a tokenizer, not a SQL parser), like the MCP `read_sql` checks:
//! true isolation between teams needs separate connectors (database users) per team.

use meerschaum_core::{Map, Value};

/// The parameter keys of a pipe's SQL definition, in the order `get_pipe_query` reads them
/// (`fetch.definition` first).
pub const DEFINITION_KEYS: &[&[&str]] = &[
    &["fetch", "definition"],
    &["definition"],
    &["query"],
    &["sql"],
];

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

/// Tokenize `sql`: comments are dropped (`--`, `#`, nested `/* */`), string literals and quoted
/// identifiers are read whole (an unterminated one runs to the end of the text). Never fails.
pub fn tokens(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // Line comments.
        if c == '#' || (c == '-' && chars.get(i + 1) == Some(&'-')) {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // Block comments (nested, as PostgreSQL and DuckDB read them).
        if c == '/' && chars.get(i + 1) == Some(&'*') {
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
            continue;
        }
        // String literals, with an `E` / `N` / `B` / `X` / `U&` prefix read as part of them.
        let prefixed = matches!(c, 'e' | 'E' | 'n' | 'N' | 'b' | 'B' | 'x' | 'X')
            && chars.get(i + 1) == Some(&'\'')
            && (i == 0 || !is_ident_char(chars[i - 1]));
        if c == '\'' || prefixed {
            let escapes = prefixed && matches!(c, 'e' | 'E');
            i += if prefixed { 2 } else { 1 };
            let mut text = String::new();
            while i < n {
                let ch = chars[i];
                if escapes && ch == '\\' && i + 1 < n {
                    text.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                if ch == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        text.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                text.push(ch);
                i += 1;
            }
            out.push(Token::Str(text));
            continue;
        }
        if let Some(len) = dollar_tag_len(&chars, i)
            && (i == 0 || !is_ident_char(chars[i - 1]))
        {
            let tag: Vec<char> = chars[i..i + len].to_vec();
            let start = i + len;
            let mut j = start;
            let mut end = n;
            let mut after = n;
            while j + len <= n {
                if chars[j..j + len] == tag[..] {
                    end = j;
                    after = j + len;
                    break;
                }
                j += 1;
            }
            out.push(Token::Str(chars[start..end.max(start)].iter().collect()));
            i = after;
            continue;
        }
        // Quoted identifiers: "..." (`""` doubles a quote), `...`, [...].
        if c == '"' || c == '`' || c == '[' {
            let closing = if c == '[' { ']' } else { c };
            i += 1;
            let mut name = String::new();
            while i < n {
                if chars[i] == closing {
                    if closing != ']' && chars.get(i + 1) == Some(&closing) {
                        name.push(closing);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                name.push(chars[i]);
                i += 1;
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
    out
}

/// The identifier chains of `sql` (`a`, `s.t`, `db.s.t`, quoted parts included), lower case.
pub fn identifier_chains(sql: &str) -> Vec<Vec<String>> {
    identifier_chains_with(sql, str::to_lowercase)
}

fn identifier_chains_with(sql: &str, normalize: impl Fn(&str) -> String) -> Vec<Vec<String>> {
    let toks = tokens(sql);
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

/// The names (lower case) a definition must not mention, besides Meerschaum's internal tables:
/// the target tables of the pipes the caller cannot see.
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

/// Check one SQL definition text (raw or resolved) of a caller who is not an administrator.
/// `Err` is the refusal (the message of a `403`).
pub fn check_definition(sql: &str, hidden: &HiddenTables) -> Result<(), String> {
    if sql.contains("MRSM{") {
        return Err(
            "Refusing a SQL definition with an `MRSM{...}` config reference: it would read the \
             server's configuration (connector credentials included) when the pipe syncs. Ask an \
             administrator to set it."
                .into(),
        );
    }
    crate::mcp::security::refuse_internal_tables(sql)
        .map_err(|e| e.replace("for an API request", "in a pipe's definition"))?;
    let chains = identifier_chains_with(sql, |name| hidden.normalize_identifier(name));
    for chain in &chains {
        for part in chain {
            if hidden.names.iter().any(|h| h == part) {
                return Err(format!(
                    "Refusing a SQL definition that names '{part}': it is the table of a pipe \
                     you cannot see."
                ));
            }
        }
    }
    for literal in string_literals(sql) {
        if let Some(name) = hidden.names.iter().find(|h| contains_word(&literal, h)) {
            return Err(format!(
                "Refusing a SQL definition that names '{name}' in a string: it is the table of \
                 a pipe you cannot see."
            ));
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
                return Err(format!(
                    "Refusing a SQL definition that reads the database's catalog ('{name}') \
                     while this instance has pipes you cannot see."
                ));
            }
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
    let normalized = identifier_chains_with(target, |name| hidden.normalize_identifier(name));
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

/// The target-name parameters of a pipe.
pub const TARGET_KEYS: &[&str] = &["target", "target_name", "target_table", "target_table_name"];

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
