//! Guardrails for the tools that can reach beyond their scope (`meerschaum/mcp/_security.py`):
//! the read-only statement check of `read_sql`, and the action allow/deny lists of
//! `execute_action`.
//!
//! The statement check is a keyword-level scanner, not a parser, exactly like v4's: it blocks
//! stacked statements, DML, DDL, `SELECT ... INTO`, executable comments and the known mutating
//! built-ins. The durable fix for untrusted callers is a read-only database role.

use super::McpConfig;
use super::registry::builtin;

/// `strip_sql_noise(query)`: the query with comments, string literals and quoted identifiers
/// removed, or the reason it must be refused outright.
pub fn strip_sql_noise(query: &str) -> Result<String, &'static str> {
    let chars: Vec<char> = query.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(query.len());
    let mut i = 0;
    let starts_with = |i: usize, pat: &str| {
        let p: Vec<char> = pat.chars().collect();
        i + p.len() <= n && chars[i..i + p.len()] == p[..]
    };
    let find = |from: usize, pat: &[char]| -> Option<usize> {
        if pat.is_empty() || from > n {
            return None;
        }
        (from..=n.saturating_sub(pat.len()))
            .find(|&j| j + pat.len() <= n && chars[j..j + pat.len()] == *pat)
    };
    while i < n {
        let c = chars[i];
        // Single-quoted string literal: `''` doubles a quote; no backslash escapes.
        if c == '\'' {
            i += 1;
            let mut closed = false;
            while i < n {
                if chars[i] == '\'' {
                    if i + 1 < n && chars[i + 1] == '\'' {
                        i += 2;
                        continue;
                    }
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                return Err("Unterminated string literal.");
            }
            out.push_str("''");
            i += 1;
            continue;
        }
        // Quoted identifiers: "..." (standard), `...` (MySQL), [...] (MSSQL).
        if c == '"' || c == '`' || c == '[' {
            let closing = if c == '[' { ']' } else { c };
            match find(i + 1, &[closing]) {
                None => return Err("Unterminated quoted identifier."),
                Some(end) => {
                    out.push(' ');
                    i = end + 1;
                    continue;
                }
            }
        }
        // PostgreSQL dollar-quoted string: `$$...$$` or `$tag$...$tag$`.
        if c == '$' {
            if let Some(tag_len) = dollar_tag_len(&chars[i..]) {
                let tag: Vec<char> = chars[i..i + tag_len].to_vec();
                match find(i + tag_len, &tag) {
                    None => return Err("Unterminated dollar-quoted string."),
                    Some(end) => {
                        out.push_str("''");
                        i = end + tag_len;
                        continue;
                    }
                }
            }
            out.push(c);
            i += 1;
            continue;
        }
        // Line comments: `--` and `#` run to the end of the line.
        if c == '#' || starts_with(i, "--") {
            out.push(' ');
            match find(i, &['\n']) {
                None => break,
                Some(newline) => {
                    i = newline;
                    continue;
                }
            }
        }
        // Block comments. `/*!` is executed by MySQL, so refuse it.
        if starts_with(i, "/*") {
            if starts_with(i, "/*!") {
                return Err(
                    "Refusing an executable comment (`/*! ... */`), which MySQL runs as SQL.",
                );
            }
            match find(i + 2, &['*', '/']) {
                None => return Err("Unterminated block comment."),
                Some(end) => {
                    out.push(' ');
                    i = end + 2;
                    continue;
                }
            }
        }
        out.push(c);
        i += 1;
    }
    Ok(out)
}

/// The length of a dollar-quote tag at the start of `chars` (`\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$`).
fn dollar_tag_len(chars: &[char]) -> Option<usize> {
    if chars.first() != Some(&'$') {
        return None;
    }
    if chars.get(1) == Some(&'$') {
        return Some(2);
    }
    let first = chars.get(1)?;
    if !(first.is_ascii_alphabetic() || *first == '_') {
        return None;
    }
    let mut j = 2;
    while let Some(c) = chars.get(j) {
        if *c == '$' {
            return Some(j + 1);
        }
        if !(c.is_ascii_alphanumeric() || *c == '_') {
            return None;
        }
        j += 1;
    }
    None
}

/// The words of a statement (`[A-Za-z_][A-Za-z0-9_]*`).
pub fn words(text: &str) -> Vec<&str> {
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
            out.push(&text[start..i]);
        } else {
            i += 1;
        }
    }
    out
}

/// One identifier part: bare, `"..."`, `` `...` `` or `[...]`.
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

/// `_BARE_IDENTIFIER_PATTERN`: a table name, optionally schema- and database-qualified.
fn is_bare_identifier(s: &str) -> bool {
    let mut rest = s;
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

/// `is_read_only_query(query)`: `Ok(())` when `query` is a single read-only statement (or a bare
/// table name), else the refusal.
pub fn is_read_only_query(query: &str) -> Result<(), String> {
    if query.trim().is_empty() {
        return Err("Refusing to execute an empty query.".into());
    }
    // `query.strip().rstrip(';').strip()`.
    let bare = query.trim().trim_end_matches(';').trim();
    if is_bare_identifier(bare) {
        return Ok(());
    }
    let stripped =
        strip_sql_noise(query).map_err(|r| format!("Refusing to execute the query: {r}"))?;
    let stripped = stripped.trim();
    let statements: Vec<&str> = stripped
        .split(';')
        .filter(|p| !p.trim().is_empty())
        .collect();
    if statements.len() > 1 {
        return Err("Refusing to execute multiple statements. \
                    Submit one read-only query at a time."
            .into());
    }
    let Some(statement) = statements.first() else {
        return Err("Refusing to execute an empty query.".into());
    };
    let words: Vec<String> = words(statement).iter().map(|w| w.to_lowercase()).collect();
    let Some(first) = words.first() else {
        return Err("Could not parse a statement from the query.".into());
    };
    let b = builtin();
    if !b.read_only_starts.contains(first) {
        let mut starts = b.read_only_starts.clone();
        starts.sort();
        return Err(format!(
            "Refusing to execute a '{}' statement. `read_sql` only accepts read-only queries ({}).",
            first.to_uppercase(),
            starts.join(", ")
        ));
    }
    for word in &words {
        if b.mutating_keywords.contains(word) || b.mutating_functions.contains(word) {
            return Err(format!(
                "Refusing to execute a query containing '{}'. `read_sql` only accepts read-only \
                 queries; use a dedicated write tool or the `execute_action` tool instead.",
                word.to_uppercase()
            ));
        }
        if b.mutating_word_prefixes
            .iter()
            .any(|p| word.starts_with(p.as_str()))
        {
            return Err(format!(
                "Refusing to execute a query containing '{word}': procedural packages are not \
                 reachable from `read_sql`."
            ));
        }
    }
    Ok(())
}

/// `is_action_permitted(action_name)`.
pub fn is_action_permitted(config: &McpConfig, action: &str) -> Result<(), String> {
    if !config.action_allowlist.is_empty() && !config.action_allowlist.iter().any(|a| a == action) {
        let mut allowed = config.action_allowlist.clone();
        allowed.sort();
        return Err(format!(
            "Action '{action}' is not in this server's MCP action allowlist. \
             Permitted actions: {}.",
            allowed.join(", ")
        ));
    }
    if config.action_denylist.iter().any(|a| a == action) {
        return Err(format!(
            "Action '{action}' is blocked by this server's MCP action denylist because it \
             executes arbitrary code on the API host. Set `api:mcp:actions:denylist` in the \
             Meerschaum config to change this."
        ));
    }
    Ok(())
}

/// `check_action_chain(words)`: no denylisted action as another action's subaction.
pub fn check_action_chain(config: &McpConfig, words: &[String]) -> Result<(), String> {
    for word in words {
        if config.action_denylist.iter().any(|a| a == word) {
            return Err(format!(
                "Action '{word}' is blocked by this server's MCP action denylist and may not be \
                 run through another action either."
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_queries() {
        assert!(is_read_only_query("SELECT 1").is_ok());
        assert!(is_read_only_query("pipes").is_ok());
        assert!(is_read_only_query("\"mrsm\".\"pipes\";").is_ok());
        assert!(is_read_only_query("[dbo].[pipes]").is_ok());
        assert!(is_read_only_query("SELECT 'a;b' AS x;").is_ok());
        assert!(is_read_only_query("WITH a AS (SELECT 1) SELECT * FROM a").is_ok());
        assert!(is_read_only_query("SELECT $$x;y$$").is_ok());
        assert!(is_read_only_query("SELECT $a$x;y").is_err());
        assert_eq!(
            is_read_only_query("DELETE FROM t").unwrap_err(),
            "Refusing to execute a 'DELETE' statement. `read_sql` only accepts read-only \
             queries (desc, describe, explain, select, show, table, values, with)."
        );
        assert!(is_read_only_query("SELECT 1; SELECT 2").is_err());
        assert!(is_read_only_query("SELECT 1 -- ; DROP TABLE t").is_ok());
        assert!(is_read_only_query("SELECT /* x */ 1").is_ok());
        assert!(is_read_only_query("SELECT /*! 1 */ 2").is_err());
        assert!(is_read_only_query("SELECT 'abc").is_err());
        assert!(is_read_only_query("SELECT xp_cmdshell").is_err());
        assert!(is_read_only_query("   ").is_err());
    }

    #[test]
    fn actions() {
        let cfg = McpConfig::default();
        assert!(is_action_permitted(&cfg, "show").is_ok());
        assert!(is_action_permitted(&cfg, "sh").is_err());
        assert!(check_action_chain(&cfg, &["pipes".into()]).is_ok());
        assert!(check_action_chain(&cfg, &["python".into()]).is_err());
        let cfg = McpConfig {
            action_allowlist: vec!["sync".into()],
            ..McpConfig::default()
        };
        assert!(is_action_permitted(&cfg, "show").is_err());
    }
}
