//! Caller-supplied SQL on a protected connector (`protected: true`).
//!
//! A protected connector refuses writes, but reading a caller's own query is still a read:
//! `mrsm sql`, `SQLConnector.read()` / `value()` in Python, a Rust plugin's `host::read_sql` and
//! the MCP `read_sql` tool. Without the allowance (`--allow-protected`,
//! `MRSM_ALLOW_PROTECTED=1`) such a query runs only when [`is_read_statement`] accepts it, and
//! then inside a transaction that is rolled back, read-only where the database allows it
//! ([`crate::SqlConnector::read_raw`], [`crate::driver::SqlDriver::begin_read_only`]):
//!
//! - the word scan stops what a rolled-back transaction cannot undo: DDL commits on its own on
//!   MySQL, MariaDB and Oracle, and SQLAlchemy's SQLite driver runs DDL and `WITH ... DELETE`
//!   outside its transactions;
//! - the rolled-back transaction, read-only on PostgreSQL, MySQL and MariaDB, stops what a word
//!   scan cannot see (`nextval()`, a function that writes).
//!
//! The scan is not a parser: it guards against mistakes (a `DELETE` sent to a protected
//! database), not against a hostile caller, who is one flag away from the allowance anyway.

use crate::flavor::{DriverFamily, Flavor};

/// The words a read statement may start with (`FROM t`, `SUMMARIZE t`, `PIVOT` and `UNPIVOT`
/// are DuckDB's).
const READ_STARTS: &[&str] = &[
    "select",
    "with",
    "values",
    "table",
    "from",
    "show",
    "describe",
    "desc",
    "explain",
    "summarize",
    "pivot",
    "unpivot",
];

/// Statements that only describe the database (`SHOW CREATE TABLE t` reads).
const DESCRIBE_STARTS: &[&str] = &["show", "describe", "desc"];

/// Words that make a statement write, wherever they appear outside quotes and comments: DML
/// (also inside a `WITH`, as PostgreSQL's data-modifying CTEs and T-SQL's `WITH ... DELETE`),
/// `SELECT ... INTO` (a new table, an `OUTFILE`), `SELECT ... FOR UPDATE` (row locks) and the
/// DDL `EXPLAIN ANALYZE` can run.
const WRITE_WORDS: &[&str] = &[
    "insert", "update", "delete", "merge", "upsert", "into", "create", "drop", "alter", "truncate",
    "grant", "revoke",
];

/// One word of a statement outside quotes and comments.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Word {
    /// Lower case.
    text: String,
    /// The previous character is `.` (`t.update` names a column, even when reserved).
    after_dot: bool,
    /// The next character (spaces skipped) is `(` (MySQL's `INSERT()` string function).
    before_paren: bool,
}

/// How the scan reads quotes and comments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Dialect {
    /// MySQL's comments: `#` starts one, and `--` only when a space follows.
    mysql_comments: bool,
    /// A backslash escapes the next character inside `'...'` and `"..."` (MySQL's default).
    backslash_escapes: bool,
    /// `$$...$$` and `$tag$...$tag$` are strings (PostgreSQL, DuckDB).
    dollar_quotes: bool,
}

impl Dialect {
    const STANDARD: Dialect = Dialect {
        mysql_comments: false,
        backslash_escapes: false,
        dollar_quotes: false,
    };
    const POSTGRES: Dialect = Dialect {
        dollar_quotes: true,
        ..Dialect::STANDARD
    };
    const MYSQL: Dialect = Dialect {
        mysql_comments: true,
        backslash_escapes: true,
        dollar_quotes: false,
    };

    /// The readings a statement for `flavor` must pass (every reading for an unknown flavor).
    ///
    /// MySQL is read with its default backslash escapes: a server in `NO_BACKSLASH_ESCAPES`
    /// mode could see a second statement where the scan sees a string, but reads run one
    /// prepared statement (Rust) or without multi-statement support (PyMySQL), in a read-only
    /// transaction.
    fn readings(flavor: Option<Flavor>) -> &'static [Dialect] {
        match flavor.map(Flavor::driver_family) {
            Some(DriverFamily::Postgres | DriverFamily::Duckdb) => &[Dialect::POSTGRES],
            Some(DriverFamily::Mysql) => &[Dialect::MYSQL],
            Some(_) => &[Dialect::STANDARD],
            None => &[Dialect::STANDARD, Dialect::POSTGRES, Dialect::MYSQL],
        }
    }
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// The words of `sql` when it is one statement (a trailing `;` is fine), or `None` when it is
/// several statements, has an unterminated quote or comment, or holds a MySQL executable
/// comment (`/*! ... */`, which MySQL runs).
fn scan(sql: &str, dialect: Dialect) -> Option<Vec<Word>> {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut words: Vec<Word> = Vec::new();
    // A statement ended (`;`): anything but blanks, comments and `;` after it is a second one.
    let mut ended = false;
    // The last character of code (outside quotes and comments).
    let mut prev: Option<char> = None;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        let line_comment = (c == '-'
            && next == Some('-')
            && (!dialect.mysql_comments || chars.get(i + 2).is_none_or(|c| c.is_whitespace())))
            || (dialect.mysql_comments && c == '#');
        if line_comment {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && next == Some('*') {
            if chars.get(i + 2) == Some(&'!') {
                return None;
            }
            let mut j = i + 2;
            while !(chars.get(j) == Some(&'*') && chars.get(j + 1) == Some(&'/')) {
                if j >= n {
                    return None;
                }
                j += 1;
            }
            i = j + 2;
            continue;
        }
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if ended && c != ';' {
            return None;
        }
        match c {
            ';' => {
                ended = true;
                prev = Some(c);
                i += 1;
            }
            // Strings and quoted identifiers: a doubled quote stays inside.
            '\'' | '"' | '`' | '[' => {
                let close = if c == '[' { ']' } else { c };
                let escapes = dialect.backslash_escapes && matches!(c, '\'' | '"');
                let mut j = i + 1;
                loop {
                    let d = *chars.get(j)?;
                    if escapes && d == '\\' {
                        j += 2;
                        continue;
                    }
                    if d == close {
                        if chars.get(j + 1) == Some(&close) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                prev = Some(close);
                i = j + 1;
            }
            // Dollar quotes start where a token starts (`a$b$` is one identifier); `$1` is a
            // parameter.
            '$' if dialect.dollar_quotes && !(i > 0 && is_word_char(chars[i - 1])) => {
                let mut j = i + 1;
                while chars.get(j).is_some_and(|&d| d != '$' && is_word_char(d)) {
                    j += 1;
                }
                let tagged = chars.get(j) == Some(&'$')
                    && (j == i + 1 || chars.get(i + 1).copied().is_some_and(is_word_start));
                if tagged {
                    let tag = &chars[i..=j];
                    let mut k = j + 1;
                    while chars.get(k..k + tag.len()) != Some(tag) {
                        if k + tag.len() > n {
                            return None;
                        }
                        k += 1;
                    }
                    prev = Some('$');
                    i = k + tag.len();
                } else {
                    prev = Some(c);
                    i += 1;
                }
            }
            _ if is_word_start(c) => {
                let start = i;
                while chars.get(i).copied().is_some_and(is_word_char) {
                    i += 1;
                }
                let before_paren = chars[i..]
                    .iter()
                    .find(|d| !d.is_whitespace())
                    .is_some_and(|d| *d == '(');
                words.push(Word {
                    text: chars[start..i].iter().collect::<String>().to_lowercase(),
                    after_dot: prev == Some('.'),
                    before_paren,
                });
                prev = Some(chars[i - 1]);
            }
            _ => {
                prev = Some(c);
                i += 1;
            }
        }
    }
    Some(words)
}

/// Whether `words` (one statement's) only read.
fn reads(words: &[Word]) -> bool {
    let Some(first) = words.first() else {
        return false;
    };
    if !READ_STARTS.contains(&first.text.as_str()) {
        return false;
    }
    if DESCRIBE_STARTS.contains(&first.text.as_str()) {
        return true;
    }
    !words.iter().any(|w| {
        !w.after_dot
            && WRITE_WORDS.contains(&w.text.as_str())
            && !(w.text == "insert" && w.before_paren)
    })
}

/// Whether `sql` is one statement that only reads: it starts with `SELECT`, `WITH`, `VALUES`,
/// `TABLE`, `FROM`, `SHOW`, `DESCRIBE`, `EXPLAIN` (or DuckDB's `SUMMARIZE`, `PIVOT`,
/// `UNPIVOT`), and no word outside quotes and comments
/// writes (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `INTO`, DDL). Quotes and comments are read the
/// way `flavor` reads them (every way for `None`, so text that hides a second statement or a
/// write under any reading is refused).
pub fn is_read_statement(sql: &str, flavor: Option<Flavor>) -> bool {
    Dialect::readings(flavor)
        .iter()
        .all(|dialect| scan(sql, *dialect).is_some_and(|words| reads(&words)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLAVORS: &[Option<Flavor>] = &[
        None,
        Some(Flavor::Postgresql),
        Some(Flavor::Mysql),
        Some(Flavor::Mariadb),
        Some(Flavor::Mssql),
        Some(Flavor::Sqlite),
        Some(Flavor::Oracle),
        Some(Flavor::Duckdb),
    ];

    #[test]
    fn reads_are_accepted() {
        for sql in [
            "SELECT 1",
            "select * from t;",
            "  SELECT * FROM t ;  ",
            "(SELECT 1) UNION (SELECT 2)",
            "SELECT * FROM t WHERE label = 'delete; drop table t'",
            "SELECT \"update\", [delete], `insert` FROM t",
            "SELECT t.update, t . delete FROM t",
            "WITH a AS (SELECT 1 AS x) SELECT * FROM a",
            "WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM r) SELECT n FROM r",
            "VALUES (1), (2)",
            "TABLE t",
            "FROM t",
            "SUMMARIZE t",
            "PIVOT t ON year USING sum(v)",
            "SHOW TABLES",
            "SHOW CREATE TABLE t",
            "DESCRIBE t",
            "EXPLAIN SELECT * FROM t",
            "SELECT INSERT('abc', 1, 1, 'x')",
            "SELECT REPLACE(label, 'a', 'b') FROM t",
            "SELECT * FROM t -- DELETE FROM t\n",
            "-- a note\nSELECT 1",
            "SELECT * FROM t /* DELETE FROM t; */",
            "/* a note */ SELECT 1",
            "SELECT 'it''s'",
            "SELECT * FROM pragma_table_info('t')",
            "SELECT created_at, updated_by, delete_flag, into_x FROM t",
            "SELECT * FROM t WITH (NOLOCK)",
            "SELECT * FROM t WHERE ts >= '2024-01-01' AND id IN (1, 2)",
            "SELECT 1e5, 0x1F",
        ] {
            for flavor in FLAVORS {
                assert!(is_read_statement(sql, *flavor), "{flavor:?}: {sql}");
            }
        }
    }

    #[test]
    fn dialect_quotes() {
        let pg = Some(Flavor::Postgresql);
        let mysql = Some(Flavor::Mysql);
        // Dollar quotes are strings on PostgreSQL and DuckDB only.
        for sql in [
            "SELECT $$a;b$$",
            "SELECT $tag$ DELETE FROM t; $tag$ AS x",
            "SELECT $1",
            "SELECT a$b FROM t",
        ] {
            assert!(is_read_statement(sql, pg), "{sql}");
            assert!(is_read_statement(sql, Some(Flavor::Duckdb)), "{sql}");
        }
        assert!(!is_read_statement("SELECT $$a;b$$", mysql));
        assert!(!is_read_statement(
            "SELECT $a$ ; DELETE FROM t ; $a$",
            Some(Flavor::Sqlite)
        ));
        // `#tmp` is an MSSQL temporary table, a comment on MySQL.
        assert!(is_read_statement("SELECT * FROM #tmp", Some(Flavor::Mssql)));
        // A backslash escapes on MySQL only: `'C:\'` ends there elsewhere.
        assert!(is_read_statement("SELECT 'C:\\'", pg));
        assert!(!is_read_statement("SELECT 'C:\\'", mysql));
        assert!(is_read_statement("SELECT 'a\\'b'", mysql));
    }

    #[test]
    fn writes_and_stacked_statements_are_refused() {
        for sql in [
            "",
            ";",
            "DELETE FROM t",
            "delete from t where x = 1 returning x",
            "UPDATE t SET x = 1",
            "INSERT INTO t VALUES (1)",
            "REPLACE INTO t VALUES (1)",
            "DROP TABLE t",
            "CREATE TABLE t AS SELECT 1",
            "ALTER TABLE t ADD COLUMN x INT",
            "TRUNCATE t",
            "SET search_path = x",
            "COPY (SELECT 1) TO STDOUT",
            "CALL p()",
            "EXEC p",
            "PRAGMA user_version = 1",
            "VACUUM",
            "BEGIN",
            "COMMIT",
            "WITH d AS (DELETE FROM t RETURNING x) SELECT * FROM d",
            "WITH c AS (SELECT * FROM t) DELETE FROM c",
            "WITH c AS (SELECT 1) INSERT t SELECT * FROM c",
            "WITH c AS (SELECT 1) UPDATE t SET x = 1",
            "SELECT * INTO new_table FROM t",
            "SELECT * FROM t INTO OUTFILE '/tmp/x'",
            "SELECT * FROM t FOR UPDATE",
            "EXPLAIN ANALYZE DELETE FROM t",
            "EXPLAIN ANALYZE CREATE TABLE t2 AS SELECT 1",
            "MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN DELETE",
            "SELECT 1; DELETE FROM t",
            "SELECT 1; SELECT 2",
            "SELECT 1;; DELETE FROM t",
            "SELECT 1; -- c\n DELETE FROM t",
            "SELECT 'unterminated",
            "SELECT \"unterminated",
            "SELECT [unterminated",
            "SELECT /* unterminated",
            "SELECT /*! DELETE FROM t */ 1",
            "SELECT 1 /* nested /* */ DELETE */",
        ] {
            for flavor in FLAVORS {
                assert!(!is_read_statement(sql, *flavor), "{flavor:?}: {sql}");
            }
        }
    }

    #[test]
    fn mysql_readings_hide_nothing() {
        for flavor in [Some(Flavor::Mysql), Some(Flavor::Mariadb), None] {
            for sql in [
                // `\'` is an escaped quote on MySQL: the `;` is code there.
                "SELECT 'a\\' , ' ; DELETE FROM t ; SELECT 'b\\' , '",
                // `--1` is not a comment on MySQL (it needs a space): the `;` is code.
                "SELECT 1 --1; DELETE FROM t",
            ] {
                assert!(!is_read_statement(sql, flavor), "{flavor:?}: {sql}");
            }
        }
        // Read the standard way, `'a\'` ends at its second quote: the `;` is code.
        assert!(!is_read_statement(
            "SELECT 'a\\' ; DELETE FROM t ; SELECT '\\' , '",
            Some(Flavor::Postgresql)
        ));
        assert!(!is_read_statement(
            "SELECT 'a\\' ; DELETE FROM t ; SELECT '\\' , '",
            None
        ));
        // `#` starts a comment on MySQL only: elsewhere the `;` is code.
        assert!(!is_read_statement(
            "SELECT 1 # ; DELETE FROM t",
            Some(Flavor::Postgresql)
        ));
        assert!(!is_read_statement("SELECT 1 # ; DELETE FROM t", None));
        assert!(is_read_statement(
            "SELECT 1 # ; DELETE FROM t",
            Some(Flavor::Mysql)
        ));
        assert!(!is_read_statement(
            "SELECT 1 # comment\n; DELETE FROM t",
            Some(Flavor::Mysql)
        ));
    }
}
