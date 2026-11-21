//! The SQL guard's tests: the dialect readings, v4's read-only wording, and every policy's
//! internal-table, hidden-table, catalog, config-reference and source-broker refusals.

use super::lexer::{DIALECTS, Token, identifier_chains, lex};
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
        SqlError::invalid_query("Refusing to execute the query: Unterminated quoted identifier.")
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
