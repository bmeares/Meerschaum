//! `sql <label> "CREATE VIEW ... AS SELECT ..."` reaches the driver through `read`: v4 routes any
//! query whose text contains `select` there (`actions/sql.py`), and pyodbc / python-oracledb / the
//! other drivers execute the statement as written. The statement must run on every flavor and
//! return no rows (v4's `read()` then fails to build a frame and the action prints `None`; the CLI
//! does the same for a frame without columns).
mod common;
use common::*;

flavor_tests!(read_runs_a_create_view, |c: Ctx| async move {
    let driver = c.conn.driver();
    let view = format!(
        "w17bo_v_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let from_dual = if c.is_oracle() { " FROM dual" } else { "" };
    let name = meerschaum_connector_sql::ident::sql_item_name(&view, c.conn.flavor(), None);
    let frame = driver
        .read_frame(&format!("CREATE VIEW {name} AS SELECT 1 AS a{from_dual}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE VIEW through read: {e}"));
    // DuckDB answers a DDL statement with a `Count` result set; the others return no result set.
    if c.flavor != "duckdb" {
        assert!(frame.names().is_empty(), "{:?}", frame.names());
    }
    let frame = driver
        .read_frame(&format!("SELECT a FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("the view was not created: {e}"));
    assert_eq!(frame.height(), 1);
    driver.exec(&format!("DROP VIEW {name}")).await.unwrap();
});

// DML through `read` (`INSERT ... SELECT` contains `select`, so `sql <label> "INSERT ... SELECT ..."`
// is routed there): the statement runs and is committed. v4.1.4 rolled it back on every flavor
// except MSSQL (`read()` raises after the statement ran, inside its transaction); see W17O-010.
flavor_tests!(read_commits_an_insert_select, |c: Ctx| async move {
    let driver = c.conn.driver();
    let tag = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let flavor = c.conn.flavor();
    let name = |n: &str| {
        meerschaum_connector_sql::ident::sql_item_name(&format!("w17bo_{n}_{tag}"), flavor, None)
    };
    let (source, target) = (name("s"), name("t"));
    for table in [&source, &target] {
        driver
            .exec(&format!("CREATE TABLE {table} (a INT)"))
            .await
            .unwrap();
    }
    driver
        .exec(&format!("INSERT INTO {source} (a) VALUES (1)"))
        .await
        .unwrap();
    let frame = driver
        .read_frame(&format!("INSERT INTO {target} (a) SELECT a FROM {source}"))
        .await
        .unwrap_or_else(|e| panic!("INSERT ... SELECT through read: {e}"));
    // DuckDB answers DML with a `Count` result set; the others return no result set.
    if c.flavor != "duckdb" {
        assert!(frame.names().is_empty(), "{:?}", frame.names());
    }
    let rows = driver
        .read_frame(&format!("SELECT COUNT(*) AS n FROM {target}"))
        .await
        .unwrap();
    assert_eq!(
        rows.data("n").unwrap().values()[0]
            .py_str()
            .trim_end_matches(".0"),
        "1",
        "the INSERT ... SELECT was not committed"
    );
    for table in [&source, &target] {
        driver.exec(&format!("DROP TABLE {table}")).await.unwrap();
    }
});
