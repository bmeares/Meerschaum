//! In-place syncs of columns whose type cannot take v4's NULL stand-in (`get_null_replacement`):
//! enums, ranges, geometric types, intervals, arrays, `inet`, `time`, `xml` and `json` on the
//! PostgreSQL family; spatial and `INET6` columns on MariaDB; `xml`, spatial, `hierarchyid` and
//! `text` columns on SQL Server; intervals, lists, structs and maps on DuckDB. Such columns are
//! compared NULL-safely without the stand-in, as value and as key columns, on the plain
//! (insert / update) path and in the change-observation query of an observed upsert. Table
//! prefix `w5sql_`.
mod common;

use common::*;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_connector_sql::instance::inplace::InplaceObserver;
use meerschaum_connector_sql::queries::metadef::Bound;

/// One column of the source table: its name, its declared type and the SQL of its value for
/// row `id` (`{id}` is replaced), and how to read it back as text to compare.
struct Col {
    name: &'static str,
    decl: &'static str,
    value: &'static str,
    text: &'static str,
}

const fn col(
    name: &'static str,
    decl: &'static str,
    value: &'static str,
    text: &'static str,
) -> Col {
    Col {
        name,
        decl,
        value,
        text,
    }
}

/// The columns under test on `flavor` (`None`: nothing to test there). `{c}` in `text` is the
/// column, `{mood}` in `decl` the test's enum type.
fn columns(flavor: &str) -> Option<(Vec<Col>, &'static str, &'static str)> {
    let pg = || {
        vec![
            col("m", "{mood}", "'happy'", "CAST({c} AS TEXT)"),
            col("r", "int4range", "int4range({id}, 10)", "CAST({c} AS TEXT)"),
            col(
                "tr",
                "tstzrange",
                "tstzrange('2024-01-01', '2024-01-02')",
                "CAST({c} AS TEXT)",
            ),
            col("pt", "point", "point({id}, 2)", "CAST({c} AS TEXT)"),
            col("iv", "interval", "interval '1 day'", "CAST({c} AS TEXT)"),
            col("arr", "integer[]", "ARRAY[{id}, 2]", "CAST({c} AS TEXT)"),
            col("ip", "inet", "'10.0.0.{id}'", "CAST({c} AS TEXT)"),
            col("tm", "time", "time '01:02:03'", "CAST({c} AS TEXT)"),
            col("x", "xml", "'<a>{id}</a>'", "CAST({c} AS TEXT)"),
            col("js", "json", "'{\"a\": {id}}'", "CAST({c} AS TEXT)"),
            col("bt", "varbit", "B'101'", "CAST({c} AS TEXT)"),
            col("v", "integer", "{id}", "CAST({c} AS TEXT)"),
        ]
    };
    match flavor {
        "postgresql" | "timescaledb" | "postgis" | "timescaledb-ha" => {
            Some((pg(), "timestamptz", "IS NOT DISTINCT FROM"))
        }
        "mariadb" => Some((
            vec![
                col("pt", "POINT", "POINT({id}, 2)", "ST_AsText({c})"),
                col(
                    "ls",
                    "LINESTRING",
                    "LINESTRING(POINT(0, 0), POINT({id}, 1))",
                    "ST_AsText({c})",
                ),
                col("ip", "INET6", "'::{id}'", "CAST({c} AS CHAR)"),
                col("v", "INT", "{id}", "CAST({c} AS CHAR)"),
            ],
            "DATETIME",
            "<=>",
        )),
        "mssql" => Some((
            vec![
                col("x", "XML", "'<a>{id}</a>'", "CAST({c} AS NVARCHAR(MAX))"),
                col(
                    "g",
                    "GEOGRAPHY",
                    "geography::Point({id}, 2, 4326)",
                    "{c}.STAsText()",
                ),
                col("h", "HIERARCHYID", "'/{id}/'", "{c}.ToString()"),
                col("t", "TEXT", "'t{id}'", "CAST({c} AS NVARCHAR(MAX))"),
                col("v", "INT", "{id}", "CAST({c} AS NVARCHAR(MAX))"),
            ],
            "DATETIME2",
            "=",
        )),
        "duckdb" => Some((
            vec![
                col("iv", "INTERVAL", "INTERVAL 1 DAY", "CAST({c} AS VARCHAR)"),
                col("lst", "INTEGER[]", "[{id}, 2]", "CAST({c} AS VARCHAR)"),
                col(
                    "st",
                    "STRUCT(a INTEGER)",
                    "{'a': {id}}",
                    "CAST({c} AS VARCHAR)",
                ),
                col(
                    "mp",
                    "MAP(VARCHAR, INTEGER)",
                    "MAP {'k': {id}}",
                    "CAST({c} AS VARCHAR)",
                ),
                col("v", "INTEGER", "{id}", "CAST({c} AS VARCHAR)"),
            ],
            "TIMESTAMP",
            "IS NOT DISTINCT FROM",
        )),
        _ => None,
    }
}

async fn exec(c: &Ctx, sql: &str) {
    assert!(c.conn.exec(sql).await.is_some(), "{}: {sql}", c.flavor);
}

/// `{c}` of `text` for column `name` of alias `alias`.
fn text_of(column: &Col, alias: &str) -> String {
    column
        .text
        .replace("{c}", &format!("{alias}.{}", column.name))
}

/// The number of source rows without an identical target row, plus the difference in row
/// counts: 0 when the target holds exactly the source's rows.
async fn differences(c: &Ctx, cols: &[Col], eq: &str, source: &str, target: &str) -> i64 {
    let same = cols
        .iter()
        .chain(std::iter::once(&col("id", "", "", "{c}")))
        .map(|column| {
            let (s, t) = (text_of(column, "s"), text_of(column, "t"));
            if eq == "=" {
                format!("({s} = {t} OR ({s} IS NULL AND {t} IS NULL))")
            } else {
                format!("{s} {eq} {t}")
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT (SELECT COUNT(*) FROM {source} s WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {same})) \
         + (SELECT COUNT(*) FROM {target}) - (SELECT COUNT(*) FROM {source})"
    );
    let value = c.conn.value(&sql).await;
    match &value {
        Some(SqlValue::Decimal(d)) => d.to_string().parse().ok(),
        Some(v) => v.as_i64(),
        None => None,
    }
    .unwrap_or_else(|| panic!("{}: {sql}: {value:?}", c.flavor))
}

#[derive(Default)]
struct Observed(std::sync::Mutex<usize>);

#[meerschaum_connector::async_trait]
impl InplaceObserver for Observed {
    async fn before_merge(&self, delta: &meerschaum_kernel::Frame) -> Result<(), String> {
        *self.0.lock().unwrap() += delta.height();
        Ok(())
    }
}

/// Sync `pipe` in place (observed, as a pipe with children is, when `observe`).
async fn sync(c: &Ctx, pipe: &meerschaum_core::Pipe, observe: bool) -> String {
    c.conn.enable_query_log();
    let result = if observe {
        let observed = Observed::default();
        c.conn
            .sync_pipe_inplace_observed(pipe, None, Bound::None, Bound::None, true, &observed)
            .await
    } else {
        c.conn
            .sync_pipe_inplace(pipe, None, Bound::None, Bound::None, true)
            .await
    };
    let log = c.conn.take_query_log();
    assert!(
        result.success,
        "{}: {}\n{}",
        c.flavor,
        result.message,
        log.join("\n;;\n")
    );
    result.message
}

/// Create the source table (rows 1 to 3, row 3 all NULL but its keys) and return its name and
/// columns.
async fn source(c: &Ctx) -> Option<(String, Vec<Col>, &'static str)> {
    let (cols, ts_type, eq) = columns(c.flavor)?;
    let table = format!("{}_src", c.prefix);
    let mood = format!("{}_mood", c.prefix);
    if c.is_pg() || c.flavor == "timescaledb-ha" {
        exec(
            c,
            &format!("CREATE TYPE \"{mood}\" AS ENUM ('happy', 'sad')"),
        )
        .await;
    }
    let decls = cols
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                column.name,
                column.decl.replace("{mood}", &format!("\"{mood}\""))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    exec(
        c,
        &format!("CREATE TABLE {table} (ts {ts_type} NOT NULL, id BIGINT NOT NULL, {decls})"),
    )
    .await;
    for id in 1..=3 {
        let values = cols
            .iter()
            .map(|column| {
                if id == 3 {
                    "NULL".to_string()
                } else {
                    column.value.replace("{id}", &id.to_string())
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        exec(
            c,
            &format!("INSERT INTO {table} VALUES ('2024-01-01 00:00:00', {id}, {values})"),
        )
        .await;
    }
    Some((table, cols, eq))
}

async fn drop_source(c: &Ctx, table: &str) {
    let _ = c.conn.exec(&format!("DROP TABLE {table}")).await;
    if c.is_pg() || c.flavor == "timescaledb-ha" {
        let _ = c
            .conn
            .exec(&format!("DROP TYPE \"{}_mood\" CASCADE", c.prefix))
            .await;
    }
}

/// Sync the source in place three times (create, nothing changed, rows changed) and check the
/// target holds the source's rows each time.
async fn steps(c: &Ctx, params: serde_json::Value, upsert: bool, observe: bool, key: Option<&str>) {
    let Some((table, cols, eq)) = source(c).await else {
        return;
    };
    let names = cols
        .iter()
        .map(|col| col.name)
        .collect::<Vec<_>>()
        .join(", ");
    let mut params = params;
    params["sql"] = serde_json::Value::String(format!("SELECT ts, id, {names} FROM {table}"));
    params["upsert"] = serde_json::Value::Bool(upsert);
    if matches!(c.flavor, "mariadb" | "mssql") {
        // The update casts each value to its column's catalog type, which fails for these
        // types (`CAST(... AS NULL)` on MariaDB, `CAST(... AS XML(MAX))` on SQL Server)
        // whatever the comparison: assign the values uncast.
        params["enforce"] = serde_json::Value::Bool(false);
    }
    let pipe = c.pipe(&c.conn.keys(), "nullrep", None, &params.to_string());
    let target = c.conn.pipe_table_name(&c.conn.sql_pipe(&pipe).unwrap());
    sync(c, &pipe, observe).await;
    assert_eq!(
        differences(c, &cols, eq, &table, &target).await,
        0,
        "{}: created",
        c.flavor
    );

    let message = sync(c, &pipe, observe).await;
    if !upsert {
        assert_eq!(message, "Inserted 0, updated 0 rows.", "{}", c.flavor);
    }
    assert_eq!(
        differences(c, &cols, eq, &table, &target).await,
        0,
        "{}: unchanged",
        c.flavor
    );

    // Row 1: values to NULL. Row 3: NULLs to row 2's values. Row 4: new, all NULL.
    let set_null = cols
        .iter()
        .filter(|col| col.name != "v" && Some(col.name) != key)
        .map(|col| format!("{} = NULL", col.name))
        .collect::<Vec<_>>()
        .join(", ");
    let set_values = cols
        .iter()
        .filter(|col| Some(col.name) != key)
        .map(|col| format!("{} = {}", col.name, col.value.replace("{id}", "2")))
        .collect::<Vec<_>>()
        .join(", ");
    let nulls = cols.iter().map(|_| "NULL").collect::<Vec<_>>().join(", ");
    exec(c, &format!("UPDATE {table} SET {set_null} WHERE id = 1")).await;
    exec(c, &format!("UPDATE {table} SET {set_values} WHERE id = 3")).await;
    exec(
        c,
        &format!("INSERT INTO {table} VALUES ('2024-01-01 00:00:00', 4, {nulls})"),
    )
    .await;
    let message = sync(c, &pipe, observe).await;
    if !upsert {
        assert_eq!(message, "Inserted 1, updated 2 rows.", "{}", c.flavor);
    }
    assert_eq!(
        differences(c, &cols, eq, &table, &target).await,
        0,
        "{}: changed",
        c.flavor
    );
    c.delete(&pipe).await;
    drop_source(c, &table).await;
}

fn keyed_by_id() -> serde_json::Value {
    serde_json::json!({"columns": {"datetime": "ts", "id": "id"}})
}

flavor_tests!(
    plain_inplace_sync_compares_without_the_stand_in,
    |c: Ctx| async move {
        steps(&c, keyed_by_id(), false, false, None).await;
    }
);

flavor_tests!(
    observed_inplace_upsert_compares_without_the_stand_in,
    |c: Ctx| async move {
        steps(&c, keyed_by_id(), true, true, None).await;
    }
);

flavor_tests!(
    observed_plain_inplace_sync_compares_without_the_stand_in,
    |c: Ctx| async move {
        steps(&c, keyed_by_id(), false, true, None).await;
    }
);

// A key column of such a type (NULL in rows 3 and 4): the rows are matched by it NULL-safely.
flavor_tests!(
    plain_inplace_sync_matches_such_key_columns,
    |c: Ctx| async move {
        let key = match c.flavor {
            "postgresql" | "timescaledb" | "postgis" => "m",
            "mariadb" => "ip",
            "duckdb" => "iv",
            _ => return,
        };
        let params = serde_json::json!({"columns": {"datetime": "ts", "id": "id", "k": key}});
        steps(&c, params, false, false, Some(key)).await;
        let params = serde_json::json!({"columns": {"datetime": "ts", "id": "id", "k": key}});
        steps(&c, params, true, true, Some(key)).await;
    }
);
