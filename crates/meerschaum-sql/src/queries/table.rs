//! Table-level query text: `CREATE TABLE` (from dtypes or a query), rename, drop, schema creation,
//! column/index introspection, version, auto-increment reset (`meerschaum/utils/sql.py`).

use indexmap::IndexMap;

use crate::dtypes::get_db_type_from_pd_type;
use crate::error::{Result, SqlError};
use crate::flavor::Flavor;
use crate::ident::{clean, sql_item_name, truncate_item_name};
use crate::pytext::{dedent_strip, indent, py_format, py_lstrip, py_rstrip};
use crate::queries::cte::{char_slice, find_top_level_select_index, get_leading_with_index};
use crate::queries::templates::{
    COLUMNS_INDICES_QUERIES, COLUMNS_TYPES_QUERIES, RESET_AUTOINCREMENT_QUERIES, lookup,
};

/// The first argument of `get_create_table_queries`: a `SELECT` or a `dtypes` dict.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOrDtypes {
    /// A `SELECT` (possibly with CTEs) whose result becomes the new table.
    Query(String),
    /// Column → Pandas/Meerschaum dtype (or DB type with `parse_dtypes = false`).
    Dtypes(IndexMap<String, String>),
}

/// Keyword arguments of `get_create_table_queries` (`utils/sql.py:2188`).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableOptions {
    /// `schema`.
    pub schema: Option<String>,
    /// `primary_key`.
    pub primary_key: Option<String>,
    /// `primary_key_db_type`.
    pub primary_key_db_type: Option<String>,
    /// `autoincrement`.
    pub autoincrement: bool,
    /// `datetime_column`.
    pub datetime_column: Option<String>,
    /// `hypertable_chunk_interval` (e.g. `'43200 minutes'` or `'100000'`).
    pub hypertable_chunk_interval: Option<String>,
    /// `hypertable_segmentby` (already quoted column refs).
    pub hypertable_segmentby: Option<Vec<String>>,
    /// `hypertable_orderby` (already quoted column refs, optional direction).
    pub hypertable_orderby: Option<Vec<String>>,
    /// `partition_by_column`.
    pub partition_by_column: Option<String>,
    /// `partition_bounds`: `(partition_name, upper_bound_literal)` (MySQL/MariaDB).
    pub partition_bounds: Option<Vec<(String, String)>>,
    /// `partition_scheme_name` (MSSQL).
    pub partition_scheme_name: Option<String>,
    /// `_parse_dtypes`.
    pub parse_dtypes: bool,
}

impl Default for CreateTableOptions {
    fn default() -> Self {
        CreateTableOptions {
            schema: None,
            primary_key: None,
            primary_key_db_type: None,
            autoincrement: false,
            datetime_column: None,
            hypertable_chunk_interval: None,
            hypertable_segmentby: None,
            hypertable_orderby: None,
            partition_by_column: None,
            partition_bounds: None,
            partition_scheme_name: None,
            parse_dtypes: true,
        }
    }
}

fn non_empty(s: &Option<String>) -> Option<&str> {
    s.as_deref().filter(|s| !s.is_empty())
}

/// `get_create_table_queries(query_or_dtypes, new_table, flavor, ...)`.
pub fn get_create_table_queries(
    query_or_dtypes: &QueryOrDtypes,
    new_table: &str,
    flavor: Flavor,
    options: &CreateTableOptions,
) -> Result<Vec<String>> {
    let mut opts = options.clone();
    opts.autoincrement = opts.autoincrement && !flavor.skip_auto_increment();
    match query_or_dtypes {
        QueryOrDtypes::Query(q) => get_create_table_query_from_cte(q, new_table, flavor, &opts),
        QueryOrDtypes::Dtypes(d) => get_create_table_query_from_dtypes(d, new_table, flavor, &opts),
    }
}

/// `get_create_table_query(...)` (deprecated in Python): the first query only.
pub fn get_create_table_query(
    query_or_dtypes: &QueryOrDtypes,
    new_table: &str,
    flavor: Flavor,
    schema: Option<&str>,
) -> Result<String> {
    let opts = CreateTableOptions {
        schema: schema.map(str::to_string),
        ..Default::default()
    };
    Ok(get_create_table_queries(query_or_dtypes, new_table, flavor, &opts)?.remove(0))
}

/// `_get_create_table_query_from_dtypes` (`utils/sql.py:2297`).
pub fn get_create_table_query_from_dtypes(
    dtypes: &IndexMap<String, String>,
    new_table: &str,
    flavor: Flavor,
    opts: &CreateTableOptions,
) -> Result<Vec<String>> {
    let primary_key = non_empty(&opts.primary_key);
    if dtypes.is_empty() && primary_key.is_none() {
        return Err(SqlError::InvalidArgument(format!(
            "Expecting columns for table '{new_table}'."
        )));
    }
    let autoincrement = opts.autoincrement && !flavor.skip_auto_increment();

    let mut cols_types: Vec<(String, String)> = Vec::new();
    if let Some(pk) = primary_key {
        let typ = dtypes.get(pk).filter(|t| !t.is_empty()).cloned();
        let db_type = if opts.parse_dtypes {
            get_db_type_from_pd_type(typ.as_deref().unwrap_or("int"), Some(flavor))
        } else {
            typ.unwrap_or_else(|| "INT".to_string())
        };
        cols_types.push((pk.to_string(), db_type));
    }
    for (col, typ) in dtypes {
        if Some(col.as_str()) == primary_key {
            continue;
        }
        let db_type = if opts.parse_dtypes {
            get_db_type_from_pd_type(typ, Some(flavor))
        } else {
            typ.clone()
        };
        cols_types.push((col.clone(), db_type));
    }

    let table_name = sql_item_name(new_table, flavor, opts.schema.as_deref());
    let primary_key_name = primary_key.map(|pk| sql_item_name(pk, flavor, None));
    let primary_key_constraint_name =
        primary_key.map(|_| sql_item_name(&format!("PK_{new_table}"), flavor, None));
    let datetime_column = non_empty(&opts.datetime_column);
    let datetime_column_name = datetime_column.map(|dt| sql_item_name(dt, flavor, None));

    let partition_by_column = opts.partition_by_column.as_deref();
    let is_native_partitioned = partition_by_column.is_some() && flavor.native_partition();
    let partition_by_column_name = match partition_by_column {
        Some(col) if flavor.native_partition() || flavor == Flavor::Mssql => {
            Some(sql_item_name(col, flavor, None))
        }
        _ => None,
    };
    let is_mssql_partitioned = flavor == Flavor::Mssql
        && opts.partition_scheme_name.is_some()
        && partition_by_column.is_some();
    let partition_scheme_item = if is_mssql_partitioned {
        opts.partition_scheme_name
            .as_deref()
            .map(|s| sql_item_name(s, flavor, None))
    } else {
        None
    };
    let primary_key_clustered = if datetime_column.is_none() || datetime_column == primary_key {
        "CLUSTERED"
    } else {
        "NONCLUSTERED"
    };
    let pk_name = primary_key_name.clone().unwrap_or_default();
    let part_name = partition_by_column_name.clone().unwrap_or_default();
    let scheme_item = partition_scheme_item.clone().unwrap_or_default();

    let mut query = format!("CREATE TABLE {table_name} (");
    if let Some(pk) = primary_key {
        let col_db_type = &cols_types[0].1;
        let auto_increment_str = if autoincrement || !dtypes.contains_key(pk) {
            format!(" {}", flavor.auto_increment_column())
        } else {
            String::new()
        };
        let col_name = sql_item_name(pk, flavor, None);
        if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            let typ = if auto_increment_str.is_empty() {
                col_db_type.as_str()
            } else {
                "INTEGER"
            };
            query.push_str(&format!(
                "\n    {col_name} {typ} PRIMARY KEY{auto_increment_str} NOT NULL,"
            ));
        } else if flavor == Flavor::Oracle {
            query.push_str(&format!(
                "\n    {col_name} {col_db_type} {auto_increment_str} PRIMARY KEY,"
            ));
        } else if (flavor.is_timescaledb()
            && datetime_column.is_some()
            && datetime_column != Some(pk))
            || (is_native_partitioned && partition_by_column != Some(pk))
            || flavor == Flavor::Mssql
        {
            // TimescaleDB and natively partitioned tables defer the PK to a composite
            // `PRIMARY KEY(...)` below; MSSQL declares a named constraint below.
            query.push_str(&format!(
                "\n    {col_name} {col_db_type}{auto_increment_str} NOT NULL,"
            ));
        } else {
            query.push_str(&format!(
                "\n    {col_name} {col_db_type} PRIMARY KEY{auto_increment_str} NOT NULL,"
            ));
        }
    }

    for (col, db_type) in &cols_types {
        if Some(col.as_str()) == primary_key {
            continue;
        }
        let col_name = sql_item_name(col, flavor, None);
        query.push_str(&format!("\n    {col_name} {db_type},"));
    }
    if flavor.is_timescaledb()
        && datetime_column.is_some()
        && primary_key.is_some()
        && datetime_column != primary_key
    {
        query.push_str(&format!(
            "\n    PRIMARY KEY({}, {pk_name}),",
            datetime_column_name.clone().unwrap_or_default()
        ));
    }

    if is_native_partitioned && primary_key.is_some() && partition_by_column != primary_key {
        // MySQL requires an `AUTO_INCREMENT` column to lead a key; PostgreSQL is order-insensitive.
        if flavor.mysql_partition() {
            query.push_str(&format!("\n    PRIMARY KEY({pk_name}, {part_name}),"));
        } else {
            query.push_str(&format!("\n    PRIMARY KEY({part_name}, {pk_name}),"));
        }
    }

    if flavor == Flavor::Mssql && primary_key.is_some() {
        let constraint = primary_key_constraint_name.clone().unwrap_or_default();
        if is_mssql_partitioned && partition_by_column != primary_key {
            query.push_str(&format!(
                "\n    CONSTRAINT {constraint} PRIMARY KEY CLUSTERED ({part_name}, {pk_name}) ON {scheme_item}({part_name}),"
            ));
        } else {
            query.push_str(&format!(
                "\n    CONSTRAINT {constraint} PRIMARY KEY {primary_key_clustered} ({pk_name}),"
            ));
        }
    }

    // `query = query[:-1]`
    query.pop();
    query.push_str("\n)");

    if is_mssql_partitioned && primary_key.is_none() {
        query.push_str(&format!("\nON {scheme_item}({part_name})"));
    }

    if is_native_partitioned && flavor.pg_partition() {
        query.push_str(&format!("\nPARTITION BY RANGE ({part_name})"));
    } else if is_native_partitioned && flavor.mysql_partition() {
        let bounds = opts.partition_bounds.clone().unwrap_or_default();
        let partition_defs = bounds
            .iter()
            .map(|(name, hi)| {
                format!(
                    "    PARTITION {} VALUES LESS THAN ({hi})",
                    sql_item_name(name, flavor, None)
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        query.push_str(&format!(
            "\nPARTITION BY RANGE COLUMNS ({part_name}) (\n{partition_defs}\n)"
        ));
    }

    if let (Some(chunk_interval), true, Some(dt)) = (
        opts.hypertable_chunk_interval.as_deref(),
        flavor.is_timescaledb(),
        datetime_column,
    ) {
        let partition_column = dt.replace('\'', "''");
        let chunk_interval = chunk_interval.replace('\'', "''");
        let mut with_options = vec![
            "tsdb.hypertable".to_string(),
            format!("tsdb.partition_column='{partition_column}'"),
            format!("tsdb.chunk_interval='{chunk_interval}'"),
        ];
        if let Some(seg) = opts.hypertable_segmentby.as_ref().filter(|v| !v.is_empty()) {
            with_options.push(format!(
                "tsdb.segmentby='{}'",
                seg.join(", ").replace('\'', "''")
            ));
        }
        if let Some(ord) = opts.hypertable_orderby.as_ref().filter(|v| !v.is_empty()) {
            with_options.push(format!(
                "tsdb.orderby='{}'",
                ord.join(", ").replace('\'', "''")
            ));
        }
        query.push_str(&format!(
            "\nWITH (\n    {}\n)",
            with_options.join(",\n    ")
        ));
    }

    Ok(vec![query])
}

/// `_get_create_table_query_from_cte` (`utils/sql.py:2502`).
///
/// The hypertable/partition options are ignored here, as in Python.
pub fn get_create_table_query_from_cte(
    query: &str,
    new_table: &str,
    flavor: Flavor,
    opts: &CreateTableOptions,
) -> Result<Vec<String>> {
    let create_cte = "create_query";
    let create_cte_name = sql_item_name(create_cte, flavor, None);
    let new_table_name = sql_item_name(new_table, flavor, opts.schema.as_deref());
    let primary_key = non_empty(&opts.primary_key);
    let primary_key_constraint_name =
        primary_key.map(|_| sql_item_name(&format!("PK_{new_table}"), flavor, None));
    // PARITY: Python formats `None` into the query text when there is no primary key; those
    // queries are discarded below, so `"None"` never escapes.
    let primary_key_name = primary_key
        .map(|pk| sql_item_name(pk, flavor, None))
        .unwrap_or_else(|| "None".to_string());
    let datetime_column = non_empty(&opts.datetime_column);
    let primary_key_clustered = if datetime_column.is_none() || datetime_column == primary_key {
        "CLUSTERED"
    } else {
        "NONCLUSTERED"
    };
    let datetime_column_name = datetime_column
        .map(|dt| sql_item_name(dt, flavor, None))
        .unwrap_or_else(|| "None".to_string());
    let query = py_lstrip(query);
    let is_with_query = get_leading_with_index(query).is_some();

    let (create_table_queries, alter_type_queries): (Vec<String>, Vec<String>) = match flavor {
        Flavor::Mssql => {
            let create = if is_with_query {
                let final_select_ix = find_top_level_select_index(query, true)
                    .ok_or_else(|| SqlError::NoTopLevelSelect(query.to_string()))?;
                let head = char_slice(query, 0, Some(final_select_ix));
                let tail = char_slice(query, final_select_ix, None);
                format!(
                    "{},\n{create_cte_name} AS (\n{}\n)\nSELECT *\nINTO {new_table_name}\nFROM {create_cte_name}",
                    py_rstrip(&head),
                    indent(&tail, "    ")
                )
            } else {
                format!(
                    "SELECT *\nINTO {new_table_name}\nFROM (\n{}\n) AS {create_cte_name}",
                    indent(query, "    ")
                )
            };
            let mut alters = Vec::new();
            if let Some(pk_type) = non_empty(&opts.primary_key_db_type) {
                alters.push(format!(
                    "ALTER TABLE {new_table_name}\nALTER COLUMN {primary_key_name} {pk_type} NOT NULL"
                ));
            }
            alters.push(format!(
                "ALTER TABLE {new_table_name}\nADD CONSTRAINT {} PRIMARY KEY {primary_key_clustered} ({primary_key_name})",
                primary_key_constraint_name.unwrap_or_else(|| "None".to_string())
            ));
            (vec![create], alters)
        }
        Flavor::Sqlite | Flavor::Geopackage => {
            let create = if is_with_query {
                format!("CREATE TABLE {new_table_name} AS\n{query}")
            } else {
                format!(
                    "CREATE TABLE {new_table_name} AS\nSELECT *\nFROM (\n{}\n)",
                    indent(query, "    ")
                )
            };
            // SQLite does not support ALTER TABLE ... ADD PRIMARY KEY.
            (vec![create], Vec::new())
        }
        _ => {
            let create = if is_with_query {
                format!("CREATE TABLE {new_table_name} AS\n{query}")
            } else {
                format!(
                    "CREATE TABLE {new_table_name} AS\nSELECT *\nFROM (\n{}\n){}",
                    indent(query, "    "),
                    if flavor != Flavor::Oracle {
                        format!(" AS {create_cte_name}")
                    } else {
                        String::new()
                    }
                )
            };
            let pk_cols_str = if flavor.is_timescaledb()
                && datetime_column.is_some()
                && datetime_column != primary_key
            {
                format!("{datetime_column_name}, {primary_key_name}")
            } else {
                primary_key_name.clone()
            };
            (
                vec![create],
                vec![format!(
                    "ALTER TABLE {new_table_name}\nADD PRIMARY KEY ({pk_cols_str})"
                )],
            )
        }
    };

    if primary_key.is_none() {
        return Ok(create_table_queries);
    }
    Ok(create_table_queries
        .into_iter()
        .chain(alter_type_queries)
        .collect())
}

/// `get_rename_table_queries(old_table, new_table, flavor, schema, new_schema)`.
pub fn get_rename_table_queries(
    old_table: &str,
    new_table: &str,
    flavor: Flavor,
    schema: Option<&str>,
    new_schema: Option<&str>,
) -> Result<Vec<String>> {
    let mut schema = schema.map(str::to_string);
    let mut new_schema = new_schema.map(str::to_string).or_else(|| schema.clone());
    if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
        schema = None;
        new_schema = None;
    }

    let mut old_table_name = sql_item_name(old_table, flavor, schema.as_deref());
    let new_table_name = sql_item_name(new_table, flavor, None);

    if flavor == Flavor::Mssql {
        let mut queries = Vec::new();
        if schema != new_schema {
            // PARITY: `sql_item_name(None, ...)` renders `[None]` (`utils/sql.py:2104`).
            let target = new_schema.clone().unwrap_or_else(|| "None".to_string());
            queries.push(format!(
                "ALTER SCHEMA {} TRANSFER {old_table_name}",
                sql_item_name(&target, flavor, None)
            ));
            schema = new_schema.clone();
            old_table_name = sql_item_name(old_table, flavor, schema.as_deref());
        }
        if old_table != new_table {
            queries.push(format!("EXEC sp_rename '{old_table_name}', '{new_table}'"));
        }
        return Ok(queries);
    }

    if flavor.mysql_partition() {
        let new_table_qualified = sql_item_name(new_table, flavor, new_schema.as_deref());
        return Ok(vec![format!(
            "RENAME TABLE {old_table_name} TO {new_table_qualified}"
        )]);
    }

    let if_exists_str = if flavor.drop_if_exists() {
        "IF EXISTS"
    } else {
        ""
    };
    if flavor == Flavor::Duckdb {
        let tmp_table = format!("_tmp_rename_{new_table}");
        let tmp_table_name = sql_item_name(&tmp_table, flavor, schema.as_deref());
        let mut queries = get_create_table_queries(
            &QueryOrDtypes::Query(format!("SELECT * FROM {old_table_name}")),
            &tmp_table,
            Flavor::Duckdb,
            &CreateTableOptions {
                schema: schema.clone(),
                ..Default::default()
            },
        )?;
        queries.extend(get_create_table_queries(
            &QueryOrDtypes::Query(format!("SELECT * FROM {tmp_table_name}")),
            new_table,
            Flavor::Duckdb,
            &CreateTableOptions {
                schema: new_schema.clone(),
                ..Default::default()
            },
        )?);
        queries.push(format!("DROP TABLE {if_exists_str} {tmp_table_name}"));
        queries.push(format!("DROP TABLE {if_exists_str} {old_table_name}"));
        return Ok(queries);
    }

    let mut queries = Vec::new();
    if schema != new_schema
        && matches!(
            flavor,
            Flavor::Postgresql
                | Flavor::Postgis
                | Flavor::Timescaledb
                | Flavor::TimescaledbHa
                | Flavor::Citus
                | Flavor::Cockroachdb
        )
    {
        let target = new_schema.clone().unwrap_or_else(|| "None".to_string());
        queries.push(format!(
            "ALTER TABLE {old_table_name} SET SCHEMA {}",
            sql_item_name(&target, flavor, None)
        ));
        schema = new_schema.clone();
        old_table_name = sql_item_name(old_table, flavor, schema.as_deref());
    }
    if old_table != new_table {
        queries.push(format!(
            "ALTER TABLE {old_table_name} RENAME TO {new_table_name}"
        ));
    }
    Ok(queries)
}

/// The `DROP TABLE` issued by `SQLConnector.drop_pipe` (`_pipes.py:3250`).
///
/// Python has no `get_drop_table_queries`; this reproduces `drop_pipe`'s statement
/// (`f"DROP TABLE {if_exists_str} {target_name}"`, so flavors without `IF EXISTS` get two
/// spaces) followed by the MSSQL partition cleanup (see
/// [`crate::queries::partition::get_partition_cleanup_queries`]), which the caller appends.
pub fn get_drop_table_queries(table: &str, flavor: Flavor, schema: Option<&str>) -> Vec<String> {
    let if_exists_str = if flavor.drop_if_exists() {
        "IF EXISTS"
    } else {
        ""
    };
    vec![format!(
        "DROP TABLE {if_exists_str} {}",
        sql_item_name(table, flavor, schema)
    )]
}

/// The `DROP TABLE` used for temporary tables (`_instance.py:73`): `DROP TABLE {if_exists} name`.
pub fn get_drop_temporary_table_query(table: &str, flavor: Flavor, schema: Option<&str>) -> String {
    let if_exists = if flavor.drop_if_exists() {
        "IF EXISTS"
    } else {
        ""
    };
    format!(
        "DROP TABLE {if_exists} {}",
        sql_item_name(table, flavor, schema)
    )
}

/// The `DROP TABLE` used by `get_alter_columns_queries` and `to_sql` (`_pipes.py:3837`,
/// `_sql.py:1024`).
///
/// PARITY: no space between `IF EXISTS` and the name (`f"DROP TABLE {if_exists_str}" + name`).
pub fn get_drop_table_query_unspaced(table: &str, flavor: Flavor, schema: Option<&str>) -> String {
    let if_exists = if flavor.drop_if_exists() {
        "IF EXISTS"
    } else {
        ""
    };
    format!(
        "DROP TABLE {if_exists}{}",
        sql_item_name(table, flavor, schema)
    )
}

/// `get_create_schema_if_not_exists_queries(schema, flavor)` (`utils/sql.py:3153`).
pub fn get_create_schema_if_not_exists_queries(
    schema: Option<&str>,
    flavor: Flavor,
) -> Result<Vec<String>> {
    let Some(schema) = schema.filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    if flavor.no_schema() {
        return Ok(Vec::new());
    }
    if Some(schema) == flavor.default_schema() {
        return Ok(Vec::new());
    }
    clean(schema)?;
    if flavor == Flavor::Mssql {
        return Ok(vec![format!(
            "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')\nBEGIN\n    EXEC('CREATE SCHEMA {}');\nEND;",
            sql_item_name(schema, flavor, None)
        )]);
    }
    if flavor == Flavor::Oracle {
        return Ok(Vec::new());
    }
    Ok(vec![format!(
        "CREATE SCHEMA IF NOT EXISTS {};",
        sql_item_name(schema, flavor, None)
    )])
}

/// The schema `get_table_cols_types` / `get_table_cols_indices` filter on.
pub fn introspection_schema(flavor: Flavor, schema: Option<&str>) -> Option<String> {
    let schema = if flavor.no_schema() { None } else { schema };
    schema
        .map(str::to_string)
        .or_else(|| flavor.default_schema().map(str::to_string))
}

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

fn introspection_args(table: &str, flavor: Flavor) -> [(String, String); 7] {
    let table_trunc = truncate_item_name(table, flavor);
    let table_lower = table.to_lowercase();
    let table_upper = table.to_uppercase();
    let table_lower_trunc = truncate_item_name(&table_lower, flavor);
    let table_upper_trunc = truncate_item_name(&table_upper, flavor);
    let db_prefix = if flavor == Flavor::Mssql && table.starts_with('#') {
        "tempdb."
    } else {
        ""
    };
    [
        ("table".into(), esc(table)),
        ("table_trunc".into(), esc(&table_trunc)),
        ("table_lower".into(), esc(&table_lower)),
        ("table_lower_trunc".into(), esc(&table_lower_trunc)),
        ("table_upper".into(), esc(&table_upper)),
        ("table_upper_trunc".into(), esc(&table_upper_trunc)),
        ("db_prefix".into(), db_prefix.into()),
    ]
}

/// The query `get_table_cols_types` executes (`utils/sql.py:1397`).
pub fn get_table_cols_types_query(table: &str, flavor: Flavor) -> String {
    let args = introspection_args(table, flavor);
    let args_ref: Vec<(&str, &str)> = args.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    dedent_strip(&py_format(
        lookup(COLUMNS_TYPES_QUERIES, flavor.as_str()),
        &args_ref,
    ))
}

/// The query `get_table_cols_indices` executes (`utils/sql.py:1579`).
///
/// `schema` is the already-resolved [`introspection_schema`]; `None` renders as `None` like
/// Python's `str.format`.
pub fn get_table_cols_indices_query(table: &str, flavor: Flavor, schema: Option<&str>) -> String {
    let mut args = introspection_args(table, flavor).to_vec();
    args.push((
        "schema".into(),
        schema.map(esc).unwrap_or_else(|| "None".to_string()),
    ));
    let args_ref: Vec<(&str, &str)> = args.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    dedent_strip(&py_format(
        lookup(COLUMNS_INDICES_QUERIES, flavor.as_str()),
        &args_ref,
    ))
}

/// One row of the `get_table_cols_types` query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnTypeRow {
    /// `database`.
    pub database: Option<String>,
    /// `schema`.
    pub schema: Option<String>,
    /// `table`.
    pub table: Option<String>,
    /// `column`.
    pub column: String,
    /// `type`.
    pub type_: String,
    /// `numeric_precision`.
    pub numeric_precision: Option<i64>,
    /// `numeric_scale`.
    pub numeric_scale: Option<i64>,
}

fn oracle_column_name(flavor: Flavor, col: &str) -> String {
    if flavor != Flavor::Oracle {
        return col.to_string();
    }
    let has_cased = col.chars().any(|c| c.is_lowercase() || c.is_uppercase());
    let is_upper = has_cased && !col.chars().any(char::is_lowercase);
    let alpha = {
        let stripped = col.replace('_', "");
        !stripped.is_empty() && stripped.chars().all(char::is_alphabetic)
    };
    if is_upper && alpha {
        col.to_lowercase()
    } else {
        col.to_string()
    }
}

fn filter_rows<'a, T>(
    rows: &'a [T],
    schema: Option<&str>,
    database: Option<&str>,
    get: impl Fn(&T) -> (Option<&str>, Option<&str>),
) -> Vec<&'a T> {
    let filtered: Vec<&T> = rows
        .iter()
        .filter(|r| {
            let (s, d) = get(r);
            (schema.is_none_or(str::is_empty) || s == schema)
                && (database.is_none_or(str::is_empty) || d == database)
        })
        .collect();
    // NOTE: This may return incorrect columns if the schema is not explicitly stated.
    if !rows.is_empty() && filtered.is_empty() {
        return rows.iter().collect();
    }
    filtered
}

/// Turn `get_table_cols_types` rows into `{column: TYPE}` (`utils/sql.py:1431-1497`).
///
/// `geometry_cols_types` is the result of the PostGIS lookup
/// ([`get_postgis_geo_columns_types_query`]) when any row has type `USER-DEFINED`.
pub fn cols_types_from_rows(
    rows: &[ColumnTypeRow],
    flavor: Flavor,
    schema: Option<&str>,
    database: Option<&str>,
    geometry_cols_types: &IndexMap<String, String>,
) -> IndexMap<String, String> {
    let schema = introspection_schema(flavor, schema);
    let database = if matches!(
        flavor,
        Flavor::Sqlite | Flavor::Duckdb | Flavor::Oracle | Flavor::Geopackage
    ) {
        None
    } else {
        database
    };
    let filtered = filter_rows(rows, schema.as_deref(), database, |r| {
        (r.schema.as_deref(), r.database.as_deref())
    });
    let mut out = IndexMap::new();
    for doc in filtered {
        let suffix = match (doc.numeric_precision, doc.numeric_scale) {
            (Some(p), Some(s)) if p != 0 && s != 0 => format!("({p},{s})"),
            _ => String::new(),
        };
        out.insert(
            oracle_column_name(flavor, &doc.column),
            format!("{}{}", doc.type_.to_uppercase(), suffix),
        );
    }
    for (k, v) in geometry_cols_types {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Whether any row needs the PostGIS geometry lookup (`type == 'USER-DEFINED'`).
pub fn needs_postgis_lookup(rows: &[ColumnTypeRow]) -> bool {
    rows.iter()
        .any(|r| r.type_.to_uppercase() == "USER-DEFINED")
}

/// One row of the `get_table_cols_indices` query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnIndexRow {
    /// `database`.
    pub database: Option<String>,
    /// `schema`.
    pub schema: Option<String>,
    /// `table`.
    pub table: Option<String>,
    /// `column`.
    pub column: String,
    /// `index`.
    pub index: Option<String>,
    /// `index_type`.
    pub index_type: Option<String>,
    /// `clustered` (MSSQL only).
    pub clustered: Option<bool>,
}

/// An entry of `get_table_cols_indices`' result: `{'name', 'type'[, 'clustered']}`.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexDoc {
    /// `name`.
    pub name: Option<String>,
    /// `type` (`'INDEX'`, `'PRIMARY KEY'`, `'UNIQUE INDEX'`).
    #[serde(rename = "type")]
    pub type_: Option<String>,
    /// `clustered` (MSSQL only).
    #[serde(default)]
    pub clustered: Option<bool>,
}

/// Turn `get_table_cols_indices` rows into `{column: [IndexDoc]}` (`utils/sql.py:1616-1661`).
pub fn cols_indices_from_rows(
    rows: &[ColumnIndexRow],
    flavor: Flavor,
    schema: Option<&str>,
    database: Option<&str>,
) -> IndexMap<String, Vec<IndexDoc>> {
    let schema = introspection_schema(flavor, schema);
    let database = if matches!(
        flavor,
        Flavor::Sqlite | Flavor::Duckdb | Flavor::Oracle | Flavor::Geopackage
    ) {
        None
    } else {
        database
    };
    let filtered = filter_rows(rows, schema.as_deref(), database, |r| {
        (r.schema.as_deref(), r.database.as_deref())
    });
    let mut out: IndexMap<String, Vec<IndexDoc>> = IndexMap::new();
    for doc in filtered {
        out.entry(oracle_column_name(flavor, &doc.column))
            .or_default()
            .push(IndexDoc {
                name: doc.index.clone(),
                type_: doc.index_type.clone(),
                clustered: if flavor == Flavor::Mssql {
                    doc.clustered
                } else {
                    None
                },
            });
    }
    out
}

/// The PostGIS `geometry_columns` / `geography_columns` lookup (`utils/sql.py:3106`).
pub fn get_postgis_geo_columns_types_query(table: &str, schema: Option<&str>) -> Result<String> {
    clean(table)?;
    clean(schema.unwrap_or("None"))?;
    let schema = schema.filter(|s| !s.is_empty()).unwrap_or("public");
    let truncated_schema_name = truncate_item_name(schema, Flavor::Postgis);
    let truncated_table_name = truncate_item_name(table, Flavor::Postgis);
    Ok(format!(
        "SELECT \"f_geometry_column\" AS \"column\", 'GEOMETRY' AS \"func\", \"type\", geom.\"srid\"\n\
         FROM \"geometry_columns\" AS geom\n\
         WHERE \"f_table_schema\" = '{truncated_schema_name}'\n    \
         AND \"f_table_name\" = '{truncated_table_name}'\n\
         UNION ALL\n\
         SELECT \"f_geography_column\" AS \"column\", 'GEOGRAPHY' AS \"func\", \"type\", geog.\"srid\"\n\
         FROM \"geography_columns\" AS geog\n\
         WHERE \"f_table_schema\" = '{truncated_schema_name}'\n    \
         AND \"f_table_name\" = '{truncated_table_name}'\n"
    ))
}

/// Map PostGIS lookup rows `(column, func, type, srid)` to `{column: 'GEOMETRY(TYPE, SRID)'}`.
pub fn postgis_geo_columns_types_from_rows(
    rows: &[(String, String, String, Option<i64>)],
) -> IndexMap<String, String> {
    let mut out = IndexMap::new();
    for (col, func, typ, srid) in rows {
        let srid = srid.unwrap_or(0);
        let typ_upper = typ.to_uppercase();
        let value = if srid != 0 {
            format!("{func}({typ_upper}, {srid})")
        } else if typ_upper != "GEOMETRY" && typ_upper != "GEOGRAPHY" {
            format!("{func}({typ_upper})")
        } else {
            func.clone()
        };
        out.insert(col.clone(), value);
    }
    out
}

/// `get_db_version`'s query (`utils/sql.py:2047`).
pub fn get_db_version_query(flavor: Flavor) -> String {
    let version_name = sql_item_name("version", flavor, None);
    py_format(
        flavor.version_query_template(),
        &[("version_name", &version_name)],
    )
}

/// `exists_queries['default']` for a table.
pub fn get_exists_query(table: &str, flavor: Flavor, schema: Option<&str>) -> String {
    py_format(
        flavor.exists_query_template(),
        &[("table_name", &sql_item_name(table, flavor, schema))],
    )
}

/// A query returning a row iff `table` exists.
///
/// Rust-only: Python calls SQLAlchemy's inspector (`insp.has_table`), which has no fixed text.
pub fn get_table_exists_query(table: &str, flavor: Flavor, schema: Option<&str>) -> String {
    let table = esc(&truncate_item_name(table, flavor));
    let schema = schema.map(esc);
    match flavor {
        Flavor::Sqlite | Flavor::Geopackage => format!(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = '{table}'"
        ),
        Flavor::Oracle => format!(
            "SELECT table_name FROM all_tables WHERE table_name IN ('{table}', '{}')",
            table.to_uppercase()
        ),
        Flavor::Mssql => match schema {
            Some(s) => format!(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{s}'"
            ),
            None => format!(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'"
            ),
        },
        Flavor::Mysql | Flavor::Mariadb => match schema {
            Some(s) => format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{s}'"
            ),
            None => format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = DATABASE()"
            ),
        },
        _ => match schema {
            Some(s) => format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = '{s}'"
            ),
            None => format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = current_schema()"
            ),
        },
    }
}

/// The `MAX(column)` lookup `get_reset_autoincrement_queries` runs first (`utils/sql.py:3037`).
pub fn get_max_id_query(table: &str, column: &str, flavor: Flavor, schema: Option<&str>) -> String {
    let max_id_name = sql_item_name("max_id", flavor, None);
    let table_name = sql_item_name(table, flavor, schema);
    let column_name = sql_item_name(column, flavor, None);
    format!(
        "\n        SELECT COALESCE(MAX({column_name}), 0) AS {max_id_name}\n        FROM {table_name}\n        "
    )
}

/// `get_reset_autoincrement_queries(...)` given the `max_id` the lookup returned
/// (`utils/sql.py:3001`). Returns `[]` when `max_id` is `None` (or the table is missing,
/// which the caller checks).
pub fn get_reset_autoincrement_queries(
    table: &str,
    column: &str,
    flavor: Flavor,
    schema: Option<&str>,
    max_id: Option<i64>,
) -> Vec<String> {
    let Some(max_id) = max_id else {
        return Vec::new();
    };
    let table_name = sql_item_name(table, flavor, schema);
    let table_seq_name = sql_item_name(&format!("{table}_{column}_seq"), flavor, schema);
    let column_name = sql_item_name(column, flavor, None);
    let template = lookup(RESET_AUTOINCREMENT_QUERIES, flavor.as_str());
    let val = max_id.to_string();
    let val_plus_1 = (max_id + 1).to_string();
    vec![py_format(
        template,
        &[
            ("column", column),
            ("column_name", &column_name),
            ("table", table),
            ("table_name", &table_name),
            ("table_seq_name", &table_seq_name),
            ("val", &val),
            ("val_plus_1", &val_plus_1),
        ],
    )]
}

/// `get_distinct_col_count`'s query (`utils/sql.py:932`).
pub fn get_distinct_col_count_query(col: &str, query: &str, flavor: Flavor) -> String {
    let col_name = sql_item_name(col, flavor, None);
    if !flavor.mysql_partition() {
        format!(
            "\n        WITH src AS ( {query} ),\n        dist AS ( SELECT DISTINCT {col_name} FROM src )\n        SELECT COUNT(*) FROM dist"
        )
    } else {
        format!(
            "\n        SELECT COUNT(*)\n        FROM (\n            SELECT DISTINCT {col_name}\n            FROM ({query}) AS src\n        ) AS dist"
        )
    }
}
