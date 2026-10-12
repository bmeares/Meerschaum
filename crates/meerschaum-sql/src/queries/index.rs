//! Index naming and `CREATE`/`DROP INDEX` builders (`meerschaum/connectors/sql/_pipes.py:473-1081`).

use indexmap::{IndexMap, IndexSet};

use crate::dtypes::{are_dtypes_equal, get_db_type_from_pd_type, get_pd_type_from_db_type};
use crate::flavor::Flavor;
use crate::ident::{sql_item_name, truncate_item_name};
use crate::pipe::{Interval, PipeSpec};
use crate::pytext::py_format;
use crate::queries::ConnectorCtx;
use crate::queries::table::{
    CreateTableOptions, IndexDoc, QueryOrDtypes, get_create_table_queries, get_rename_table_queries,
};
use crate::queries::templates::has_update_query;
use crate::queries::update::get_null_replacement;
use crate::value::PyValue;

/// The default `index_template` (`_pipes.py:480`).
pub const DEFAULT_INDEX_TEMPLATE: &str = "IX_{schema_str}{target}_{column_names}";

/// `SQLConnector.get_pipe_index_names(pipe)`: index key → (truncated) index name.
pub fn get_pipe_index_names(ctx: &ConnectorCtx, pipe: &PipeSpec) -> IndexMap<String, String> {
    let template = pipe
        .param("index_template")
        .map(PyValue::py_str)
        .unwrap_or_else(|| DEFAULT_INDEX_TEMPLATE.to_string());
    // PARITY: Python computes a `schema_str` and then overwrites it with `''`
    // (`_pipes.py:489-490`).
    let schema_str = "";
    let location_key = pipe
        .location_key
        .clone()
        .unwrap_or_else(|| "None".to_string());
    let mut seen: IndexMap<String, String> = IndexMap::new();
    for (ix, cols) in pipe.truthy_indices() {
        let name = py_format(
            &template,
            &[
                ("target", &pipe.target),
                ("column_names", &cols.joined()),
                ("connector_keys", &pipe.connector_keys),
                ("metric_key", &pipe.metric_key),
                ("location_key", &location_key),
                ("schema_str", schema_str),
            ],
        );
        // NOTE: Skip any duplicate indices (the first key wins).
        seen.entry(name).or_insert(ix);
    }
    seen.into_iter()
        .map(|(name, ix)| (ix, truncate_item_name(&name, ctx.flavor)))
        .collect()
}

/// `_is_non_indexable_col(col, existing_cols_types, flavor)` (`_pipes.py:526`).
pub fn is_non_indexable_col(
    col: &str,
    existing_cols_types: &IndexMap<String, String>,
    flavor: Flavor,
) -> bool {
    let col_db_type = existing_cols_types
        .get(col)
        .map(|t| t.to_uppercase())
        .unwrap_or_default();
    match flavor {
        Flavor::Mysql | Flavor::Mariadb => {
            col_db_type.contains("TEXT") || col_db_type.contains("BLOB")
        }
        Flavor::Mssql => {
            col_db_type == "TEXT" || col_db_type == "NTEXT" || col_db_type.contains("MAX")
        }
        _ => false,
    }
}

/// The table state `get_create_index_queries` reads from the database in Python.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexState {
    /// `pipe.get_columns_types()`: column → DB type.
    pub existing_cols_types: IndexMap<String, String>,
    /// `self.get_pipe_columns_indices(pipe)`: column → index docs.
    pub existing_cols_indices: IndexMap<String, Vec<IndexDoc>>,
    /// `get_config('system', 'experimental', 'space')` (default `False`).
    pub space_partition: bool,
    /// `get_distinct_col_count(id, ...)` when `space_partition` is on.
    pub id_distinct_count: Option<i64>,
    /// Override for `pipe.get_chunk_interval()`.
    pub chunk_interval: Option<Interval>,
}

/// Deduplicate preserving first occurrence.
///
/// PARITY: Python builds `indices_cols_str` from a `set` (`_pipes.py:899`), whose order depends on
/// `PYTHONHASHSEED`; Rust keeps `columns` order.
fn ordered_unique(items: impl IntoIterator<Item = String>) -> Vec<String> {
    let set: IndexSet<String> = items.into_iter().collect();
    set.into_iter().collect()
}

/// `SQLConnector.get_create_index_queries(pipe)`: key → queries, in Python's insertion order.
pub fn get_create_index_queries(
    ctx: &ConnectorCtx,
    pipe: &PipeSpec,
    state: &IndexState,
) -> IndexMap<String, Vec<String>> {
    let flavor = ctx.flavor;
    // NOTE: Due to recent breaking changes in DuckDB, indices don't behave properly.
    if flavor == Flavor::Duckdb {
        return IndexMap::new();
    }
    let q = |item: &str| sql_item_name(item, flavor, None);
    let mut index_queries: IndexMap<String, Vec<String>> = IndexMap::new();

    let upsert = pipe.upsert() && has_update_query(&format!("{}-upsert", flavor.as_str()));
    let is_static = pipe.is_static();
    let null_indices = pipe.null_indices();
    let mut index_names = get_pipe_index_names(ctx, pipe);
    let unique_index_name_unquoted = index_names
        .get("unique")
        .filter(|n| !n.is_empty())
        .cloned()
        .unwrap_or_else(|| format!("IX_{}_unique", pipe.target));
    if upsert {
        index_names.shift_remove("unique");
    }
    let indices = pipe.indices();
    let columns = pipe.columns();
    let existing_cols_types = &state.existing_cols_types;
    let existing_cols_pd_types: IndexMap<String, String> = existing_cols_types
        .iter()
        .map(|(c, t)| (c.clone(), get_pd_type_from_db_type(t, true)))
        .collect();
    let mut existing_ix_names: IndexSet<String> = IndexSet::new();
    let mut existing_primary_keys: Vec<String> = Vec::new();
    let mut existing_clustered_primary_keys: Vec<String> = Vec::new();
    for (col, col_indices) in &state.existing_cols_indices {
        for doc in col_indices {
            existing_ix_names.insert(doc.name.clone().unwrap_or_default().to_lowercase());
            if doc.type_.as_deref() == Some("PRIMARY KEY") {
                existing_primary_keys.push(col.to_lowercase());
                if doc.clustered.unwrap_or(true) {
                    existing_clustered_primary_keys.push(col.to_lowercase());
                }
            }
        }
    }

    let datetime = columns.get("datetime").cloned();
    let datetime_name = datetime.as_deref().map(q);
    let datetime_index_name = index_names
        .get("datetime")
        .filter(|n| !n.is_empty())
        .map(|n| q(n));
    let id = columns.get("id").cloned();
    let id_name = id.as_deref().map(q);
    let primary_key = columns.get("primary").cloned();
    let primary_key_name = primary_key.as_deref().map(q).unwrap_or_default();
    let autoincrement = pipe.autoincrement()
        || primary_key
            .as_ref()
            .is_some_and(|pk| !existing_cols_pd_types.contains_key(pk));
    let dtypes = pipe.dtypes();
    let primary_key_db_type = primary_key.as_ref().map(|pk| {
        let typ = dtypes
            .get(pk)
            .filter(|t| !t.is_empty())
            .cloned()
            .unwrap_or_else(|| "int".to_string());
        get_db_type_from_pd_type(&typ, Some(flavor))
    });
    let pk_db_type = primary_key_db_type.clone().unwrap_or_default();
    let primary_key_constraint_name = q(&format!("PK_{}", pipe.target));
    let primary_key_clustered = if datetime.is_none() {
        "CLUSTERED"
    } else {
        "NONCLUSTERED"
    };
    let datetime_clustered = if existing_clustered_primary_keys.is_empty() && datetime.is_some() {
        "CLUSTERED"
    } else {
        "NONCLUSTERED"
    };
    let include_columns_str = existing_cols_types
        .keys()
        .filter(|c| Some(c.as_str()) != datetime.as_deref())
        .map(|c| q(c))
        .collect::<Vec<_>>()
        .join("\n    ,");
    let include_columns_str = include_columns_str.trim_end_matches(',');
    let include_clause = if datetime_clustered == "NONCLUSTERED" {
        format!("\nINCLUDE (\n    {include_columns_str}\n)")
    } else {
        String::new()
    };
    // PARITY: a missing `id` index name renders as `None` (`_pipes.py:687`).
    let id_index_name = index_names
        .get("id")
        .filter(|n| !n.is_empty())
        .map(|n| q(n))
        .unwrap_or_else(|| "None".to_string());
    let pipe_schema = ctx.get_pipe_schema(pipe);
    let pipe_name = sql_item_name(&pipe.target, flavor, pipe_schema.as_deref());
    let create_space_partition = state.space_partition;

    // Create the datetime index.
    let mut dt_query: Option<String> = None;
    if let Some(dt) = &datetime {
        if flavor.is_timescaledb() && pipe.hypertable() {
            let id_count = if id.is_some() && create_space_partition {
                state
                    .id_distinct_count
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "None".to_string())
            } else {
                "None".to_string()
            };
            let chunk_interval = state
                .chunk_interval
                .unwrap_or_else(|| pipe.get_chunk_interval());
            let chunk_interval_minutes = chunk_interval.whole_minutes();
            let chunk_time_interval = match chunk_interval {
                Interval::Duration(_) => format!("INTERVAL '{chunk_interval_minutes} MINUTES'"),
                Interval::Int(_) => chunk_interval_minutes.to_string(),
            };
            let space = match (&id, create_space_partition) {
                (Some(id), true) => format!("'{id}', {id_count}, "),
                _ => String::new(),
            };
            dt_query = Some(format!(
                "SELECT public.create_hypertable('{pipe_name}', '{dt}', {space}chunk_time_interval => {chunk_time_interval}, if_not_exists => true, migrate_data => true);"
            ));
        } else if let Some(ix_name) = &datetime_index_name {
            if Some(dt) != primary_key.as_ref() {
                let dt_name = datetime_name.clone().unwrap_or_default();
                dt_query = Some(if flavor == Flavor::Mssql {
                    format!(
                        "CREATE {datetime_clustered} INDEX {ix_name} \nON {pipe_name} ({dt_name}){include_clause}"
                    )
                } else {
                    format!("CREATE INDEX {ix_name} ON {pipe_name} ({dt_name})")
                });
            }
        }
    }
    if let (Some(query), Some(dt)) = (dt_query, &datetime) {
        index_queries.insert(dt.clone(), vec![query]);
    }

    // Primary key.
    if let Some(pk) = &primary_key {
        if !existing_primary_keys.contains(&pk.to_lowercase()) && !is_static {
            let mut primary_queries: Vec<String> = Vec::new();
            let pk_exists = existing_cols_pd_types.contains_key(pk);
            if autoincrement && !pk_exists {
                primary_queries.push(format!(
                    "ALTER TABLE {pipe_name}\nADD {primary_key_name} {pk_db_type} {}",
                    flavor.auto_increment_column()
                ));
            } else if !autoincrement && pk_exists {
                match flavor {
                    Flavor::Sqlite | Flavor::Geopackage => {
                        let new_table = format!("_new_{}", pipe.target);
                        let new_table_name =
                            sql_item_name(&new_table, flavor, pipe_schema.as_deref());
                        let select_cols_str = existing_cols_types
                            .keys()
                            .map(|c| q(c))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let create = get_create_table_queries(
                            &QueryOrDtypes::Dtypes(existing_cols_pd_types.clone()),
                            &new_table,
                            flavor,
                            &CreateTableOptions {
                                schema: pipe_schema.clone(),
                                primary_key: Some(pk.clone()),
                                ..Default::default()
                            },
                        )
                        .unwrap_or_default();
                        primary_queries.extend(create);
                        primary_queries.push(format!(
                            "INSERT INTO {new_table_name} ({select_cols_str})\nSELECT {select_cols_str}\nFROM {pipe_name}"
                        ));
                        primary_queries.push(format!("DROP TABLE {pipe_name}"));
                        primary_queries.extend(
                            get_rename_table_queries(
                                &new_table,
                                &pipe.target,
                                flavor,
                                pipe_schema.as_deref(),
                                None,
                            )
                            .unwrap_or_default(),
                        );
                    }
                    Flavor::Oracle => {
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nMODIFY {primary_key_name} NOT NULL"
                        ));
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                        ));
                    }
                    Flavor::Mysql | Flavor::Mariadb => {
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nMODIFY {primary_key_name} {pk_db_type} NOT NULL"
                        ));
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                        ));
                    }
                    Flavor::Timescaledb | Flavor::TimescaledbHa => {
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nALTER COLUMN {primary_key_name} SET NOT NULL"
                        ));
                        let dt_prefix = datetime_name
                            .as_ref()
                            .map(|n| format!("{n}, "))
                            .unwrap_or_default();
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({dt_prefix}{primary_key_name})"
                        ));
                    }
                    Flavor::Citus | Flavor::Postgresql | Flavor::Duckdb | Flavor::Postgis => {
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nALTER COLUMN {primary_key_name} SET NOT NULL"
                        ));
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                        ));
                    }
                    _ => {
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nALTER COLUMN {primary_key_name} {pk_db_type} NOT NULL"
                        ));
                        primary_queries.push(format!(
                            "ALTER TABLE {pipe_name}\nADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY {primary_key_clustered} ({primary_key_name})"
                        ));
                    }
                }
            }
            // PARITY: the key is set even when no query was produced (`_pipes.py:836`).
            index_queries.insert(pk.clone(), primary_queries);
        }
    }

    // The `id` index.
    if let (Some(id), Some(id_name)) = (&id, &id_name) {
        let id_query = if flavor.is_timescaledb() {
            if create_space_partition {
                None
            } else {
                Some(format!(
                    "CREATE INDEX IF NOT EXISTS {id_index_name} ON {pipe_name} ({id_name})"
                ))
            }
        } else if is_non_indexable_col(id, existing_cols_types, flavor) {
            None
        } else {
            Some(format!(
                "CREATE INDEX {id_index_name} ON {pipe_name} ({id_name})"
            ))
        };
        if let Some(query) = id_query {
            index_queries.insert(id.clone(), vec![query]);
        }
    }

    // Indices for other labels in `pipe.columns` and `pipe.indices`.
    let other_index_names: Vec<(String, String)> = index_names
        .iter()
        .filter(|(ix_key, ix_unquoted)| {
            !matches!(ix_key.as_str(), "datetime" | "id" | "primary")
                && !existing_ix_names.contains(&ix_unquoted.to_lowercase())
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (ix_key, ix_unquoted) in other_index_names {
        let ix_name = q(&ix_unquoted);
        let mut cols: Vec<String> = indices.get(&ix_key).map(|c| c.as_vec()).unwrap_or_default();
        if ix_key == "unique" && upsert {
            continue;
        }
        if matches!(flavor, Flavor::Mysql | Flavor::Mariadb | Flavor::Mssql) {
            cols.retain(|c| !c.is_empty() && !is_non_indexable_col(c, existing_cols_types, flavor));
        }
        let cols_names: Vec<String> = cols
            .iter()
            .filter(|c| !c.is_empty())
            .map(|c| q(c))
            .collect();
        if cols_names.is_empty() {
            continue;
        }
        let cols_names_str = cols_names.join(", ");
        let mut params_clause = format!(" ({cols_names_str})");
        if matches!(flavor, Flavor::Postgis | Flavor::TimescaledbHa) {
            for _col in &cols {
                // PARITY: Python checks `cols[0]` on every iteration (`_pipes.py:885`).
                let col_typ = existing_cols_pd_types
                    .get(&cols[0])
                    .cloned()
                    .unwrap_or_else(|| "object".to_string());
                if col_typ != "object" && are_dtypes_equal(&col_typ, "geometry") {
                    params_clause = format!(" USING GIST ({cols_names_str})");
                    break;
                }
            }
        }
        index_queries.insert(
            ix_key.clone(),
            vec![format!(
                "CREATE INDEX {ix_name} ON {pipe_name}{params_clause}"
            )],
        );
    }

    let indexed_columns: Vec<(String, String)> = columns
        .iter()
        .filter(|(_, col)| existing_cols_types.contains_key(*col))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let indices_cols_str = ordered_unique(indexed_columns.iter().map(|(_, c)| q(c))).join(", ");
    let coalesce_indices_cols_str = indexed_columns
        .iter()
        .map(|(ix_key, col)| {
            if ix_key != "datetime" && null_indices {
                format!(
                    "COALESCE({}, {}) ",
                    q(col),
                    get_null_replacement(&existing_cols_types[col], Some(flavor))
                )
            } else {
                q(col)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let unique_index_name = q(&unique_index_name_unquoted);
    let constraint_name_unquoted = unique_index_name_unquoted.replace("IX_", "UQ_");
    let constraint_name = q(&constraint_name_unquoted);
    let unique_target_exists = existing_ix_names
        .contains(&unique_index_name_unquoted.to_lowercase())
        || (flavor.postgres_unique_constraint()
            && existing_ix_names.contains(&constraint_name_unquoted.to_lowercase()));
    let add_constraint_query = format!(
        "ALTER TABLE {pipe_name} ADD CONSTRAINT {constraint_name} UNIQUE ({indices_cols_str})"
    );
    let unique_index_cols_str = if !flavor.coalesce_unique_index() || !null_indices {
        indices_cols_str.clone()
    } else {
        coalesce_indices_cols_str
    };
    let create_unique_index_query =
        format!("CREATE UNIQUE INDEX {unique_index_name} ON {pipe_name} ({unique_index_cols_str})");
    let mut constraint_queries = if flavor.postgres_unique_constraint() {
        vec![add_constraint_query.clone()]
    } else {
        vec![create_unique_index_query]
    };
    if !matches!(flavor, Flavor::Sqlite | Flavor::Geopackage)
        && !flavor.postgres_unique_constraint()
    {
        constraint_queries.push(add_constraint_query);
    }
    if upsert && !indices_cols_str.is_empty() && !unique_target_exists {
        index_queries.insert(unique_index_name, constraint_queries);
        // Remove regular indices covering the same single column as the unique index.
        if Some(&unique_index_cols_str) == id_name.as_ref() {
            if let Some(id) = &id {
                index_queries.shift_remove(id);
            }
        }
        if Some(&unique_index_cols_str) == datetime_name.as_ref() {
            if let Some(dt) = &datetime {
                index_queries.shift_remove(dt);
            }
        }
    }
    index_queries
}

/// The table state `get_drop_index_queries` reads from the database in Python.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DropIndexState {
    /// `pipe.exists()`.
    pub exists: bool,
    /// `pipe.get_columns_indices()`.
    pub cols_indices: IndexMap<String, Vec<IndexDoc>>,
    /// Whether the hypertable size query returned a value (TimescaleDB / Citus).
    pub is_hypertable: bool,
    /// Whether `_<target>_temp_migration` exists.
    pub temp_table_exists: bool,
}

/// `SQLConnector.get_drop_index_queries(pipe)`: key → queries.
pub fn get_drop_index_queries(
    ctx: &ConnectorCtx,
    pipe: &PipeSpec,
    state: &DropIndexState,
) -> IndexMap<String, Vec<String>> {
    let flavor = ctx.flavor;
    // NOTE: Due to breaking changes within DuckDB, indices must be skipped.
    if flavor == Flavor::Duckdb || !state.exists {
        return IndexMap::new();
    }
    let mut drop_queries: IndexMap<String, Vec<String>> = IndexMap::new();
    let schema = ctx.get_pipe_schema(pipe);
    let index_schema = if flavor == Flavor::Mssql {
        None
    } else {
        schema.clone()
    };
    let indices = get_pipe_index_names(ctx, pipe);
    let mut existing_indices: IndexSet<String> = IndexSet::new();
    let mut clustered_ix: Option<String> = None;
    for metas in state.cols_indices.values() {
        for meta in metas {
            let ix_name = meta.name.clone().unwrap_or_default();
            if meta.clustered.unwrap_or(false) {
                clustered_ix = Some(ix_name.clone());
            }
            existing_indices.insert(ix_name.to_lowercase());
        }
    }
    let pipe_name = sql_item_name(&pipe.target, flavor, schema.as_deref());
    let pipe_name_no_schema = sql_item_name(&pipe.target, flavor, None);
    let upsert = pipe.upsert();
    let is_hypertable = flavor.hypertable_query_template().is_some() && state.is_hypertable;

    let if_exists_str = if flavor.drop_index_if_exists() {
        "IF EXISTS "
    } else {
        ""
    };
    if is_hypertable {
        let mut nuke_queries = Vec::new();
        let temp_table = format!("_{}_temp_migration", pipe.target);
        let temp_table_name = sql_item_name(&temp_table, flavor, schema.as_deref());
        if state.temp_table_exists {
            nuke_queries.push(format!("DROP TABLE {if_exists_str} {temp_table_name}"));
        }
        nuke_queries.push(format!("SELECT * INTO {temp_table_name} FROM {pipe_name}"));
        nuke_queries.push(format!("DROP TABLE {if_exists_str}{pipe_name}"));
        nuke_queries.push(format!(
            "ALTER TABLE {temp_table_name} RENAME TO {pipe_name_no_schema}"
        ));
        for ix_key in ["datetime", "id"] {
            if indices.contains_key(ix_key) {
                drop_queries
                    .entry(ix_key.to_string())
                    .or_default()
                    .extend(nuke_queries.clone());
                break;
            }
        }
    }

    for (ix_key, ix_unquoted) in &indices {
        if drop_queries.contains_key(ix_key) {
            continue;
        }
        let is_unique_upsert = ix_key == "unique" && upsert;
        let constraint_name_unquoted = ix_unquoted.replace("IX_", "UQ_");
        let index_exists = existing_indices.contains(&ix_unquoted.to_lowercase());
        let constraint_exists = if flavor.postgres_unique_constraint() {
            existing_indices.contains(&constraint_name_unquoted.to_lowercase())
        } else {
            index_exists
        };
        if !index_exists && !(is_unique_upsert && constraint_exists) {
            continue;
        }
        if is_unique_upsert
            && !matches!(flavor, Flavor::Sqlite | Flavor::Geopackage)
            && !is_hypertable
            && constraint_exists
        {
            let constraint_name = sql_item_name(&constraint_name_unquoted, flavor, None);
            let constraint_or_index = if flavor.mysql_partition() {
                "INDEX"
            } else {
                "CONSTRAINT"
            };
            drop_queries
                .entry(ix_key.clone())
                .or_default()
                .push(format!(
                    "ALTER TABLE {pipe_name}\nDROP {constraint_or_index} {constraint_name}"
                ));
        }
        if !index_exists {
            continue;
        }
        let mut query = format!(
            "{}DROP INDEX {if_exists_str}{}",
            if flavor.mysql_partition() {
                format!("ALTER TABLE {pipe_name}\n")
            } else {
                String::new()
            },
            sql_item_name(ix_unquoted, flavor, index_schema.as_deref())
        );
        if flavor == Flavor::Mssql {
            query.push_str(&format!("\nON {pipe_name}"));
            if Some(ix_unquoted) == clustered_ix.as_ref() {
                query.push_str("\nWITH (ONLINE = ON, MAXDOP = 4)");
            }
        }
        drop_queries.entry(ix_key.clone()).or_default().push(query);
    }
    drop_queries
}
