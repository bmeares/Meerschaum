//! Index management (`_pipes.py`: `create_indices`, `drop_indices`, `create_pipe_indices`,
//! `drop_pipe_indices`, `get_pipe_index_names`, `get_create_index_queries`,
//! `get_drop_index_queries`). The query text comes from [`crate::queries::index`]; this module
//! gathers the table state those builders take as input.

use indexmap::{IndexMap, IndexSet};
use meerschaum_connector::ConnectorResult;
use meerschaum_core::{Pipe, SuccessTuple};

use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::SqlPipe;
use crate::queries::ConnectorCtx;
use crate::queries::index::{
    DropIndexState, IndexState, get_create_index_queries, get_drop_index_queries,
    get_pipe_index_names,
};

use super::{SqlConnector, be, geopackage};

/// Expected catalog identity, derived from the typed index plan (never parsed from SQL).
struct OracleInternalIndex {
    name: String,
    columns: Vec<String>,
    unique: bool,
    constraint: bool,
}

fn oracle_catalog_identifier(emitted: &str) -> String {
    match emitted.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => emitted.to_uppercase(),
    }
}

fn oracle_catalog_name(name: &str) -> String {
    oracle_catalog_identifier(&sql_item_name(name, Flavor::Oracle, None))
}

fn oracle_literal(value: &str) -> String {
    value.replace('\'', "''")
}

impl OracleInternalIndex {
    fn guard(&self, query: &str, table: &str, owner: &str) -> String {
        let (view, columns_view, relation, position, properties, codes) = if self.constraint {
            (
                "all_constraints",
                "all_cons_columns",
                "cols.owner = existing.owner AND cols.constraint_name = existing.constraint_name",
                "position",
                "existing.constraint_type = 'U' AND existing.status = 'ENABLED' \
                 AND existing.validated = 'VALIDATED' AND existing.deferrable = 'NOT DEFERRABLE'"
                    .to_owned(),
                "-2261, -2264",
            )
        } else {
            (
                "all_indexes",
                "all_ind_columns",
                "cols.index_owner = existing.owner AND cols.index_name = existing.index_name",
                "column_position",
                format!(
                    "existing.table_owner = {owner} AND existing.uniqueness = '{}' \
                     AND existing.status = 'VALID' AND existing.index_type = 'NORMAL'",
                    if self.unique { "UNIQUE" } else { "NONUNIQUE" },
                ),
                "-955, -1408",
            )
        };
        let column_matches = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "(cols.{position} = {} AND cols.column_name = '{}'{} )",
                    index + 1,
                    oracle_literal(&oracle_catalog_name(column)),
                    if self.constraint {
                        ""
                    } else {
                        " AND cols.descend = 'ASC'"
                    },
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let object_name = if self.constraint {
            "constraint_name"
        } else {
            "index_name"
        };
        let count = self.columns.len();
        format!(
            "DECLARE matching_objects PLS_INTEGER;\nBEGIN\n    EXECUTE IMMEDIATE '{}';\n\
             EXCEPTION WHEN OTHERS THEN\n    IF SQLCODE NOT IN ({codes}) THEN RAISE; END IF;\n\
                 SELECT COUNT(*) INTO matching_objects FROM {view} existing\n\
                 WHERE existing.owner = {owner} AND existing.table_name = '{}'\n\
                 AND existing.{object_name} = '{}' AND {properties}\n\
                 AND (SELECT COUNT(*) FROM {columns_view} cols WHERE {relation}) = {count}\n\
                 AND (SELECT COUNT(*) FROM {columns_view} cols WHERE {relation}\n\
                      AND ({column_matches})) = {count};\n\
                 IF matching_objects != 1 THEN RAISE; END IF;\nEND;",
            oracle_literal(query),
            oracle_literal(&oracle_catalog_name(table)),
            oracle_literal(&oracle_catalog_name(&self.name)),
        )
    }
}

/// Oracle has no CREATE INDEX IF NOT EXISTS. Accept only a completed identical
/// internal definition after a stale-plan duplicate, preserving every genuine DDL error.
fn guard_oracle_internal_indices(
    ctx: &ConnectorCtx,
    sp: &SqlPipe,
    state: &IndexState,
    queries: &mut IndexMap<String, Vec<String>>,
) {
    let q = |name: &str| sql_item_name(name, Flavor::Oracle, None);
    let table = sql_item_name(
        &sp.target,
        Flavor::Oracle,
        ctx.get_pipe_schema(sp).as_deref(),
    );
    let owner = if ctx.get_pipe_schema(sp).is_some() {
        // The typed renderer determines the qualifier's quoting on Oracle. Strip
        // only that exact known target suffix, rather than parsing arbitrary SQL.
        let Some(qualifier) = table.strip_suffix(&format!(".{}", q(&sp.target))) else {
            return;
        };
        format!(
            "'{}'",
            oracle_literal(&oracle_catalog_identifier(qualifier))
        )
    } else {
        "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')".to_owned()
    };
    let names = get_pipe_index_names(ctx, sp);
    let mut expected = IndexMap::new();
    for (role, columns) in sp.index_columns() {
        let Some(name) = names.get(&role) else {
            continue;
        };
        let columns: Vec<_> = columns
            .as_vec()
            .into_iter()
            .filter(|c| !c.is_empty())
            .collect();
        if columns.is_empty() {
            continue;
        }
        let rendered = columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        expected.insert(
            format!("CREATE INDEX {} ON {table} ({rendered})", q(name)),
            OracleInternalIndex {
                name: name.clone(),
                columns,
                unique: false,
                constraint: false,
            },
        );
    }
    if sp.upsert() {
        let name = names
            .get("unique")
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("IX_{}_unique", sp.target));
        let columns: Vec<_> = sp
            .columns()
            .values()
            .map(meerschaum_core::Value::py_str)
            .filter(|column| state.existing_cols_types.contains_key(column))
            .collect::<IndexSet<_>>()
            .into_iter()
            .collect();
        if !columns.is_empty() {
            let rendered = columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
            expected.insert(
                format!("CREATE UNIQUE INDEX {} ON {table} ({rendered})", q(&name)),
                OracleInternalIndex {
                    name: name.clone(),
                    columns: columns.clone(),
                    unique: true,
                    constraint: false,
                },
            );
            let constraint = name.replace("IX_", "UQ_");
            expected.insert(
                format!(
                    "ALTER TABLE {table} ADD CONSTRAINT {} UNIQUE ({rendered})",
                    q(&constraint)
                ),
                OracleInternalIndex {
                    name: constraint,
                    columns,
                    unique: true,
                    constraint: true,
                },
            );
        }
    }
    // Exact equality limits the guard to these typed ordinary/unique definitions.
    // Unsupported or future DDL retains its original error behavior.
    for query in queries.values_mut().flatten() {
        if let Some(index) = expected.get(query) {
            *query = index.guard(query, &sp.target, &owner);
        }
    }
}

impl SqlConnector {
    /// `get_pipe_index_names(pipe)`: index key → index name on the database.
    pub fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<IndexMap<String, String>> {
        let sp = self.sql_pipe(pipe)?;
        Ok(get_pipe_index_names(&self.ctx(), &sp))
    }

    /// `get_create_index_queries(pipe)`: index key → queries.
    pub async fn get_create_index_queries(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<String>>> {
        let sp = self.sql_pipe(pipe)?;
        self.create_index_queries(&sp, None).await
    }

    pub(crate) async fn create_index_queries(
        &self,
        sp: &SqlPipe,
        cols_types: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<IndexMap<String, Vec<String>>> {
        let existing_cols_types = match cols_types {
            Some(c) => c,
            None => self.columns_types(sp).await,
        };
        let existing_cols_indices = self
            .get_table_cols_indices(&sp.target, self.get_pipe_schema(sp).as_deref())
            .await;
        let state = IndexState {
            existing_cols_types,
            existing_cols_indices,
            // `get_config('system', 'experimental', 'space')` is off by default.
            space_partition: false,
            id_distinct_count: None,
            chunk_interval: None,
            // rc15 S3: NULL-safe upsert keys as v4's constraint on PostgreSQL 15+.
            nulls_not_distinct: crate::queries::index::null_safe_unique_index(self.flavor, sp)
                && self.supports_nulls_not_distinct().await,
        };
        let mut queries = get_create_index_queries(&self.ctx(), sp, &state).map_err(be)?;
        // The v4 renderer always emits datetime/id DDL, even for existing indices.
        // Cold internal readiness checks run against already initialized tables too;
        // skip those ordinary indices at runtime while retaining missing unique keys.
        if sp.pipe().temporary && sp.pipe().connector_keys() == "mrsm" {
            let names = get_pipe_index_names(&self.ctx(), sp);
            for role in ["datetime", "id"] {
                if role == "datetime" && self.flavor.is_timescaledb() && sp.hypertable() {
                    continue;
                }
                let Some(name) = names.get(role) else {
                    continue;
                };
                let exists = state.existing_cols_indices.values().flatten().any(|index| {
                    index
                        .name
                        .as_deref()
                        .is_some_and(|existing| existing.eq_ignore_ascii_case(name))
                });
                if exists && let Some(column) = sp.column(role) {
                    queries.shift_remove(&column);
                }
            }
        }
        // GeoPackage: an index over the registered geometry column is an R-tree (§6.2).
        if self.flavor == Flavor::Geopackage {
            for (key, col) in self.geopackage_spatial_index_keys(sp).await {
                if queries.contains_key(&key)
                    || !self
                        .sqlite_master_has(&geopackage::rtree_name(&sp.target, &col))
                        .await
                {
                    queries.insert(key, geopackage::get_rtree_create_queries(&sp.target, &col));
                }
            }
        }
        // Independent SQLite writers can plan the same missing internal index before
        // either creates it. Let SQLite arbitrate that catalog race atomically, including
        // UNIQUE indexes; every other DDL error (e.g. duplicate rows) still propagates.
        // Ordinary pipes and the pure v4 query builders retain their existing SQL.
        if sp.pipe().temporary
            && sp.pipe().connector_keys() == "mrsm"
            && matches!(self.flavor, Flavor::Sqlite | Flavor::Geopackage)
        {
            for query in queries.values_mut().flatten() {
                for prefix in ["CREATE INDEX ", "CREATE UNIQUE INDEX "] {
                    if let Some(rest) = query.strip_prefix(prefix)
                        && !rest.starts_with("IF NOT EXISTS ")
                    {
                        *query = format!("{prefix}IF NOT EXISTS {rest}");
                        break;
                    }
                }
            }
        }
        if sp.pipe().temporary
            && sp.pipe().connector_keys() == "mrsm"
            && self.flavor == Flavor::Oracle
        {
            guard_oracle_internal_indices(&self.ctx(), sp, &state, &mut queries);
        }
        Ok(queries)
    }

    async fn sqlite_master_has(&self, name: &str) -> bool {
        self.value_params(
            "SELECT 1 FROM sqlite_master WHERE name = ?",
            &[crate::driver::SqlParam::Text(name.to_string())],
        )
        .await
        .ok()
        .flatten()
        .is_some()
    }

    /// `get_drop_index_queries(pipe)`: index key → queries.
    pub async fn get_drop_index_queries(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<String>>> {
        let sp = self.sql_pipe(pipe)?;
        let schema = self.get_pipe_schema(&sp);
        let exists = self.table_exists(&sp.target, schema.as_deref()).await;
        if !exists {
            return Ok(IndexMap::new());
        }
        let cols_indices = self
            .get_table_cols_indices(&sp.target, schema.as_deref())
            .await;
        let is_hypertable = match self.flavor.hypertable_query_template() {
            Some(template) => {
                let pipe_name = self.pipe_table_name(&sp);
                let query =
                    meerschaum_core::pyfmt::py_format(template, &[("table_name", &pipe_name)]);
                self.value(&query).await.is_some()
            }
            None => false,
        };
        let temp_table = format!("_{}_temp_migration", sp.target);
        let temp_table_exists =
            is_hypertable && self.table_exists(&temp_table, schema.as_deref()).await;
        let state = DropIndexState {
            exists,
            cols_indices,
            is_hypertable,
            temp_table_exists,
        };
        let mut queries = get_drop_index_queries(&self.ctx(), &sp, &state);
        if self.flavor == Flavor::Geopackage {
            let extensions = self.sqlite_master_has("gpkg_extensions").await;
            for (key, col) in self.geopackage_spatial_index_keys(&sp).await {
                if self
                    .sqlite_master_has(&geopackage::rtree_name(&sp.target, &col))
                    .await
                {
                    queries.insert(
                        key,
                        geopackage::get_rtree_drop_queries(&sp.target, &col, extensions),
                    );
                }
            }
        }
        Ok(queries)
    }

    /// `create_indices(pipe, columns, indices)`: create the pipe's indices (only those keyed by
    /// `columns` / `indices` when given). Returns whether every index was created.
    pub async fn create_indices(
        &self,
        pipe: &Pipe,
        columns: Option<&[String]>,
        indices: Option<&[String]>,
    ) -> bool {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return false;
        };
        self.create_indices_sp(&sp, columns, indices, None).await
    }

    pub(crate) async fn create_indices_sp(
        &self,
        sp: &SqlPipe,
        columns: Option<&[String]>,
        indices: Option<&[String]>,
        cols_types: Option<IndexMap<String, String>>,
    ) -> bool {
        if sp.indices().is_empty() {
            tracing::warn!(
                "{} has no index columns; skipping index creation.",
                sp.pipe()
            );
            return true;
        }
        let include: Vec<String> = columns
            .unwrap_or_default()
            .iter()
            .chain(indices.unwrap_or_default())
            .cloned()
            .collect();
        let ix_queries = match self.create_index_queries(sp, cols_types).await {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("Failed to build index queries for {}:\n{e}", sp.pipe());
                return false;
            }
        };
        let mut success = true;
        for (col, queries) in ix_queries {
            if !include.is_empty() && !include.contains(&col) {
                continue;
            }
            let ix_success = self.exec_queries_all(&queries, false, true).await;
            success = success && ix_success;
            if !ix_success {
                tracing::warn!("Failed to create index on column: {col}");
            }
        }
        success
    }

    /// `drop_indices(pipe, columns, indices)`.
    pub async fn drop_indices(
        &self,
        pipe: &Pipe,
        columns: Option<&[String]>,
        indices: Option<&[String]>,
    ) -> bool {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return false;
        };
        if sp.indices().is_empty() {
            tracing::warn!("No indices to drop for {pipe}.");
            return false;
        }
        let include: Vec<String> = columns
            .unwrap_or_default()
            .iter()
            .chain(indices.unwrap_or_default())
            .cloned()
            .collect();
        let ix_queries = match self.get_drop_index_queries(pipe).await {
            Ok(q) => q,
            Err(_) => return false,
        };
        let mut success = true;
        for (col, queries) in ix_queries {
            if !include.is_empty() && !include.contains(&col) {
                continue;
            }
            if !self.exec_queries_all(&queries, false, true).await {
                success = false;
                tracing::debug!("Failed to drop index on column: {col}");
            }
        }
        success
    }

    /// `create_pipe_indices(pipe, columns)`.
    pub async fn create_pipe_indices(
        &self,
        pipe: &Pipe,
        columns: Option<&[String]>,
    ) -> SuccessTuple {
        if let Some(refused) = self.protected_refusal(pipe, "create indices for") {
            return refused;
        }
        if self.create_indices(pipe, columns, None).await {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to create indices for {pipe}."))
        }
    }

    /// `drop_pipe_indices(pipe, columns)`.
    pub async fn drop_pipe_indices(&self, pipe: &Pipe, columns: Option<&[String]>) -> SuccessTuple {
        if let Some(refused) = self.protected_refusal(pipe, "drop indices for") {
            return refused;
        }
        if let Some(refused) = self
            .lock_refusal(pipe, meerschaum_core::access::LockOp::Schema)
            .await
        {
            return refused;
        }
        if self.drop_indices(pipe, columns, None).await {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to drop indices for {pipe}."))
        }
    }

    /// The quoted name of an index (helper for tests and the façade).
    pub fn quoted_index_name(&self, name: &str) -> String {
        sql_item_name(name, self.flavor, None)
    }
}
