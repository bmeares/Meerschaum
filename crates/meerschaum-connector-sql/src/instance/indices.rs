//! Index management (`_pipes.py`: `create_indices`, `drop_indices`, `create_pipe_indices`,
//! `drop_pipe_indices`, `get_pipe_index_names`, `get_create_index_queries`,
//! `get_drop_index_queries`). The query text comes from [`crate::queries::index`]; this module
//! gathers the table state those builders take as input.

use indexmap::IndexMap;
use meerschaum_connector::ConnectorResult;
use meerschaum_core::{Pipe, SuccessTuple};

use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::SqlPipe;
use crate::queries::index::{
    DropIndexState, IndexState, get_create_index_queries, get_drop_index_queries,
    get_pipe_index_names,
};

use super::{SqlConnector, be, geopackage};

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
        };
        let mut queries = get_create_index_queries(&self.ctx(), sp, &state).map_err(be)?;
        // The v4 renderer always emits datetime/id DDL, even for existing indices.
        // Cold internal readiness checks run against already initialized tables too;
        // skip those ordinary indices at runtime while retaining missing unique keys.
        if sp.pipe().temporary && sp.is_static() && sp.pipe().connector_keys() == "mrsm" {
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
        if self.create_indices(pipe, columns, None).await {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to create indices for {pipe}."))
        }
    }

    /// `drop_pipe_indices(pipe, columns)`.
    pub async fn drop_pipe_indices(&self, pipe: &Pipe, columns: Option<&[String]>) -> SuccessTuple {
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
