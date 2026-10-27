//! `sync_pipe_inplace` (`_pipes.py:2324`): sync a `sql:` pipe whose source connector is its
//! instance entirely inside the database, staging the fetch, backtrack, delta, joined, unseen
//! and update sets in temporary tables within one transaction (SQLAlchemy `Session`).

use indexmap::IndexMap;
use meerschaum_core::dtypes::{are_dtypes_equal, get_pd_type_from_db_type};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};

use crate::dateadd::{DateArg, Number, dateadd_for_execution as dateadd_str};
use crate::driver::{SqlTransaction, SqlValue};
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::{DriverFamily, Flavor};
use crate::ident::sql_item_name;
use crate::queries::metadef::{Bound, SQLITE_STORED_DATETIME_GLOB, sqlite_canonical_datetime};
use crate::queries::table::{
    ColumnTypeRow, CreateTableOptions, QueryOrDtypes, cols_types_from_rows,
    get_create_schema_if_not_exists_queries, get_create_table_queries, get_table_cols_types_query,
};
use crate::queries::templates::has_update_query;
use crate::queries::update::{UpdateQueriesArgs, get_null_replacement, get_update_queries};
use meerschaum_connector::DataQuery;

use super::data::{DataQueryOptions, fmt_thousands};
use super::fetch::FetchArgs;
use super::values::sync_time_from_sql;
use super::{SqlConnector, frame_docs, kv_int, kv_str};

const TEMP_TABLE_ROOTS: [&str; 6] = ["backtrack", "new", "delta", "joined", "unseen", "update"];

/// Sees the rows an in-place sync is about to write, before it writes them
/// ([`SqlConnector::sync_pipe_inplace_observed`]): the engine records its dirty-range ledger
/// here, so the commit that follows keeps v4's in-place semantics (the definition's column
/// types, no frame round trip).
#[meerschaum_connector::async_trait]
pub trait InplaceObserver: Send + Sync {
    /// `delta` has one row per row about to be inserted, updated or upserted, with only the
    /// pipe's datetime column (no columns when the pipe has none). `Err` aborts the sync with
    /// nothing merged.
    async fn before_merge(&self, delta: &meerschaum_kernel::Frame) -> Result<(), String>;

    /// A replacing in-place sync is about to delete the rows of `[begin, end)` (`None`:
    /// unbounded on that side) and insert `delta`'s rows in their place: called instead of
    /// [`before_merge`](Self::before_merge). The window's rows may be deleted without a
    /// replacement, so a ledger marks the whole window. The default hands over `delta` only.
    async fn before_replace(
        &self,
        delta: &meerschaum_kernel::Frame,
        begin: Option<&meerschaum_connector::DateBound>,
        end: Option<&meerschaum_connector::DateBound>,
    ) -> Result<(), String> {
        let _ = (begin, end);
        self.before_merge(delta).await
    }
}

/// `session_execute(session, query, with_results=True)`: run one statement in the transaction,
/// rolling it back on failure. Returns the rows affected (`-1` for `CREATE TABLE ... AS` on SQLite,
/// as `sqlite3` reports it).
async fn session_execute(
    tx: &mut Option<SqlTransaction>,
    query: &str,
    flavor: Flavor,
) -> Result<i64, String> {
    let Some(t) = tx.as_mut() else {
        return Err("Failed to execute queries.\nThe session was rolled back.".into());
    };
    match t.exec(query).await {
        Ok(n) => {
            let ctas = query
                .trim_start()
                .to_uppercase()
                .starts_with("CREATE TABLE");
            if ctas && flavor.driver_family() == DriverFamily::Sqlite {
                Ok(-1)
            } else {
                Ok(n as i64)
            }
        }
        Err(e) => {
            if let Some(t) = tx.take() {
                let _ = t.rollback().await;
            }
            Err(format!("Failed to execute queries.\n{e}"))
        }
    }
}

impl SqlConnector {
    /// `get_table_cols_types(table, connectable=session, schema, database)` inside an open
    /// transaction (the temporary tables are not visible to other connections yet).
    async fn tx_table_cols_types(
        &self,
        tx: &mut Option<SqlTransaction>,
        table: &str,
        schema: Option<&str>,
    ) -> IndexMap<String, String> {
        let Some(t) = tx.as_mut() else {
            return IndexMap::new();
        };
        let query = get_table_cols_types_query(table, self.flavor);
        let Ok(df) = t.read_frame(&query).await else {
            return IndexMap::new();
        };
        let rows: Vec<ColumnTypeRow> = frame_docs(&df)
            .iter()
            .filter_map(|d| {
                Some(ColumnTypeRow {
                    database: kv_str(d.get("database")),
                    schema: kv_str(d.get("schema")),
                    table: kv_str(d.get("table")),
                    column: kv_str(d.get("column"))?,
                    type_: kv_str(d.get("type")).unwrap_or_default(),
                    numeric_precision: kv_int(d.get("numeric_precision")),
                    numeric_scale: kv_int(d.get("numeric_scale")),
                })
            })
            .collect();
        let database = match self.flavor {
            Flavor::Sqlite | Flavor::Geopackage | Flavor::Duckdb | Flavor::Oracle => None,
            _ => self.database(),
        };
        // NOTE: Check for PostGIS GEOMETRY columns.
        let mut geometry = IndexMap::new();
        if self.flavor.driver_family() == DriverFamily::Postgres
            && crate::queries::table::needs_postgis_lookup(&rows)
        {
            let ischema = crate::queries::table::introspection_schema(self.flavor, schema);
            if let Ok(q) = crate::queries::table::get_postgis_geo_columns_types_query(
                table,
                ischema.as_deref(),
            ) && let Some(t) = tx.as_mut()
                && let Ok(gdf) = t.read_frame(&q).await
            {
                let geo_rows: Vec<(String, String, String, Option<i64>)> = frame_docs(&gdf)
                    .iter()
                    .filter_map(|d| {
                        Some((
                            kv_str(d.get("column"))?,
                            kv_str(d.get("func"))?,
                            kv_str(d.get("type")).unwrap_or_default(),
                            kv_int(d.get("srid")),
                        ))
                    })
                    .collect();
                geometry = crate::queries::table::postgis_geo_columns_types_from_rows(&geo_rows);
            }
        }
        cols_types_from_rows(&rows, self.flavor, schema, database, &geometry)
    }

    /// `sync_pipe_inplace(pipe, params, begin, end, check_existing)`: sync a pipe whose SQL
    /// definition runs on this same database without reading rows into memory.
    pub async fn sync_pipe_inplace(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        begin: Bound,
        end: Bound,
        check_existing: bool,
    ) -> SuccessTuple {
        self.sync_pipe_inplace_with(pipe, params, begin, end, check_existing, false, None)
            .await
    }

    /// [`SqlConnector::sync_pipe_inplace`] (or, with `observer`,
    /// [`SqlConnector::sync_pipe_inplace_observed`]) that may replace the fetched window:
    /// with `replace` (or the pipe's `fetch: {replace: true}`) the rows of `[begin, end)`
    /// (matching `params`) are deleted and the definition's rows of that window inserted, in
    /// one transaction, instead of diffing them. A first sync creates the table as usual.
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_pipe_inplace_replacing(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        begin: Bound,
        end: Bound,
        check_existing: bool,
        replace: bool,
        observer: Option<&dyn InplaceObserver>,
    ) -> SuccessTuple {
        self.sync_pipe_inplace_with(pipe, params, begin, end, check_existing, replace, observer)
            .await
    }

    /// [`SqlConnector::sync_pipe_inplace`], handing `observer` the rows it is about to write
    /// (their datetime column only) right before it writes them; an `Err` from the observer
    /// aborts the sync with nothing merged. Costs one extra `SELECT` of that column.
    pub async fn sync_pipe_inplace_observed(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        begin: Bound,
        end: Bound,
        check_existing: bool,
        observer: &dyn InplaceObserver,
    ) -> SuccessTuple {
        self.sync_pipe_inplace_with(
            pipe,
            params,
            begin,
            end,
            check_existing,
            false,
            Some(observer),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync_pipe_inplace_with(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        begin: Bound,
        end: Bound,
        check_existing: bool,
        replace: bool,
        observer: Option<&dyn InplaceObserver>,
    ) -> SuccessTuple {
        if let Some(refused) = self.protected_refusal(pipe, "sync") {
            return refused;
        }
        let flavor = self.flavor;
        let pipe = &self.prepare_pipe(pipe).await;
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let state = self.pipe_state(&sp).await;
        let internal_schema = self.internal_schema().map(str::to_string);
        let isch = internal_schema.as_deref();
        let target = sp.target.clone();
        let transact_id = meerschaum_core::users::generate_password(
            self.settings.temporary_target.transaction_id_length,
        )
        .unwrap_or_else(|_| "0000".into());
        let temp_tables: IndexMap<&str, String> = TEMP_TABLE_ROOTS
            .iter()
            .map(|r| {
                (
                    *r,
                    self.get_temporary_target(&target, Some(&transact_id), Some(r)),
                )
            })
            .collect();
        let names: IndexMap<&str, String> = temp_tables
            .iter()
            .map(|(r, t)| (*r, sql_item_name(t, flavor, isch)))
            .collect();
        let aliases: IndexMap<&str, String> = TEMP_TABLE_ROOTS
            .iter()
            .map(|r| (*r, sql_item_name(r, flavor, None)))
            .collect();
        let alias_as = if flavor != Flavor::Oracle { " AS" } else { "" };

        // `begin=''` uses the instance's sync time minus the backtrack window.
        let sync_time = if begin == Bound::Default {
            self.sync_time(&sp, &state, None, true).await.and_then(|b| {
                self.parse_date_bound(&sp, &state.dtypes, Some(b))
                    .ok()
                    .flatten()
            })
        } else {
            None
        };
        let metadef = match self.get_pipe_metadef_for_sync(
            pipe,
            &FetchArgs {
                begin: begin.clone(),
                end: end.clone(),
                check_existing,
                params: params.map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                sync_time,
                parent: None,
            },
        ) {
            Ok(Some(m)) => m,
            Ok(None) => {
                return SuccessTuple::fail(format!(
                    "Could not determine a SQL definition for {pipe}."
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let pipe_name = self.pipe_table_name(&sp);
        let upsert = sp.upsert() && has_update_query(&format!("{}-upsert", flavor.as_str()));
        let is_static = sp.is_static();
        let primary_key = sp.column("primary");
        let primary_key_db_type = primary_key
            .as_ref()
            .and_then(|pk| state.dtypes.get(pk))
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        if super::data::columns_map(&sp).is_empty() {
            return SuccessTuple::fail("Cannot sync in-place without index columns.");
        }
        let autoincrement = sp.autoincrement();
        let dt_col = sp.column("datetime");
        let dt_col_name = dt_col.as_deref().map(|c| sql_item_name(c, flavor, None));
        // What `observer` reads: the datetime column of the rows about to be written.
        let observed_col = dt_col_name.clone().unwrap_or_else(|| "1".into());
        let dt_typ = dt_col.as_ref().map(|c| {
            state
                .dtypes
                .get(c)
                .cloned()
                .unwrap_or_else(|| "datetime".into())
        });
        let dt_db_type = dt_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        // SQLite has no datetime type: datetimes are text, compared as text. A definition's
        // computed datetime (`date(ts)` is '2024-01-03') is rewritten in the canonical form the
        // other syncs write (`YYYY-MM-DD HH:MM:SS.ffffff`), so that bounds, joins and re-syncs
        // compare like for like (rc15 B1).
        let sqlite_family = matches!(flavor, Flavor::Sqlite | Flavor::Geopackage);
        let sqlite_dt_cols: Vec<String> = if sqlite_family {
            state
                .dtypes
                .iter()
                .filter(|(_, t)| are_dtypes_equal(t, "datetime") && !are_dtypes_equal(t, "int"))
                .map(|(c, _)| c.clone())
                .collect()
        } else {
            Vec::new()
        };
        // `replace` (rc15 Q19): the fetched window is authoritative.
        let replace = replace || fetch_replace(pipe);
        if replace && (matches!(begin, Bound::Str(_)) || matches!(end, Bound::Str(_))) {
            return SuccessTuple::fail(format!(
                "Cannot replace a window of {pipe} bounded by a SQL expression: pass datetimes or integers as `begin` / `end`."
            ));
        }

        let clean_up = |ready_to_drop: bool| {
            let tables: Vec<String> = if !upsert {
                temp_tables.values().cloned().collect()
            } else {
                vec![temp_tables["update"].clone()]
            };
            async move {
                let (ok, msg) = self
                    .log_temporary_tables_creation(&tables, ready_to_drop, !pipe.temporary)
                    .await;
                if !ok {
                    tracing::warn!("{msg}");
                }
                let (ok, msg) = self.drop_old_temporary_tables(false).await;
                if !ok {
                    tracing::warn!("{msg}");
                }
            }
        };

        if !state.exists && sqlite_family {
            // Stage the definition's rows, type every column (the pipe's dtypes, the staged
            // declared types, else the values' storage classes), then create the table typed
            // and copy the rows: `CREATE TABLE ... AS SELECT` leaves computed columns untyped.
            let schema = self.get_pipe_schema(&sp);
            let staged = &temp_tables["new"];
            let staged_name = &names["new"];
            let stage = match get_create_table_queries(
                &QueryOrDtypes::Query(metadef.clone()),
                staged,
                flavor,
                &CreateTableOptions {
                    schema: internal_schema.clone(),
                    ..Default::default()
                },
            ) {
                Ok(q) => q,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            if !self.exec_queries_all(&stage, false, true).await {
                clean_up(false).await;
                return SuccessTuple::fail(format!(
                    "Could not stage new data for {pipe} from its SQL query definition."
                ));
            }
            let staged_types = self.get_table_cols_types(staged, isch).await;
            let canonical: Vec<String> =
                sqlite_canonical_updates(staged_name, staged_types.keys(), &sqlite_dt_cols, flavor);
            if !canonical.is_empty() && !self.exec_queries_all(&canonical, true, true).await {
                clean_up(false).await;
                return SuccessTuple::fail(format!(
                    "Could not normalize the datetimes of new data for {pipe}."
                ));
            }
            if let Some(o) = observer {
                let query = format!("SELECT {observed_col} FROM {staged_name}");
                let observed = match self.driver().read_frame(&query).await {
                    Ok(delta) => o.before_merge(&delta).await,
                    Err(e) => Err(e.to_string()),
                };
                if let Err(e) = observed {
                    clean_up(false).await;
                    return SuccessTuple::fail(e);
                }
            }
            if !staged_types.values().any(|t| untyped(t)) {
                // Every column is typed (plain column references): `CREATE TABLE ... AS` from
                // the staged rows keeps those types, as v4 does.
                let opts = CreateTableOptions {
                    schema: schema.clone(),
                    primary_key: primary_key.clone(),
                    primary_key_db_type: primary_key_db_type.clone(),
                    autoincrement,
                    datetime_column: dt_col.clone(),
                    ..Default::default()
                };
                let queries = match get_create_table_queries(
                    &QueryOrDtypes::Query(format!("SELECT * FROM {staged_name}")),
                    &target,
                    flavor,
                    &opts,
                ) {
                    Ok(q) => q,
                    Err(e) => return SuccessTuple::fail(e.to_string()),
                };
                if !self.exec_queries_all(&queries, false, true).await {
                    clean_up(false).await;
                    return SuccessTuple::fail(format!(
                        "Could not insert new data into {pipe} from its SQL query definition."
                    ));
                }
                if !self.create_indices_sp(&sp, None, None, None).await {
                    tracing::warn!("Failed to create indices for {pipe}. Continuing...");
                }
                let rowcount = self
                    .get_pipe_rowcount(pipe, &DataQuery::default())
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                clean_up(true).await;
                return SuccessTuple::ok(format!(
                    "Inserted {}, updated 0 rows.",
                    fmt_thousands(rowcount)
                ));
            }
            let db_types = self
                .sqlite_typed_columns(staged_name, &staged_types, &state.explicit_dtypes)
                .await;
            let create = match get_create_table_queries(
                &QueryOrDtypes::Dtypes(db_types.clone()),
                &target,
                flavor,
                &CreateTableOptions {
                    schema: schema.clone(),
                    primary_key: primary_key.clone(),
                    primary_key_db_type: primary_key
                        .as_ref()
                        .and_then(|pk| db_types.get(pk))
                        .filter(|t| !t.is_empty())
                        .cloned()
                        .or(primary_key_db_type.clone()),
                    autoincrement,
                    datetime_column: dt_col.clone(),
                    parse_dtypes: false,
                    ..Default::default()
                },
            ) {
                Ok(q) => q,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            let cols = db_types
                .keys()
                .map(|c| sql_item_name(c, flavor, None))
                .collect::<Vec<_>>()
                .join(", ");
            let insert =
                format!("INSERT INTO {pipe_name} ({cols})\nSELECT {cols}\nFROM {staged_name}");
            let mut tx = match self.driver().begin_write().await {
                Ok(t) => Some(t),
                Err(e) => {
                    clean_up(false).await;
                    return SuccessTuple::fail(e.to_string());
                }
            };
            for query in &create {
                self.log(query);
                if let Err(e) = session_execute(&mut tx, query, flavor).await {
                    clean_up(false).await;
                    return SuccessTuple::fail(e);
                }
            }
            self.log(&insert);
            let inserted = match session_execute(&mut tx, &insert, flavor).await {
                Ok(n) => n,
                Err(e) => {
                    clean_up(false).await;
                    return SuccessTuple::fail(e);
                }
            };
            if let Some(t) = tx.take()
                && let Err(e) = t.commit().await
            {
                clean_up(false).await;
                return SuccessTuple::fail(e.to_string());
            }
            if !self.create_indices_sp(&sp, None, None, None).await {
                tracing::warn!("Failed to create indices for {pipe}. Continuing...");
            }
            clean_up(true).await;
            return SuccessTuple::ok(format!(
                "Inserted {}, updated 0 rows.",
                fmt_thousands(inserted)
            ));
        }

        if !state.exists {
            let schema = self.get_pipe_schema(&sp);
            let opts = CreateTableOptions {
                schema: schema.clone(),
                primary_key: primary_key.clone(),
                primary_key_db_type: primary_key_db_type.clone(),
                autoincrement,
                datetime_column: dt_col.clone(),
                ..Default::default()
            };
            let mut queries = match get_create_table_queries(
                &QueryOrDtypes::Query(metadef.clone()),
                &target,
                flavor,
                &opts,
            ) {
                Ok(q) => q,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            if let Some(s) = schema.as_deref().filter(|s| !s.is_empty())
                && let Ok(mut q) = get_create_schema_if_not_exists_queries(Some(s), flavor)
            {
                q.extend(queries);
                queries = q;
            }
            if let Some(o) = observer {
                // Stage the definition's rows once, so the table is created from exactly the
                // rows the observer saw (CREATE TABLE ... AS keeps the definition's types).
                let staged = &temp_tables["new"];
                let mut stage =
                    get_create_schema_if_not_exists_queries(isch, flavor).unwrap_or_default();
                match get_create_table_queries(
                    &QueryOrDtypes::Query(metadef.clone()),
                    staged,
                    flavor,
                    &CreateTableOptions {
                        schema: internal_schema.clone(),
                        ..Default::default()
                    },
                ) {
                    Ok(q) => stage.extend(q),
                    Err(e) => return SuccessTuple::fail(e.to_string()),
                }
                if !self.exec_queries_all(&stage, false, true).await {
                    clean_up(false).await;
                    return SuccessTuple::fail(format!(
                        "Could not stage new data for {pipe} from its SQL query definition."
                    ));
                }
                let query = format!("SELECT {observed_col} FROM {}", names["new"]);
                let observed = match self.driver().read_frame(&query).await {
                    Ok(delta) => o.before_merge(&delta).await,
                    Err(e) => Err(e.to_string()),
                };
                if let Err(e) = observed {
                    clean_up(false).await;
                    return SuccessTuple::fail(e);
                }
                queries = match get_create_table_queries(
                    &QueryOrDtypes::Query(format!("SELECT * FROM {}", names["new"])),
                    &target,
                    flavor,
                    &opts,
                ) {
                    Ok(q) => q,
                    Err(e) => return SuccessTuple::fail(e.to_string()),
                };
                if let Some(s) = schema.as_deref().filter(|s| !s.is_empty())
                    && let Ok(mut q) = get_create_schema_if_not_exists_queries(Some(s), flavor)
                {
                    q.extend(queries);
                    queries = q;
                }
            }
            if !self.exec_queries_all(&queries, false, true).await {
                clean_up(false).await;
                return SuccessTuple::fail(format!(
                    "Could not insert new data into {pipe} from its SQL query definition."
                ));
            }
            if !self.create_indices_sp(&sp, None, None, None).await {
                tracing::warn!("Failed to create indices for {pipe}. Continuing...");
            }
            let rowcount = self
                .get_pipe_rowcount(pipe, &DataQuery::default())
                .await
                .ok()
                .flatten()
                .unwrap_or(0);
            clean_up(false).await;
            return SuccessTuple::ok(format!(
                "Inserted {}, updated 0 rows.",
                fmt_thousands(rowcount)
            ));
        }

        let mut tx: Option<SqlTransaction> = match self.driver().begin_write().await {
            Ok(t) => Some(t),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let ctas = |query: &str, table: &str| -> Result<String, String> {
            get_create_table_queries(
                &QueryOrDtypes::Query(query.to_string()),
                table,
                flavor,
                &CreateTableOptions {
                    schema: internal_schema.clone(),
                    ..Default::default()
                },
            )
            .map(|mut q| q.remove(0))
            .map_err(|e| e.to_string())
        };
        macro_rules! fail {
            ($msg:expr) => {{
                if let Some(t) = tx.take() {
                    let _ = t.rollback().await;
                }
                clean_up(false).await;
                return SuccessTuple::fail($msg);
            }};
        }
        // Hand `observer` the rows about to be written (inside the transaction).
        // On SQLite (one writer at a time) the observer's own writes would wait on this
        // session's write lock, so the staged tables are committed first (v4's `sqlite3`
        // autocommits that DDL anyway), the observer runs outside the session, and a new
        // session performs the merge.
        // `observe!(query, window)`: a replacing sync hands over its window too.
        macro_rules! observe {
            ($query:expr) => {
                observe!($query, None)
            };
            ($query:expr, $window:expr) => {{
                if let Some(o) = observer {
                    let query: String = $query;
                    let window: Option<(
                        Option<&meerschaum_connector::DateBound>,
                        Option<&meerschaum_connector::DateBound>,
                    )> = $window;
                    if flavor.driver_family() == DriverFamily::Sqlite {
                        if let Some(t) = tx.take() {
                            if let Err(e) = t.commit().await {
                                fail!(e.to_string());
                            }
                        }
                        let delta = match self.driver().read_frame(&query).await {
                            Ok(delta) => delta,
                            Err(e) => fail!(e.to_string()),
                        };
                        let observed = match window {
                            Some((b, e)) => o.before_replace(&delta, b, e).await,
                            None => o.before_merge(&delta).await,
                        };
                        if let Err(e) = observed {
                            fail!(e);
                        }
                        // The next session takes the write lock when it begins, so it begins only
                        // after the observer: the observer's own writes go through another
                        // connection and would wait for that lock for as long as the session holds it.
                        tx = match self.driver().begin_write().await {
                            Ok(t) => Some(t),
                            Err(e) => fail!(e.to_string()),
                        };
                    } else {
                        let read = match tx.as_mut() {
                            Some(t) => t.read_frame(&query).await.map_err(|e| e.to_string()),
                            None => Err("The session was rolled back.".to_string()),
                        };
                        match read {
                            Ok(delta) => {
                                let observed = match window {
                                    Some((b, e)) => o.before_replace(&delta, b, e).await,
                                    None => o.before_merge(&delta).await,
                                };
                                if let Err(e) = observed {
                                    fail!(e);
                                }
                            }
                            Err(e) => fail!(e),
                        }
                    }
                }
            }};
        }

        let new_root = if !upsert { "new" } else { "update" };
        let create_new_query = match ctas(&metadef, &temp_tables[new_root]) {
            Ok(q) => q,
            Err(e) => fail!(e),
        };
        self.log(&create_new_query);
        let new_count = match session_execute(&mut tx, &create_new_query, flavor).await {
            Ok(n) => n,
            Err(e) => fail!(e),
        };
        let mut new_cols_types = if !is_static {
            self.tx_table_cols_types(&mut tx, &temp_tables[new_root], isch)
                .await
        } else {
            state.cols_types.clone()
        };
        if sqlite_family {
            for query in sqlite_canonical_updates(
                &names[new_root],
                new_cols_types.keys(),
                &sqlite_dt_cols,
                flavor,
            ) {
                self.log(&query);
                if let Err(e) = session_execute(&mut tx, &query, flavor).await {
                    fail!(e);
                }
            }
            // Computed columns are untyped on SQLite: type them like a new table would be.
            if !is_static && new_cols_types.values().any(|t| untyped(t)) {
                let typed = self
                    .sqlite_typed_columns_tx(
                        &mut tx,
                        &names[new_root],
                        &new_cols_types,
                        &state.explicit_dtypes,
                    )
                    .await;
                for (c, t) in typed {
                    if let Some(slot) = new_cols_types.get_mut(&c)
                        && untyped(slot)
                    {
                        *slot = t;
                    }
                }
            }
        }
        if new_cols_types.is_empty() {
            if let Some(t) = tx.take() {
                let _ = t.rollback().await;
            }
            return SuccessTuple::fail(format!("Failed to get new columns for {pipe}."));
        }
        let new_cols: IndexMap<String, String> = new_cols_types
            .iter()
            .map(|(c, t)| (c.clone(), get_pd_type_from_db_type(t, true)))
            .collect();
        let q = |c: &str| sql_item_name(c, flavor, None);
        let new_cols_str = format!(
            "\n    {}",
            new_cols
                .keys()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(",\n    ")
        );
        let get_col_typ = |col: &str, cols_types: &IndexMap<String, String>| -> String {
            if flavor == Flavor::Oracle
                && new_cols_types
                    .get(col)
                    .is_some_and(|t| t.eq_ignore_ascii_case("char"))
            {
                return new_cols_types[col].clone();
            }
            cols_types.get(col).cloned().unwrap_or_default()
        };

        // Add new columns and retype altered ones on their own connection, like v4. On SQLite,
        // v4's `sqlite3` driver autocommits DDL (`CREATE TABLE ... AS` included), so the staged
        // table is committed first and the session resumes afterwards.
        // DuckDB gets the same treatment for another reason: its transactions are snapshots, so the
        // open session cannot see a column that `ALTER TABLE ... ADD` adds on another connection
        // after it began (`Referenced column "a" not found` in the backtrack query). Committing the
        // staged tables first and starting a new session afterwards gives it the new schema.
        let sqlite = matches!(
            flavor.driver_family(),
            DriverFamily::Sqlite | DriverFamily::Duckdb
        );
        if sqlite
            && let Some(t) = tx.take()
            && let Err(e) = t.commit().await
        {
            fail!(e.to_string());
        }
        let pipe_schema = self.get_pipe_schema(&sp);
        let table_cols_types = self
            .get_table_cols_types(&target, pipe_schema.as_deref())
            .await;
        let add_cols_queries = self.add_columns_queries_for(&sp, &new_cols, &table_cols_types);
        if !add_cols_queries.is_empty() {
            let _ = self.exec_queries(&add_cols_queries, false, true).await;
        }
        let table_cols_types = self
            .get_table_cols_types(&target, pipe_schema.as_deref())
            .await;
        let state = self.pipe_state(&sp).await;
        let input = super::sync::AlterColumnsInput {
            numeric_cols: new_cols
                .iter()
                .filter(|(_, t)| t.starts_with("numeric"))
                .map(|(c, _)| c.clone())
                .collect(),
            df_cols_types: new_cols.clone(),
            db_cols_types: table_cols_types,
            pipe_dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
            new_special_cols: IndexMap::new(),
            session_id: meerschaum_core::users::generate_password(3)
                .unwrap_or_else(|_| "000".into()),
        };
        let (alter_cols_queries, new_dtypes) = self.alter_columns_queries_for(&sp, &input);
        if !alter_cols_queries.is_empty() {
            let _ = self.exec_queries(&alter_cols_queries, false, true).await;
        }
        let mut updated = pipe.clone();
        if let Some(explicit) = new_dtypes {
            updated.set_dtypes(
                explicit
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(v)))
                    .collect(),
            );
            if !updated.temporary {
                let _ = self.edit_pipe(&updated, false).await;
            }
        }
        let sp = match self.sql_pipe(&updated) {
            Ok(sp) => sp,
            Err(e) => fail!(e.to_string()),
        };
        let state = self.pipe_state(&sp).await;
        if sqlite {
            tx = match self.driver().begin_write().await {
                Ok(t) => Some(t),
                Err(e) => fail!(e.to_string()),
            };
        }

        // The window a replacing sync deletes (`[begin, end)`, the sync time minus the backtrack
        // for the default `begin`).
        let replace_window = if replace {
            let window_begin = match &begin {
                // As `build_data_query` resolves the default `begin`.
                Bound::Default => match self.sync_time(&sp, &state, None, true).await {
                    Some(b) => match sp.backtrack_interval(check_existing) {
                        Ok(bt) => self
                            .parse_date_bound(
                                &sp,
                                &state.dtypes,
                                Some(super::data::subtract_interval(b, bt)),
                            )
                            .ok()
                            .flatten(),
                        Err(e) => fail!(e.to_string()),
                    },
                    None => None,
                },
                Bound::DateTime(d) => Some(meerschaum_connector::DateBound::Datetime(
                    super::values::kernel_datetime(d),
                )),
                Bound::Int(i) => Some(meerschaum_connector::DateBound::Int(*i)),
                Bound::None | Bound::Str(_) => None,
            };
            let window_end = match &end {
                Bound::DateTime(d) => Some(meerschaum_connector::DateBound::Datetime(
                    super::values::kernel_datetime(d),
                )),
                Bound::Int(i) => Some(meerschaum_connector::DateBound::Int(*i)),
                _ => None,
            };
            Some((window_begin, window_end))
        } else {
            None
        };

        // SQLite: rows an older sync stored with non-canonical datetime text inside the window
        // are rewritten in the canonical form first, so that the comparisons below find them.
        // `OR IGNORE`: a row whose canonical form already exists under a unique key is left.
        if sqlite_family && let Some(dt) = dt_col_name.as_deref() {
            let repair = match &replace_window {
                Some((b, e)) => sqlite_repair_window_query(
                    &pipe_name,
                    dt,
                    b.as_ref().map(|b| self.sqlite_bound_sql(b)),
                    e.as_ref().map(|e| self.sqlite_bound_sql(e)),
                ),
                None => sqlite_repair_window_query(
                    &pipe_name,
                    dt,
                    Some(format!("(SELECT MIN({dt}) FROM {})", names[new_root])),
                    Some(format!("(SELECT MAX({dt}) FROM {})", names[new_root])),
                ),
            };
            if sqlite_dt_cols
                .iter()
                .any(|c| sql_item_name(c, flavor, None) == dt)
            {
                self.log(&repair);
                if let Err(e) = session_execute(&mut tx, &repair, flavor).await {
                    fail!(e);
                }
            }
        }

        if let Some((window_begin, window_end)) = replace_window {
            let query = DataQuery {
                begin: window_begin,
                end: window_end,
                params: params.cloned(),
                ..Default::default()
            };
            // One predicate for both statements: the staged rows inserted are exactly the rows of
            // the window deleted. The definition's own bounds may be wider (SQLite's
            // `datetime(...)` drops fractions of a second), and a row outside the window would be
            // inserted next to its old copy.
            let window_where = match self.replace_window_where(&sp, &state, &query) {
                Ok(w) => w,
                Err(e) => fail!(e.to_string()),
            };
            observe!(
                format!(
                    "SELECT {observed_col} FROM {}{alias_as} {}\n{window_where}",
                    names[new_root], aliases["new"]
                ),
                Some((query.begin.as_ref(), query.end.as_ref()))
            );
            let delete = format!("DELETE FROM {pipe_name}\n{window_where}");
            self.log(&delete);
            let deleted = match session_execute(&mut tx, &delete, flavor).await {
                Ok(n) => n,
                Err(e) => fail!(e),
            };
            let insert = format!(
                "INSERT INTO {pipe_name} ({new_cols_str})\nSELECT {new_cols_str}\nFROM {}{alias_as} {}\n{window_where}",
                names[new_root], aliases["new"]
            );
            self.log(&insert);
            let inserted = match session_execute(&mut tx, &insert, flavor).await {
                Ok(n) => n,
                Err(e) => fail!(e),
            };
            // The staged rows outside the window (`CREATE TABLE ... AS` on SQLite reports no
            // count: ask).
            let staged = if new_count >= 0 {
                Some(new_count)
            } else {
                let count = format!("SELECT COUNT(*) AS {} FROM {}", q("n"), names[new_root]);
                match tx.as_mut() {
                    Some(t) => t
                        .read_frame(&count)
                        .await
                        .ok()
                        .and_then(|df| frame_docs(&df).into_iter().next())
                        .and_then(|d| kv_int(d.get("n"))),
                    None => None,
                }
            };
            let skipped = staged.map_or(0, |n| (n - inserted.max(0)).max(0) as usize);
            if let Some(t) = tx.take()
                && let Err(e) = t.commit().await
            {
                clean_up(false).await;
                return SuccessTuple::fail(e.to_string());
            }
            clean_up(true).await;
            let result = super::sync::replace_result(
                deleted.max(0) as u64,
                inserted.max(0) as u64,
                skipped,
                true,
            );
            return SuccessTuple::ok(result.message);
        }

        if !check_existing && !upsert {
            observe!(format!("SELECT {observed_col} FROM {}", names["new"]));
            let insert_query = format!(
                "INSERT INTO {pipe_name} ({new_cols_str})\nSELECT {new_cols_str}\nFROM {}{alias_as} {}",
                names["new"], aliases["new"]
            );
            self.log(&insert_query);
            if let Err(e) = session_execute(&mut tx, &insert_query, flavor).await {
                fail!(e);
            }
        }
        if !check_existing {
            if let Some(t) = tx.take()
                && let Err(e) = t.commit().await
            {
                fail!(e.to_string());
            }
            clean_up(false).await;
            return SuccessTuple::ok(format!("Inserted {new_count}, updated 0 rows."));
        }

        let mut begin_bound = match &begin {
            Bound::DateTime(d) => Some(meerschaum_connector::DateBound::Datetime(
                super::values::kernel_datetime(d),
            )),
            Bound::Int(i) => Some(meerschaum_connector::DateBound::Int(*i)),
            _ => None,
        };
        let mut end_bound = match &end {
            Bound::DateTime(d) => Some(meerschaum_connector::DateBound::Datetime(
                super::values::kernel_datetime(d),
            )),
            Bound::Int(i) => Some(meerschaum_connector::DateBound::Int(*i)),
            _ => None,
        };
        if let (Some(dt_name), false) = (&dt_col_name, upsert) {
            let da = |agg: &str| {
                dateadd_str(
                    Some(flavor),
                    Some("day"),
                    Number::Int(0),
                    &DateArg::Str(format!("{agg}({dt_name})")),
                    dt_db_type.as_deref(),
                )
            };
            let (min_da, max_da) = match (da("MIN"), da("MAX")) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => fail!(format!(
                    "Could not determine in-place datetime bounds:\n{e}"
                )),
            };
            let bounds_query = format!(
                "SELECT\n    {min_da} AS {},\n    {max_da} AS {}\nFROM {}\nWHERE {dt_name} IS NOT NULL",
                q("min_dt"),
                q("max_dt"),
                names[new_root]
            );
            self.log(&bounds_query);
            let df = match tx.as_mut() {
                Some(t) => t.read_frame(&bounds_query).await,
                None => fail!("Could not determine in-place datetime bounds.".to_string()),
            };
            let df = match df {
                Ok(df) => df,
                Err(e) => fail!(format!(
                    "Could not determine in-place datetime bounds:\nFailed to execute queries.\n{e}"
                )),
            };
            let doc = frame_docs(&df).into_iter().next().unwrap_or_default();
            let to_bound = |k: &str| {
                doc.get(k).and_then(|v| {
                    let sv = match v {
                        meerschaum_kernel::Value::Datetime(dt) => {
                            return Some(meerschaum_connector::DateBound::Datetime(dt.clone()));
                        }
                        meerschaum_kernel::Value::Int(i) => SqlValue::Int(*i),
                        meerschaum_kernel::Value::Str(s) => SqlValue::Text(s.clone()),
                        meerschaum_kernel::Value::Date(d) => SqlValue::Date(*d),
                        meerschaum_kernel::Value::Float(f) => SqlValue::Float(*f),
                        _ => return None,
                    };
                    sync_time_from_sql(&sv)
                })
            };
            begin_bound = to_bound("min_dt");
            end_bound = to_bound("max_dt");
        }

        // MSSQL cannot add minutes to a `DATE` (`DATEADD(minute, 1, CAST(... AS DATE))` fails):
        // a date-only datetime column takes the next day as its exclusive end instead.
        let date_only_dt = flavor == Flavor::Mssql
            && dt_col
                .as_ref()
                .and_then(|c| state.cols_types.get(c))
                .is_some_and(|t| t.eq_ignore_ascii_case("date"));
        if date_only_dt
            && let Some(meerschaum_connector::DateBound::Datetime(d)) = end_bound.as_mut()
        {
            d.utc += chrono::TimeDelta::days(1);
        }
        let backtrack_query = DataQuery {
            begin: begin_bound,
            end: end_bound,
            params: params.cloned(),
            ..Default::default()
        };
        let backtrack_def = match self
            .build_data_query(
                &sp,
                &state,
                &backtrack_query,
                &DataQueryOptions {
                    end_add_minutes: if date_only_dt { 0 } else { 1 },
                    no_order: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(q) => q,
            Err(e) => fail!(e.to_string()),
        };
        if !upsert {
            let create_backtrack_query = match ctas(&backtrack_def, &temp_tables["backtrack"]) {
                Ok(q) => q,
                Err(e) => fail!(e),
            };
            self.log(&create_backtrack_query);
            if let Err(e) = session_execute(&mut tx, &create_backtrack_query, flavor).await {
                fail!(e);
            }
        }
        let backtrack_cols_types = if !(upsert || is_static) {
            self.tx_table_cols_types(&mut tx, &temp_tables["backtrack"], isch)
                .await
        } else {
            new_cols_types.clone()
        };
        let common_cols: Vec<String> = new_cols
            .keys()
            .filter(|c| backtrack_cols_types.contains_key(*c))
            .cloned()
            .collect();
        let on_cols: IndexMap<String, String> = match &primary_key {
            None => super::data::columns_map(&sp)
                .into_iter()
                .filter(|(k, c)| {
                    k != "value" && backtrack_cols_types.contains_key(c) && new_cols.contains_key(c)
                })
                .map(|(_, c)| {
                    let t = new_cols.get(&c).cloned().unwrap_or_default();
                    (c, t)
                })
                .collect(),
            Some(pk) => {
                let mut m = IndexMap::new();
                m.insert(pk.clone(), new_cols.get(pk).cloned().unwrap_or_default());
                m
            }
        };
        if on_cols.is_empty() {
            fail!("Cannot sync without common index columns.".to_string());
        }

        let (a_new, a_bt, a_delta, a_joined) = (
            &aliases["new"],
            &aliases["backtrack"],
            &aliases["delta"],
            &aliases["joined"],
        );
        let null_replace_new_cols_str = format!(
            "\n    {}",
            new_cols
                .keys()
                .map(|col| format!(
                    "COALESCE({a_new}.{}, {}) AS {}",
                    q(col),
                    get_null_replacement(&get_col_typ(col, &new_cols_types), Some(flavor)),
                    q(col)
                ))
                .collect::<Vec<_>>()
                .join(",\n    ")
        );
        let select_delta_query = format!(
            "SELECT{null_replace_new_cols_str}\nFROM {}{alias_as} {a_new}\nLEFT OUTER JOIN {}{alias_as} {a_bt}\n    ON\n    {}\nWHERE\n    {}",
            names["new"],
            names["backtrack"],
            common_cols
                .iter()
                .map(|c| format!(
                    "    COALESCE({a_new}.{}, {})\n        =\n        COALESCE({a_bt}.{}, {}) ",
                    q(c),
                    get_null_replacement(&get_col_typ(c, &new_cols_types), Some(flavor)),
                    q(c),
                    get_null_replacement(&get_col_typ(c, &backtrack_cols_types), Some(flavor)),
                ))
                .collect::<Vec<_>>()
                .join("\n    AND\n    "),
            common_cols
                .iter()
                .map(|c| format!("{a_bt}.{} IS NULL", q(c)))
                .collect::<Vec<_>>()
                .join("\n    AND\n    "),
        );
        if !upsert {
            let create_delta_query = match ctas(&select_delta_query, &temp_tables["delta"]) {
                Ok(q) => q,
                Err(e) => fail!(e),
            };
            self.log(&create_delta_query);
            if let Err(e) = session_execute(&mut tx, &create_delta_query, flavor).await {
                fail!(e);
            }
        }
        let mut delta_cols_types = if !(upsert || is_static) {
            self.tx_table_cols_types(&mut tx, &temp_tables["delta"], isch)
                .await
        } else {
            new_cols_types.clone()
        };
        // This is a weird bug on SQLite: sometimes the dtypes are all empty strings.
        if delta_cols_types.values().any(|t| t.is_empty()) || delta_cols_types.is_empty() {
            delta_cols_types = new_cols_types.clone();
        }
        let delta_cols: IndexMap<String, String> = delta_cols_types
            .iter()
            .map(|(c, t)| (c.clone(), get_pd_type_from_db_type(t, true)))
            .collect();
        let delta_cols_str = delta_cols
            .keys()
            .map(|c| q(c))
            .collect::<Vec<_>>()
            .join(", ");

        let select_joined_query = format!(
            "SELECT\n    {},\n    {}\nFROM {}{alias_as} {a_delta}\nLEFT OUTER JOIN {}{alias_as} {a_bt}\n    ON\n    {}",
            delta_cols
                .keys()
                .map(|c| format!("{a_delta}.{} AS {}", q(c), q(&format!("{c}_delta"))))
                .collect::<Vec<_>>()
                .join(",\n    "),
            backtrack_cols_types
                .keys()
                .map(|c| format!("{a_bt}.{} AS {}", q(c), q(&format!("{c}_backtrack"))))
                .collect::<Vec<_>>()
                .join(",\n    "),
            names["delta"],
            names["backtrack"],
            on_cols
                .keys()
                .map(|c| format!(
                    "    COALESCE({a_delta}.{}, {})\n        =\n        COALESCE({a_bt}.{}, {})",
                    q(c),
                    get_null_replacement(&get_col_typ(c, &new_cols_types), Some(flavor)),
                    q(c),
                    get_null_replacement(&get_col_typ(c, &new_cols_types), Some(flavor)),
                ))
                .collect::<Vec<_>>()
                .join("\n    AND\n    "),
        );
        if !upsert {
            let create_joined_query = match ctas(&select_joined_query, &temp_tables["joined"]) {
                Ok(q) => q,
                Err(e) => fail!(e),
            };
            self.log(&create_joined_query);
            if let Err(e) = session_execute(&mut tx, &create_joined_query, flavor).await {
                fail!(e);
            }
        }

        let case_cols = delta_cols
            .keys()
            .map(|c| {
                format!(
                    "CASE\n        WHEN {} != {} THEN {}\n        ELSE NULL\n    END AS {}",
                    q(&format!("{c}_delta")),
                    get_null_replacement(&get_col_typ(c, &delta_cols_types), Some(flavor)),
                    q(&format!("{c}_delta")),
                    q(c)
                )
            })
            .collect::<Vec<_>>()
            .join(",\n    ");
        let select_unseen_query = format!(
            "SELECT\n    {case_cols}\nFROM {}{alias_as} {a_joined}\nWHERE\n    {}",
            names["joined"],
            delta_cols
                .keys()
                .map(|c| format!("{} IS NULL", q(&format!("{c}_backtrack"))))
                .collect::<Vec<_>>()
                .join("\n    AND\n    "),
        );
        if !upsert {
            let create_unseen_query = match ctas(&select_unseen_query, &temp_tables["unseen"]) {
                Ok(q) => q,
                Err(e) => fail!(e),
            };
            self.log(&create_unseen_query);
            if let Err(e) = session_execute(&mut tx, &create_unseen_query, flavor).await {
                fail!(e);
            }
        }
        let select_update_query = format!(
            "SELECT\n    {case_cols}\nFROM {}{alias_as} {a_joined}\nWHERE\n    {}",
            names["joined"],
            delta_cols
                .keys()
                .map(|c| format!("{} IS NOT NULL", q(&format!("{c}_backtrack"))))
                .collect::<Vec<_>>()
                .join("\n    OR\n    "),
        );
        if !upsert {
            let create_update_query = match ctas(&select_update_query, &temp_tables["update"]) {
                Ok(q) => q,
                Err(e) => fail!(e),
            };
            self.log(&create_update_query);
            if let Err(e) = session_execute(&mut tx, &create_update_query, flavor).await {
                fail!(e);
            }
        }

        let target_cols_types = self.columns_types(&sp).await;
        let schema = self.get_pipe_schema(&sp);
        let db_version = if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            self.db_version().await
        } else {
            None
        };
        let mut args = UpdateQueriesArgs::new(
            &target,
            &temp_tables["update"],
            on_cols.keys().cloned().collect(),
            flavor,
        );
        args.upsert = upsert;
        args.schema = schema.as_deref();
        args.patch_schema = isch;
        args.target_cols_types = target_cols_types;
        args.patch_cols_types = delta_cols_types.clone();
        args.datetime_col = dt_col.as_deref();
        args.null_indices = sp.null_indices();
        args.cast_columns = sp.enforce();
        args.db_version = db_version.as_deref();
        if upsert {
            // The unique key's own columns / expressions; where that key treats NULL keys as
            // distinct, the statements compare them (rc15 S3).
            let (target, key_null_safe) =
                match self.upsert_conflict_target(&sp, &args.join_cols).await {
                    Some((target, null_safe)) => (Some(target), null_safe),
                    None => (None, false),
                };
            args.conflict_target = target;
            args.null_safe_upsert = sp.null_indices()
                && pipe.connector_keys() != "mrsm"
                && (matches!(flavor, Flavor::Mysql | Flavor::Mariadb) || !key_null_safe);
        }
        let apply_update_queries = get_update_queries(&args);

        observe!(if upsert {
            format!("SELECT {observed_col} FROM {}", names["update"])
        } else {
            format!(
                "SELECT {observed_col} FROM {} UNION ALL SELECT {observed_col} FROM {}",
                names["unseen"], names["update"]
            )
        });
        let mut unseen_count = 0;
        if !upsert {
            let apply_unseen_query = format!(
                "INSERT INTO {pipe_name} ({delta_cols_str})\nSELECT {delta_cols_str}\nFROM {}",
                names["unseen"]
            );
            self.log(&apply_unseen_query);
            unseen_count = match session_execute(&mut tx, &apply_unseen_query, flavor).await {
                Ok(n) => n,
                Err(e) => fail!(e),
            };
        }
        let mut update_count = 0;
        for (i, query) in apply_update_queries.iter().enumerate() {
            self.log(query);
            match session_execute(&mut tx, query, flavor).await {
                // PARITY: only the first statement's rowcount is reported (`_pipes.py:2905`).
                Ok(n) if i == 0 => update_count = n,
                Ok(_) => {}
                Err(e) => fail!(e),
            }
        }
        if let Some(t) = tx.take()
            && let Err(e) = t.commit().await
        {
            clean_up(false).await;
            return SuccessTuple::fail(e.to_string());
        }
        let msg = if !upsert {
            format!(
                "Inserted {}, updated {} rows.",
                fmt_thousands(unseen_count),
                fmt_thousands(update_count)
            )
        } else {
            format!(
                "Upserted {} row{}.",
                fmt_thousands(update_count),
                if update_count != 1 { "s" } else { "" }
            )
        };
        clean_up(true).await;
        SuccessTuple::ok(msg)
    }
}

/// Whether a SQLite column has no declared type (a computed column of `CREATE TABLE ... AS`).
fn untyped(declared: &str) -> bool {
    declared.is_empty() || declared.eq_ignore_ascii_case("null")
}

/// Whether the pipe sets `fetch: {replace: true}` (each fetched window is authoritative).
fn fetch_replace(pipe: &Pipe) -> bool {
    pipe.param("fetch")
        .and_then(|f| f.get("replace"))
        .is_some_and(|v| v.truthy())
}

/// SQLite: `UPDATE` statements rewriting the staged table's datetime columns (those of `cols`
/// in `dt_cols`) in canonical text ([`sqlite_canonical_datetime`]).
fn sqlite_canonical_updates<'a>(
    table: &str,
    cols: impl Iterator<Item = &'a String>,
    dt_cols: &[String],
    flavor: Flavor,
) -> Vec<String> {
    cols.filter(|c| dt_cols.contains(c))
        .map(|c| {
            let q = sql_item_name(c, flavor, None);
            format!(
                "UPDATE {table} SET {q} = {} WHERE typeof({q}) = 'text' AND NOT ({q} GLOB '{SQLITE_STORED_DATETIME_GLOB}')",
                sqlite_canonical_datetime(&q)
            )
        })
        .collect()
}

/// SQLite: rewrite the target's non-canonical datetime text in a window, a coarse date-prefix
/// range around `[begin, end]` (SQL expressions; `None`: unbounded) that the datetime index can
/// drive, so that no row of the window escapes a text comparison.
fn sqlite_repair_window_query(
    table: &str,
    dt: &str,
    begin: Option<String>,
    end: Option<String>,
) -> String {
    let mut wheres = vec![
        format!("typeof({dt}) = 'text'"),
        format!("NOT ({dt} GLOB '{SQLITE_STORED_DATETIME_GLOB}')"),
    ];
    if let Some(b) = begin {
        wheres.insert(0, format!("{dt} >= date({b}, '-1 day')"));
    }
    if let Some(e) = end {
        wheres.insert(1.min(wheres.len()), format!("{dt} < date({e}, '+2 day')"));
    }
    format!(
        "UPDATE OR IGNORE {table} SET {dt} = {}\nWHERE {}",
        sqlite_canonical_datetime(dt),
        wheres.join("\n    AND ")
    )
}

/// The storage class names (`typeof`) of a column's values as a pandas dtype: `int64`,
/// `float64`, `string`, `bytes`, or `None` when mixed with text or unknown.
fn dtype_from_storage_classes(classes: &str) -> Option<&'static str> {
    let mut set: Vec<&str> = classes
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty() && *c != "null")
        .collect();
    set.sort_unstable();
    set.dedup();
    match set.as_slice() {
        ["integer"] => Some("int64"),
        ["real"] | ["integer", "real"] => Some("float64"),
        ["text"] => Some("string"),
        ["blob"] => Some("bytes"),
        _ => None,
    }
}

impl SqlConnector {
    /// A datetime / integer bound as a SQLite expression (`datetime('...', '0 day')` or the
    /// integer).
    fn sqlite_bound_sql(&self, bound: &meerschaum_connector::DateBound) -> String {
        match bound {
            meerschaum_connector::DateBound::Int(i) => i.to_string(),
            meerschaum_connector::DateBound::Datetime(d) => format!(
                "datetime('{}', '0 day')",
                d.utc.format("%Y-%m-%d %H:%M:%S%.6f")
            ),
        }
    }

    /// SQLite: the database types for a table created from the staged `table`. A column the
    /// staged table declares a type for (a plain column reference) keeps it, as v4's
    /// `CREATE TABLE ... AS` does; an untyped (computed) column takes the pipe's dtype, else the
    /// storage class of its values; `''` (no type) when none applies.
    async fn sqlite_typed_columns(
        &self,
        table: &str,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
    ) -> IndexMap<String, String> {
        let classes = match self
            .driver()
            .read_frame(&storage_classes_query(table, staged.keys(), self.flavor))
            .await
        {
            Ok(df) => frame_docs(&df).into_iter().next().unwrap_or_default(),
            Err(_) => Default::default(),
        };
        self.typed_from_classes(staged, explicit, &classes)
    }

    /// [`Self::sqlite_typed_columns`] inside the sync's transaction.
    async fn sqlite_typed_columns_tx(
        &self,
        tx: &mut Option<SqlTransaction>,
        table: &str,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
    ) -> IndexMap<String, String> {
        let classes = match tx.as_mut() {
            Some(t) => match t
                .read_frame(&storage_classes_query(table, staged.keys(), self.flavor))
                .await
            {
                Ok(df) => frame_docs(&df).into_iter().next().unwrap_or_default(),
                Err(_) => Default::default(),
            },
            None => Default::default(),
        };
        self.typed_from_classes(staged, explicit, &classes)
    }

    fn typed_from_classes(
        &self,
        staged: &IndexMap<String, String>,
        explicit: &IndexMap<String, String>,
        classes: &IndexMap<String, meerschaum_kernel::Value>,
    ) -> IndexMap<String, String> {
        staged
            .iter()
            .enumerate()
            .map(|(i, (col, declared))| {
                let typ = if !untyped(declared) {
                    declared.clone()
                } else if let Some(dtype) = explicit.get(col) {
                    get_db_type_from_pd_type(dtype, Some(self.flavor))
                } else {
                    kv_str(classes.get(&format!("c{i}")))
                        .as_deref()
                        .and_then(dtype_from_storage_classes)
                        .map(|t| get_db_type_from_pd_type(t, Some(self.flavor)))
                        .unwrap_or_default()
                };
                (col.clone(), typ)
            })
            .collect()
    }
}

/// One row: the distinct storage classes of each column's non-null values (`c0`, `c1`, ...).
fn storage_classes_query<'a>(
    table: &str,
    cols: impl Iterator<Item = &'a String>,
    flavor: Flavor,
) -> String {
    let selects = cols
        .enumerate()
        .map(|(i, c)| {
            let q = sql_item_name(c, flavor, None);
            format!(
                "group_concat(DISTINCT CASE WHEN {q} IS NOT NULL THEN typeof({q}) END) AS \"c{i}\""
            )
        })
        .collect::<Vec<_>>();
    format!("SELECT {} FROM {table}", selects.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_classes_type_untyped_columns() {
        assert_eq!(dtype_from_storage_classes("integer"), Some("int64"));
        assert_eq!(dtype_from_storage_classes("real,integer"), Some("float64"));
        assert_eq!(dtype_from_storage_classes("text"), Some("string"));
        assert_eq!(dtype_from_storage_classes("blob"), Some("bytes"));
        assert_eq!(dtype_from_storage_classes("text,integer"), None);
        assert_eq!(dtype_from_storage_classes(""), None);
    }

    #[test]
    fn repair_queries_bound_the_window() {
        let q = sqlite_repair_window_query("\"t\"", "\"day\"", Some("b".into()), Some("e".into()));
        assert!(
            q.starts_with("UPDATE OR IGNORE \"t\" SET \"day\" = (CASE"),
            "{q}"
        );
        assert!(q.contains("\"day\" >= date(b, '-1 day')"), "{q}");
        assert!(q.contains("\"day\" < date(e, '+2 day')"), "{q}");
        assert!(q.contains("typeof(\"day\") = 'text'"), "{q}");
    }
}
