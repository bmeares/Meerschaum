//! `sync_pipe_inplace` (`_pipes.py:2324`): sync a `sql:` pipe whose source connector is its
//! instance entirely inside the database, staging the fetch, backtrack, delta, joined, unseen
//! and update sets in temporary tables within one transaction (SQLAlchemy `Session`).

use indexmap::IndexMap;
use meerschaum_core::dtypes::get_pd_type_from_db_type;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};

use crate::dateadd::{DateArg, Number, dateadd_str};
use crate::driver::{SqlTransaction, SqlValue};
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::{DriverFamily, Flavor};
use crate::ident::sql_item_name;
use crate::queries::metadef::Bound;
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
            ) {
                if let Some(t) = tx.as_mut() {
                    if let Ok(gdf) = t.read_frame(&q).await {
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
                        geometry =
                            crate::queries::table::postgis_geo_columns_types_from_rows(&geo_rows);
                    }
                }
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
        let flavor = self.flavor;
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
        let metadef = match self.get_pipe_metadef(
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
            if let Some(s) = schema.as_deref().filter(|s| !s.is_empty()) {
                if let Ok(mut q) = get_create_schema_if_not_exists_queries(Some(s), flavor) {
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

        let mut tx: Option<SqlTransaction> = match self.driver().begin().await {
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
        let new_cols_types = if !is_static {
            self.tx_table_cols_types(&mut tx, &temp_tables[new_root], isch)
                .await
        } else {
            state.cols_types.clone()
        };
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
        let sqlite = flavor.driver_family() == DriverFamily::Sqlite;
        if sqlite {
            if let Some(t) = tx.take() {
                if let Err(e) = t.commit().await {
                    fail!(e.to_string());
                }
            }
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
            tx = match self.driver().begin().await {
                Ok(t) => Some(t),
                Err(e) => fail!(e.to_string()),
            };
        }

        if !check_existing && !upsert {
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
            if let Some(t) = tx.take() {
                if let Err(e) = t.commit().await {
                    fail!(e.to_string());
                }
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
                    end_add_minutes: 1,
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
        let apply_update_queries = get_update_queries(&args);

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
        if let Some(t) = tx.take() {
            if let Err(e) = t.commit().await {
                clean_up(false).await;
                return SuccessTuple::fail(e.to_string());
            }
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
