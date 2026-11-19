//! Replace-window, deduplication, upsert, and update helpers for [`SqlConnector`].

use super::*;

impl SqlConnector {
    /// The `DELETE` of a replacing sync's window (`[begin, end)` matching `params`): the
    /// `clear_pipe` query, except on SQLite, whose `datetime(...)` bounds drop fractions of a
    /// second (a window ending just after `00:00:00.5` would end at `00:00:00`): there the
    /// datetime bounds are the exact canonical text the rows are stored as.
    pub(crate) fn replace_delete_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        Ok(format!(
            "DELETE FROM {}\n{}",
            self.pipe_table_name(sp),
            self.replace_window_where(sp, state, query)?
        ))
    }

    /// The `WHERE` clause of a replacing sync's window ([`replace_delete_query`]'s): the
    /// in-place sync applies it to the staged rows too, so that it inserts exactly the rows of
    /// the window it deletes. Its column references are unqualified.
    ///
    /// [`replace_delete_query`]: Self::replace_delete_query
    pub(crate) fn replace_window_where(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        let delete = self.replace_window_delete(sp, state, query)?;
        let prefix = format!("DELETE FROM {}\n", self.pipe_table_name(sp));
        delete
            .strip_prefix(&prefix)
            .map(str::to_string)
            .ok_or_else(|| {
                be(format!(
                    "Unexpected clear query for {}:\n{delete}",
                    sp.pipe()
                ))
            })
    }

    pub(super) fn replace_window_delete(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        let flavor = self.flavor;
        let dt_col = sp.column("datetime");
        let text_datetimes = matches!(flavor, Flavor::Sqlite | Flavor::Geopackage)
            && dt_col.as_ref().is_some_and(|c| {
                state
                    .dtypes
                    .get(c)
                    .is_none_or(|t| !are_dtypes_equal(t, "int"))
            })
            && [&query.begin, &query.end]
                .iter()
                .any(|b| matches!(b, Some(meerschaum_connector::DateBound::Datetime(_))));
        if !text_datetimes {
            return self.get_clear_pipe_query(sp, state, query);
        }
        let without_bounds = meerschaum_connector::DataQuery {
            begin: None,
            end: None,
            ..query.clone()
        };
        let mut sql = self.get_clear_pipe_query(sp, state, &without_bounds)?;
        let dt = sql_item_name(&dt_col.unwrap_or_default(), flavor, None);
        let literal = |b: &meerschaum_connector::DateBound| match b {
            meerschaum_connector::DateBound::Datetime(d) => {
                format!("'{}'", d.utc.format("%Y-%m-%d %H:%M:%S%.6f"))
            }
            meerschaum_connector::DateBound::Int(i) => i.to_string(),
        };
        if let Some(b) = &query.begin {
            sql.push_str(&format!("\n    AND {dt} >= {}", literal(b)));
        }
        if let Some(e) = &query.end {
            sql.push_str(&format!("\n    AND {dt} <  {}", literal(e)));
        }
        Ok(sql)
    }

    /// `pipe.filter_existing(df)`: split a frame into `(unseen, update, delta)` against the
    /// rows already in the table (the kernel diff with the backtrack read from this instance).
    pub(crate) async fn filter_existing_frame(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        df: Frame,
    ) -> ConnectorResult<(Frame, Frame, Frame)> {
        let fe_params = FilterExistingParams {
            columns: columns_map(sp),
            configured_dtypes: state.dtypes.clone(),
            dtypes_ctx: self.dtypes_ctx(sp, state),
            autoincrement: sp.autoincrement(),
            autotime: sp.autotime(),
            static_: sp.is_static(),
            include_unchanged_columns: false,
            date_bound_only: false,
            enforce_dtypes: false,
            filter_params_index_limit: self.settings.filter_params_index_limit,
            // Declared text / index columns keep their null-like strings (rc15 Q18).
            literal_null_markers: null_marker_columns(sp, state),
        };
        // The kernel's callback is synchronous: capture the backtrack request first, fetch it,
        // then run the diff with the fetched frame.
        let mut request: Option<BacktrackRequest> = None;
        let _ = filter_existing(PipeInput::Frame(df.clone()), &fe_params, |req| {
            request = Some(req.clone());
            None
        });
        let backtrack = match &request {
            Some(req) if state.exists => {
                let q = meerschaum_connector::DataQuery {
                    begin: meerschaum_connector::DateBound::from_value(&req.begin),
                    end: meerschaum_connector::DateBound::from_value(&req.end),
                    params: Some(backtrack_params(req)),
                    ..Default::default()
                };
                self.get_pipe_data_with_state(
                    sp,
                    state,
                    &q,
                    &crate::instance::data::DataQueryOptions::default(),
                )
                .await?
            }
            _ => None,
        };
        filter_existing(PipeInput::Frame(df), &fe_params, move |_| backtrack).map_err(be)
    }

    /// Internal state updates need no staging table. Null/duplicate keys keep the general path.
    pub(super) async fn upsert_small_internal(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        df: &Frame,
    ) -> ConnectorResult<Option<(bool, String)>> {
        let cols = df.names();
        let mut keys = sp
            .column("primary")
            .map(|k| vec![k])
            .unwrap_or_else(|| columns_map(sp).into_values().collect());
        if (self.should_partition(sp) || self.flavor.is_timescaledb())
            && let Some(dt) = sp.column("datetime")
            && !keys.contains(&dt)
        {
            keys.push(dt);
        }
        let mut unique_keys = std::collections::HashSet::new();
        keys.retain(|key| unique_keys.insert(key.clone()));
        if keys.is_empty() || keys.iter().any(|k| !df.contains(k)) {
            return Ok(None);
        }
        let key_names = keys.clone();
        // DuckDB has no staged upsert template, so a batch repeating a key is collapsed to its
        // last row (applying the rows in order would leave the last one) instead of being
        // rejected. Null keys still cannot be upserted.
        let deduped;
        let df = if self.flavor == Flavor::Duckdb && df.height() > 1 {
            let key_values: Vec<Vec<KValue>> = key_names
                .iter()
                .map(|k| df.data(k).expect("key column").values())
                .collect();
            if key_values.iter().any(|v| v.iter().any(|x| x.is_na())) {
                return Ok(None);
            }
            let mut last: IndexMap<Vec<String>, usize> = IndexMap::new();
            for row in 0..df.height() {
                let key: Vec<String> = key_values.iter().map(|v| v[row].py_str()).collect();
                last.insert(key, row);
            }
            if last.len() == df.height() {
                df
            } else {
                let mut rows: Vec<usize> = last.into_values().collect();
                rows.sort_unstable();
                deduped = df.take(&rows);
                &deduped
            }
        } else {
            df
        };
        let columns: Vec<Vec<KValue>> = cols
            .iter()
            .map(|c| df.data(c).expect("column").values())
            .collect();
        let key_indices: Vec<usize> = keys
            .iter()
            .map(|k| cols.iter().position(|c| c == k).expect("key"))
            .collect();
        let mut seen = std::collections::HashSet::new();
        for (row, _) in columns[0].iter().enumerate() {
            let values: Vec<String> = key_indices
                .iter()
                .map(|&i| columns[i][row].py_str())
                .collect();
            if key_indices.iter().any(|&i| columns[i][row].is_na()) || !seen.insert(values) {
                return Ok(None);
            }
        }
        let f = self.flavor;
        // The column types type the bound text cells on PostgreSQL (`typed_placeholder`). The
        // state predates a table this sync just created, so read the table when it lacks any.
        let fresh_types;
        let cols_types = if f.driver_family() == DriverFamily::Postgres
            && cols.iter().any(|c| !state.cols_types.contains_key(c))
        {
            fresh_types = self
                .get_table_cols_types(&sp.target, self.get_pipe_schema(sp).as_deref())
                .await;
            &fresh_types
        } else {
            &state.cols_types
        };
        let q = |c: &str| sql_item_name(c, f, None);
        let quoted: Vec<String> = cols.iter().map(|c| q(c)).collect();
        let key_sql = keys.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
        let updates: Vec<&String> = cols.iter().filter(|c| !keys.contains(c)).collect();
        let mut params = Vec::new();
        let mut values = Vec::new();
        let mut selects = Vec::new();
        let literal_cols = null_marker_columns(sp, state);
        let encoding = |i: usize| ColumnEncoding {
            name: cols[i].clone(),
            numeric: state
                .cols_types
                .get(&cols[i])
                .and_then(|t| numeric_precision_scale(t))
                .map(|(p, s)| (Some(p), Some(s))),
            keep_null_markers: literal_cols.contains(&cols[i]),
        };
        // Oracle's `UNION ALL` branches must agree on every column's datatype, and a bound `NULL`
        // is a typeless `VARCHAR2` (ORA-01790 next to a bound timestamp or number): a `NULL` cell
        // of a column that has typed cells in this batch is written as `CAST(NULL AS <type>)`.
        let oracle_null_types: Vec<Option<&'static str>> = if f == Flavor::Oracle {
            (0..cols.len())
                .map(|i| {
                    let enc = encoding(i);
                    columns[i]
                        .iter()
                        .find_map(|v| match encode_param(v, f, &enc) {
                            SqlParam::Bool(_) | SqlParam::Int(_) => Some("NUMBER"),
                            SqlParam::Float(_) => Some("BINARY_DOUBLE"),
                            SqlParam::Text(_) | SqlParam::Json(_) => Some("VARCHAR2(4000)"),
                            SqlParam::Timestamp(_) => Some("TIMESTAMP"),
                            SqlParam::TimestampTz(_) => Some("TIMESTAMP WITH TIME ZONE"),
                            SqlParam::Date(_) => Some("DATE"),
                            SqlParam::Bytes(_) => Some("RAW(2000)"),
                            SqlParam::Null => None,
                        })
                })
                .collect()
        } else {
            Vec::new()
        };
        for (row, _) in columns[0].iter().enumerate() {
            let mut cells = Vec::new();
            for (i, col) in cols.iter().enumerate() {
                let enc = encoding(i);
                let param = encode_param(&columns[i][row], f, &enc);
                if let (SqlParam::Null, Some(Some(typ))) = (&param, oracle_null_types.get(i)) {
                    cells.push(format!("CAST(NULL AS {typ})"));
                    continue;
                }
                cells.push(crate::driver::params::typed_placeholder(
                    f,
                    params.len() + 1,
                    &param,
                    cols_types.get(col).map(String::as_str).unwrap_or(""),
                ));
                params.push(param);
            }
            values.push(format!("({})", cells.join(", ")));
            selects.push(format!(
                "SELECT {}{}",
                cells
                    .iter()
                    .zip(&quoted)
                    .map(|(v, c)| format!("{v} AS {c}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if f == Flavor::Oracle {
                    " FROM dual"
                } else {
                    ""
                }
            ));
        }
        let table = self.pipe_table_name(sp);
        let insert = format!(
            "INSERT INTO {table} ({}) VALUES {}",
            quoted.join(", "),
            values.join(", ")
        );
        let sql = match f.driver_family() {
            DriverFamily::Postgres | DriverFamily::Sqlite => {
                let action = if updates.is_empty() {
                    "NOTHING".into()
                } else {
                    format!(
                        "UPDATE SET {}",
                        updates
                            .iter()
                            .map(|c| format!("{}=excluded.{}", q(c), q(c)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let target = if f.driver_family() == DriverFamily::Sqlite {
                    // SQLite can retain a pre-DDL schema on another pooled connection while
                    // preparing an explicit conflict target. Internal pipes have only their
                    // declared key constraints, so the targetless form is equivalent.
                    String::new()
                } else {
                    format!(" ({key_sql})")
                };
                format!("{insert} ON CONFLICT{target} DO {action}")
            }
            DriverFamily::Mysql => {
                let update = if updates.is_empty() {
                    vec![format!("{}={}", q(&keys[0]), q(&keys[0]))]
                } else {
                    updates
                        .iter()
                        .map(|c| format!("{}=VALUES({})", q(c), q(c)))
                        .collect()
                };
                format!("{insert} ON DUPLICATE KEY UPDATE {}", update.join(", "))
            }
            DriverFamily::Mssql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                let on = keys
                    .iter()
                    .map(|k| format!("t.{}=s.{}", q(k), q(k)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let update = if updates.is_empty() {
                    String::new()
                } else {
                    format!(
                        " WHEN MATCHED THEN UPDATE SET {}",
                        updates
                            .iter()
                            .map(|c| format!("{}=s.{}", q(c), q(c)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let lock = if f == Flavor::Mssql {
                    " WITH (HOLDLOCK)"
                } else {
                    ""
                };
                let terminator = if f == Flavor::Mssql { ";" } else { "" };
                format!(
                    "MERGE INTO {table}{lock} t USING ({}) s ON ({on}){update} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({}){terminator}",
                    selects.join(" UNION ALL "),
                    quoted.join(", "),
                    quoted
                        .iter()
                        .map(|c| format!("s.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        };
        self.log(&sql);
        self.driver.exec_with(&sql, &params).await.map_err(be)?;
        Ok(Some((true, String::new())))
    }

    /// The `ON CONFLICT (...)` target of an upsert into the pipe's table (PostgreSQL family,
    /// SQLite, GeoPackage): the key columns and expressions of the table's unique index over
    /// `join_cols`, exactly as the database lists them, so the conflict target always matches
    /// the index (v4's plain unique constraint, one declared `NULLS NOT DISTINCT`, or an older
    /// table's `COALESCE(...)` index), and whether that key treats NULL keys as equal (else the
    /// upsert compares NULL keys itself: `UpdateQueriesArgs::null_safe_upsert`). `None` (render
    /// the join columns, as v4) when no unique index covers exactly `join_cols` or on other
    /// flavors.
    pub(crate) async fn upsert_conflict_target(
        &self,
        sp: &SqlPipe,
        join_cols: &[String],
    ) -> Option<(String, bool)> {
        if join_cols.is_empty() {
            return None;
        }
        // Kept for `settings.catalog_cache_seconds` (see `instance/catalog.rs`).
        let schema = self.get_pipe_schema(sp);
        let ttl = self.catalog_ttl();
        let generation = {
            let cache = self.catalog();
            if let Some(hit) =
                ttl.and_then(|ttl| cache.conflict(&sp.target, schema.as_deref(), join_cols, ttl))
            {
                return Some(hit);
            }
            cache.generation()
        };
        let target = self.read_upsert_conflict_target(sp, join_cols).await;
        if let Some(found) = &target
            && ttl.is_some()
        {
            self.catalog().put_conflict(
                &sp.target,
                schema.as_deref(),
                join_cols,
                found,
                generation,
            );
        }
        target
    }

    /// [`upsert_conflict_target`](Self::upsert_conflict_target) from the database.
    pub(super) async fn read_upsert_conflict_target(
        &self,
        sp: &SqlPipe,
        join_cols: &[String],
    ) -> Option<(String, bool)> {
        let flavor = self.flavor;
        let table = truncate_item_name(&sp.target, flavor);
        // (index name, key expressions in order)
        let mut indices: Vec<(String, Vec<String>)> = Vec::new();
        // The indices declared `NULLS NOT DISTINCT` (PostgreSQL 15+).
        let mut nulls_not_distinct: Vec<String> = Vec::new();
        match flavor.driver_family() {
            DriverFamily::Postgres => {
                let schema = self.get_pipe_schema(sp);
                let nnd = if self.supports_nulls_not_distinct().await {
                    "ix.indnullsnotdistinct"
                } else {
                    "false"
                };
                let sql = format!(
                    "SELECT i.relname AS \"index\", k.k AS \"position\", \
                    pg_catalog.pg_get_indexdef(ix.indexrelid, k.k, true) AS \"expr\", \
                    {nnd} AS \"nnd\" \
                    FROM pg_catalog.pg_index ix \
                    JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid \
                    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid \
                    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace \
                    CROSS JOIN LATERAL generate_series(1, ix.indnkeyatts::int) AS k(k) \
                    WHERE ix.indisunique AND ix.indpred IS NULL AND t.relname = $1 \
                    AND n.nspname = COALESCE($2, current_schema()) \
                    ORDER BY i.relname, k.k"
                );
                let params = [
                    SqlParam::Text(table.clone()),
                    match schema {
                        Some(s) => SqlParam::Text(truncate_item_name(&s, flavor)),
                        None => SqlParam::Null,
                    },
                ];
                let df = self.driver().read_frame_with(&sql, &params).await.ok()?;
                for doc in crate::instance::frame_docs(&df) {
                    let (Some(name), Some(expr)) = (
                        crate::instance::kv_str(doc.get("index")),
                        crate::instance::kv_str(doc.get("expr")),
                    ) else {
                        continue;
                    };
                    if matches!(doc.get("nnd"), Some(KValue::Bool(true)))
                        && !nulls_not_distinct.contains(&name)
                    {
                        nulls_not_distinct.push(name.clone());
                    }
                    match indices.iter_mut().find(|(n, _)| *n == name) {
                        Some((_, exprs)) => exprs.push(expr),
                        None => indices.push((name, vec![expr])),
                    }
                }
            }
            DriverFamily::Sqlite => {
                let sql = "SELECT name, sql FROM sqlite_master WHERE type = 'index' \
                    AND tbl_name = ? AND sql IS NOT NULL";
                let df = self
                    .driver()
                    .read_frame_with(sql, &[SqlParam::Text(table.clone())])
                    .await
                    .ok()?;
                for doc in crate::instance::frame_docs(&df) {
                    let (Some(name), Some(sql)) = (
                        crate::instance::kv_str(doc.get("name")),
                        crate::instance::kv_str(doc.get("sql")),
                    ) else {
                        continue;
                    };
                    if !sql
                        .trim_start()
                        .to_uppercase()
                        .starts_with("CREATE UNIQUE INDEX")
                    {
                        continue;
                    }
                    if let Some(exprs) = sqlite_index_key_list(&sql) {
                        indices.push((name, exprs));
                    }
                }
            }
            _ => return None,
        }
        let wanted: std::collections::BTreeSet<String> =
            join_cols.iter().map(|c| c.to_lowercase()).collect();
        let matching: Vec<&(String, Vec<String>)> = indices
            .iter()
            .filter(|(_, exprs)| {
                let cols: Option<std::collections::BTreeSet<String>> = exprs
                    .iter()
                    .map(|e| key_expression_column(e).map(|c| c.to_lowercase()))
                    .collect();
                cols.is_some_and(|c| c == wanted && c.len() == exprs.len())
            })
            .collect();
        // Prefer the index that makes NULL keys equal.
        let null_safe = |(name, exprs): &&(String, Vec<String>)| {
            nulls_not_distinct.contains(name) || exprs.iter().any(|e| !is_plain_identifier(e))
        };
        let chosen = matching
            .iter()
            .find(|i| null_safe(i))
            .or_else(|| matching.first())?;
        Some((
            chosen
                .1
                .iter()
                .map(|e| {
                    if is_plain_identifier(e) || flavor.driver_family() == DriverFamily::Sqlite {
                        e.clone()
                    } else {
                        format!("({e})")
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
            null_safe(chosen),
        ))
    }

    /// Whether the server accepts `UNIQUE NULLS NOT DISTINCT` (PostgreSQL 15 and later, Citus
    /// aside): the NULL-safe unique key of an upsert pipe is then v4's own constraint
    /// ([`crate::queries::index::IndexState::nulls_not_distinct`]).
    pub(crate) async fn supports_nulls_not_distinct(&self) -> bool {
        if self.flavor.driver_family() != DriverFamily::Postgres || self.flavor == Flavor::Citus {
            return false;
        }
        self.db_version()
            .await
            .and_then(|v| postgres_major_version(&v))
            .is_some_and(|major| major >= 15)
    }

    /// The update / upsert path of `sync_pipe`: stage the rows in a temporary table, then
    /// `get_update_queries`.
    pub(super) async fn apply_update(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        update_df: &Frame,
        upsert: bool,
        autoincrement: bool,
    ) -> ConnectorResult<(bool, String, u64)> {
        let flavor = self.flavor;
        let pipe = sp.pipe();
        let input_count = update_df.height() as u64;
        if upsert
            && !autoincrement
            && !(sp.upsert_changed_only() && flavor.supports_upsert_changed_only())
            && pipe.connector_keys() == "mrsm"
            && (update_df.height() <= 64 || flavor == Flavor::Duckdb)
        {
            if let Some(result) = self.upsert_small_internal(sp, state, update_df).await? {
                return Ok((result.0, result.1, input_count));
            }
            if flavor == Flavor::Duckdb {
                // v4 skips DuckDB indices and has no staged upsert template. Do not silently
                // turn an invalid internal upsert into a plain insert or an empty update.
                return Err(be("Internal DuckDB upserts require non-null keys."));
            }
        }
        let existing_cols = if state.cols_types.is_empty() {
            self.columns_types(sp).await
        } else {
            state.cols_types.clone()
        };
        let dt_col = sp.column("datetime");
        let primary_key = sp.column("primary");
        // A partitioned table folds the datetime column into its composite primary key, so the
        // conflict target must include it too.
        let partition_upsert = dt_col.as_ref().is_some_and(|c| update_df.contains(c))
            && (flavor.is_timescaledb() || self.should_partition(sp));
        let join_cols: Vec<String> = match &primary_key {
            Some(pk) if flavor != Flavor::Oracle => {
                if partition_upsert {
                    vec![dt_col.clone().unwrap_or_default(), pk.clone()]
                } else {
                    vec![pk.clone()]
                }
            }
            _ => columns_map(sp)
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| existing_cols.contains_key(c))
                .collect(),
        };
        // NULL keys must be equal (rc15 S3). The conflict target follows the table's own unique
        // key; when that key treats NULLs as distinct (MySQL / MariaDB, SQLite, PostgreSQL before
        // 15, a table made before 5.0.0rc15) and the batch has a NULL key, the upsert compares
        // the keys itself (`UpdateQueriesArgs::null_safe_upsert`).
        let (conflict_target, key_null_safe) = if upsert {
            match self.upsert_conflict_target(sp, &join_cols).await {
                Some((target, null_safe)) => (Some(target), null_safe),
                None => (None, false),
            }
        } else {
            (None, false)
        };
        let null_safe_upsert = upsert
            && sp.null_indices()
            && pipe.connector_keys() != "mrsm"
            && match flavor {
                Flavor::Mysql | Flavor::Mariadb => true,
                Flavor::Postgresql
                | Flavor::Postgis
                | Flavor::Timescaledb
                | Flavor::TimescaledbHa
                | Flavor::Sqlite
                | Flavor::Geopackage => {
                    !key_null_safe
                        && join_cols
                            .iter()
                            .filter(|c| Some(*c) != dt_col.as_ref())
                            .any(|c| {
                                update_df
                                    .data(c)
                                    .map(|d| d.is_null_mask().iter().any(|n| *n))
                                    .unwrap_or(false)
                            })
                }
                _ => false,
            };
        // These statements need one row per key: a batch that repeats a key keeps its last row,
        // as `INSERT ... ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE` resolve it (NULL keys
        // are equal here).
        let deduplicated;
        let update_df = if null_safe_upsert {
            deduplicated = last_row_per_key(update_df, &join_cols);
            &deduplicated
        } else {
            update_df
        };
        let temp_target = self.get_temporary_target(
            &sp.target,
            None,
            Some(if upsert { "upsert" } else { "update" }),
        );
        let _ = self
            .log_temporary_tables_creation(
                std::slice::from_ref(&temp_target),
                false,
                !pipe.temporary,
            )
            .await;
        let pipe_dtypes = &state.dtypes;
        let mut update_dtypes: IndexMap<String, String> = update_df.dtypes();
        for (c, t) in get_special_cols(update_df) {
            update_dtypes.insert(c, t);
        }
        for col in update_df.names() {
            if let Some(t) = pipe_dtypes.get(&col) {
                let all_null = update_df
                    .data(&col)
                    .map(|d| d.is_null_mask().iter().all(|n| *n))
                    .unwrap_or(false);
                if all_null {
                    update_dtypes.insert(col.clone(), t.clone());
                }
            }
        }
        let temp_columns: Map = columns_map(sp)
            .into_iter()
            .filter(|(_, c)| update_df.contains(c))
            .map(|(k, c)| {
                (
                    if k == "primary" {
                        "primary_".to_string()
                    } else {
                        k
                    },
                    Value::Str(c),
                )
            })
            .collect();
        let mut parameters = Map::new();
        parameters.insert(
            "schema".into(),
            self.internal_schema()
                .map(|s| Value::Str(s.into()))
                .unwrap_or(Value::Null),
        );
        parameters.insert("hypertable".into(), Value::Bool(false));
        let mut builder = Pipe::builder(
            &format!("{}_", pipe.connector_keys().replace(':', "_")),
            pipe.metric_key(),
        )
        .location(pipe.location_key())
        .instance(pipe.instance_keys())
        .parameters(parameters)
        .kwarg("columns", Value::Map(temp_columns))
        .kwarg(
            "dtypes",
            Value::Map(
                update_dtypes
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        )
        .kwarg("target", Value::Str(temp_target.clone()))
        .kwarg("temporary", true)
        .kwarg("enforce", false)
        .kwarg("static", true)
        .kwarg("autoincrement", false)
        .kwarg("cache", false);
        builder = builder.defaults(self.settings.pipe_defaults.clone());
        let mut temp_pipe = builder.build().map_err(be)?;
        let temp_columns_types: IndexMap<String, String> = update_dtypes
            .iter()
            .map(|(c, t)| {
                // The broad pandas object/bytes equality used by the legacy mapping
                // otherwise resolves an exact Arrow binary column to SQLite TEXT.
                let dtype = if flavor == Flavor::Sqlite && native_binary_dtype(t) {
                    "bytes"
                } else {
                    t
                };
                (c.clone(), history_db_type(sp, flavor, dtype))
            })
            .collect();
        let temp_opts = SyncOptions {
            check_existing: false,
            ..Default::default()
        };
        let temp_result = self
            .sync_pipe_impl(
                &mut temp_pipe,
                update_df.clone(),
                &temp_opts,
                InternalSync {
                    cached_state: None,
                    columns_types: Some(temp_columns_types.clone()),
                    skip_check_indices: true,
                    new_special_cols: None,
                    skip_temporary_tables_check: false,
                    history_datetime_precision: mysql_engine_history(sp, flavor),
                },
            )
            .await;
        if !temp_result.success {
            return Ok((false, temp_result.message, 0));
        }

        let schema = self.get_pipe_schema(sp);
        let internal_schema = self.internal_schema().map(str::to_string);
        let db_version = if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            self.db_version().await
        } else {
            None
        };
        let datetime_col = dt_col.as_deref().filter(|c| update_df.contains(c));
        let mut args = UpdateQueriesArgs::new(&sp.target, &temp_target, join_cols, flavor);
        args.upsert = upsert;
        args.schema = schema.as_deref();
        args.patch_schema = internal_schema.as_deref();
        args.target_cols_types = existing_cols;
        args.patch_cols_types = temp_columns_types;
        args.datetime_col = datetime_col;
        args.identity_insert = autoincrement
            && primary_key
                .as_ref()
                .is_some_and(|pk| update_df.contains(pk));
        args.null_indices = sp.null_indices();
        args.cast_columns = sp.enforce();
        args.db_version = db_version.as_deref();
        if upsert {
            // Name the unique key's own columns / expressions; compare NULL keys in the
            // statements where that key does not.
            args.conflict_target = conflict_target;
            args.null_safe_upsert = null_safe_upsert;
            args.changed_only = sp.upsert_changed_only();
        }
        // SQLite cannot use a plain key index for the COALESCE equality used by v4.
        // Index the small staging table's exact expressions so a scattered update does not
        // scan that table once per row in the (potentially very large) target date range.
        // PARITY: retain v4's null-sentinel matching, including sentinel collisions.
        if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) && args.null_indices && !upsert {
            let expressions = args
                .join_cols
                .iter()
                .filter_map(|column| {
                    args.target_cols_types.get(column).map(|dtype| {
                        format!(
                            "COALESCE({}, {})",
                            sql_item_name(column, flavor, None),
                            crate::queries::update::get_null_replacement(dtype, Some(flavor))
                        )
                    })
                })
                .collect::<Vec<_>>();
            if !expressions.is_empty() {
                let index = sql_item_name(&format!("IX_{temp_target}_join"), flavor, None);
                let table = sql_item_name(&temp_target, flavor, internal_schema.as_deref());
                let _ = self
                    .exec(&format!(
                        "CREATE INDEX {index} ON {table} ({})",
                        expressions.join(", ")
                    ))
                    .await;
            }
        }
        let update_queries = get_update_queries(&args);
        let count_all = upsert && sp.upsert_changed_only() && flavor.supports_upsert_changed_only();
        let update_start = std::time::Instant::now();
        let results = self.exec_queries(&update_queries, true, true).await;
        let update_success =
            results.len() == update_queries.len() && results.iter().all(Option::is_some);
        crate::facade::trace("sql: execute update", update_start);
        let _ = self
            .log_temporary_tables_creation(&[temp_target], true, !pipe.temporary)
            .await;
        if !update_success {
            tracing::warn!("Failed to apply update to {pipe}.");
            return Ok((false, format!("Failed to apply update to {pipe}."), 0));
        }
        let written = if count_all {
            results.into_iter().flatten().sum()
        } else {
            input_count
        };
        Ok((true, String::new(), written))
    }
}

/// Whether a key expression is a bare (possibly quoted) column name.
/// The major version in PostgreSQL's `version()` text (`PostgreSQL 17.5 (Debian ...) on ...`),
/// or in a bare version (`15.4`).
pub(crate) fn postgres_major_version(version: &str) -> Option<u32> {
    let v = version.trim();
    let v = v.strip_prefix("PostgreSQL").unwrap_or(v).trim_start();
    let digits: String = v.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// The rows of `df` left when a key (`keys`, NULL equal to NULL) that repeats keeps only its
/// last row, in their original order: how `INSERT ... ON DUPLICATE KEY UPDATE` resolves a batch
/// that repeats a key (the last row wins). `df` itself when no key repeats or a key column is
/// missing.
pub(crate) fn last_row_per_key(df: &Frame, keys: &[String]) -> Frame {
    if df.height() < 2 || keys.is_empty() {
        return df.clone();
    }
    let Ok(columns) = keys
        .iter()
        .map(|k| df.data(k).map(|d| d.values()))
        .collect::<Result<Vec<Vec<KValue>>, _>>()
    else {
        return df.clone();
    };
    let mut last: std::collections::HashMap<Vec<KValue>, usize> =
        std::collections::HashMap::with_capacity(df.height());
    for i in 0..df.height() {
        last.insert(columns.iter().map(|c| c[i].clone()).collect(), i);
    }
    if last.len() == df.height() {
        return df.clone();
    }
    let mut keep: Vec<usize> = last.into_values().collect();
    keep.sort_unstable();
    df.take(&keep)
}

pub(super) fn is_plain_identifier(expr: &str) -> bool {
    let e = expr.trim();
    if let Some(inner) = e.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        return !inner.is_empty() && !inner.replace("\"\"", "").contains('"');
    }
    if let Some(inner) = e.strip_prefix('`').and_then(|r| r.strip_suffix('`')) {
        return !inner.is_empty() && !inner.contains('`');
    }
    if let Some(inner) = e.strip_prefix('[').and_then(|r| r.strip_suffix(']')) {
        return !inner.is_empty() && !inner.contains(']');
    }
    !e.is_empty()
        && e.chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
        && e.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// A quoted or bare identifier's name.
pub(super) fn unquote_identifier(expr: &str) -> String {
    let e = expr.trim();
    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
        if let Some(inner) = e.strip_prefix(open).and_then(|r| r.strip_suffix(close)) {
            return inner.replace("\"\"", "\"");
        }
    }
    e.to_string()
}

/// The column a unique-index key expression is over: the column itself, or the first argument
/// of `COALESCE(col, ...)` (Meerschaum's NULL-safe key). `None` for any other expression.
pub(super) fn key_expression_column(expr: &str) -> Option<String> {
    let e = expr.trim();
    if is_plain_identifier(e) {
        return Some(unquote_identifier(e));
    }
    let upper = e.to_uppercase();
    let rest = upper
        .strip_prefix("COALESCE")
        .map(|_| e["COALESCE".len()..].trim_start())?;
    let inner = rest.strip_prefix('(')?;
    let args = split_top_level(inner.strip_suffix(')').unwrap_or(inner));
    let first = args.first()?.trim();
    // `COALESCE(("id")::text, ...)` or a cast: take the identifier inside.
    let first = first.trim_start_matches('(');
    let first = first.split(')').next().unwrap_or(first);
    let first = first.split("::").next().unwrap_or(first).trim();
    is_plain_identifier(first).then(|| unquote_identifier(first))
}

/// Split on the commas outside parentheses and quotes.
pub(super) fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for c in s.chars() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '(' => {
                    depth += 1;
                    cur.push(c);
                }
                ')' => {
                    depth -= 1;
                    cur.push(c);
                }
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// The key list of a SQLite `CREATE [UNIQUE] INDEX ... ON table (keys)` statement.
pub(super) fn sqlite_index_key_list(sql: &str) -> Option<Vec<String>> {
    let upper = sql.to_uppercase();
    let on = upper.find(" ON ")?;
    let open = on + sql[on..].find('(')?;
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    for (i, c) in sql[open..].char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let keys = split_top_level(&sql[open + 1..open + i]);
                        return (!keys.is_empty()).then_some(keys);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

#[cfg(test)]
mod conflict_target_tests {
    use super::*;

    #[test]
    pub(super) fn key_expressions_name_their_columns() {
        assert_eq!(key_expression_column("ts").as_deref(), Some("ts"));
        assert_eq!(key_expression_column("\"Ts\"").as_deref(), Some("Ts"));
        assert_eq!(
            key_expression_column("COALESCE(id, '-987654321'::integer)").as_deref(),
            Some("id")
        );
        assert_eq!(
            key_expression_column("COALESCE(\"id\", -987654321) ").as_deref(),
            Some("id")
        );
        assert_eq!(
            key_expression_column("COALESCE((station)::text, '-987654321'::text)").as_deref(),
            Some("station")
        );
        assert_eq!(key_expression_column("lower(name)"), None);
        assert!(is_plain_identifier("\"a b\"") && !is_plain_identifier("COALESCE(a, 1)"));
    }

    #[test]
    pub(super) fn repeated_keys_keep_their_last_row() {
        use meerschaum_kernel::frame::Column;
        let df = Frame::new(vec![
            Column::object(
                "id",
                vec![
                    KValue::Int(1),
                    KValue::Int(2),
                    KValue::Null,
                    KValue::Int(1),
                    KValue::Null,
                    KValue::Int(3),
                ],
            ),
            Column::object("v", (1..=6).map(KValue::Int).collect::<Vec<_>>()),
        ])
        .unwrap();
        let out = last_row_per_key(&df, &["id".to_string()]);
        let values = |c: &str| out.data(c).unwrap().values();
        // Key 1 keeps v=4, NULL keeps v=5 (NULL keys are equal), order kept.
        assert_eq!(
            values("v"),
            vec![
                KValue::Int(2),
                KValue::Int(4),
                KValue::Int(5),
                KValue::Int(6)
            ]
        );
        // No repeats (or a missing key column): unchanged.
        let unique = df.take(&[0, 1, 2]);
        assert_eq!(last_row_per_key(&unique, &["id".to_string()]).height(), 3);
        assert_eq!(last_row_per_key(&df, &["missing".to_string()]).height(), 6);
    }

    #[test]
    pub(super) fn postgres_versions_parse() {
        assert_eq!(
            postgres_major_version(
                "PostgreSQL 17.5 (Debian 17.5-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc"
            ),
            Some(17)
        );
        assert_eq!(
            postgres_major_version("PostgreSQL 9.6.24 on x86_64"),
            Some(9)
        );
        assert_eq!(postgres_major_version("15.4"), Some(15));
        assert_eq!(postgres_major_version("CockroachDB CCL v23.1"), None);
    }

    #[test]
    pub(super) fn sqlite_key_lists_parse() {
        assert_eq!(
            sqlite_index_key_list(
                "CREATE UNIQUE INDEX \"IX_t_ts_id\" ON \"t\" (\"ts\", COALESCE(\"id\", -987654321) )"
            ),
            Some(vec![
                "\"ts\"".to_string(),
                "COALESCE(\"id\", -987654321)".to_string()
            ])
        );
        assert_eq!(
            sqlite_index_key_list("CREATE UNIQUE INDEX ix ON t (a, b)"),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }
}
