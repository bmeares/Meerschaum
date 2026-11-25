//! Table creation and schema evolution (`create_pipe_table_from_df`, add/alter columns) for [`SqlConnector`].

use super::*;

impl SqlConnector {
    /// `create_pipe_table_from_df(pipe, df)`: create the target table from the pipe's dtypes
    /// and the incoming frame (hypertables, Hypercore, native partitions included).
    pub(crate) async fn create_pipe_table_from_df(
        &self,
        sp: &SqlPipe,
        df: &Frame,
        state: &PipeState,
        history_datetime_precision: bool,
    ) -> SuccessTuple {
        let flavor = self.flavor;
        let pipe_dtypes = &state.dtypes;
        let df_dtypes = df.dtypes();
        let primary_key = sp.column("primary");
        let primary_key_typ = primary_key.as_ref().map(|pk| {
            pipe_dtypes
                .get(pk)
                .cloned()
                .unwrap_or_else(|| df_dtypes.get(pk).cloned().unwrap_or_else(|| "int".into()))
        });
        let mut primary_key_db_type = primary_key_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let dt_col = sp.column("datetime");
        let mut new_dtypes: IndexMap<String, String> = df_dtypes.clone();
        for (ix, col) in columns_map(sp) {
            if ix != "primary" {
                new_dtypes.insert(
                    col.clone(),
                    df_dtypes.get(&col).cloned().unwrap_or_else(|| "int".into()),
                );
            }
        }
        for (c, t) in get_special_cols(df) {
            new_dtypes.insert(c, t);
        }
        for (c, t) in pipe_dtypes {
            new_dtypes.insert(c.clone(), t.clone());
        }
        let autoincrement = sp.autoincrement()
            || primary_key
                .as_ref()
                .is_some_and(|pk| !new_dtypes.contains_key(pk));
        if autoincrement && let Some(pk) = &primary_key {
            new_dtypes.shift_remove(pk);
        }
        // Internal state keys must be indexable on engines which disallow TEXT keys.
        // Keep the public pipe dtype unchanged; only the physical key type is bounded.
        let internal_key_types = sp.pipe().connector_keys() == "mrsm"
            && matches!(
                flavor,
                Flavor::Mysql | Flavor::Mariadb | Flavor::Mssql | Flavor::Oracle
            );
        let precise_history = matches!(flavor, Flavor::Mysql | Flavor::Mariadb)
            && (mysql_engine_history(sp, flavor) || history_datetime_precision);
        let physical_types = internal_key_types || precise_history;
        if physical_types {
            let keys = columns_map(sp);
            for (column, dtype) in &mut new_dtypes {
                let db_type = history_db_type(sp, flavor, dtype);
                *dtype = if precise_history && db_type == "DATETIME" {
                    "DATETIME(6)".into()
                } else if internal_key_types
                    && keys.values().any(|key| key == column)
                    && matches!(
                        db_type.as_str(),
                        "TEXT" | "NVARCHAR(MAX)" | "VARCHAR(MAX)" | "NVARCHAR2(2000)"
                    )
                {
                    match flavor {
                        Flavor::Mssql => "NVARCHAR(255)",
                        Flavor::Oracle => "NVARCHAR2(255)",
                        _ => "VARCHAR(255)",
                    }
                    .into()
                } else {
                    db_type
                };
            }
            // The Syncing Engine's state tables (new in v5, no v4 layout to keep) hold free text
            // (planner reasons, sync messages): declare utf8mb4 so a database whose default
            // charset is not (MySQL 5.7 defaults to latin1) still records every row.
            if matches!(flavor, Flavor::Mysql | Flavor::Mariadb)
                && meerschaum_connector::pipe_ops::engine_state_parameters(sp.pipe().metric_key())
                    .is_some()
            {
                for dtype in new_dtypes.values_mut() {
                    let upper = dtype.to_ascii_uppercase();
                    let textual = [
                        "TEXT",
                        "TINYTEXT",
                        "MEDIUMTEXT",
                        "LONGTEXT",
                        "VARCHAR",
                        "CHAR",
                    ]
                    .iter()
                    .any(|t| upper == *t || upper.starts_with(&format!("{t}(")));
                    if textual {
                        dtype.push_str(" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
                    }
                }
            }
            primary_key_db_type = primary_key
                .as_ref()
                .and_then(|key| new_dtypes.get(key).cloned());
        }
        let schema = self.get_pipe_schema(sp);

        let hypertable = flavor.is_timescaledb() && sp.hypertable() && dt_col.is_some();
        let hypercore = hypertable && sp.hypercore();
        let (mut chunk_str, mut segmentby, mut orderby) = (None, None, None);
        if hypercore {
            if let Ok(ci) = sp.chunk_interval() {
                chunk_str = Some(match ci {
                    ChunkInterval::Int(i) => i.to_string(),
                    ChunkInterval::Delta(d) => {
                        format!("{} minutes", (d.num_seconds() as f64 / 60.0) as i64)
                    }
                });
            }
            let settings = get_compress_settings(flavor, sp);
            segmentby = Some(settings.segmentby).filter(|s| !s.is_empty());
            orderby = Some(settings.orderby).filter(|s| !s.is_empty());
        }

        let mut partition_by_column = crate::queries::partition::get_partition_column(flavor, sp);
        let partition_bounds = if partition_by_column.is_some() && self.inline_partitions() {
            Some(self.initial_partition_bounds(sp, df))
        } else {
            None
        };
        if partition_by_column.is_some()
            && self.inline_partitions()
            && partition_bounds.as_ref().is_none_or(|b| b.is_empty())
        {
            partition_by_column = None;
        }

        // MSSQL partitions through a function + scheme created before the table; its clustered
        // index is placed on the scheme.
        let (partition_scheme_name, partition_creation_queries) =
            if partition_by_column.is_some() && flavor == Flavor::Mssql {
                let (name, queries) = self.mssql_partition_creation(sp, df);
                (Some(name), queries)
            } else {
                (None, Vec::new())
            };

        let build = |chunk: Option<String>| -> crate::Result<Vec<String>> {
            let opts = CreateTableOptions {
                unlogged_staging: false,
                schema: schema.clone(),
                primary_key: primary_key.clone(),
                primary_key_db_type: primary_key_db_type.clone(),
                autoincrement: false,
                datetime_column: dt_col.clone(),
                hypertable_segmentby: if chunk.is_some() {
                    segmentby.clone()
                } else {
                    None
                },
                hypertable_orderby: if chunk.is_some() {
                    orderby.clone()
                } else {
                    None
                },
                hypertable_chunk_interval: chunk,
                partition_by_column: partition_by_column.clone(),
                partition_bounds: partition_bounds.clone(),
                partition_scheme_name: partition_scheme_name.clone(),
                parse_dtypes: !physical_types,
            };
            let mut queries = get_create_table_queries(
                &QueryOrDtypes::Dtypes(new_dtypes.clone()),
                &sp.target,
                flavor,
                &opts,
            )?;
            if !partition_creation_queries.is_empty() {
                let mut q = partition_creation_queries.clone();
                q.extend(queries);
                queries = q;
            }
            if let Some(s) = schema.as_deref().filter(|s| !s.is_empty()) {
                let mut q = get_create_schema_if_not_exists_queries(Some(s), flavor)?;
                if sp.pipe().temporary
                    && sp.pipe().connector_keys() == "mrsm"
                    && flavor == Flavor::Mssql
                    && !q.is_empty()
                {
                    // Different internal targets have different initialization locks but
                    // may create their shared schema together. Lock only while it is absent,
                    // using the existing create-table transaction, then check again. MSSQL's
                    // generic CREATE SCHEMA error 2759 cannot distinguish a duplicate from a
                    // permission failure, so no schema/DDL errors are suppressed.
                    use sha2::{Digest, Sha256};

                    let schema = truncate_item_name(s, flavor);
                    let name = schema.replace('\'', "''");
                    let create = format!("CREATE SCHEMA {}", sql_item_name(&schema, flavor, None))
                        .replace('\'', "''");
                    let mut digest = Sha256::new();
                    digest.update(b"meerschaum:internal-schema-initialization:");
                    digest.update(schema.to_lowercase().as_bytes());
                    let key = hex::encode(digest.finalize());
                    let limit = crate::driver::connect::connect_timeout(self.spec());
                    let timeout_ms = i32::try_from(limit.as_millis()).unwrap_or(i32::MAX);
                    q = vec![format!(
                        "IF SCHEMA_ID(N'{name}') IS NULL\n\
                         BEGIN\n\
                             DECLARE @mrsm_schema_lock int;\n\
                             EXEC @mrsm_schema_lock = sys.sp_getapplock \
                                 @Resource = N'{key}', @LockMode = 'Exclusive', \
                                 @LockOwner = 'Transaction', @LockTimeout = {timeout_ms}, \
                                 @DbPrincipal = 'public';\n\
                             IF @mrsm_schema_lock < 0\n\
                                 THROW 50000, 'Could not acquire internal schema initialization lock.', 1;\n\
                             IF SCHEMA_ID(N'{name}') IS NULL\n\
                                 EXEC(N'{create}');\n\
                         END;"
                    )];
                }
                if sp.pipe().temporary
                    && sp.is_static()
                    && sp.pipe().connector_keys() == "mrsm"
                    && flavor.driver_family() == DriverFamily::Postgres
                    && flavor != Flavor::Cockroachdb
                {
                    use sha2::{Digest, Sha256};

                    // Distinct target locks do not protect creation of their common
                    // schema. Lock only while an absent schema is created, in the same
                    // transaction as its DDL. Existing schemas take no schema lock.
                    let schema = truncate_item_name(s, flavor);
                    let mut digest = Sha256::new();
                    digest.update(b"meerschaum:internal-schema-initialization:");
                    digest.update(schema.as_bytes());
                    let key =
                        i64::from_be_bytes(digest.finalize()[..8].try_into().expect("eight bytes"));
                    q.insert(0, format!(
                        "SELECT pg_advisory_xact_lock({key}) WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '{}')",
                        schema.replace('\'', "''"),
                    ));
                }
                q.extend(queries);
                queries = q;
            }
            Ok(queries)
        };
        let queries = match build(chunk_str.clone()) {
            Ok(q) => q,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let mut success = self.exec_queries_all(&queries, true, true).await;
        if !success && hypercore {
            // Declarative hypertable syntax unsupported; retry as a plain table.
            if let Ok(queries) = build(None) {
                success = self.exec_queries_all(&queries, true, true).await;
            }
        }
        if !success && self.table_exists(&sp.target, schema.as_deref()).await {
            // Another connection created the table between our existence check and the
            // CREATE (two writers' first sync of a new pipe, e.g. the engine's state pipes):
            // the table is there, so the rows can be written to it.
            tracing::debug!("{} was created concurrently.", self.pipe_table_name(sp));
            return SuccessTuple::ok("Success");
        }
        if success && hypercore && !self.set_integer_now_func(sp).await {
            tracing::warn!(
                "Could not register an `integer_now` function for {}; its columnstore policy will fail on every run.",
                sp.pipe()
            );
        }
        if success && flavor == Flavor::Geopackage {
            return self.init_geopackage_pipe(sp, &new_dtypes, df).await;
        }
        if success {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to create {}.", self.pipe_table_name(sp)))
        }
    }

    /// `df.dtypes` with object columns refined from their first value (`get_add_columns_queries`).
    pub(super) fn df_cols_types_refined(&self, df: &Frame) -> IndexMap<String, String> {
        let mut out = df.dtypes();
        for (col, typ) in &mut out {
            if self.flavor == Flavor::Sqlite && native_binary_dtype(typ) {
                // Resolve typed binary columns before SQL's generic object fallback.
                *typ = "bytes".into();
                continue;
            }
            if typ != "object" || df.height() == 0 {
                continue;
            }
            let Ok(data) = df.data(col) else { continue };
            if self.flavor == Flavor::Sqlite
                && let meerschaum_kernel::frame::ColumnData::Object(values) = data
            {
                let mut present = values.iter().filter(|value| !value.is_na());
                if matches!(present.next(), Some(KValue::Bytes(_)))
                    && present.all(|value| matches!(value, KValue::Bytes(_)))
                {
                    *typ = "bytes".into();
                    continue;
                }
            }
            match data.get(0) {
                KValue::Json(_) => *typ = "json".into(),
                KValue::Decimal(_) => *typ = "numeric".into(),
                KValue::Str(_) => *typ = "str".into(),
                _ => {}
            }
        }
        out
    }

    /// `get_add_columns_queries(pipe, df)`: `ALTER TABLE ... ADD` for columns new to the table.
    ///
    /// PARITY: Python iterates a `set` of new columns (hash order); Rust keeps frame order.
    pub async fn get_add_columns_queries(&self, sp: &SqlPipe, df: &Frame) -> Vec<String> {
        let schema = self.get_pipe_schema(sp);
        if !self.table_exists(&sp.target, schema.as_deref()).await
            || (sp.is_static() && !engine_history(sp))
        {
            return Vec::new();
        }
        let df_cols_types = self.df_cols_types_refined(df);
        let db_cols_types = self
            .get_table_cols_types(&sp.target, schema.as_deref())
            .await;
        self.add_columns_queries_for(sp, &df_cols_types, &db_cols_types)
    }

    pub(super) fn columns_to_add(
        &self,
        sp: &SqlPipe,
        df_cols_types: &IndexMap<String, String>,
        db_cols_types: &IndexMap<String, String>,
    ) -> Vec<(String, String)> {
        if sp.is_static() && !engine_history(sp) {
            return Vec::new();
        }
        let flavor = self.flavor;
        let declared = sp.dtypes();
        let declared_history;
        let incoming = if engine_history(sp) {
            // An empty initializer has no incoming Arrow columns. Its declared schema still
            // has to be made ready, without fabricating values for existing legacy rows.
            let mut types = df_cols_types.clone();
            for (column, dtype) in &declared {
                types.insert(column.clone(), dtype.py_str());
            }
            declared_history = types;
            &declared_history
        } else {
            df_cols_types
        };
        incoming
            .iter()
            .filter(|(c, t)| !db_cols_types.contains_key(*c) && !c.is_empty() && !t.is_empty())
            // Managed static history may gain new declared fields, such as selection or
            // registration provenance. Unrecognized incoming fields retain static semantics.
            .filter(|(column, _)| !sp.is_static() || declared.contains_key(*column))
            .map(|(c, t)| (c.clone(), history_db_type(sp, flavor, t)))
            .collect()
    }

    /// The pure part of `get_add_columns_queries`: the incoming pandas dtypes against the
    /// table's database types.
    pub fn add_columns_queries_for(
        &self,
        sp: &SqlPipe,
        df_cols_types: &IndexMap<String, String>,
        db_cols_types: &IndexMap<String, String>,
    ) -> Vec<String> {
        let flavor = self.flavor;
        let new_cols = self.columns_to_add(sp, df_cols_types, db_cols_types);
        if new_cols.is_empty() {
            return Vec::new();
        }
        let mut alter = format!("ALTER TABLE {}", self.pipe_table_name(sp));
        let mut queries = Vec::new();
        for (col, typ) in &new_cols {
            let add = format!("\nADD {} {typ},", sql_item_name(col, flavor, None));
            if flavor.single_alter_table() {
                queries.push(format!("{alter}{}", &add[..add.len() - 1]));
            } else {
                alter.push_str(&add);
            }
        }
        if queries.is_empty() {
            alter.pop();
            queries.push(alter);
        }
        queries
    }

    /// `get_alter_columns_queries(pipe, df)`: retype columns whose incoming dtype conflicts with
    /// the table (to `TEXT`, or `NUMERIC` for numeric columns). May persist numeric dtypes.
    pub async fn get_alter_columns_queries(
        &self,
        pipe: &mut Pipe,
        df: &Frame,
        new_special_cols: Option<&IndexMap<String, String>>,
    ) -> Vec<String> {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return Vec::new();
        };
        let schema = self.get_pipe_schema(&sp);
        if !self.table_exists(&sp.target, schema.as_deref()).await || sp.is_static() {
            return Vec::new();
        }
        let state = self.pipe_state(&sp).await;
        self.alter_columns_with_state(pipe, df, new_special_cols, &state)
            .await
    }

    pub(super) async fn alter_columns_with_state(
        &self,
        pipe: &mut Pipe,
        df: &Frame,
        new_special_cols: Option<&IndexMap<String, String>>,
        state: &PipeState,
    ) -> Vec<String> {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return Vec::new();
        };
        if !state.exists || sp.is_static() {
            return Vec::new();
        }
        let input = AlterColumnsInput {
            df_cols_types: df.dtypes(),
            numeric_cols: get_numeric_cols(df),
            db_cols_types: state.cols_types.clone(),
            pipe_dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
            new_special_cols: new_special_cols.cloned().unwrap_or_default(),
            session_id: meerschaum_core::users::generate_password(3)
                .unwrap_or_else(|_| "000".into()),
        };
        let (queries, new_dtypes) = self.alter_columns_queries_for(&sp, &input);
        if let Some(explicit) = new_dtypes {
            pipe.set_dtypes(
                explicit
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(v)))
                    .collect(),
            );
            if !pipe.temporary {
                let res = self.edit_pipe(pipe, false).await;
                // A `parameters` lock keeps the parameters as they are; the sync goes on.
                if !res.success && !meerschaum_connector::locks::refused_by_lock(&res) {
                    tracing::warn!(
                        "Failed to update dtypes for numeric columns {}:\n{}",
                        crate::instance::values::items_str(&input.numeric_cols),
                        res.message
                    );
                }
            }
        }
        queries
    }

    /// The pure part of `get_alter_columns_queries`. Returns the queries and, when numeric
    /// columns were detected, the explicit dtypes Python persists with `pipe.edit()`.
    pub fn alter_columns_queries_for(
        &self,
        sp: &SqlPipe,
        input: &AlterColumnsInput,
    ) -> (Vec<String>, Option<IndexMap<String, String>>) {
        if sp.is_static() {
            return (Vec::new(), None);
        }
        let flavor = self.flavor;
        let schema = self.get_pipe_schema(sp);
        let target = sp.target.clone();
        let session_id = input.session_id.clone();
        let mut numeric_cols = input.numeric_cols.clone();
        let df_cols_types = &input.df_cols_types;
        let db_cols_types: IndexMap<String, String> = input
            .db_cols_types
            .iter()
            .map(|(c, t)| (c.clone(), get_pd_type_from_db_type(t, true)))
            .collect();
        let pipe_dtypes = &input.pipe_dtypes;
        let pipe_bool_cols: Vec<&String> = pipe_dtypes
            .iter()
            .filter(|(_, t)| are_dtypes_equal(t, "bool"))
            .map(|(c, _)| c)
            .collect();
        let mut aliases: Vec<(&str, &str)> = vec![
            ("int", "bool"),
            ("float", "bool"),
            ("numeric", "bool"),
            ("guid", "object"),
        ];
        match flavor {
            Flavor::Oracle => {
                aliases.retain(|(k, _)| !matches!(*k, "int" | "numeric"));
                aliases.extend([("int", "numeric"), ("date", "datetime"), ("numeric", "int")]);
            }
            Flavor::Geopackage => aliases.extend([("geometry", "bytes"), ("bytes", "geometry")]),
            _ => {}
        }
        let db_typ_of = |c: &str| {
            db_cols_types
                .get(c)
                .cloned()
                .unwrap_or_else(|| "object".into())
        };
        let mut altered_cols: IndexMap<String, (String, String)> = df_cols_types
            .iter()
            .filter(|(c, t)| {
                let db = db_typ_of(c);
                !are_dtypes_equal(t, &db.to_lowercase()) && !are_dtypes_equal(&db, "string")
            })
            .map(|(c, t)| (c.clone(), (db_typ_of(c), t.clone())))
            .collect();
        let new_special = input.new_special_cols.clone();
        for (c, t) in &new_special {
            altered_cols.insert(c.clone(), (db_typ_of(c), t.clone()));
        }
        // NOTE: Sometimes bools are coerced into ints or floats.
        let mut ignore: Vec<String> = Vec::new();
        for (col, (db_typ, df_typ)) in &altered_cols {
            for (db_alias, df_alias) in &aliases {
                if db_typ.to_lowercase().contains(db_alias)
                    && df_typ.to_lowercase().contains(df_alias)
                    && !new_special.contains_key(col)
                {
                    ignore.push(col.clone());
                }
            }
        }
        let bool_compat = |t: &str| {
            are_dtypes_equal("int", t)
                || are_dtypes_equal("float", t)
                || are_dtypes_equal("numeric", t)
                || are_dtypes_equal("bool", t)
        };
        for bool_col in pipe_bool_cols {
            if let Some((db, dft)) = altered_cols.get(bool_col)
                && bool_compat(db)
                && bool_compat(dft)
            {
                ignore.push(bool_col.clone());
            }
        }
        for c in &ignore {
            altered_cols.shift_remove(c);
        }
        if altered_cols.is_empty() {
            return (Vec::new(), None);
        }
        let mut new_dtypes = None;
        if !numeric_cols.is_empty() {
            let mut explicit = input.explicit_dtypes.clone();
            for c in &numeric_cols {
                explicit.insert(c.clone(), "numeric".into());
            }
            new_dtypes = Some(explicit);
        } else {
            numeric_cols.extend(
                pipe_dtypes
                    .iter()
                    .filter(|(_, t)| t.starts_with("numeric"))
                    .map(|(c, _)| c.clone()),
            );
        }
        let numeric_type = get_db_type_from_pd_type("numeric", Some(flavor));
        let text_type = get_db_type_from_pd_type("str", Some(flavor));
        let altered_types: IndexMap<String, String> = altered_cols
            .keys()
            .map(|c| {
                (
                    c.clone(),
                    if numeric_cols.contains(c) {
                        numeric_type.clone()
                    } else {
                        text_type.clone()
                    },
                )
            })
            .collect();
        let q = |c: &str| sql_item_name(c, flavor, None);
        let target_name = sql_item_name(&target, flavor, schema.as_deref());

        if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            let temp_table_name = format!("-{session_id}_{target}");
            let rename_query = format!(
                "ALTER TABLE {target_name} RENAME TO {}",
                q(&temp_table_name)
            );
            // PARITY: the rebuilt table declares the *pandas* types `get_pd_type_from_db_type`
            // returns for unaltered columns (`_pipes.py:3793`).
            let mut create_query = format!("CREATE TABLE {target_name} (\n");
            for (col, typ) in &db_cols_types {
                let t = altered_types.get(col).unwrap_or(typ);
                create_query.push_str(&format!("{} {t},\n", q(col)));
            }
            create_query.truncate(create_query.len() - 2);
            create_query.push_str("\n)");
            let cols: Vec<String> = db_cols_types.keys().map(|c| q(c)).collect();
            let mut insert_query =
                format!("INSERT INTO {target_name} ({})\nSELECT\n", cols.join(", "));
            for col in db_cols_types.keys() {
                let s = match altered_types.get(col) {
                    Some(t) => format!("CAST({} AS {t})", q(col)),
                    None => q(col),
                };
                insert_query.push_str(&format!("{s},\n"));
            }
            insert_query.truncate(insert_query.len() - 2);
            insert_query.push_str(&format!(
                "\nFROM {}",
                sql_item_name(&temp_table_name, flavor, schema.as_deref())
            ));
            let if_exists = if flavor.drop_if_exists() {
                "IF EXISTS"
            } else {
                ""
            };
            let drop_query = format!(
                "DROP TABLE {if_exists}{}",
                sql_item_name(&temp_table_name, flavor, schema.as_deref())
            );
            return (
                vec![rename_query, create_query, insert_query, drop_query],
                new_dtypes,
            );
        }

        if flavor == Flavor::Oracle {
            // `_pipes.py:3843`: Oracle cannot change a populated column's type in place (and has
            // no `TYPE` clause), so each altered column goes through a temporary column.
            let mut queries = Vec::new();
            let temp = |c: &str| q(&format!("{c}_temp"));
            for (col, typ) in &altered_types {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nADD {} {typ}",
                    temp(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "UPDATE {target_name}\nSET {} = {}",
                    temp(col),
                    q(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!("UPDATE {target_name}\nSET {} = NULL", q(col)));
            }
            for (col, typ) in &altered_types {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nMODIFY {} {typ}",
                    q(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "UPDATE {target_name}\nSET {} = {}",
                    q(col),
                    temp(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nDROP COLUMN {}",
                    temp(col)
                ));
            }
            return (queries, new_dtypes);
        }

        let mut queries = Vec::new();
        let mut query = format!("ALTER TABLE {target_name}");
        for (col, typ) in &altered_types {
            let prefix = if flavor.mysql_partition() || flavor == Flavor::Oracle {
                "MODIFY"
            } else {
                "ALTER"
            };
            let type_prefix = if matches!(flavor, Flavor::Mssql | Flavor::Mariadb | Flavor::Mysql) {
                ""
            } else {
                "TYPE "
            };
            let column_str = if flavor != Flavor::Oracle {
                "COLUMN"
            } else {
                ""
            };
            let suffix = format!("\n{prefix} {column_str} {} {type_prefix}{typ},", q(col));
            if !flavor.single_alter_table() {
                query.push_str(&suffix);
            } else {
                queries.push(format!("{query}{}", &suffix[..suffix.len() - 1]));
            }
        }
        if !flavor.single_alter_table() {
            query.pop();
            queries.push(query);
        }
        (queries, new_dtypes)
    }

    /// `get_to_sql_dtype(pipe, df, update_dtypes)`: column → database type for `to_sql`.
    pub fn get_to_sql_dtype(
        &self,
        sp: &SqlPipe,
        df: &Frame,
        state: &PipeState,
        update_dtypes: bool,
    ) -> IndexMap<String, String> {
        let mut df_dtypes = df.dtypes();
        for (c, t) in get_special_cols(df) {
            df_dtypes.insert(c, t);
        }
        if update_dtypes {
            for (c, t) in &state.dtypes {
                df_dtypes.insert(c.clone(), t.clone());
            }
        }
        df_dtypes
            .into_iter()
            .filter(|(c, t)| !c.is_empty() && !t.is_empty())
            .map(|(c, t)| (c, history_db_type(sp, self.flavor, &t)))
            .collect()
    }
}
