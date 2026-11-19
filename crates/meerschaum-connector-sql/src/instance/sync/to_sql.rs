//! Bulk frame insertion (`to_sql`, `COPY`, multi-row `INSERT`) for [`SqlConnector`].

use super::*;

impl SqlConnector {
    /// `to_sql(df, name, if_exists='append', schema, chunksize, dtype)`: insert a frame's rows
    /// (PostgreSQL `COPY` when bulk inserts are enabled, multi-row bound `INSERT`s otherwise),
    /// in one transaction. Returns `(success, error message)`.
    pub async fn to_sql(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        dtype: &IndexMap<String, String>,
        chunksize: Option<i64>,
    ) -> (bool, String) {
        let (ok, msg, _) = self
            .to_sql_inner(df, name, schema, dtype, chunksize, &ToSqlExtras::default())
            .await;
        (ok, msg)
    }

    /// [`Self::to_sql`] with what a pipe sync adds ([`ToSqlExtras`]): `SET IDENTITY_INSERT ...
    /// ON` / `OFF` on the same connection and transaction (MSSQL autoincrement pipes given
    /// explicit keys, `_pipes.py:2201`), the text columns that keep null-like strings, and
    /// statements run first in the same transaction (a replacing sync's `DELETE`). Returns
    /// `(success, error message, rows affected by the first prelude statement)`.
    pub(crate) async fn to_sql_inner(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        dtype: &IndexMap<String, String>,
        chunksize: Option<i64>,
        extras: &ToSqlExtras<'_>,
    ) -> (bool, String, Option<u64>) {
        let identity_insert = extras.identity_insert;
        let flavor = self.flavor;
        let schema = schema.or(self.schema());
        let truncated = truncate_item_name(name, flavor);
        let cols = df.names();
        if df.height() == 0 || cols.is_empty() {
            if extras.prelude.is_empty() {
                return (true, String::new(), None);
            }
            // A replacing sync with no rows still clears its window.
            let statements: Vec<(String, Vec<SqlParam>)> = extras
                .prelude
                .iter()
                .map(|q| (q.clone(), Vec::new()))
                .collect();
            return match self.driver().exec_many_with(&statements).await {
                Ok(results) if results.iter().all(Option::is_some) => {
                    (true, String::new(), results.first().copied().flatten())
                }
                Ok(_) => (false, format!("Failed to replace rows of {name}."), None),
                Err(e) => (false, e.to_string(), None),
            };
        }
        let numeric_cols = get_numeric_cols(df);
        let encodings: Vec<ColumnEncoding> = cols
            .iter()
            .map(|c| {
                let db_type = dtype.get(c).cloned().unwrap_or_default();
                let is_numeric_dtype = db_type.to_lowercase().contains("numeric");
                let numeric = if is_numeric_dtype {
                    Some(
                        numeric_precision_scale(&db_type)
                            .map(|(p, s)| (Some(p), Some(s)))
                            .unwrap_or((None, None)),
                    )
                } else if numeric_cols.contains(c) {
                    Some(crate::dtypes::get_numeric_precision_scale(
                        Some(flavor),
                        None,
                    ))
                } else {
                    None
                };
                ColumnEncoding {
                    name: c.clone(),
                    numeric,
                    keep_null_markers: extras.keep_null_markers.contains(c),
                }
            })
            .collect();
        let n = df.height();
        let columns: Vec<_> = df.columns().iter().map(|c| &c.data).collect();
        let table_name = sql_item_name(&truncated, flavor, schema);

        if flavor.driver_family() == DriverFamily::Postgres && self.settings.bulk_insert {
            let col_list: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
            let copy_sql = format!(
                "COPY {} ({}) FROM STDIN WITH CSV NULL '\\N'",
                sql_item_name(&truncated, Flavor::Postgresql, schema),
                col_list.join(", ")
            );
            let mut buf = String::new();
            let mut fields: Vec<Option<String>> = Vec::with_capacity(cols.len());
            // Rows are read column-major.
            #[allow(clippy::needless_range_loop)]
            for i in 0..n {
                fields.clear();
                for (j, enc) in encodings.iter().enumerate() {
                    fields.push(encode_copy_field(&columns[j].value_ref(i), enc));
                }
                csv_row(&fields, &mut buf);
            }
            self.log(&copy_sql);
            if !extras.prelude.is_empty() {
                return match self
                    .driver()
                    .copy_in_after(&extras.prelude, &copy_sql, buf.into_bytes())
                    .await
                {
                    Ok((counts, _)) => (true, String::new(), counts.first().copied()),
                    Err(e) => {
                        tracing::warn!("Failed to insert into {table_name}:\n{e}");
                        (false, e.to_string(), None)
                    }
                };
            }
            return match self.driver().copy_in(&copy_sql, buf.into_bytes()).await {
                Ok(_) => (true, String::new(), None),
                Err(e) => {
                    tracing::warn!("Failed to insert into {table_name}:\n{e}");
                    (false, e.to_string(), None)
                }
            };
        }

        if flavor == Flavor::Mssql
            && self.settings.bulk_insert
            && let Some(statements) = mssql_insert_json_statements(
                &table_name,
                &cols,
                &columns,
                &encodings,
                dtype,
                &numeric_cols,
                n,
                match chunksize {
                    Some(-1) => Some(self.settings.chunksize),
                    other => other,
                },
            )
        {
            return self
                .exec_insert_statements(statements, &table_name, identity_insert, &extras.prelude)
                .await;
        }

        let default_chunksize = self.settings.chunksize;
        let mut chunk = match chunksize {
            Some(-1) => Some(default_chunksize),
            other => other,
        };
        if let (Some(c), Some(max)) = (chunk, max_chunks(flavor))
            && c > max
        {
            chunk = Some(max);
        }
        let max_rows_per_stmt = (max_bind_params(flavor) / cols.len()).max(1);
        let rows_per_stmt = if flavor == Flavor::Oracle {
            // PARITY: SQLAlchemy uses executemany on Oracle. Multi-row VALUES is unavailable
            // before 23c, and INSERT ALL evaluates an identity sequence only once per statement.
            1
        } else {
            chunk
                .map(|c| (c.max(1) as usize).min(max_rows_per_stmt))
                .unwrap_or(max_rows_per_stmt)
        };
        let col_names: Vec<String> = cols
            .iter()
            .map(|c| sql_item_name(c, flavor, None))
            .collect();
        let mut statements: Vec<(String, Vec<SqlParam>)> = Vec::new();
        let mut start = 0;
        while start < n {
            let end = (start + rows_per_stmt).min(n);
            let mut params = Vec::with_capacity((end - start) * cols.len());
            let mut groups = Vec::with_capacity(end - start);
            #[allow(clippy::needless_range_loop)]
            for i in start..end {
                let mut cells = Vec::with_capacity(cols.len());
                for (j, enc) in encodings.iter().enumerate() {
                    let param = encode_param(&columns[j].value_ref(i), flavor, enc);
                    cells.push(typed_placeholder(
                        flavor,
                        params.len() + 1,
                        &param,
                        dtype.get(&cols[j]).map(String::as_str).unwrap_or(""),
                    ));
                    params.push(param);
                }
                groups.push(format!("({})", cells.join(", ")));
            }
            let sql = format!(
                "INSERT INTO {table_name} ({}) VALUES {}",
                col_names.join(", "),
                groups.join(", ")
            );
            statements.push((sql, params));
            start = end;
        }
        self.exec_insert_statements(statements, &table_name, identity_insert, &extras.prelude)
            .await
    }

    /// Run `to_sql`'s insert statements in one transaction, optionally between
    /// `SET IDENTITY_INSERT ... ON` and `OFF`, after `prelude` (in the same transaction).
    pub(super) async fn exec_insert_statements(
        &self,
        mut statements: Vec<(String, Vec<SqlParam>)>,
        table_name: &str,
        identity_insert: bool,
        prelude: &[String],
    ) -> (bool, String, Option<u64>) {
        if let Some((sql, _)) = statements.first() {
            self.log(sql);
        }
        if identity_insert {
            statements.insert(
                0,
                (format!("SET IDENTITY_INSERT {table_name} ON"), Vec::new()),
            );
            statements.push((format!("SET IDENTITY_INSERT {table_name} OFF"), Vec::new()));
        }
        for (i, q) in prelude.iter().enumerate() {
            self.log(q);
            statements.insert(i, (q.clone(), Vec::new()));
        }
        match self.driver().exec_many_with(&statements).await {
            Ok(results) if results.iter().all(Option::is_some) => (
                true,
                String::new(),
                if prelude.is_empty() {
                    None
                } else {
                    results.first().copied().flatten()
                },
            ),
            Ok(_) => (
                false,
                format!("Failed to insert rows into {table_name}."),
                None,
            ),
            Err(e) => (false, e.to_string(), None),
        }
    }
}

/// `mssql_insert_json` (`_sql.py:1192`), the MSSQL bulk path: one
/// `INSERT ... SELECT ... FROM OPENJSON(@P1) WITH (...)` per chunk of rows, the rows bound as a
/// single JSON document. `None` when a frame column has no declared type (the row-batch
/// `INSERT` path handles it instead).
#[allow(clippy::too_many_arguments)]
fn mssql_insert_json_statements(
    table_name: &str,
    cols: &[String],
    columns: &[&meerschaum_kernel::ColumnData],
    encodings: &[ColumnEncoding],
    dtype: &IndexMap<String, String>,
    numeric_cols: &[String],
    n: usize,
    chunksize: Option<i64>,
) -> Option<Vec<(String, Vec<SqlParam>)>> {
    let flavor = Flavor::Mssql;
    // `cols_pd_types` -> `cols_db_types` (`_sql.py:885-897`).
    let cols_types: IndexMap<String, String> = dtype
        .iter()
        .map(|(col, typ)| {
            let mut pd = get_pd_type_from_db_type(typ, true);
            if cols.contains(col)
                && (typ.to_lowercase().contains("numeric") || numeric_cols.contains(col))
                && let Some((p, s)) =
                    numeric_precision_scale(typ).filter(|(p, s)| *p != 0 && *s != 0)
            {
                pd = format!("numeric[{p},{s}]");
            }
            (col.clone(), get_db_type_from_pd_type(&pd, Some(flavor)))
        })
        .collect();
    if cols.iter().any(|c| !cols_types.contains_key(c)) {
        return None;
    }
    let col_list = cols
        .iter()
        .map(|c| format!("[{c}]"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let with_clause = cols_types
        .iter()
        .map(|(c, t)| format!("[{c}] {t} '$.\"{c}\"'"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let sql = format!(
        "INSERT INTO {table_name} (\n    {col_list}\n)\nSELECT\n    {col_list}\nFROM OPENJSON({})\nWITH (\n    {with_clause}\n);",
        crate::driver::params::placeholder(flavor, 1)
    );
    let rows_per_chunk = chunksize
        .filter(|c| *c > 0)
        .map(|c| c as usize)
        .unwrap_or(n)
        .max(1);
    let mut statements = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + rows_per_chunk).min(n);
        let docs: Vec<serde_json::Value> = (start..end)
            .map(|i| {
                let mut doc = serde_json::Map::with_capacity(cols.len());
                for (j, enc) in encodings.iter().enumerate() {
                    doc.insert(
                        cols[j].clone(),
                        encode_json_cell(&columns[j].value_ref(i), flavor, enc),
                    );
                }
                serde_json::Value::Object(doc)
            })
            .collect();
        let json = serde_json::to_string(&docs).ok()?;
        statements.push((sql.clone(), vec![SqlParam::Text(json)]));
        start = end;
    }
    Some(statements)
}
