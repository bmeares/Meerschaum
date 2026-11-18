//! First plain native target: CREATE, native indices and admitted inserts share
//! the real writer and a structural commit intent. No standalone DDL fallback.
use super::*;
use meerschaum_connector::mutation::{NativeDataEffect, NativeSchemaEffect};
use sha2::{Digest, Sha256};
use std::sync::Mutex;

pub(super) fn digest<T: serde::Serialize>(value: &T) -> Result<String, crate::SqlError> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| error("Cannot seal the native schema effect."))?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) struct CreatePlan {
    ddl: Vec<(String, Vec<SqlParam>)>,
    types: IndexMap<String, String>,
    table_sql: String,
    index_forms: Vec<String>,
}
impl CreatePlan {
    pub(super) fn new(
        conn: &SqlConnector,
        pipe: &SqlPipe,
        state: &PipeState,
        frame: &Frame,
    ) -> ConnectorResult<Self> {
        refuse_internal_target(conn, pipe)?;
        if conn.flavor != Flavor::Sqlite
            || state.exists
            || frame.width() == 0
            || frame.width() > 512
            || pipe.autoincrement()
            || conn.should_partition(pipe)
            || conn
                .get_pipe_schema(pipe)
                .as_deref()
                .is_some_and(|schema| schema != "main")
        {
            return Err(be(
                "This first target needs an unsupported native allocation/schema adapter.",
            ));
        }
        let mut dtypes = frame.dtypes();
        for (_, column) in columns_map(pipe) {
            if !dtypes.contains_key(&column) {
                return Err(be(
                    "A first target cannot generate a missing declared role column.",
                ));
            }
        }
        for (column, dtype) in get_special_cols(frame) {
            dtypes.insert(column, dtype);
        }
        for (column, dtype) in &state.dtypes {
            dtypes.insert(column.clone(), dtype.clone());
        }
        let types = dtypes
            .into_iter()
            .map(|(column, dtype)| {
                (
                    column,
                    get_db_type_from_pd_type(&dtype, Some(Flavor::Sqlite)),
                )
            })
            .collect::<IndexMap<_, _>>();
        if types.values().any(|dtype| !native_type(dtype)) {
            return Err(be("A first target has an unproven native type."));
        }
        let primary = pipe.column("primary");
        if primary.as_ref().is_some_and(|column| {
            frame.column(column).is_none_or(|values| {
                (0..frame.height()).any(|row| values.data.value_ref(row).is_na())
            })
        }) {
            return Err(be(
                "A first target cannot allocate or replace a generated primary key.",
            ));
        }
        let table = get_create_table_queries(
            &QueryOrDtypes::Dtypes(types.clone()),
            &pipe.target,
            Flavor::Sqlite,
            &CreateTableOptions {
                primary_key: primary.clone(),
                primary_key_db_type: primary
                    .as_ref()
                    .and_then(|column| types.get(column))
                    .cloned(),
                autoincrement: false,
                parse_dtypes: false,
                ..Default::default()
            },
        )
        .map_err(be)?;
        if table.len() != 1 {
            return Err(be("A first target requires one plain native table."));
        }
        let indices = get_create_index_queries(
            &conn.ctx(),
            pipe,
            &IndexState {
                existing_cols_types: types.clone(),
                // CREATE TABLE already declares this exact native key. Without
                // this fact the legacy index builder plans a separate rebuild.
                existing_cols_indices: primary
                    .as_ref()
                    .map(|column| {
                        IndexMap::from([(
                            column.clone(),
                            vec![crate::queries::table::IndexDoc {
                                type_: Some("PRIMARY KEY".into()),
                                ..Default::default()
                            }],
                        )])
                    })
                    .unwrap_or_default(),
                ..Default::default()
            },
        )
        .map_err(be)?
        .into_values()
        .flatten()
        .collect::<Vec<_>>();
        if indices.iter().any(|sql| {
            !sql.starts_with("CREATE INDEX ") && !sql.starts_with("CREATE UNIQUE INDEX ")
        }) {
            return Err(be(
                "The native first-target index plan requires an unsupported rebuild or schema effect.",
            ));
        }
        let index_forms = indices.iter().flat_map(|sql| references(sql)).collect();
        let table_sql = ddl(&table[0]);
        let ddl = table
            .into_iter()
            .chain(indices)
            .map(|sql| (sql, Vec::new()))
            .collect::<Vec<_>>();
        crate::driver::write_guard::bounded_inputs(&ddl).map_err(be)?;
        Ok(Self {
            ddl,
            types,
            table_sql,
            index_forms,
        })
    }
    fn validate_created(&self, target: &str, objects: &Objects) -> Result<(), crate::SqlError> {
        let mut tables = 0;
        for (kind, name, sql) in objects {
            match (kind.as_str(), sql) {
                ("table", Some(sql))
                    if name.eq_ignore_ascii_case(target) && ddl(sql) == self.table_sql =>
                {
                    tables += 1
                }
                ("index", None) => {} // only the exact plain table can create an implicit key index
                ("index", Some(sql)) if self.index_forms.contains(&ddl(sql)) => {}
                _ => {
                    return Err(error(
                        "The created target differs from its exact native schema plan.",
                    ));
                }
            }
        }
        if tables != 1 {
            return Err(error(
                "The native first target was not created exactly once.",
            ));
        }
        Ok(())
    }
}

struct CreateGuard {
    original: WindowGuard,
    plan: CreatePlan,
    after: Mutex<Option<SchemaSeal>>,
}
impl CreateGuard {
    async fn verify_created(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        let after = self
            .after
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| error("The first target has no held native schema proof."))?;
        after.verify(transaction, writer).await?;
        self.original
            .verify_registration(transaction, &self.original.original)
            .await
    }
}
#[async_trait::async_trait]
impl TargetGuard for CreateGuard {
    fn binding(&self) -> &TargetBinding {
        &self.original.schema.binding
    }
    fn statements(&self, statements: &[(String, Vec<SqlParam>)]) -> Result<(), crate::SqlError> {
        if statements
            .iter()
            .map(|(sql, _)| sql)
            .ne(self.original.expected_sql.iter())
        {
            return Err(error("The first-target insert plan changed."));
        }
        crate::driver::write_guard::bounded_inputs(statements.iter().chain(&self.plan.ddl))
    }
    async fn prepare(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        // This real writer was acquired before its first fresh source check. Its
        // empty auth snapshot cannot retain a role change committed while waiting.
        self.original.verify_original(transaction, writer).await?;
        for (sql, params) in &self.plan.ddl {
            writer.execute_native(transaction, sql, params).await?;
        }
        let batch = transaction
            .raw_query(
                objects_query(),
                &[SqlParam::Text(self.original.schema.target.clone())],
            )
            .await?;
        let objects = decode_objects(&Frame::from_record_batches(&[batch])?)?;
        self.plan
            .validate_created(&self.original.schema.target, &objects)?;
        *self.after.lock().unwrap_or_else(|e| e.into_inner()) = Some(SchemaSeal {
            target: self.original.schema.target.clone(),
            objects,
            binding: self.original.schema.binding.clone(),
        });
        Ok(())
    }
    async fn validate(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.verify_created(transaction, writer).await
    }
    fn data_effect(&self, counts: &[Option<u64>]) -> Result<NativeDataEffect, crate::SqlError> {
        let after = self.after.lock().unwrap_or_else(|e| e.into_inner());
        let after = after
            .as_ref()
            .ok_or_else(|| error("The structural effect has no actual native schema."))?;
        let schema = NativeSchemaEffect::create_target(
            digest(&self.original.schema.objects)?,
            digest(&after.objects)?,
            digest(&self.plan.ddl.iter().map(|(sql, _)| sql).collect::<Vec<_>>())?,
        )
        .map_err(|reason| error(&reason))?;
        Ok(self.original.data_effect(counts)?.with_schema(schema))
    }
}

impl SqlConnector {
    pub(in super::super) async fn exec_window_create(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        frame: &Frame,
        options: &SyncOptions,
        window: &DataWindow,
    ) -> ConnectorResult<SyncResult> {
        window
            .validate_frame(pipe.pipe(), frame, options)
            .map_err(be)?;
        let plan = CreatePlan::new(self, pipe, state, frame)?;
        let schema = SchemaSeal::capture(self, pipe, state).await?;
        if !schema.objects.is_empty() {
            return Err(be("The native first target is no longer absent."));
        }
        let registration = self
            .get_pipe_id(pipe.pipe())
            .await?
            .ok_or_else(|| be("The first target lost its original registration."))?;
        let prepared = PipeState {
            exists: true,
            cols_types: plan.types.clone(),
            dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
        };
        let dtype = self.get_to_sql_dtype(pipe, frame, &prepared, true);
        let statements = super::super::snapshot::insert_statements_in(
            &pipe.target,
            Some("main"),
            frame,
            &dtype,
            &null_marker_columns(pipe, &prepared),
        )?;
        let guard = CreateGuard {
            original: WindowGuard {
                schema,
                add_columns: None,
                window: window.clone(),
                patch_axis_refusal: None,
                old_rows: None,
                registration,
                metadata_table: self.instance_table_name("mrsm_pipes"),
                original: pipe.pipe().raw_parameters().clone(),
                expected_sql: statements.iter().map(|(sql, _)| sql.clone()).collect(),
                stage: None,
                effect_pipe: pipe.pipe().clone(),
                effect_options: options.clone(),
                committed_parameters: None,
            },
            plan,
            after: Mutex::new(None),
        };
        self.forget_catalog_table(&pipe.target);
        let result = self
            .driver()
            .exec_many_guarded(
                statements,
                Arc::new(guard),
                std::time::Duration::from_secs(30),
            )
            .await;
        self.forget_catalog_table(&pipe.target);
        let (counts, receipt) = result.map_err(be)?;
        if let Some(receipt) = receipt {
            receipt.acknowledge().await.map_err(be)?;
        }
        let inserted = counts.into_iter().flatten().sum();
        let mut result = SyncResult::new(true, format!("Inserted {inserted}, updated 0 rows."));
        result.inserted = inserted;
        Ok(result)
    }
}
