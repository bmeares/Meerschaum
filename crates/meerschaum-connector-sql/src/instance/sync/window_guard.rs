//! Native SQLite OLD-row guard: exact target from SqlPipe, exact matching
//! key predicates from the native update plan, SAME held writer after BEGIN/precommit.
//! No generic SQL target guessing, request descriptors, host imports or PG source audit.

use super::*;
use crate::driver::write_guard::{HeldWriter, TargetBinding, TargetGuard};
use crate::queries::index::{IndexState, get_create_index_queries};
use crate::queries::update::GuardedUpdate;
use meerschaum_connector::mutation::{DataWindow, RowPredicate};
use std::sync::Arc;

#[cfg(all(test, unix))]
mod tests;

mod create;
mod schema_add;
pub(super) use schema_add::AddColumns;

const MAX_SCHEMA_ROWS: usize = 4096;
const MAX_SCHEMA_SQL: usize = 128 * 1024;
type Objects = Vec<(String, String, Option<String>)>;
fn error(reason: &str) -> crate::SqlError {
    crate::SqlError::InvalidArgument(reason.into())
}
fn union(spans: &[String]) -> String {
    match spans {
        [] => "0=1".into(),
        [span] => span.clone(),
        _ => {
            let middle = spans.len() / 2;
            format!(
                "({}) OR ({})",
                union(&spans[..middle]),
                union(&spans[middle..])
            )
        }
    }
}

/// Compare native emitted DDL without changing any quoted identifier/literal. This is
/// reference comparison against repo builders, not an unknown expression allowlist.
fn ddl(sql: &str) -> String {
    let mut result = String::new();
    let mut quote = None;
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        if let Some(end) = quote {
            result.push(character);
            if character == end {
                if chars.peek() == Some(&end) {
                    result.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
        } else if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
            result.push(character);
        } else if !character.is_whitespace() {
            result.push(character);
        }
    }
    result
}
fn references(sql: &str) -> Vec<String> {
    let mut forms = vec![ddl(sql)];
    for prefix in [
        "CREATE INDEX IF NOT EXISTS ",
        "CREATE UNIQUE INDEX IF NOT EXISTS ",
    ] {
        if let Some(rest) = sql.strip_prefix(prefix) {
            forms.push(ddl(&format!(
                "{} {rest}",
                prefix.replace(" IF NOT EXISTS ", "")
            )));
        }
    }
    forms
}
/// Compare a planned native index with the definition SQLite actually stored.
pub(super) fn same_index_ddl(planned: &str, stored: &str) -> bool {
    references(planned).contains(&ddl(stored))
}
fn native_type(dtype: &str) -> bool {
    let dtype = dtype.trim().to_ascii_uppercase();
    let (name, parameters) = dtype
        .split_once('(')
        .map_or((dtype.as_str(), None), |(name, rest)| {
            (name.trim(), Some(rest))
        });
    matches!(
        name,
        "INTEGER"
            | "INT"
            | "BIGINT"
            | "SMALLINT"
            | "TINYINT"
            | "BOOLEAN"
            | "BOOL"
            | "FLOAT"
            | "DOUBLE"
            | "REAL"
            | "TEXT"
            | "BLOB"
            | "TIMESTAMP"
            | "DATETIME"
            | "DATE"
            | "NUMERIC"
            | "DECIMAL"
            | "VARCHAR"
    ) && parameters.is_none_or(|parameters| {
        parameters.ends_with(')')
            && parameters[..parameters.len() - 1]
                .chars()
                .all(|character| character.is_ascii_digit() || character == ',' || character == ' ')
    })
}
fn objects_query() -> &'static str {
    "WITH objects AS (SELECT type,name,sql,length(CAST(sql AS BLOB)) AS sql_bytes,CASE WHEN sql IS NULL THEN 1 ELSE 0 END AS implicit_sql FROM main.sqlite_schema WHERE lower(tbl_name)=lower(?) ORDER BY type,name LIMIT 4097), bounded AS (SELECT *,SUM(COALESCE(sql_bytes,0)+length(CAST(name AS BLOB))) OVER() AS total_bytes FROM objects) SELECT type,CASE WHEN total_bytes<=131072 AND length(CAST(name AS BLOB))<=2048 THEN name ELSE NULL END AS name,sql_bytes,implicit_sql,CASE WHEN total_bytes<=131072 THEN sql ELSE NULL END AS sql FROM bounded"
}
fn decode_objects(frame: &Frame) -> Result<Objects, crate::SqlError> {
    if frame.height() > MAX_SCHEMA_ROWS {
        return Err(error("The window target schema exceeds its bound."));
    }
    let docs = meerschaum_connector::pipe_ops::frame_to_docs(frame);
    let mut bytes = 0usize;
    docs.into_iter()
        .map(|row| {
            let typ = super::super::kv_str(row.get("type"))
                .ok_or_else(|| error("Unknown target schema object."))?;
            let name = super::super::kv_str(row.get("name"))
                .ok_or_else(|| error("Unknown target schema name."))?;
            let sql = super::super::kv_str(row.get("sql"));
            // Kernel NULL flavors vary with the native column dtype (NA/NaN/Null).
            // Only the explicit native source flag can distinguish an implicit
            // index from an explicit schema string removed by the byte cap.
            let valid_sql = match super::super::kv_int(row.get("implicit_sql")) {
                Some(1) => {
                    typ == "index"
                        && sql.is_none()
                        && row.get("sql_bytes").is_some_and(KValue::is_na)
                }
                Some(0) => sql.as_ref().is_some_and(|sql| {
                    super::super::kv_int(row.get("sql_bytes"))
                        .is_some_and(|bytes| bytes >= 0 && bytes as usize == sql.len())
                }),
                _ => false,
            };
            if !valid_sql {
                return Err(error(
                    "An oversized explicit schema object cannot be treated as an implicit index.",
                ));
            }
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(sql.as_ref().map_or(0, String::len));
            if bytes > MAX_SCHEMA_SQL || (sql.is_none() && typ != "index") {
                return Err(error(
                    "The window target schema is unavailable or exceeds its bound.",
                ));
            }
            Ok((typ, name, sql))
        })
        .collect()
}

/// This captured guard cannot use the legacy parser's empty-map fallback: SQL
/// truncation, NULL and malformed metadata must never equal a registered `{}`.
fn decode_definition(value: Option<&KValue>) -> Result<Map, crate::SqlError> {
    let Some(KValue::Str(text)) = value else {
        return Err(error(
            "The bounded native registered definition is unavailable.",
        ));
    };
    if text.len() > 4 * 1024 * 1024 {
        return Err(error(
            "The native registered definition exceeds its byte bound.",
        ));
    }
    match Value::from_json_str(text) {
        Ok(Value::Map(parameters)) => Ok(parameters),
        _ => Err(error(
            "The native registered definition is not a valid JSON map.",
        )),
    }
}

/// Native target identity, including the physical/truncated alias. A held auth
/// snapshot may never be used while changing its own roles or registrations.
fn refuse_internal_target(conn: &SqlConnector, pipe: &SqlPipe) -> ConnectorResult<()> {
    let physical = truncate_item_name(&pipe.target, conn.flavor);
    let name = physical.to_ascii_lowercase();
    if pipe.pipe().connector_keys() == "mrsm"
        || pipe.pipe().temporary
        || name.starts_with("mrsm_")
        || name.starts_with("_mrsm_")
        || name.starts_with("sqlite_")
        || super::super::tables::INSTANCE_TABLES
            .iter()
            .any(|internal| {
                conn.pipe_table_name(pipe)
                    .eq_ignore_ascii_case(&conn.instance_table_name(internal))
            })
    {
        return Err(be(
            "A captured data window cannot mutate protected authorization/registration or internal storage, including physical target aliases.",
        ));
    }
    Ok(())
}

fn plain_table_sql(pipe: &SqlPipe, types: &IndexMap<String, String>) -> ConnectorResult<String> {
    let primary = pipe.column("primary");
    let sql = get_create_table_queries(
        &QueryOrDtypes::Dtypes(types.clone()),
        &pipe.target,
        Flavor::Sqlite,
        &CreateTableOptions {
            primary_key_db_type: primary
                .as_ref()
                .and_then(|column| types.get(column))
                .cloned(),
            primary_key: primary,
            autoincrement: pipe.autoincrement(),
            parse_dtypes: false,
            ..Default::default()
        },
    )
    .map_err(be)?;
    match sql.as_slice() {
        [statement] => Ok(ddl(statement)),
        _ => Err(be("Expected one native plain-table declaration.")),
    }
}

#[derive(Clone)]
struct SchemaSeal {
    target: String,
    objects: Objects,
    binding: TargetBinding,
}
impl SchemaSeal {
    /// Read-only native preflight BEFORE metadata/DDL as well as just before final DML.
    async fn capture(
        conn: &SqlConnector,
        pipe: &SqlPipe,
        state: &PipeState,
    ) -> ConnectorResult<Self> {
        refuse_internal_target(conn, pipe)?;
        if conn.flavor != Flavor::Sqlite
            || conn
                .get_pipe_schema(pipe)
                .as_deref()
                .is_some_and(|schema| schema != "main")
        {
            return Err(be("This target has no proven native window adapter."));
        }
        if state.cols_types.values().any(|dtype| !native_type(dtype)) {
            return Err(be("The target has a custom/unproven native type."));
        }
        let target = truncate_item_name(&pipe.target, Flavor::Sqlite);
        let binding = conn.driver().bind_window_target().await.map_err(be)?;
        let foreign_query = "SELECT 1 AS found FROM main.sqlite_schema AS s JOIN pragma_foreign_key_list(s.name) AS f WHERE s.type='table' AND (lower(s.name)=lower(?) OR lower(f.\"table\")=lower(?)) LIMIT 1";
        let mut frames = conn
            .driver()
            .probe_window_target(
                vec![
                    (objects_query().into(), vec![SqlParam::Text(target.clone())]),
                    (
                        foreign_query.into(),
                        vec![
                            SqlParam::Text(target.clone()),
                            SqlParam::Text(target.clone()),
                        ],
                    ),
                ],
                binding.clone(),
            )
            .await
            .map_err(be)?;
        let foreign = frames
            .pop()
            .ok_or_else(|| be("Missing native schema side-effect proof."))?;
        let objects = decode_objects(
            &frames
                .pop()
                .ok_or_else(|| be("Missing native schema proof."))?,
        )
        .map_err(be)?;
        let table = objects
            .iter()
            .find(|(typ, name, _)| typ == "table" && name.eq_ignore_ascii_case(&target));
        if state.exists {
            let Some((_, _, Some(table))) = table else {
                return Err(be("The exact native window target is not a plain table."));
            };
            if ddl(table) != plain_table_sql(pipe, &state.cols_types)? {
                return Err(be(
                    "The target's default/generated/constraint layout is not a proven native schema. Windowed writes refuse this layout before effects.",
                ));
            }
        } else if !objects.is_empty() {
            return Err(be("A non-table object occupies the target."));
        }
        let ctx = conn.ctx();
        let index_state = IndexState {
            existing_cols_types: state.cols_types.clone(),
            ..Default::default()
        };
        let legacy = if objects
            .iter()
            .any(|(kind, name, _)| kind == "index" && name == "None")
        {
            crate::queries::index::legacy_sqlite_id_index(&ctx, pipe, &index_state).map_err(be)?
        } else {
            None
        };
        let expected_indices = get_create_index_queries(&ctx, pipe, &index_state)
            .map_err(be)?
            .into_values()
            .flatten()
            .chain(legacy)
            .flat_map(|sql| references(&sql))
            .collect::<Vec<_>>();
        for (typ, _, sql) in &objects {
            match (typ.as_str(), sql) {
                ("table", Some(_)) => {}
                ("index", None) => {} // implicit native PK/UNIQUE; table DDL was exact above.
                ("index", Some(sql)) if expected_indices.contains(&ddl(sql)) => {}
                _ => {
                    return Err(be(
                        "The target has an unproven trigger/view/index expression; no windowed DML was admitted.",
                    ));
                }
            }
        }
        // Both outbound and inbound cascades could affect rows outside the issued pipe.
        if foreign.height() != 0 {
            return Err(be(
                "The target participates in foreign-key side effects without a bounded native adapter.",
            ));
        }
        Ok(Self {
            target,
            objects,
            binding,
        })
    }
    async fn verify(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        if !writer.belongs_to(&self.binding) {
            return Err(error("The target writer/file identity changed."));
        }
        let batch = transaction
            .raw_query(objects_query(), &[SqlParam::Text(self.target.clone())])
            .await?;
        let frame = Frame::from_record_batches(&[batch])?;
        if decode_objects(&frame)? != self.objects {
            return Err(error(
                "The native target schema changed after its window preparation.",
            ));
        }
        let foreign=transaction.raw_query("SELECT 1 FROM main.sqlite_schema AS s JOIN pragma_foreign_key_list(s.name) AS f WHERE s.type='table' AND (lower(s.name)=lower(?) OR lower(f.\"table\")=lower(?)) LIMIT 1",
            &[SqlParam::Text(self.target.clone()),SqlParam::Text(self.target.clone())]).await?;
        if foreign.num_rows() != 0 {
            return Err(error(
                "A foreign-key side effect appeared before the locked write.",
            ));
        }
        Ok(())
    }
}

pub(super) struct WindowGuard {
    schema: SchemaSeal,
    add_columns: Option<AddColumns>,
    window: DataWindow,
    /// Exact native patch table has already cast rows using its real SQLite affinities.
    /// No VALUES/CAST approximation or attacker-proposed matching predicate is accepted.
    patch_axis_refusal: Option<String>,
    old_rows: Option<String>,
    registration: i64,
    metadata_table: String,
    original: Map,
    expected_sql: Vec<String>,
    stage: Option<NativeStage>,
    effect_pipe: Pipe,
    effect_options: SyncOptions,
    committed_parameters: Option<Map>,
}

struct NativeStage {
    setup: Vec<(String, Vec<SqlParam>)>,
    cleanup: (String, Vec<SqlParam>),
}
pub(super) struct WindowBatch<'a> {
    pub pipe: &'a SqlPipe,
    pub state: &'a PipeState,
    pub window: &'a DataWindow,
    pub unseen: &'a Frame,
    pub update: Option<&'a Frame>,
    pub options: &'a SyncOptions,
    pub upsert: bool,
    pub add_columns: Option<AddColumns>,
}
struct PreparedUpdate {
    evidence: GuardedUpdate,
    stage: NativeStage,
    count_all: bool,
    input_count: u64,
}

impl SqlConnector {
    pub(super) async fn snapshot_window_guard(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        window: DataWindow,
        options: &SyncOptions,
        registration: i64,
        original: &Map,
    ) -> ConnectorResult<WindowGuard> {
        let deletion = self.replace_delete_query(
            pipe,
            state,
            &meerschaum_connector::DataQuery {
                begin: options.begin.clone(),
                end: options.end.clone(),
                params: options.params.clone(),
                ..Default::default()
            },
        )?;
        let mut guard = self
            .window_guard(
                pipe,
                state,
                window,
                None,
                &[(deletion, Vec::new())],
                options,
            )
            .await?;
        // These are the private native Plan's exact original proof, separately
        // compared against the SAME held registry transaction before any changes.
        if guard.registration != registration {
            return Err(be(
                "The snapshot registration changed before its native window guard.",
            ));
        }
        guard.original = original.clone();
        Ok(guard)
    }
    pub(super) async fn preflight_window_schema(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        window: &DataWindow,
    ) -> ConnectorResult<()> {
        if window.native_reference() != pipe.pipe().pipe_ref() {
            return Err(be("The window belongs to another native pipe."));
        }
        Self::filter_columns(pipe, state, window)?;
        SchemaSeal::capture(self, pipe, state).await?;
        Ok(())
    }
    pub(crate) async fn preflight_window_frame(
        &self,
        pipe: &Pipe,
        frame: &Frame,
        options: &SyncOptions,
        window: &DataWindow,
    ) -> ConnectorResult<()> {
        let sql_pipe = self.sql_pipe(pipe)?;
        refuse_internal_target(self, &sql_pipe)?;
        window.validate_frame(pipe, frame, options).map_err(be)?;
        if frame.height() == 0 && frame.width() == 0 && !options.replace {
            return Ok(());
        }
        if let Some(axis) = window.column()
            && pipe.column("primary").as_deref() == Some(axis)
            && frame.height() > 0
            && frame.column(axis).is_none_or(|column| {
                (0..frame.height()).any(|row| column.data.value_ref(row).is_na())
            })
        {
            return Err(be(
                "The database-generated axis requires a controlled native allocation adapter; no metadata or DDL was admitted.",
            ));
        }
        let state = self.pipe_state(&sql_pipe).await;
        if pipe.temporary || pipe.connector_keys() == "mrsm" || self.should_partition(&sql_pipe) {
            return Err(be(
                "This internal/partitioned target has no admitted row-window transition adapter.",
            ));
        }
        if sql_pipe.autoincrement()
            && sql_pipe.column("primary").as_ref().is_some_and(|primary| {
                frame.column(primary).is_some_and(|column| {
                    (0..frame.height()).any(|row| !column.data.value_ref(row).is_na())
                })
            })
        {
            return Err(be(
                "Explicit sequence-resetting primary keys require a controlled native allocation adapter; this batch was refused before inference or DDL.",
            ));
        }
        // This coherent first adapter covers a prepared native target. No earlier
        // unguarded ADD/ALTER/rebuild may run while target-schema transition support
        // remains unregistered. Ordinary unmarked native writes keep their old path.
        let incoming = self.df_cols_types_refined(frame);
        // The native alter planner interprets this field as NEW special
        // columns, not every special value observed in the frame. Re-adding an
        // already declared BLOB/bytes column forces a false TEXT rebuild.
        let detected_special = get_special_cols(frame);
        let new_special_cols = detected_special
            .iter()
            .filter(|(column, _)| {
                state
                    .dtypes
                    .get(*column)
                    .is_none_or(|dtype| !is_dtype_special(dtype))
            })
            .map(|(column, dtype)| (column.clone(), dtype.clone()))
            .collect();
        let mut alter = AlterColumnsInput {
            df_cols_types: frame.dtypes(),
            numeric_cols: get_numeric_cols(frame),
            db_cols_types: state.cols_types.clone(),
            pipe_dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
            new_special_cols,
            session_id: "window-read-only-plan".into(),
        };
        if !state.exists {
            create::CreatePlan::new(self, &sql_pipe, &state, frame)?;
            return self
                .preflight_window_schema(&sql_pipe, &state, window)
                .await;
        }
        let add = !self
            .add_columns_queries_for(&sql_pipe, &incoming, &state.cols_types)
            .is_empty();
        if add {
            AddColumns::new(self, &sql_pipe, &state, &incoming)?
                .project_alter(&mut alter, &incoming);
        }
        let change = !self
            .alter_columns_queries_for(&sql_pipe, &alter)
            .0
            .is_empty();
        if change || (add && !alter.new_special_cols.is_empty()) {
            // Main-owned diagnostics only: never emit row values, query text,
            // credentials, or full definitions into the public refusal body.
            fn types(map: &IndexMap<String, String>) -> Vec<(String, String)> {
                let bounded =
                    |value: &str| value.chars().filter(|c| !c.is_control()).take(48).collect();
                map.iter()
                    .take(12)
                    .map(|(column, dtype)| (bounded(column), bounded(dtype)))
                    .collect()
            }
            tracing::warn!(
                incoming = ?types(&alter.df_cols_types),
                stored = ?types(&state.cols_types),
                declared = ?types(&state.dtypes),
                detected_special = ?types(&detected_special),
                new_special = ?types(&alter.new_special_cols),
                add, change, "Captured native frame requires a schema transition"
            );
            return Err(be(
                "This node needs a Main-guarded native schema transition before its row window can run. No schema fallback or metadata/DDL effects were admitted by this frame preflight.",
            ));
        }
        self.preflight_window_schema(&sql_pipe, &state, window)
            .await
    }
    /// Called with the SAME native update evidence that emitted the actual DML. Insert
    /// uses no OLD-row query because its native SQL is plain INSERT + exact schema;
    /// arbitrary conflict REPLACE/defaults/triggers have already been rejected.
    pub(super) async fn window_guard(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        window: DataWindow,
        evidence: Option<&GuardedUpdate>,
        statements: &[(String, Vec<SqlParam>)],
        options: &SyncOptions,
    ) -> ConnectorResult<WindowGuard> {
        window.validate_replacement(options).map_err(be)?;
        let expected_sql = if let Some(evidence) = evidence {
            if options.replace {
                return Err(be("A replacement cannot adopt an update/upsert plan."));
            }
            evidence.queries().to_vec()
        } else {
            let deletion = if options.replace && state.exists {
                Some(self.replace_delete_query(
                    pipe,
                    state,
                    &meerschaum_connector::DataQuery {
                        begin: options.begin.clone(),
                        end: options.end.clone(),
                        params: options.params.clone(),
                        ..Default::default()
                    },
                )?)
            } else {
                None
            };
            let offset = usize::from(deletion.is_some());
            if deletion.as_ref().is_some_and(|delete| {
                statements
                    .first()
                    .is_none_or(|statement| &statement.0 != delete)
            }) {
                return Err(be(
                    "The actual replacement DELETE differs from Main's exact native bounds/filter plan.",
                ));
            }
            let prefix = format!(
                "INSERT INTO {} (",
                sql_item_name(&pipe.target, Flavor::Sqlite, Some("main"))
            );
            if statements
                .iter()
                .skip(offset)
                .any(|(sql, _)| !sql.starts_with(&prefix))
            {
                return Err(be(
                    "The target write did not use its native plain-insert compiler.",
                ));
            }
            statements.iter().map(|(sql, _)| sql.clone()).collect()
        };
        if statements
            .iter()
            .map(|(sql, _)| sql)
            .ne(expected_sql.iter())
        {
            return Err(be("The native DML changed after exact window preparation."));
        }
        let schema = SchemaSeal::capture(self, pipe, state).await?;
        let registration = self
            .get_pipe_id(pipe.pipe())
            .await?
            .ok_or_else(|| be("The window registration is absent."))?;
        let allowed = self.window_where(pipe, state, &window)?;
        let outside = format!("NOT COALESCE(({allowed}), 0)");
        let generated_axis = window
            .column()
            .filter(|axis| pipe.column("primary").as_deref() == Some(*axis))
            .map(|axis| format!(" OR {} IS NULL", sql_item_name(axis, Flavor::Sqlite, None)))
            .unwrap_or_default();
        let patch_axis_refusal = evidence.map(|evidence| {
            format!(
                "SELECT 1 FROM ({}) AS checked WHERE ({outside}){generated_axis} LIMIT 1",
                evidence.assigned_rows
            )
        });
        let old_rows = evidence.map(|evidence| {
            format!(
                "SELECT 1 FROM ({}) AS checked WHERE {outside} LIMIT 1",
                evidence.touched_old_rows
            )
        });
        Ok(WindowGuard {
            schema,
            add_columns: None,
            window,
            patch_axis_refusal,
            old_rows,
            registration,
            metadata_table: self.instance_table_name("mrsm_pipes"),
            original: pipe.pipe().raw_parameters().clone(),
            expected_sql,
            stage: None,
            effect_pipe: pipe.pipe().clone(),
            effect_options: options.clone(),
            committed_parameters: None,
        })
    }
    /// Verify every column which the native WHERE compiler must preserve. Its ordinary
    /// clear API skips unknown column names, which cannot represent an issued predicate.
    fn filter_columns<'a>(
        pipe: &SqlPipe,
        state: &PipeState,
        window: &'a DataWindow,
    ) -> ConnectorResult<Option<&'a Map>> {
        match window.row_predicate() {
            RowPredicate::All => Ok(None),
            RowPredicate::Filter { params, columns }
                if state.exists
                    && params.iter().all(|(name, value)| {
                        columns.get(name).is_some_and(|column| {
                            matches!(value, Value::Int(_))
                                && column.supports_integer_equality()
                                && pipe.pipe().dtype(name) == column.dtype
                                && state.cols_types.get(name) == column.native_type.as_ref()
                        })
                    }) =>
            {
                Ok(Some(params))
            }
            _ => Err(be(
                "The native SQLite filter requires its captured existing integer columns.",
            )),
        }
    }
    fn window_where(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        window: &DataWindow,
    ) -> ConnectorResult<String> {
        let params = Self::filter_columns(pipe, state, window)?;
        if window.covers_relation() && params.is_none() {
            return Ok("1=1".into());
        }
        window
            .spans()
            .iter()
            .map(|span| {
                let query = meerschaum_connector::DataQuery {
                    begin: span.begin.map(Into::into),
                    end: span.end.map(Into::into),
                    params: params.cloned(),
                    ..Default::default()
                };
                self.replace_window_where(pipe, state, &query)
                    .and_then(|where_sql| {
                        let sql = where_sql.trim();
                        if sql.is_empty() {
                            return Ok("(1=1)".into());
                        }
                        let sql = sql
                            .strip_prefix("WHERE")
                            .ok_or_else(|| be("Unknown native window predicate form."))?
                            .trim_start();
                        Ok(format!("({sql})"))
                    })
            })
            .collect::<ConnectorResult<Vec<_>>>()
            .map(|spans| {
                let predicate = union(&spans);
                match window.column().filter(|_| !window.covers_relation()) {
                    Some(column) => format!(
                        "{} IS NOT NULL AND ({predicate})",
                        sql_item_name(column, Flavor::Sqlite, None)
                    ),
                    None => predicate,
                }
            })
    }

    async fn prepare_window_update(
        &self,
        pipe: &SqlPipe,
        state: &PipeState,
        frame: &Frame,
        window: &DataWindow,
        upsert: bool,
    ) -> ConnectorResult<PreparedUpdate> {
        if self.flavor != Flavor::Sqlite || frame.width() == 0 || frame.width() > 512 {
            return Err(be(
                "This native window update has no bounded SQLite staging adapter.",
            ));
        }
        if let Some(params) = Self::filter_columns(pipe, state, window)?
            && params.keys().any(|name| !frame.contains(name))
        {
            return Err(be(
                "The native update stage must retain each captured filter column.",
            ));
        }
        let input_count = frame.height() as u64;
        let primary = pipe.column("primary");
        let axis = pipe.column("datetime");
        let join_cols: Vec<String> = match primary {
            Some(primary) => vec![primary],
            None => columns_map(pipe)
                .into_values()
                .filter(|column| state.cols_types.contains_key(column))
                .collect(),
        };
        let (conflict_target, key_null_safe) = if upsert {
            match self.upsert_conflict_target(pipe, &join_cols).await {
                Some((target, safe)) => (Some(target), safe),
                None => (None, false),
            }
        } else {
            (None, false)
        };
        let null_safe_upsert = upsert
            && pipe.null_indices()
            && !key_null_safe
            && join_cols
                .iter()
                .filter(|column| Some(*column) != axis.as_ref())
                .any(|column| {
                    frame
                        .data(column)
                        .is_ok_and(|values| values.is_null_mask().iter().any(|value| *value))
                });
        let deduplicated;
        let frame = if null_safe_upsert {
            deduplicated = last_row_per_key(frame, &join_cols);
            &deduplicated
        } else {
            frame
        };
        let stage = format!("mrsm_window_{}", uuid::Uuid::new_v4().simple());
        let columns = frame
            .names()
            .iter()
            .map(|name| {
                let dtype = state
                    .cols_types
                    .get(name)
                    .filter(|dtype| native_type(dtype))
                    .ok_or_else(|| be("The window update needs an unadmitted schema change."))?;
                Ok((name.clone(), dtype.clone()))
            })
            .collect::<ConnectorResult<IndexMap<String, String>>>()?;
        let stage_name = sql_item_name(&stage, Flavor::Sqlite, Some("temp"));
        let definitions = columns
            .iter()
            .map(|(name, dtype)| format!("{} {dtype}", sql_item_name(name, Flavor::Sqlite, None)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut setup = vec![(
            format!("CREATE TEMP TABLE {stage_name} ({definitions})"),
            Vec::new(),
        )];
        let dtype = self.get_to_sql_dtype(pipe, frame, state, true);
        setup.extend(super::snapshot::insert_statements_in(
            &stage,
            Some("temp"),
            frame,
            &dtype,
            &null_marker_columns(pipe, state),
        )?);
        let cleanup = (format!("DROP TABLE {stage_name}"), Vec::new());
        let version = self.db_version().await;
        let mut args = UpdateQueriesArgs::new(&pipe.target, &stage, join_cols, Flavor::Sqlite);
        args.schema = Some("main");
        args.patch_schema = Some("temp");
        args.target_cols_types = state.cols_types.clone();
        args.patch_cols_types = columns;
        args.datetime_col = axis.as_deref().filter(|axis| frame.contains(axis));
        args.upsert = upsert;
        args.null_indices = pipe.null_indices();
        args.cast_columns = pipe.enforce();
        args.db_version = version.as_deref();
        args.conflict_target = conflict_target;
        args.null_safe_upsert = null_safe_upsert;
        args.changed_only = pipe.upsert_changed_only();
        let compiled = crate::queries::update::compile_guarded_update(
            &args,
            window.column(),
            window.covers_relation(),
        )
        .map_err(be)?;
        Ok(PreparedUpdate {
            evidence: compiled,
            stage: NativeStage { setup, cleanup },
            count_all: upsert && pipe.upsert_changed_only(),
            input_count,
        })
    }

    /// All known incoming rows were already checked before metadata/DDL. The one
    /// target transaction checks every matching-key OLD row before either INSERT
    /// or UPDATE, so an outside OLD row cannot leave an earlier insert committed.
    pub(super) async fn exec_window_batch(
        &self,
        input: WindowBatch<'_>,
    ) -> ConnectorResult<(Option<u64>, u64)> {
        let WindowBatch {
            pipe,
            state,
            window,
            unseen,
            update,
            options,
            upsert,
            add_columns,
        } = input;
        let projected;
        let write_state = if let Some(add) = &add_columns {
            projected = add.projected_state(pipe, state);
            &projected
        } else {
            state
        };
        window
            .validate_frame(pipe.pipe(), unseen, options)
            .map_err(be)?;
        if let Some(update) = update {
            window
                .validate_frame(pipe.pipe(), update, options)
                .map_err(be)?;
        }
        if options.replace && update.is_some_and(|frame| frame.height() > 0) {
            return Err(be("Replacement cannot adopt a separate update plan."));
        }
        let update = match update.filter(|frame| frame.height() > 0) {
            Some(frame) => Some(
                self.prepare_window_update(pipe, write_state, frame, window, upsert)
                    .await?,
            ),
            None => None,
        };
        let dtype = self.get_to_sql_dtype(pipe, unseen, write_state, true);
        let mut statements = super::snapshot::insert_statements_in(
            &pipe.target,
            Some("main"),
            unseen,
            &dtype,
            &null_marker_columns(pipe, state),
        )?;
        let deletion = options.replace && state.exists;
        if deletion {
            statements.insert(
                0,
                (
                    self.replace_delete_query(
                        pipe,
                        state,
                        &meerschaum_connector::DataQuery {
                            begin: options.begin.clone(),
                            end: options.end.clone(),
                            params: options.params.clone(),
                            ..Default::default()
                        },
                    )?,
                    Vec::new(),
                ),
            );
        }
        let first_update = statements.len();
        let mut guard = if let Some(update) = &update {
            let update_statements = update
                .evidence
                .queries()
                .iter()
                .map(|sql| (sql.clone(), Vec::new()))
                .collect::<Vec<_>>();
            let guard = self
                .window_guard(
                    pipe,
                    state,
                    window.clone(),
                    Some(&update.evidence),
                    &update_statements,
                    &SyncOptions::default(),
                )
                .await?;
            statements.extend(update_statements);
            guard
        } else {
            self.window_guard(pipe, state, window.clone(), None, &statements, options)
                .await?
        };
        guard.expected_sql = statements.iter().map(|(sql, _)| sql.clone()).collect();
        let (count_all, input_count) = update
            .as_ref()
            .map_or((false, 0), |update| (update.count_all, update.input_count));
        guard.stage = update.map(|update| update.stage);
        guard.effect_options = options.clone();
        guard.add_columns = add_columns;
        let (results, receipt) = self
            .driver()
            .exec_many_guarded(
                statements,
                Arc::new(guard),
                std::time::Duration::from_secs(30),
            )
            .await
            .map_err(be)?;
        if let Some(receipt) = receipt {
            receipt.acknowledge().await.map_err(be)?;
        }
        let written = if count_all {
            results.iter().skip(first_update).flatten().sum()
        } else {
            input_count
        };
        Ok((
            if deletion {
                results.first().copied().flatten()
            } else {
                None
            },
            written,
        ))
    }
}

#[async_trait::async_trait]
impl TargetGuard for WindowGuard {
    fn data_effect(
        &self,
        counts: &[Option<u64>],
    ) -> Result<meerschaum_connector::mutation::NativeDataEffect, crate::SqlError> {
        let counts = counts
            .iter()
            .copied()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| error("The native target returned an unknown affected-row count."))?;
        let effect = meerschaum_connector::mutation::NativeDataEffect::from_native(
            &self.effect_pipe,
            Value::Int(self.registration),
            &self.window,
            &self.effect_options,
            counts,
        )
        .map_err(|reason| error(&reason))?;
        Ok(match &self.add_columns {
            Some(add) => effect.with_schema(add.effect(&self.schema)?),
            None => effect,
        })
    }
    async fn commit_recheck(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.verify_definition(
            transaction,
            writer,
            self.committed_parameters.as_ref().unwrap_or(&self.original),
        )
        .await
    }
    fn statements(&self, statements: &[(String, Vec<SqlParam>)]) -> Result<(), crate::SqlError> {
        if !statements
            .iter()
            .map(|(sql, _)| sql)
            .eq(self.expected_sql.iter())
        {
            return Err(error("The exact native target statements were replaced."));
        }
        crate::driver::write_guard::bounded_inputs(
            statements
                .iter()
                .chain(self.add_columns.iter().flat_map(|add| &add.statements))
                .chain(
                    self.stage.iter().flat_map(|stage| {
                        stage.setup.iter().chain(std::iter::once(&stage.cleanup))
                    }),
                ),
        )
    }
    fn binding(&self) -> &TargetBinding {
        &self.schema.binding
    }
    async fn prepare(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.schema.verify(transaction, writer).await?;
        if let Some(add) = &self.add_columns {
            self.verify_registration(transaction, &self.original)
                .await?;
            add.apply(transaction, writer, &self.schema).await?;
        }
        if let Some(stage) = &self.stage {
            for (sql, params) in &stage.setup {
                writer.execute_native(transaction, sql, params).await?;
            }
        }
        Ok(())
    }
    async fn after(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.validate(transaction, writer).await?;
        if let Some(stage) = &self.stage {
            writer
                .execute_native(transaction, &stage.cleanup.0, &stage.cleanup.1)
                .await?;
        }
        Ok(())
    }
    async fn validate(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.verify_original(transaction, writer).await?;
        // The existing Source/ACL/JWT guard runs after BEGIN and before each actual
        // statement too. It must confirm the SAME window proof; no replacement adoption.
        if let Some(query) = &self.patch_axis_refusal
            && transaction.raw_query(query, &[]).await?.num_rows() != 0
        {
            return Err(error(
                "The native staged assignment contains an outside/null axis. No target DML was admitted.",
            ));
        }
        if let Some(query) = &self.old_rows
            && transaction.raw_query(query, &[]).await?.num_rows() != 0
        {
            return Err(error(
                "A native matching-key OLD row lies outside the Main-issued window; this target transaction was refused.",
            ));
        }
        Ok(())
    }
}

impl WindowGuard {
    pub(super) fn native_inference(
        &mut self,
        change: &meerschaum_connector::mutation::MetadataInference,
    ) -> Result<(), String> {
        change.validate()?;
        if change.pipe.pipe_ref() != self.effect_pipe.pipe_ref()
            || change.registration != Value::Int(self.registration)
            || change.before != self.original
        {
            return Err("The native snapshot inference lost its exact held definition.".into());
        }
        self.committed_parameters = Some(change.after.clone());
        Ok(())
    }
    async fn verify_original(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
    ) -> Result<(), crate::SqlError> {
        self.verify_definition(transaction, writer, &self.original)
            .await
    }
    async fn verify_definition(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        writer: &HeldWriter,
        expected: &Map,
    ) -> Result<(), crate::SqlError> {
        let schema = match &self.add_columns {
            Some(add) => add.after()?,
            None => &self.schema,
        };
        schema.verify(transaction, writer).await?;
        self.verify_registration(transaction, expected).await
    }
    async fn verify_registration(
        &self,
        transaction: &mut crate::driver::SqlTransaction,
        expected: &Map,
    ) -> Result<(), crate::SqlError> {
        let check = meerschaum_connector::mutation::current()
            .ok_or_else(|| error("The original native window authority was lost."))?;
        if meerschaum_connector::mutation::current_window(&check)
            .await
            .map_err(|reason| error(&reason))?
            .as_ref()
            != Some(&self.window)
        {
            return Err(error(
                "The original Main window changed before the native target write.",
            ));
        }
        let batch=transaction.raw_query(&format!("SELECT CASE WHEN length(CAST(parameters AS BLOB))<=4194304 THEN parameters ELSE NULL END AS parameters FROM {} WHERE pipe_id=? LIMIT 2",self.metadata_table),&[SqlParam::Int(self.registration)]).await?;
        let frame = Frame::from_record_batches(&[batch])?;
        let docs = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
        if docs.len() != 1 || &decode_definition(docs[0].get("parameters"))? != expected {
            return Err(error(
                "The native window registration/old definition changed after the lock wait.",
            ));
        }
        Ok(())
    }
}
