//! One atomic SQLite replacement from an already completed, bounded source snapshot.
//!
//! Planning only reads metadata and bounded frames. Applying retains one captured writer
//! transaction across schema/metadata changes, window deletion and every row batch.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_connector::source_replace::SourceReplaceSummary;
use meerschaum_connector::{ConnectorError, ConnectorResult, DateBound, SyncOptions, async_trait};
use meerschaum_core::{Map, Pipe, SuccessTuple, SyncResult, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_kernel::{Column, Frame};

use super::super::data::PipeState;
use super::super::pipes::{bump_metadata_epoch, parse_parameters};
use super::super::values::{ColumnEncoding, encode_param, numeric_precision_scale};
use super::{
    AlterColumnsInput, SqlConnector, SqlParam, SqlPipe, be, current_timestamp,
    get_db_type_from_pd_type, get_numeric_cols, get_special_cols, max_bind_params, sql_item_name,
    typed_placeholder,
};
use crate::driver::write_guard::TargetGuard;
use crate::flavor::Flavor;
use crate::queries::index::{IndexState, get_create_index_queries};
use crate::queries::table::{CreateTableOptions, QueryOrDtypes, get_create_table_queries};
use meerschaum_connector::mutation::DataWindow;

/// A main-owned frozen source. Implementations authorize the original actor and canonical
/// source before each read, return one bounded Frame, and never restart the source SQL.
#[async_trait]
pub trait Frames: Send {
    /// Return the next bounded frozen frame, or actual EOF.
    async fn next(&mut self) -> ConnectorResult<Option<Frame>>;
    /// Main may identify the exact fully consumed, unprojected registered SQLite
    /// source. Ordinary callers and snapshot readers without this evidence supply none.
    fn completed_source(&self) -> Option<&str> {
        None
    }
    /// Rewind this exact private snapshot for the atomic insertion pass.
    async fn rewind(&mut self) -> ConnectorResult<()>;
}

/// Immutable native plan retained beside the private source lease. No reusable credential
/// is stored here. The commit request supplies freshly captured original-caller authority.
#[derive(Clone)]
pub struct Plan {
    /// Main-issued process-local authority. A commit request cannot replace it.
    window: Option<DataWindow>,
    registration: i64,
    original: Map,
    pipe: SqlPipe,
    state: PipeState,
    context: PipeDtypesContext,
    options: SyncOptions,
    timestamp: Option<(String, KValue)>,
    drop_primary: Option<String>,
    literal_null_markers: Vec<String>,
    signature: String,
    schema: Vec<String>,
    indices: Vec<String>,
    parameters: Map,
    dtype: IndexMap<String, String>,
    summary: SourceReplaceSummary,
}

impl Plan {
    /// Exact row/window summary, without collecting source data into a full-result Frame.
    pub fn summary(&self) -> &SourceReplaceSummary {
        &self.summary
    }

    fn normalize(&self, frame: Frame) -> ConnectorResult<(Frame, usize)> {
        let mut frame = pipe_enforce_dtypes(PipeInput::Frame(frame), &self.context).map_err(be)?;
        if let Some((name, timestamp)) = &self.timestamp
            && !frame.contains(name)
        {
            let rows = frame.height();
            frame
                .set_column(Column::object(name, vec![timestamp.clone(); rows]))
                .map_err(be)?;
            frame = pipe_enforce_dtypes(PipeInput::Frame(frame), &self.context).map_err(be)?;
        }
        // Check the entire typed frame BEFORE the ordinary requested-window
        // filter can silently discard a mixed outlier from a delegated batch.
        if let Some(window) = &self.window {
            window
                .validate_frame(self.pipe.pipe(), &frame, &self.options)
                .map_err(be)?;
        }
        let (mut frame, skipped) = meerschaum_connector::replace::rows_in_window(
            self.pipe.column("datetime").as_deref(),
            &frame,
            &self.options,
            &self.literal_null_markers,
        )
        .map_err(be)?;
        if let Some(primary) = &self.drop_primary {
            frame.drop(primary);
        }
        Ok((frame, skipped))
    }
}

impl SqlConnector {
    /// Read-only preparation. The canonical parameters are retained raw for conditional
    /// metadata persistence; resolved parameters are used only for execution planning.
    pub async fn plan_snapshot_replace(
        &self,
        canonical: &Pipe,
        resolved: &Pipe,
        frames: &mut dyn Frames,
        options: &SyncOptions,
        enforce_dtypes: bool,
    ) -> ConnectorResult<Plan> {
        if self.flavor != Flavor::Sqlite || !options.replace {
            return Err(ConnectorError::NotImplemented(
                "Atomic registered-source replacement currently requires SQLite.".into(),
            ));
        }
        if let Some(refused) = self.protected_refusal(canonical, "sync") {
            return Err(be(refused.message));
        }
        if let Some(refused) = self
            .lock_refusal(canonical, meerschaum_connector::locks::sync_op(true))
            .await
        {
            return Err(be(refused.message));
        }
        let pipe = self.sql_pipe(resolved)?;
        if self
            .get_pipe_schema(&pipe)
            .as_deref()
            .is_some_and(|schema| !schema.is_empty() && schema != "main")
        {
            return Err(ConnectorError::NotImplemented(
                "Atomic source replacement requires SQLite's main schema.".into(),
            ));
        }
        let registration = self.get_pipe_id(canonical).await?.ok_or_else(|| {
            be("A canonical source replacement requires an existing registration.")
        })?;
        if pipe.column("datetime").is_none() && (options.begin.is_some() || options.end.is_some()) {
            return Err(be(meerschaum_connector::replace::no_datetime_window(
                canonical,
            )));
        }
        self.forget_catalog_table(&pipe.target);
        self.forget_synced_columns(&super::synced_columns_key(resolved));
        let state = self.pipe_state(&pipe).await;
        let (signature, stored_indices) = table_signature(self, &pipe).await?;
        let mut context = self.dtypes_ctx(&pipe, &state);
        context.enforce = context.enforce && enforce_dtypes;
        let timestamp = if pipe.autotime() {
            let name = pipe
                .column("datetime")
                .unwrap_or_else(|| self.settings.pipe_defaults.autotime_column.clone());
            let dtype = context
                .dtypes
                .get(&name)
                .map(String::as_str)
                .unwrap_or("datetime");
            Some((
                name,
                current_timestamp(
                    pipe.precision_unit().map_err(be)?,
                    meerschaum_core::dtypes::are_dtypes_equal(dtype, "int"),
                ),
            ))
        } else {
            None
        };
        let mut plan = Plan {
            window: match meerschaum_connector::mutation::current() {
                Some(authority) => {
                    meerschaum_connector::mutation::recheck(&authority)
                        .await
                        .map_err(be)?;
                    meerschaum_connector::mutation::current_window(&authority)
                        .await
                        .map_err(be)?
                }
                None => None,
            },
            registration,
            original: canonical.raw_parameters().clone(),
            parameters: canonical.raw_parameters().clone(),
            literal_null_markers: super::null_marker_columns(&pipe, &state),
            pipe,
            state,
            context,
            options: options.clone(),
            timestamp,
            drop_primary: None,
            signature,
            schema: Vec::new(),
            indices: Vec::new(),
            dtype: IndexMap::new(),
            summary: SourceReplaceSummary {
                begin: options.begin.clone(),
                end: options.end.clone(),
                first: None,
                last: None,
                rows: 0,
                skipped: 0,
            },
        };
        if let Some(window) = &plan.window {
            window.validate_replacement(&plan.options).map_err(be)?;
            if !plan.state.exists {
                return Err(be(
                    "This delegated replacement requires a Main-prepared native target.",
                ));
            }
            self.preflight_window_schema(&plan.pipe, &plan.state, window)
                .await?;
        }
        let primary = plan.pipe.column("primary");
        let mut primary_present = false;
        let mut incoming = IndexMap::<String, String>::new();
        let mut special = IndexMap::<String, String>::new();
        let mut numeric = BTreeSet::<String>::new();
        frames.rewind().await?;
        while let Some(frame) = frames.next().await? {
            let (frame, skipped) = plan.normalize(frame)?;
            if let Some(window) = &plan.window {
                self.preflight_window_frame(plan.pipe.pipe(), &frame, &plan.options, window)
                    .await?;
            }
            plan.summary.rows = plan
                .summary
                .rows
                .checked_add(frame.height() as u64)
                .ok_or_else(|| be("Source replacement row count overflowed."))?;
            plan.summary.skipped = plan
                .summary
                .skipped
                .checked_add(skipped as u64)
                .ok_or_else(|| be("Source replacement row count overflowed."))?;
            if let Some(primary) = &primary {
                primary_present |= frame
                    .data(primary)
                    .is_ok_and(|data| data.is_null_mask().iter().any(|is_null| !is_null));
            }
            for (column, dtype) in self.df_cols_types_refined(&frame) {
                if let Some(previous) = incoming.get(&column) {
                    if !meerschaum_core::dtypes::are_dtypes_equal(previous, &dtype) {
                        return Err(be(
                            "The frozen source changed column types between pages; declare canonical SQL casts.",
                        ));
                    }
                } else {
                    incoming.insert(column, dtype);
                }
            }
            special.extend(get_special_cols(&frame));
            numeric.extend(get_numeric_cols(&frame));
            widen_summary(
                &mut plan.summary,
                &frame,
                plan.pipe.column("datetime").as_deref(),
            )?;
        }
        if incoming.len() > 1024 {
            return Err(be("The source replacement has too many columns."));
        }
        let autoincrement = plan.pipe.autoincrement()
            || (!plan.state.exists
                && primary
                    .as_ref()
                    .is_some_and(|column| !incoming.contains_key(column)));
        if autoincrement {
            plan.parameters.insert("autoincrement".into(), true.into());
            if !primary_present {
                plan.drop_primary = primary.clone();
                if let Some(primary) = &primary {
                    incoming.shift_remove(primary);
                }
            }
        }
        let mut execution = plan.pipe.pipe().clone();
        let mut execution_parameters = execution.raw_parameters().clone();
        if autoincrement {
            execution_parameters.insert("autoincrement".into(), true.into());
        }
        let mut explicit = canonical.dtypes();
        for (column, dtype) in &special {
            explicit.insert(column.clone(), dtype.clone().into());
        }
        if !special.is_empty() {
            plan.parameters
                .insert("dtypes".into(), Value::Map(explicit));
        }
        execution_parameters.extend(
            plan.parameters
                .iter()
                .filter(|(key, _)| matches!(key.as_str(), "autoincrement" | "dtypes"))
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        execution.set_parameters(execution_parameters);
        plan.pipe = self.sql_pipe(&execution)?;
        if plan.state.exists {
            plan.schema.extend(self.add_columns_queries_for(
                &plan.pipe,
                &incoming,
                &plan.state.cols_types,
            ));
            // Ordinary sync refreshes its schema after ADD COLUMN before planning a
            // SQLite rebuild. Include the same newly added columns here, or a later
            // rebuild would silently discard them before the first bounded INSERT.
            let mut after_add = plan.state.cols_types.clone();
            let declared = plan.pipe.dtypes();
            for (column, dtype) in &incoming {
                if !after_add.contains_key(column)
                    && (!plan.pipe.is_static() || declared.contains_key(column))
                {
                    after_add.insert(
                        column.clone(),
                        super::history_db_type(&plan.pipe, self.flavor, dtype),
                    );
                }
            }
            let alter = AlterColumnsInput {
                df_cols_types: incoming.clone(),
                numeric_cols: numeric.into_iter().collect(),
                db_cols_types: after_add.clone(),
                pipe_dtypes: super::super::data::pipe_dtypes(plan.pipe.pipe(), &after_add, true),
                explicit_dtypes: super::super::data::pipe_dtypes(
                    plan.pipe.pipe(),
                    &after_add,
                    false,
                ),
                new_special_cols: special,
                session_id: uuid::Uuid::new_v4().simple().to_string(),
            };
            let (queries, dtypes) = self.alter_columns_queries_for(&plan.pipe, &alter);
            plan.schema.extend(queries);
            if let Some(dtypes) = dtypes {
                plan.parameters.insert(
                    "dtypes".into(),
                    Value::Map(
                        dtypes
                            .into_iter()
                            .map(|(key, value)| (key, value.into()))
                            .collect(),
                    ),
                );
            }
        } else if plan.summary.rows > 0 {
            let mut create_types = incoming.clone();
            for (role, column) in super::columns_map(&plan.pipe) {
                if role != "primary" {
                    create_types.entry(column).or_insert_with(|| "int".into());
                }
            }
            create_types.extend(plan.state.dtypes.clone());
            if autoincrement && let Some(primary) = &primary {
                create_types.shift_remove(primary);
            }
            let primary_type = primary.as_ref().map(|column| {
                plan.state
                    .dtypes
                    .get(column)
                    .or_else(|| incoming.get(column))
                    .cloned()
                    .unwrap_or_else(|| "int".into())
            });
            let options = CreateTableOptions {
                primary_key: primary.clone(),
                primary_key_db_type: primary_type
                    .map(|dtype| get_db_type_from_pd_type(&dtype, Some(self.flavor))),
                datetime_column: plan.pipe.column("datetime"),
                ..Default::default()
            };
            plan.schema.extend(
                get_create_table_queries(
                    &QueryOrDtypes::Dtypes(create_types),
                    &plan.pipe.target,
                    self.flavor,
                    &options,
                )
                .map_err(be)?,
            );
        }
        for (column, dtype) in &incoming {
            plan.dtype.insert(
                column.clone(),
                get_db_type_from_pd_type(dtype, Some(self.flavor)),
            );
        }
        for (column, dtype) in &plan.state.dtypes {
            plan.dtype.insert(
                column.clone(),
                get_db_type_from_pd_type(dtype, Some(self.flavor)),
            );
        }
        if plan.state.exists || plan.summary.rows > 0 {
            let mut types = plan.state.cols_types.clone();
            types.extend(plan.dtype.clone());
            let index_state = IndexState {
                existing_cols_types: types,
                existing_cols_indices: self
                    .get_table_cols_indices(
                        &plan.pipe.target,
                        self.get_pipe_schema(&plan.pipe).as_deref(),
                    )
                    .await,
                ..Default::default()
            };
            plan.indices = get_create_index_queries(&self.ctx(), &plan.pipe, &index_state)
                .map_err(be)?
                .into_values()
                .flatten()
                .collect();
        }
        let locks = meerschaum_core::access::Locks::from_parameters(canonical.raw_parameters());
        if meerschaum_core::access::lock_refusal(
            canonical,
            &locks,
            meerschaum_core::access::LockOp::EditParameters,
        )
        .is_some()
        {
            plan.parameters = plan.original.clone();
        }
        frames.rewind().await?;
        if plan.window.is_some() {
            // The generic SQLite index builder emits its datetime index even when
            // present. Only identical DDL in the sealed, transaction-rechecked
            // schema is a no-op. A missing or changed index still needs admission.
            plan.indices.retain(|query| {
                !stored_indices
                    .iter()
                    .any(|ddl| super::window_guard::same_index_ddl(query, ddl))
            });
        }
        if plan.window.is_some() && (!plan.schema.is_empty() || !plan.indices.is_empty()) {
            return Err(be(
                "This delegated replacement needs a controlled Main schema/index transition; no target changes were admitted.",
            ));
        }
        Ok(plan)
    }

    /// Apply one prepared plan under a main-supplied original-caller mutation scope.
    /// Database errors return through rollback. A post-commit operation-receipt refusal is
    /// reported with the truthful committed row counts, without claiming data was rolled back.
    pub async fn apply_snapshot_replace(
        &self,
        plan: &Plan,
        frames: &mut dyn Frames,
        cancelled: Arc<AtomicBool>,
        budget: Duration,
    ) -> ConnectorResult<SyncResult> {
        if self.flavor != Flavor::Sqlite
            || meerschaum_connector::mutation::current().is_none()
            || budget.is_zero()
            || budget > Duration::from_secs(300)
        {
            return Err(be(
                "Atomic source replacement requires captured SQLite mutation authority and a bounded budget.",
            ));
        }
        if let Some(refused) = self.protected_refusal(plan.pipe.pipe(), "sync") {
            return Err(be(refused.message));
        }
        let authority = meerschaum_connector::mutation::current()
            .ok_or_else(|| be("Atomic replacement lost its original mutation authority."))?;
        if meerschaum_connector::mutation::current_window(&authority)
            .await
            .map_err(be)?
            != plan.window
        {
            return Err(be(
                "The original Main replacement window changed after preparation.",
            ));
        }
        let mut window_guard = match &plan.window {
            Some(window) => Some(
                self.snapshot_window_guard(
                    &plan.pipe,
                    &plan.state,
                    window.clone(),
                    &plan.options,
                    plan.registration,
                    &plan.original,
                )
                .await?,
            ),
            None => None,
        };
        if let Some(guard) = &mut window_guard
            && plan.parameters != plan.original
        {
            guard
                .native_inference(&meerschaum_connector::mutation::MetadataInference {
                    pipe: plan.pipe.pipe().clone(),
                    registration: Value::Int(plan.registration),
                    before: plan.original.clone(),
                    after: plan.parameters.clone(),
                })
                .map_err(be)?;
        }
        let deadline = Instant::now() + budget;
        let mut target_counts = None;
        let mut prepared = None;
        let native_batch = authority.native_batch();
        if native_batch.is_some() && window_guard.is_none() {
            return Err(be("The batched replacement lost its native window."));
        }
        let mut transaction = self.driver().begin_write().await.map_err(be)?;
        if let Some(batch) = &native_batch
            && let Err(reason) = batch.begin_target()
        {
            transaction.rollback().await.map_err(be)?;
            return Err(be(reason));
        }
        let authorization_scope = transaction.authorization_read_scope();
        let work = async {
            transaction
                .set_sqlite_read_budget(deadline, cancelled.clone())
                .await
                .map_err(be)?;
            if let Some(guard) = &window_guard {
                let writer = self
                    .driver()
                    .held_window_writer(&mut transaction, guard.binding())
                    .await
                    .map_err(be)?;
                guard
                    .validate(&mut transaction, &writer)
                    .await
                    .map_err(be)?;
            }
            let signature = transaction
                .read_frame(&signature_query(&plan.pipe.target))
                .await
                .map_err(be)?;
            if signature_text(&signature)? != plan.signature {
                return Err(be(
                    "The destination schema changed after replacement preparation.",
                ));
            }
            let table = self.instance_table_name("mrsm_pipes");
            let attributes = transaction
                .read_frame(&format!(
                    "SELECT pipe_id, parameters FROM {table} WHERE pipe_id = {}",
                    plan.registration
                ))
                .await
                .map_err(be)?;
            let docs = super::super::frame_docs(&attributes);
            let Some(row) = docs.first() else {
                return Err(be("The destination registration disappeared."));
            };
            if parse_parameters(row.get("parameters")) != plan.original {
                return Err(be(
                    "The destination parameters changed after replacement preparation.",
                ));
            }
            if cancelled.load(Ordering::Relaxed) {
                return Err(be("Source replacement was cancelled."));
            }
            for query in &plan.schema {
                transaction.exec(query).await.map_err(be)?;
            }
            // Newly added/retyped columns participate in params filtering exactly as in
            // ordinary replacement. Read their actual uncommitted schema on this writer;
            // using the pre-DDL column map could silently drop a new filter from DELETE.
            let columns = transaction
                .read_frame(&format!(
                    "PRAGMA main.table_info('{}')",
                    plan.pipe.target.replace('\'', "''")
                ))
                .await
                .map_err(be)?;
            let mut after = plan.state.clone();
            if let (Some(names), Some(types)) = (columns.column("name"), columns.column("type")) {
                after.cols_types = (0..columns.height())
                    .map(|row| (names.data.get(row).py_str(), types.data.get(row).py_str()))
                    .collect();
                after.dtypes =
                    super::super::data::pipe_dtypes(plan.pipe.pipe(), &after.cols_types, true);
                after.explicit_dtypes =
                    super::super::data::pipe_dtypes(plan.pipe.pipe(), &after.cols_types, false);
            }
            let deleted = if plan.state.exists {
                let query = meerschaum_connector::DataQuery {
                    begin: plan.options.begin.clone(),
                    end: plan.options.end.clone(),
                    params: plan.options.params.clone(),
                    ..Default::default()
                };
                transaction
                    .exec(&self.replace_delete_query(&plan.pipe, &after, &query)?)
                    .await
                    .map_err(be)?
            } else {
                0
            };
            let mut inserted = 0u64;
            let mut skipped = 0u64;
            frames.rewind().await?;
            while let Some(frame) = frames.next().await? {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(be("Source replacement was cancelled."));
                }
                let (frame, outside) = plan.normalize(frame)?;
                skipped += outside as u64;
                for (query, parameters) in insert_statements(
                    &plan.pipe.target,
                    &frame,
                    &plan.dtype,
                    &plan.literal_null_markers,
                )? {
                    if meerschaum_connector::mutation::current_window(&authority)
                        .await
                        .map_err(be)?
                        != plan.window
                    {
                        return Err(be(
                            "The original delegated window changed before its native replacement batch.",
                        ));
                    }
                    inserted += transaction
                        .raw_execute(&query, &parameters)
                        .await
                        .map_err(be)?;
                }
            }
            if inserted != plan.summary.rows || skipped != plan.summary.skipped {
                return Err(be(
                    "The prepared source row counts changed during replacement.",
                ));
            }
            for query in &plan.indices {
                transaction.exec(query).await.map_err(be)?;
            }
            if plan.parameters != plan.original {
                // Validate the original held old-map proof BEFORE this operation's
                // own metadata change. The ordinary post-commit receipt advances
                // only the proven non-axis fields; it never widens a window.
                if let Some(guard) = &window_guard {
                    let writer = self
                        .driver()
                        .held_window_writer(&mut transaction, guard.binding())
                        .await
                        .map_err(be)?;
                    guard
                        .validate(&mut transaction, &writer)
                        .await
                        .map_err(be)?;
                }
                transaction
                    .raw_execute(
                        &format!("UPDATE {table} SET parameters = ? WHERE pipe_id = ?"),
                        &[
                            self.json_param(&plan.parameters),
                            SqlParam::Int(plan.registration),
                        ],
                    )
                    .await
                    .map_err(be)?;
            }
            if let Some(guard) = &window_guard {
                self.driver()
                    .held_window_writer(&mut transaction, guard.binding())
                    .await
                    .map_err(be)?;
                if meerschaum_connector::mutation::current_window(&authority)
                    .await
                    .map_err(be)?
                    != plan.window
                {
                    return Err(be("The Main window changed before replacement commit."));
                }
            }
            if let (Some(snapshot_sha256), Some(window)) = (frames.completed_source(), &plan.window)
                && native_batch.is_some()
                && skipped == 0
                && meerschaum_connector::mutation::commit::PreparedReplacement::covers(
                    window,
                    &plan.options,
                )
            {
                let column = crate::ident::sql_item_name(
                    window.column().expect("integer axis"),
                    Flavor::Sqlite,
                    None,
                );
                let table = self.pipe_table_name(&plan.pipe);
                let watermark = transaction.read_frame(&format!(
                    "SELECT typeof(MAX({column})) AS kind, CAST(MAX({column}) AS TEXT) AS watermark FROM {table}"
                )).await.map_err(be)?;
                let kind = watermark
                    .column("kind")
                    .filter(|_| watermark.height() == 1)
                    .ok_or_else(|| be("The held native target did not return one watermark."))?
                    .data
                    .get(0);
                let value = watermark
                    .column("watermark")
                    .ok_or_else(|| be("The native target lost its watermark."))?
                    .data
                    .get(0);
                let watermark = match (kind, value) {
                    (KValue::Str(kind), _) if kind == "null" => Some(None),
                    (KValue::Str(kind), KValue::Str(value)) if kind == "integer" => {
                        value.parse::<i64>().ok().map(Some)
                    }
                    _ => None,
                };
                prepared = watermark.map(|watermark| {
                    meerschaum_connector::mutation::commit::PreparedReplacement {
                        snapshot_sha256: snapshot_sha256.to_owned(),
                        rows: inserted,
                        watermark,
                    }
                });
            }
            target_counts = Some(vec![Some(deleted), Some(inserted)]);
            Ok(meerschaum_connector::replace::replace_result(
                deleted,
                inserted,
                skipped as usize,
                true,
            ))
        };
        let result = if let Some(scope) = &authorization_scope {
            scope.scope(work).await
        } else {
            work.await
        };
        let cleared = transaction.clear_sqlite_read_budget().await;
        if result.is_err() || cleared.is_err() {
            let rollback = transaction.rollback_target(native_batch.as_ref()).await;
            self.forget_catalog_table(&plan.pipe.target);
            self.forget_synced_columns(&super::synced_columns_key(plan.pipe.pipe()));
            if cleared.is_err() {
                self.driver().close().await;
            }
            rollback.map_err(be)?;
            return result.and_then(|_| Err(be("Could not clear the replacement VM budget.")));
        }
        let receipt = if let Some(guard) = &window_guard {
            let proof = async {
                let writer = self
                    .driver()
                    .held_window_writer(&mut transaction, guard.binding())
                    .await
                    .map_err(be)?;
                let counts = target_counts
                    .as_deref()
                    .ok_or_else(|| be("The native snapshot lost its actual target counts."))?;
                let effect = guard.data_effect(counts).map_err(be)?;
                let effect = match prepared {
                    Some(prepared) => effect.with_prepared_replacement(prepared).map_err(be)?,
                    None => effect,
                };
                Ok::<_, meerschaum_connector::ConnectorError>((writer, effect))
            }
            .await;
            let (writer, effect) = match proof {
                Ok(proof) => proof,
                Err(error) => {
                    transaction
                        .rollback_target(native_batch.as_ref())
                        .await
                        .map_err(be)?;
                    return Err(error);
                }
            };
            self.driver()
                .commit_windowed(
                    transaction,
                    guard,
                    &writer,
                    effect,
                    deadline,
                    cancelled.clone(),
                )
                .await
                .map_err(be)?
        } else {
            transaction.commit().await.map_err(be)?;
            None
        };
        if plan.parameters != plan.original {
            let _ = bump_metadata_epoch(SuccessTuple::success());
        }
        self.forget_catalog_table(&plan.pipe.target);
        self.forget_synced_columns(&super::synced_columns_key(plan.pipe.pipe()));
        let mut result = result?;
        if plan.parameters != plan.original && authority.tracks_metadata_inference() {
            let change = meerschaum_connector::mutation::MetadataInference {
                pipe: plan.pipe.pipe().clone(),
                registration: Value::Int(plan.registration),
                before: plan.original.clone(),
                after: plan.parameters.clone(),
            };
            let accepted = match change.validate() {
                Ok(()) => authority.metadata_inferred(change).await,
                Err(reason) => Err(reason),
            };
            if let Err(reason) = accepted {
                result.success = false;
                result.message = format!(
                    "{} Native replacement committed, but its operation metadata was not accepted: {}. The committed rows were not rolled back; create a fresh operation after resolving its original authorization or metadata conflict.",
                    result.message,
                    meerschaum_core::debug::redact_text(&reason)
                        .chars()
                        .take(512)
                        .collect::<String>()
                );
            }
        }
        if result.success
            && let Some(receipt) = receipt
        {
            receipt.acknowledge().await.map_err(be)?;
        }
        Ok(result)
    }
}

fn signature_query(target: &str) -> String {
    format!(
        "SELECT type, name, sql FROM main.sqlite_schema WHERE lower(tbl_name) = lower('{}') ORDER BY type, name",
        target.replace('\'', "''")
    )
}
async fn table_signature(
    connector: &SqlConnector,
    pipe: &SqlPipe,
) -> ConnectorResult<(String, Vec<String>)> {
    let frame = connector
        .driver()
        .read_frame(&signature_query(&pipe.target))
        .await
        .map_err(be)?;
    let signature = signature_text(&frame)?;
    let kind = frame
        .column("type")
        .ok_or_else(|| be("The destination schema proof is unavailable."))?;
    let sql = frame
        .column("sql")
        .ok_or_else(|| be("The destination schema proof is unavailable."))?;
    let stored_indices = (0..frame.height())
        .filter_map(|row| match (kind.data.get(row), sql.data.get(row)) {
            (KValue::Str(kind), KValue::Str(sql)) if kind == "index" => Some(sql),
            _ => None,
        })
        .collect();
    Ok((signature, stored_indices))
}
fn signature_text(frame: &Frame) -> ConnectorResult<String> {
    if frame.height() > 4096 {
        return Err(be("The destination schema exceeds its metadata bound."));
    }
    let columns = ["type", "name", "sql"].map(|name| frame.column(name));
    if columns.iter().any(Option::is_none) {
        return Err(be("The destination schema proof is unavailable."));
    }
    let mut proof = Vec::with_capacity(frame.height());
    let mut bytes = 0usize;
    for row in 0..frame.height() {
        let mut values = Vec::with_capacity(3);
        for column in &columns {
            let value = column.expect("checked schema column").data.get(row);
            let text = if value.is_na() {
                None
            } else {
                Some(value.py_str())
            };
            bytes = bytes.saturating_add(text.as_ref().map_or(0, String::len));
            if bytes > 4 * 1024 * 1024 {
                return Err(be("The destination schema exceeds its byte bound."));
            }
            values.push(text);
        }
        proof.push(values);
    }
    // This is bounded metadata containing nullable strings, never full source records.
    let text = serde_json::to_string(&proof).map_err(be)?;
    if text.len() > 4 * 1024 * 1024 {
        return Err(be("The destination schema exceeds its byte bound."));
    }
    Ok(text)
}

fn widen_summary(
    summary: &mut SourceReplaceSummary,
    frame: &Frame,
    column: Option<&str>,
) -> ConnectorResult<()> {
    let Some(data) = column.and_then(|name| frame.column(name)) else {
        return Ok(());
    };
    for index in 0..frame.height() {
        let value = match data.data.get(index) {
            KValue::Int(value) => Some(DateBound::Int(value)),
            KValue::Float(value) if value.is_finite() && value.fract() == 0.0 => {
                Some(DateBound::Int(value as i64))
            }
            KValue::Datetime(value) => Some(DateBound::Datetime(value)),
            KValue::Date(value) => value.and_hms_opt(0, 0, 0).map(|value| {
                DateBound::Datetime(meerschaum_kernel::value::DateTimeValue::naive(value))
            }),
            KValue::Str(value) => DateBound::parse(&value),
            _ => None,
        };
        let Some(value) = value else {
            continue;
        };
        let compare = |other: &DateBound| {
            meerschaum_connector::pipe_ops::compare_values(&value.to_value(), &other.to_value())
        };
        if summary
            .first
            .as_ref()
            .is_none_or(|first| compare(first).is_lt())
        {
            summary.first = Some(value.clone());
        }
        if summary
            .last
            .as_ref()
            .is_none_or(|last| compare(last).is_gt())
        {
            summary.last = Some(value);
        }
    }
    Ok(())
}

/// The ordinary SQLite bound-parameter encoder, separated from transaction ownership.
/// Each input Frame is already byte/row bounded by the main-owned snapshot reader.
pub(super) fn insert_statements(
    target: &str,
    frame: &Frame,
    dtype: &IndexMap<String, String>,
    literal: &[String],
) -> ConnectorResult<Vec<(String, Vec<SqlParam>)>> {
    insert_statements_in(target, None, frame, dtype, literal)
}

pub(super) fn insert_statements_in(
    target: &str,
    schema: Option<&str>,
    frame: &Frame,
    dtype: &IndexMap<String, String>,
    literal: &[String],
) -> ConnectorResult<Vec<(String, Vec<SqlParam>)>> {
    if frame.height() == 0 || frame.width() == 0 {
        return Ok(Vec::new());
    }
    let columns = frame.names();
    let numeric = get_numeric_cols(frame);
    let encodings = columns
        .iter()
        .map(|name| {
            let typ = dtype.get(name).cloned().unwrap_or_default();
            let decimal = if typ.to_lowercase().contains("numeric") {
                Some(
                    numeric_precision_scale(&typ)
                        .map(|(p, s)| (Some(p), Some(s)))
                        .unwrap_or((None, None)),
                )
            } else if numeric.contains(name) {
                Some(crate::dtypes::get_numeric_precision_scale(
                    Some(Flavor::Sqlite),
                    None,
                ))
            } else {
                None
            };
            ColumnEncoding {
                name: name.clone(),
                numeric: decimal,
                keep_null_markers: literal.contains(name),
            }
        })
        .collect::<Vec<_>>();
    let table = sql_item_name(target, Flavor::Sqlite, schema);
    let names = columns
        .iter()
        .map(|name| sql_item_name(name, Flavor::Sqlite, None))
        .collect::<Vec<_>>()
        .join(", ");
    let rows_per_statement = (max_bind_params(Flavor::Sqlite) / columns.len()).clamp(1, 1000);
    let mut statements = Vec::new();
    for start in (0..frame.height()).step_by(rows_per_statement) {
        let mut parameters = Vec::new();
        let mut groups = Vec::new();
        for row in start..(start + rows_per_statement).min(frame.height()) {
            let mut cells = Vec::new();
            for (index, encoding) in encodings.iter().enumerate() {
                let parameter = encode_param(
                    &frame.columns()[index].data.value_ref(row),
                    Flavor::Sqlite,
                    encoding,
                );
                cells.push(typed_placeholder(
                    Flavor::Sqlite,
                    parameters.len() + 1,
                    &parameter,
                    dtype.get(&columns[index]).map(String::as_str).unwrap_or(""),
                ));
                parameters.push(parameter);
            }
            groups.push(format!("({})", cells.join(", ")));
        }
        statements.push((
            format!("INSERT INTO {table} ({names}) VALUES {}", groups.join(", ")),
            parameters,
        ));
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Authority(AtomicBool);
    #[async_trait]
    impl meerschaum_connector::mutation::MutationCheck for Authority {
        async fn check(&self) -> Result<(), String> {
            if self.0.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err("original caller was revoked".into())
            }
        }
    }
    struct Pages {
        frames: Vec<Frame>,
        next: usize,
        authority: Arc<Authority>,
        revoke: Option<usize>,
    }
    #[async_trait]
    impl Frames for Pages {
        async fn next(&mut self) -> ConnectorResult<Option<Frame>> {
            if self.revoke == Some(self.next) {
                self.authority.0.store(false, Ordering::SeqCst);
                self.revoke = None;
            }
            let result = self.frames.get(self.next).cloned();
            self.next += 1;
            Ok(result)
        }
        async fn rewind(&mut self) -> ConnectorResult<()> {
            self.next = 0;
            Ok(())
        }
    }
    fn map(value: serde_json::Value) -> Map {
        Value::from_json_str(&value.to_string()).unwrap().to_map()
    }
    fn frame(rows: &[(i64, i64)], extra: bool) -> Frame {
        let docs = rows
            .iter()
            .map(|(id, value)| {
                let mut row = IndexMap::from_iter([
                    ("id".into(), KValue::Int(*id)),
                    ("value".into(), KValue::Int(*value)),
                ]);
                if extra {
                    row.insert(
                        "extra".into(),
                        KValue::Json(meerschaum_kernel::value::JsonValue::new(
                            json!({"kind":"new"}),
                        )),
                    );
                }
                row
            })
            .collect::<Vec<_>>();
        meerschaum_kernel::docs::frame_from_docs(&docs).unwrap()
    }
    async fn fixture() -> (tempfile::TempDir, SqlConnector, Pipe) {
        let root = tempfile::tempdir().unwrap();
        let connector = SqlConnector::from_uri(
            "target",
            &format!("sqlite:///{}", root.path().join("target.db").display()),
        )
        .await
        .unwrap();
        let pipe = Pipe::builder("sql:source", "snapshot_atomic").instance("sql:target")
            .parameters(map(json!({"columns":{"datetime":"id","id":"id"},"dtypes":{"id":"int","value":"int"},"upsert":true}))).build().unwrap();
        assert!(connector.register_pipe(&pipe).await.success);
        let result = connector
            .sync_pipe(
                &pipe,
                frame(&[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)], false),
                &SyncOptions::default(),
            )
            .await;
        assert!(result.success, "{}", result.message);
        (root, connector, pipe)
    }

    #[tokio::test]
    async fn late_revocation_rolls_back_window_rows_new_schema_and_metadata_then_fresh_write_reuses_pool()
     {
        let (_root, connector, pipe) = fixture().await;
        let original = connector.get_pipe_parameters(&pipe).await.unwrap().unwrap();
        let authority = Arc::new(Authority(AtomicBool::new(true)));
        let mut pages = Pages {
            frames: vec![frame(&[(1, 10), (2, 20)], true), frame(&[(3, 30)], true)],
            next: 0,
            authority: authority.clone(),
            revoke: None,
        };
        let options = SyncOptions {
            replace: true,
            begin: Some(DateBound::Int(1)),
            end: Some(DateBound::Int(4)),
            ..Default::default()
        };
        let plan = connector
            .plan_snapshot_replace(&pipe, &pipe, &mut pages, &options, true)
            .await
            .unwrap();
        assert_eq!(plan.summary().rows, 3);
        pages.revoke = Some(1);
        let result = meerschaum_connector::mutation::scope(
            authority.clone(),
            connector.apply_snapshot_replace(
                &plan,
                &mut pages,
                Arc::new(AtomicBool::new(false)),
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            result.is_err(),
            "revocation after the first inserted page must abort the whole replacement"
        );
        assert!(
            result
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("original caller was revoked"),
            "{result:?}"
        );
        let rows = connector
            .get_pipe_data(&pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rows.height(), 5);
        assert!(
            !rows.contains("extra"),
            "new physical column rolls back with row changes"
        );
        assert_eq!(
            connector.get_pipe_parameters(&pipe).await.unwrap().unwrap(),
            original
        );
        let sum = connector
            .driver()
            .read_frame(&format!(
                "SELECT sum(value) AS total FROM {}",
                connector.pipe_table_name(&connector.sql_pipe(&pipe).unwrap())
            ))
            .await
            .unwrap();
        assert_eq!(
            sum.column("total").unwrap().data.get(0).py_str(),
            "10",
            "no inserted/deleted partial window survives"
        );
        authority.0.store(true, Ordering::SeqCst);
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.apply_snapshot_replace(
                &plan,
                &mut pages,
                Arc::new(AtomicBool::new(false)),
                Duration::from_secs(5),
            ),
        )
        .await
        .unwrap();
        assert!(result.success);
        assert_eq!((result.deleted, result.inserted), (3, 3));
        let rows = connector
            .get_pipe_data(&pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rows.height(), 5);
        assert!(rows.contains("extra"));
        let parameters = connector.get_pipe_parameters(&pipe).await.unwrap().unwrap();
        assert_eq!(
            parameters
                .get("dtypes")
                .unwrap()
                .get("extra")
                .and_then(Value::as_str),
            Some("json")
        );
    }

    #[tokio::test]
    async fn newly_added_filter_column_is_not_dropped_from_atomic_window_delete() {
        let (_root, connector, mut pipe) = fixture().await;
        // No uniqueness conflict: new rows occupy IDs not present in the old target.
        let mut parameters = pipe.raw_parameters().clone();
        parameters.insert("upsert".into(), false.into());
        pipe.set_parameters(parameters);
        assert!(connector.edit_pipe(&pipe, false).await.success);
        let authority = Arc::new(Authority(AtomicBool::new(true)));
        let mut pages = Pages {
            frames: vec![frame(&[(10, 100), (11, 110)], true)],
            next: 0,
            authority: authority.clone(),
            revoke: None,
        };
        let options = SyncOptions {
            replace: true,
            begin: Some(DateBound::Int(0)),
            end: Some(DateBound::Int(20)),
            params: Some(map(json!({"extra":{"kind":"new"}}))),
            ..Default::default()
        };
        let plan = connector
            .plan_snapshot_replace(&pipe, &pipe, &mut pages, &options, true)
            .await
            .unwrap();
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.apply_snapshot_replace(
                &plan,
                &mut pages,
                Arc::new(AtomicBool::new(false)),
                Duration::from_secs(5),
            ),
        )
        .await
        .unwrap();
        assert!(result.success);
        assert_eq!(
            result.deleted, 0,
            "old rows have NULL in the newly added filter column"
        );
        assert_eq!(result.inserted, 2);
        assert_eq!(
            connector
                .get_pipe_rowcount(&pipe, &Default::default())
                .await
                .unwrap(),
            Some(7)
        );
    }
}
