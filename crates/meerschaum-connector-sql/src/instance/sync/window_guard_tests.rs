//! Actual prepared SQLite targets; no services or worker-reported effects.
use super::*;
use meerschaum_connector::mutation::{
    DataAxis, DataCoverage, DataSpan, MutationCheck, NativeDataEffect, PendingDataCommit,
};
use meerschaum_core::success::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn bounded_definition_decode_preserves_empty_map_without_accepting_missing_or_bad_metadata() {
    assert_eq!(
        decode_definition(Some(&KValue::Str("{}".into()))).unwrap(),
        Map::new()
    );
    for value in [
        KValue::Null,
        KValue::NA,
        KValue::Float(f64::NAN),
        KValue::Str("{".into()),
        KValue::Str("null".into()),
        KValue::Str("[]".into()),
        KValue::Str("0".into()),
        KValue::Str(" ".repeat(4 * 1024 * 1024 + 1)),
    ] {
        assert!(decode_definition(Some(&value)).is_err());
    }
    assert!(decode_definition(None).is_err());
}

#[test]
fn native_implicit_index_null_flavors_cannot_hide_explicit_or_oversized_schema() {
    let row = |flag: Option<i64>, bytes: KValue| {
        let mut row = IndexMap::from([
            ("type".into(), KValue::Str("index".into())),
            (
                "name".into(),
                KValue::Str("sqlite_autoindex_native_1".into()),
            ),
            ("sql".into(), KValue::Float(f64::NAN)),
            ("sql_bytes".into(), bytes),
        ]);
        if let Some(flag) = flag {
            row.insert("implicit_sql".into(), KValue::Int(flag));
        }
        meerschaum_kernel::docs::frame_from_docs(&[row]).unwrap()
    };
    for null in [KValue::Null, KValue::NA, KValue::Float(f64::NAN)] {
        let frame = row(Some(1), null);
        assert_eq!(
            decode_objects(&frame).unwrap(),
            vec![("index".into(), "sqlite_autoindex_native_1".into(), None)]
        );
    }
    for frame in [
        row(Some(0), KValue::Int((MAX_SCHEMA_SQL + 1) as i64)),
        row(Some(0), KValue::NA),
        row(None, KValue::NA),
        row(Some(2), KValue::NA),
    ] {
        assert!(
            decode_objects(&frame).is_err(),
            "An absent/truncated explicit catalog object became an implicit index"
        );
    }
}

struct Original {
    window: DataWindow,
    allowed: AtomicBool,
    prepares: AtomicUsize,
    structural: AtomicUsize,
    commits: AtomicUsize,
    acknowledgements: AtomicUsize,
    rollbacks: AtomicUsize,
}
struct Pending(Arc<Original>);
#[meerschaum_connector::async_trait]
impl PendingDataCommit for Pending {
    async fn before_commit(&self, _: &NativeDataEffect) -> Result<(), String> {
        self.0.check().await
    }
    async fn committed(&self, _: &NativeDataEffect) -> Result<(), String> {
        self.0.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn acknowledge(&self, _: &NativeDataEffect) -> Result<(), String> {
        self.0.check().await?;
        self.0.acknowledgements.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn rolled_back(&self) -> Result<(), String> {
        Ok(())
    }
}
struct Authority(Arc<Original>);
#[meerschaum_connector::async_trait]
impl MutationCheck for Original {
    async fn check(&self) -> Result<(), String> {
        self.allowed
            .load(Ordering::SeqCst)
            .then_some(())
            .ok_or_else(|| "Original source revoked".into())
    }
}
#[meerschaum_connector::async_trait]
impl MutationCheck for Authority {
    async fn check(&self) -> Result<(), String> {
        self.0.check().await
    }
    async fn write_window(&self) -> Result<Option<DataWindow>, String> {
        self.0.check().await?;
        Ok(Some(self.0.window.clone()))
    }
    async fn prepare_data_commit(
        &self,
        effect: &NativeDataEffect,
    ) -> Result<Option<Arc<dyn PendingDataCommit>>, String> {
        self.0.check().await?;
        assert_eq!(effect.window(), &self.0.window);
        assert!(effect.has_effects());
        if let Some(schema) = effect.schema() {
            assert!(matches!(schema.kind(), "create_target" | "add_columns"));
            assert_ne!(schema.before_sha256(), schema.after_sha256());
            self.0.structural.fetch_add(1, Ordering::SeqCst);
        }
        self.0.prepares.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Arc::new(Pending(self.0.clone()))))
    }
    fn rollback_completed(&self, succeeded: bool) {
        if succeeded {
            self.0.rollbacks.fetch_add(1, Ordering::SeqCst);
        }
    }
}
fn frame(rows: &[(i64, Option<i64>, i64)]) -> Frame {
    // Native JSON/document normalization uses this same convert_dtypes boundary.
    let frame = meerschaum_kernel::docs::frame_from_columns(&IndexMap::from([
        (
            "key".into(),
            rows.iter().map(|r| KValue::Int(r.0)).collect(),
        ),
        (
            "tick".into(),
            rows.iter()
                .map(|r| r.1.map(KValue::Int).unwrap_or(KValue::Null))
                .collect(),
        ),
        (
            "value".into(),
            rows.iter().map(|r| KValue::Int(r.2)).collect(),
        ),
    ]))
    .unwrap();
    assert!(frame.dtypes().values().all(|dtype| dtype != "object"));
    frame
}
async fn fixture() -> (tempfile::TempDir, SqlConnector, Pipe, Arc<Original>) {
    fixture_with_upsert(true).await
}
async fn fixture_with_upsert(
    upsert: bool,
) -> (tempfile::TempDir, SqlConnector, Pipe, Arc<Original>) {
    let root = tempfile::tempdir().unwrap();
    let connector = SqlConnector::from_uri(
        "window_fixture",
        &format!("sqlite:///{}", root.path().join("owned.db").display()),
    )
    .await
    .unwrap();
    let mut parameters=Value::from_json_str(r#"{"columns":{"datetime":"tick","primary":"key"},"dtypes":{"key":"int","tick":"int","value":"int"},"autoincrement":false}"#).unwrap().to_map();
    parameters.insert("upsert".into(), Value::Bool(upsert));
    let pipe = Pipe::builder("manual:fixture", "native_window")
        .instance("sql:window_fixture")
        .parameters(parameters)
        .build()
        .unwrap();
    assert!(connector.register_pipe(&pipe).await.success);
    let initial = connector
        .sync_pipe_result(
            &pipe,
            frame(&[(1, Some(5), 10), (2, Some(12), 20)]),
            &SyncOptions::default(),
        )
        .await;
    assert!(initial.success, "{}", initial.message);
    let window = DataWindow::new(
        pipe.pipe_ref(),
        "a".repeat(64),
        DataAxis::Integer {
            column: "tick".into(),
        },
        DataCoverage::AxisRanges,
        vec![DataSpan {
            begin: Some(Bound::Int(10)),
            end: Some(Bound::Int(20)),
        }],
        None,
    )
    .unwrap();
    let original = Arc::new(Original {
        window,
        allowed: AtomicBool::new(true),
        prepares: AtomicUsize::new(0),
        structural: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        acknowledgements: AtomicUsize::new(0),
        rollbacks: AtomicUsize::new(0),
    });
    (root, connector, pipe, original)
}
async fn rows(connector: &SqlConnector, pipe: &Pipe) -> Frame {
    connector
        .get_pipe_data(pipe, &meerschaum_connector::DataQuery::default())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn actual_guarded_insert_and_update_commit_once_and_publish_native_receipts() {
    let (_root, connector, pipe, original) = fixture().await;
    let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
    let input = frame(&[(2, Some(13), 21), (3, Some(14), 30)]);
    let supplied_types = input.dtypes();
    let result = meerschaum_connector::mutation::scope(
        authority,
        connector.sync_pipe_result(&pipe, input, &SyncOptions::default()),
    )
    .await;
    let state = connector
        .pipe_state(&connector.sql_pipe(&pipe).unwrap())
        .await;
    assert!(
        result.success,
        "{}; fixture target_exists={} db_types={:?} supplied_types={:?}",
        result.message, state.exists, state.cols_types, supplied_types
    );
    assert_eq!(rows(&connector, &pipe).await.height(), 3);
    assert_eq!(original.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(original.commits.load(Ordering::SeqCst), 1);
    assert_eq!(original.acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(
        original.rollbacks.load(Ordering::SeqCst),
        0,
        "Read-only schema probes emitted target rollback receipts"
    );
}
#[tokio::test]
async fn known_mixed_outliers_and_outside_matching_old_rows_never_leave_an_earlier_insert() {
    for (index, input) in [
        frame(&[(3, Some(14), 30), (4, Some(25), 40)]),
        frame(&[(3, Some(14), 30), (4, None, 40)]),
        frame(&[(3, Some(14), 30), (1, Some(15), 99)]),
    ]
    .into_iter()
    .enumerate()
    {
        let (_root, connector, pipe, original) = fixture().await;
        let before = meerschaum_connector::pipe_ops::frame_to_docs(&rows(&connector, &pipe).await);
        let attributes = connector.get_pipe_attributes(&pipe).await.unwrap();
        let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.sync_pipe_result(&pipe, input, &SyncOptions::default()),
        )
        .await;
        assert!(
            !result.success,
            "A mixed/outside OLD-row batch was accepted"
        );
        if index == 2 {
            assert!(
                result.message.contains("matching-key OLD row"),
                "Outside OLD-row case did not reach its exact native guard: {}",
                result.message
            );
            assert_eq!(
                original.rollbacks.load(Ordering::SeqCst),
                1,
                "The original target writer did not report its actual rollback"
            );
        }
        assert_eq!(
            meerschaum_connector::pipe_ops::frame_to_docs(&rows(&connector, &pipe).await),
            before
        );
        assert_eq!(
            connector.get_pipe_attributes(&pipe).await.unwrap(),
            attributes
        );
        assert_eq!(original.prepares.load(Ordering::SeqCst), 0);
        assert_eq!(original.commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn first_native_target_and_rows_commit_together_with_structural_receipt() {
    let (_root, connector, pipe, original) = fixture().await;
    assert!(connector.drop_pipe(&pipe).await.success);
    assert!(!connector.pipe_exists(&pipe).await.unwrap());
    let before = connector.get_pipe_attributes(&pipe).await.unwrap();
    let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
    let result = meerschaum_connector::mutation::scope(
        authority,
        connector.sync_pipe_result(&pipe, frame(&[(3, Some(14), 30)]), &SyncOptions::default()),
    )
    .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(rows(&connector, &pipe).await.height(), 1);
    assert_eq!(connector.get_pipe_attributes(&pipe).await.unwrap(), before);
    assert_eq!(original.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(original.structural.load(Ordering::SeqCst), 1);
    assert_eq!(original.commits.load(Ordering::SeqCst), 1);
    assert_eq!(original.acknowledgements.load(Ordering::SeqCst), 1);
}

struct RevokeBeforeCommit(Arc<Original>);
#[meerschaum_connector::async_trait]
impl MutationCheck for RevokeBeforeCommit {
    async fn check(&self) -> Result<(), String> {
        self.0.check().await
    }
    async fn write_window(&self) -> Result<Option<DataWindow>, String> {
        Authority(self.0.clone()).write_window().await
    }
    async fn prepare_data_commit(
        &self,
        effect: &NativeDataEffect,
    ) -> Result<Option<Arc<dyn PendingDataCommit>>, String> {
        let pending = Authority(self.0.clone())
            .prepare_data_commit(effect)
            .await?;
        self.0.allowed.store(false, Ordering::SeqCst);
        Ok(pending)
    }
    fn rollback_completed(&self, succeeded: bool) {
        Authority(self.0.clone()).rollback_completed(succeeded);
    }
}
#[tokio::test]
async fn first_target_outlier_and_native_precommit_revoke_leave_no_table_or_metadata_effect() {
    for revoked in [false, true] {
        let (_root, connector, pipe, original) = fixture().await;
        assert!(connector.drop_pipe(&pipe).await.success);
        let before = connector.get_pipe_attributes(&pipe).await.unwrap();
        let authority: Arc<dyn MutationCheck> = if revoked {
            Arc::new(RevokeBeforeCommit(original.clone()))
        } else {
            Arc::new(Authority(original.clone()))
        };
        let incoming = if revoked {
            frame(&[(3, Some(14), 30)])
        } else {
            frame(&[(3, Some(14), 30), (4, Some(25), 40)])
        };
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.sync_pipe_result(&pipe, incoming, &SyncOptions::default()),
        )
        .await;
        assert!(!result.success, "Unadmitted first target committed");
        assert!(!connector.pipe_exists(&pipe).await.unwrap());
        assert_eq!(connector.get_pipe_attributes(&pipe).await.unwrap(), before);
        assert_eq!(original.commits.load(Ordering::SeqCst), 0);
        assert_eq!(
            original.structural.load(Ordering::SeqCst),
            usize::from(revoked)
        );
        if revoked {
            assert_eq!(original.rollbacks.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn native_window_refuses_protected_physical_targets_without_relying_on_http_guard() {
    let root = tempfile::tempdir().unwrap();
    let connector = SqlConnector::from_uri(
        "window_fixture",
        &format!("sqlite:///{}", root.path().join("owned.db").display()),
    )
    .await
    .unwrap();
    for target in [
        "MRSM_USERS",
        "mrsm_pipes",
        "mrsm_plugins",
        "mrsm_tokens",
        "_mrsm_engine_nodes",
        "sqlite_schema",
    ] {
        let pipe = Pipe::builder("manual:fixture", "protected")
            .instance("sql:window_fixture")
            .parameters(
                Value::from_json_str(&format!(
                    r#"{{"target":"{target}","dtypes":{{"key":"int","tick":"int","value":"int"}}}}"#
                ))
                .unwrap()
                .to_map(),
            )
            .build()
            .unwrap();
        let window = DataWindow::new(
            pipe.pipe_ref(),
            "b".repeat(64),
            DataAxis::WholeRelation,
            DataCoverage::WholeRelation,
            vec![DataSpan {
                begin: None,
                end: None,
            }],
            None,
        )
        .unwrap();
        let result = connector
            .preflight_window_frame(
                &pipe,
                &frame(&[(1, Some(14), 30)]),
                &SyncOptions::default(),
                &window,
            )
            .await;
        assert!(
            result.is_err(),
            "Native protected target was admitted: {target}"
        );
    }
}

#[tokio::test]
async fn zero_row_native_schema_creation_is_a_real_structural_commit_or_actual_rollback() {
    for revoked in [false, true] {
        let (_root, connector, pipe, original) = fixture().await;
        assert!(connector.drop_pipe(&pipe).await.success);
        let before = connector.get_pipe_attributes(&pipe).await.unwrap();
        // Preserve native int64 schema while supplying exactly zero values.
        let empty = frame(&[(3, Some(14), 30)]).take(&[]);
        assert_eq!(empty.height(), 0);
        assert_eq!(empty.width(), 3);
        let authority: Arc<dyn MutationCheck> = if revoked {
            Arc::new(RevokeBeforeCommit(original.clone()))
        } else {
            Arc::new(Authority(original.clone()))
        };
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.sync_pipe_result(&pipe, empty, &SyncOptions::default()),
        )
        .await;
        assert_eq!(result.success, !revoked, "{}", result.message);
        assert_eq!(connector.pipe_exists(&pipe).await.unwrap(), !revoked);
        assert_eq!(connector.get_pipe_attributes(&pipe).await.unwrap(), before);
        assert_eq!(result.inserted, 0);
        assert_eq!(original.prepares.load(Ordering::SeqCst), 1);
        assert_eq!(original.structural.load(Ordering::SeqCst), 1);
        assert_eq!(
            original.commits.load(Ordering::SeqCst),
            usize::from(!revoked)
        );
        assert_eq!(
            original.acknowledgements.load(Ordering::SeqCst),
            usize::from(!revoked)
        );
        if revoked {
            assert_eq!(original.rollbacks.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(rows(&connector, &pipe).await.height(), 0);
        }
    }
}

#[tokio::test]
async fn zero_row_without_native_schema_remains_a_noop_and_cannot_create_a_target() {
    let (_root, connector, pipe, original) = fixture().await;
    assert!(connector.drop_pipe(&pipe).await.success);
    let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
    let result = meerschaum_connector::mutation::scope(
        authority,
        connector.sync_pipe_result(&pipe, Frame::empty(), &SyncOptions::default()),
    )
    .await;
    assert!(result.success, "{}", result.message);
    assert!(!connector.pipe_exists(&pipe).await.unwrap());
    assert_eq!(original.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(original.structural.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn known_blob_special_dtype_reaches_native_commit_without_a_schema_rebuild() {
    let root = tempfile::tempdir().unwrap();
    let connector = SqlConnector::from_uri(
        "window_fixture",
        &format!("sqlite:///{}", root.path().join("blob.db").display()),
    )
    .await
    .unwrap();
    let pipe = Pipe::builder("manual:fixture", "known_blob").instance("sql:window_fixture")
        .parameters(Value::from_json_str(r#"{"columns":{"id":"id"},"dtypes":{"id":"int64","payload":"bytes"},"target":"known_blob"}"#).unwrap().to_map()).build().unwrap();
    assert!(connector.register_pipe(&pipe).await.success);
    let input = meerschaum_kernel::docs::frame_from_columns(&IndexMap::from([
        ("id".into(), vec![KValue::Int(2)]),
        ("payload".into(), vec![KValue::Bytes(vec![1, 2, 3])]),
    ]))
    .unwrap();
    let seeded = connector
        .sync(
            &pipe,
            PipeInput::Frame(input.clone()),
            &SyncOptions::default(),
        )
        .await;
    assert!(seeded.success, "{}", seeded.message);
    let sp = connector.sql_pipe(&pipe).unwrap();
    let state = connector.pipe_state(&sp).await;
    assert_eq!(state.cols_types.get("payload"), Some(&"BLOB".to_string()));
    let input =
        pipe_enforce_dtypes(PipeInput::Frame(input), &connector.dtypes_ctx(&sp, &state)).unwrap();
    assert!(are_dtypes_equal(
        input.dtypes().get("payload").unwrap(),
        "bytes"
    ));
    let before = connector.get_pipe_attributes(&pipe).await.unwrap();
    let window = DataWindow::new(
        pipe.pipe_ref(),
        "c".repeat(64),
        DataAxis::WholeRelation,
        DataCoverage::WholeRelation,
        vec![DataSpan {
            begin: None,
            end: None,
        }],
        None,
    )
    .unwrap();
    let original = Arc::new(Original {
        window,
        allowed: AtomicBool::new(true),
        prepares: AtomicUsize::new(0),
        structural: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        acknowledgements: AtomicUsize::new(0),
        rollbacks: AtomicUsize::new(0),
    });
    let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
    let result = meerschaum_connector::mutation::scope(
        authority,
        connector.sync_pipe_result(
            &pipe,
            input,
            &SyncOptions {
                check_existing: false,
                ..Default::default()
            },
        ),
    )
    .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(
        connector.pipe_state(&sp).await.cols_types.get("payload"),
        Some(&"BLOB".to_string())
    );
    assert_eq!(connector.get_pipe_attributes(&pipe).await.unwrap(), before);
    assert_eq!(original.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(original.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        original.structural.load(Ordering::SeqCst),
        0,
        "An already compatible binary column caused DDL"
    );
    assert_eq!(rows(&connector, &pipe).await.height(), 2);
    // A genuinely incompatible native frame still cannot request a BLOB->TEXT
    // rebuild merely because the registered column is a known special dtype.
    let incompatible = meerschaum_kernel::docs::frame_from_columns(&IndexMap::from([
        ("id".into(), vec![KValue::Int(3)]),
        ("payload".into(), vec![KValue::Int(7)]),
    ]))
    .unwrap();
    let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
    let refused = meerschaum_connector::mutation::scope(
        authority,
        connector.sync_pipe_result(
            &pipe,
            incompatible,
            &SyncOptions {
                check_existing: false,
                ..Default::default()
            },
        ),
    )
    .await;
    assert!(
        !refused.success && refused.message.contains("schema transition"),
        "{}",
        refused.message
    );
    assert_eq!(rows(&connector, &pipe).await.height(), 2);
    assert_eq!(original.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(
        connector.pipe_state(&sp).await.cols_types.get("payload"),
        Some(&"BLOB".to_string())
    );
}

#[tokio::test]
async fn nullable_column_and_changed_rows_share_one_sqlite_commit() {
    for (upsert, rolled_back) in [(true, false), (false, false), (true, true), (false, true)] {
        let (_root, connector, pipe, original) = fixture_with_upsert(upsert).await;
        let mut input = frame(&[(2, Some(12), 21), (3, Some(14), 30)]);
        let mut temperature = input.column("value").unwrap().clone();
        temperature.name = "temperature".into();
        input.set_column(temperature).unwrap();
        let authority: Arc<dyn MutationCheck> = if rolled_back {
            Arc::new(RevokeBeforeCommit(original.clone()))
        } else {
            Arc::new(Authority(original.clone()))
        };
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.sync_pipe_result(&pipe, input, &SyncOptions::default()),
        )
        .await;
        assert_eq!(result.success, !rolled_back, "{}", result.message);
        let stored = rows(&connector, &pipe).await;
        assert_eq!(original.structural.load(Ordering::SeqCst), 1);
        if rolled_back {
            assert!(!stored.contains("temperature"));
            assert_eq!(stored.height(), 2);
            assert_eq!(original.rollbacks.load(Ordering::SeqCst), 1);
        } else {
            let values = &stored.column("temperature").unwrap().data;
            assert_eq!(stored.height(), 3);
            assert!(values.get(0).is_na());
            assert_eq!(values.get(1), KValue::Int(21));
            assert_eq!(values.get(2), KValue::Int(30));
            assert_eq!(original.commits.load(Ordering::SeqCst), 1);
            assert_eq!(original.acknowledgements.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn inferred_binary_value_column_records_its_dtype_and_native_addition() {
    struct BinaryInference {
        native: Authority,
        received: AtomicUsize,
    }
    #[meerschaum_connector::async_trait]
    impl MutationCheck for BinaryInference {
        async fn check(&self) -> Result<(), String> {
            self.native.check().await
        }
        async fn write_window(&self) -> Result<Option<DataWindow>, String> {
            self.native.write_window().await
        }
        async fn prepare_data_commit(
            &self,
            effect: &NativeDataEffect,
        ) -> Result<Option<Arc<dyn PendingDataCommit>>, String> {
            self.native.prepare_data_commit(effect).await
        }
        fn tracks_metadata_inference(&self) -> bool {
            true
        }
        async fn metadata_inferred(
            &self,
            change: meerschaum_connector::mutation::MetadataInference,
        ) -> Result<(), String> {
            change.validate()?;
            let mut expected = change.before;
            let Some(Value::Map(dtypes)) = expected.get_mut("dtypes") else {
                return Err("Expected the registered fixture dtypes.".into());
            };
            assert!(
                dtypes
                    .insert("payload".into(), Value::from("bytes"))
                    .is_none()
            );
            assert_eq!(change.after, expected);
            self.received.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for guarded in [false, true] {
        let (_root, connector, pipe, original) = fixture_with_upsert(false).await;
        let mut input = frame(&[(3, Some(14), 30)]);
        let bytes = meerschaum_kernel::docs::frame_from_columns(&IndexMap::from([(
            "payload".into(),
            vec![KValue::Bytes(vec![0, 255, 10])],
        )]))
        .unwrap();
        input
            .set_column(bytes.column("payload").unwrap().clone())
            .unwrap();
        let sp = connector.sql_pipe(&pipe).unwrap();
        let state = connector.pipe_state(&sp).await;
        let normalized = pipe_enforce_dtypes(
            PipeInput::Frame(input.clone()),
            &connector.dtypes_ctx(&sp, &state),
        )
        .unwrap();
        let incoming = connector.df_cols_types_refined(&normalized);
        let planned = connector.columns_to_add(&sp, &incoming, &state.cols_types);
        assert_eq!(
            planned,
            [("payload".into(), "BLOB".into())],
            "types {:?}, incoming {incoming:?}, detected {:?}",
            normalized.dtypes(),
            get_special_cols(&normalized)
        );
        let authority = Arc::new(BinaryInference {
            native: Authority(original.clone()),
            received: AtomicUsize::new(0),
        });
        let options = SyncOptions::default();
        let sync = connector.sync(&pipe, PipeInput::Frame(input), &options);
        let result = if guarded {
            meerschaum_connector::mutation::scope(authority.clone(), sync).await
        } else {
            sync.await
        };
        assert!(result.success, "{}", result.message);
        let target = connector.pipe_table_name(&connector.sql_pipe(&pipe).unwrap());
        let raw = connector.read(&format!(
            "SELECT typeof(payload) AS storage_type, hex(payload) AS stored_hex FROM {target} WHERE key = 3"
        )).await.unwrap();
        let metadata = connector.get_pipe_columns_types(&pipe).await.unwrap();
        let expected_type = metadata.get("payload").map(String::as_str);
        let storage = raw.column("storage_type").unwrap().data.get(0);
        let hex = raw.column("stored_hex").unwrap().data.get(0);
        assert_eq!(
            (expected_type, storage, hex),
            (
                Some("BLOB"),
                KValue::Str("blob".into()),
                KValue::Str("00FF0A".into())
            ),
            "guarded={guarded}"
        );
        let stored = rows(&connector, &pipe).await;
        let value = &stored.column("payload").unwrap().data;
        assert_eq!(stored.height(), 3);
        assert!(value.get(0).is_na() && value.get(1).is_na());
        assert_eq!(
            value.get(2),
            KValue::Bytes(vec![0, 255, 10]),
            "guarded={guarded}"
        );
        let registration = connector.get_pipe_attributes(&pipe).await.unwrap();
        assert_eq!(
            registration["parameters"]
                .get("dtypes")
                .unwrap()
                .get("payload")
                .and_then(Value::as_str),
            Some("bytes")
        );
        assert_eq!(
            authority.received.load(Ordering::SeqCst),
            usize::from(guarded)
        );
        assert_eq!(
            original.structural.load(Ordering::SeqCst),
            usize::from(guarded)
        );
        assert_eq!(
            original.acknowledgements.load(Ordering::SeqCst),
            usize::from(guarded)
        );
    }
}

#[tokio::test]
async fn existing_sqlite_filter_accepts_multiple_rows_and_singleton() {
    let (_root, connector, pipe, mut original) = fixture_with_upsert(false).await;
    let types = connector.get_pipe_columns_types(&pipe).await.unwrap();
    let params = Map::from_iter([("value".into(), Value::Int(20))]);
    let window = original
        .window
        .clone()
        .with_row_predicate(RowPredicate::Filter {
            params: params.clone(),
            columns: std::collections::BTreeMap::from([(
                "value".into(),
                meerschaum_connector::mutation::RowColumn {
                    dtype: pipe.dtype("value"),
                    native_type: types.get("value").cloned(),
                    literal_null_marker: false,
                },
            )]),
        })
        .unwrap();
    Arc::get_mut(&mut original).unwrap().window = window;
    for incoming in [
        frame(&[(3, Some(14), 20), (4, Some(15), 20)]),
        frame(&[(5, Some(16), 20)]),
    ] {
        let authority: Arc<dyn MutationCheck> = Arc::new(Authority(original.clone()));
        let result = meerschaum_connector::mutation::scope(
            authority,
            connector.sync_pipe_result(&pipe, incoming, &SyncOptions::default()),
        )
        .await;
        assert!(result.success, "{}", result.message);
    }
    assert_eq!(rows(&connector, &pipe).await.height(), 5);
    assert_eq!(original.commits.load(Ordering::SeqCst), 2);
}
