//! Native inference receipts retain exact old metadata through write-lock waits.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use meerschaum_connector::mutation::{self, MetadataInference, MutationCheck};
use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{JsonValue, Value as KValue};

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct MainData {
    connector: Arc<SqlConnector>,
    pipe: Pipe,
    accepted: Mutex<Map>,
    receipts: Mutex<Vec<MetadataInference>>,
    refuse_receipt: bool,
    checks: AtomicUsize,
}
#[meerschaum_connector::async_trait]
impl MutationCheck for MainData {
    fn tracks_metadata_inference(&self) -> bool {
        true
    }
    async fn check(&self) -> Result<(), String> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        let actual = self
            .connector
            .get_pipe_parameters(&self.pipe)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("Lost registration")?;
        if actual != *self.accepted.lock().unwrap() {
            return Err("The accepted definition changed".into());
        }
        Ok(())
    }
    async fn metadata_inferred(&self, change: MetadataInference) -> Result<(), String> {
        change.validate()?;
        assert_eq!(change.pipe.pipe_ref(), self.pipe.pipe_ref());
        assert_eq!(change.before, *self.accepted.lock().unwrap());
        assert_eq!(
            self.connector
                .get_pipe_parameters(&self.pipe)
                .await
                .unwrap(),
            Some(change.after.clone())
        );
        self.receipts.lock().unwrap().push(change.clone());
        if self.refuse_receipt {
            return Err("Original source was revoked after metadata committed".into());
        }
        *self.accepted.lock().unwrap() = change.after;
        Ok(())
    }
}

async fn fixture(refuse_receipt: bool) -> (tempfile::TempDir, Arc<MainData>) {
    let root = tempfile::tempdir().unwrap();
    let connector = Arc::new(
        SqlConnector::from_uri(
            "inference",
            &format!("sqlite:///{}", root.path().join("main.db").display()),
        )
        .await
        .unwrap(),
    );
    let parameters = map(
        serde_json::json!({"columns":{"id":"id"},"dtypes":{"id":"int64"},"target":"inference_rows","access":{"owner":"alice","visibility":"private"}}),
    );
    let pipe = Pipe::builder("manual:fixture", "inference")
        .instance("sql:inference")
        .parameters(parameters)
        .build()
        .unwrap();
    assert!(connector.register_pipe(&pipe).await.success);
    assert!(
        connector
            .sync(
                &pipe,
                PipeInput::Docs(vec![indexmap::IndexMap::from_iter([(
                    "id".into(),
                    KValue::Int(1)
                )])]),
                &SyncOptions::default()
            )
            .await
            .success
    );
    let accepted = connector.get_pipe_parameters(&pipe).await.unwrap().unwrap();
    (
        root,
        Arc::new(MainData {
            connector,
            pipe,
            accepted: Mutex::new(accepted),
            receipts: Mutex::new(Vec::new()),
            refuse_receipt,
            checks: AtomicUsize::new(0),
        }),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_native_inference_keeps_fresh_old_metadata_reads_under_its_write_lock() {
    // Isolate the actual metadata UPDATE from data/DDL preparation. The other tests run
    // complete syncs; this case verifies a large exact old-map CAS and committed receipt.
    let (_root, main) = fixture(false).await;
    let mut main = Arc::try_unwrap(main)
        .ok()
        .expect("the fixture owns its original authority");
    let mut parameters = main.accepted.get_mut().unwrap().clone();
    parameters.insert("annotation".into(), Value::Str("x".repeat(3 * 1024 * 1024)));
    main.pipe.set_parameters(parameters.clone());
    let seeded = main.connector.edit_pipe(&main.pipe, false).await;
    assert!(
        seeded.success,
        "Could not install the accepted large definition: {}",
        seeded.message
    );
    *main.accepted.get_mut().unwrap() = parameters.clone();
    let mut inferred = main.pipe.clone();
    parameters
        .get_mut("dtypes")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("payload".into(), Value::Str("json".into()));
    inferred.set_parameters(parameters);
    let main = Arc::new(main);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        mutation::scope(main.clone(), main.connector.edit_pipe(&inferred, false)),
    )
    .await
    .expect("Native metadata inference exceeded its 15-second bound");
    assert!(result.success, "{}", result.message);
    assert!(
        main.checks.load(Ordering::SeqCst) >= 3,
        "The native metadata write did not retain its original checks through commit"
    );
    let current = main
        .connector
        .get_pipe_parameters(&main.pipe)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current
            .get("annotation")
            .and_then(Value::as_str)
            .unwrap()
            .len(),
        3 * 1024 * 1024
    );
    assert_eq!(current, *main.accepted.lock().unwrap());
    assert_eq!(
        main.receipts.lock().unwrap().len(),
        1,
        "The native metadata write did not produce exactly one committed receipt"
    );
}

fn new_json_row() -> PipeInput {
    PipeInput::Docs(vec![indexmap::IndexMap::from_iter([
        ("id".into(), KValue::Int(2)),
        (
            "payload".into(),
            KValue::Json(JsonValue::new(serde_json::json!({"nested":[1,2]}))),
        ),
    ])])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_native_inference_updates_metadata_and_rows_without_adopting_protected_fields() {
    let (_root, main) = fixture(false).await;
    let result = mutation::scope(
        main.clone(),
        main.connector
            .sync(&main.pipe, new_json_row(), &SyncOptions::default()),
    )
    .await;
    assert!(result.success, "{}", result.message);
    let receipts = main.receipts.lock().unwrap().clone();
    assert!(
        !receipts.is_empty(),
        "Native JSON inference emitted no receipt"
    );
    let current = main
        .connector
        .get_pipe_parameters(&main.pipe)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current, *main.accepted.lock().unwrap());
    assert_eq!(current.get("target"), receipts[0].before.get("target"));
    assert_eq!(current.get("access"), receipts[0].before.get("access"));
    assert_eq!(
        current["dtypes"].as_map().unwrap()["payload"],
        Value::Str("json".into())
    );
    let mut pipe = main.pipe.clone();
    pipe.set_parameters(current);
    assert_eq!(
        main.connector
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap()
            .height(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_metadata_receipt_refusal_prevents_later_row_writes_and_remains_explicit() {
    let (_root, main) = fixture(true).await;
    let old = main.accepted.lock().unwrap().clone();
    let result = mutation::scope(
        main.clone(),
        main.connector
            .sync(&main.pipe, new_json_row(), &SyncOptions::default()),
    )
    .await;
    assert!(
        !result.success,
        "Refused metadata still authorized rows: {}",
        result.message
    );
    assert!(!main.receipts.lock().unwrap().is_empty());
    let current = main
        .connector
        .get_pipe_parameters(&main.pipe)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        current, old,
        "The fixture did not commit inference before revocation"
    );
    assert_eq!(
        *main.accepted.lock().unwrap(),
        old,
        "A rejected receipt advanced authority"
    );
    let mut pipe = main.pipe.clone();
    pipe.set_parameters(current);
    assert_eq!(
        main.connector
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap()
            .height(),
        1,
        "Rows committed after original source refusal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_edit_while_inference_waits_for_lock_is_never_adopted_or_reverted() {
    let (_root, main) = fixture(false).await;
    let mut proposed = main.pipe.clone();
    let mut parameters = main.accepted.lock().unwrap().clone();
    parameters.insert(
        "dtypes".into(),
        Value::Map(map(serde_json::json!({"id":"int64","payload":"json"}))),
    );
    proposed.set_parameters(parameters);
    let mut blocker = main.connector.driver().begin_write().await.unwrap();
    let table = main.connector.instance_table_name("mrsm_pipes");
    let registration = main
        .connector
        .get_pipe_id(&main.pipe)
        .await
        .unwrap()
        .unwrap();
    let mut peer = main.accepted.lock().unwrap().clone();
    peer.insert("target".into(), Value::Str("peer_destination".into()));
    blocker
        .raw_execute(
            &format!("UPDATE {table} SET parameters = ? WHERE pipe_id = ?"),
            &[
                meerschaum_connector_sql::driver::SqlParam::Text(
                    Value::Map(peer.clone()).to_json_string(),
                ),
                meerschaum_connector_sql::driver::SqlParam::Int(registration),
            ],
        )
        .await
        .unwrap();
    let authority = main.clone();
    let connector = main.connector.clone();
    let pending = tokio::spawn(async move {
        mutation::scope(authority, connector.edit_pipe(&proposed, false)).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !pending.is_finished(),
        "The contender did not wait behind the metadata writer"
    );
    blocker.commit().await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(!result.success, "Peer metadata was adopted by inference");
    assert!(main.receipts.lock().unwrap().is_empty());
    assert_eq!(
        main.connector
            .get_pipe_parameters(&main.pipe)
            .await
            .unwrap(),
        Some(peer)
    );
}
