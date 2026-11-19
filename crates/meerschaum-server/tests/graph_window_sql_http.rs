//! Actual Main router JSON normalization + native SQLite window
//! writer. The test's trusted action provider persists the real Journal model; production
//! Controller/Catalog composition is a further CLI acceptance, not claimed by this fixture.
mod common;

use async_trait::async_trait;
use axum::{Router, http::HeaderMap};
use meerschaum_connector::{
    InstanceConnector, SyncOptions,
    mutation::{
        BatchClaimOutcome, MetadataInference, NativeBatch, NativeBatchReceipt, NativeDataEffect,
        PendingDataCommit, native_request_digest,
    },
};
use meerschaum_connector_api::client::{
    BATCH_HEADER, BatchOutcome, BatchReceipt, BatchReply, BatchRequest, BatchSelector,
    GRAPH_BATCH_PATH, GraphBinding,
};
use meerschaum_connector_sql::{SqlConnector, uri::ConnectionSpec};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value, success::Bound};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::{
    AppState,
    access::Access,
    auth::Principal,
    backends::{ActionsBackend, InstanceResolver},
    executor::{
        callbacks,
        pipe_operation::{
            CallbackBinding, NativeGraphRegistries, NodeWindow, PipeOperation, ReadAccess,
            Snapshot, SnapshotMember, WindowAxis, WindowCoverage, WindowRange,
            commits::{
                Journal, Ticket,
                batches::{self, Claim, Context, Selector},
            },
        },
        source_ceiling::Ceiling,
    },
};
use serde_json::{Value as Json, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;
use uuid::Uuid;

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
fn rights(write: bool) -> Map {
    map(json!({"role":"custom","scopes":if write{vec!["pipes:write"]}else{vec![]} }))
}
struct Resolver(Arc<SqlConnector>, Option<Arc<dyn InstanceConnector>>);
impl InstanceResolver for Resolver {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        match keys {
            "sql:data" => Some(self.0.clone()),
            "api:writer" => self.1.clone(),
            _ => None,
        }
    }
}
struct Model {
    state: OnceLock<AppState>,
    auth: Arc<common::MemoryInstance>,
    operation: Mutex<PipeOperation>,
    invalidate_registration_catalog: Option<Arc<SqlConnector>>,
    binding: Mutex<Option<CallbackBinding>>,
    journal: Mutex<Journal>,
    file: std::path::PathBuf,
    prepares: AtomicUsize,
    schema_effects: AtomicUsize,
    confirms: AtomicUsize,
    reads: AtomicUsize,
    mode: AtomicU8,
}
impl Model {
    fn operation(&self) -> PipeOperation {
        self.operation.lock().unwrap().clone()
    }

    fn persist(&self, journal: &Journal) -> Result<(), String> {
        use std::io::Write;
        let mut file = std::fs::File::create(&self.file).map_err(|e| e.to_string())?;
        file.write_all(&serde_json::to_vec(journal).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::File::open(self.file.parent().unwrap())
            .and_then(|parent| parent.sync_all())
            .map_err(|e| e.to_string())
    }
    async fn read(&self, caller: &Access) -> Result<Option<ReadAccess>, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let Some(Principal::Execution(identity)) = &caller.auth else {
            return Ok(None);
        };
        let binding = self
            .binding
            .lock()
            .unwrap()
            .clone()
            .ok_or("No native fixture binding")?;
        if identity.grant.id != binding.grant || identity.grant.launch_id != binding.launch {
            return Err("Wrong original fixture source".into());
        }
        let state = self.state.get().unwrap();
        let fresh = callbacks::load_identity(state, binding.grant, binding.launch)
            .await
            .map_err(|e| e.detail_text())?;
        if fresh.grant.graph_origin.as_ref() != Some(&binding)
            || fresh.grant.graph_tracking != 1
            || fresh.grant.graph_pending
        {
            return Err("Wrong native fixture graph lineage".into());
        }
        let mut bounded = fresh.caller();
        bounded.principal.permissions =
            meerschaum_server::access::intersect(&bounded.principal.permissions, &caller.scopes());
        let operation = self.operation();
        let registries = NativeGraphRegistries::load(state, operation.metadata.as_ref())
            .await
            .map_err(|e| e.detail_text())?;
        registries
            .scope(state, async {
                operation
                    .authorize(state, &bounded, &operation.sysargs().unwrap())
                    .await
                    .map_err(|e| e.detail_text())
            })
            .await?;
        let pending_nodes = if self
            .journal
            .lock()
            .unwrap()
            .has_pending_for(&operation.pipe_ref)
        {
            BTreeSet::from([operation.pipe_ref.clone()])
        } else {
            BTreeSet::new()
        };
        Ok(Some(ReadAccess {
            caller: bounded,
            operation: operation.into(),
            binding: binding.clone(),
            issuer: binding,
            pending_nodes,
        }))
    }
}
fn selector(value: &BatchSelector) -> Selector {
    Selector {
        stream: value.stream,
        ordinal: value.ordinal,
        batch: value.batch,
    }
}

fn wire_selector(value: &Selector) -> BatchSelector {
    BatchSelector {
        stream: value.stream,
        ordinal: value.ordinal,
        batch: value.batch,
    }
}

fn wire_receipt(value: &batches::Receipt) -> BatchReceipt {
    BatchReceipt {
        selector: wire_selector(value.selector()),
        sha256: value.sha256().into(),
    }
}

/// A route can refuse before it reaches the native writer. The same Journal CAS
/// proves no handoff before retiring that fixture-owned claim.
struct Unstarted {
    model: Arc<Model>,
    claim: Arc<Claim>,
}

impl Drop for Unstarted {
    fn drop(&mut self) {
        if self.claim.never_started() {
            let mut journal = self.model.journal.lock().unwrap();
            if journal.reject_batch_before_handoff(&self.claim).is_ok() {
                let _ = self.model.persist(&journal);
            }
        }
    }
}

struct Backend(Arc<Model>);
#[async_trait]
impl ActionsBackend for Backend {
    fn list(&self) -> Vec<String> {
        vec!["sync".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        SuccessTuple::fail("Unused")
    }
    async fn pipe_operation_read(&self, caller: &Access) -> Result<Option<ReadAccess>, String> {
        self.0.read(caller).await
    }
    async fn graph_batch(
        &self,
        caller: &Access,
        request: &BatchRequest,
    ) -> Result<Option<BatchReply>, String> {
        let Some(read) = self.0.read(caller).await? else {
            return Ok(None);
        };
        let context = read
            .native_batch_context(request.reference())
            .map_err(|error| error.detail_text())?;
        let mut journal = self.0.journal.lock().unwrap();
        let outcome = match request {
            BatchRequest::Issue {
                predecessor,
                request_sha256,
                ..
            } => match journal.issue_batch(
                context.clone(),
                predecessor.as_ref().map(selector),
                request_sha256.clone(),
            )? {
                batches::Issue::Reserved(value) => BatchOutcome::Reserved {
                    selector: wire_selector(&value),
                },
                batches::Issue::Accepted(receipt) => BatchOutcome::AlreadyCommitted {
                    receipt: wire_receipt(&receipt),
                },
                batches::Issue::Unknown => BatchOutcome::Unknown,
            },
            BatchRequest::Delivered { receipt, .. } => {
                journal.deliver_batch(&context, &selector(&receipt.selector), &receipt.sha256)?;
                BatchOutcome::Delivered {
                    receipt: receipt.clone(),
                }
            }
        };
        self.0.persist(&journal)?;
        let wire_binding = |binding: &CallbackBinding| GraphBinding {
            grant: binding.grant,
            launch: binding.launch,
            revision: binding.context_revision,
        };
        Ok(Some(BatchReply {
            version: 1,
            consumer: wire_binding(context.consumer()),
            issuer: wire_binding(context.issuer()),
            reference: context.reference().into(),
            outcome,
        }))
    }

    async fn claim_pipe_batch(
        &self,
        caller: &Access,
        reference: &str,
        selected: &BatchSelector,
        digest: &str,
    ) -> Result<BatchClaimOutcome, String> {
        let context = self.batch_context(caller, reference).await?;
        let outcome = {
            let mut journal = self.0.journal.lock().unwrap();
            let outcome = journal.claim_batch(&context, &selector(selected), digest)?;
            self.0.persist(&journal)?;
            outcome
        };
        match outcome {
            batches::ClaimOutcome::Native(claim) => {
                let claim = Arc::new(claim);
                let guard = Arc::new(Unstarted {
                    model: self.0.clone(),
                    claim: claim.clone(),
                });
                let model = self.0.clone();
                let rollback = claim.clone();
                let native = NativeBatch::from_host(
                    reference.into(),
                    digest.into(),
                    claim,
                    Claim::begin_target,
                )?
                .with_host_lifecycle(
                    move || {
                        let model = model.clone();
                        let claim = rollback.clone();
                        async move {
                            let mut journal = model.journal.lock().unwrap();
                            journal.rollback_native_batch(claim)?;
                            model.persist(&journal)
                        }
                    },
                    guard,
                );
                Ok(BatchClaimOutcome::Native(native))
            }
            batches::ClaimOutcome::Accepted(receipt) => Ok(BatchClaimOutcome::AlreadyCommitted(
                native_receipt(&receipt)?,
            )),
            batches::ClaimOutcome::Unknown => Ok(BatchClaimOutcome::Unknown),
        }
    }

    async fn confirm_pipe_batch(&self, caller: &Access, batch: &NativeBatch) -> Result<(), String> {
        self.inspect(caller, batch).await?;
        self.0.confirms.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn accepted_pipe_batch(
        &self,
        caller: &Access,
        batch: &NativeBatch,
    ) -> Result<NativeBatchReceipt, String> {
        let receipt = self
            .inspect(caller, batch)
            .await?
            .ok_or("The fixture has no accepted native result")?;
        native_receipt(&receipt)
    }

    async fn accept_pipe_inference(
        &self,
        _: &Access,
        change: MetadataInference,
    ) -> Result<bool, String> {
        self.0
            .operation
            .lock()
            .unwrap()
            .metadata
            .as_mut()
            .unwrap()
            .accept_inference(&change)
    }

    async fn prepare_pipe_commit(
        &self,
        caller: &Access,
        effect: NativeDataEffect,
    ) -> Result<Arc<dyn PendingDataCommit>, String> {
        let admitted = self
            .0
            .read(caller)
            .await?
            .ok_or("Missing native fixture graph")?;
        let binding = admitted
            .native_commit_binding(&effect)
            .map_err(|e| e.detail_text())?;
        let ticket = {
            let mut journal = self.0.journal.lock().unwrap();
            let ticket = journal.prepare(binding, &effect)?;
            self.0.persist(&journal)?;
            ticket
        };
        self.0.prepares.fetch_add(1, Ordering::SeqCst);
        if effect.schema().is_some() {
            self.0.schema_effects.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(data) = &self.0.invalidate_registration_catalog {
            // This fixture evicts the warm catalog answer after the write snapshot is frozen.
            // The database and actual registration stay the same throughout.
            data.note_raw_statement("SELECT pipe_id FROM mrsm_pipes", true);
        }
        if self.0.mode.load(Ordering::SeqCst) == 1 {
            self.0.auth.seed_user_attributes("alice", rights(false));
        }
        Ok(Arc::new(Pending {
            model: self.0.clone(),
            caller: caller.clone(),
            ticket: Arc::new(ticket),
        }))
    }
}
fn native_receipt(receipt: &batches::Receipt) -> Result<NativeBatchReceipt, String> {
    NativeBatchReceipt::from_host(
        receipt.selector().stream,
        receipt.selector().ordinal,
        receipt.selector().batch,
        receipt.sha256().into(),
    )
}

impl Backend {
    async fn batch_context(&self, caller: &Access, reference: &str) -> Result<Context, String> {
        self.0
            .read(caller)
            .await?
            .ok_or("Missing native fixture graph")?
            .native_batch_context(reference)
            .map_err(|error| error.detail_text())
    }

    async fn inspect(
        &self,
        caller: &Access,
        batch: &NativeBatch,
    ) -> Result<Option<batches::Receipt>, String> {
        let claim = batch.owner::<Claim>().ok_or("Wrong fixture batch owner")?;
        let context = self.batch_context(caller, batch.reference()).await?;
        self.0
            .journal
            .lock()
            .unwrap()
            .confirm_batch_claim(&claim, &context)
    }
}

struct Pending {
    model: Arc<Model>,
    caller: Access,
    ticket: Arc<Ticket>,
}
#[async_trait]
impl PendingDataCommit for Pending {
    async fn before_commit(&self, effect: &NativeDataEffect) -> Result<(), String> {
        let admitted = self
            .model
            .read(&self.caller)
            .await?
            .ok_or("Graph missing before commit")?;
        let binding = admitted
            .native_commit_binding(effect)
            .map_err(|e| e.detail_text())?;
        self.model
            .journal
            .lock()
            .unwrap()
            .confirm_prepared(&self.ticket, &binding)
    }
    async fn committed(&self, _: &NativeDataEffect) -> Result<(), String> {
        {
            let mut journal = self.model.journal.lock().unwrap();
            journal.observed_commit(&self.ticket)?;
            self.model.persist(&journal)?;
        }
        if self.model.mode.load(Ordering::SeqCst) == 2 {
            self.model.auth.seed_user_attributes("alice", rights(false));
        }
        Ok(())
    }
    async fn acknowledge(&self, effect: &NativeDataEffect) -> Result<(), String> {
        let admitted = self
            .model
            .read(&self.caller)
            .await?
            .ok_or("Graph missing after commit")?;
        let binding = admitted
            .native_commit_binding(effect)
            .map_err(|e| e.detail_text())?;
        let mut journal = self.model.journal.lock().unwrap();
        journal.acknowledge(&self.ticket, &binding)?;
        self.model.persist(&journal)
    }
    async fn rolled_back(&self) -> Result<(), String> {
        let mut journal = self.model.journal.lock().unwrap();
        journal.rolled_back(&self.ticket, true)?;
        self.model.persist(&journal)
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    app: Router,
    data: Arc<SqlConnector>,
    pipe: Pipe,
    model: Arc<Model>,
    token: String,
    _self_api: Option<SelfApiServer>,
}

struct SelfApiServer(tokio::task::JoinHandle<()>);

impl Drop for SelfApiServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn fixture() -> Fixture {
    fixture_options(false, false, None).await
}
async fn fixture_options(sql_auth: bool, missing_target: bool, protected: Option<&str>) -> Fixture {
    fixture_with_alias(sql_auth, missing_target, protected, None).await
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum BinarySchema {
    Prepared,
    Inferred,
}

async fn fixture_with_alias(
    sql_auth: bool,
    missing_target: bool,
    protected: Option<&str>,
    binary_schema: Option<BinarySchema>,
) -> Fixture {
    fixture_for_database(sql_auth, missing_target, protected, binary_schema, None).await
}

async fn fixture_for_database(
    sql_auth: bool,
    missing_target: bool,
    protected: Option<&str>,
    binary_schema: Option<BinarySchema>,
    innodb_spec: Option<ConnectionSpec>,
) -> Fixture {
    let innodb = innodb_spec.is_some();
    let alias = binary_schema.is_some();
    let prepared_blob = binary_schema == Some(BinarySchema::Prepared);
    let root = tempfile::tempdir().unwrap();
    let auth = Arc::new(common::MemoryInstance::new("fixture"));
    auth.seed_user("alice", "pass", None);
    auth.seed_user_attributes("alice", rights(true));
    let sqlite_uri = format!("sqlite:///{}", root.path().join("data.db").display());
    let data = Arc::new(match innodb_spec {
        Some(spec) => SqlConnector::connect("data", spec).await.unwrap(),
        None => SqlConnector::from_uri("data", &sqlite_uri).await.unwrap(),
    });
    if sql_auth {
        let mut user = meerschaum_core::users::User::new("alice", Some("pass"));
        user.attributes = Some(rights(true));
        if !innodb || data.get_user_id(&user).await.unwrap().is_none() {
            let registered = data.register_user(&mut user).await;
            assert!(registered.success, "{}", registered.message);
        }
    }
    let mut parameters = map(
        json!({"columns":{"datetime":"tick","primary":"key"},"dtypes":{"key":"int","tick":"int","value":"int"},"autoincrement":false,"upsert":true,"access":{"owner":"alice","visibility":"private"}}),
    );
    if prepared_blob {
        parameters
            .get_mut("dtypes")
            .unwrap()
            .as_map_mut()
            .unwrap()
            .insert("payload".into(), Value::Str("bytes".into()));
    }
    if alias {
        use meerschaum_core::access::{Team, TeamRole};
        let mut service =
            meerschaum_core::users::User::new("fixture_service", Some("fixture-service-password"));
        service.attributes = Some(map(
            json!({"role":"custom","scopes":["pipes:read","pipes:write"]}),
        ));
        if !innodb || data.get_user_id(&service).await.unwrap().is_none() {
            let registered = data.register_user(&mut service).await;
            assert!(registered.success, "{}", registered.message);
        }
        let mut team = Team::new("Fixture");
        team.add_member("alice", TeamRole::Member);
        team.attributes =
            map(json!({"isolated":true,"instance":"api:writer","connectors":["api:writer"]}));
        if !innodb || data.get_team(&team.name).await.unwrap().is_none() {
            let registered = data.register_team(&mut team).await;
            assert!(registered.success, "{}", registered.message);
        }
        parameters.insert(
            "access".into(),
            Value::Map(map(
                json!({"owner":"fixture_service","team":"Fixture","visibility":"team"}),
            )),
        );
    }
    if let Some(target) = protected {
        parameters.insert("target".into(), Value::Str(target.into()));
    }
    let metric = if innodb {
        let metric = format!("append_{}", Uuid::new_v4().simple());
        parameters.insert("target".into(), Value::Str(metric.clone()));
        parameters.insert("upsert".into(), Value::Bool(false));
        metric
    } else {
        "http_window".into()
    };
    let pipe = Pipe::builder("manual:fixture", &metric)
        .instance("sql:data")
        .parameters(parameters)
        .build()
        .unwrap();
    assert!(data.register_pipe(&pipe).await.success);
    let initial = meerschaum_kernel::docs::frame_from_docs(&[
        [
            ("key".into(), KValue::Int(1)),
            ("tick".into(), KValue::Int(5)),
            ("value".into(), KValue::Int(10)),
        ]
        .into_iter()
        .collect(),
        [
            ("key".into(), KValue::Int(2)),
            ("tick".into(), KValue::Int(12)),
            ("value".into(), KValue::Int(20)),
        ]
        .into_iter()
        .collect(),
    ])
    .unwrap();
    if innodb {
        data.driver().exec(&format!(
            "CREATE TABLE `{metric}` (`key` BIGINT PRIMARY KEY, tick BIGINT NOT NULL, value BIGINT NOT NULL, payload BLOB NOT NULL) ENGINE=InnoDB"
        )).await.unwrap();
        data.driver()
            .exec(&format!(
                "INSERT INTO `{metric}` VALUES (1, 5, 10, X'01'), (2, 12, 20, X'02')"
            ))
            .await
            .unwrap();
    } else if !missing_target && protected.is_none() {
        let initial = if prepared_blob {
            let mut docs = meerschaum_connector::pipe_ops::frame_to_docs(&initial);
            docs[0].insert("payload".into(), KValue::Bytes(vec![1]));
            docs[1].insert("payload".into(), KValue::Bytes(vec![2]));
            meerschaum_kernel::docs::frame_from_docs(&docs).unwrap()
        } else {
            initial
        };
        let seeded = data
            .sync_pipe_result(&pipe, initial, &SyncOptions::default())
            .await;
        assert!(seeded.success, "{}", seeded.message);
    }
    let registration = Value::Int(data.get_pipe_id(&pipe).await.unwrap().unwrap());
    let graph_pipe = if alias {
        Pipe::builder(pipe.connector_keys(), pipe.metric_key())
            .instance("api:writer")
            .parameters(pipe.raw_parameters().clone())
            .build()
            .unwrap()
    } else {
        pipe.clone()
    };
    let window = NodeWindow::from_main_plan(
        &graph_pipe,
        registration.clone(),
        WindowAxis::Integer {
            column: "tick".into(),
            explicit_unit: None,
        },
        vec![WindowRange {
            begin: Some(Bound::Int(10)),
            end: Some(Bound::Int(20)),
        }],
        WindowCoverage::AxisRanges,
        false,
    )
    .unwrap();
    let operation = PipeOperation {
        row_params: None,
        pipe_ref: graph_pipe.pipe_ref(),
        registration: registration.clone(),
        verify: false,
        begin: Some("10".into()),
        end: Some("20".into()),
        parents: None,
        children: None,
        metadata: Some(
            Snapshot::new(BTreeMap::from([(
                graph_pipe.pipe_ref(),
                SnapshotMember {
                    registration,
                    parameters: pipe.raw_parameters().clone(),
                    writable: true,
                    node_window: Some(window),
                },
            )]))
            .unwrap(),
        ),
    };
    let model = Arc::new(Model {
        state: OnceLock::new(),
        auth: auth.clone(),
        operation: Mutex::new(operation),
        invalidate_registration_catalog: (alias && !innodb).then(|| data.clone()),
        binding: Mutex::new(None),
        journal: Mutex::new(Journal::default()),
        file: root.path().join("native-journal"),
        prepares: AtomicUsize::new(0),
        schema_effects: AtomicUsize::new(0),
        confirms: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        mode: AtomicU8::new(0),
    });
    let mut config = common::config(root.path());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(json!({"sql":{"data":{"flavor":data.flavor().as_str()}}}));
    let listener = if alias {
        Some(
            tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let api = if let Some(listener) = &listener {
        let addr = listener.local_addr().unwrap();
        config.instance_keys = "sql:data".into();
        config.execution_callback_url = Some(format!("http://{addr}"));
        config.allowed_instance_keys.push("api:writer".into());
        let settings = json!({"host":"127.0.0.1","port":addr.port(),"instance_keys":"sql:data","username":"fixture_service","password":"fixture-service-password","teams":["Fixture"]});
        config.connectors = map(
            json!({"sql":{"data":{"flavor":data.flavor().as_str()}},"api":{"writer":settings}}),
        );
        Some(Arc::new(
            meerschaum_connector_api::APIConnector::new(Some("writer"), map(settings)).unwrap(),
        ) as Arc<dyn InstanceConnector>)
    } else {
        None
    };
    let instance: Arc<dyn InstanceConnector> = if sql_auth { data.clone() } else { auth };
    let state = AppState::builder(config, instance)
        .resolver(Arc::new(Resolver(data.clone(), api)))
        .native_self_api_engines()
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Backend(model.clone())))
        .build()
        .unwrap();
    assert!(model.state.set(state.clone()).is_ok());
    let app = meerschaum_server::app(state.clone());
    let self_api = listener.map(|listener| {
        let app = app.clone();
        SelfApiServer(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }))
    });
    let original = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", original.parse().unwrap());
    let source = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(
        source.has("pipes:write")
            && !source.has("pipes:read")
            && !source.has("actions:execute")
            && !source.has("jobs:execute")
    );
    let launch = Uuid::new_v4();
    let grant = callbacks::register_graph_with_sources(
        &state,
        &source,
        launch,
        None,
        &["pipes:write"],
        Ceiling::default(),
    )
    .await
    .unwrap();
    let binding = CallbackBinding {
        grant: grant.id,
        launch,
        context_revision: 1,
    };
    *model.binding.lock().unwrap() = Some(binding);
    callbacks::bind_graph_origin(&state, grant.id, launch, 1, None)
        .await
        .unwrap();
    let credential = callbacks::credential(&state, grant.id, launch)
        .await
        .unwrap();
    Fixture {
        _root: root,
        app,
        data,
        pipe,
        model,
        token: format!("Bearer {}", credential.token),
        _self_api: self_api,
    }
}
const DATA_PATH: &str =
    "/pipes/manual:fixture/http_window/None/data?instance_keys=sql%3Adata&force=false&force=true";

fn data_request(
    token: &str,
    body: Vec<u8>,
    selector: Option<&BatchSelector>,
) -> axum::http::Request<axum::body::Body> {
    let mut request = common::req(
        "POST",
        DATA_PATH,
        Some(token),
        Some(("application/json", body)),
    );
    if let Some(selector) = selector {
        request.headers_mut().insert(
            BATCH_HEADER,
            serde_json::to_string(selector).unwrap().parse().unwrap(),
        );
    }
    request
}

async fn control_response(f: &Fixture, request: &BatchRequest) -> common::Resp {
    common::send(
        &f.app,
        common::req(
            "POST",
            GRAPH_BATCH_PATH,
            Some(&f.token),
            Some(("application/json", serde_json::to_vec(request).unwrap())),
        ),
    )
    .await
}

async fn control(f: &Fixture, request: &BatchRequest) -> BatchReply {
    let response = control_response(f, request).await;
    assert_eq!(response.status, 200, "{}", response.text());
    let envelope = response.json();
    assert_eq!(envelope["success"], true);
    let reply: BatchReply = serde_json::from_value(envelope["batch"].clone()).unwrap();
    reply.validate(request).unwrap();
    let expected = f.model.binding.lock().unwrap().clone().unwrap();
    for binding in [&reply.issuer, &reply.consumer] {
        assert_eq!(binding.grant, expected.grant);
        assert_eq!(binding.launch, expected.launch);
        assert_eq!(binding.revision, expected.context_revision);
    }
    reply
}

fn issue(f: &Fixture, body: &[u8], predecessor: Option<&BatchSelector>) -> BatchRequest {
    issue_at(f, DATA_PATH, body, predecessor)
}

fn issue_at(
    f: &Fixture,
    path: &str,
    body: &[u8],
    predecessor: Option<&BatchSelector>,
) -> BatchRequest {
    let query = meerschaum_connector_api::wire::parse_query(path.split_once('?').unwrap().1);
    let reference = f.model.operation().pipe_ref;
    let request_sha256 = native_request_digest(&reference, "data", &query, body).unwrap();
    BatchRequest::Issue {
        reference,
        predecessor: predecessor.cloned(),
        request_sha256,
    }
}

struct Write {
    body: Vec<u8>,
    selector: BatchSelector,
}

impl Write {
    fn request(&self, token: &str) -> axum::http::Request<axum::body::Body> {
        data_request(token, self.body.clone(), Some(&self.selector))
    }
}

async fn reserve(f: &Fixture, body: Json, predecessor: Option<&BatchSelector>) -> Write {
    let body = serde_json::to_vec(&body).unwrap();
    let reply = control(f, &issue(f, &body, predecessor)).await;
    let BatchOutcome::Reserved { selector } = reply.outcome else {
        panic!("The exact fixture data write did not receive a reservation: {reply:?}");
    };
    Write { body, selector }
}

async fn post(f: &Fixture, write: &Write) -> common::Resp {
    common::send(&f.app, write.request(&f.token)).await
}

fn succeeded(response: &common::Resp) -> bool {
    let result = response.json();
    response.status.is_success() && (result["result"][0] == true || result[0] == true)
}

async fn deliver(f: &Fixture, write: &Write, response: &common::Resp) {
    assert!(succeeded(response), "{}", response.text());
    let response = response.json();
    assert_eq!(response.as_object().unwrap().len(), 2);
    let receipt: BatchReceipt = serde_json::from_value(response["receipt"].clone()).unwrap();
    assert_eq!(receipt.selector, write.selector);
    let reply = control(
        f,
        &BatchRequest::Delivered {
            reference: f.model.operation().pipe_ref,
            receipt: receipt.clone(),
        },
    )
    .await;
    let BatchOutcome::Delivered { receipt: confirmed } = reply.outcome else {
        panic!("Main did not confirm delivery of its exact native receipt");
    };
    assert_eq!(confirmed, receipt);
}

async fn rows(f: &Fixture) -> Vec<indexmap::IndexMap<String, KValue>> {
    meerschaum_connector::pipe_ops::frame_to_docs(
        &f.data
            .get_pipe_data(&f.pipe, &Default::default())
            .await
            .unwrap()
            .unwrap(),
    )
}
#[tokio::test]
async fn main_json_normalization_reaches_actual_native_window_commit_and_old_row_guard() {
    let f = fixture().await;
    let first = reserve(
        &f,
        json!([{"key":2,"tick":13,"value":21},{"key":3,"tick":14,"value":30}]),
        None,
    )
    .await;
    let success = post(&f, &first).await;
    assert!(succeeded(&success), "{}", success.text());
    assert!(f.model.journal.lock().unwrap().has_pending());
    deliver(&f, &first, &success).await;
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(rows(&f).await.len(), 3);
    assert!(!f.model.journal.lock().unwrap().has_pending());
    let before = rows(&f).await;
    let second = reserve(
        &f,
        json!([{"key":4,"tick":15,"value":40},{"key":1,"tick":16,"value":99}]),
        Some(&first.selector),
    )
    .await;
    let outside = post(&f, &second).await;
    assert!(
        outside.json()[0] == false && outside.text().contains("matching-key OLD row"),
        "{}",
        outside.text()
    );
    assert_eq!(rows(&f).await, before);
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    f.data.driver().close().await;
}
#[tokio::test]
async fn main_original_source_revoke_before_commit_rolls_back_but_after_commit_retains_truth() {
    for mode in [1, 2] {
        let f = fixture().await;
        let before = rows(&f).await;
        f.model.mode.store(mode, Ordering::SeqCst);
        let write = reserve(&f, json!([{"key":3,"tick":14,"value":30}]), None).await;
        let response = post(&f, &write).await;
        assert!(!succeeded(&response), "{}", response.text());
        if mode == 1 {
            assert_eq!(rows(&f).await, before);
            assert!(!f.model.journal.lock().unwrap().has_pending());
        } else {
            assert!(
                response.text().contains("already committed"),
                "{}",
                response.text()
            );
            assert_eq!(rows(&f).await.len(), 3);
            assert!(f.model.journal.lock().unwrap().has_pending());
            f.model.auth.seed_user_attributes("alice", rights(true));
            let replay = post(&f, &write).await;
            assert!(!replay.status.is_success(), "{}", replay.text());
            assert_eq!(rows(&f).await.len(), 3);
            assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
        }
        f.data.driver().close().await;
    }
}

#[tokio::test]
async fn direct_main_sql_auth_first_target_records_structural_commit_without_metadata_changes() {
    let f = fixture_options(true, true, None).await;
    let before = f.data.get_pipe_attributes(&f.pipe).await.unwrap();
    let first = reserve(&f, json!([{"key":3,"tick":14,"value":30}]), None).await;
    let response = post(&f, &first).await;
    assert!(succeeded(&response), "{}", response.text());
    deliver(&f, &first, &response).await;
    assert_eq!(rows(&f).await.len(), 1);
    assert_eq!(f.data.get_pipe_attributes(&f.pipe).await.unwrap(), before);
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    assert!(!f.model.journal.lock().unwrap().has_pending());
    // The same new target remains compatible with the guarded update path.
    let second = reserve(
        &f,
        json!([{"key":3,"tick":15,"value":31}]),
        Some(&first.selector),
    )
    .await;
    let update = post(&f, &second).await;
    assert!(succeeded(&update), "{}", update.text());
    deliver(&f, &second, &update).await;
    assert_eq!(rows(&f).await.len(), 1);
    f.data.driver().close().await;
}

#[tokio::test]
async fn direct_main_sql_role_revoked_during_real_writer_wait_never_creates_target() {
    let f = fixture_options(true, true, None).await;
    let write = reserve(&f, json!([{"key":3,"tick":14,"value":30}]), None).await;
    let mut held = f.data.driver().begin_write().await.unwrap();
    let baseline = f.model.reads.load(Ordering::SeqCst);
    let app = f.app.clone();
    let request = write.request(&f.token);
    let request = tokio::spawn(async move { common::send(&app, request).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while f.model.reads.load(Ordering::SeqCst) <= baseline
            || f.model.confirms.load(Ordering::SeqCst) == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Request never confirmed its exact native batch under the held writer");
    assert!(
        !request.is_finished(),
        "The real target writer did not wait for the held native writer"
    );
    // This role change commits AFTER request admission and BEFORE its writer
    // exists. A pre-lock snapshot would incorrectly retain the former scope.
    let attributes = Value::Map(rights(false))
        .to_json_string()
        .replace('\'', "''");
    assert_eq!(
        held.exec(&format!(
            "UPDATE mrsm_users SET attributes='{attributes}' WHERE username='alice'"
        ))
        .await
        .unwrap(),
        1
    );
    held.commit().await.unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(20), request)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !succeeded(&response),
        "Revoked role wrote target: {}",
        response.text()
    );
    assert!(!f.data.pipe_exists(&f.pipe).await.unwrap());
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 0);
    assert!(!f.model.journal.lock().unwrap().has_pending());
    f.data.driver().close().await;
}

#[tokio::test]
async fn direct_main_captured_target_cannot_alias_auth_or_registration_storage() {
    for target in ["MRSM_USERS", "mrsm_pipes", "_mrsm_internal", "mrsm_tokens"] {
        let f = fixture_options(true, true, Some(target)).await;
        let before = f
            .data
            .driver()
            .read_frame("SELECT username,password_hash,attributes FROM mrsm_users ORDER BY user_id")
            .await
            .unwrap();
        let before = meerschaum_connector::pipe_ops::frame_to_docs(&before);
        let attributes = f.data.get_pipe_attributes(&f.pipe).await.unwrap();
        let body = serde_json::to_vec(&json!([{"key":3,"tick":14,"value":30}])).unwrap();
        let denied = "The original pipe registration or object permission changed.";
        let issued = control_response(&f, &issue(&f, &body, None)).await;
        assert_eq!(issued.status, 503, "{target}: {}", issued.text());
        assert_eq!(issued.json(), json!({"detail": denied}), "{target}");
        assert!(!f.model.journal.lock().unwrap().has_pending(), "{target}");

        // No selector was issued. A well-formed but unissued selector lets the write route
        // reach fresh object admission without testing only its missing-header guard.
        let unissued = BatchSelector {
            stream: Uuid::new_v4(),
            ordinal: 1,
            batch: Uuid::new_v4(),
        };
        let response = common::send(&f.app, data_request(&f.token, body, Some(&unissued))).await;
        assert_eq!(response.status, 409, "{target}: {}", response.text());
        assert_eq!(response.json(), json!({"detail": denied}), "{target}");
        assert_eq!(f.model.prepares.load(Ordering::SeqCst), 0);
        assert!(!f.model.journal.lock().unwrap().has_pending(), "{target}");
        let after = f
            .data
            .driver()
            .read_frame("SELECT username,password_hash,attributes FROM mrsm_users ORDER BY user_id")
            .await
            .unwrap();
        assert_eq!(
            meerschaum_connector::pipe_ops::frame_to_docs(&after),
            before
        );
        assert_eq!(
            f.data.get_pipe_attributes(&f.pipe).await.unwrap(),
            attributes
        );
        f.data.driver().close().await;
    }
}

#[tokio::test]
async fn main_typed_empty_columns_create_structural_receipt_and_precommit_revoke_rolls_back_schema()
{
    for revoked in [false, true] {
        // Positive exercises actual same-file SQL authentication. The revoke case
        // uses the existing separately mutable original-user store, so it can
        // revoke DURING the target writer without issuing a competing SQL write.
        let f = fixture_options(!revoked, true, None).await;
        let before = f.data.get_pipe_attributes(&f.pipe).await.unwrap();
        if revoked {
            f.model.mode.store(1, Ordering::SeqCst);
        }
        let write = reserve(&f, json!({"key": [], "tick": [], "value": []}), None).await;
        let response = post(&f, &write).await;
        assert_eq!(succeeded(&response), !revoked, "{}", response.text());
        if !revoked {
            deliver(&f, &write, &response).await;
        }
        assert_eq!(f.data.pipe_exists(&f.pipe).await.unwrap(), !revoked);
        assert_eq!(
            f.model.prepares.load(Ordering::SeqCst),
            1,
            "A real structural0 effect was skipped"
        );
        assert!(!f.model.journal.lock().unwrap().has_pending());
        assert_eq!(f.data.get_pipe_attributes(&f.pipe).await.unwrap(), before);
        if !revoked {
            assert_eq!(rows(&f).await.len(), 0);
        }
        f.data.driver().close().await;
    }
}

#[tokio::test]
async fn captured_data_without_a_batch_selector_is_refused_before_native_write() {
    let f = fixture().await;
    let before = rows(&f).await;
    let body = serde_json::to_vec(&json!([{"key":3,"tick":14,"value":30}])).unwrap();
    let response = common::send(&f.app, data_request(&f.token, body, None)).await;
    assert_eq!(response.status, 409, "{}", response.text());
    assert!(response.text().contains("Main-issued batch selector"));
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 0);
    assert!(!f.model.journal.lock().unwrap().has_pending());
    assert_eq!(rows(&f).await, before);
    f.data.driver().close().await;
}

#[tokio::test]
async fn lost_write_response_stays_undelivered_and_repeated_post_does_not_write_again() {
    let f = fixture().await;
    let write = reserve(
        &f,
        json!([{"key":2,"tick":13,"value":21},{"key":3,"tick":14,"value":30}]),
        None,
    )
    .await;
    let response = f
        .app
        .clone()
        .oneshot(write.request(&f.token))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    // Main completed the native request. Discard the entire original response body
    // before reading its original result or its receipt, as with a lost delivery.
    drop(response);
    let after = rows(&f).await;
    assert_eq!(after.len(), 3);
    assert!(after.iter().any(|row| {
        row["key"] == KValue::Int(2)
            && row["tick"] == KValue::Int(13)
            && row["value"] == KValue::Int(21)
    }));
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    assert!(f.model.journal.lock().unwrap().has_pending());

    let retry = control(&f, &issue(&f, &write.body, None)).await;
    let BatchOutcome::AlreadyCommitted { receipt } = retry.outcome else {
        panic!("Main offered a fresh write after the original committed");
    };
    assert_eq!(receipt.selector, write.selector);
    let replay = post(&f, &write).await;
    assert_eq!(replay.status, 409, "{}", replay.text());
    assert!(replay.text().contains("original batch committed"));
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    assert!(f.model.journal.lock().unwrap().has_pending());
    assert_eq!(rows(&f).await, after);
    f.data.driver().close().await;
}

#[tokio::test]
async fn self_api_restricted_graph_commits_native_binary_row() {
    assert_self_api_binary_row(BinarySchema::Prepared).await;
}

#[tokio::test]
async fn self_api_restricted_graph_commits_new_nullable_binary_column() {
    assert_self_api_binary_row(BinarySchema::Inferred).await;
}

async fn assert_self_api_binary_row(schema: BinarySchema) {
    let f = fixture_with_alias(true, false, None, Some(schema)).await;
    let columns_before = f.data.get_pipe_columns_types(&f.pipe).await.unwrap();
    assert_eq!(
        columns_before.contains_key("payload"),
        schema == BinarySchema::Prepared
    );
    let path = "/pipes/manual:fixture/http_window/None/data?instance_keys=api%3Awriter&new_special_cols=%7B%22payload%22%3A%22bytes%22%7D";
    let body =
        serde_json::to_vec(&json!([{"key":3,"tick":14,"value":30,"payload":"AP8tAA=="}])).unwrap();
    let reply = control(&f, &issue_at(&f, path, &body, None)).await;
    let BatchOutcome::Reserved { selector } = reply.outcome else {
        panic!("Main did not reserve the binary batch");
    };
    let write = Write { body, selector };
    let mut request = write.request(&f.token);
    *request.uri_mut() = path.parse().unwrap();
    let response = common::send(&f.app, request).await;
    assert!(succeeded(&response), "{}", response.text());
    deliver(&f, &write, &response).await;
    let registered = f.data.get_pipe_attributes(&f.pipe).await.unwrap();
    assert_eq!(
        registered["parameters"]
            .get("dtypes")
            .unwrap()
            .get("payload"),
        Some(&Value::Str("bytes".into()))
    );
    let cols = f.data.get_pipe_columns_types(&f.pipe).await.unwrap();
    assert_eq!(cols.get("payload").map(String::as_str), Some("BLOB"));
    let target = f.pipe.target(Some("sqlite")).unwrap();
    let stored = f.data.driver().read_frame(&format!("SELECT key, typeof(payload) AS storage_type, hex(payload) AS stored_hex FROM {target} ORDER BY key")).await.unwrap();
    let stored = meerschaum_connector::pipe_ops::frame_to_docs(&stored);
    let [first, second, new] = stored.as_slice() else {
        panic!("Wrong number of committed binary rows: {stored:?}");
    };
    if schema == BinarySchema::Inferred {
        assert_eq!(first["storage_type"], KValue::Str("null".into()));
        assert_eq!(second["storage_type"], KValue::Str("null".into()));
    }
    assert_eq!(new["key"], KValue::Int(3));
    assert_eq!(new["storage_type"], KValue::Str("blob".into()));
    assert_eq!(new["stored_hex"], KValue::Str("00FF2D00".into()));
    assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.model.schema_effects.load(Ordering::SeqCst),
        usize::from(schema == BinarySchema::Inferred)
    );
    assert!(!f.model.journal.lock().unwrap().has_pending());
    f.data.driver().close().await;
}

#[tokio::test]
#[ignore = "requires owned local InnoDB fixtures"]
async fn self_api_restricted_graph_commits_innodb_append() {
    let mut instances = 0;
    for (variable, flavor) in [
        (
            "MEERSCHAUM_TEST_MYSQL_APPEND_URI",
            meerschaum_connector_sql::Flavor::Mysql,
        ),
        (
            "MEERSCHAUM_TEST_MARIADB_APPEND_URI",
            meerschaum_connector_sql::Flavor::Mariadb,
        ),
    ] {
        let Ok(uri) = std::env::var(variable) else {
            continue;
        };
        instances += 1;
        let spec = ConnectionSpec::from_uri(&uri, Some("data"), Some(flavor)).unwrap();
        assert_eq!(spec.flavor, flavor);
        assert!(matches!(spec.host.as_deref(), Some("127.0.0.1" | "::1")));
        assert!(spec.port.is_some_and(|port| port > 1024 && port != 3306));
        assert!(
            spec.database
                .as_deref()
                .is_some_and(|db| db.starts_with("mrsm_innodb_append_"))
        );
        assert!(spec.username.as_deref().is_some_and(|user| user != "root"));
        let f =
            fixture_for_database(true, false, None, Some(BinarySchema::Prepared), Some(spec)).await;
        let path = format!(
            "/pipes/manual:fixture/{}/None/data?instance_keys=api%3Awriter&check_existing=false",
            f.pipe.metric_key()
        );
        let body =
            serde_json::to_vec(&json!([{"key":3,"tick":14,"value":30,"payload":"AP8tAA=="}]))
                .unwrap();
        let before = f.data.get_pipe_attributes(&f.pipe).await.unwrap();
        let reply = control(&f, &issue_at(&f, &path, &body, None)).await;
        let BatchOutcome::Reserved { selector } = reply.outcome else {
            panic!("Main did not reserve the InnoDB append");
        };
        let write = Write { body, selector };
        let mut request = write.request(&f.token);
        *request.uri_mut() = path.parse().unwrap();
        let response = common::send(&f.app, request).await;
        assert!(succeeded(&response), "{flavor}: {}", response.text());
        deliver(&f, &write, &response).await;
        let stored = rows(&f).await;
        assert_eq!(stored.len(), 3);
        let posted = stored
            .iter()
            .find(|row| row["key"] == KValue::Int(3))
            .unwrap();
        assert_eq!(posted["payload"], KValue::Bytes(vec![0, 255, 45, 0]));
        assert_eq!(f.data.get_pipe_attributes(&f.pipe).await.unwrap(), before);
        assert_eq!(f.model.prepares.load(Ordering::SeqCst), 1);
        assert_eq!(f.model.schema_effects.load(Ordering::SeqCst), 0);
        assert!(!f.model.journal.lock().unwrap().has_pending());
        f.data.driver().close().await;
    }
    assert!(
        instances > 0,
        "An owned loopback MySQL or MariaDB endpoint is required"
    );
    if std::env::var_os("MRSM_TEST_ISOLATED").as_deref() == Some(std::ffi::OsStr::new("1")) {
        assert_eq!(
            instances, 2,
            "The isolated CI lane must exercise both InnoDB flavors"
        );
    }
}
