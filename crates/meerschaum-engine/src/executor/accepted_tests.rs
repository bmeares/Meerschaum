//! Direct execute regression for the optional PlanAuthority consumer seam.
//! Exercises the real scheduler without external services or a Main issuer.

use super::*;
use crate::events::VecSink;
use crate::schedule::EwmaDurations;
use crate::state::{MemoryState, MemoryStateData};
use crate::{Begin, Depth, GraphOptions, Pipeline, Selection};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct ScriptedAuthority {
    issued: BTreeMap<PipeRef, PlannedNode>,
    ready: Mutex<Vec<PipeRef>>,
    deny_child: bool,
}
#[meerschaum_connector::async_trait]
impl crate::PlanAuthority for ScriptedAuthority {
    async fn initial(&self, _: &Plan) -> Result<Plan, String> {
        Err("Direct execute already received its initial plan".into())
    }
    async fn ready(&self, reference: &PipeRef) -> Result<PlannedNode, String> {
        self.ready.lock().unwrap().push(reference.clone());
        if self.deny_child && reference.parts().metric_key == "child" {
            return Err("Main did not admit the child after its parent completed".into());
        }
        self.issued
            .get(reference)
            .cloned()
            .ok_or_else(|| "No issued Main node".into())
    }
}
struct PipeliningExecutor {
    authority: Arc<ScriptedAuthority>,
    calls: Mutex<Vec<SyncRequest>>,
    speculative: AtomicBool,
    without_ready: AtomicBool,
}
impl SyncExecutor for PipeliningExecutor {
    fn supports_chunk_pipelining(&self) -> bool {
        true
    }
    async fn sync(&self, request: SyncRequest) -> SyncResult {
        // The public direct execute entry point must not hand a speculative
        // progress capability to an executor when Main ready admission is used.
        self.speculative
            .fetch_or(request.progress.is_some(), Ordering::SeqCst);
        self.without_ready.fetch_or(
            !self
                .authority
                .ready
                .lock()
                .unwrap()
                .contains(&request.pipe_ref),
            Ordering::SeqCst,
        );
        self.calls.lock().unwrap().push(request);
        SyncResult::new(true, "Main-admitted node ran")
    }
}

#[tokio::test]
async fn direct_execute_main_ready_admission_disables_requested_speculation_and_refusal_has_no_child_sync()
 {
    // Caller explicitly requests chunk_pipelining and executor advertises support.
    // Both a success and a Main refusal must still pass through real Dispatcher
    // ready admission. This catches the direct-execute path Engine::run cannot fix.
    for deny_child in [false, true] {
        let parent = PipeRef::new("sql:main", "sql:main", "parent", None);
        let child = PipeRef::new("sql:main", "sql:main", "child", None);
        let params = |child: bool| {
            let mut map = meerschaum_core::Value::from_json_str(
                &serde_json::json!({
                    "columns":{"datetime":"tick"},"dtypes":{"tick":"int64"}
                })
                .to_string(),
            )
            .unwrap()
            .to_map();
            if child {
                map.insert(
                    "parents".into(),
                    meerschaum_core::Value::from_json_str(
                        r#"[{"connector":"sql:main","metric":"parent"}]"#,
                    )
                    .unwrap(),
                );
            }
            map
        };
        let graph = Graph::build(
            [
                PipeRecord::new("sql:main", "sql:main", "parent", None)
                    .with_parameters(params(false)),
                PipeRecord::new("sql:main", "sql:main", "child", None)
                    .with_parameters(params(true)),
            ],
            &GraphOptions::default(),
        );
        let subgraph =
            Pipeline::new(Selection::refs([child.clone()]).with_parents(Some(Depth::All)))
                .resolve(&graph)
                .unwrap();
        assert_eq!(
            graph
                .parent_edges(&child)
                .map(|edge| &edge.parent)
                .collect::<Vec<_>>(),
            vec![&parent]
        );
        assert_eq!(
            subgraph.order.len(),
            2,
            "The regression must exercise a real parent-child scheduling edge"
        );
        let state = MemoryState::new(MemoryStateData::default());
        let local = crate::plan::plan(&graph, &subgraph, &state, &PlanOptions::default()).await;
        let issued = local
            .nodes
            .iter()
            .map(|node| {
                let mut node = node.clone();
                node.bounds = Some(Bounds {
                    begin: Begin::At(Bound::Int(5)),
                    end: Some(Bound::Int(10)),
                });
                node.action = if node.pipe_ref == parent {
                    Action::Root
                } else {
                    Action::Incremental
                };
                node.provisional = false;
                node.clear = false;
                node.requires_confirmation = false;
                node.ranges.clear();
                node.chunks.clear();
                (node.pipe_ref.clone(), node)
            })
            .collect();
        let authority = Arc::new(ScriptedAuthority {
            issued,
            ready: Mutex::new(Vec::new()),
            deny_child,
        });
        let executor = Arc::new(PipeliningExecutor {
            authority: authority.clone(),
            calls: Mutex::new(Vec::new()),
            speculative: AtomicBool::new(false),
            without_ready: AtomicBool::new(false),
        });
        let leases = Arc::new(MemoryLeases::default());
        let durations = EwmaDurations::new(0.5, Duration::from_secs(1));
        let options = ExecutorOptions {
            plan_authority: Some(authority.clone()),
            chunk_pipelining: true,
            ..Default::default()
        };
        let record = execute(
            ExecuteContext {
                graph: &graph,
                subgraph: &subgraph,
                state: &state,
                executor: executor.clone(),
                leases,
                sink: Arc::new(VecSink::default()),
                durations: &durations,
            },
            local,
            &options,
        )
        .await;
        assert_eq!(
            *authority.ready.lock().unwrap(),
            vec![parent.clone(), child.clone()]
        );
        assert!(
            !executor.without_ready.load(Ordering::SeqCst),
            "A node executed without actual Main ready admission"
        );
        assert!(
            !executor.speculative.load(Ordering::SeqCst),
            "Direct execute exposed speculative progress despite Main plan authority"
        );
        let calls = executor.calls.lock().unwrap();
        assert_eq!(calls.len(), if deny_child { 1 } else { 2 });
        assert!(calls.iter().all(|request| request.bounds
            == Some(Bounds {
                begin: Begin::At(Bound::Int(5)),
                end: Some(Bound::Int(10))
            })));
        assert_eq!(record.run.success, Some(!deny_child));
        if deny_child {
            assert_eq!(
                record.node(&child).unwrap().status,
                NodeStatus::UpstreamFailed
            );
        }
    }
}

#[derive(Debug)]
struct RetryAuthority {
    lost_delivery: bool,
    issued: PlannedNode,
    calls: AtomicUsize,
}
#[meerschaum_connector::async_trait]
impl crate::PlanAuthority for RetryAuthority {
    fn node_delivery(
        &self,
        node: &PipeRef,
    ) -> Option<meerschaum_connector::batch_delivery::NodeDelivery> {
        use meerschaum_connector::batch_delivery::{GraphBinding, NodeDelivery};
        let actor = || GraphBinding {
            grant: uuid::Uuid::new_v4(),
            launch: uuid::Uuid::new_v4(),
            revision: 1,
        };
        self.lost_delivery
            .then(|| NodeDelivery::new(actor(), actor(), node.as_str().into()))
    }
    async fn initial(&self, _: &Plan) -> Result<Plan, String> {
        Err("Direct execute has its initial plan".into())
    }
    async fn ready(&self, reference: &PipeRef) -> Result<PlannedNode, String> {
        assert_eq!(reference, &self.issued.pipe_ref);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(self.issued.clone())
        } else {
            Err("Main refused the retry after the original source changed".into())
        }
    }
}
struct FailAttempt(AtomicUsize);
impl SyncExecutor for FailAttempt {
    async fn sync(&self, request: SyncRequest) -> SyncResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some(owner) = request.delivery {
            let mut lease = owner.acquire(request.pipe_ref.as_str()).unwrap();
            assert!(lease.issue().unwrap().is_none());
            // The native writer lost its response; the plugin ignored the failed callback.
            return SyncResult::new(true, "Plugin ignored callback failure");
        }
        SyncResult::new(false, "Transient source failure")
    }
}

#[tokio::test]
async fn direct_execute_retries_require_new_main_ready_admission_before_second_sync() {
    let reference = PipeRef::new("sql:main", "sql:main", "retry", None);
    let parameters = meerschaum_core::Value::from_json_str(
        &serde_json::json!({
            "columns":{"datetime":"tick"},"dtypes":{"tick":"int64"},
            "retry":{"max":1,"backoff_seconds":0}
        })
        .to_string(),
    )
    .unwrap()
    .to_map();
    let graph = Graph::build(
        [PipeRecord::new("sql:main", "sql:main", "retry", None).with_parameters(parameters)],
        &GraphOptions::default(),
    );
    let subgraph = Pipeline::new(Selection::refs([reference.clone()]))
        .resolve(&graph)
        .unwrap();
    let state = MemoryState::new(MemoryStateData::default());
    let local = crate::plan::plan(&graph, &subgraph, &state, &PlanOptions::default()).await;
    let mut issued = local.nodes[0].clone();
    issued.bounds = Some(Bounds {
        begin: Begin::At(Bound::Int(5)),
        end: Some(Bound::Int(10)),
    });
    issued.provisional = false;
    issued.action = Action::Root;
    issued.clear = false;
    issued.requires_confirmation = false;
    for lost_delivery in [false, true] {
        let authority = Arc::new(RetryAuthority {
            lost_delivery,
            issued: issued.clone(),
            calls: AtomicUsize::new(0),
        });
        let executor = Arc::new(FailAttempt(AtomicUsize::new(0)));
        let durations = EwmaDurations::new(0.5, Duration::from_secs(1));
        let options = ExecutorOptions {
            plan_authority: Some(authority.clone()),
            ..Default::default()
        };
        let record = execute(
            ExecuteContext {
                graph: &graph,
                subgraph: &subgraph,
                state: &state,
                executor: executor.clone(),
                leases: Arc::new(MemoryLeases::default()),
                sink: Arc::new(VecSink::default()),
                durations: &durations,
            },
            local.clone(),
            &options,
        )
        .await;
        assert_eq!(
            authority.calls.load(Ordering::SeqCst),
            if lost_delivery { 1 } else { 2 },
            "Only a pre-write failure may dispatch a whole-node retry"
        );
        assert_eq!(
            executor.0.load(Ordering::SeqCst),
            1,
            "A second sync ran after Main refused renewed admission"
        );
        assert_eq!(
            record.node(&reference).unwrap().status,
            if lost_delivery {
                NodeStatus::Failed
            } else {
                NodeStatus::UpstreamFailed
            }
        );
        assert_eq!(record.node(&reference).unwrap().attempts, 1);
        assert_eq!(record.run.success, Some(false));
    }
}
