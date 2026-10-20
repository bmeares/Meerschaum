//! Wave 18 (DECISIONS item 26, GAP-002): removing a pipe also forgets the engine state it left
//! on other instances: its ledger entries and the cursors in which it is the parent, written onto
//! the instances of its cross-instance children. The home instance finds them through
//! remote-state markers; an instance that cannot be reached keeps its marker and is reported.

mod common;

use std::sync::Arc;

use common::*;
use meerschaum_connector::pipe_ops::{forget_remote_engine_state, remote_state_instances};
use meerschaum_connector::{InstanceConnector, InstanceProvider};
use meerschaum_core::Pipe;
use meerschaum_engine::{
    EdgeCursor, InstanceState, PipeRef, StateSource, StateWriter, StaticRegistry, SyncArgs,
};

const OTHER: &str = "sql:other";

struct Other(Arc<dyn InstanceConnector>);

impl InstanceProvider for Other {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == OTHER).then(|| self.0.clone())
    }
}

fn parent_params() -> serde_json::Value {
    serde_json::json!({
        "sql": "SELECT ts, id, v FROM src",
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    })
}

type Fixture = (Env, Arc<InstanceState>, Pipe, PipeRef, PipeRef, Other);

async fn setup(tag: &str) -> Fixture {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    let parent = env.register("w18_parent", &parent_params()).await;
    let other = connect(&env.dir.join("other.db")).await;
    let other_instance: Arc<dyn InstanceConnector> = other.clone();
    env.conn
        .set_instance_provider(Arc::new(Other(other_instance.clone())));
    let provider = Other(other_instance.clone());
    let main: Arc<dyn InstanceConnector> = env.conn.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, main)
            .with_instance(OTHER, other_instance),
    );
    let state = Arc::new(InstanceState::new(registry, INSTANCE));
    let parent_ref = PipeRef::from_pipe(&parent);
    // The child lives on the other instance.
    let child = Pipe::builder(INSTANCE, "w18_child")
        .instance(OTHER)
        .build()
        .unwrap();
    (
        env,
        state,
        parent,
        parent_ref,
        PipeRef::from_pipe(&child),
        provider,
    )
}

fn cursor(parent: &PipeRef, child: &PipeRef) -> EdgeCursor {
    EdgeCursor {
        parent_ref: parent.clone(),
        child_ref: child.clone(),
        seq: 1,
        parent_watermark_at_cursor: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_parent_forgets_its_state_on_the_child_instances() {
    let (env, state, parent, parent_ref, child_ref, _provider) = setup("w18_remote").await;
    // The parent has data (so it can be dropped) and state on both instances.
    let res = env
        .engine()
        .sync_pipe(&parent, None, &SyncArgs::default())
        .await;
    assert!(res.success, "{}", res.message);
    state
        .append_ledger(OTHER, &parent_ref, vec![(None, None)])
        .await
        .unwrap();
    state
        .put_cursors(OTHER, vec![cursor(&parent_ref, &child_ref)])
        .await
        .unwrap();
    assert_eq!(state.latest_seq(OTHER, &parent_ref).await.unwrap(), Some(1));
    assert!(
        state
            .cursor(OTHER, &parent_ref, &child_ref)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        remote_state_instances(env.conn.as_ref(), &parent).await,
        vec![OTHER.to_string()]
    );

    // Through the trait: dropping a pipe also forgets its engine state.
    let conn: &dyn InstanceConnector = env.conn.as_ref();
    let dropped = conn.drop_pipe(&parent).await;
    assert!(dropped.success, "{}", dropped.message);
    // Its own instance, and the other one, hold nothing of it any more.
    assert_eq!(state.latest_seq(INSTANCE, &parent_ref).await.unwrap(), None);
    assert_eq!(state.latest_seq(OTHER, &parent_ref).await.unwrap(), None);
    assert!(
        state
            .cursor(OTHER, &parent_ref, &child_ref)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        remote_state_instances(env.conn.as_ref(), &parent)
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_instance_is_reported_and_keeps_its_marker() {
    let (env, state, parent, parent_ref, child_ref, provider) = setup("w18_remote_gone").await;
    // State on an instance this host cannot reach (the marker is written first; the write fails).
    let gone = PipeRef::from_pipe(
        &Pipe::builder(INSTANCE, "w18_child_gone")
            .instance("sql:gone")
            .build()
            .unwrap(),
    );
    let _ = state
        .put_cursors("sql:gone", vec![cursor(&parent_ref, &gone)])
        .await;
    state
        .put_cursors(OTHER, vec![cursor(&parent_ref, &child_ref)])
        .await
        .unwrap();
    let mut instances = remote_state_instances(env.conn.as_ref(), &parent).await;
    instances.sort();
    assert_eq!(instances, vec!["sql:gone".to_string(), OTHER.to_string()]);

    let res = forget_remote_engine_state(env.conn.as_ref(), &provider, &parent).await;
    assert!(!res.success);
    assert!(res.message.contains("sql:gone"), "{}", res.message);
    assert_eq!(
        remote_state_instances(env.conn.as_ref(), &parent).await,
        vec!["sql:gone".to_string()]
    );
}
