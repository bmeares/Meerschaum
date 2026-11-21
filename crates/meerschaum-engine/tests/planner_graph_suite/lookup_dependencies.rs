//! Lookup inputs affect query results without imposing event-time completeness bounds.

use chrono::{TimeDelta, TimeZone, Utc};
use meerschaum_core::{Value, success::Bound};
use meerschaum_engine::graph::{EdgeRole, Graph, GraphOptions};
use meerschaum_engine::ledger::EdgeCursor;
use meerschaum_engine::node::{PipeRecord, PipeRef};
use meerschaum_engine::plan::{Action, Plan, PlanOptions, plan};
use meerschaum_engine::selection::{Depth, Pipeline, Selection};
use meerschaum_engine::state::{MemoryState, MemoryStateData};

fn reference(name: &str) -> PipeRef {
    PipeRef::new("sql:main", "sql:main", name, None)
}

fn at(day: u32, hour: u32) -> Bound {
    Bound::Datetime(Utc.with_ymd_and_hms(2024, 1, day, hour, 0, 0).unwrap())
}

fn record(name: &str, parameters: &str) -> PipeRecord {
    PipeRecord::new("sql:main", "sql:main", name, None)
        .with_parameters(Value::from_json_str(parameters).unwrap().to_map())
}

fn records(lookup: bool) -> Vec<PipeRecord> {
    let mut records = vec![record("events", r#"{"columns":{"datetime":"ts"}}"#)];
    let parents = if lookup {
        r#"["Pipe('sql:main', 'events')", {"connector":"sql:main","metric":"catalog","role":"lookup"}]"#
    } else {
        r#"["Pipe('sql:main', 'events')"]"#
    };
    if lookup {
        records.push(record("catalog", r#"{"columns":{"datetime":"ts"}}"#));
    }
    records.push(record("result", &format!(
        r#"{{"columns":{{"datetime":"ts"}},"verify":{{"chunk_days":30}},"fetch":{{"backtrack_minutes":60}},"parents":{parents}}}"#
    )));
    records
}

async fn planned(state: MemoryStateData, lookup: bool) -> Plan {
    planned_records(state, records(lookup)).await
}

async fn planned_records(state: MemoryStateData, records: Vec<PipeRecord>) -> Plan {
    let graph = Graph::build(records, &GraphOptions::default());
    assert!(graph.warnings().is_empty(), "{:?}", graph.warnings());
    let selection = Selection::refs([reference("result")]).with_parents(Some(Depth::All));
    let subgraph = Pipeline::new(selection).resolve(&graph).unwrap();
    plan(
        &graph,
        &subgraph,
        &MemoryState::new(state),
        &PlanOptions::default(),
    )
    .await
}

fn state() -> MemoryStateData {
    MemoryStateData {
        watermarks: [
            (reference("events"), Some(at(20, 12))),
            (reference("catalog"), Some(at(1, 0))),
            (reference("result"), Some(at(20, 11))),
        ]
        .into(),
        ..Default::default()
    }
}

fn cursor(parent: &str, role: EdgeRole) -> EdgeCursor {
    EdgeCursor {
        parent_ref: reference(parent),
        child_ref: reference("result"),
        seq: 0,
        parent_watermark_at_cursor: Some(if parent == "events" {
            at(20, 12)
        } else {
            at(1, 0)
        }),
        role,
    }
}

fn bounded_records() -> Vec<PipeRecord> {
    vec![
        record("events", r#"{"columns":{"datetime":"ts"}}"#),
        record("catalog", r#"{"columns":{"datetime":"ts"}}"#),
        record(
            "result",
            r#"{"columns":{"datetime":"ts"},"verify":{"chunk_days":1},"fetch":{"backtrack_minutes":60},"parents":["Pipe('sql:main','events')",{"connector":"sql:main","metric":"catalog","role":"lookup","lookback_days":1,"lookahead_minutes":30}]}"#,
        ),
    ]
}

fn bounded_state() -> MemoryStateData {
    let mut state = state();
    state
        .watermarks
        .insert(reference("catalog"), Some(at(19, 0)));
    let mut lookup_cursor = cursor("catalog", EdgeRole::Lookup);
    lookup_cursor.parent_watermark_at_cursor = Some(at(19, 0));
    state.cursors.insert(
        "sql:main".into(),
        vec![cursor("events", EdgeRole::Data), lookup_cursor],
    );
    state
}

fn change_lookup(state: &mut MemoryStateData, begin: Bound, end: Bound) {
    state.ledger.insert(
        "sql:main".into(),
        vec![meerschaum_engine::ledger::LedgerEntry {
            pipe_ref: reference("catalog"),
            seq: 1,
            begin: Some(begin),
            end: Some(end),
            created: Utc::now(),
        }],
    );
}

#[tokio::test]
async fn bounded_lookup_changes_near_the_tail_stay_incremental() {
    let mut state = bounded_state();
    state
        .watermarks
        .insert(reference("catalog"), Some(at(20, 11)));
    change_lookup(&mut state, at(20, 11), at(20, 12));
    let plan = planned_records(state, bounded_records()).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Incremental, "{child:?}");
    assert_eq!(
        child.bounds.unwrap().end,
        Some(at(20, 12)),
        "The lookup must not cap the data stream at 11:00."
    );
}

#[tokio::test]
async fn bounded_lookup_historical_changes_replay_only_the_dependency_window() {
    let mut state = bounded_state();
    change_lookup(&mut state, at(10, 12), at(10, 13));
    let plan = planned_records(state, bounded_records()).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert_eq!(child.ranges[0].begin, Some(at(10, 0)));
    assert_eq!(child.ranges.last().unwrap().end, Some(at(12, 0)));
    assert!(
        child
            .chunks
            .iter()
            .all(|chunk| chunk.begin.is_some() && chunk.end.is_some())
    );
}

#[tokio::test]
async fn a_bounded_lookup_without_a_cursor_still_checks_full_history() {
    let mut state = state();
    change_lookup(&mut state, at(10, 12), at(10, 13));
    let plan = planned_records(state, bounded_records()).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert!(child.ranges[0].begin.is_none());
}

#[tokio::test]
async fn a_bounded_lookup_with_unknown_changed_ranges_checks_full_history() {
    let mut state = bounded_state();
    state
        .watermarks
        .insert(reference("catalog"), Some(at(20, 11)));
    let plan = planned_records(state, bounded_records()).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert!(child.ranges[0].begin.is_none());
}

#[tokio::test]
async fn old_lookup_timestamp_does_not_stop_recent_event_data() {
    let plan = planned(state(), true).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert_eq!(child.bounds.unwrap().end, Some(at(20, 12)));
    assert!(
        child
            .consumes
            .iter()
            .any(|c| c.parent_ref == reference("catalog") && c.role == EdgeRole::Lookup)
    );
}

#[tokio::test]
async fn consumed_lookup_and_event_inputs_do_not_sync_again() {
    let mut state = state();
    state.cursors.insert(
        "sql:main".into(),
        vec![
            cursor("events", EdgeRole::Data),
            cursor("catalog", EdgeRole::Lookup),
        ],
    );
    let plan = planned(state, true).await;
    assert_eq!(
        plan.node(&reference("result")).unwrap().action,
        Action::Skip
    );
}

#[tokio::test]
async fn changing_a_data_dependency_to_lookup_rechecks_its_history() {
    let mut state = state();
    state.cursors.insert(
        "sql:main".into(),
        vec![
            cursor("events", EdgeRole::Data),
            cursor("catalog", EdgeRole::Data),
        ],
    );
    let plan = planned(state, true).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert_eq!(child.bounds.unwrap().end, Some(at(20, 12)));
}

#[tokio::test]
async fn a_changed_lookup_invalidates_old_results_without_capping_them() {
    let mut state = state();
    state.cursors.insert(
        "sql:main".into(),
        vec![
            cursor("events", EdgeRole::Data),
            cursor("catalog", EdgeRole::Lookup),
        ],
    );
    state.ledger.insert(
        "sql:main".into(),
        vec![meerschaum_engine::ledger::LedgerEntry {
            pipe_ref: reference("catalog"),
            seq: 1,
            begin: Some(at(1, 0)),
            end: Some(at(1, 1)),
            created: Utc::now(),
        }],
    );
    let plan = planned(state, true).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Backfill);
    assert_eq!(child.ranges[0].begin, None);
    assert_eq!(child.bounds.unwrap().end, Some(at(20, 12)));
}

#[tokio::test]
async fn new_tail_data_stays_incremental_with_a_wide_chunk_grid() {
    let mut state = state();
    state
        .cursors
        .insert("sql:main".into(), vec![cursor("events", EdgeRole::Data)]);
    state.ledger.insert(
        "sql:main".into(),
        vec![meerschaum_engine::ledger::LedgerEntry {
            pipe_ref: reference("events"),
            seq: 1,
            begin: Some(at(20, 12)),
            end: Some(Bound::Datetime(
                Utc.with_ymd_and_hms(2024, 1, 20, 12, 0, 0).unwrap() + TimeDelta::microseconds(1),
            )),
            created: Utc::now(),
        }],
    );
    let plan = planned(state, false).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Incremental);
    assert_eq!(child.bounds.unwrap().end, Some(at(20, 12)));
    assert_eq!(child.chunks.len(), 1);
    assert_eq!(child.chunks[0].begin, Some(at(20, 10)));
    assert_eq!(child.chunks[0].end, Some(at(20, 12)));
}

#[test]
fn legacy_cursors_keep_their_data_role_and_wire_shape() {
    let value = serde_json::json!({"parent_ref":"sql:main|sql:main|events|None", "child_ref":"sql:main|sql:main|result|None", "seq":2,"parent_watermark_at_cursor":null});
    let cursor: EdgeCursor = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(cursor.role, EdgeRole::Data);
    assert_eq!(serde_json::to_value(cursor).unwrap(), value);
}

#[tokio::test]
async fn sources_with_only_lookup_inputs_keep_their_polling_behavior() {
    let mut state = state();
    state
        .cursors
        .insert("sql:main".into(), vec![cursor("catalog", EdgeRole::Lookup)]);
    let records = vec![
        record("catalog", r#"{"columns":{"datetime":"ts"}}"#),
        record(
            "result",
            r#"{"columns":{"datetime":"ts"},"parents":[{"connector":"sql:main","metric":"catalog","role":"lookup"}]}"#,
        ),
    ];
    let plan = planned_records(state, records).await;
    let child = plan.node(&reference("result")).unwrap();
    assert_eq!(child.action, Action::Root);
    assert!(child.reason.contains("lookup"));
}

#[test]
fn an_explicit_lookup_role_survives_an_implicit_sql_reference() {
    let graph = Graph::build(
        [
            record("catalog", r#"{}"#),
            record(
                "result",
                r#"{"parents":[{"connector":"sql:main","metric":"catalog","role":"lookup"}],"sql":"SELECT * FROM {{ Pipe('sql:main', 'catalog') }}"}"#,
            ),
        ],
        &GraphOptions::default(),
    );
    assert!(graph.warnings().is_empty(), "{:?}", graph.warnings());
    assert_eq!(graph.edges().len(), 1);
    assert_eq!(graph.edges()[0].options.role, EdgeRole::Lookup);
    assert!(graph.edges()[0].sources.parents && graph.edges()[0].sources.template);
}

#[test]
fn lookup_roles_may_be_declared_on_the_inputs_children() {
    let graph = Graph::build(
        [
            record(
                "catalog",
                r#"{"children":[{"connector":"sql:main","metric":"result","role":"lookup"}]}"#,
            ),
            record(
                "result",
                r#"{"sql":"SELECT * FROM {{ Pipe('sql:main', 'catalog') }}"}"#,
            ),
        ],
        &GraphOptions::default(),
    );
    assert!(graph.warnings().is_empty(), "{:?}", graph.warnings());
    assert_eq!(graph.edges().len(), 1);
    assert_eq!(graph.edges()[0].options.role, EdgeRole::Lookup);
    assert!(graph.edges()[0].sources.children && graph.edges()[0].sources.template);
}
