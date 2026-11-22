use super::*;
use crate::graph::{GraphOptions, GraphWarning};
use crate::node::PipeRecord;
use crate::selection::{Depth, Pipeline, Selection};
use crate::state::{MemoryState, MemoryStateData};
use chrono::{TimeZone, Utc};
use meerschaum_core::Value;

#[test]
fn chunk_budget_is_per_pipe() {
    let axis = Axis::int("seq", None, 1, 0);
    let ranges = [
        Interval::new(Some(1), Some(4)),
        Interval::new(Some(10), Some(13)),
    ];
    let mut warnings = Vec::new();
    let chunks = chunks_for(&axis, &ranges, 4, &mut warnings, None);
    assert!(
        chunks.len() <= 4,
        "allocated {} chunks for a budget of 4",
        chunks.len()
    );
    for position in [1, 2, 3, 10, 11, 12] {
        assert!(chunks.iter().any(|chunk| {
            chunk
                .begin
                .as_ref()
                .and_then(|value| axis.pos(value))
                .is_none_or(|begin| begin <= position)
                && chunk
                    .end
                    .as_ref()
                    .and_then(|value| axis.pos(value))
                    .is_none_or(|end| position < end)
        }));
    }
    assert!(!warnings.is_empty());
}

#[test]
fn chunk_budget_covers_many_disjoint_ranges() {
    let axis = Axis::int("seq", None, 1, 0);
    let ranges: Vec<_> = (0..20)
        .map(|index| Interval::new(Some(index * 10), Some(index * 10 + 1)))
        .collect();
    let mut warnings = Vec::new();
    let chunks = chunks_for(&axis, &ranges, 4, &mut warnings, None);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].begin, Some(Bound::Int(0)));
    assert_eq!(chunks[0].end, Some(Bound::Int(191)));
    assert!(!warnings.is_empty());
}

#[test]
fn chunk_budget_handles_extreme_integer_bounds_without_expanding() {
    let axis = Axis::int("seq", None, 1, 0);
    let mut warnings = Vec::new();
    let chunks = chunks_for(
        &axis,
        &[Interval::new(Some(i64::MIN), Some(i64::MAX))],
        16,
        &mut warnings,
        None,
    );
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].begin, Some(Bound::Int(i64::MIN)));
    assert_eq!(chunks[0].end, Some(Bound::Int(i64::MAX)));
    assert!(!warnings.is_empty());
}

#[test]
fn chunk_budget_preserves_small_aligned_boundaries() {
    let axis = Axis::int("seq", None, 3, 0);
    for (begin, end) in [(-8, 5), (0, 0), (0, 5), (4, 4), (2, 11)] {
        let expected = get_chunk_bounds(Some(begin), Some(end), true, 3, true, false, None, None)
            .unwrap()
            .into_iter()
            .map(|(begin, end)| to_span(&axis, &Interval::new(begin, end)))
            .collect::<Vec<_>>();
        let actual = chunks_for(
            &axis,
            &[Interval::new(Some(begin), Some(end))],
            16,
            &mut Vec::new(),
            None,
        );
        assert_eq!(actual, expected);
    }
}

fn rec(metric: &str, json: &str) -> PipeRecord {
    PipeRecord::new("sql:main", "sql:main", metric, None)
        .with_parameters(Value::from_json_str(json).unwrap().to_map())
}

fn r(metric: &str) -> PipeRef {
    PipeRef::new("sql:main", "sql:main", metric, None)
}

fn dt(day: u32, hour: u32) -> Bound {
    Bound::Datetime(Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 0).unwrap())
}

async fn plan_for(
    records: Vec<PipeRecord>,
    state: MemoryStateData,
    target: &str,
    options: PlanOptions,
) -> Plan {
    let graph = Graph::build(records, &GraphOptions::default());
    let sub = Pipeline::new(Selection::refs([r(target)]).with_parents(Some(Depth::All)))
        .resolve(&graph)
        .unwrap();
    plan(&graph, &sub, &MemoryState::new(state), &options).await
}

fn pair() -> Vec<PipeRecord> {
    vec![
        rec(
            "p",
            r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_days": 1}}"#,
        ),
        rec(
            "c",
            r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_days": 1},
                "fetch": {"backtrack_minutes": 0}, "parents": ["Pipe('sql:main', 'p')"]}"#,
        ),
    ]
}

#[tokio::test]
async fn watermark_fallback_without_a_cursor() {
    // First engine run, the child is behind: incremental from its own watermark.
    let mut state = MemoryStateData::default();
    state.watermarks.insert(r("p"), Some(dt(12, 6)));
    state.watermarks.insert(r("c"), Some(dt(12, 3)));
    let p = plan_for(pair(), state.clone(), "c", PlanOptions::default()).await;
    let c = p.node(&r("c")).unwrap();
    assert_eq!(c.action, Action::Incremental);
    assert_eq!(
        c.bounds,
        Some(Bounds {
            begin: Begin::At(dt(12, 3)),
            end: Some(dt(12, 6))
        })
    );
    // The child is level with its parent: skip.
    state.watermarks.insert(r("c"), Some(dt(12, 6)));
    let p = plan_for(pair(), state, "c", PlanOptions::default()).await;
    assert_eq!(p.node(&r("c")).unwrap().action, Action::Skip);
}

#[tokio::test]
async fn user_bounds_clamp_and_roots_take_them_verbatim() {
    let mut state = MemoryStateData::default();
    state.watermarks.insert(r("p"), Some(dt(12, 6)));
    state.watermarks.insert(r("c"), Some(dt(10, 0)));
    let options = PlanOptions {
        begin: Some(dt(11, 0)),
        end: Some(dt(12, 0)),
        ..PlanOptions::default()
    };
    let p = plan_for(pair(), state.clone(), "c", options.clone()).await;
    assert_eq!(
        p.node(&r("p")).unwrap().bounds,
        Some(Bounds {
            begin: Begin::At(dt(11, 0)),
            end: Some(dt(12, 0))
        })
    );
    let c = p.node(&r("c")).unwrap();
    assert_eq!(c.action, Action::Incremental);
    assert_eq!(
        c.bounds,
        Some(Bounds {
            begin: Begin::At(dt(11, 0)),
            end: Some(dt(12, 0))
        })
    );
    assert_eq!(c.chunks.len(), 1);
    // Requested bounds entirely past the window: nothing to do.
    let options = PlanOptions {
        begin: Some(dt(13, 0)),
        ..PlanOptions::default()
    };
    let p = plan_for(pair(), state, "c", options).await;
    assert_eq!(p.node(&r("c")).unwrap().action, Action::Skip);
}

#[tokio::test]
async fn a_columns_change_is_a_definition_change() {
    let old =
        Value::from_json_str(r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 1"}"#).unwrap();
    let mut state = MemoryStateData::default();
    state.definition_hashes.insert(
        r("p"),
        crate::ledger::definition_hash(old.as_map().unwrap()),
    );
    let options = PlanOptions {
        yes: true,
        ..PlanOptions::default()
    };
    // The same definition: no rebuild.
    let same = vec![rec(
        "p",
        r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 1"}"#,
    )];
    let p = plan_for(same, state.clone(), "p", options.clone()).await;
    assert_ne!(p.nodes[0].action, Action::Rebuild);
    // The datetime column was renamed: rebuild.
    let renamed = vec![rec(
        "p",
        r#"{"columns": {"datetime": "time"}, "sql": "SELECT 1"}"#,
    )];
    let p = plan_for(renamed, state.clone(), "p", options.clone()).await;
    assert_eq!(p.nodes[0].action, Action::Rebuild);
    assert!(
        p.nodes[0].reason.contains("columns"),
        "{}",
        p.nodes[0].reason
    );
    // An rc1 hash (dtypes, fetch, sql only) of an unchanged definition is a baseline.
    let mut legacy = MemoryStateData::default();
    legacy.definition_hashes.insert(
        r("p"),
        crate::ledger::legacy_definition_hash(old.as_map().unwrap()),
    );
    let same = vec![rec(
        "p",
        r#"{"columns": {"datetime": "time"}, "sql": "SELECT 1"}"#,
    )];
    let p = plan_for(same, legacy, "p", options).await;
    assert_ne!(p.nodes[0].action, Action::Rebuild);
}

#[tokio::test]
async fn decline_and_confirm() {
    let records = vec![rec("p", r#"{"sql": "SELECT 1"}"#)];
    let mut state = MemoryStateData::default();
    state.definition_hashes.insert(r("p"), "v1:old".into());
    let options = PlanOptions {
        interactive: true,
        ..PlanOptions::default()
    };
    let mut p = plan_for(records.clone(), state.clone(), "p", options.clone()).await;
    assert!(p.needs_confirmation());
    assert!(p.to_string().contains("rebuild?"));
    p.decline_rebuilds();
    assert_eq!(p.nodes[0].action, Action::RebuildPending);
    let mut p = plan_for(records.clone(), state.clone(), "p", options).await;
    p.confirm_rebuilds();
    assert!(!p.needs_confirmation());
    assert_eq!(p.nodes[0].action, Action::Rebuild);
    assert_eq!(
        p.nodes[0].bounds,
        Some(Bounds {
            begin: Begin::Unbounded,
            end: None
        })
    );
    // A hash from another scheme is not a change.
    state.definition_hashes.insert(r("p"), "v0:old".into());
    let p = plan_for(records, state, "p", PlanOptions::default()).await;
    assert_eq!(p.nodes[0].action, Action::Root);
}

#[test]
fn begin_serde() {
    for (b, json) in [
        (Begin::Default, r#""""#),
        (Begin::Unbounded, "null"),
        (Begin::At(Bound::Int(5)), "5"),
    ] {
        assert_eq!(serde_json::to_string(&b).unwrap(), json);
        assert_eq!(serde_json::from_str::<Begin>(json).unwrap(), b);
    }
    assert!(serde_json::from_str::<Begin>(r#""x""#).is_err());
}

#[test]
fn integer_parent_without_a_unit_warns() {
    let graph = Graph::build(
        [
            rec(
                "landing",
                r#"{"columns": {"datetime": "ts_s"}, "dtypes": {"ts_s": "int"}}"#,
            ),
            rec(
                "clean",
                r#"{"columns": {"datetime": "ts"}, "parents": ["Pipe('sql:main', 'landing')"]}"#,
            ),
        ],
        &GraphOptions::default(),
    );
    assert!(matches!(
        graph.warnings(),
        [GraphWarning::AxisUnitInferred { unit, .. }] if unit == "microsecond"
    ));
}
