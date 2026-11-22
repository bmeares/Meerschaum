//! Tests of the run monitor: the event reducer, the Gantt cells and the rendered screens.

use super::*;
use crate::tui::app::Overlay;
use crate::tui::app::tests::{key, screen};
use chrono::TimeZone;
use meerschaum_core::success::SyncResult;

fn t(secs: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 2, 3, 0, 0).unwrap() + chrono::Duration::seconds(secs)
}

fn r(metric: &str) -> PipeRef {
    PipeRef::from_string(format!("sql:local|sql:local|{metric}|None"))
}

fn ev(seq: u64, secs: i64, node: Option<&str>, kind: EventKind) -> Event {
    Event {
        run_id: "run1".into(),
        seq,
        ts: t(secs),
        pipe_ref: node.map(r),
        kind,
    }
}

/// A run: `raw` succeeds over 2 windows, `clean` retries then fails, `daily` is skipped.
pub(crate) fn events() -> Vec<Event> {
    let mut result = SyncResult::new(true, "Inserted 10 rows.");
    result.inserted = 10;
    result.updated = 2;
    vec![
        ev(
            0,
            0,
            None,
            EventKind::RunStarted {
                nodes: 3,
                workers: 2,
                targets: vec![r("daily")],
            },
        ),
        ev(
            1,
            0,
            Some("raw"),
            EventKind::NodePlanned {
                action: Action::Root,
                reason: "no parents".into(),
                provisional: false,
            },
        ),
        ev(
            2,
            0,
            Some("clean"),
            EventKind::NodePlanned {
                action: Action::Incremental,
                reason: "tail".into(),
                provisional: true,
            },
        ),
        ev(
            3,
            1,
            Some("raw"),
            EventKind::NodeStarted {
                action: Action::Root,
                attempt: 1,
            },
        ),
        ev(
            4,
            1,
            Some("raw"),
            EventKind::ChunkStarted {
                chunk: 0,
                begin: None,
                end: None,
                source: ChunkSource::Window,
                rows: None,
                total: Some(2),
            },
        ),
        ev(
            5,
            2,
            Some("raw"),
            EventKind::ChunkFinished {
                chunk: 0,
                success: true,
                message: String::new(),
                source: ChunkSource::Window,
                rows: Some(6),
                inserted: 6,
                updated: 0,
                upserted: 0,
                begin: None,
                end: None,
                duration_ms: Some(1000),
            },
        ),
        ev(
            6,
            3,
            Some("raw"),
            EventKind::ChunkFinished {
                chunk: 1,
                success: true,
                message: String::new(),
                source: ChunkSource::Window,
                rows: Some(6),
                inserted: 4,
                updated: 2,
                upserted: 0,
                begin: None,
                end: None,
                duration_ms: Some(1000),
            },
        ),
        ev(7, 3, Some("raw"), EventKind::NodeFinished { result }),
        ev(
            8,
            4,
            Some("clean"),
            EventKind::NodeStarted {
                action: Action::Incremental,
                attempt: 1,
            },
        ),
        ev(
            9,
            5,
            Some("clean"),
            EventKind::NodeFailed {
                error: "boom\ntrace".into(),
                traceback: None,
                attempt: 1,
                will_retry: true,
            },
        ),
        ev(
            10,
            6,
            Some("clean"),
            EventKind::NodeStarted {
                action: Action::Incremental,
                attempt: 2,
            },
        ),
        ev(
            11,
            7,
            Some("clean"),
            EventKind::NodeFailed {
                error: "boom again".into(),
                traceback: None,
                attempt: 2,
                will_retry: false,
            },
        ),
        ev(
            12,
            7,
            Some("daily"),
            EventKind::NodeSkipped {
                reason: "upstream failed: clean".into(),
            },
        ),
        ev(
            13,
            8,
            None,
            EventKind::RunFinished {
                success: false,
                message: "1 of 3 nodes failed.".into(),
            },
        ),
    ]
}

#[test]
fn reducer_tracks_nodes_chunks_retries_and_the_run() {
    let mut state = RunState::new("run1");
    let all = events();
    for e in &all[..9] {
        assert!(state.apply(e));
    }
    let raw = &state.nodes[&r("raw")];
    assert_eq!(raw.phase, Phase::Succeeded);
    assert_eq!(raw.chunk_progress(), "2/2");
    assert_eq!((raw.inserted, raw.updated), (10, 2));
    assert_eq!(raw.rows, 12);
    assert_eq!(raw.duration_seconds(t(100)), Some(2.0));
    assert_eq!(raw.rows_per_second(t(100)), Some(6.0));
    let clean = &state.nodes[&r("clean")];
    assert_eq!(clean.phase, Phase::Running);
    assert!(clean.provisional);
    // Running durations are measured to `now`.
    assert_eq!(clean.duration_seconds(t(10)), Some(6.0));
    assert_eq!(state.status_name(), "running");
    assert_eq!(state.tally(), (1, 1, 0, 0, 0));

    for e in &all[9..10] {
        state.apply(e);
    }
    assert_eq!(state.nodes[&r("clean")].phase, Phase::Retrying);
    for e in &all[10..] {
        state.apply(e);
    }
    let clean = &state.nodes[&r("clean")];
    assert_eq!(clean.phase, Phase::Failed);
    assert_eq!((clean.attempt, clean.retries), (2, 1));
    assert_eq!(state.nodes[&r("daily")].phase, Phase::UpstreamFailed);
    assert_eq!(state.status_name(), "failed");
    assert_eq!(state.wall_seconds(t(1000)), 8.0);
    assert_eq!(state.tally(), (0, 1, 2, 0, 0));
    // Node order is first appearance.
    let order: Vec<String> = state.nodes.keys().map(short_label).collect();
    assert_eq!(
        order,
        ["sql:local:raw", "sql:local:clean", "sql:local:daily"]
    );
    assert_eq!(state.log.len(), all.len());
}

#[test]
fn reducer_ignores_repeats_and_other_runs() {
    let mut state = RunState::new("run1");
    let all = events();
    for e in &all {
        state.apply(e);
    }
    let snapshot = state.clone();
    for e in &all {
        assert!(!state.apply(e), "repeated seq {}", e.seq);
    }
    let mut other = all[3].clone();
    other.run_id = "run2".into();
    other.seq = 99;
    assert!(!state.apply(&other));
    assert_eq!(state, snapshot);
    assert_eq!(state.last_seq, Some(13));
}

#[test]
fn events_round_trip_through_rows() {
    for e in events() {
        assert_eq!(event_of_row(&e.to_row()), Some(e.clone()));
    }
    let line = event_line(&events()[9]);
    assert_eq!(
        line,
        "03:00:05  sql:local:clean  attempt 1 failed, retrying: boom"
    );
}

#[test]
fn gantt_cells_mark_wait_plan_and_activity() {
    let cells: String = gantt_cells(10.0, 10, Some((2.0, 5.0)), None)
        .into_iter()
        .collect();
    assert_eq!(cells, "wwaaa     ");
    let cells: String = gantt_cells(10.0, 10, None, Some((0.0, 4.0)))
        .into_iter()
        .collect();
    assert_eq!(cells, "pppp      ");
    let cells: String = gantt_cells(10.0, 10, Some((6.0, 6.0)), Some((0.0, 4.0)))
        .into_iter()
        .collect();
    assert_eq!(cells, "ppppwwa   ");
    assert!(gantt_cells(1.0, 0, Some((0.0, 1.0)), None).is_empty());
}

#[test]
fn scroll_keeps_the_selection_visible() {
    assert_eq!(scroll_offset(0, 5, 3), 0);
    assert_eq!(scroll_offset(4, 5, 10), 0);
    assert_eq!(scroll_offset(5, 5, 10), 1);
    assert_eq!(scroll_offset(9, 5, 10), 5);
    assert_eq!(scroll_offset(3, 0, 10), 0);
}

fn monitor() -> RunMonitor {
    let mut m = RunMonitor::new("run1", "sql:local", Source::None);
    for e in events() {
        m.state.apply(&e);
    }
    m.now = Some(t(9));
    m
}

#[test]
fn renders_header_timeline_table_and_log() {
    let mut m = monitor();
    let theme = Theme::plain();
    let rows = screen(&mut m, &Overlay::None, &theme, 100, 30);
    let text = rows.join("\n");
    assert!(rows[0].contains("run run1 on sql:local"), "{text}");
    assert!(rows[1].contains("failed"), "{text}");
    assert!(rows[1].contains("1 ok"), "{text}");
    assert!(text.contains("1 of 3 nodes failed."), "{text}");
    assert!(text.contains("Timeline"), "{text}");
    assert!(text.contains("#"), "ASCII bars: {text}");
    assert!(text.contains("Nodes"), "{text}");
    assert!(text.contains("sql:local:clean"), "{text}");
    assert!(text.contains("2 (1r)"), "retries: {text}");
    assert!(text.contains("2/2"), "chunks: {text}");
    assert!(text.contains("Events"), "{text}");
    assert!(text.contains("run failed: 1 of 3 nodes failed."), "{text}");
    // The selected node is marked by the theme (reverse video in plain mode); details open.
    m.on_key(key(KeyCode::Down));
    assert_eq!(m.selected, 1);
    assert_eq!(m.on_key(key(KeyCode::Enter)), Step::Continue);
    let text = screen(&mut m, &Overlay::None, &theme, 100, 30).join("\n");
    assert!(text.contains("Esc closes"), "{text}");
    assert!(text.contains("boom again"), "{text}");
    assert!(text.contains("1 retried"), "{text}");
    assert_eq!(m.on_key(key(KeyCode::Esc)), Step::Continue);
    assert!(!m.details);
    // Esc without details is the loop's (quit).
    assert_eq!(m.on_key(key(KeyCode::Esc)), Step::Ignored);
    // No detach for a stored run.
    assert_eq!(m.on_key(key(KeyCode::Char('d'))), Step::Ignored);
}

#[test]
fn small_and_resized_terminals_render() {
    let mut m = monitor();
    let theme = Theme {
        color: true,
        unicode: true,
    };
    for (w, h) in [(20, 5), (40, 10), (80, 15), (200, 60), (1, 1)] {
        let rows = screen(&mut m, &Overlay::None, &theme, w, h);
        assert_eq!(rows.len(), h as usize);
    }
    let text = screen(&mut m, &Overlay::None, &theme, 80, 12).join("\n");
    assert!(
        !text.contains("Timeline"),
        "the timeline is dropped first: {text}"
    );
    assert!(text.contains("Nodes"), "{text}");
}

#[test]
fn log_focus_scrolls_and_follows() {
    let mut m = monitor();
    m.on_key(key(KeyCode::Tab));
    assert_eq!(m.focus, Focus::Log);
    m.on_key(key(KeyCode::Up));
    m.on_key(key(KeyCode::PageUp));
    assert_eq!(m.log_scroll, 11);
    let text = screen(&mut m, &Overlay::None, &Theme::plain(), 100, 30).join("\n");
    assert!(text.contains("scrolled 11 up"), "{text}");
    m.on_key(key(KeyCode::End));
    assert_eq!(m.log_scroll, 0);
    m.on_key(key(KeyCode::Home));
    assert_eq!(m.log_scroll, m.state.log.len());
}

#[test]
fn live_source_applies_broadcast_events_and_detaches() {
    let (tx, rx) = tokio::sync::broadcast::channel(16);
    let mut m = RunMonitor::new("run1", "sql:local", Source::live(rx, None));
    assert!(!m.tick());
    for e in events().into_iter().take(4) {
        tx.send(e).unwrap();
    }
    assert!(m.tick());
    assert_eq!(m.state.nodes.len(), 2);
    assert_eq!(m.state.nodes[&r("raw")].phase, Phase::Running);
    drop(tx);
    assert!(!m.tick());
    assert_eq!(m.on_key(key(KeyCode::Char('d'))), Step::Detach);
    assert!(m.hints().contains("d inline"));
}

#[test]
fn late_older_events_do_not_move_a_node_back() {
    let all = events();
    let mut in_order = RunState::new("run1");
    for e in &all {
        in_order.apply(e);
    }
    // The newest events first (the broadcast), the older ones after (a late seed).
    let mut shuffled = RunState::new("run1");
    for e in all[6..].iter().chain(all[..6].iter()) {
        shuffled.apply(e);
    }
    for name in ["raw", "clean", "daily"] {
        let (a, b) = (&in_order.nodes[&r(name)], &shuffled.nodes[&r(name)]);
        assert_eq!(a.phase, b.phase, "{name}");
        assert_eq!(a.started, b.started, "{name}");
        assert_eq!(a.finished, b.finished, "{name}");
        assert_eq!(a.changed(), b.changed(), "{name}");
        assert_eq!(a.chunk_progress(), b.chunk_progress(), "{name}");
        assert_eq!(a.retries, b.retries, "{name}");
    }
    assert_eq!(shuffled.nodes[&r("raw")].phase, Phase::Succeeded);
    assert!(shuffled.nodes[&r("raw")].finished.is_some());
    let seqs: Vec<u64> = shuffled.log.iter().map(|l| l.seq).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
    assert_eq!(shuffled.started, in_order.started);
}

/// A seed fed by hand: `(sender, poller)`.
fn seed() -> (std::sync::mpsc::Sender<Poll>, StoredPoller) {
    let (tx, rx) = std::sync::mpsc::channel();
    let poller = StoredPoller {
        rx,
        stop: Arc::new(AtomicBool::new(false)),
    };
    (tx, poller)
}

#[test]
fn live_feed_holds_broadcast_events_until_the_seed_catches_up() {
    let all = events();
    let (tx, rx) = tokio::sync::broadcast::channel(64);
    let (seed_tx, poller) = seed();
    let mut m = RunMonitor::new("run1", "sql:local", Source::live(rx, Some(poller)));
    // Subscribed mid-run: the broadcast starts at seq 8; 0..=7 were emitted before.
    for e in &all[8..11] {
        tx.send(e.clone()).unwrap();
    }
    m.tick();
    assert!(m.state.nodes.is_empty(), "held until the seed catches up");
    // The first read only has what was flushed so far.
    seed_tx.send(Poll::Events(all[..4].to_vec())).unwrap();
    m.tick();
    assert_eq!(m.state.nodes[&r("raw")].phase, Phase::Running);
    assert!(
        !m.state.nodes.contains_key(&r("clean"))
            || m.state.nodes[&r("clean")].phase != Phase::Running
    );
    // The next read reaches the held events (and overlaps them): all applied, in order.
    seed_tx.send(Poll::Events(all[4..10].to_vec())).unwrap();
    m.tick();
    assert!(matches!(&m.source, Source::Live { seed: None, .. }));
    assert_eq!(m.state.nodes[&r("raw")].phase, Phase::Succeeded);
    assert_eq!(m.state.nodes[&r("raw")].changed(), 12);
    assert_eq!(m.state.nodes[&r("clean")].phase, Phase::Running);
    assert_eq!(m.state.nodes[&r("clean")].attempt, 2);
    let seqs: Vec<u64> = m.state.log.iter().map(|l| l.seq).collect();
    assert_eq!(seqs, (0..=10).collect::<Vec<u64>>());
    // From here on the broadcast applies directly.
    for e in &all[11..] {
        tx.send(e.clone()).unwrap();
    }
    m.tick();
    assert!(m.state.is_finished());
    assert_eq!(m.state.nodes[&r("clean")].phase, Phase::Failed);
}

#[test]
fn live_feed_stops_waiting_for_events_that_were_never_persisted() {
    let all = events();
    let (tx, rx) = tokio::sync::broadcast::channel(64);
    let (seed_tx, poller) = seed();
    let mut m = RunMonitor::new("run1", "sql:local", Source::live(rx, Some(poller)));
    tx.send(all[8].clone()).unwrap();
    seed_tx.send(Poll::Events(all[..3].to_vec())).unwrap();
    m.tick();
    assert!(
        !m.state.nodes.contains_key(&r("clean"))
            || m.state.nodes[&r("clean")].phase != Phase::Running
    );
    if let Source::Live { holding_since, .. } = &mut m.source {
        *holding_since = Some(std::time::Instant::now() - SEED_WAIT);
    }
    m.tick();
    assert!(matches!(&m.source, Source::Live { seed: None, .. }));
    assert_eq!(m.state.nodes[&r("clean")].phase, Phase::Running);
}

#[test]
fn a_quiet_unfinished_run_is_flagged() {
    let mut m = monitor();
    m.state = RunState::new("run1");
    for e in &events()[..9] {
        m.state.apply(e);
    }
    m.now = Some(t(60));
    assert_eq!(m.status(), None);
    m.now = Some(t(4) + chrono::Duration::from_std(super::super::watch::IDLE_LIMIT).unwrap());
    let (text, _) = m.status().expect("a notice");
    assert!(text.contains("may have died"), "{text}");
}
