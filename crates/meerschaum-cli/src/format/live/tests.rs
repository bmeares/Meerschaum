use super::*;
use meerschaum_core::SyncResult;
use meerschaum_engine::Action;
use std::sync::Mutex as StdMutex;

/// The painter is process-wide: tests that start a [`Region`] take this lock.
static SERIAL: StdMutex<()> = StdMutex::new(());

const PLAIN: LiveStyle = LiveStyle {
    unicode: true,
    color: false,
};

fn pipe(metric: &str) -> PipeRef {
    PipeRef::new("sql:local", "sql:src", metric, None)
}

fn label(metric: &str) -> String {
    pipe(metric).label()
}

struct Feed {
    seq: u64,
    run_id: String,
}

impl Feed {
    fn new(run_id: &str) -> Feed {
        Feed {
            seq: 0,
            run_id: run_id.into(),
        }
    }

    fn event(&mut self, pipe_ref: Option<PipeRef>, kind: EventKind) -> Event {
        self.seq += 1;
        Event {
            run_id: self.run_id.clone(),
            seq: self.seq,
            ts: chrono::Utc::now(),
            pipe_ref,
            kind,
        }
    }

    fn started(&mut self, nodes: usize) -> Event {
        self.event(
            None,
            EventKind::RunStarted {
                nodes,
                workers: 4,
                targets: Vec::new(),
            },
        )
    }

    fn node(&mut self, metric: &str, kind: EventKind) -> Event {
        self.event(Some(pipe(metric)), kind)
    }
}

fn node_started(attempt: u32) -> EventKind {
    EventKind::NodeStarted {
        action: Action::Incremental,
        attempt,
    }
}

fn chunk_started(source: ChunkSource, chunk: usize, total: Option<usize>) -> EventKind {
    EventKind::ChunkStarted {
        chunk,
        begin: None,
        end: None,
        source,
        rows: None,
        total,
    }
}

fn chunk_finished(source: ChunkSource, chunk: usize, rows: Option<u64>, ins: u64) -> EventKind {
    EventKind::ChunkFinished {
        chunk,
        success: true,
        message: "Success".into(),
        source,
        rows,
        inserted: ins,
        updated: 0,
        upserted: 0,
        begin: None,
        end: None,
        duration_ms: Some(10),
    }
}

fn finished(ins: u64, upd: u64) -> EventKind {
    let mut result = SyncResult::new(true, "Success");
    result.inserted = ins;
    result.updated = upd;
    EventKind::NodeFinished { result }
}

fn failed(error: &str, attempt: u32, will_retry: bool) -> EventKind {
    EventKind::NodeFailed {
        error: error.into(),
        traceback: None,
        attempt,
        will_retry,
    }
}

#[test]
fn a_multi_node_run_shows_running_rows_and_prints_finished_lines() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-1");
    let mut live = LiveRun::new(PLAIN, true);
    assert!(live.lines(t0, 0, 120, 30, None).is_empty());
    assert!(live.apply(&feed.started(3), t0).is_empty());
    let planned = live.apply(
        &feed.node(
            "a",
            EventKind::NodePlanned {
                action: Action::Incremental,
                reason: "new rows upstream".into(),
                provisional: false,
            },
        ),
        t0,
    );
    assert_eq!(
        planned,
        vec![format!("  · {} incremental: new rows upstream", label("a"))]
    );
    // The same decision again (re-planned) is not printed twice; skips get their own line later.
    let again = feed.node(
        "a",
        EventKind::NodePlanned {
            action: Action::Incremental,
            reason: "new rows upstream".into(),
            provisional: false,
        },
    );
    assert!(live.apply(&again, t0).is_empty());
    let skip_plan = feed.node(
        "c",
        EventKind::NodePlanned {
            action: Action::Skip,
            reason: "up to date".into(),
            provisional: false,
        },
    );
    assert!(live.apply(&skip_plan, t0).is_empty());

    assert!(live.apply(&feed.node("a", node_started(1)), t0).is_empty());
    assert!(live.apply(&feed.node("b", node_started(1)), t0).is_empty());
    live.apply(
        &feed.node("a", chunk_started(ChunkSource::Window, 0, Some(4))),
        t0,
    );
    live.apply(
        &feed.node("a", chunk_finished(ChunkSource::Window, 0, None, 1500)),
        t0,
    );
    let now = t0 + Duration::from_secs(2);
    let region = live.lines(now, 0, 120, 30, Some("w: watch"));
    assert_eq!(region.len(), 3, "{region:#?}");
    assert_eq!(region[0], "⠋ Syncing 0/3 pipes · 2 running  2.0s  w: watch");
    assert_eq!(
        region[1],
        format!(
            "  {}  incremental  1/4 chunks  +1.5k ins ~0 upd +0 ups  750 rows/s  2.0s",
            label("a")
        )
    );
    assert_eq!(region[2], format!("  {}  incremental  2.0s", label("b")));

    let done = live.apply(&feed.node("a", finished(1500, 2)), now);
    assert_eq!(
        done,
        vec![format!("  ✔ {} +1500 ins ~2 upd +0 ups", label("a"))]
    );
    let skipped = live.apply(
        &feed.node(
            "c",
            EventKind::NodeSkipped {
                reason: "up to date".into(),
            },
        ),
        now,
    );
    assert_eq!(skipped, vec![format!("  ○ {} up to date", label("c"))]);
    let region = live.lines(now, 1, 120, 30, None);
    assert_eq!(region[0], "⠙ Syncing 2/3 pipes · 1 running  2.0s");
    assert_eq!(region.len(), 2);
    assert_eq!(live.running_labels(), vec![label("b")]);

    live.apply(&feed.node("b", finished(0, 0)), now);
    live.apply(
        &feed.event(
            None,
            EventKind::RunFinished {
                success: true,
                message: "ok".into(),
            },
        ),
        now,
    );
    assert!(live.lines(now, 2, 120, 30, None).is_empty());
    assert_eq!(live.run_id(), Some("run-1"));
}

#[test]
fn retries_show_on_the_row_and_print_inline() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-2");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&feed.started(1), t0);
    live.apply(&feed.node("a", node_started(1)), t0);
    let lines = live.apply(&feed.node("a", failed("boom\ntraceback", 1, true)), t0);
    assert_eq!(
        lines,
        vec![format!(
            "  ↻ {} attempt 1 failed, retrying: boom",
            label("a")
        )]
    );
    let region = live.lines(t0, 0, 120, 30, None);
    assert!(region[1].contains("incremental · retrying"), "{region:#?}");
    live.apply(&feed.node("a", node_started(2)), t0);
    let region = live.lines(t0, 0, 120, 30, None);
    assert!(region[1].contains("incremental · attempt 2"), "{region:#?}");
    let lines = live.apply(&feed.node("a", failed("boom again", 2, false)), t0);
    assert_eq!(lines, vec![format!("  ✘ {} boom again", label("a"))]);
    let region = live.lines(t0, 0, 120, 30, None);
    assert_eq!(
        region[0],
        "⠋ Syncing 1/1 pipes · 0 running · 1 failed  0.0s"
    );
}

#[test]
fn window_counts_hold_their_fetch_chunks_and_are_not_counted_twice() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-3");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&feed.started(1), t0);
    live.apply(&feed.node("a", node_started(1)), t0);
    live.apply(
        &feed.node("a", chunk_started(ChunkSource::Window, 0, Some(2))),
        t0,
    );
    for i in 0..3 {
        live.apply(
            &feed.node("a", chunk_finished(ChunkSource::Fetch, i, Some(10), 10)),
            t0,
        );
    }
    // Mid-window: the fetch chunks so far.
    let row = &live.lines(t0, 0, 160, 30, None)[1];
    assert!(
        row.contains("0/2 chunks") && row.contains("+30 ins"),
        "{row}"
    );
    // The window reports the same rows again: kept once.
    live.apply(
        &feed.node("a", chunk_finished(ChunkSource::Window, 0, None, 30)),
        t0,
    );
    let row = &live.lines(t0, 0, 160, 30, None)[1];
    assert!(
        row.contains("1/2 chunks") && row.contains("+30 ins"),
        "{row}"
    );
    // A window without counts (the Python executor) commits its fetch chunks' counts.
    live.apply(
        &feed.node("a", chunk_finished(ChunkSource::Fetch, 3, Some(5), 5)),
        t0,
    );
    live.apply(
        &feed.node("a", chunk_finished(ChunkSource::Window, 1, None, 0)),
        t0,
    );
    let row = &live.lines(t0 + Duration::from_secs(5), 0, 160, 30, None)[1];
    assert!(
        row.contains("2/2 chunks") && row.contains("+35 ins"),
        "{row}"
    );
    assert!(row.contains("7 rows/s"), "35 rows in 5 s: {row}");
}

#[test]
fn a_lagging_receiver_notes_the_gap_and_resyncs_from_the_outcomes() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-4");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&feed.started(3), t0);
    live.apply(&feed.node("a", node_started(1)), t0);
    live.apply(&feed.node("b", node_started(1)), t0);
    live.apply(&feed.node("a", finished(1, 0)), t0);
    let gap = live.missed(42);
    assert_eq!(gap.len(), 1);
    assert!(gap[0].contains("42 live events were missed"), "{gap:?}");
    assert_eq!(live.missed_events(), 42);
    let region = live.lines(t0, 0, 120, 30, None);
    assert!(region[0].contains("42 events missed"), "{region:?}");
    // b may have finished in the gap: hidden until its next event.
    assert!(region[0].contains("· 0 running"), "{region:?}");
    assert_eq!(region.len(), 1, "{region:?}");
    live.apply(
        &feed.node("b", chunk_finished(ChunkSource::Fetch, 0, Some(5), 5)),
        t0,
    );
    assert_eq!(live.running_labels(), vec![label("b")]);
    let row = &live.lines(t0, 0, 120, 30, None)[1];
    assert!(
        row.contains("incremental") && row.contains("+5 ins"),
        "{row}"
    );
    let outcome = |metric: &str, success: bool, ins: u64, message: &str| NodeOutcome {
        pipe_ref: pipe(metric),
        pipe: label(metric),
        result: {
            let mut r = SyncResult::new(success, message);
            r.inserted = ins;
            r
        },
    };
    // Skipped, upstream-failed and pending nodes have no outcome: their lines come from the
    // run's record.
    let node = |metric: &str, status: NodeStatus, reason: &str| {
        let mut row =
            meerschaum_engine::runs::NodeRow::new("run-4", &pipe(metric), Action::Skip, reason);
        row.status = status;
        row
    };
    let record = RunRecord {
        run: meerschaum_engine::runs::RunRow {
            run_id: "run-4".into(),
            started: chrono::Utc::now(),
            finished: None,
            success: None,
            message: None,
            targets: vec![],
            workers: 4,
            num_nodes: 6,
            resumed_from: None,
            holder: "test".into(),
            selection: None,
        },
        nodes: vec![
            node("a", NodeStatus::Succeeded, ""),
            node("b", NodeStatus::Succeeded, ""),
            node("d", NodeStatus::Skipped, "no new rows"),
            node(
                "e",
                NodeStatus::UpstreamFailed,
                "not synced because its parent b failed",
            ),
            node("f", NodeStatus::RebuildPending, "definition changed"),
            node("g", NodeStatus::Pending, ""),
        ],
    };
    let lines = live.resync(
        &[
            outcome("a", true, 1, "Success"),
            outcome("b", true, 7, "Success"),
            outcome("c", false, 0, "no table\nmore"),
        ],
        Some(&record),
    );
    assert_eq!(
        lines,
        vec![
            format!("  ✔ {} +7 ins ~0 upd +0 ups", label("b")),
            format!("  ✘ {} no table", label("c")),
            format!("  ○ {} no new rows", label("d")),
            format!("  ○ {} not synced because its parent b failed", label("e")),
            format!("  ○ {} rebuild pending: definition changed", label("f")),
        ],
        "a is not printed again; g never ran"
    );
    assert!(live.lines(t0, 0, 120, 30, None).is_empty());
}

#[test]
fn narrow_and_short_terminals_cut_rows_and_fold_the_rest() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-5");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&feed.started(12), t0);
    for i in 0..10 {
        let metric = format!("a_rather_long_metric_key_{i}");
        live.apply(&feed.node(&metric, node_started(1)), t0);
        live.apply(
            &feed.node(
                &metric,
                chunk_finished(ChunkSource::Fetch, 0, Some(100), 100),
            ),
            t0,
        );
    }
    let now = t0 + Duration::from_secs(3);
    for width in [20, 40, 60, 80, 200] {
        for line in live.lines(now, 3, width, 50, Some("w: watch")) {
            assert!(
                visible_width(&line) < width.max(11),
                "{width}: {line:?} is {} wide",
                visible_width(&line)
            );
        }
    }
    // The elapsed time stays; the details go first.
    let narrow = live.lines(now, 0, 40, 50, None);
    assert!(narrow[1].ends_with("3.0s"), "{narrow:#?}");
    assert!(narrow[1].contains('…'), "{narrow:#?}");
    // 8 rows: header + 5 rows + "and 5 more".
    let short = live.lines(now, 0, 120, 8, None);
    assert_eq!(short.len(), 7, "{short:#?}");
    assert_eq!(short.last().unwrap(), "  … and 5 more running");
}

#[test]
fn ascii_and_color_styles() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-6");
    let mut live = LiveRun::new(
        LiveStyle {
            unicode: false,
            color: true,
        },
        true,
    );
    live.apply(&feed.started(1), t0);
    live.apply(&feed.node("a", node_started(1)), t0);
    let region = live.lines(t0, 1, 120, 30, None);
    assert!(
        region[0].starts_with("\x1b[36m/\x1b[0m Syncing 0/1 pipes"),
        "{region:?}"
    );
    assert!(!region.iter().any(|l| l.contains('·') || l.contains('⠋')));
    let done = live.apply(&feed.node("a", finished(3, 0)), t0);
    assert_eq!(
        done,
        vec![format!(
            "  \x1b[32m✔\x1b[0m {} +3 ins ~0 upd +0 ups",
            label("a")
        )]
    );
    assert_eq!(
        strip_sgr(&done[0]),
        format!("  ✔ {} +3 ins ~0 upd +0 ups", label("a"))
    );
}

#[test]
fn rows_only_mode_prints_no_node_lines() {
    let t0 = Instant::now();
    let mut feed = Feed::new("lap-1");
    let mut live = LiveRun::new(PLAIN, false);
    live.apply(&feed.started(1), t0);
    assert!(live.apply(&feed.node("a", node_started(1)), t0).is_empty());
    assert_eq!(live.lines(t0, 0, 120, 30, None).len(), 2);
    assert!(live.apply(&feed.node("a", finished(1, 0)), t0).is_empty());
    assert!(live.missed(3).is_empty());
    // The next lap is a new run.
    let mut next = Feed::new("lap-2");
    live.apply(
        &feed.event(
            None,
            EventKind::RunFinished {
                success: true,
                message: String::new(),
            },
        ),
        t0,
    );
    assert!(live.lines(t0, 0, 120, 30, None).is_empty());
    live.apply(&next.started(2), t0);
    assert_eq!(live.run_id(), Some("lap-2"));
    assert!(live.lines(t0, 0, 120, 30, None)[0].contains("Syncing 0/2 pipes"));
}

#[test]
fn compact_numbers() {
    assert_eq!(compact(0), "0");
    assert_eq!(compact(999), "999");
    assert_eq!(compact(1_234), "1.2k");
    assert_eq!(compact(5_600_000), "5.6M");
    assert_eq!(compact(7_000_000_000), "7.0B");
}

#[test]
fn erase_counts_reflowed_rows_after_a_resize() {
    assert_eq!(erase_sequence(&[], 80), "");
    assert_eq!(erase_sequence(&[10], 80), "\r\x1b[J");
    assert_eq!(erase_sequence(&[10, 70, 79], 80), "\r\x1b[2A\x1b[J");
    // The terminal went from 80 to 40 columns: the 79-column line now takes two rows.
    assert_eq!(erase_sequence(&[10, 70, 79], 40), "\r\x1b[4A\x1b[J");
}

fn capture(cols: usize) -> (RegionSink, Arc<StdMutex<String>>, Arc<StdMutex<usize>>) {
    let out = Arc::new(StdMutex::new(String::new()));
    let width = Arc::new(StdMutex::new(cols));
    let sink = {
        let out = out.clone();
        let width = width.clone();
        RegionSink::Custom {
            write: Arc::new(move |s: &str| out.lock().unwrap().push_str(s)),
            size: Arc::new(move || (*width.lock().unwrap(), 24)),
        }
    };
    (sink, out, width)
}

#[test]
fn the_region_is_erased_around_output_and_redrawn_after_it() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, out, width) = capture(80);
    let region = Region::start(sink);
    assert!(is_active());
    region.show(vec!["header".into(), "  row one".into()]);
    assert_eq!(*out.lock().unwrap(), "\x1b[?25lheader\n  row one");
    out.lock().unwrap().clear();

    // A print: erased first, drawn again once the line is complete.
    suspend();
    assert_eq!(*out.lock().unwrap(), "\r\x1b[1A\x1b[J");
    out.lock().unwrap().clear();
    resume(false);
    assert_eq!(*out.lock().unwrap(), "", "held after a partial line");
    resume(true);
    assert_eq!(*out.lock().unwrap(), "header\n  row one");
    out.lock().unwrap().clear();

    // `set` changes what the next redraw shows without drawing.
    region.set(vec!["only".into()]);
    assert_eq!(*out.lock().unwrap(), "");
    suspend();
    resume(true);
    assert_eq!(*out.lock().unwrap(), "\r\x1b[1A\x1b[Jonly");
    out.lock().unwrap().clear();

    // Lines wider than the terminal are cut (never wrapped).
    *width.lock().unwrap() = 10;
    region.show(vec!["\x1b[2mabcdefghijklmnop\x1b[0m".into()]);
    assert_eq!(*out.lock().unwrap(), "\r\x1b[Jabcdefgh…");
    out.lock().unwrap().clear();

    drop(region);
    assert!(!is_active());
    assert_eq!(*out.lock().unwrap(), "\r\x1b[J\x1b[?25h");
    out.lock().unwrap().clear();
    // Inactive: the hooks write nothing.
    suspend();
    resume(true);
    assert_eq!(*out.lock().unwrap(), "");
}

#[test]
fn hide_and_signal_clear_give_the_cursor_back() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, out, _) = capture(80);
    let region = Region::start(sink);
    region.show(vec!["a".into(), "b".into(), "c".into()]);
    out.lock().unwrap().clear();
    region.hide();
    assert_eq!(*out.lock().unwrap(), "\r\x1b[2A\x1b[J\x1b[?25h");
    region.show(vec!["a".into()]);
    out.lock().unwrap().clear();
    clear_for_signal();
    assert_eq!(*out.lock().unwrap(), "\r\x1b[J\x1b[?25h");
    drop(region);
}

#[test]
fn inert_keys_read_nothing() {
    let mut keys = Keys::inert();
    assert!(!keys.is_on());
    assert!(keys.pressed().is_empty());
    keys.pause();
    drop(keys);
}

#[test]
fn only_final_plans_are_printed_and_long_ones_are_cut() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-7");
    let mut live = LiveRun::new(PLAIN, true);
    live.set_width(60);
    live.apply(&feed.started(1), t0);
    let plan = |provisional: bool, reason: &str| EventKind::NodePlanned {
        action: Action::Backfill,
        reason: reason.into(),
        provisional,
    };
    assert!(
        live.apply(&feed.node("a", plan(true, "first guess")), t0)
            .is_empty()
    );
    let long = "an unconsumed ledger entry ".repeat(5);
    let lines = live.apply(&feed.node("a", plan(false, &long)), t0);
    assert_eq!(lines.len(), 1);
    assert_eq!(visible_width(&lines[0]), 59, "{lines:?}");
    assert!(lines[0].ends_with('…'), "{lines:?}");
    // Finished lines keep their whole text.
    live.apply(&feed.node("a", node_started(1)), t0);
    let error = format!("{long}!");
    let lines = live.apply(&feed.node("a", failed(&error, 1, false)), t0);
    assert!(lines[0].ends_with('!'), "{lines:?}");
}

#[test]
fn narrow_rows_keep_the_end_of_labels_that_share_a_prefix() {
    let t0 = Instant::now();
    let mut feed = Feed::new("run-8");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&feed.started(3), t0);
    for metric in ["mid_0", "mid_1", "mid_2"] {
        live.apply(&feed.node(metric, node_started(1)), t0);
    }
    let rows = live.lines(t0 + Duration::from_millis(600), 0, 30, 24, None);
    assert_eq!(rows.len(), 4, "{rows:#?}");
    for (row, metric) in rows[1..].iter().zip(["mid_0", "mid_1", "mid_2"]) {
        assert!(row.contains(&format!("'{metric}')")), "{row:?}");
        assert!(row.contains('…'), "{row:?}");
        assert!(visible_width(row) < 30, "{row:?}");
    }
    // The action outlasts the elapsed time on a narrower terminal.
    let row = &live.lines(t0, 0, 34, 24, None)[1];
    assert!(row.contains("incremental"), "{row:?}");
    assert_eq!(truncate_middle("abcdefghij", 20, PLAIN), "abcdefghij");
    assert_eq!(truncate_middle("abcdefghijklmnop", 8, PLAIN), "ab…lmnop");
}

#[test]
fn runs_alongside_keep_their_own_rows() {
    let t0 = Instant::now();
    let mut first = Feed::new("run-a");
    let mut other = Feed::new("run-b");
    let mut live = LiveRun::new(PLAIN, true);
    live.apply(&first.started(2), t0);
    live.apply(&first.node("a", node_started(1)), t0);
    live.apply(&other.started(1), t0);
    live.apply(&other.node("b", node_started(1)), t0);
    assert!(live.lines(t0, 0, 120, 30, None)[0].contains("Syncing 0/3 pipes"));
    let end = |feed: &mut Feed| {
        feed.event(
            None,
            EventKind::RunFinished {
                success: true,
                message: String::new(),
            },
        )
    };
    // The run alongside ends: only its rows go, and `w` still opens the first run.
    live.apply(&end(&mut other), t0);
    assert_eq!(live.running_labels(), vec![label("a")]);
    assert_eq!(live.run_id(), Some("run-a"));
    assert!(!live.lines(t0, 0, 120, 30, None).is_empty());
    live.apply(&end(&mut first), t0);
    assert!(live.lines(t0, 0, 120, 30, None).is_empty());
}

#[test]
fn overlapping_writers_redraw_only_when_the_last_one_is_done() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, out, _) = capture(80);
    let region = Region::start(sink);
    region.show(vec!["header".into()]);
    out.lock().unwrap().clear();
    // The main thread's print and a worker's output overlap.
    suspend();
    suspend();
    resume(true);
    assert_eq!(
        *out.lock().unwrap(),
        "\r\x1b[J",
        "not drawn under the other writer"
    );
    resume(true);
    assert_eq!(*out.lock().unwrap(), "\r\x1b[Jheader");
    drop(region);
}

#[test]
fn a_worker_prompt_keeps_the_region_off_until_a_full_line() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, out, _) = capture(80);
    let region = Region::start(sink);
    region.show(vec!["header".into()]);
    out.lock().unwrap().clear();
    suspend();
    worker_prompt(true);
    resume(false);
    assert!(worker_prompting());
    // Other output and redraws while the worker waits draw nothing.
    suspend();
    resume(true);
    region.show(vec!["header".into()]);
    assert_eq!(*out.lock().unwrap(), "\r\x1b[J");
    let mut keys = Keys::inert();
    assert!(keys.pressed().is_empty());
    // The worker wrote a full line: back.
    suspend();
    worker_prompt(false);
    resume(true);
    assert!(!worker_prompting());
    assert!(
        out.lock().unwrap().ends_with("header"),
        "{:?}",
        out.lock().unwrap()
    );
    // A region dropped while a worker waits leaves no prompt state behind.
    worker_prompt(true);
    drop(region);
    assert!(!worker_prompting());
}

#[test]
fn worker_output_held_for_the_monitor_comes_back_in_order() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    assert!(!hold_or_pass(false, b"now\n"), "not held by default");
    hold_worker_output();
    assert!(hold_or_pass(false, b"one\n"));
    assert!(hold_or_pass(true, b"two\n"));
    let written = StdMutex::new(Vec::new());
    release_worker_output(|stderr, bytes| {
        written
            .lock()
            .unwrap()
            .push((stderr, String::from_utf8_lossy(bytes).into_owned()))
    });
    assert_eq!(
        *written.lock().unwrap(),
        vec![(false, "one\n".to_string()), (true, "two\n".to_string())]
    );
    assert!(!hold_or_pass(false, b"later\n"));
    // Beyond the bound the oldest output is dropped, with a note.
    hold_worker_output();
    let big = vec![b'x'; HELD_BYTES / 2 + 1];
    for _ in 0..3 {
        hold_or_pass(false, &big);
    }
    written.lock().unwrap().clear();
    release_worker_output(|stderr, bytes| {
        written
            .lock()
            .unwrap()
            .push((stderr, String::from_utf8_lossy(bytes).into_owned()))
    });
    let written = written.into_inner().unwrap();
    assert_eq!(written.len(), 2, "a note and the newest chunk");
    assert!(written[0].0 && written[0].1.contains("bytes of plugin output were dropped"));
}
