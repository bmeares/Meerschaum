//! `parameters['schedule']` in continuous mode (§5.5): a scheduled root only syncs when due,
//! unscheduled pipes keep the lap cadence, and without a `Cadence` the key is ignored.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use common::*;
use meerschaum_engine::{
    Cadence, Engine, KeyFilter, LapEvent, LoopOptions, RunOptions, Selection, StopToken, Timer,
};

struct EverySeconds(f64);
struct Ticker {
    step: TimeDelta,
    last: Option<DateTime<Utc>>,
}
impl Timer for Ticker {
    fn first(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.last = Some(now + self.step);
        self.last
    }
    fn after(&mut self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut t = self.last?;
        while t <= after {
            t += self.step;
        }
        self.last = Some(t);
        self.last
    }
}
impl Cadence for EverySeconds {
    fn timer(&self, schedule: &str, _now: DateTime<Utc>) -> Result<Box<dyn Timer>, String> {
        if schedule == "nonsense" {
            return Err("cannot parse".into());
        }
        Ok(Box::new(Ticker {
            step: TimeDelta::milliseconds((self.0 * 1000.0) as i64),
            last: None,
        }))
    }
}

fn params(schedule: Option<&str>) -> serde_json::Value {
    let mut p = serde_json::json!({
        "sql": "SELECT ts, id, v FROM src",
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    });
    if let Some(s) = schedule {
        p["schedule"] = serde_json::json!(s);
    }
    p
}

#[allow(clippy::let_and_return)]
async fn laps(engine: Engine, run_for: Duration) -> Vec<(String, String)> {
    let log: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let sink = log.clone();
    let stop = StopToken::default();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(run_for).await;
        stopper.stop();
    });
    engine
        .run_loop(
            RunOptions {
                selection: Some(Selection::filter(KeyFilter::default())),
                workers: Some(1),
                ..Default::default()
            },
            LoopOptions {
                run_loop: true,
                min_seconds: 0.4,
                stop,
                ..Default::default()
            },
            Arc::new(move |e| {
                if let LapEvent::PipeFinished(o) = e {
                    let name = o.pipe_ref.parts().metric_key.to_string();
                    sink.lock().unwrap().push((name, "ran".into()));
                }
            }),
        )
        .await;
    let out = log.lock().unwrap().clone();
    out
}

async fn setup(tag: &str) -> Env {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register("fast", &params(None)).await;
    env.register("slow", &params(Some("every 2 seconds"))).await;
    env.register("broken", &params(Some("nonsense"))).await;
    env
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_pipe_waits_for_its_fire_time() {
    let env = setup("sched").await;
    let engine = env.engine().with_cadence(Arc::new(EverySeconds(1.5)));
    let ran = laps(engine, Duration::from_millis(3300)).await;
    let count = |m: &str| ran.iter().filter(|(n, _)| n == m).count();
    // `fast` and the unparsable `broken` run every lap; `slow` only when due (about 1.5 s, 3 s).
    assert!(count("fast") >= 4, "{ran:?}");
    assert_eq!(count("fast"), count("broken"), "{ran:?}");
    assert!((1..=2).contains(&count("slow")), "{ran:?}");
    // It did not run on the first lap.
    let first_slow = ran.iter().position(|(n, _)| n == "slow").unwrap();
    assert!(first_slow >= 2, "{ran:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_cadence_the_schedule_key_is_ignored() {
    let env = setup("nosched").await;
    let ran = laps(env.engine(), Duration::from_millis(1500)).await;
    let count = |m: &str| ran.iter().filter(|(n, _)| n == m).count();
    assert_eq!(count("fast"), count("slow"), "{ran:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_scheduled_selection_idles_until_due() {
    let env = env("allsched").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.register("only", &params(Some("every 1 seconds"))).await;
    let engine = env.engine().with_cadence(Arc::new(EverySeconds(1.0)));
    let ran = laps(engine, Duration::from_millis(2600)).await;
    // No cooldown warnings or empty runs: the pipe ran about twice.
    assert!((1..=3).contains(&ran.len()), "{ran:?}");
}

/// S4 (rc15): a scheduled pipe with parents in the run does not follow its schedule. The loop
/// says so once when it starts (naming the pipe and why), and the plan lists it as a warning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scheduled_child_is_warned_about_once() {
    let env = env("sched_child").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register("parent", &params(None)).await;
    let mut child = params(Some("every 1 hours"));
    child["sql"] = serde_json::json!(format!(
        "SELECT ts, id, v FROM {{{{ Pipe('{INSTANCE}', 'parent', instance='{INSTANCE}') }}}}"
    ));
    env.register("child", &child).await;
    let engine = env.engine().with_cadence(Arc::new(EverySeconds(3600.0)));
    let selection = Selection::refs([env.pipe_ref("parent")])
        .with_children(Some(meerschaum_engine::Depth::All));

    // The plan says it.
    let planned = engine
        .plan(&RunOptions {
            selection: Some(selection.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let expected = "Pipe('sql:eng', 'child'): its schedule 'every 1 hours' is ignored because it has parents in this run; it syncs when its parents have new data.";
    assert!(
        planned.plan.warnings.iter().any(|w| w == expected),
        "{:?}",
        planned.plan.warnings
    );
    assert!(planned.plan.to_string().contains(expected));

    // The loop says it once.
    let warnings: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = warnings.clone();
    let stop = StopToken::default();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        stopper.stop();
    });
    engine
        .run_loop(
            RunOptions {
                selection: Some(selection),
                workers: Some(1),
                yes: true,
                ..Default::default()
            },
            LoopOptions {
                run_loop: true,
                min_seconds: 0.3,
                stop,
                ..Default::default()
            },
            Arc::new(move |e| {
                if let LapEvent::Warning { message } = e {
                    sink.lock().unwrap().push(message);
                }
            }),
        )
        .await;
    let warnings = warnings.lock().unwrap().clone();
    assert_eq!(warnings, vec![expected.to_string()]);
}

/// Every pipe of a flat selection is a root: a scheduled child with a parent in the selection
/// follows its schedule (it does not run before it is due), and nothing is warned about.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flat_selection_follows_a_scheduled_childs_schedule() {
    let env = env("sched_flat_child").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register("parent", &params(None)).await;
    let mut child = params(Some("every 1 hours"));
    child["sql"] = serde_json::json!(format!(
        "SELECT ts, id, v FROM {{{{ Pipe('{INSTANCE}', 'parent', instance='{INSTANCE}') }}}}"
    ));
    env.register("child", &child).await;
    let engine = env.engine().with_cadence(Arc::new(EverySeconds(3600.0)));
    let planned = engine
        .plan(&RunOptions {
            selection: Some(Selection::filter(KeyFilter::default())),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(planned.plan.flat);
    assert!(
        planned.plan.warnings.is_empty(),
        "{:?}",
        planned.plan.warnings
    );
    let ran = laps(engine, Duration::from_millis(1200)).await;
    assert!(ran.iter().any(|(n, _)| n == "parent"), "{ran:?}");
    assert!(!ran.iter().any(|(n, _)| n == "child"), "{ran:?}");
}
