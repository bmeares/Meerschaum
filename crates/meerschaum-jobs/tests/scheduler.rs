//! Per-root schedule and debounce state: two roots on one host never share either.

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use meerschaum_jobs::scheduler::RootScheduler;
use meerschaum_jobs::{JobsContext, schedule::Fire};

fn fire(s: &str) -> Fire {
    DateTime::parse_from_rfc3339(s).unwrap()
}

#[test]
fn two_roots_do_not_share_schedule_state() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let sa = RootScheduler::for_context(&common::sh_context(a.path()));
    let sb = RootScheduler::for_context(&common::sh_context(b.path()));
    assert_ne!(sa.root(), sb.root());

    // The same job name and schedule on both roots, recorded on one.
    let now = fire("2026-03-01T00:00:00+00:00");
    let next = sa
        .next_fire("nightly", "every 6 hours", now)
        .unwrap()
        .unwrap();
    sa.record_fire("nightly", "every 6 hours", now, Some(next))
        .unwrap();
    assert_eq!(sa.state("nightly").unwrap().next_fire, Some(next));
    assert!(sb.state("nightly").is_none(), "root b saw root a's state");

    // Root a resumes the recorded fire; root b computes its own from its own clock.
    let later = fire("2026-03-01T01:00:00+00:00");
    assert_eq!(
        sa.next_fire("nightly", "every 6 hours", later).unwrap(),
        Some(next)
    );
    let own = sb
        .next_fire("nightly", "every 6 hours", later)
        .unwrap()
        .unwrap();
    assert_ne!(own, next, "root b was anchored to root a's cadence");

    // Clearing one root leaves the other alone.
    sb.record_fire("nightly", "every 6 hours", later, Some(own))
        .unwrap();
    sa.clear("nightly");
    assert!(sa.state("nightly").is_none());
    assert!(sb.state("nightly").is_some());
}

#[test]
fn two_roots_do_not_share_debounce_windows() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let sa = RootScheduler::for_root(a.path());
    let sb = RootScheduler::for_root(b.path());
    let t0 = "2026-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let window = Duration::from_secs(60);

    assert!(sa.debounce("stream:children", window, t0));
    // Inside root a's window, root b's window for the same key is untouched.
    assert!(!sa.debounce(
        "stream:children",
        window,
        t0 + chrono::TimeDelta::seconds(5)
    ));
    assert!(sb.debounce(
        "stream:children",
        window,
        t0 + chrono::TimeDelta::seconds(5)
    ));
    assert!(!sb.debounce(
        "stream:children",
        window,
        t0 + chrono::TimeDelta::seconds(6)
    ));
}

#[test]
fn one_root_debounces_across_handles_and_threads() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = "2026-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let granted: usize = (0..8)
        .map(|_| {
            let root = dir.path().to_path_buf();
            std::thread::spawn(move || {
                RootScheduler::for_root(&root).debounce("shared", Duration::from_secs(3600), t0)
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum();
    assert_eq!(granted, 1, "exactly one handle may fire inside the window");
}

#[test]
fn two_roots_list_their_own_jobs() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let ca: JobsContext = common::sh_context(a.path());
    let cb: JobsContext = common::sh_context(b.path());
    assert_ne!(ca.paths.jobs_dir, cb.paths.jobs_dir);
    let ea = meerschaum_jobs::executor::make_executor("local", &ca);
    let eb = meerschaum_jobs::executor::make_executor("local", &cb);
    let res = ea.create_job("only-on-a", &["echo".to_string()], None);
    assert!(res.success, "{}", res.message);
    assert!(ea.get_job_exists("only-on-a"));
    assert!(!eb.get_job_exists("only-on-a"));
    assert!(eb.get_job_names().is_empty());
    // The job (a supervisor and its `echo`) writes its logs and error log under the root until
    // it exits: wait for that before the temporary directories are removed.
    assert!(
        common::wait_for(30.0, || {
            meerschaum_jobs::detached::find_pids(
                "only-on-a",
                &ca.paths.jobs_dir,
                &Default::default(),
                None,
                true,
            )
            .is_empty()
        }),
        "the job's supervisor did not exit"
    );
}
