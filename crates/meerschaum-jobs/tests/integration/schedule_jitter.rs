//! Schedule jitter (D23): `-s 'every 5 minutes jitter 30 seconds'`, `--jitter`, and
//! `jobs.schedule.jitter_seconds` move every fire time by a FIXED per-job offset.

use chrono::{DateTime, Duration, FixedOffset};
use meerschaum_jobs::schedule::{
    JitterSpec, Trigger, parse_jitter_seconds, parse_schedule, parse_schedule_jittered,
    split_jitter,
};
use meerschaum_jobs::startup::fixed_offset;

fn now() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("2026-03-04T05:06:07+00:00").unwrap()
}

fn fires(trigger: &mut Trigger, n: usize) -> Vec<DateTime<FixedOffset>> {
    (0..n).map(|_| trigger.next().unwrap()).collect()
}

#[test]
fn durations_parse_with_or_without_units() {
    for (text, seconds) in [
        ("30", 30.0),
        ("30s", 30.0),
        ("30 seconds", 30.0),
        ("1.5 sec", 1.5),
        ("5m", 300.0),
        ("5 minutes", 300.0),
        ("1h", 3600.0),
        ("2 hours", 7200.0),
        ("250ms", 0.25),
        ("0", 0.0),
    ] {
        assert_eq!(parse_jitter_seconds(text), Some(seconds), "{text}");
    }
    for bad in ["", "x", "-5", "5 fortnights", "five", "1e3"] {
        assert_eq!(parse_jitter_seconds(bad), None, "{bad}");
    }
}

#[test]
fn the_suffix_is_split_off() {
    assert_eq!(
        split_jitter("every 5 minutes jitter 30 seconds").unwrap(),
        ("every 5 minutes".to_string(), Some(30.0))
    );
    assert_eq!(
        split_jitter("daily starting tomorrow JITTER 2m").unwrap(),
        ("daily starting tomorrow".to_string(), Some(120.0))
    );
    assert_eq!(
        split_jitter("every 5 minutes").unwrap(),
        ("every 5 minutes".to_string(), None)
    );
    let e = split_jitter("every 5 minutes jitter soon").unwrap_err();
    assert!(e.py_message().contains("Invalid jitter 'soon'"), "{e}");
}

#[test]
fn a_schedule_without_jitter_is_untouched() {
    let t = parse_schedule("every 5 minutes", now()).unwrap();
    assert_eq!(t.type_name(), "_IntervalTrigger");
    let t = parse_schedule_jittered(
        "every 5 minutes",
        now(),
        Some(JitterSpec {
            default_seconds: 0.0,
            seed: "job",
        }),
    )
    .unwrap();
    assert_eq!(t.type_name(), "_IntervalTrigger");
}

#[test]
fn the_offset_is_fixed_per_name_and_inside_the_bound() {
    fn spec(seed: &str) -> Option<JitterSpec<'_>> {
        Some(JitterSpec {
            default_seconds: 30.0,
            seed,
        })
    }
    let mut plain = parse_schedule("every 5 minutes", now()).unwrap();
    let mut a = parse_schedule_jittered("every 5 minutes", now(), spec("job-a")).unwrap();
    assert_eq!(a.type_name(), "_JitteredTrigger");
    let base = fires(&mut plain, 4);
    let jittered = fires(&mut a, 4);
    let offset = Duration::from_std(fixed_offset("job-a", 30.0)).unwrap();
    for (b, j) in base.iter().zip(&jittered) {
        // The same offset on every fire.
        assert_eq!(*j - *b, offset);
    }
    assert!(offset <= Duration::seconds(30));
    // The same name, the same offset, parse after parse.
    let mut again = parse_schedule_jittered("every 5 minutes", now(), spec("job-a")).unwrap();
    assert_eq!(fires(&mut again, 4), jittered);
    // Other names spread out.
    let offsets: std::collections::HashSet<i64> = (0..40)
        .map(|i| {
            let name = format!("job-{i}");
            let mut t = parse_schedule_jittered("every 5 minutes", now(), spec(&name)).unwrap();
            (t.next().unwrap() - base[0]).num_milliseconds()
        })
        .collect();
    assert!(offsets.len() > 30, "offsets collapse: {offsets:?}");
    assert!(offsets.iter().all(|o| (0..=30_000).contains(o)));
}

#[test]
fn the_suffix_beats_the_default_and_the_seed_defaults_to_the_schedule() {
    let spec = Some(JitterSpec {
        default_seconds: 30.0,
        seed: "job-a",
    });
    let mut t = parse_schedule_jittered("every 1 hour jitter 5 seconds", now(), spec).unwrap();
    let mut plain = parse_schedule("every 1 hour", now()).unwrap();
    let delta = t.next().unwrap() - plain.next().unwrap();
    assert_eq!(
        delta,
        Duration::from_std(fixed_offset("job-a", 5.0)).unwrap()
    );
    // `parse_schedule` alone seeds the offset with the schedule text.
    let mut t = parse_schedule("every 1 hour jitter 5 seconds", now()).unwrap();
    let mut plain = parse_schedule("every 1 hour", now()).unwrap();
    assert_eq!(
        t.next().unwrap() - plain.next().unwrap(),
        Duration::from_std(fixed_offset("every 1 hour", 5.0)).unwrap()
    );
}

#[test]
fn next_after_keeps_the_offset() {
    let spec = Some(JitterSpec {
        default_seconds: 30.0,
        seed: "job-a",
    });
    let offset = Duration::from_std(fixed_offset("job-a", 30.0)).unwrap();
    assert!(offset > Duration::zero(), "pick another seed for this test");
    let mut t = parse_schedule_jittered("every 1 hour", now(), spec).unwrap();
    let first = t.next().unwrap();
    // Just before the jittered fire time: that fire is still ahead.
    let mut t2 = parse_schedule_jittered("every 1 hour", now(), spec).unwrap();
    assert_eq!(
        t2.next_after(first - Duration::milliseconds(1)),
        Some(first)
    );
    // At the fire time: the one after.
    let mut t3 = parse_schedule_jittered("every 1 hour", now(), spec).unwrap();
    assert_eq!(t3.next_after(first), Some(first + Duration::hours(1)));
    // Past the base time but before the jittered one still resolves to the jittered fire.
    let mut t4 = parse_schedule_jittered("every 1 hour", now(), spec).unwrap();
    let base = first - offset;
    assert_eq!(t4.next_after(base), Some(first));
}

#[test]
fn cron_and_combined_schedules_jitter_too() {
    let spec = Some(JitterSpec {
        default_seconds: 20.0,
        seed: "cron-job",
    });
    let offset = Duration::from_std(fixed_offset("cron-job", 20.0)).unwrap();
    for schedule in ["daily", "mon", "0 3 * * *", "every 2 hours | every 3 hours"] {
        let mut plain = parse_schedule(schedule, now()).unwrap();
        let mut jittered = parse_schedule_jittered(schedule, now(), spec).unwrap();
        assert_eq!(
            jittered.next().unwrap() - plain.next().unwrap(),
            offset,
            "{schedule}"
        );
    }
}

#[test]
fn the_scheduler_resumes_a_jittered_time_and_seeds_fresh_ones_by_name() {
    use meerschaum_jobs::scheduler::RootScheduler;
    let dir = tempfile::tempdir().unwrap();
    let s = RootScheduler::for_root(dir.path());
    let t0 = now();
    let a = s
        .next_fire_jittered("job-a", "every 6 hours", t0, 30.0)
        .unwrap()
        .unwrap();
    let b = s
        .next_fire_jittered("job-b", "every 6 hours", t0, 30.0)
        .unwrap()
        .unwrap();
    let plain = s.next_fire("job-c", "every 6 hours", t0).unwrap().unwrap();
    // The plain schedule's first fire after `now` is one interval away; a jittered interval
    // fires at its start (`now`) plus the job's offset, which is still ahead.
    assert_eq!(plain, t0 + Duration::hours(6));
    assert_eq!(
        a,
        t0 + Duration::from_std(fixed_offset("job-a", 30.0)).unwrap()
    );
    assert_eq!(
        b,
        t0 + Duration::from_std(fixed_offset("job-b", 30.0)).unwrap()
    );
    assert_ne!(a, b);
    // A restart resumes the recorded time, offset included.
    assert!(a > later_bound(t0));
    s.record_fire("job-a", "every 6 hours", t0, Some(a))
        .unwrap();
    // (The recorded fire is a few seconds away: still ahead 5 s later.)
    let later = t0 + Duration::seconds(5);
    assert_eq!(
        s.next_fire_jittered("job-a", "every 6 hours", later, 30.0)
            .unwrap(),
        Some(a)
    );
}

fn later_bound(t0: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    t0 + Duration::seconds(5)
}
