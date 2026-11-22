//! Tests of the schedule grammar and the fire times of its triggers.

use super::*;

fn now() -> Fire {
    chrono::DateTime::parse_from_rfc3339("2025-03-14T15:26:53.589793+00:00").unwrap()
}

#[test]
fn daily_and_weekday() {
    let mut t = parse_schedule("daily", now()).unwrap();
    assert_eq!(
        isoformat(&t.next().unwrap()),
        "2025-03-14T15:26:53.589793+00:00"
    );
    assert_eq!(
        isoformat(&t.next().unwrap()),
        "2025-03-15T15:26:53.589793+00:00"
    );
    let mut t = parse_schedule("mon", now()).unwrap();
    assert_eq!(
        isoformat(&t.next().unwrap()),
        "2025-03-17T15:26:53.589793+00:00"
    );
    assert_eq!(
        isoformat(&t.next().unwrap()),
        "2025-03-17T16:26:53.589793+00:00"
    );
}

#[test]
fn periods_are_the_longest_gap_between_fires() {
    assert_eq!(period_seconds_at("every 10 minutes", now()), Some(600.0));
    assert_eq!(period_seconds_at("daily", now()), Some(86400.0));
    assert_eq!(period_seconds_at("hourly", now()), Some(3600.0));
    assert_eq!(period_seconds_at("weekly", now()), Some(7.0 * 86400.0));
    assert_eq!(period_seconds_at("not a schedule", now()), None);
    // Combined schedules are not searched (their fire times can take long to find).
    let begin = std::time::Instant::now();
    assert_eq!(period_seconds_at("daily and mon-fri", now()), None);
    assert!(begin.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn periods_of_zoned_schedules_include_the_clock_change() {
    let march = chrono::DateTime::parse_from_rfc3339("2027-03-01T00:00:00Z").unwrap();
    // The day the clock springs forward is 23 hours; the longest gap is still a day.
    assert_eq!(
        period_seconds_at("daily starting 2027-03-12 21:00 tz America/New_York", march),
        Some(86400.0)
    );
    // The day it falls back is 25 hours.
    assert_eq!(
        period_seconds_at("daily starting 2027-11-05 21:00 tz America/New_York", march),
        Some(90000.0)
    );
    assert_eq!(
        period_seconds_at("every 10 minutes tz Asia/Tokyo jitter 30", march),
        Some(600.0)
    );
}

#[test]
fn quirky_errors() {
    let e = parse_schedule("every 5 fortnights", now()).unwrap_err();
    assert!(e.py_message().starts_with("Invalid interval 'fs'."));
    let e = parse_schedule("daily starting Tomorrow", now()).unwrap_err();
    assert_eq!(
        e.py_message(),
        "\n\x1b[1;31m 🛑 Unknown string format: Tomorrow\x1b[0m"
    );
}

fn new_york(as_of: &str) -> ParseEnv {
    ParseEnv::in_zone(
        "America/New_York",
        chrono::DateTime::parse_from_rfc3339(as_of)
            .unwrap()
            .to_utc(),
        NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
    )
}

fn start(schedule: &str, env: &ParseEnv) -> (Result<String, String>, Vec<String>) {
    let mut warnings = Vec::new();
    let start = parse_start_time_in(schedule, Some(now()), env, &mut warnings)
        .map(|f| isoformat(&f))
        .map_err(|e| e.to_string());
    (start, warnings)
}

#[test]
fn either_abbreviation_of_the_local_zone_is_the_local_zone() {
    let env = new_york("2026-11-14T12:00:00Z");
    for name in ["EDT", "EST"] {
        // PARITY: dateutil maps both `time.tzname` entries to `tzlocal()`.
        assert_eq!(
            start(&format!("daily starting 21:00 {name}"), &env).0,
            Ok("2026-11-14T21:00:00-05:00".to_string())
        );
        assert_eq!(
            start(&format!("daily starting 2026-07-01 21:00 {name}"), &env).0,
            Ok("2026-07-01T21:00:00-04:00".to_string())
        );
    }
    // A repeated wall time takes the occurrence that carries the name.
    assert_eq!(
        start("daily starting 2026-11-01 01:30 EST", &env).0,
        Ok("2026-11-01T01:30:00-05:00".to_string())
    );
    assert_eq!(
        start("daily starting 2026-11-01 01:30 EDT", &env).0,
        Ok("2026-11-01T01:30:00-04:00".to_string())
    );
}

#[test]
fn utc_names_and_offsets_do_not_need_a_local_zone() {
    let env = ParseEnv {
        local: None,
        today: NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
        host: None,
    };
    for (text, want) in [
        ("21:00 UTC", "2026-11-14T21:00:00+00:00"),
        ("21:00 GMT", "2026-11-14T21:00:00+00:00"),
        ("21:00 Z", "2026-11-14T21:00:00+00:00"),
        ("21:00Z", "2026-11-14T21:00:00+00:00"),
        ("21:00 -0400", "2026-11-14T21:00:00-04:00"),
        ("21:00 UTC+3", "2026-11-14T21:00:00-03:00"),
        ("21:00 GMT-3", "2026-11-14T21:00:00+03:00"),
        ("21:00 EDT+3", "2026-11-14T21:00:00-03:00"),
    ] {
        let (got, warnings) = start(&format!("daily starting {text}"), &env);
        assert_eq!(got.as_deref(), Ok(want), "{text}");
        assert!(warnings.is_empty(), "{text}");
    }
    // PARITY: dateutil reads zone names only in capitals.
    for text in [
        "21:00 utc",
        "21:00 gmt",
        "21:00 edt",
        "2026-07-01 EDT",
        "21:00 ABCDEF",
    ] {
        assert!(
            start(&format!("daily starting {text}"), &env).0.is_err(),
            "{text}"
        );
    }
}

#[test]
fn an_unknown_abbreviation_warns_once_and_reads_utc() {
    let env = new_york("2026-11-14T12:00:00Z");
    let (got, warnings) = start("daily starting 21:00 PDT", &env);
    assert_eq!(got, Ok("2026-11-14T21:00:00+00:00".to_string()));
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].starts_with("tzname PDT identified but not understood."));
    // Without a local zone even EDT is unknown (a host whose zone cannot be named).
    let none = ParseEnv {
        local: None,
        ..new_york("2026-11-14T12:00:00Z")
    };
    let (got, warnings) = start("daily starting 21:00 EDT", &none);
    assert_eq!(got, Ok("2026-11-14T21:00:00+00:00".to_string()));
    assert_eq!(warnings.len(), 1);
    // A schedule parse carries the same warning to its caller.
    let (_, warnings) = parse_schedule_in("daily starting 21:00 PDT", now(), None, &env).unwrap();
    assert_eq!(warnings.len(), 1);
}

#[test]
fn a_resumed_fire_keeps_its_cadence() {
    let spec = JitterSpec {
        default_seconds: 30.0,
        seed: "prune",
    };
    // A `daily` job drew 08:46 in an earlier process and restarts at 05:21 the next day.
    let mut before = parse_schedule_jittered("daily", now(), Some(spec)).unwrap();
    let resumed = before.next_after(now()).unwrap();
    let restart = resumed - Duration::hours(3);
    let mut restarted = parse_schedule_jittered("daily", restart, Some(spec)).unwrap();
    assert_ne!(
        restarted.next_after(resumed),
        Some(resumed + Duration::days(1))
    );
    let mut t = parse_schedule_resumed("daily", resumed, Some(spec)).unwrap();
    assert_eq!(t.next_after(resumed), Some(resumed + Duration::days(1)));
    // Cron fields do not depend on the anchor.
    let mut cron = parse_schedule_resumed("30 3 * * *", resumed, Some(spec)).unwrap();
    let mut fresh = parse_schedule_jittered("30 3 * * *", restart, Some(spec)).unwrap();
    assert_eq!(cron.next_after(resumed), fresh.next_after(resumed));
}
