//! Zone-aware schedules (v5): the `tz <zone>` clause and wall-clock stepping across clock
//! changes. v4 has no `tz` clause, so these expectations are written by hand (the 2027 changes:
//! `America/New_York` springs forward 2027-03-14 02:00 -> 03:00 and falls back 2027-11-07
//! 02:00 -> 01:00; `Australia/Lord_Howe` falls back 2027-04-04 02:00 -> 01:30 and springs forward
//! 2027-10-03 02:00 -> 02:30; `Asia/Tokyo` has no daylight saving time).

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Timelike};
use meerschaum_jobs::schedule::{
    Fire, JitterSpec, ParseEnv, Trigger, isoformat, parse_schedule_in, parse_schedule_resumed_in,
};

const NY: &str = "America/New_York";
const LORD_HOWE: &str = "Australia/Lord_Howe";
const TOKYO: &str = "Asia/Tokyo";

fn at(s: &str) -> Fire {
    DateTime::parse_from_rfc3339(s).expect("rfc3339")
}

/// A process whose own zone is `host`.
fn env(host: &str) -> ParseEnv {
    ParseEnv::in_zone(
        host,
        at("2026-11-14T12:00:00Z").to_utc(),
        NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
    )
}

fn parse(schedule: &str, now: &str, host: &str) -> (Trigger, Vec<String>) {
    parse_schedule_in(schedule, at(now), None, &env(host))
        .unwrap_or_else(|e| panic!("{schedule}: {e}"))
}

fn fires_in(schedule: &str, now: &str, host: &str, n: usize) -> Vec<String> {
    let (mut trigger, _) = parse(schedule, now, host);
    (0..n)
        .map(|_| trigger.next().map(|f| isoformat(&f)).unwrap_or_default())
        .collect()
}

/// Fire times under a `TZ=UTC` process (the `tz` clause names the zone).
fn fires(schedule: &str, now: &str, n: usize) -> Vec<String> {
    fires_in(schedule, now, "UTC", n)
}

#[test]
fn tz_local_is_the_host_zone() {
    let s = "daily starting 2027-03-12 21:00 tz local";
    let now = "2027-03-12T12:00:00Z";
    assert_eq!(
        fires_in(s, now, NY, 4),
        [
            "2027-03-12T21:00:00-05:00",
            "2027-03-13T21:00:00-05:00",
            "2027-03-14T21:00:00-04:00",
            "2027-03-15T21:00:00-04:00",
        ]
    );
    assert_eq!(
        fires_in(s, now, TOKYO, 2),
        ["2027-03-12T21:00:00+09:00", "2027-03-13T21:00:00+09:00"]
    );
    assert_eq!(
        fires_in(s, now, "UTC", 2),
        ["2027-03-12T21:00:00+00:00", "2027-03-13T21:00:00+00:00"]
    );
    let (trigger, warnings) = parse(s, now, NY);
    assert!(warnings.is_empty());
    let zone = trigger.zone().unwrap();
    assert!(zone.is_host());
    assert_eq!(zone.name(), NY);
}

#[test]
fn tz_local_without_a_host_zone_is_utc_with_one_warning() {
    let env = ParseEnv {
        local: None,
        today: NaiveDate::from_ymd_opt(2026, 11, 14).unwrap(),
        host: None,
    };
    let (mut t, warnings) = parse_schedule_in(
        "daily starting 2027-03-12 21:00 tz local",
        at("2027-03-12T12:00:00Z"),
        None,
        &env,
    )
    .unwrap();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("'tz local' steps in UTC"));
    assert!(t.zone().is_none());
    assert_eq!(isoformat(&t.next().unwrap()), "2027-03-12T21:00:00+00:00");
}

#[test]
fn a_named_zone_matches_without_regard_to_case() {
    let now = "2027-03-12T12:00:00Z";
    let want = [
        "2027-03-13T21:00:00-05:00",
        "2027-03-14T21:00:00-04:00",
        "2027-03-15T21:00:00-04:00",
    ];
    for zone in ["America/New_York", "america/new_york", "AMERICA/NEW_YORK"] {
        let s = format!("daily starting 2027-03-13 21:00 tz {zone}");
        assert_eq!(fires(&s, now, 3), want, "{zone}");
        assert_eq!(parse(&s, now, "UTC").0.zone().unwrap().name(), NY);
    }
    // `TZ` in capitals is the same keyword.
    assert_eq!(
        fires(
            "daily starting 2027-03-13 21:00 TZ America/New_York",
            now,
            3
        ),
        want
    );
}

#[test]
fn unknown_zones_and_misplaced_clauses_are_errors() {
    let e = parse_schedule_in(
        "daily tz Mars/Olympus",
        at("2027-03-12T12:00:00Z"),
        None,
        &env("UTC"),
    )
    .unwrap_err()
    .py_message();
    assert!(e.contains("Unknown time zone 'Mars/Olympus'"), "{e}");
    assert!(
        e.contains("'local'") && e.contains("America/New_York"),
        "{e}"
    );
    for s in [
        "daily tz local starting 21:00",
        "daily tz local and mon",
        "tz local daily",
    ] {
        let e = parse_schedule_in(s, at("2027-03-12T12:00:00Z"), None, &env("UTC"))
            .unwrap_err()
            .py_message();
        assert!(e.contains("must come at the end"), "{s}: {e}");
    }
}

#[test]
fn tz_utc_is_the_default() {
    let now = "2027-03-12T12:00:00Z";
    for base in [
        "daily",
        "every 7 hours starting 2027-03-13 21:00",
        "monthly starting 2027-02-14 02:30",
        "30 2 * * *",
        "mon-fri",
        "daily starting 2027-03-12 21:00 -04:00",
        "daily and mon starting tomorrow 21:00",
    ] {
        for host in [NY, "UTC"] {
            assert_eq!(
                fires_in(&format!("{base} tz utc"), now, host, 6),
                fires_in(base, now, host, 6),
                "{base} under {host}"
            );
        }
        assert!(parse(&format!("{base} tz UTC"), now, NY).0.zone().is_none());
    }
}

#[test]
fn times_without_a_zone_are_read_in_the_clause_zone() {
    // An absolute time, `today` / `tomorrow`, and a bare time (on the zone's date).
    let now = "2027-07-01T12:00:00Z";
    assert_eq!(
        fires(
            "daily starting 2027-07-01 21:00 tz America/New_York",
            now,
            1
        ),
        ["2027-07-01T21:00:00-04:00"]
    );
    assert_eq!(
        fires("daily starting tomorrow 06:30 tz Asia/Tokyo", now, 1),
        ["2027-07-02T06:30:00+09:00"]
    );
    assert_eq!(
        fires("daily starting today 21:00 tz America/New_York", now, 1),
        ["2027-07-01T21:00:00-04:00"]
    );
    // 12:00Z is 21:00 on 2027-07-01 in Tokyo; `today` is Tokyo's date.
    assert_eq!(
        fires("daily starting today 23:00 tz Asia/Tokyo", now, 1),
        ["2027-07-01T23:00:00+09:00"]
    );
    // Relative starts are instants: `now` and `in 5 minutes` in the zone's offset.
    assert_eq!(
        fires("hourly tz America/New_York", now, 2),
        ["2027-07-01T08:00:00-04:00", "2027-07-01T09:00:00-04:00"]
    );
    assert_eq!(
        fires("hourly starting in 5 minutes tz Asia/Tokyo", now, 1),
        ["2027-07-01T21:05:00+09:00"]
    );
    // Cron fields match the zone's wall clock.
    assert_eq!(
        fires("0 9 * * * tz America/New_York", now, 2),
        ["2027-07-01T09:00:00-04:00", "2027-07-02T09:00:00-04:00"]
    );
    // A named weekday keeps the start's minute on the zone's clock (Kolkata is +05:30).
    assert_eq!(
        fires("mon tz Asia/Kolkata", "2027-07-05T12:07:00Z", 2),
        ["2027-07-05T17:37:00+05:30", "2027-07-05T18:37:00+05:30"]
    );
}

#[test]
fn a_time_with_its_own_zone_keeps_its_instant() {
    let now = "2027-03-12T12:00:00Z";
    // 21:00+09:00 is 07:00 in New York: the steps follow New York's wall clock from there.
    assert_eq!(
        fires(
            "daily starting 2027-03-13 21:00 +09:00 tz America/New_York",
            now,
            3
        ),
        [
            "2027-03-13T07:00:00-05:00",
            "2027-03-14T07:00:00-04:00",
            "2027-03-15T07:00:00-04:00",
        ]
    );
    // A host abbreviation is the host's zone for the instant; `tz` sets the steps.
    assert_eq!(
        fires_in(
            "daily starting 2027-03-13 21:00 EST tz Asia/Tokyo",
            now,
            NY,
            2
        ),
        ["2027-03-14T11:00:00+09:00", "2027-03-15T11:00:00+09:00"]
    );
}

/// Every interval class across New York's spring-forward change.
#[test]
fn intervals_across_spring_forward() {
    let now = "2027-03-01T00:00:00Z";
    let cases: &[(&str, &[&str])] = &[
        (
            "every 5 minutes starting 2027-03-14 01:50",
            &[
                "2027-03-14T01:50:00-05:00",
                "2027-03-14T01:55:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-14T03:05:00-04:00",
            ],
        ),
        (
            "hourly starting 2027-03-14 00:30",
            &[
                "2027-03-14T00:30:00-05:00",
                "2027-03-14T01:30:00-05:00",
                "2027-03-14T03:30:00-04:00",
                "2027-03-14T04:30:00-04:00",
            ],
        ),
        (
            "every 12 hours starting 2027-03-13 09:00",
            &[
                "2027-03-13T09:00:00-05:00",
                "2027-03-13T21:00:00-05:00",
                "2027-03-14T09:00:00-04:00",
                "2027-03-14T21:00:00-04:00",
            ],
        ),
        // A missing wall time of an interval longer than the gap fires just after it.
        (
            "every 12 hours starting 2027-03-13 14:30",
            &[
                "2027-03-13T14:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-14T14:30:00-04:00",
                "2027-03-15T02:30:00-04:00",
            ],
        ),
        (
            "every 7 hours starting 2027-03-13 12:30",
            &[
                "2027-03-13T12:30:00-05:00",
                "2027-03-13T19:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-14T09:30:00-04:00",
                "2027-03-14T16:30:00-04:00",
            ],
        ),
        (
            "daily starting 2027-03-13 02:30",
            &[
                "2027-03-13T02:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-15T02:30:00-04:00",
            ],
        ),
        (
            "daily starting 2027-03-13 21:00",
            &[
                "2027-03-13T21:00:00-05:00",
                "2027-03-14T21:00:00-04:00",
                "2027-03-15T21:00:00-04:00",
            ],
        ),
        (
            "weekly starting 2027-03-07 02:30",
            &[
                "2027-03-07T02:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-21T02:30:00-04:00",
            ],
        ),
        (
            "monthly starting 2027-02-14 02:30",
            &[
                "2027-02-14T02:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-04-14T02:30:00-04:00",
            ],
        ),
        (
            "yearly starting 2026-03-14 02:30",
            &[
                "2026-03-14T02:30:00-04:00",
                "2027-03-14T03:00:00-04:00",
                "2028-03-14T02:30:00-04:00",
            ],
        ),
    ];
    for (base, want) in cases {
        let s = format!("{base} tz America/New_York");
        assert_eq!(fires(&s, now, want.len()), *want, "{s}");
    }
}

/// Every interval class across New York's fall-back change.
#[test]
fn intervals_across_fall_back() {
    let now = "2027-10-01T00:00:00Z";
    let cases: &[(&str, &[&str])] = &[
        // No longer than the repeated hour: both occurrences, an unbroken cadence.
        (
            "every 20 minutes starting 2027-11-07 00:40",
            &[
                "2027-11-07T00:40:00-04:00",
                "2027-11-07T01:00:00-04:00",
                "2027-11-07T01:20:00-04:00",
                "2027-11-07T01:40:00-04:00",
                "2027-11-07T01:00:00-05:00",
                "2027-11-07T01:20:00-05:00",
                "2027-11-07T01:40:00-05:00",
                "2027-11-07T02:00:00-05:00",
            ],
        ),
        (
            "hourly starting 2027-11-07 00:30",
            &[
                "2027-11-07T00:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-07T01:30:00-05:00",
                "2027-11-07T02:30:00-05:00",
            ],
        ),
        // Longer than the repeated hour: the first occurrence.
        (
            "every 12 hours starting 2027-11-06 13:30",
            &[
                "2027-11-06T13:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-07T13:30:00-05:00",
                "2027-11-08T01:30:00-05:00",
            ],
        ),
        (
            "every 7 hours starting 2027-11-06 18:30",
            &[
                "2027-11-06T18:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-07T08:30:00-05:00",
                "2027-11-07T15:30:00-05:00",
            ],
        ),
        (
            "daily starting 2027-11-06 01:30",
            &[
                "2027-11-06T01:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-08T01:30:00-05:00",
            ],
        ),
        (
            "daily starting 2027-11-06 21:00",
            &[
                "2027-11-06T21:00:00-04:00",
                "2027-11-07T21:00:00-05:00",
                "2027-11-08T21:00:00-05:00",
            ],
        ),
        (
            "weekly starting 2027-10-31 01:30",
            &[
                "2027-10-31T01:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-14T01:30:00-05:00",
            ],
        ),
        (
            "monthly starting 2027-10-07 01:30",
            &[
                "2027-10-07T01:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-12-07T01:30:00-05:00",
            ],
        ),
        (
            "yearly starting 2026-11-07 01:30",
            &[
                "2026-11-07T01:30:00-05:00",
                "2027-11-07T01:30:00-04:00",
                "2028-11-07T01:30:00-05:00",
            ],
        ),
    ];
    for (base, want) in cases {
        let s = format!("{base} tz America/New_York");
        assert_eq!(fires(&s, now, want.len()), *want, "{s}");
    }
}

#[test]
fn cron_with_a_specific_or_a_wildcard_hour() {
    let cases: &[(&str, &str, &[&str])] = &[
        // A specific hour: once just after the gap, once (the first) in a repeated hour.
        (
            "30 2 * * * starting 2027-03-13 00:00",
            "2027-03-01T00:00:00Z",
            &[
                "2027-03-13T02:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-15T02:30:00-04:00",
            ],
        ),
        (
            "30 1 * * * starting 2027-11-06 00:00",
            "2027-10-01T00:00:00Z",
            &[
                "2027-11-06T01:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-08T01:30:00-05:00",
            ],
        ),
        // Several matches inside the gap still fire once.
        (
            "*/20 2 * * * starting 2027-03-14 00:00",
            "2027-03-01T00:00:00Z",
            &[
                "2027-03-14T03:00:00-04:00",
                "2027-03-15T02:00:00-04:00",
                "2027-03-15T02:20:00-04:00",
            ],
        ),
        // A wildcard hour: nothing inside the gap, both occurrences of a repeated hour.
        (
            "*/30 * * * * starting 2027-03-14 01:00",
            "2027-03-01T00:00:00Z",
            &[
                "2027-03-14T01:00:00-05:00",
                "2027-03-14T01:30:00-05:00",
                "2027-03-14T03:00:00-04:00",
                "2027-03-14T03:30:00-04:00",
            ],
        ),
        (
            "*/30 * * * * starting 2027-11-07 00:00",
            "2027-10-01T00:00:00Z",
            &[
                "2027-11-07T00:00:00-04:00",
                "2027-11-07T00:30:00-04:00",
                "2027-11-07T01:00:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-07T01:00:00-05:00",
                "2027-11-07T01:30:00-05:00",
                "2027-11-07T02:00:00-05:00",
            ],
        ),
        (
            "0 21 * * mon-fri starting 2027-03-12 00:00",
            "2027-03-01T00:00:00Z",
            &[
                "2027-03-12T21:00:00-05:00",
                "2027-03-15T21:00:00-04:00",
                "2027-03-16T21:00:00-04:00",
            ],
        ),
    ];
    for (base, now, want) in cases {
        let s = format!("{base} tz America/New_York");
        assert_eq!(fires(&s, now, want.len()), *want, "{s}");
    }
}

#[test]
fn cron_next_after_inside_a_repeated_hour() {
    // `after` in the first 01:xx: the next fires are the rest of the first occurrence, then the
    // second occurrence from 01:00 (which a walk from `after` alone would miss).
    let (mut t, _) = parse(
        "*/30 * * * * starting 2027-11-01 00:00 tz America/New_York",
        "2027-10-01T00:00:00Z",
        "UTC",
    );
    let got: Vec<String> = [
        t.next_after(at("2027-11-07T01:10:00-04:00")),
        t.next(),
        t.next(),
    ]
    .iter()
    .map(|f| isoformat(&f.unwrap()))
    .collect();
    assert_eq!(
        got,
        [
            "2027-11-07T01:30:00-04:00",
            "2027-11-07T01:00:00-05:00",
            "2027-11-07T01:30:00-05:00",
        ]
    );
    // `after` in the second occurrence.
    let got = t.next_after(at("2027-11-07T01:10:00-05:00")).unwrap();
    assert_eq!(isoformat(&got), "2027-11-07T01:30:00-05:00");
}

#[test]
fn lord_howe_changes_by_half_an_hour() {
    let cases: &[(&str, &str, &[&str])] = &[
        // Fall back 02:00 -> 01:30: 01:30 to 02:00 repeats.
        (
            "every 15 minutes starting 2027-04-04 01:15",
            "2027-04-01T00:00:00Z",
            &[
                "2027-04-04T01:15:00+11:00",
                "2027-04-04T01:30:00+11:00",
                "2027-04-04T01:45:00+11:00",
                "2027-04-04T01:30:00+10:30",
                "2027-04-04T01:45:00+10:30",
                "2027-04-04T02:00:00+10:30",
            ],
        ),
        (
            "hourly starting 2027-04-04 00:45",
            "2027-04-01T00:00:00Z",
            &[
                "2027-04-04T00:45:00+11:00",
                "2027-04-04T01:45:00+11:00",
                "2027-04-04T02:45:00+10:30",
            ],
        ),
        // Spring forward 02:00 -> 02:30.
        (
            "every 10 minutes starting 2027-10-03 01:50",
            "2027-10-01T00:00:00Z",
            &[
                "2027-10-03T01:50:00+10:30",
                "2027-10-03T02:30:00+11:00",
                "2027-10-03T02:40:00+11:00",
            ],
        ),
        (
            "hourly starting 2027-10-03 00:15",
            "2027-10-01T00:00:00Z",
            &[
                "2027-10-03T00:15:00+10:30",
                "2027-10-03T01:15:00+10:30",
                "2027-10-03T02:30:00+11:00",
                "2027-10-03T03:15:00+11:00",
            ],
        ),
        (
            "daily starting 2027-10-02 02:15",
            "2027-10-01T00:00:00Z",
            &[
                "2027-10-02T02:15:00+10:30",
                "2027-10-03T02:30:00+11:00",
                "2027-10-04T02:15:00+11:00",
            ],
        ),
        (
            "15 2 * * * starting 2027-10-02 00:00",
            "2027-10-01T00:00:00Z",
            &[
                "2027-10-02T02:15:00+10:30",
                "2027-10-03T02:30:00+11:00",
                "2027-10-04T02:15:00+11:00",
            ],
        ),
    ];
    for (base, now, want) in cases {
        let s = format!("{base} tz Australia/Lord_Howe");
        assert_eq!(fires(&s, now, want.len()), *want, "{s}");
    }
}

#[test]
fn tokyo_has_no_changes() {
    let now = "2027-03-01T00:00:00Z";
    assert_eq!(
        fires("daily starting 2027-03-13 21:00 tz Asia/Tokyo", now, 3),
        [
            "2027-03-13T21:00:00+09:00",
            "2027-03-14T21:00:00+09:00",
            "2027-03-15T21:00:00+09:00",
        ]
    );
    assert_eq!(
        fires("0 21 * * * tz Asia/Tokyo", now, 2),
        ["2027-03-01T21:00:00+09:00", "2027-03-02T21:00:00+09:00"]
    );
    // A zone without changes steps exactly like a fixed offset.
    for base in [
        "every 7 hours starting 2027-03-13 21:00",
        "monthly starting 2027-01-31 21:00",
        "*/15 9-17 * * mon-fri",
    ] {
        let zoned = fires(&format!("{base} tz Asia/Tokyo"), now, 20);
        let fixed: Vec<String> = fires(&base.replace("21:00", "21:00 +09:00"), now, 20);
        if base.contains("starting") {
            assert_eq!(zoned, fixed, "{base}");
        }
        assert!(zoned.iter().all(|f| f.ends_with("+09:00")), "{base}");
    }
}

#[test]
fn compounds_step_in_the_clause_zone() {
    let now = "2027-03-01T00:00:00Z";
    assert_eq!(
        fires(
            "daily and mon-fri starting 2027-03-12 21:00 tz America/New_York",
            now,
            3
        ),
        [
            "2027-03-12T21:00:00-05:00",
            "2027-03-15T21:00:00-04:00",
            "2027-03-16T21:00:00-04:00",
        ]
    );
    assert_eq!(
        fires(
            "daily or 0 9 * * * starting 2027-03-13 21:00 tz America/New_York",
            now,
            3
        ),
        [
            "2027-03-13T21:00:00-05:00",
            "2027-03-14T09:00:00-04:00",
            "2027-03-14T21:00:00-04:00",
        ]
    );
}

#[test]
fn jitter_is_a_fixed_offset_on_top_of_the_zone() {
    let now = at("2027-03-01T00:00:00Z");
    let spec = JitterSpec {
        default_seconds: 0.0,
        seed: "nightly",
    };
    let plain = fires(
        "daily starting 2027-03-13 21:00 tz America/New_York",
        "2027-03-01T00:00:00Z",
        3,
    );
    for s in [
        "daily starting 2027-03-13 21:00 tz America/New_York jitter 5 minutes",
        "daily starting 2027-03-13 21:00 jitter 5 minutes tz America/New_York",
    ] {
        let (mut t, _) = parse_schedule_in(s, now, Some(spec), &env("UTC")).unwrap();
        let got: Vec<Fire> = (0..3).map(|_| t.next().unwrap()).collect();
        let offset = got[0] - at(&plain[0]);
        assert!(
            offset >= Duration::zero() && offset <= Duration::minutes(5),
            "{s}"
        );
        for (g, p) in got.iter().zip(&plain) {
            assert_eq!(*g - at(p), offset, "{s}");
            // Reported in the zone's offset at that instant.
            assert_eq!(g.offset(), at(p).offset(), "{s}");
        }
    }
}

#[test]
fn jitter_zero_overrides_a_default_jitter() {
    let now = at("2027-03-01T00:00:00Z");
    let spec = JitterSpec {
        default_seconds: 300.0,
        seed: "nightly",
    };
    let exact = "2027-03-13T21:00:00-05:00";
    let (mut t, _) = parse_schedule_in(
        "daily starting 2027-03-13 21:00 tz America/New_York jitter 0",
        now,
        Some(spec),
        &env("UTC"),
    )
    .unwrap();
    assert_eq!(isoformat(&t.next().unwrap()), exact);
    // Without `jitter 0` the default applies.
    let (mut t, _) = parse_schedule_in(
        "daily starting 2027-03-13 21:00 tz America/New_York",
        now,
        Some(spec),
        &env("UTC"),
    )
    .unwrap();
    assert_ne!(isoformat(&t.next().unwrap()), exact);
}

#[test]
fn a_restart_on_either_side_of_a_change_keeps_the_wall_clock_cadence() {
    let spec = JitterSpec {
        default_seconds: 30.0,
        seed: "nightly",
    };
    let host = env(NY);
    for s in ["daily tz local", "daily tz America/New_York"] {
        // Started at 21:00 EST on 2027-03-12.
        let (mut t, _) =
            parse_schedule_in(s, at("2027-03-13T02:00:00Z"), Some(spec), &host).unwrap();
        let drawn: Vec<Fire> = (0..4).map(|_| t.next().unwrap()).collect();
        let wall = |f: &Fire| f.with_timezone(&chrono_tz::America::New_York).time();
        assert!(drawn.iter().all(|f| wall(f) == wall(&drawn[0])), "{s}");
        assert_eq!(drawn[2] - drawn[1], Duration::hours(23), "{s}");
        // A restart resumes each drawn fire and continues at the same wall-clock time.
        for (i, fire) in drawn.iter().enumerate().take(2) {
            let mut resumed = parse_schedule_resumed_in(s, *fire, Some(spec), &host).unwrap();
            assert_eq!(
                resumed.next_after(*fire),
                Some(drawn[i + 1]),
                "{s} from {i}"
            );
            assert_eq!(resumed.next(), Some(drawn[i + 2]), "{s} from {i}");
        }
    }
    // An interval resumed inside a repeated hour continues through the second occurrence.
    let s = "hourly tz America/New_York";
    let fire = at("2027-11-07T01:30:00-04:00");
    let mut resumed = parse_schedule_resumed_in(s, fire, None, &host).unwrap();
    assert_eq!(
        resumed.next_after(fire).map(|f| isoformat(&f)).as_deref(),
        Some("2027-11-07T01:30:00-05:00")
    );
    let fire = at("2027-11-07T01:30:00-05:00");
    let mut resumed = parse_schedule_resumed_in(s, fire, None, &host).unwrap();
    assert_eq!(
        resumed.next_after(fire).map(|f| isoformat(&f)).as_deref(),
        Some("2027-11-07T02:30:00-05:00")
    );
}

/// The real time between two fires.
fn gap(a: &Fire, b: &Fire) -> Duration {
    *b - *a
}

/// A schedule, its longest interval, and the wall time a day-or-longer schedule asks for.
type YearCase = (&'static str, Duration, Option<(u32, u32)>);

/// A year of fires for several schedules and zones: instants strictly increase; day-or-longer
/// schedules fire at the requested wall time except where it does not exist (then just after
/// the gap); and no gap exceeds the interval plus the zone's clock change.
#[test]
fn a_year_of_fires_keeps_the_model() {
    let zones = [
        (NY, Duration::hours(1)),
        (LORD_HOWE, Duration::minutes(30)),
        ("Europe/London", Duration::hours(1)),
        ("Australia/Sydney", Duration::hours(1)),
        (TOKYO, Duration::zero()),
    ];
    let month = Duration::days(31);
    // (schedule, longest interval, requested wall time for day-or-longer schedules)
    let schedules: &[YearCase] = &[
        (
            "every 5 minutes starting 2027-01-01 00:00",
            Duration::minutes(5),
            None,
        ),
        (
            "every 20 minutes starting 2027-01-01 00:10",
            Duration::minutes(20),
            None,
        ),
        ("hourly starting 2027-01-01 00:30", Duration::hours(1), None),
        ("hourly starting 2027-01-01 00:15", Duration::hours(1), None),
        (
            "every 7 hours starting 2027-01-01 02:30",
            Duration::hours(7),
            None,
        ),
        (
            "every 12 hours starting 2027-01-01 02:15",
            Duration::hours(12),
            None,
        ),
        (
            "daily starting 2027-01-01 02:30",
            Duration::days(1),
            Some((2, 30)),
        ),
        (
            "daily starting 2027-01-01 01:30",
            Duration::days(1),
            Some((1, 30)),
        ),
        (
            "daily starting 2027-01-01 02:15",
            Duration::days(1),
            Some((2, 15)),
        ),
        (
            "weekly starting 2027-01-03 02:30",
            Duration::weeks(1),
            Some((2, 30)),
        ),
        ("monthly starting 2027-01-01 02:30", month, Some((2, 30))),
        (
            "30 2 * * * starting 2027-01-01 00:00",
            Duration::days(1),
            Some((2, 30)),
        ),
        (
            "15 2 * * * starting 2027-01-01 00:00",
            Duration::days(1),
            Some((2, 15)),
        ),
        (
            "*/15 * * * * starting 2027-01-01 00:00",
            Duration::minutes(15),
            None,
        ),
        (
            "0 * * * * starting 2027-01-01 00:00",
            Duration::hours(1),
            None,
        ),
        (
            "daily and mon-fri starting 2027-01-01 02:30",
            Duration::days(3),
            Some((2, 30)),
        ),
    ];
    let end = at("2028-01-01T00:00:00Z");
    for (zone, change) in zones {
        let tz: chrono_tz::Tz = zone.parse().unwrap();
        for (base, interval, wall_time) in schedules {
            let s = format!("{base} tz {zone}");
            let (mut t, _) = parse(&s, "2026-12-01T00:00:00Z", "UTC");
            let mut previous: Option<Fire> = None;
            let mut count = 0;
            while let Some(fire) = t.next() {
                if fire >= end {
                    break;
                }
                count += 1;
                // Reported with the zone's offset at that instant.
                assert_eq!(
                    fire.offset().local_minus_utc(),
                    fire.with_timezone(&tz)
                        .fixed_offset()
                        .offset()
                        .local_minus_utc(),
                    "{s}: {fire}"
                );
                if let Some(prev) = previous {
                    assert!(fire > prev, "{s}: {fire} after {prev}");
                    assert!(
                        gap(&prev, &fire) <= *interval + change,
                        "{s}: {} between {prev} and {fire}",
                        gap(&prev, &fire)
                    );
                }
                if let Some((h, m)) = wall_time {
                    let local = fire.with_timezone(&tz).naive_local();
                    let requested = local
                        .date()
                        .and_time(NaiveTime::from_hms_opt(*h, *m, 0).unwrap());
                    let exists = !matches!(
                        chrono::TimeZone::from_local_datetime(&tz, &requested),
                        chrono::LocalResult::None
                    );
                    if exists {
                        assert_eq!((local.hour(), local.minute()), (*h, *m), "{s}: {fire}");
                    } else {
                        // Just after the gap: the first instant whose wall time is later.
                        let before = fire - Duration::seconds(1);
                        assert!(
                            before.with_timezone(&tz).naive_local() < requested,
                            "{s}: {fire} is not just after the gap"
                        );
                    }
                }
                previous = Some(fire);
            }
            assert!(count > 3, "{s}: {count} fires");
        }
    }
}
