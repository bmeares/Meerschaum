//! The `-s` grammar reproduces Python's fire times (`tests/fixtures/schedule.json`, from
//! `gen_schedule.py`), except at the clock changes listed in `CLOCK_CHANGE_DEVIATIONS`.

use crate::common;
use meerschaum_core::value::Value;
use meerschaum_jobs::schedule::{Fire, ParseEnv, isoformat, parse_schedule_in};

/// The zone of the cases that name none: the generator runs them under `TZ=UTC`.
const DEFAULT_TZ: &str = "UTC";

/// Cases where v5 deliberately differs from v4 at a clock change (`PARITY.md`
/// 51-SCHEDULE-TZNAME). The fixture keeps v4's values; the test checks v4's fire times before
/// `from` and these hand-written v5 values from `from` on (`from: None`: the fire times all match
/// and only `next_after` differs), and `next_after` against the listed values where given.
struct Deviation {
    tz: &'static str,
    /// The fixture schedules (the same case with each abbreviation of the zone).
    schedules: &'static [&'static str],
    /// The first fire index at which v5 differs.
    from: Option<usize>,
    /// v5's fire times from `from` on.
    fires: &'static [&'static str],
    /// v5's `next_after(after)` and the two `next()` after it, where they differ from v4.
    next_after: &'static [(&'static str, [&'static str; 3])],
    /// Why.
    #[allow(dead_code)]
    reason: &'static str,
}

const NY: &str = "America/New_York";

const CLOCK_CHANGE_DEVIATIONS: &[Deviation] = &[
    Deviation {
        tz: NY,
        schedules: &[
            "daily starting 2026-03-08 02:30 EDT",
            "daily starting 2026-03-08 02:30 EST",
        ],
        from: Some(0),
        fires: &[
            "2026-03-08T03:00:00-04:00",
            "2026-03-09T02:30:00-04:00",
            "2026-03-10T02:30:00-04:00",
            "2026-03-11T02:30:00-04:00",
        ],
        next_after: &[
            (
                "2025-03-14T18:34:05.089793+00:00",
                [
                    "2026-03-08T03:00:00-04:00",
                    "2026-03-09T02:30:00-04:00",
                    "2026-03-10T02:30:00-04:00",
                ],
            ),
            (
                "2024-02-08T15:26:53.589793+00:00",
                [
                    "2026-03-08T03:00:00-04:00",
                    "2026-03-09T02:30:00-04:00",
                    "2026-03-10T02:30:00-04:00",
                ],
            ),
        ],
        reason: "a missing 02:30 fires just after the gap, not at 02:30-04:00 (an hour before it)",
    },
    Deviation {
        tz: NY,
        schedules: &["daily starting 2026-11-01 01:30 EST"],
        from: None,
        fires: &[],
        next_after: &[
            (
                "2025-03-14T18:34:05.089793+00:00",
                [
                    "2026-11-01T01:30:00-05:00",
                    "2026-11-02T01:30:00-05:00",
                    "2026-11-03T01:30:00-05:00",
                ],
            ),
            (
                "2024-02-08T15:26:53.589793+00:00",
                [
                    "2026-11-01T01:30:00-05:00",
                    "2026-11-02T01:30:00-05:00",
                    "2026-11-03T01:30:00-05:00",
                ],
            ),
        ],
        reason: "next_after before the start is the start (v4: the first 01:30, an hour before it)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "daily starting 2027-03-12 02:30 EDT",
            "daily starting 2027-03-12 02:30 EST",
        ],
        from: Some(2),
        fires: &[
            "2027-03-14T03:00:00-04:00",
            "2027-03-15T02:30:00-04:00",
            "2027-03-16T02:30:00-04:00",
            "2027-03-17T02:30:00-04:00",
            "2027-03-18T02:30:00-04:00",
            "2027-03-19T02:30:00-04:00",
            "2027-03-20T02:30:00-04:00",
            "2027-03-21T02:30:00-04:00",
        ],
        next_after: &[
            (
                "2027-03-12T15:07:11.500000+00:00",
                [
                    "2027-03-13T02:30:00-05:00",
                    "2027-03-14T03:00:00-04:00",
                    "2027-03-15T02:30:00-04:00",
                ],
            ),
            (
                "2026-02-05T12:00:00+00:00",
                [
                    "2027-03-12T02:30:00-05:00",
                    "2027-03-13T02:30:00-05:00",
                    "2027-03-14T03:00:00-04:00",
                ],
            ),
        ],
        reason: "a missing 02:30 fires just after the gap, not at 02:30-04:00 (an hour before it)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "hourly starting 2027-03-13 22:30 EDT",
            "hourly starting 2027-03-13 22:30 EST",
        ],
        from: Some(4),
        fires: &[
            "2027-03-14T03:30:00-04:00",
            "2027-03-14T04:30:00-04:00",
            "2027-03-14T05:30:00-04:00",
            "2027-03-14T06:30:00-04:00",
            "2027-03-14T07:30:00-04:00",
            "2027-03-14T08:30:00-04:00",
            "2027-03-14T09:30:00-04:00",
            "2027-03-14T10:30:00-04:00",
        ],
        next_after: &[],
        reason: "the missing 02:30 is skipped (v4 fires 02:30-04:00, the same instant as 01:30-05:00)",
    },
    Deviation {
        tz: NY,
        schedules: &["hourly starting 2027-03-13 00:30 EST"],
        from: Some(26),
        fires: &[
            "2027-03-14T03:30:00-04:00",
            "2027-03-14T04:30:00-04:00",
            "2027-03-14T05:30:00-04:00",
            "2027-03-14T06:30:00-04:00",
        ],
        next_after: &[],
        reason: "the missing 02:30 is skipped (v4 fires 02:30-04:00, the same instant as 01:30-05:00)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "hourly starting 2027-11-06 22:30 EDT",
            "hourly starting 2027-11-06 22:30 EST",
        ],
        from: Some(4),
        fires: &[
            "2027-11-07T01:30:00-05:00",
            "2027-11-07T02:30:00-05:00",
            "2027-11-07T03:30:00-05:00",
            "2027-11-07T04:30:00-05:00",
            "2027-11-07T05:30:00-05:00",
            "2027-11-07T06:30:00-05:00",
            "2027-11-07T07:30:00-05:00",
            "2027-11-07T08:30:00-05:00",
        ],
        next_after: &[],
        reason: "the repeated 01:30 fires in both occurrences (v4 leaves a two-hour gap)",
    },
    Deviation {
        tz: NY,
        schedules: &["hourly starting 2027-11-06 00:30 EST"],
        from: Some(26),
        fires: &[
            "2027-11-07T01:30:00-05:00",
            "2027-11-07T02:30:00-05:00",
            "2027-11-07T03:30:00-05:00",
            "2027-11-07T04:30:00-05:00",
        ],
        next_after: &[],
        reason: "the repeated 01:30 fires in both occurrences (v4 leaves a two-hour gap)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "monthly starting 2027-02-14 02:30 EDT",
            "monthly starting 2027-02-14 02:30 EST",
        ],
        from: Some(1),
        fires: &[
            "2027-03-14T03:00:00-04:00",
            "2027-04-14T02:30:00-04:00",
            "2027-05-14T02:30:00-04:00",
            "2027-06-14T02:30:00-04:00",
            "2027-07-14T02:30:00-04:00",
            "2027-08-14T02:30:00-04:00",
            "2027-09-14T02:30:00-04:00",
            "2027-10-14T02:30:00-04:00",
            "2027-11-14T02:30:00-05:00",
        ],
        next_after: &[
            (
                "2027-03-12T15:07:11.500000+00:00",
                [
                    "2027-03-14T03:00:00-04:00",
                    "2027-04-14T02:30:00-04:00",
                    "2027-05-14T02:30:00-04:00",
                ],
            ),
            (
                "2026-02-05T12:00:00+00:00",
                [
                    "2027-02-14T02:30:00-05:00",
                    "2027-03-14T03:00:00-04:00",
                    "2027-04-14T02:30:00-04:00",
                ],
            ),
        ],
        reason: "March fires just after the gap (v4 skips the month whose 02:30 is missing)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "yearly starting 2026-03-14 02:30 EDT",
            "yearly starting 2026-03-14 02:30 EST",
        ],
        from: Some(1),
        fires: &[
            "2027-03-14T03:00:00-04:00",
            "2028-03-14T02:30:00-04:00",
            "2029-03-14T02:30:00-04:00",
        ],
        next_after: &[
            (
                "2027-03-12T15:07:11.500000+00:00",
                [
                    "2027-03-14T03:00:00-04:00",
                    "2028-03-14T02:30:00-04:00",
                    "2029-03-14T02:30:00-04:00",
                ],
            ),
            (
                "2026-02-05T12:00:00+00:00",
                [
                    "2026-03-14T02:30:00-04:00",
                    "2027-03-14T03:00:00-04:00",
                    "2028-03-14T02:30:00-04:00",
                ],
            ),
        ],
        reason: "2027 fires just after the gap (v4 skips the year whose 02:30 is missing)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "30 2 * * * starting 2027-03-12 00:00 EDT",
            "30 2 * * * starting 2027-03-12 00:00 EST",
        ],
        from: Some(2),
        fires: &[
            "2027-03-14T03:00:00-04:00",
            "2027-03-15T02:30:00-04:00",
            "2027-03-16T02:30:00-04:00",
            "2027-03-17T02:30:00-04:00",
            "2027-03-18T02:30:00-04:00",
            "2027-03-19T02:30:00-04:00",
        ],
        next_after: &[
            (
                "2027-03-12T15:07:11.500000+00:00",
                [
                    "2027-03-13T02:30:00-05:00",
                    "2027-03-14T03:00:00-04:00",
                    "2027-03-15T02:30:00-04:00",
                ],
            ),
            (
                "2026-02-05T12:00:00+00:00",
                [
                    "2027-03-12T02:30:00-05:00",
                    "2027-03-13T02:30:00-05:00",
                    "2027-03-14T03:00:00-04:00",
                ],
            ),
        ],
        reason: "a specific hour fires once just after the gap (v4 skips the day)",
    },
    Deviation {
        tz: NY,
        schedules: &[
            "30 1 * * * starting 2027-11-05 00:00 EDT",
            "30 1 * * * starting 2027-11-05 00:00 EST",
        ],
        from: Some(3),
        fires: &[
            "2027-11-08T01:30:00-05:00",
            "2027-11-09T01:30:00-05:00",
            "2027-11-10T01:30:00-05:00",
            "2027-11-11T01:30:00-05:00",
            "2027-11-12T01:30:00-05:00",
        ],
        next_after: &[(
            "2027-11-05T15:07:11.500000+00:00",
            [
                "2027-11-06T01:30:00-04:00",
                "2027-11-07T01:30:00-04:00",
                "2027-11-08T01:30:00-05:00",
            ],
        )],
        reason: "a specific hour fires a repeated 01:30 once (v4 fires it in both occurrences)",
    },
];

fn deviation(tz: &str, schedule: &str) -> Option<&'static Deviation> {
    CLOCK_CHANGE_DEVIATIONS
        .iter()
        .find(|d| d.tz == tz && d.schedules.contains(&schedule))
}

fn parse_iso(s: &str) -> Fire {
    chrono::DateTime::parse_from_rfc3339(s).expect("isoformat")
}

fn fires(list: &[Option<Fire>]) -> Value {
    Value::List(
        list.iter()
            .map(|f| f.map(|f| Value::Str(isoformat(&f))).unwrap_or(Value::Null))
            .collect(),
    )
}

#[test]
fn schedule_corpus_matches_python() {
    let fixture = common::load_fixture("schedule.json");
    let default_n = fixture.get("num_fires").and_then(Value::as_i64).unwrap() as usize;
    // The process zone and date the generator pinned (`TZ`); dateutil reads both from them.
    let as_of = chrono::DateTime::parse_from_rfc3339("2026-11-14T12:00:00+00:00")
        .unwrap()
        .to_utc();
    let today = chrono::NaiveDate::from_ymd_opt(2026, 11, 14).unwrap();
    let env_for = |tz: &str| ParseEnv::in_zone(tz, as_of, today);
    let cases = fixture.get("cases").and_then(Value::as_list).unwrap();
    assert!(cases.len() > 1000, "corpus too small: {}", cases.len());
    let mut mismatches = Vec::new();
    let mut deviations_seen = 0;
    for case in cases {
        let schedule = case.get("schedule").and_then(Value::as_str).unwrap();
        let now = parse_iso(case.get("now").and_then(Value::as_str).unwrap());
        let tz = case.get("tz").and_then(Value::as_str).unwrap_or(DEFAULT_TZ);
        let n = case
            .get("num_fires")
            .and_then(Value::as_i64)
            .map_or(default_n, |n| n as usize);
        let env = env_for(tz);
        let label = format!("{schedule:?} @ {} (TZ={tz})", isoformat(&now));
        let dev = deviation(tz, schedule);
        deviations_seen += usize::from(dev.is_some());
        let parsed = parse_schedule_in(schedule, now, None, &env);
        if let Ok((_, warnings)) = &parsed {
            // dateutil's `UnknownTimezoneWarning`: the same abbreviation, once per parse.
            let expected: Vec<&str> = case
                .get("warnings")
                .and_then(Value::as_list)
                .map(|l| l.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let lead = |m: &str| m.split(".  ").next().unwrap_or(m).to_string();
            let got: Vec<String> = warnings.iter().map(|w| lead(w)).collect();
            let want: Vec<String> = expected.iter().map(|w| lead(w)).collect();
            if got != want {
                mismatches.push(format!("{label}: warnings {got:?} != {want:?}"));
            }
        }
        match (parsed.map(|(trigger, _)| trigger), case.get("error")) {
            (Err(e), Some(err)) => {
                if Some(e.py_message().as_str()) != err.get("message").and_then(Value::as_str) {
                    mismatches.push(format!("{label}: error {:?} != {err:?}", e.py_message()));
                }
            }
            (Err(e), None) => mismatches.push(format!("{label}: unexpected error {e}")),
            (Ok(_), Some(err)) => mismatches.push(format!("{label}: expected error {err:?}")),
            (Ok(mut trigger), None) => {
                if Some(trigger.type_name()) != case.get("type").and_then(Value::as_str) {
                    mismatches.push(format!("{label}: type {}", trigger.type_name()));
                }
                let got: Vec<Option<Fire>> = (0..n).map(|_| trigger.next()).collect();
                let v4 = case.get("fires").and_then(Value::as_list).unwrap();
                let want = match dev.and_then(|d| d.from.map(|from| (d, from))) {
                    None => Value::List(v4.clone()),
                    Some((d, from)) => {
                        // v4 up to the clock change, v5's own values from there.
                        assert_eq!(from + d.fires.len(), n, "{label}: deviation length");
                        Value::List(
                            v4[..from]
                                .iter()
                                .cloned()
                                .chain(d.fires.iter().map(|f| Value::Str(f.to_string())))
                                .collect(),
                        )
                    }
                };
                if fires(&got) != want {
                    mismatches.push(format!("{label}: fires {:?} != {want:?}", fires(&got)));
                }
                if let Some((d, from)) = dev.and_then(|d| d.from.map(|from| (d, from))) {
                    // The v5 values replace v4's: make sure they really differ there.
                    assert_ne!(
                        Value::Str(d.fires[0].to_string()),
                        v4[from],
                        "{label}: not a deviation at {from}"
                    );
                }
                for na in case.get("next_after").and_then(Value::as_list).unwrap() {
                    let after_text = na.get("after").and_then(Value::as_str).unwrap();
                    let after = parse_iso(after_text);
                    let mut t = parse_schedule_in(schedule, now, None, &env).unwrap().0;
                    let mut got = vec![t.next_after(after)];
                    got.push(t.next());
                    got.push(t.next());
                    let want = dev
                        .and_then(|d| d.next_after.iter().find(|(a, _)| *a == after_text))
                        .map(|(_, v5)| {
                            Value::List(v5.iter().map(|f| Value::Str(f.to_string())).collect())
                        })
                        .unwrap_or_else(|| na.get("fires").cloned().unwrap_or(Value::Null));
                    if fires(&got) != want {
                        mismatches.push(format!(
                            "{label}: next_after({}) {:?} != {want:?}",
                            isoformat(&after),
                            fires(&got),
                        ));
                    }
                }
            }
        }
    }
    let listed: usize = CLOCK_CHANGE_DEVIATIONS
        .iter()
        .map(|d| d.schedules.len())
        .sum();
    assert_eq!(
        deviations_seen, listed,
        "a listed deviation is not in the fixture"
    );
    assert!(
        mismatches.is_empty(),
        "{} mismatches over {} cases:\n{}",
        mismatches.len(),
        cases.len(),
        mismatches
            .iter()
            .take(25)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
