//! The `-s` grammar reproduces Python's fire times (`tests/fixtures/schedule.json`, from
//! `gen_schedule.py`).

mod common;

use std::sync::Arc;

use meerschaum_core::value::Value;
use meerschaum_jobs::local_zone::LocalZone;
use meerschaum_jobs::schedule::{Fire, ParseEnv, isoformat, parse_schedule_in};

/// The zone of the cases that name none: the generator runs them under `TZ=UTC`.
const DEFAULT_TZ: &str = "UTC";

/// Zone-aware cases the fixed-offset triggers cannot match yet: v4's triggers carry the start's
/// `tzlocal()` and keep the local wall clock across a daylight-saving change (`datetime +
/// timedelta` on an aware value, `fromtimestamp(..., tz)`), while `Fire` is a fixed offset. The
/// fixture test skips these with a printed note; the parser already produces their first fire
/// time. Emptying this list is the goal of making triggers carry a zone.
const ZONE_AWARE_PENDING: &[(&str, &str)] = &[
    (
        "America/New_York",
        "0 21 * * * starting 2027-03-12 00:00 EDT",
    ),
    (
        "America/New_York",
        "0 21 * * * starting 2027-03-12 00:00 EST",
    ),
    (
        "America/New_York",
        "30 1 * * * starting 2027-11-05 00:00 EDT",
    ),
    (
        "America/New_York",
        "30 1 * * * starting 2027-11-05 00:00 EST",
    ),
    (
        "America/New_York",
        "30 2 * * * starting 2027-03-12 00:00 EDT",
    ),
    (
        "America/New_York",
        "30 2 * * * starting 2027-03-12 00:00 EST",
    ),
    (
        "America/New_York",
        "daily and mon-fri starting 2027-03-11 21:00 EDT",
    ),
    (
        "America/New_York",
        "daily and mon-fri starting 2027-03-11 21:00 EST",
    ),
    (
        "America/New_York",
        "daily and mon-fri starting 2027-11-04 21:00 EDT",
    ),
    (
        "America/New_York",
        "daily and mon-fri starting 2027-11-04 21:00 EST",
    ),
    (
        "America/New_York",
        "daily or mon starting 2027-03-12 21:00 EDT",
    ),
    (
        "America/New_York",
        "daily or mon starting 2027-03-12 21:00 EST",
    ),
    ("America/New_York", "daily starting 2026-11-01 01:30 EDT"),
    ("America/New_York", "daily starting 2026-11-01 01:30 EST"),
    ("America/New_York", "daily starting 2027-03-12 01:30 EDT"),
    ("America/New_York", "daily starting 2027-03-12 01:30 EST"),
    ("America/New_York", "daily starting 2027-03-12 02:30 EDT"),
    ("America/New_York", "daily starting 2027-03-12 02:30 EST"),
    ("America/New_York", "daily starting 2027-03-12 21:00 EDT"),
    ("America/New_York", "daily starting 2027-03-12 21:00 EST"),
    ("America/New_York", "daily starting 2027-11-05 01:30 EDT"),
    ("America/New_York", "daily starting 2027-11-05 01:30 EST"),
    ("America/New_York", "daily starting 2027-11-05 02:30 EDT"),
    ("America/New_York", "daily starting 2027-11-05 02:30 EST"),
    ("America/New_York", "daily starting 2027-11-05 21:00 EDT"),
    ("America/New_York", "daily starting 2027-11-05 21:00 EST"),
    (
        "America/New_York",
        "every 1 day and mon starting 2027-03-08 21:00 EDT",
    ),
    (
        "America/New_York",
        "every 1 day and mon starting 2027-03-08 21:00 EST",
    ),
    (
        "America/New_York",
        "every 12 hours starting 2027-03-13 09:00 EDT",
    ),
    (
        "America/New_York",
        "every 12 hours starting 2027-03-13 09:00 EST",
    ),
    (
        "America/New_York",
        "every 12 hours starting 2027-11-06 09:00 EDT",
    ),
    (
        "America/New_York",
        "every 12 hours starting 2027-11-06 09:00 EST",
    ),
    (
        "America/New_York",
        "every 2 days starting 2027-03-10 21:00 EDT",
    ),
    (
        "America/New_York",
        "every 2 days starting 2027-03-10 21:00 EST",
    ),
    (
        "America/New_York",
        "every 2 days starting 2027-11-03 21:00 EDT",
    ),
    (
        "America/New_York",
        "every 2 days starting 2027-11-03 21:00 EST",
    ),
    ("America/New_York", "hourly starting 2027-03-13 00:30 EST"),
    ("America/New_York", "hourly starting 2027-03-13 22:30 EDT"),
    ("America/New_York", "hourly starting 2027-03-13 22:30 EST"),
    ("America/New_York", "hourly starting 2027-11-06 00:30 EST"),
    ("America/New_York", "hourly starting 2027-11-06 22:30 EDT"),
    ("America/New_York", "hourly starting 2027-11-06 22:30 EST"),
    ("America/New_York", "mon-fri starting 2027-03-12 21:00 EDT"),
    ("America/New_York", "mon-fri starting 2027-03-12 21:00 EST"),
    ("America/New_York", "mon-fri starting 2027-11-05 21:00 EDT"),
    ("America/New_York", "mon-fri starting 2027-11-05 21:00 EST"),
    ("America/New_York", "monthly starting 2027-02-14 02:30 EDT"),
    ("America/New_York", "monthly starting 2027-02-14 02:30 EST"),
    ("America/New_York", "monthly starting 2027-02-20 21:00 EDT"),
    ("America/New_York", "monthly starting 2027-02-20 21:00 EST"),
    ("America/New_York", "monthly starting 2027-10-07 01:30 EDT"),
    ("America/New_York", "monthly starting 2027-10-07 01:30 EST"),
    ("America/New_York", "weekly starting 2027-03-01 21:00 EDT"),
    ("America/New_York", "weekly starting 2027-03-01 21:00 EST"),
    ("America/New_York", "weekly starting 2027-10-25 21:00 EDT"),
    ("America/New_York", "weekly starting 2027-10-25 21:00 EST"),
    ("America/New_York", "yearly starting 2026-03-14 02:30 EDT"),
    ("America/New_York", "yearly starting 2026-03-14 02:30 EST"),
];

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
    let env_for = |tz: &str| ParseEnv {
        local: Some(Arc::new(
            LocalZone::from_name(tz, as_of).expect("an IANA zone"),
        )),
        today,
    };
    let mut pending_skipped = 0;
    let cases = fixture.get("cases").and_then(Value::as_list).unwrap();
    assert!(cases.len() > 1000, "corpus too small: {}", cases.len());
    let mut mismatches = Vec::new();
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
        let pending = ZONE_AWARE_PENDING.contains(&(tz, schedule));
        // The fire sequences; a pending case may differ here but never in its first fire time.
        let mut sequence: Vec<String> = Vec::new();
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
                if Some(&fires(&got)) != case.get("fires") {
                    sequence.push(format!("{label}: fires {:?}", fires(&got)));
                    let want_first = case.get("fires").and_then(Value::as_list).map(|l| &l[0]);
                    if Some(&fires(&got[..1]))
                        != want_first.map(|f| Value::List(vec![f.clone()])).as_ref()
                    {
                        mismatches.push(format!("{label}: first fire {:?}", fires(&got[..1])));
                    }
                }
                for na in case.get("next_after").and_then(Value::as_list).unwrap() {
                    let after = parse_iso(na.get("after").and_then(Value::as_str).unwrap());
                    let mut t = parse_schedule_in(schedule, now, None, &env).unwrap().0;
                    let mut got = vec![t.next_after(after)];
                    got.push(t.next());
                    got.push(t.next());
                    if Some(&fires(&got)) != na.get("fires") {
                        sequence.push(format!(
                            "{label}: next_after({}) {:?} != {:?}",
                            isoformat(&after),
                            fires(&got),
                            na.get("fires")
                        ));
                    }
                }
            }
        }
        match (pending, sequence.is_empty()) {
            (false, _) => mismatches.extend(sequence),
            (true, false) => pending_skipped += 1,
            (true, true) => {
                println!("note: {label} matches now; remove it from ZONE_AWARE_PENDING")
            }
        }
    }
    println!(
        "note: {pending_skipped} zone-aware cases skipped (ZONE_AWARE_PENDING: fixed-offset \
         triggers keep the start's offset across daylight-saving changes)"
    );
    assert!(
        mismatches.is_empty(),
        "{} of {} cases differ:\n{}",
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
