//! The `-s` grammar reproduces Python's fire times (`tests/fixtures/schedule.json`, from
//! `gen_schedule.py`).

mod common;

use meerschaum_core::value::Value;
use meerschaum_jobs::schedule::{Fire, isoformat, parse_schedule};

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
    let n = fixture.get("num_fires").and_then(Value::as_i64).unwrap() as usize;
    let cases = fixture.get("cases").and_then(Value::as_list).unwrap();
    assert!(cases.len() > 1000, "corpus too small: {}", cases.len());
    let mut mismatches = Vec::new();
    for case in cases {
        let schedule = case.get("schedule").and_then(Value::as_str).unwrap();
        let now = parse_iso(case.get("now").and_then(Value::as_str).unwrap());
        let label = format!("{schedule:?} @ {}", isoformat(&now));
        match (parse_schedule(schedule, now), case.get("error")) {
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
                    mismatches.push(format!("{label}: fires {:?}", fires(&got)));
                }
                for na in case.get("next_after").and_then(Value::as_list).unwrap() {
                    let after = parse_iso(na.get("after").and_then(Value::as_str).unwrap());
                    let mut t = parse_schedule(schedule, now).unwrap();
                    let mut got = vec![t.next_after(after)];
                    got.push(t.next());
                    got.push(t.next());
                    if Some(&fires(&got)) != na.get("fires") {
                        mismatches.push(format!(
                            "{label}: next_after({}) {:?} != {:?}",
                            isoformat(&after),
                            fires(&got),
                            na.get("fires")
                        ));
                    }
                }
            }
        }
    }
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
