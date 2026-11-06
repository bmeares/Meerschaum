//! A job written by Meerschaum 4 keeps its command: v4 stored the sysargs only in `pickle.pkl`
//! (with an empty label for jobs created through the API), which `v4_pickle` reads as data.
//! Fixture: `fixtures/v4_pickles.json` (`gen_v4_pickles.py`, run by Meerschaum 4.1.4).

mod common;

use meerschaum_core::value::Value;
use meerschaum_jobs::job::Job;
use meerschaum_jobs::{JobsContext, v4_pickle};

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

struct Case {
    name: String,
    sysargs: Option<Vec<String>>,
    pickle: Vec<u8>,
}

fn cases() -> Vec<Case> {
    let fixture = common::load_fixture("v4_pickles.json");
    fixture
        .get("cases")
        .and_then(Value::as_list)
        .expect("cases")
        .iter()
        .map(|case| Case {
            name: case.get("name").unwrap().py_str(),
            sysargs: case
                .get("sysargs")
                .and_then(Value::as_list)
                .map(|args| args.iter().map(Value::py_str).collect()),
            pickle: hex(&case.get("pickle_hex").unwrap().py_str()),
        })
        .collect()
}

#[test]
fn reads_the_sysargs_of_every_v4_job_pickle() {
    let cases = cases();
    assert!(cases.len() >= 4);
    for case in cases {
        assert_eq!(
            v4_pickle::sysargs_from_bytes(&case.pickle),
            case.sysargs,
            "{}",
            case.name
        );
    }
}

#[test]
fn a_damaged_pickle_reads_as_nothing() {
    for case in cases() {
        for cut in [1, case.pickle.len() / 2, case.pickle.len() - 1] {
            assert_eq!(v4_pickle::sysargs_from_bytes(&case.pickle[..cut]), None);
        }
    }
}

/// A v4 job directory as the API left it: properties without a target, an empty label.
fn v4_job(ctx: &JobsContext, case: &Case) {
    let dir = ctx.paths.jobs_dir.join(&case.name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("properties.json"),
        r#"{"restart": true, "label": "", "result": [true, "Exited."], "process": {"began": "2026-11-05T03:45:29.954852", "ended": "2026-11-05T03:50:47.574852"}}"#,
    )
    .unwrap();
    std::fs::write(dir.join("pickle.pkl"), &case.pickle).unwrap();
}

#[test]
fn a_v4_job_opens_with_the_command_from_its_pickle() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    for case in cases() {
        v4_job(&ctx, &case);
        let mut job = Job::open(&case.name, &ctx).unwrap();
        let expected = case.sysargs.clone().unwrap_or_default();
        assert_eq!(job.sysargs(), expected, "{}", case.name);
    }
}

#[test]
fn a_persisted_target_wins_over_the_pickle() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    let case = cases().into_iter().next().unwrap();
    v4_job(&ctx, &case);
    std::fs::write(
        ctx.paths.jobs_dir.join(&case.name).join("properties.json"),
        r#"{"label": "", "target": {"name": "entry", "module": "meerschaum._internal.entry", "args": [["show", "version"]], "kw": {}}}"#,
    )
    .unwrap();
    let mut job = Job::open(&case.name, &ctx).unwrap();
    assert_eq!(
        job.sysargs(),
        vec!["show".to_string(), "version".to_string()]
    );
}

#[test]
fn an_empty_persisted_command_falls_back_to_the_pickle() {
    // 5.0.0 started v4 jobs it could not read and persisted `args: [[]]`.
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    let case = cases().into_iter().next().unwrap();
    v4_job(&ctx, &case);
    std::fs::write(
        ctx.paths.jobs_dir.join(&case.name).join("properties.json"),
        r#"{"restart": true, "label": "", "target": {"name": "entry", "module": "meerschaum._internal.entry", "args": [[]], "kw": {}}}"#,
    )
    .unwrap();
    let mut job = Job::open(&case.name, &ctx).unwrap();
    assert_eq!(job.sysargs(), case.sysargs.unwrap());
}
