//! 5.1: every native action and subaction is either allowed to a caller who may not run system
//! commands (`execute_policy::NON_ADMIN_COMMANDS`) or refused to them
//! (`execute_policy::NON_ADMIN_REFUSED`), never both and never neither, and neither list names a
//! command the CLI does not have. A new action fails this test until someone decides.

use meerschaum_server::execute_policy::{NON_ADMIN_COMMANDS, NON_ADMIN_REFUSED};

/// Every `(action, subaction)` of the CLI (`""`: the action on its own).
fn native_pairs() -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for action in meerschaum_cli::actions::CORE_ACTIONS {
        // The action on its own (`help`, `sh`, or `show` with no subaction).
        pairs.push((action.name.to_string(), String::new()));
        for sub in action.subactions {
            pairs.push((action.name.to_string(), sub.name.to_string()));
        }
    }
    pairs
}

fn listed(list: &[(&str, &[&str])]) -> Vec<(String, String)> {
    list.iter()
        .flat_map(|(action, subs)| {
            subs.iter()
                .map(move |s| (action.to_string(), s.to_string()))
        })
        .collect()
}

#[test]
fn every_native_command_is_allowed_or_refused_to_non_administrators() {
    let native = native_pairs();
    let allowed = listed(NON_ADMIN_COMMANDS);
    let refused = listed(NON_ADMIN_REFUSED);
    let neither: Vec<String> = native
        .iter()
        .filter(|pair| !allowed.contains(pair) && !refused.contains(pair))
        .map(|(a, s)| format!("(\"{a}\", \"{s}\")"))
        .collect();
    assert!(
        neither.is_empty(),
        "native commands on neither list (decide, then add each to NON_ADMIN_COMMANDS or \
         NON_ADMIN_REFUSED):\n{}",
        neither.join("\n")
    );
    let both: Vec<_> = allowed.iter().filter(|p| refused.contains(p)).collect();
    assert!(both.is_empty(), "on both lists: {both:?}");
    let stale: Vec<_> = allowed
        .iter()
        .chain(&refused)
        .filter(|pair| !native.contains(pair))
        .collect();
    assert!(
        stale.is_empty(),
        "listed but not a native command: {stale:?}"
    );
}

#[test]
fn the_allowlist_decides_with_the_dispatchers_words() {
    use meerschaum_server::execute_policy::non_admin_may_run;
    assert!(non_admin_may_run("show", Some("pipes")));
    assert!(non_admin_may_run("show", Some("pipe")));
    assert!(non_admin_may_run("help", None));
    for (action, sub) in [
        ("show", None),
        ("show", Some("config")),
        ("show", Some("users")),
        ("show", Some("runs")),
        ("show", Some("graph")),
        ("copy", Some("pipes")),
        ("register", Some("pipelines")),
        ("edit", Some("pipelines")),
        ("python", None),
        ("sh", None),
        ("start", Some("jobs")),
    ] {
        assert!(!non_admin_may_run(action, sub), "{action} {sub:?}");
    }
}
