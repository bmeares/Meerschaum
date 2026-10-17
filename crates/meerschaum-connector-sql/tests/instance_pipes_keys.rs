//! `fetch_pipes_keys` (`_pipes.py:148`) against the registered pipes: connector / metric /
//! location filters with `_` negation, `None` locations, tags (`,`-ANDed groups, ORed across
//! groups, `_` negation), and the dict return form carrying the parameters.

mod common;

use common::*;
use meerschaum_connector::{PipesKeys, PipesKeysQuery};

fn keys(pk: &PipesKeys, prefix: &str) -> Vec<String> {
    pk.entries()
        .iter()
        .filter(|e| e.connector_keys.starts_with(prefix))
        .map(|e| {
            format!(
                "{}:{}:{}",
                e.connector_keys.trim_start_matches(prefix),
                e.metric_key,
                e.location_key.as_deref().unwrap_or("None")
            )
        })
        .collect()
}

flavor_tests!(test_fetch_pipes_keys, |c: Ctx| async move {
    let p = &c.prefix;
    let pipes = [
        c.pipe("a", "m1", None, r#"{"tags": ["x", "y"]}"#),
        c.pipe("a", "m2", Some("l1"), r#"{"tags": ["x"]}"#),
        c.pipe(
            "b",
            "m1",
            Some("l2"),
            r#"{"tags": ["y", "z"], "columns": {"id": "id"}}"#,
        ),
    ];
    for pipe in &pipes {
        ok(c.conn.register_pipe(pipe).await);
    }
    let ck = |s: &str| format!("{p}{s}");
    let run = |q: PipesKeysQuery| {
        let conn = &c.conn;
        async move { conn.fetch_pipes_keys(&q).await.unwrap() }
    };
    let both = vec![ck("a"), ck("b")];

    // All of this test's pipes, ordered by connector, metric, location (NULLs first).
    let all = run(PipesKeysQuery {
        connector_keys: both.clone(),
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&all, p), vec!["a:m1:None", "a:m2:l1", "b:m1:l2"]);
    // The dict form carries the parameters.
    let PipesKeys::Dict(d) = &all else {
        panic!("expected the dict form")
    };
    let first = d.values().find(|e| e.metric_key == "m1").unwrap();
    assert_eq!(
        first.extra.as_ref().unwrap().get("tags").unwrap(),
        &meerschaum_core::Value::from_json_str(r#"["x", "y"]"#).unwrap()
    );

    // Connector negation.
    let neg = run(PipesKeysQuery {
        connector_keys: vec![ck("a"), format!("_{}", ck("a")), ck("b")],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&neg, p), vec!["b:m1:l2"]);

    // Metric and location filters; `None` matches null locations.
    let m1 = run(PipesKeysQuery {
        connector_keys: both.clone(),
        metric_keys: vec!["m1".into()],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&m1, p), vec!["a:m1:None", "b:m1:l2"]);
    let null_loc = run(PipesKeysQuery {
        connector_keys: both.clone(),
        location_keys: vec![None],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&null_loc, p), vec!["a:m1:None"]);
    let not_l1 = run(PipesKeysQuery {
        connector_keys: both.clone(),
        location_keys: vec![Some("_l1".into())],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&not_l1, p), vec!["a:m1:None", "b:m1:l2"]);

    // Tags: `x` OR `z`; `x,y` ANDed; `_y` excludes.
    let tag_or = run(PipesKeysQuery {
        connector_keys: both.clone(),
        tags: vec!["x".into(), "z".into()],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&tag_or, p), vec!["a:m1:None", "a:m2:l1", "b:m1:l2"]);
    let tag_and = run(PipesKeysQuery {
        connector_keys: both.clone(),
        tags: vec!["x,y".into()],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&tag_and, p), vec!["a:m1:None"]);
    let tag_not = run(PipesKeysQuery {
        connector_keys: both.clone(),
        tags: vec!["_y".into()],
        ..Default::default()
    })
    .await;
    assert_eq!(keys(&tag_not, p), vec!["a:m2:l1"]);

    for pipe in &pipes {
        ok(c.conn.delete_pipe(pipe).await);
    }
    let none = run(PipesKeysQuery {
        connector_keys: both.clone(),
        ..Default::default()
    })
    .await;
    assert!(keys(&none, p).is_empty());
});
