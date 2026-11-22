//! What each role may do to the fixture's pipes, and visibility after an owner's change.

use super::*;

#[tokio::test]
async fn roles_decide_what_each_user_may_do() {
    let f = fixture().await;
    let (vera, ed, opal, admin) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "opal").await,
        token(&f, "admin").await,
    );
    // A viewer reads.
    assert_eq!(
        get(&f, &vera, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::OK
    );
    let r = call(
        &f,
        "POST",
        &vera,
        &url(("plugin:data", "new"), "/register"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(message(&r), "Missing required scope: 'pipes:register'");
    let r = call(
        &f,
        "PATCH",
        &vera,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["a"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(message(&r), "Missing required scope: 'pipes:write'");
    // An editor (the default role) changes pipes, but deletes no rows.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["a"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(tuple_of(&r).0, "{}", r.text());
    for auth in [&ed, &opal] {
        let r = call(&f, "DELETE", auth, &url(TEMPERATURE, "/clear"), None).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN);
        assert_eq!(message(&r), "Missing required scope: 'pipes:delete'");
    }
    let r = call(&f, "DELETE", &admin, &url(TEMPERATURE, "/clear"), None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    // Actions need `actions:execute` (operators); the console's pipe operations do not.
    let execute = |sysargs: Json| Some(json!({"sysargs": sysargs}));
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!(["show", "version"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "Missing required scope: 'actions:execute'");
    let r = call(
        &f,
        "POST",
        &opal,
        "/execute",
        execute(json!(["show", "version"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!([
            "sync",
            "pipes",
            "-c",
            "plugin:data",
            "-m",
            "temperature"
        ])),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // ... on pipes the caller may change: every pipe includes a hidden one and one whose owner
    // keeps the changes.
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!(["sync", "pipes"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        message(&r).contains("selects 2 pipes you may not change"),
        "{}",
        r.text()
    );
    // A viewer runs nothing.
    let r = call(
        &f,
        "POST",
        &vera,
        "/execute",
        execute(json!(["sync", "pipes", "-c", "plugin:shared"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // `/actions/{action}`: the console's policy and the same pipe checks for non-administrators.
    let r = call(
        &f,
        "POST",
        &ed,
        "/actions/show",
        Some(json!({"action": ["version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/sh",
        Some(json!({"action": ["id"]})),
    )
    .await;
    assert!(!tuple_of(&r).0, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["config"]})),
    )
    .await;
    assert!(!tuple_of(&r).0, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["pipes"]})),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(!ok && text.contains("you may not see"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["pipes"], "connector_keys": ["plugin:shared"]})),
    )
    .await;
    assert_eq!(tuple_of(&r), (true, "ran show".to_string()), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/actions/sh",
        Some(json!({"action": ["id"]})),
    )
    .await;
    assert_eq!(tuple_of(&r), (true, "ran sh".to_string()), "{}", r.text());
}

#[tokio::test]
async fn a_pipe_changed_by_its_owner_is_still_seen_by_everyone() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let edit = Some(json!({"tags": ["b"]}));
    assert_eq!(
        get(&f, &ed, &url(PUBLIC, "/attributes")).await.status,
        StatusCode::OK
    );
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(PUBLIC, "/edit?patch=true"),
        edit.clone(),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("You cannot change"), "{}", r.text());
    let r = get(&f, &ed, &url(PUBLIC, "/access")).await;
    assert_eq!(
        r.json()["may"],
        json!({"edit": false, "manage_access": false, "lock": false})
    );
    for auth in [&otto, &admin] {
        let r = call(
            &f,
            "PATCH",
            auth,
            &url(PUBLIC, "/edit?patch=true"),
            edit.clone(),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        assert!(tuple_of(&r).0, "{}", r.text());
    }
}
