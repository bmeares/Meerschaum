//! New pipes belong to their creator and primary team; who may change a pipe's access and locks.

use super::*;

#[tokio::test]
async fn new_pipes_belong_to_their_creator_and_primary_team() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    // An owner named by someone else than an administrator is the caller anyway.
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("plugin:data", "fresh"), "/register"),
        Some(json!({"columns": {"datetime": "ts"}, "access": {"owner": "otto"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let access = stored_access(&f, ("plugin:data", "fresh"));
    assert_eq!(access["owner"], "ed");
    assert_eq!(access["team"], "Data");
    // Only teams of one's own.
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("plugin:ops", "fresh"), "/register"),
        Some(json!({"access": {"team": "Data"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "You are not a member of the team 'Data'.");
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("plugin:data", "typo"), "/register"),
        Some(json!({"access": {"visibility": "secret"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    // An administrator gives a pipe to anyone.
    let r = call(
        &f,
        "POST",
        &admin,
        &url(("plugin:ops", "given"), "/register"),
        Some(json!({"access": {"owner": "otto", "team": "Ops"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let access = stored_access(&f, ("plugin:ops", "given"));
    assert_eq!(
        (access["owner"].clone(), access["team"].clone()),
        (json!("otto"), json!("Ops"))
    );
}

#[tokio::test]
async fn access_and_locks_are_changed_by_the_owner_the_managers_and_administrators() {
    let f = fixture().await;
    let (vera, ed, mia) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "mia").await,
    );
    let r = get(&f, &ed, &url(TEMPERATURE, "/access")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["success"], true);
    assert_eq!(r.json()["access"]["team"], "Data");
    assert_eq!(
        r.json()["may"],
        json!({"edit": true, "manage_access": false, "lock": false})
    );
    // A member who is not the owner changes neither the access nor the locks.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/access"),
        Some(json!({"visibility": "private"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // Nor through a parameter edit.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"locks": ["all"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"access": {"team": "Data", "visibility": "everyone"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // A viewer does not even hold `pipes:write`.
    let r = call(&f, "POST", &vera, &url(TEMPERATURE, "/lock"), None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // The team's manager does.
    let r = call(
        &f,
        "POST",
        &mia,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["locks"], json!(["sync"]));
    // The lock refuses everyone's syncs, and says how to unlock.
    let r = call(
        &f,
        "POST",
        &ed,
        &url(TEMPERATURE, "/data"),
        Some(json!([{"ts": "2026-01-01"}])),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(
        !ok && text.contains("it is locked (sync)") && text.contains("mrsm unlock pipes"),
        "{}",
        r.text()
    );
    // Other edits still work under a `sync` lock.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["c"]})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A `parameters` lock refuses edits but not the change of the locks themselves.
    let r = call(
        &f,
        "POST",
        &mia,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": "parameters"})),
    )
    .await;
    assert_eq!(
        r.json()["locks"],
        json!(["parameters", "sync"]),
        "{}",
        r.text()
    );
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["d"]})),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(
        !ok && text.contains("it is locked (parameters)"),
        "{}",
        r.text()
    );
    let r = call(&f, "POST", &mia, &url(TEMPERATURE, "/unlock"), None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["locks"], json!([]));

    // The manager makes the pipe private: the members who are not managers no longer see it.
    let r = call(
        &f,
        "PATCH",
        &mia,
        &url(TEMPERATURE, "/access"),
        Some(json!({"visibility": "private"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(
        get(&f, &vera, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // ... and gives it only to teams they are in.
    let r = call(
        &f,
        "PATCH",
        &mia,
        &url(TEMPERATURE, "/access"),
        Some(json!({"team": "Ops"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // The owner gives up ownership but gives the pipe to nobody else.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(SECRET_PIPE, "/access"),
        Some(json!({"owner": "vera"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(SECRET_PIPE, "/access"),
        Some(json!({"owner": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, SECRET_PIPE)["owner"], Json::Null);
    // No longer its owner, `ed` (a member, not a manager) cannot see the private pipe.
    assert_eq!(
        get(&f, &ed, &url(SECRET_PIPE, "/attributes")).await.status,
        StatusCode::NOT_FOUND
    );
}
