//! The commands a non-administrator may run on their own objects, and the permissions page that
//! lists them.

use super::*;

/// 5.1, deny by default: each command a non-administrator may run, through
/// `POST /execute/validate` (the decision `POST /execute`, `POST /jobs`, `POST /actions`, MCP and
/// the restricted web terminal share), on the objects they may act on with the route's
/// permission, refused on the others; every other command refused; administrators unaffected.
#[tokio::test]
async fn non_administrators_run_only_the_allowed_commands_on_their_own_objects() {
    let f = fixture().await;
    let admin = token(&f, "admin").await;
    let opal = token(&f, "opal").await; // Operator, Data.
    let oscar = token(&f, "oscar").await; // Every permission, Ops.
    let validate = |auth: String, sysargs: Vec<&str>| {
        let f = &f;
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        async move {
            call(
                f,
                "POST",
                &auth,
                "/execute/validate",
                Some(json!({"sysargs": sysargs})),
            )
            .await
        }
    };
    let status = |auth: &String, sysargs: Vec<&str>| {
        let future = validate(auth.clone(), sysargs);
        async move { future.await.status }
    };
    let ok = StatusCode::OK;
    let no = StatusCode::FORBIDDEN;
    let data = ["-c", TEMPERATURE.0, "-m", TEMPERATURE.1];
    let ops = ["-c", PRESSURE.0, "-m", PRESSURE.1];
    let with = |words: &[&str], keys: &[&str]| -> Vec<String> {
        words.iter().chain(keys).map(|s| s.to_string()).collect()
    };

    // Informational commands reveal nothing about anyone's objects.
    for sysargs in [
        vec!["show", "version"],
        vec!["show", "help"],
        vec!["show", "actions"],
        vec!["show", "arguments"],
        vec!["show", "schedules", "every 1 minute"],
        vec!["help"],
    ] {
        assert_eq!(status(&opal, sysargs.clone()).await, ok, "{sysargs:?}");
    }

    // Reads (`pipes:read`): the selected pipes must be readable.
    for sub in [
        "pipes",
        "data",
        "columns",
        "rowcounts",
        "sizes",
        "partitions",
        "targets",
        "tags",
    ] {
        let words = ["show", sub];
        let mine = with(&words, &data);
        let theirs = with(&words, &ops);
        let mine: Vec<&str> = mine.iter().map(String::as_str).collect();
        let theirs: Vec<&str> = theirs.iter().map(String::as_str).collect();
        assert_eq!(status(&opal, mine.clone()).await, ok, "{mine:?}");
        assert_eq!(status(&opal, theirs.clone()).await, no, "{theirs:?}");
    }
    // Writes (`pipes:write`): the selected pipes must be changeable.
    for words in [
        &["sync", "pipes"][..],
        &["verify", "pipes"],
        &["verify", "rowcounts"],
        &["verify", "schemas"],
        &["deduplicate", "pipes"],
        &["edit", "pipes", "-P", "{\"tags\": [\"daily\"]}"],
        &["partition", "pipes"],
        &["tag", "pipes", "daily"],
    ] {
        let mine = with(words, &data);
        let theirs = with(words, &ops);
        let mine: Vec<&str> = mine.iter().map(String::as_str).collect();
        let theirs: Vec<&str> = theirs.iter().map(String::as_str).collect();
        assert_eq!(status(&opal, mine.clone()).await, ok, "{mine:?}");
        assert_eq!(status(&opal, theirs.clone()).await, no, "{theirs:?}");
    }
    // Locks need the owner, a manager or an administrator (`pipes:write`).
    let lock: Vec<&str> = ["lock", "pipes"].iter().chain(&data).copied().collect();
    assert_eq!(status(&opal, lock).await, no);
    // Deleting needs `pipes:delete` / `pipes:drop` (an Operator has neither) on changeable pipes.
    for words in [
        &["clear", "pipes"][..],
        &["drop", "pipes"],
        &["delete", "pipes"],
    ] {
        let mine = with(words, &data);
        let mine: Vec<&str> = mine.iter().map(String::as_str).collect();
        let r = validate(opal.clone(), mine.clone()).await;
        assert_eq!(r.status, no, "{mine:?}");
        assert!(
            message(&r).contains("Missing required scope"),
            "{}",
            message(&r)
        );
        let theirs = with(words, &data);
        let ops_own = with(words, &ops);
        let theirs: Vec<&str> = theirs.iter().map(String::as_str).collect();
        let ops_own: Vec<&str> = ops_own.iter().map(String::as_str).collect();
        assert_eq!(status(&oscar, ops_own.clone()).await, ok, "{ops_own:?}");
        assert_eq!(status(&oscar, theirs.clone()).await, no, "{theirs:?}");
    }
    // The route's permission, not only visibility: a key without `pipes:read` / `pipes:write`.
    let narrow = key_for(
        &f,
        "opal",
        &["actions:execute", "jobs:execute", "jobs:write"],
    )
    .await;
    let show: Vec<&str> = ["show", "pipes"].iter().chain(&data).copied().collect();
    let edit: Vec<&str> = ["edit", "pipes", "-P", "{}"]
        .iter()
        .chain(&data)
        .copied()
        .collect();
    for (sysargs, scope) in [(show, "pipes:read"), (edit, "pipes:write")] {
        let r = validate(narrow.clone(), sysargs).await;
        assert_eq!(r.status, no);
        assert!(message(&r).contains(scope), "{}", message(&r));
    }

    // Pipelines: named, visible, with their members (read) or changeable members (the others).
    let register = |auth: String, name: &'static str, targets: Vec<String>| {
        let f = &f;
        async move {
            let body = json!({"pipeline": {"name": name, "targets": targets}});
            let r = call(
                f,
                "POST",
                &auth,
                &format!("/pipelines/{name}/register"),
                Some(body),
            )
            .await;
            assert_eq!(r.status, StatusCode::OK, "{name}: {}", r.text());
        }
    };
    register(opal.clone(), "data-line", vec![pipe_ref(TEMPERATURE)]).await;
    register(oscar.clone(), "ops-line", vec![pipe_ref(PRESSURE)]).await;
    register(
        admin.clone(),
        "mixed",
        vec![pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)],
    )
    .await;
    assert_eq!(
        status(&opal, vec!["show", "pipelines", "data-line"]).await,
        ok
    );
    assert_eq!(
        status(&opal, vec!["show", "pipelines", "ops-line"]).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(status(&opal, vec!["show", "pipelines", "mixed"]).await, no);
    assert_eq!(status(&opal, vec!["show", "pipelines"]).await, no);
    for action in ["delete", "clear", "drop"] {
        // An Operator lacks `pipes:delete` / `pipes:drop`.
        assert_eq!(
            status(&opal, vec![action, "pipelines", "data-line"]).await,
            no
        );
        assert_eq!(
            status(&oscar, vec![action, "pipelines", "ops-line"]).await,
            ok
        );
        assert_eq!(
            status(&oscar, vec![action, "pipelines", "data-line"]).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(status(&oscar, vec![action, "pipelines", "mixed"]).await, no);
    }
    assert_eq!(
        status(&opal, vec!["sync", "pipelines", "data-line"]).await,
        ok
    );

    // Tokens: only the caller's own.
    for (user, label) in [("opal", "opal-key"), ("otto", "otto-key")] {
        let mut t = meerschaum_core::users::Token::new(
            Some(label),
            meerschaum_core::users::Expiration::Never,
            Some(vec!["pipes:read".into()]),
            366,
            chrono::Utc::now(),
        );
        t.user_id = f
            .instance
            .get_user_id(&User::new(user, None))
            .await
            .unwrap();
        assert!(f.instance.register_token(&mut t).await.success);
    }
    assert_eq!(status(&opal, vec!["show", "tokens", "opal-key"]).await, ok);
    assert_eq!(status(&opal, vec!["show", "tokens", "otto-key"]).await, no);
    assert_eq!(status(&opal, vec!["show", "tokens"]).await, no);

    // Everything else is refused to a non-administrator and runs for an administrator.
    for sysargs in [
        vec!["show"],
        vec!["show", "config"],
        vec!["show", "users"],
        vec!["show", "connectors"],
        vec!["show", "graph"],
        vec!["show", "runs"],
        vec!["show", "teams"],
        vec!["show", "plugins"],
        vec!["copy", "pipes", "-c", TEMPERATURE.0],
        vec!["register", "pipelines", "new-line"],
        vec!["edit", "pipelines", "data-line"],
        vec!["index", "pipes"],
        vec!["start", "jobs", "x"],
        vec!["python"],
        vec!["sh", "id"],
        vec!["touch", "/tmp/tpr-x"],
    ] {
        let r = validate(opal.clone(), sysargs.clone()).await;
        assert_eq!(r.status, no, "{sysargs:?}: {}", message(&r));
        assert_eq!(
            status(&admin, sysargs.clone()).await,
            ok,
            "admin {sysargs:?}"
        );
    }
    // `POST /execute` decides the same (it starts the job only when allowed).
    for (auth, sysargs, expected) in [
        (&opal, json!(["show", "graph"]), no),
        (
            &opal,
            json!(["show", "pipes", "-c", PRESSURE.0, "-m", PRESSURE.1]),
            no,
        ),
        (
            &opal,
            json!(["show", "pipes", "-c", TEMPERATURE.0, "-m", TEMPERATURE.1]),
            ok,
        ),
        (&admin, json!(["show", "graph"]), ok),
    ] {
        let r = call(
            &f,
            "POST",
            auth,
            "/execute",
            Some(json!({"sysargs": sysargs, "name": format!("execute-tpr-{}", uuid_like())})),
        )
        .await;
        assert_eq!(r.status, expected, "{sysargs}: {}", r.text());
    }
    let refused = validate(opal.clone(), vec!["show", "graph"]).await;
    assert!(
        message(&refused).contains("not among the commands non-administrators may run"),
        "{}",
        message(&refused)
    );
}

/// The table of commands in `permissions.md` is the allowlist's (skipped outside a checkout).
#[test]
fn the_permissions_page_lists_the_allowed_commands() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/zensical/reference/api-instance/permissions.md");
    let Ok(page) = std::fs::read_to_string(path) else {
        return;
    };
    let table = meerschaum_server::execute_policy::non_admin_commands_markdown();
    let start = "<!-- non-admin-commands:start -->\n";
    let end = "<!-- non-admin-commands:end -->";
    let inner = page
        .split(start)
        .nth(1)
        .and_then(|rest| rest.split(end).next())
        .expect("the generated table's markers in permissions.md");
    assert_eq!(
        inner, table,
        "regenerate the table from execute_policy::non_admin_commands_markdown()"
    );
}

/// A short unique suffix for job names.
fn uuid_like() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    format!("{}", N.fetch_add(1, Ordering::SeqCst))
}
