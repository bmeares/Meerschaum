//! Users and credentials: emails, permission changes and grids, revocation, credentials bounded by
//! their owner, the first account and registration, and plugin upload tokens.

use super::*;

#[tokio::test]
async fn emails_are_shown_to_their_owner_administrators_and_team_managers() {
    let f = fixture().await;
    let emails = |r: &Resp| -> Vec<(String, Json)> {
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        r.json()["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| {
                (
                    u["username"].as_str().unwrap().to_string(),
                    u["email"].clone(),
                )
            })
            .collect()
    };
    let visible = |rows: &[(String, Json)]| -> Vec<String> {
        rows.iter()
            .filter(|(_, e)| !e.is_null())
            .map(|(u, _)| u.clone())
            .collect()
    };
    let vera = token(&f, "vera").await;
    assert_eq!(
        visible(&emails(&get(&f, &vera, "/users/permissions").await)),
        ["vera"]
    );
    let mia = token(&f, "mia").await;
    assert_eq!(
        visible(&emails(&get(&f, &mia, "/users/permissions").await)),
        ["ed", "mia", "opal", "vera"]
    );
    let admin = token(&f, "admin").await;
    assert_eq!(
        visible(&emails(&get(&f, &admin, "/users/permissions").await)).len(),
        7
    );
    let r = get(&f, &mia, "/users/otto/permissions").await;
    assert_eq!(r.json()["user"]["email"], Json::Null, "{}", r.text());
    let otto = token(&f, "otto").await;
    let r = get(&f, &otto, "/users/otto/permissions").await;
    assert_eq!(r.json()["user"]["email"], "otto@example.com");
    // The rows name the roles.
    let roles: Vec<(String, String)> = get(&f, &admin, "/users/permissions").await.json()["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| {
            (
                u["username"].as_str().unwrap().into(),
                u["role"].as_str().unwrap().into(),
            )
        })
        .collect();
    for (user, role) in [
        ("admin", "admin"),
        ("vera", "viewer"),
        ("ed", "editor"),
        ("opal", "operator"),
    ] {
        assert!(
            roles.contains(&(user.into(), role.into())),
            "{user}: {roles:?}"
        );
    }
}

#[tokio::test]
async fn permissions_change_through_the_api_and_reach_existing_credentials() {
    let f = fixture().await;
    let (vera, ed, admin) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "admin").await,
    );
    let r = call(
        &f,
        "POST",
        &ed,
        "/tokens/register",
        Some(json!({"label": "k", "scopes": ["pipes:read", "pipes:write"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let key = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    let edit = Some(json!({"tags": ["k"]}));
    let r = call(
        &f,
        "PATCH",
        &key,
        &url(LEGACY, "/edit?patch=true"),
        edit.clone(),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // Only administrators change rights; a user may change their own primary team.
    let r = call(
        &f,
        "PATCH",
        &vera,
        "/users/ed/permissions",
        Some(json!({"role": "operator"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"role": "operator"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"primary_team": "Ops"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"primary_team": "Data"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // A user edits their own attributes, never the ones that hold rights: refused, not dropped.
    for attributes in [
        json!({"scopes": ["*"], "color": "blue"}),
        json!({"role": "operator"}),
    ] {
        let r = call(
            &f,
            "PATCH",
            &ed,
            "/users/ed/permissions",
            Some(json!({"attributes": attributes})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert_eq!(
            message(&r),
            "Only administrators may change a user's role or permissions."
        );
    }
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"attributes": {"color": "blue"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let stored = f.instance.user_attributes("ed").unwrap();
    assert!(
        stored.get("scopes").is_none() && stored.get("role").is_none(),
        "{stored:?}"
    );
    assert_eq!(stored.get("color"), Some(&Value::Str("blue".into())));
    // v4's form route refuses the same.
    let r = send(
        &f.app,
        req(
            "POST",
            "/users/edit",
            Some(&ed),
            form(&[("username", "ed"), ("attributes", r#"{"scopes": ["*"]}"#)]),
        ),
    )
    .await;
    assert_eq!(
        tuple_of(&r),
        (
            false,
            "Only administrators may change a user's role or permissions.".to_string()
        ),
        "{}",
        r.text()
    );

    // An administrator makes `ed` a viewer: the key and the session lose `pipes:write` at once.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"role": "viewer"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["user"]["role"], "viewer");
    for auth in [&key, &ed] {
        let r = call(
            &f,
            "PATCH",
            auth,
            &url(LEGACY, "/edit?patch=true"),
            edit.clone(),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert_eq!(message(&r), "Missing required scope: 'pipes:write'");
    }
    // Explicit permissions that match no preset are a custom role.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": ["pipes:read", "webterm:use"]})),
    )
    .await;
    assert_eq!(r.json()["user"]["role"], "custom", "{}", r.text());
    assert_eq!(
        r.json()["user"]["permissions"],
        json!(["pipes:read", "webterm:use"])
    );
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"role": "admin"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
}

#[tokio::test]
async fn edited_permission_grids_do_not_regain_unchecked_permissions() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let key = key_for(&f, "ed", &["*"]).await;
    for permissions in [vec!["pipes:write"], vec!["actions:execute"]] {
        let r = call(
            &f,
            "PATCH",
            &admin,
            "/users/ed/permissions",
            Some(json!({"permissions": permissions})),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        assert_eq!(r.json()["user"]["permissions"], json!(permissions));
        assert_eq!(r.json()["user"]["role"], "custom");
        let stored = f.instance.user_attributes("ed").unwrap();
        assert_eq!(stored.get("role"), Some(&Value::Str("custom".into())));
        let fresh = token(&f, "ed").await;
        for auth in [&ed, &key, &fresh] {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(axum::http::header::AUTHORIZATION, auth.parse().unwrap());
            let access = meerschaum_server::access::resolve(&f.state, &headers)
                .await
                .unwrap();
            assert!(
                !access.has("pipes:register")
                    && !access.has("pipes:define_sql")
                    && !access.has("webterm:use")
            );
            assert!(!access.has("*"));
            let r = call(
                &f,
                "POST",
                auth,
                &url(("plugin:data", "grid-new"), "/register"),
                Some(json!({})),
            )
            .await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
            let r = call(
                &f,
                "POST",
                auth,
                "/pipelines/grid-new/register",
                Some(json!({"pipeline": {"name": "grid-new", "targets": []}})),
            )
            .await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        }
        let caller = Caller::api(
            Scopes::List(permissions.iter().map(|p| p.to_string()).collect()),
            Some(Principal::User(User::new("ed", None))),
        );
        let r = tool(
            &f,
            &caller,
            "register_pipe",
            json!({"connector_keys": "plugin:data", "metric_key": "grid-new"}),
        )
        .await;
        assert!(
            r.as_ref().is_err_and(|e| e.contains("pipes:register")),
            "{r:?}"
        );
    }
    // Registration is independent of editing for new permission lists, including MCP.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": ["pipes:register"]})),
    )
    .await;
    assert_eq!(r.json()["user"]["permissions"], json!(["pipes:register"]));
    let fresh = token(&f, "ed").await;
    let r = call(
        &f,
        "POST",
        &fresh,
        &url(("plugin:data", "grid-register"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let r = register_mcp_pipe(
        &f,
        &fresh,
        json!({"connector_keys": "plugin:data", "metric_key": "grid-mcp"}),
    )
    .await;
    assert!(r.is_ok(), "{r:?}");
    // A genuine v4 scopes-only record retains the old meaning of pipes:write.
    f.instance
        .seed_user_attributes("ed", map(json!({"scopes": ["pipes:write"]})));
    f.state.access_cache().forget_identities();
    let legacy = token(&f, "ed").await;
    let r = call(
        &f,
        "POST",
        &legacy,
        &url(("plugin:data", "legacy-register"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
}

#[tokio::test]
async fn revoking_every_permission_revokes_existing_credentials() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let key = key_for(&f, "ed", &["*"]).await;
    assert_eq!(get(&f, &ed, "/pipes/keys").await.status, StatusCode::OK);
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": []})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["user"]["permissions"], json!([]));
    for auth in [&ed, &key] {
        assert_eq!(
            get(&f, auth, "/pipes/keys").await.status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(get(&f, auth, "/teams").await.status, StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn a_credential_never_holds_more_than_its_owner() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    // `ed` (an editor) cannot mint a key with more than they hold.
    let r = call(
        &f,
        "POST",
        &ed,
        "/tokens/register",
        Some(json!({"label": "x", "scopes": ["actions:execute"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // A `*` key made on the instance for `ed`'s account still holds only what `ed` holds when
    // it is used.
    let key = key_for(&f, "ed", &["*"]).await;
    let r = call(
        &f,
        "POST",
        &key,
        "/execute",
        Some(json!({"sysargs": ["show", "version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "Missing required scope: 'actions:execute'");
    // The narrow key has pipes:read and can list pipes visible to Everyone.
    let r = call(
        &f,
        "POST",
        &admin,
        "/tokens/register",
        Some(json!({"label": "ro", "scopes": ["pipes:read"]})),
    )
    .await;
    let ro = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    assert_eq!(
        listed(&get(&f, &ro, "/pipes/keys").await),
        names(&[PUBLIC, LEGACY])
    );
    let r = call(
        &f,
        "PATCH",
        &ro,
        &url(LEGACY, "/edit?patch=true"),
        Some(json!({"tags": []})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

fn form(fields: &[(&str, &str)]) -> Option<(&'static str, Vec<u8>)> {
    let body: Vec<String> = fields
        .iter()
        .map(|(k, v)| format!("{k}={}", meerschaum_core::pyfmt::quote_plus(v)))
        .collect();
    Some((
        "application/x-www-form-urlencoded",
        body.join("&").into_bytes(),
    ))
}

fn empty_state(
    tweak: impl FnOnce(&mut ServerConfig),
) -> (Router, Arc<MemoryInstance>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    (meerschaum_server::app(state), instance, dir)
}

#[tokio::test]
async fn the_first_account_is_an_administrator_and_then_registration_is_closed() {
    let (app, instance, _dir) = empty_state(|_| {});
    let register = |fields: &[(&str, &str)], auth: Option<&str>| {
        req("POST", "/users/register", auth, form(fields))
    };
    let r = send(
        &app,
        register(&[("username", "first"), ("password", "first-pass")], None),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let first = User::new("first", None);
    assert_eq!(
        instance.get_user_type(&first).await.unwrap().as_deref(),
        Some("admin")
    );
    // Registration is now closed (`api:permissions:registration:users` ships `false`).
    let r = send(
        &app,
        register(&[("username", "second"), ("password", "second-pass")], None),
    )
    .await;
    assert_eq!(r.json()[0], json!(false), "{}", r.text());
    assert!(
        r.json()[1]
            .as_str()
            .unwrap()
            .contains("has not allowed user registration")
    );
    // The administrator still registers users, administrators included.
    let admin = login(&app, "first", "first-pass", None).await;
    let r = send(
        &app,
        register(
            &[
                ("username", "third"),
                ("password", "third-pass"),
                ("type", "admin"),
            ],
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    assert_eq!(
        instance
            .get_user_type(&User::new("third", None))
            .await
            .unwrap()
            .as_deref(),
        Some("admin")
    );
}

#[tokio::test]
async fn concurrent_account_deletions_keep_an_administrator() {
    let f = fixture().await;
    f.instance.seed_user("second", "pw", Some("admin"));
    let (admin, second) = (token(&f, "admin").await, token(&f, "second").await);
    let (a, b) = tokio::join!(
        call(&f, "DELETE", &admin, "/users/admin", None),
        call(&f, "DELETE", &second, "/users/second", None),
    );
    assert_ne!(
        tuple_of(&a).0,
        tuple_of(&b).0,
        "{} / {}",
        a.text(),
        b.text()
    );
    let mut remaining = 0;
    for username in f.instance.get_users().await.unwrap() {
        remaining += usize::from(
            f.instance
                .get_user_type(&User::new(&username, None))
                .await
                .unwrap()
                .as_deref()
                == Some("admin"),
        );
    }
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn open_registration_gives_the_default_role_and_never_rights() {
    let (app, instance, _dir) = empty_state(|cfg| cfg.registration_users = true);
    instance.seed_user("admin", "pass", Some("admin"));
    let fields = [
        ("username", "newbie"),
        ("password", "newbie-pass"),
        (
            "attributes",
            r#"{"scopes": ["*"], "role": "operator", "color": "red"}"#,
        ),
    ];
    let r = send(&app, req("POST", "/users/register", None, form(&fields))).await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let attributes = instance.user_attributes("newbie").unwrap();
    assert!(
        attributes.get("scopes").is_none() && attributes.get("role").is_none(),
        "{attributes:?}"
    );
    assert_eq!(attributes.get("color"), Some(&Value::Str("red".into())));
    for user_type in ["admin", "Admin", " ADMIN "] {
        let r = send(
            &app,
            req(
                "POST",
                "/users/register",
                None,
                form(&[
                    ("username", "boss"),
                    ("password", "boss-pass"),
                    ("type", user_type),
                ]),
            ),
        )
        .await;
        assert_eq!(r.json()[0], json!(false), "{user_type}: {}", r.text());
    }
    // The new user is an editor (the default role).
    let newbie = login(&app, "newbie", "newbie-pass", None).await;
    let r = send(
        &app,
        req("GET", "/users/newbie/permissions", Some(&newbie), None),
    )
    .await;
    assert_eq!(r.json()["user"]["role"], "editor", "{}", r.text());
}

#[tokio::test]
async fn plugin_upload_tokens_use_current_owner_permissions_and_keep_ownership() {
    for use_jwt in [false, true] {
        let f = fixture().await;
        async fn credential(f: &Fixture, user: &str, scopes: &[&str], use_jwt: bool) -> String {
            let key = key_for(f, user, scopes).await;
            if !use_jwt {
                return key;
            }
            let response = send(
                &f.app,
                req(
                    "POST",
                    "/login",
                    Some(&key),
                    Some(("application/x-www-form-urlencoded", Vec::new())),
                ),
            )
            .await;
            assert_eq!(response.status, StatusCode::OK, "{}", response.text());
            format!(
                "Bearer {}",
                response.json()["access_token"].as_str().unwrap()
            )
        }
        async fn upload(f: &Fixture, auth: &str, version: &str, body: &str) -> Resp {
            let multipart = format!(
                "--rc30boundary\r\nContent-Disposition: form-data; name=\"archive\"; filename=\"rc30.tar.gz\"\r\nContent-Type: application/gzip\r\n\r\n{body}\r\n--rc30boundary--\r\n"
            );
            send(
                &f.app,
                req(
                    "POST",
                    &format!("/plugins/rc30?version={version}"),
                    Some(auth),
                    Some((
                        "multipart/form-data; boundary=rc30boundary",
                        multipart.into_bytes(),
                    )),
                ),
            )
            .await
        }
        let read_only = credential(&f, "admin", &["plugins:read"], use_jwt).await;
        assert_eq!(
            upload(&f, &read_only, "1.0", "refused").await.status,
            StatusCode::FORBIDDEN
        );
        let owner_key = credential(&f, "oscar", &["plugins:write"], use_jwt).await;
        let first = upload(&f, &owner_key, "1.0", "first").await;
        assert_eq!(first.status, StatusCode::OK, "{}", first.text());
        assert!(tuple_of(&first).0, "{}", first.text());
        let plugin = meerschaum_connector::PluginRecord {
            name: "rc30".into(),
            ..Default::default()
        };
        assert_eq!(
            f.instance.get_plugin_user_id(&plugin).await.unwrap(),
            f.instance
                .get_user_id(&User::new("oscar", None))
                .await
                .unwrap()
        );
        let updated = upload(&f, &owner_key, "2.0", "updated").await;
        assert!(tuple_of(&updated).0, "{}", updated.text());
        // Even another administrator's write token may not take over this plugin.
        let other_owner = credential(&f, "admin", &["plugins:write"], use_jwt).await;
        let rejected = upload(&f, &other_owner, "3.0", "stolen").await;
        assert_eq!(rejected.status, StatusCode::OK);
        assert!(!tuple_of(&rejected).0);
        assert_eq!(get(&f, &owner_key, "/plugins/rc30").await.text(), "updated");
        // This credential still lists plugins:write, but its owner no longer holds it.
        f.instance
            .seed_user_attributes("oscar", map(json!({"scopes": ["plugins:read"]})));
        assert_eq!(
            upload(&f, &owner_key, "3.0", "revoked").await.status,
            StatusCode::FORBIDDEN
        );
    }
}

#[tokio::test]
async fn plugin_artifacts_belong_to_the_owner_and_exact_source_release() {
    use meerschaum_connector_api::plugin_artifacts::TARGET_HEADER;
    use std::io::Write;
    let f = fixture().await;
    let owner = key_for(&f, "oscar", &["plugins:write", "plugins:read"]).await;
    let other = key_for(&f, "admin", &["plugins:write"]).await;
    let reader = key_for(&f, "admin", &["plugins:read"]).await;
    const NAME: &str = "demo";
    const TARGET: &str = "x86_64-unknown-linux-gnu";
    const SOURCE: &[u8] = b"registered source";
    let digest = meerschaum_server::executor::plugins::sha256(SOURCE);
    let target_uri = format!("/plugins/{NAME}/artifacts/{TARGET}?version=1&source_sha256={digest}");
    async fn upload(f: &Fixture, token: &str, uri: &str, bytes: &[u8]) -> Resp {
        let mut body = b"--plugin\r\nContent-Disposition: form-data; name=\"archive\"; filename=\"demo.tar.gz\"\r\n\r\n".to_vec();
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n--plugin--\r\n");
        send(
            &f.app,
            req(
                "POST",
                uri,
                Some(token),
                Some(("multipart/form-data; boundary=plugin", body)),
            ),
        )
        .await
    }
    async fn download(f: &Fixture, auth: &str, target: &str) -> Resp {
        let mut request = req("GET", "/plugins/demo", Some(auth), None);
        request
            .headers_mut()
            .insert(TARGET_HEADER, target.parse().unwrap());
        send(&f.app, request).await
    }
    let mut tar = tar::Builder::new(Vec::new());
    for (path, content) in [
        ("demo/plugin.toml", b"name='demo'\nversion='1'".as_slice()),
        ("demo/libdemo.so", b"synthetic library"),
    ] {
        let mut entry = tar::Header::new_gnu();
        entry.set_size(content.len() as u64);
        entry.set_mode(0o644);
        entry.set_cksum();
        tar.append_data(&mut entry, path, content).unwrap();
    }
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&tar.into_inner().unwrap()).unwrap();
    let binary = gzip.finish().unwrap();
    assert_eq!(
        upload(&f, &owner, &target_uri, &binary).await.status,
        StatusCode::CONFLICT
    );
    assert!(tuple_of(&upload(&f, &owner, "/plugins/demo?version=1", SOURCE).await).0);
    assert_eq!(
        upload(&f, &reader, &target_uri, &binary).await.status,
        StatusCode::FORBIDDEN
    );
    assert!(!tuple_of(&upload(&f, &other, &target_uri, &binary).await).0);
    assert_eq!(
        upload(
            &f,
            &owner,
            &target_uri.replace("version=1", "version=2"),
            &binary
        )
        .await
        .status,
        StatusCode::CONFLICT
    );
    assert_eq!(
        upload(&f, &owner, &target_uri, b"invalid archive")
            .await
            .status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        upload(
            &f,
            &owner,
            &target_uri.replace(TARGET, "aarch64-apple-darwin"),
            &binary
        )
        .await
        .status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let registered = upload(&f, &owner, &target_uri, &binary).await;
    assert!(tuple_of(&registered).0, "{}", registered.text());
    assert_eq!(download(&f, &reader, TARGET).await.body.as_ref(), binary);
    assert_eq!(
        download(&f, &reader, "x86_64-unknown-linux-musl")
            .await
            .body
            .as_ref(),
        SOURCE
    );
    assert_eq!(
        get(&f, &reader, "/plugins/demo").await.body.as_ref(),
        SOURCE
    );
    assert_eq!(
        download(&f, &reader, "../../demo").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    f.instance.seed_user("april", "pw", None);
    let mut team = Team::new("Sensors");
    team.add_member("april", TeamRole::Member);
    team.attributes = map(json!({"isolated": true,"plugins":["demo"]}));
    assert!(f.instance.register_team(&mut team).await.success);
    let confined = token(&f, "april").await;
    assert_eq!(download(&f, &confined, TARGET).await.body.as_ref(), SOURCE);

    // Some instances refuse a duplicate version. Whether accepted or refused, an exact retry
    // must leave every already published target usable.
    let repeated = upload(&f, &owner, "/plugins/demo?version=1", SOURCE).await;
    assert_eq!(repeated.status, StatusCode::OK);
    assert_eq!(download(&f, &reader, TARGET).await.body.as_ref(), binary);

    let owner_login = token(&f, "oscar").await;
    let deleted = send(
        &f.app,
        req("DELETE", "/plugins/demo", Some(&owner_login), None),
    )
    .await;
    assert!(tuple_of(&deleted).0, "{}", deleted.text());
    assert!(tuple_of(&upload(&f, &owner, "/plugins/demo?version=1", SOURCE).await).0);
    assert_eq!(download(&f, &reader, TARGET).await.body.as_ref(), SOURCE);
    assert!(tuple_of(&upload(&f, &owner, &target_uri, &binary).await).0);

    // Old-style concurrent registries and administrators can replace the canonical bytes at
    // the same version. No target with the previous release's digest may then be selected.
    std::fs::write(
        f.state.config.plugins_archives_path.join("demo.tar.gz"),
        b"replacement",
    )
    .unwrap();
    assert_eq!(
        download(&f, &reader, TARGET).await.body.as_ref(),
        b"replacement"
    );
    assert_eq!(
        upload(&f, &owner, &target_uri, &binary).await.status,
        StatusCode::CONFLICT
    );
    let upgraded = upload(&f, &owner, "/plugins/demo?version=2", SOURCE).await;
    assert!(tuple_of(&upgraded).0, "{}", upgraded.text());
    assert_eq!(download(&f, &reader, TARGET).await.body.as_ref(), SOURCE);

    let old = upload(&f, &owner, &target_uri, &binary).await;
    assert_eq!(old.status, StatusCode::CONFLICT);
}
