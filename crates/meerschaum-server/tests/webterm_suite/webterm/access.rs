//! Who may open a terminal: the page and the websocket need an authorized session, non-administrators
//! follow `api:permissions:actions:non_admin`, and cross-origin websockets are refused.

use super::*;

#[tokio::test]
async fn the_page_requires_an_authorized_session() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let app = webterm::routes(state.clone(), config(&state, sh(), None));

    let get = |id: &str| {
        Request::get(format!("/webterm/{id}"))
            .body(Body::empty())
            .unwrap()
    };

    let ok = send(&app, get(ADMIN_SESSION)).await;
    assert_eq!(ok.status, StatusCode::OK);
    assert!(
        ok.text()
            .contains(&format!("data-session=\"{ADMIN_SESSION}\""))
    );
    assert_eq!(ok.headers["cache-control"], "no-store");
    assert_eq!(ok.headers["referrer-policy"], "no-referrer");

    // v4's `401` page links to the console login.
    let unknown = send(&app, get("not-a-session")).await;
    assert_eq!(unknown.status, StatusCode::UNAUTHORIZED);
    assert!(unknown.text().contains("<h2>Unauthorized</h2>"));
    assert!(unknown.text().contains("href=\"/dash/login\""));

    // A logged-in user who is not an administrator gets a shell with `webterm:use` (the role
    // `operator`); v4 gave every logged-in user one.
    let user = send(&app, get(USER_SESSION)).await;
    assert_eq!(user.status, StatusCode::OK);
    // The default role (`editor`) has no terminal: a page that says so.
    let editor = send(&app, get(EDITOR_SESSION)).await;
    assert_eq!(editor.status, StatusCode::FORBIDDEN);
    assert!(
        editor.text().contains("No terminal access"),
        "{}",
        editor.text()
    );
    // A session whose user is not on the instance (v4 asked only the session store).
    let ghost = send(&app, get(GHOST_SESSION)).await;
    assert_eq!(ghost.status, StatusCode::UNAUTHORIZED);

    // The trailing slash v4's `/webterm/(.+)/?` accepted.
    let slash = send(
        &app,
        Request::get(format!("/webterm/{ADMIN_SESSION}/"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(slash.status, StatusCode::OK);
}

/// The page status and whether the websocket gets a shell (a `setup` message) or is accepted
/// and closed (code 1000, v4's behavior for an unauthorized socket).
async fn access(app: &Router, addr: std::net::SocketAddr, session: &str) -> (StatusCode, bool) {
    let page = send(
        app,
        Request::get(format!("/webterm/{session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .status;
    let mut socket = connect(addr, session).await;
    let shell = match next_message(&mut socket).await {
        Some(message) if message == json!(["setup", {}]) => true,
        // A refusal says why before it closes.
        Some(message) => {
            assert_eq!(message[0], "stdout", "{message}");
            false
        }
        None => false,
    };
    (page, shell)
}

#[tokio::test]
async fn non_admin_users_follow_api_permissions_actions_non_admin() {
    // The terminal's gate: administrators, and users with `webterm:use` (the role
    // `operator`) while `api:permissions:actions:non_admin` allows non-administrators (its
    // default; `start api --secure` and `false` close it). v4's other key
    // (`system:api:permissions:actions:non_admin`, undefined by default) is not consulted.
    let root = tempfile::tempdir().unwrap();
    let cfg = server_config(root.path(), ServerFlags::default(), None);
    assert!(
        cfg.actions_non_admin,
        "the default config allows non-admins"
    );
    assert!(
        !cfg.system_actions_non_admin,
        "the actions routes' key is undefined by default"
    );
    let state = state_from(cfg);
    let app = webterm::routes(state.clone(), config(&state, sh(), None));
    let addr = serve(app.clone()).await;
    assert_eq!(
        access(&app, addr, USER_SESSION).await,
        (StatusCode::OK, true)
    );
    assert_eq!(
        access(&app, addr, ADMIN_SESSION).await,
        (StatusCode::OK, true)
    );
    assert_eq!(
        access(&app, addr, EDITOR_SESSION).await,
        (StatusCode::FORBIDDEN, false)
    );
    assert_eq!(
        access(&app, addr, UNKNOWN_SESSION).await,
        (StatusCode::UNAUTHORIZED, false)
    );

    // `api:permissions:actions:non_admin: false`, and `--secure`: administrators only.
    for (flags, key) in [
        (ServerFlags::default(), Some(false)),
        (
            ServerFlags {
                secure: true,
                ..Default::default()
            },
            None,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let cfg = server_config(root.path(), flags, key);
        assert!(!cfg.actions_non_admin);
        let state = state_from(cfg);
        let app = webterm::routes(state.clone(), config(&state, sh(), None));
        let addr = serve(app.clone()).await;
        assert_eq!(
            access(&app, addr, USER_SESSION).await,
            (StatusCode::FORBIDDEN, false)
        );
        assert_eq!(
            access(&app, addr, ADMIN_SESSION).await,
            (StatusCode::OK, true)
        );
    }
}

#[tokio::test]
async fn a_websocket_without_a_session_is_accepted_and_closed() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;

    // PARITY: v4 accepts the socket, then closes it; no PTY starts. v4's terminal stayed blank;
    // v5 says why first.
    let mut socket = connect(addr, "no-such-session").await;
    let (out, disconnected, code) = ending(&mut socket, Duration::from_secs(10)).await;
    assert!(out.contains("Log in again"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1000));
    // So does a session id the store does not know.
    let mut socket = connect(addr, UNKNOWN_SESSION).await;
    let (out, disconnected, code) = ending(&mut socket, Duration::from_secs(10)).await;
    assert!(out.contains("Log in again"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1000));
    // A user without the terminal, and a session store that fails.
    let mut socket = connect(addr, EDITOR_SESSION).await;
    let (out, _, code) = ending(&mut socket, Duration::from_secs(10)).await;
    assert!(out.contains("permission"), "{out:?}");
    assert_eq!(code, Some(1000));
    let sessions = sessions();
    sessions.fail(true);
    let addr = serve(webterm::routes(
        state.clone(),
        config_with(&state, sessions, sh(), None),
    ))
    .await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    let (out, _, code) = ending(&mut socket, Duration::from_secs(10)).await;
    assert!(out.contains("Try again later"), "{out:?}");
    assert_eq!(code, Some(1000));
}

#[tokio::test]
async fn cross_origin_websockets_are_refused() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;
    let open = |origin: Option<String>| async move {
        let mut request = format!("ws://{addr}/websocket/{ADMIN_SESSION}")
            .into_client_request()
            .unwrap();
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("origin", origin.parse().unwrap());
        }
        tokio_tungstenite::connect_async(request).await
    };
    // Another site's page: refused at the handshake, before any session lookup or PTY.
    for origin in [
        "https://evil.example",
        "http://localhost:1",
        "null",
        "garbage",
    ] {
        match open(Some(origin.into())).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{origin}");
            }
            other => panic!("{origin}: expected a 403, got {:?}", other.map(|_| ())),
        }
    }
    // Same origin (the page the terminal was served from) and no `Origin` (not a browser) work.
    for origin in [Some(format!("http://{addr}")), None] {
        let (mut socket, _) = open(origin).await.expect("handshake");
        expect_setup(&mut socket).await;
    }
}

#[test]
fn origin_checks_follow_host_and_forwarded_host() {
    use axum::http::HeaderMap;
    let headers = |pairs: &[(&str, &str)]| {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        map
    };
    assert!(webterm::origin_allowed(&headers(&[("host", "a:1")])));
    assert!(webterm::origin_allowed(&headers(&[
        ("host", "a:1"),
        ("origin", "http://a:1")
    ])));
    assert!(webterm::origin_allowed(&headers(&[
        ("host", "internal:8000"),
        ("x-forwarded-host", "term.example"),
        ("origin", "https://term.example")
    ])));
    assert!(!webterm::origin_allowed(&headers(&[
        ("host", "a:1"),
        ("origin", "http://a:2")
    ])));
    assert!(!webterm::origin_allowed(&headers(&[(
        "origin",
        "http://a:1"
    )])));
}
