//! Wave 18 (FX7, RV2 follow-up): `/webterm/assets/*` serves only the six embedded files, whatever
//! the path says. The assets live in a table (`assets.rs`), so no spelling of `..` can reach the
//! file system; this pins that for the spellings a client can send.
#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use common::{config, send, state_with};
use meerschaum_web::{ShellCommand, WebConfig, WebtermSettings};

#[tokio::test]
async fn webterm_assets_do_not_leave_the_embedded_table() {
    let root = tempfile::tempdir().unwrap();
    // A file the traversal would find if the route read the disk.
    std::fs::write(root.path().join("canary.txt"), "canary-outside-the-assets").unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut web = WebConfig::for_state(&state);
    web.webterm = Some(WebtermSettings::new(ShellCommand::new("sh")));
    let app = meerschaum_web::app(state, web).unwrap();

    let manifest = env!("CARGO_MANIFEST_DIR");
    let escapes = [
        "../Cargo.toml".to_string(),
        "../../../../../../../../etc/passwd".to_string(),
        "..%2f..%2f..%2f..%2fetc%2fpasswd".to_string(),
        "%2e%2e/%2e%2e/etc/passwd".to_string(),
        "%252e%252e/%252e%252e/etc/passwd".to_string(),
        "....//....//etc/passwd".to_string(),
        "..\\..\\etc\\passwd".to_string(),
        "%5c..%5c..%5cetc%5cpasswd".to_string(),
        "xterm.js%00.png".to_string(),
        "xterm.js/../../Cargo.toml".to_string(),
        "/etc/passwd".to_string(),
        "..%2fassets%2fxterm.js".to_string(),
        format!("..%2f..%2f{}", root.path().join("canary.txt").display()),
        format!("{manifest}/Cargo.toml"),
    ];
    for escape in escapes {
        let request = Request::get(format!("/webterm/assets/{escape}"))
            .body(Body::empty())
            .unwrap();
        let r = send(&app, request).await;
        let body = r.text();
        for secret in [
            "root:x:0:0",
            "canary-outside-the-assets",
            "[package]",
            "meerschaum-web",
        ] {
            assert!(!body.contains(secret), "{escape}: {secret} leaked");
        }
        // Only a name in the table is a file; everything else is a 404 (or never reaches the
        // handler).
        assert!(
            matches!(r.status, StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST),
            "{escape}: {}",
            r.status
        );
    }
}
