//! Tests for the Grafana settings, team organizations, users and the reverse proxy's headers.

use super::*;

fn section(json: &str) -> Map {
    Value::from_json_str(json).unwrap().to_map()
}

#[test]
fn settings_need_enabled_and_a_url() {
    let dir = tempfile::tempdir().unwrap();
    let file = admin_password_path(dir.path());
    assert_eq!(GrafanaSettings::from_section(&Map::new(), &file), Ok(None));
    assert_eq!(
        GrafanaSettings::from_section(&section(r#"{"url": "http://grafana:3000"}"#), &file),
        Ok(None)
    );
    assert!(!file.exists());
    let settings = GrafanaSettings::from_section(
        &section(r#"{"enabled": true, "url": "http://grafana:3000/", "admin_password": "s3"}"#),
        &file,
    )
    .unwrap()
    .unwrap();
    assert_eq!(settings.url, "http://grafana:3000");
    assert_eq!(settings.admin_user, "admin");
    assert_eq!(settings.admin_password, "s3");
    assert!(!file.exists());
}

#[test]
fn without_a_password_one_is_generated_and_kept_never_admin() {
    let dir = tempfile::tempdir().unwrap();
    let file = admin_password_path(dir.path());
    let enabled = section(r#"{"enabled": true}"#);
    let first = GrafanaSettings::from_section(&enabled, &file)
        .unwrap()
        .unwrap();
    assert_eq!(first.url, "http://grafana:3000");
    assert_ne!(first.admin_password, DEFAULT_ADMIN_PASSWORD);
    assert_eq!(first.admin_password.len(), 64);
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        first.admin_password
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
    }
    // The same password every time (the stack and the server read the same file).
    let again = GrafanaSettings::from_section(&enabled, &file)
        .unwrap()
        .unwrap();
    assert_eq!(again.admin_password, first.admin_password);
    // Grafana's default password is refused, configured or kept in the file.
    assert!(
        GrafanaSettings::from_section(
            &section(r#"{"enabled": true, "admin_password": "admin"}"#),
            &file
        )
        .is_err()
    );
    std::fs::write(&file, "admin\n").unwrap();
    assert!(GrafanaSettings::from_section(&enabled, &file).is_err());
}

#[test]
fn list_settings_are_joined_with_commas() {
    let s = section(
        r#"{"proxy_whitelist": ["10.0.0.2", " 10.0.0.3 "], "root_url": "https://x/grafana/",
                "empty": [], "none": null}"#,
    );
    assert_eq!(
        setting_text(&s, "proxy_whitelist").as_deref(),
        Some("10.0.0.2,10.0.0.3")
    );
    assert_eq!(
        setting_text(&s, "root_url").as_deref(),
        Some("https://x/grafana/")
    );
    assert_eq!(setting_text(&s, "empty"), None);
    assert_eq!(setting_text(&s, "none"), None);
    assert_eq!(setting_text(&s, "missing"), None);
}

#[test]
fn only_administrators_may_be_named_as_grafanas_administrator() {
    assert!(!may_sign_in_as("admin", false, "admin"));
    assert!(!may_sign_in_as("ADMIN", false, "admin"));
    assert!(!may_sign_in_as("Root", false, "root"));
    assert!(may_sign_in_as("admin", true, "admin"));
    assert!(may_sign_in_as("ann", false, "admin"));
}

#[test]
fn team_organizations_come_from_the_saved_id_and_tombstones_are_no_team_names() {
    let mut team = Team::new("Wetlands");
    assert_eq!(team_org_id(&team), None);
    let mut record = Map::new();
    record.insert("org_id".into(), Value::Int(7));
    team.attributes
        .insert(TEAM_GRAFANA_ATTRIBUTE.into(), Value::Map(record));
    assert_eq!(team_org_id(&team), Some(7));
    let tombstone = tombstone_name("Wetlands", 7);
    assert!(tombstone.starts_with(TOMBSTONE_PREFIX));
    assert!(!access::valid_team_name(&tombstone).success);
}

#[test]
fn the_main_and_anonymous_organizations_are_never_a_teams() {
    let policy = AnonymousPolicy {
        enabled: true,
        org_name: "public".into(),
        org_id: Some(5),
        role: "Viewer".into(),
        cookie_names: vec![],
    };
    assert!(policy.refuses(Some(MAIN_ORG_ID), None).is_some());
    assert!(policy.refuses(Some(5), None).is_some());
    assert!(policy.refuses(None, Some("Public")).is_some());
    assert!(policy.refuses(Some(6), Some("wetlands")).is_none());
}

#[test]
fn identity_headers_from_the_browser_are_never_forwarded() {
    let mut headers = HeaderMap::new();
    headers.insert("x-webauth-user", HeaderValue::from_static("root"));
    headers.insert("X-WEBAUTH-ROLE", HeaderValue::from_static("Admin"));
    headers.insert("authorization", HeaderValue::from_static("Basic YWRtaW4="));
    headers.insert("x-grafana-org-id", HeaderValue::from_static("1"));
    headers.insert("accept", HeaderValue::from_static("text/html"));
    headers.insert("host", HeaderValue::from_static("data.example.com"));
    headers.insert(
        "origin",
        HeaderValue::from_static("https://data.example.com"),
    );
    headers.insert(
        "cookie",
        HeaderValue::from_static(
            "mrsm-session-id=7b0c3a52-5a5b-4d5e-9d0c-2f3f1f1f1f1f; grafana_session=abc; theme=dark",
        ),
    );
    let out = forwarded_headers(&headers, false, &[]);
    assert!(out.get("x-webauth-user").is_none());
    assert!(out.get("x-webauth-role").is_none());
    assert!(out.get("authorization").is_none());
    assert!(out.get("x-grafana-org-id").is_none());
    assert_eq!(out.get("accept").unwrap(), "text/html");
    assert_eq!(out.get("host").unwrap(), "data.example.com");
    assert_eq!(out.get("origin").unwrap(), "https://data.example.com");
    assert_eq!(
        out.get("cookie").unwrap(),
        "grafana_session=abc; theme=dark"
    );
    headers.insert(
        "cookie",
        HeaderValue::from_static("mrsm-session-id=7b0c3a52-5a5b-4d5e-9d0c-2f3f1f1f1f1f"),
    );
    assert!(
        forwarded_headers(&headers, false, &[])
            .get("cookie")
            .is_none()
    );
}

#[test]
fn signed_out_requests_carry_no_grafana_session() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "cookie",
        HeaderValue::from_static(
            "grafana_session=abc; grafana_session_expiry=1; my_session=z; theme=dark",
        ),
    );
    headers.insert("x-webauth-user", HeaderValue::from_static("admin"));
    let dropped: Vec<String> = ["grafana_session", "grafana_session_expiry", "my_session"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let out = forwarded_headers(&headers, false, &dropped);
    assert_eq!(out.get("cookie").unwrap(), "theme=dark");
    assert!(out.get("x-webauth-user").is_none());
    // A signed-in request keeps Grafana's cookies.
    assert_eq!(
        forwarded_headers(&headers, false, &[])
            .get("cookie")
            .unwrap(),
        "grafana_session=abc; grafana_session_expiry=1; my_session=z; theme=dark"
    );
}

#[test]
fn upgrades_keep_their_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("upgrade", HeaderValue::from_static("websocket"));
    headers.insert("connection", HeaderValue::from_static("Upgrade"));
    headers.insert("sec-websocket-key", HeaderValue::from_static("k"));
    let out = forwarded_headers(&headers, true, &[]);
    assert_eq!(out.get("upgrade").unwrap(), "websocket");
    assert_eq!(out.get("connection").unwrap(), "upgrade");
    assert_eq!(out.get("sec-websocket-key").unwrap(), "k");
    assert!(
        forwarded_headers(&headers, false, &[])
            .get("upgrade")
            .is_none()
    );
}

#[test]
fn grafana_users_follow_their_teams() {
    let mut wetlands = Team::new("Wetlands");
    wetlands
        .attributes
        .insert("isolated".into(), Value::Bool(true));
    let data = Team::new("Data");
    let records = vec![Team::everyone(), wetlands, data];
    let teams = vec![
        (access::EVERYONE.to_string(), TeamRole::Member),
        ("Wetlands".to_string(), TeamRole::Manager),
    ];
    let ann = GrafanaUser::new("ann", false, &teams, &records, "Wetlands");
    assert!(ann.isolated && !ann.is_admin);
    assert_eq!(
        ann.teams,
        vec![TeamOrg {
            name: "Wetlands".into(),
            manages: true,
            org: None,
        }]
    );
    assert_eq!(ann.primary.as_deref(), Some("Wetlands"));
    // An administrator is never walled off; a user without teams lands in the main org.
    let root = GrafanaUser::new("root", true, &teams, &records, "Wetlands");
    assert!(!root.isolated && root.is_admin);
    let everyone = vec![(access::EVERYONE.to_string(), TeamRole::Member)];
    let bob = GrafanaUser::new("bob", false, &everyone, &records, access::EVERYONE);
    assert!(!bob.isolated && bob.teams.is_empty() && bob.primary.is_none());
}
