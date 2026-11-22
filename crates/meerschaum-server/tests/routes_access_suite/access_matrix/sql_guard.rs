//! The SQL guard and reserved connectors: implicit targets, raw internal pipes and definitions
//! never reach hidden or internal tables; connectors reserved for teams.

use super::*;

#[tokio::test]
async fn implicit_targets_cannot_alias_hidden_or_internal_tables() {
    let f = fixture().await;
    let otto = token(&f, "otto").await;
    for keys in [
        ("plugin", "data_temperature"),
        ("mrsm", "users"),
        ("mrsm", "tokens"),
    ] {
        let r = call(&f, "POST", &otto, &url(keys, "/register"), Some(json!({}))).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{keys:?}: {}", r.text());
    }
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("plugin:ops", "aliased"), "/register"),
        Some(json!({"target": "{{ self.parameters['destination'] }}", "destination": "plugin_data_temperature"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn raw_internal_pipes_cannot_bypass_their_filtered_routes() {
    let f = fixture().await;
    register(
        &f.instance,
        ("plugin:shared", "raw_engine"),
        json!({"target": "mrsm_engine_nodes"}),
    )
    .await;
    register(
        &f.instance,
        ("plugin:shared", "raw_teams"),
        json!({"target": "mrsm_teams"}),
    )
    .await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    for keys in [
        ("plugin:shared", "raw_engine"),
        ("plugin:shared", "raw_teams"),
    ] {
        for suffix in ["/attributes", "/data", "/docs", "/csv", "/rowcount"] {
            let r = get(&f, &ed, &url(keys, suffix)).await;
            assert_eq!(
                r.status,
                StatusCode::NOT_FOUND,
                "{keys:?}{suffix}: {}",
                r.text()
            );
        }
    }
    let r = get(&f, &ed, "/pipes/keys").await;
    assert!(
        !r.text().contains("raw_engine") && !r.text().contains("raw_teams"),
        "{}",
        r.text()
    );
    let r = get(&f, &ed, "/pipes").await;
    assert!(
        !r.text().contains("raw_engine") && !r.text().contains("raw_teams"),
        "{}",
        r.text()
    );
    assert_eq!(
        get(
            &f,
            &admin,
            &url(("plugin:shared", "raw_engine"), "/attributes")
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn definitions_never_reach_hidden_or_internal_tables() {
    let f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:main".into()];
    })
    .await;
    let otto = token(&f, "otto").await;
    let mut n = 0;
    let mut try_definition = async |auth: &str, definition: &str| {
        n += 1;
        call(
            &f,
            "POST",
            auth,
            &url(("sql:main", &format!("peek{n}")), "/register"),
            Some(json!({"fetch": {"definition": definition}})),
        )
        .await
    };
    for definition in [
        "SELECT * FROM plugin_data_temperature",
        "SELECT * FROM \"plugin_data_temperature\"",
        "SELECT * FROM [dbo].[plugin_data_temperature]",
        "SELECT * FROM public.Plugin_Data_Temperature",
        "SELECT * FROM `plugin_data_temperature` -- a comment",
        "SELECT * FROM {{ Pipe('plugin:data', 'temperature', instance='sql:fixture') }}",
        "SELECT * FROM mrsm_users",
        "SELECT * FROM \"mrsm_tokens\"",
        "SELECT table_name FROM information_schema.tables",
        "SELECT 'MRSM{meerschaum:connectors:sql:main:database}' AS x",
    ] {
        let r = try_definition(&otto, definition).await;
        assert_eq!(
            r.status,
            StatusCode::FORBIDDEN,
            "{definition}: {}",
            r.text()
        );
    }
    let r = try_definition(&otto, "SELECT * FROM plugin_ops_pressure").await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A team member reads their team's table.
    let ed = token(&f, "ed").await;
    let r = try_definition(&ed, "SELECT * FROM plugin_data_temperature").await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A target never names a hidden or internal table.
    for target in ["plugin_data_temperature", "mrsm_users"] {
        let r = call(
            &f,
            "POST",
            &otto,
            &url(("plugin:ops", target), "/register"),
            Some(json!({"target": target})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{target}: {}", r.text());
    }
    // Setting a definition needs `pipes:define_sql` (a list naming the 5.0 permissions is taken
    // as written; `pipes:write` alone, as written before 5.0, still grants it).
    let key = key_for(&f, "otto", &["pipes:read", "pipes:register", "pipes:write"]).await;
    let r = try_definition(&key, "SELECT 1 AS one").await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("pipes:define_sql"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &key,
        &url(("plugin:ops", "plain"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // An edit is checked like a registration.
    let r = call(
        &f,
        "PATCH",
        &otto,
        &url(PRESSURE, "/edit?patch=true"),
        Some(json!({"sql": "SELECT * FROM \"plugin_data_secret\""})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn definitions_use_the_sources_visibility_byte_limit_and_instance_policy() {
    let mut f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:other".into()];
    })
    .await;
    let other = Arc::new(MemoryInstance::new("other").with_identifier_byte_limit(63));
    register(
        &other,
        LEGACY,
        json!({"access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    register(&other, PUBLIC, json!({})).await;
    let unicode = "界".repeat(21);
    register(
        &other,
        ("plugin:source", "unicode"),
        json!({"target": unicode, "access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    let resolver = Arc::new(Instances(vec![("sql:other".into(), other)]));
    f.state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    f.mcp = McpServer::for_state(&f.state).with_backend(Arc::new(Mcp::default()));
    let otto = token(&f, "otto").await;
    // The same table on the destination is public, including as a visible template reference.
    for (metric, definition) in [
        ("raw", "SELECT * FROM plugin_shared_legacy".to_string()),
        (
            "resolved",
            "SELECT * FROM {{ Pipe('plugin:shared', 'legacy', instance='sql:fixture') }}"
                .to_string(),
        ),
        (
            "unicode",
            format!("SELECT * FROM \"public\".\"{unicode}different_suffix\""),
        ),
    ] {
        let r = call(
            &f,
            "POST",
            &otto,
            &url(("sql:other", metric), "/register"),
            Some(json!({"fetch": {"definition": definition}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{metric}: {}", r.text());
        assert!(message(&r).contains("cannot see"), "{metric}: {}", r.text());
    }
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:other", "allowed"), "/register"),
        Some(json!({"fetch": {"definition": "SELECT * FROM plugin_ops_public"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A destination target is checked against the destination, even if it is private only at source.
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:other", "destination_target"), "/register"),
        Some(json!({"target": "plugin_shared_legacy", "fetch": {"definition": "SELECT * FROM plugin_ops_public"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // MCP registration shares the same source guard.
    let r = register_mcp_pipe(
        &f,
        &otto,
        json!({"connector_keys": "sql:other", "metric_key": "mcp_source", "parameters": {
            "fetch": {"definition": "SELECT * FROM plugin_shared_legacy"}
        }}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("cannot see")), "{r:?}");
    let narrow_admin = key_for(
        &f,
        "admin",
        &["pipes:read", "pipes:write", "pipes:register"],
    )
    .await;
    let r = call(
        &f,
        "POST",
        &narrow_admin,
        &url(("sql:other", "narrow_admin"), "/register"),
        Some(json!({"fetch": {"definition": "SELECT 1"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("pipes:define_sql"), "{}", r.text());
    let mut cfg = (*f.state.config).clone();
    cfg.allowed_instance_keys = vec![INSTANCE.into()];
    f.state = AppState::builder(cfg, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver)
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    let admin = token(&f, "admin").await;
    for auth in [&otto, &admin] {
        let r = call(
            &f,
            "POST",
            auth,
            &url(("sql:other", "unserved_source"), "/register"),
            Some(json!({"fetch": {"definition": "SELECT 1"}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert!(
            message(&r).contains("allowed instances patterns"),
            "{}",
            r.text()
        );
    }
}

#[tokio::test]
async fn a_connector_reserved_for_teams_serves_only_their_pipes() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let sql_labels = |r: Resp| -> Vec<String> {
        r.json()["sql"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(sql_labels(get(&f, &otto, "/connectors").await), ["main"]);
    assert_eq!(
        sql_labels(get(&f, &ed, "/connectors").await),
        ["main", "secure"]
    );
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:secure", "x"), "/register"),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("reserved"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("sql:secure", "y"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    assert_eq!(stored_access(&f, ("sql:secure", "y"))["team"], "Data");
    // Its pipes stay with the team.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(("sql:secure", "y"), "/access"),
        Some(json!({"team": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("'Data'"), "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &admin,
        &url(("sql:secure", "y"), "/access"),
        Some(json!({"team": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}
