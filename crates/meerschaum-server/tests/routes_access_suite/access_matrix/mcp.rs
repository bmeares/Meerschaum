//! MCP tools apply the same access rules as the HTTP routes.

use super::*;

#[tokio::test]
async fn mcp_tools_apply_the_same_rules() {
    let f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:main".into(), "sql:secure".into()];
    })
    .await;
    let otto = caller("otto", Role::Editor);
    let ed = caller("ed", Role::Editor);
    let listing = tool(&f, &otto, "list_pipes", json!({})).await.unwrap();
    let text = listing.to_string();
    assert!(
        text.contains("pressure") && !text.contains("temperature"),
        "{text}"
    );
    let keys = |k: (&str, &str)| json!({"connector_keys": k.0, "metric_key": k.1});
    let r = tool(&f, &otto, "get_pipe_attributes", keys(TEMPERATURE)).await;
    assert!(
        r.as_ref().is_err_and(|e| e.contains("was not found")),
        "{r:?}"
    );
    let mut edit = keys(PUBLIC);
    edit["parameters"] = json!({"tags": ["m"]});
    let r = tool(&f, &ed, "edit_pipe", edit).await;
    assert!(
        r.as_ref().is_err_and(|e| e.contains("You cannot change")),
        "{r:?}"
    );
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:main", "query": "SELECT * FROM \"plugin_data_temperature\""}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("cannot see")), "{r:?}");
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:secure", "query": "SELECT 1"}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("reserved")), "{r:?}");
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:main", "query": "SELECT * FROM plugin_ops_pressure"}),
    )
    .await;
    assert!(r.is_ok(), "{r:?}");
    let r = tool(&f, &otto, "list_connectors", json!({})).await.unwrap();
    assert!(!r.to_string().contains("sql:secure"), "{r}");
    let r = tool(&f, &ed, "list_connectors", json!({})).await.unwrap();
    assert!(r.to_string().contains("sql:secure"), "{r}");
    let r = register_mcp_pipe(
        &f,
        &token(&f, "otto").await,
        json!({"connector_keys": "sql:secure", "metric_key": "z"}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("reserved")), "{r:?}");
    let operator = caller("opal", Role::Operator);
    let r = tool(&f, &operator, "execute_action", json!({"action": "sh id"})).await;
    let text = format!("{r:?}");
    assert!(text.contains("false") || r.is_err(), "{text}");
}

#[tokio::test]
async fn mcp_named_sql_uses_the_selected_instances_access_and_policy_before_its_backend() {
    let mut f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:other".into()];
    })
    .await;
    let other = Arc::new(MemoryInstance::new("other"));
    // This target is public on the default instance and private on the named instance.
    register(
        &other,
        LEGACY,
        json!({"access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    register(&other, PUBLIC, json!({})).await;
    let resolver = Arc::new(Instances(vec![("sql:other".into(), other)]));
    let state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    let backend = Arc::new(Mcp::default());
    f.mcp = McpServer::for_state(&state).with_backend(backend.clone());
    f.state = state;
    let otto = caller("otto", Role::Editor);
    let query = |connector: &str, target: &str| json!({"connector_keys": connector, "query": format!("SELECT * FROM {target}")});
    assert!(
        tool(
            &f,
            &otto,
            "read_sql",
            query(INSTANCE, "plugin_shared_legacy")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_shared_legacy"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("cannot see"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
    assert!(
        tool(
            &f,
            &otto,
            "read_sql",
            query("sql:other", "plugin_ops_public")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    for who in [&otto, &caller("admin", Role::Admin)] {
        let refused = tool(&f, who, "read_sql", query("sql:secure", "arbitrary"))
            .await
            .unwrap_err();
        assert!(refused.contains("allowed instances patterns"), "{refused}");
        assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    }
    let mut cfg = (*f.state.config).clone();
    cfg.connectors = map(json!({"sql": {"other": {"teams": ["Data"]}}}));
    f.state = AppState::builder(cfg.clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_ops_public"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("reserved"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    assert!(
        tool(
            &f,
            &caller("ed", Role::Editor),
            "read_sql",
            query("sql:other", "plugin_ops_public")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 3);
    cfg.allow_multiple_instances = false;
    f.state = AppState::builder(cfg, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver)
        .build()
        .unwrap();
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_ops_public"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("additional instances"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 3);
    // Stdio retains local trust and does not inherit HTTP instance restrictions.
    assert!(
        tool(
            &f,
            &Caller::stdio(),
            "read_sql",
            query("sql:unserved", "arbitrary")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 4);
}
