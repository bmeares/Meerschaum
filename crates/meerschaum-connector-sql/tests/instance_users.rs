//! `tests/test_users.py`, plus plugins and tokens round-trips through `InstanceConnector`.
mod common;
use chrono::Utc;
use common::*;
use meerschaum_connector::{InstanceConnector, PluginRecord};
use meerschaum_core::Value;
use meerschaum_core::users::{Expiration, Token, User, verify_password};

fn unique(tag: &str) -> String {
    format!(
        "rs_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

flavor_tests!(test_register_user, |c: Ctx| async move {
    let username = unique("user");
    let mut user = User::new(&username, Some("test1234"));
    user.email = Some("none@none.com".into());
    user.attributes = Some(params(r#"{"scopes": ["custom:read"], "nested": {"a": 1}}"#));
    ok(c.conn.register_user(&mut user).await);

    // Registering again fails.
    let mut dup = User::new(&username, Some("x"));
    let res = c.conn.register_user(&mut dup).await;
    assert!(!res.success);
    assert_eq!(res.message, format!("User '{username}' already exists."));

    let user_id = c
        .conn
        .get_user_id(&user)
        .await
        .unwrap()
        .expect("registered");
    assert!(matches!(user_id, Value::Int(_)));
    assert!(c.conn.get_users().await.unwrap().contains(&username));
    assert_eq!(
        InstanceConnector::get_username(&c.conn, &user_id)
            .await
            .unwrap(),
        Some(Value::Str(username.clone()))
    );

    // The stored hash is passlib's pbkdf2-sha256 and validates the password.
    let hash = c.conn.get_user_password_hash(&user).await.unwrap().unwrap();
    assert!(hash.starts_with("$pbkdf2-sha256$"), "{hash}");
    assert!(verify_password("test1234", &hash).unwrap());
    assert!(!verify_password("wrong", &hash).unwrap());

    let attrs = c.conn.get_user_attributes(&user).await.unwrap().unwrap();
    assert_eq!(
        attrs.get("scopes"),
        Some(&Value::List(vec![Value::Str("custom:read".into())]))
    );
    assert_eq!(c.conn.get_user_type(&user).await.unwrap(), None);

    // Edit: new type and password.
    let mut edited = User::new(&username, Some("newpass"));
    edited.user_type = Some("admin".into());
    edited.email = Some("a@b.com".into());
    ok(c.conn.edit_user(&edited).await);
    assert_eq!(
        c.conn.get_user_type(&user).await.unwrap(),
        Some("admin".into())
    );
    let hash = c.conn.get_user_password_hash(&user).await.unwrap().unwrap();
    assert!(verify_password("newpass", &hash).unwrap());
    // Attributes untouched when the edit carries none.
    assert!(
        c.conn
            .get_user_attributes(&user)
            .await
            .unwrap()
            .unwrap()
            .contains_key("nested")
    );

    // Invalid usernames are rejected.
    let mut bad = User::new("bad name!", Some("x"));
    let res = c.conn.register_user(&mut bad).await;
    assert!(!res.success);
    assert!(res.message.contains("is invalid"), "{}", res.message);

    ok(c.conn.delete_user(&user).await);
    assert_eq!(c.conn.get_user_id(&user).await.unwrap(), None);
    let res = c.conn.delete_user(&User::new(&username, None)).await;
    assert!(!res.success);
    assert_eq!(
        res.message,
        format!("User '{username}' is not registered and cannot be deleted.")
    );
});

flavor_tests!(test_plugins_roundtrip, |c: Ctx| async move {
    let username = unique("owner");
    let mut user = User::new(&username, Some("test1234"));
    ok(c.conn.register_user(&mut user).await);
    let user_id = c.conn.get_user_id(&user).await.unwrap().unwrap();

    let name = unique("plugin");
    let mut plugin = PluginRecord {
        name: name.clone(),
        version: Some("1.0.0".into()),
        user_id: Some(user_id.clone()),
        attributes: params(r#"{"description": "test"}"#),
    };
    ok(c.conn.register_plugin(&plugin, false).await);
    let id = c.conn.get_plugin_id(&plugin).await.unwrap();
    assert!(id.is_some());
    assert_eq!(
        c.conn.get_plugin_version(&plugin).await.unwrap(),
        Some("1.0.0".into())
    );
    assert_eq!(
        c.conn.get_plugin_user_id(&plugin).await.unwrap(),
        user_id.as_i64()
    );
    assert_eq!(
        c.conn
            .get_plugin_attributes(&plugin)
            .await
            .unwrap()
            .get("description"),
        Some(&Value::Str("test".into()))
    );
    // PARITY: returns *an* owner's username (cross join); at least it is set.
    assert!(c.conn.get_plugin_username(&plugin).await.unwrap().is_some());
    let mine = c.conn.get_plugins(Some(&user_id), None).await.unwrap();
    assert_eq!(mine, vec![name.clone()]);
    let search = c.conn.get_plugins(None, Some(&name[..12])).await.unwrap();
    assert!(search.contains(&name));

    // A non-increasing version is rejected without `force`.
    let res = c.conn.register_plugin(&plugin, false).await;
    assert!(!res.success);
    assert_eq!(
        res.message,
        format!(
            "Version '1.0.0' of plugin '{name}' must be greater than existing version '1.0.0'."
        )
    );
    plugin.version = Some("1.0.1".into());
    ok(c.conn.register_plugin(&plugin, false).await);
    assert_eq!(
        c.conn.get_plugin_version(&plugin).await.unwrap(),
        Some("1.0.1".into())
    );
    assert_eq!(c.conn.get_plugin_id(&plugin).await.unwrap(), id);
    // The trait form returns the id as a string.
    assert_eq!(
        InstanceConnector::get_plugin_id(&c.conn, &plugin)
            .await
            .unwrap(),
        id.map(|i| i.to_string())
    );

    ok(c.conn.delete_plugin(&plugin).await);
    assert_eq!(c.conn.get_plugin_id(&plugin).await.unwrap(), None);
    let res = c.conn.delete_plugin(&plugin).await;
    assert!(res.success);
    assert_eq!(res.message, format!("Plugin '{name}' was not registered."));

    // Deleting a user deletes their plugins.
    let plugin2 = PluginRecord {
        name: unique("plugin2"),
        version: None,
        user_id: Some(user_id.clone()),
        attributes: Default::default(),
    };
    ok(c.conn.register_plugin(&plugin2, false).await);
    let res = c.conn.delete_user(&user).await;
    if c.is_sqlite() || c.is_duckdb() {
        // SQLite leaves foreign keys off: the user row goes, then their plugins.
        ok(res);
        assert_eq!(c.conn.get_plugin_id(&plugin2).await.unwrap(), None);
    } else {
        // PARITY: v4 deletes the user before their plugins, so the `mrsm_plugins` foreign key
        // rejects it on databases that enforce it (`_users.py:263`).
        assert!(!res.success);
        assert_eq!(res.message, format!("Failed to delete user '{username}'."));
        ok(c.conn.delete_plugin(&plugin2).await);
        ok(c.conn.delete_user(&user).await);
    }
});

flavor_tests!(test_tokens_roundtrip, |c: Ctx| async move {
    let username = unique("tokuser");
    let mut user = User::new(&username, Some("test1234"));
    ok(c.conn.register_user(&mut user).await);
    let label = unique("token");
    let mut token = Token::new(
        Some(&label),
        Expiration::Default,
        Some(vec!["pipes:read".into(), "pipes:write".into()]),
        366,
        Utc::now(),
    );
    token.user = Some(user.clone());
    ok(InstanceConnector::register_token(&c.conn, &mut token).await);
    let id = token.id.unwrap();
    assert!(InstanceConnector::token_exists(&c.conn, &id).await.unwrap());
    let model = InstanceConnector::get_token_model(&c.conn, &id)
        .await
        .unwrap()
        .expect("token model");
    assert_eq!(model.label.as_deref(), Some(label.as_str()));
    assert_eq!(
        model.scopes,
        vec!["pipes:read".to_string(), "pipes:write".to_string()]
    );
    assert!(model.is_valid);
    let hash = InstanceConnector::get_token_secret_hash(&c.conn, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(verify_password(token.secret.as_deref().unwrap(), &hash).unwrap());
    let user_id = c.conn.get_user_id(&user).await.unwrap();
    let tokens = InstanceConnector::get_tokens(&c.conn, Some(&user), None, None)
        .await
        .unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(
        tokens[0].user_id.as_ref().and_then(Value::as_i64),
        user_id.as_ref().and_then(Value::as_i64)
    );

    ok(InstanceConnector::invalidate_token(&c.conn, &token).await);
    let model = InstanceConnector::get_token_model(&c.conn, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(!model.is_valid);

    ok(InstanceConnector::delete_token(&c.conn, &token).await);
    assert!(!InstanceConnector::token_exists(&c.conn, &id).await.unwrap());
    ok(c.conn.delete_user(&user).await);
});

// test_login_preserves_custom_user_scopes / test_login_treats_admin_as_wildcard_scope:
// skipped — the password grant is served by the API flavor only.
