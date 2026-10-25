//! The unique keys of the instance tables (5.0, `instance/unique_keys.rs`): a pipe, a username or
//! a plugin name cannot be registered twice, and an existing table with duplicate rows is left as
//! it is until they are removed.
mod common;
use common::*;
use meerschaum_connector::PluginRecord;
use meerschaum_connector_sql::driver::SqlParam;
use meerschaum_connector_sql::driver::params::placeholders;
use meerschaum_connector_sql::instance::tables::instance_table_ddl;
use meerschaum_connector_sql::instance::unique_keys::{
    KeyStatus, PIPES_KEY, PLUGINS_KEY, USERS_KEY, UniqueKey,
};
use meerschaum_core::Pipe;
use meerschaum_core::users::User;

/// Whether the key also covers pipes without a location key (MySQL 5.7 and MariaDB index the
/// columns, whose `NULL`s are distinct).
fn covers_missing_location(flavor: &str) -> bool {
    !matches!(flavor, "mysql" | "mariadb")
}

/// Insert a registration row for `pipe` directly, as a second process would.
async fn insert_pipe_row(c: &Ctx, pipe: &Pipe) -> bool {
    let table = c.conn.instance_table_name("mrsm_pipes");
    let query = format!(
        "INSERT INTO {table} (connector_keys, metric_key, location_key) VALUES ({})",
        placeholders(c.conn.flavor(), 1, 3)
    );
    let params = vec![
        SqlParam::Text(pipe.connector_keys().to_string()),
        SqlParam::Text(pipe.metric_key().to_string()),
        SqlParam::opt_text(pipe.location_key()),
    ];
    c.conn.exec_params(&query, &params).await.is_some()
}

/// Insert a row for `username` directly, as a second process would.
async fn insert_user_row(c: &Ctx, username: &str) -> bool {
    let table = c.conn.instance_table_name("mrsm_users");
    let query = format!(
        "INSERT INTO {table} (username) VALUES ({})",
        placeholders(c.conn.flavor(), 1, 1)
    );
    c.conn
        .exec_params(&query, &[SqlParam::Text(username.to_string())])
        .await
        .is_some()
}

flavor_tests!(a_pipe_cannot_be_registered_twice, |c: Ctx| async move {
    for location in [None, Some("here")] {
        let pipe = c.pipe("pk", "twice", location, "{}");
        let res = c.conn.register_pipe(&pipe).await;
        assert!(res.success, "{}", res.message);
        assert!(
            c.conn.unique_key_exists(&PIPES_KEY).await.unwrap(),
            "{}",
            c.flavor
        );

        let res = c.conn.register_pipe(&pipe).await;
        assert!(!res.success);
        assert!(
            res.message.ends_with("is already registered."),
            "{}",
            res.message
        );

        // A second process which passed the existence check before the first row was written.
        let inserted = insert_pipe_row(&c, &pipe).await;
        if location.is_some() || covers_missing_location(c.flavor) {
            assert!(!inserted, "{}: a duplicate row was inserted", c.flavor);
        }
        // (Twice where the duplicate could be inserted.)
        while c.conn.get_pipe_id(&pipe).await.unwrap().is_some() {
            let res = c.conn.delete_pipe(&pipe).await;
            assert!(res.success, "{}", res.message);
        }
    }
});

/// Insert a row for plugin `name` owned by `user_id` directly, as a second process would.
async fn insert_plugin_row(c: &Ctx, name: &str, user_id: i64) -> bool {
    let table = c.conn.instance_table_name("mrsm_plugins");
    let query = format!(
        "INSERT INTO {table} (plugin_name, user_id) VALUES ({})",
        placeholders(c.conn.flavor(), 1, 2)
    );
    c.conn
        .exec_params(
            &query,
            &[SqlParam::Text(name.to_string()), SqlParam::Int(user_id)],
        )
        .await
        .is_some()
}

flavor_tests!(
    a_plugin_name_cannot_be_registered_twice,
    |c: Ctx| async move {
        let mut owner = User::new(&format!("{}_owner", c.prefix), Some("test1234"));
        let res = c.conn.register_user(&mut owner).await;
        assert!(res.success, "{}", res.message);
        let user_id = c.conn.get_user_id(&owner).await.unwrap().unwrap();
        let plugin = PluginRecord {
            name: format!("{}_plugin", c.prefix),
            version: Some("1.0.0".into()),
            user_id: Some(user_id.clone()),
            attributes: Default::default(),
        };

        // Registered at once by several processes: one row, and the others take the update path
        // (refused here: the version is not greater), never "Failed to register".
        let results =
            futures_util::future::join_all((0..6).map(|_| c.conn.register_plugin(&plugin, false)))
                .await;
        assert!(results.iter().any(|r| r.success), "{results:?}");
        for res in &results {
            assert!(
                res.success
                    || res
                        .message
                        .contains("must be greater than existing version"),
                "{}: {}",
                c.flavor,
                res.message
            );
        }
        assert!(
            c.conn.unique_key_exists(&PLUGINS_KEY).await.unwrap(),
            "{}",
            c.flavor
        );
        assert!(
            !insert_plugin_row(&c, &plugin.name, user_id.as_i64().unwrap()).await,
            "{}: a duplicate plugin",
            c.flavor
        );

        let res = c.conn.delete_plugin(&plugin).await;
        assert!(res.success, "{}", res.message);
        let res = c.conn.delete_user(&owner).await;
        assert!(res.success, "{}", res.message);
    }
);

flavor_tests!(a_username_cannot_be_registered_twice, |c: Ctx| async move {
    let username = format!("{}_user", c.prefix);
    let mut user = User::new(&username, Some("test1234"));
    let res = c.conn.register_user(&mut user).await;
    assert!(res.success, "{}", res.message);
    assert!(
        c.conn.unique_key_exists(&USERS_KEY).await.unwrap(),
        "{}",
        c.flavor
    );

    let mut again = User::new(&username, Some("other"));
    let res = c.conn.register_user(&mut again).await;
    assert!(!res.success);
    assert!(res.message.ends_with("already exists."), "{}", res.message);
    assert!(
        !insert_user_row(&c, &username).await,
        "{}: a duplicate user",
        c.flavor
    );

    let res = c.conn.delete_user(&user).await;
    assert!(res.success, "{}", res.message);
});

/// Create `key.table` as v4 does (no unique key), insert `values` twice, and check that the key
/// is not added over the duplicate but is once the extra row is deleted.
async fn check_existing_duplicates(c: &Ctx, key: &UniqueKey, insert: impl AsyncFn() -> bool) {
    let ddl = instance_table_ddl(key.table, c.conn.flavor(), c.conn.instance_schema());
    assert!(c.conn.exec_queries_all(&ddl, true, true).await, "{ddl:?}");
    assert_eq!(
        c.conn.unique_key_status(key).await.unwrap(),
        KeyStatus::Missing { duplicates: vec![] }
    );
    assert!(insert().await);
    assert!(insert().await);

    // The key is not added and nothing is deleted.
    c.conn.ensure_unique_keys().await;
    let KeyStatus::Missing { duplicates } = c.conn.unique_key_status(key).await.unwrap() else {
        panic!("{}: the key was added over duplicates", key.table);
    };
    assert_eq!(duplicates.len(), 1);
    assert_eq!(duplicates[0].ids.len(), 2);

    // Once the extra row is gone, the next connector adds it.
    let table = c.conn.instance_table_name(key.table);
    let extra = duplicates[0].ids[1];
    let delete = format!("DELETE FROM {table} WHERE {} = {extra}", key.id_column);
    assert!(c.conn.exec(&delete).await.is_some(), "{delete}");
    c.conn.ensure_unique_keys().await;
    assert_eq!(
        c.conn.unique_key_status(key).await.unwrap(),
        KeyStatus::Present
    );
    assert!(!insert().await, "{}: {}", c.flavor, key.table);
}

flavor_tests!(
    existing_tables_with_duplicates_are_left_alone,
    |c: Ctx| async move {
        // Only on a database of its own: the server flavors share the instance tables between
        // tests (the PostgreSQL family gets a schema of its own below).
        let c = if matches!(c.flavor, "postgresql" | "timescaledb" | "postgis") {
            let schema = format!("w_uk_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
            let Some(scoped) = ctx_in_schema(c.flavor, "unique_keys", &schema).await else {
                return;
            };
            Some((scoped, Some(schema)))
        } else if matches!(c.flavor, "sqlite" | "duckdb" | "geopackage") {
            Some((c, None))
        } else {
            None
        };
        let Some((c, schema)) = c else {
            return;
        };
        let outcome = std::panic::AssertUnwindSafe(async {
            let pipe = c.pipe("pk", "dup", None, "{}");
            check_existing_duplicates(&c, &PIPES_KEY, async || insert_pipe_row(&c, &pipe).await)
                .await;
            let username = format!("{}_dup", c.prefix);
            check_existing_duplicates(&c, &USERS_KEY, async || {
                insert_user_row(&c, &username).await
            })
            .await;
            let user_id = c
                .conn
                .get_user_id(&User::new(&username, None))
                .await
                .unwrap()
                .and_then(|id| id.as_i64())
                .expect("the remaining user");
            // (Looking the user up ensured the instance tables, keys included: start again from a
            // table as v4 created it.)
            let plugins = c.conn.instance_table_name("mrsm_plugins");
            assert!(
                c.conn
                    .exec(&format!("DROP TABLE {plugins}"))
                    .await
                    .is_some()
            );
            let name = format!("{}_dup_plugin", c.prefix);
            check_existing_duplicates(&c, &PLUGINS_KEY, async || {
                insert_plugin_row(&c, &name, user_id).await
            })
            .await;
        });
        let outcome = futures_util::FutureExt::catch_unwind(outcome).await;
        if let Some(schema) = schema {
            let _ = c.conn.exec(&format!("DROP SCHEMA {schema} CASCADE")).await;
        }
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
);
