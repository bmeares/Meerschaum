//! Unit tests for native plugin routes, call limits, and host access.

use super::extensions::*;
use super::host::*;
use super::*;

#[cfg(test)]
mod legacy_route_tests {
    use super::legacy_route_response;
    use serde_json::json;

    #[test]
    fn legacy_route_replies_parse_as_before() {
        // An invalid or missing status is 200; the content type defaults to text.
        for status in [json!("201"), json!(0), json!(70000), json!(null)] {
            let r = legacy_route_response(&json!({"status": status, "body": "ok"}));
            assert_eq!(r.status, 200, "{status}");
            assert_eq!(
                r.headers,
                vec![(
                    "content-type".to_string(),
                    "text/plain; charset=utf-8".to_string()
                )]
            );
            assert_eq!(r.body, b"ok");
        }
        let r = legacy_route_response(&json!({"status": 404, "content_type": "text/html"}));
        assert_eq!(r.status, 404);
        assert_eq!(r.headers[0].1, "text/html");
        assert!(r.body.is_empty());
    }
}

#[cfg(test)]
mod call_limit_tests {
    use super::CallLimits;
    use std::time::Duration;

    #[tokio::test]
    async fn the_wait_queue_is_bounded_and_excess_calls_are_refused_at_once() {
        let limits = CallLimits::new(2, 1, Duration::from_secs(30));
        let a = limits.acquire("p").await.unwrap();
        let _b = limits.acquire("p").await.unwrap();
        // The third waits in the queue (its one place).
        let waiter = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The fourth finds the queue full: refused without waiting for the timeout.
        let started = std::time::Instant::now();
        let refused = limits.acquire("p").await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(refused.contains("busy"), "{refused}");
        // Another plugin has its own slots.
        let _other = limits.acquire("q").await.unwrap();
        // A freed slot goes to the waiter, and the queue frees its place.
        drop(a);
        let _c = waiter.await.unwrap().unwrap();
        let queued = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!queued.is_finished());
        queued.abort();
        let _ = queued.await;
        // A cancelled waiter leaves the queue.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let next = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !next.is_finished(),
            "a cancelled waiter must not keep its place"
        );
        next.abort();
    }

    #[tokio::test]
    async fn a_queued_call_times_out() {
        let limits = CallLimits::new(1, 4, Duration::from_millis(50));
        let _a = limits.acquire("p").await.unwrap();
        assert!(limits.acquire("p").await.is_err());
    }
}

#[cfg(test)]
mod host_access_tests {
    use super::*;
    use meerschaum_core::users::User;
    use meerschaum_plugin::HostServices;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread")]
    async fn api_sql_never_reaches_the_internal_tables() {
        let root = tempfile::tempdir().unwrap();
        let state = server_state(root.path(), |_| {}).await;
        let config =
            meerschaum_core::config::Config::new(meerschaum_core::config::ConfigOptions::with_env(
                vec![("MRSM_ROOT_DIR".into(), root.path().display().to_string())],
            ))
            .unwrap();
        let env = HostEnv::new(
            crate::native::Session::with_runtime(tokio::runtime::Handle::current()),
            host_config(&config),
            Arc::new(NativePlugins::empty()),
            None,
        );
        let host = PluginHost::api(env, state, vec!["*".into()], None);
        let check = |query: &'static str, is_read: bool| {
            let host = &host;
            async move { host.check_api_sql("sql:local", query, is_read, None).await }
        };
        for refused in [
            "SELECT * FROM mrsm_users",
            "select password_hash from \"mrsm_users\"",
            "SELECT * FROM `MRSM_TOKENS`",
            "SELECT * FROM [dbo].[mrsm_pipes]",
            "SELECT * FROM meerschaum.mrsm_plugins",
            "SELECT * FROM mrsm_engine_runs",
            "SELECT * FROM _mrsm_internal.engine_runs",
            "SELECT query_to_xml('select 1', true, true, '')",
            "SELECT * FROM query ('SELECT 1')",
            "SELECT database_to_xml(true, true, '')",
            "SELECT * FROM U&\"mrsm\\005fusers\"",
            "SELECT * FROM sqlite_dbpage",
            "SELECT 'mrsm_' || 'users'",
            "UPDATE mrsm_users SET user_type = 'admin'",
            // Hidden behind what only some databases read as a comment.
            "SELECT 1 # 2 FROM mrsm_users",
            "SELECT * FROM #t JOIN mrsm_users ON 1=1",
            "SELECT 1--1 FROM mrsm_users",
            "SELECT /* /* */ * FROM mrsm_users -- */",
        ] {
            for is_read in [true, false] {
                assert!(check(refused, is_read).await.is_err(), "{refused}");
            }
        }
        let error = check("SELECT * FROM \"MRSM_USERS\"", true)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("'MRSM_USERS'"), "{error}");
        // A read is one read-only statement however a database reads its comments.
        for stacked in [
            "SELECT /* /* */ 1; DELETE FROM t -- */",
            "SELECT 1 --1 INTO OUTFILE '/tmp/x'",
        ] {
            assert!(check(stacked, true).await.is_err(), "{stacked}");
        }
        for allowed in [
            "SELECT * FROM plugin_hosted_parent",
            "SELECT query FROM searches",
            "SELECT COUNT(*) AS n FROM \"plugin_readings\"",
            "SELECT 'mrsm' AS name",
            "SELECT 1 # note",
            "SELECT * FROM #temp",
        ] {
            for is_read in [true, false] {
                assert!(check(allowed, is_read).await.is_ok(), "{allowed}");
            }
        }
        // `exec_sql` keeps MySQL's backslash-escaped strings.
        assert!(check("UPDATE t SET note = 'it\\'s'", false).await.is_ok());
    }

    fn call(method: &str, args: serde_json::Value) -> meerschaum_plugin::HostCall {
        meerschaum_plugin::HostCall {
            plugin: "isolated".into(),
            method: method.into(),
            pipe: None,
            args,
        }
    }

    /// A plugin's `write_plugin_config` keeps the other sections as written (secret
    /// references included) and never writes what `MRSM_PATCH` holds.
    #[test]
    fn write_plugin_config_never_writes_environment_secrets() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let secret = root.path().join("plugin-token");
        std::fs::write(&secret, "plugin-file-s3cret\n").unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let file = config_dir.join("plugins.json");
        std::fs::write(
            &file,
            r#"{"other": {"token": "MRSM{file:plugin-token}", "keys": "MRSM{meerschaum:instance}"}}"#,
        )
        .unwrap();
        let config = meerschaum_core::config::Config::new(
            meerschaum_core::config::ConfigOptions::with_env(vec![
                ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    plugins.path().display().to_string(),
                ),
                (
                    "MRSM_PATCH".into(),
                    r#"{"plugins": {"other": {"api_key": "patch-only-plugin-s3cret"}}}"#.into(),
                ),
            ]),
        )
        .unwrap();
        assert_eq!(
            config.get_plugin_config("other", &["token"]),
            Some(Value::Str("plugin-file-s3cret".into()))
        );
        let mut out = Vec::new();
        let mut rt = crate::dispatch::Runtime::new(
            crate::types::DateContext::system(),
            80,
            &mut out,
            |_| None,
        );
        rt.config = Some(config);
        let natives = Arc::new(NativePlugins::empty());
        let request = meerschaum_plugin::Request {
            method: "action".into(),
            connector_type: String::new(),
            label: String::new(),
            config: serde_json::Value::Null,
            context: json!({}),
        };
        let written = with_command_host(&mut rt, &natives, None, |host| {
            host.host_call(
                &request,
                &call("write_plugin_config", json!({"config": {"cursor": 1}})),
                None,
            )
        })
        .unwrap()
        .value;
        assert_eq!(written[0], json!(true), "{written}");
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(!text.contains("s3cret"), "{text}");
        assert!(text.contains("MRSM{file:plugin-token}"), "{text}");
        assert!(text.contains("\"cursor\": 1"), "{text}");
    }

    #[test]
    fn a_commands_host_calls_use_its_own_config_and_later_reads_see_their_writes() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let config = meerschaum_core::config::Config::new(
            meerschaum_core::config::ConfigOptions::with_env(vec![
                ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    plugins.path().display().to_string(),
                ),
            ]),
        )
        .unwrap();
        let mut out = Vec::new();
        let mut rt = crate::dispatch::Runtime::new(
            crate::types::DateContext::system(),
            80,
            &mut out,
            |_| None,
        );
        rt.config = Some(config);
        let natives = Arc::new(NativePlugins::empty());
        let request = meerschaum_plugin::Request {
            method: "action".into(),
            connector_type: String::new(),
            label: String::new(),
            config: serde_json::Value::Null,
            context: json!({}),
        };
        let write = |rt: &mut crate::dispatch::Runtime<'_>, value: serde_json::Value| {
            with_command_host(rt, &natives, None, |host| {
                host.host_call(
                    &request,
                    &call("write_plugin_config", json!({"config": value})),
                    None,
                )
            })
            .unwrap()
            .value
        };
        // Another plugin's section, with a config reference.
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let file = config_dir.join("plugins.json");
        std::fs::write(&file, r#"{"other": {"keys": "MRSM{meerschaum:instance}"}}"#).unwrap();

        assert_eq!(
            write(&mut rt, json!({"cursor": 5})),
            json!([true, "Wrote the config of plugin 'isolated'."])
        );
        // In the runtime's root (not the process's), the other section kept as the file had it.
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(
            on_disk,
            json!({"other": {"keys": "MRSM{meerschaum:instance}"}, "isolated": {"cursor": 5}})
        );
        // The runtime got its config back, and it reads the write.
        assert_eq!(
            rt.config().get_plugin_config("isolated", &["cursor"]),
            Some(Value::Int(5))
        );
        assert!(rt.config().get_plugin_config("other", &["keys"]).is_some());

        // A copy made before a later write (another run's registry, the next `--loop` run) reads
        // that write back from the file.
        let stale = host_config(rt.config());
        write(&mut rt, json!({"cursor": 6}));
        let session = crate::native::Session::new();
        let env = HostEnv::new(session.clone(), stale, natives.clone(), None);
        assert_eq!(env.plugin_config("isolated"), json!({"cursor": 6}));
        let read = PluginHost::local(env)
            .host_call(&request, &call("plugin_config", json!({})), None)
            .unwrap();
        assert_eq!(read.value, json!({"cursor": 6}));

        // A `plugins` file that cannot be read is never replaced.
        std::fs::write(&file, "{not json").unwrap();
        let refused = write(&mut rt, json!({"cursor": 7}));
        assert_eq!(refused[0], json!(false), "{refused}");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "{not json");
        session.shutdown();
        if let Some(session) = rt.native.take() {
            session.shutdown();
        }
    }

    async fn server_state(
        root: &std::path::Path,
        configure: impl FnOnce(&mut meerschaum_server::ServerConfig),
    ) -> meerschaum_server::AppState {
        let uri = format!("sqlite:///{}", root.join("instance.db").display());
        let sql = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri("local", &uri)
                .await
                .unwrap(),
        );
        if sql
            .get_user_id(&User::new("root", None))
            .await
            .unwrap()
            .is_none()
        {
            let mut admin = User::new("root", Some("synthetic-pass-1"));
            admin.user_type = Some("admin".into());
            assert!(sql.register_user(&mut admin).await.success);
            let mut ada = User::new("ada", Some("synthetic-pass-1"));
            let mut attributes = Map::new();
            attributes.insert(
                "scopes".into(),
                Value::List(vec![
                    Value::Str("pipes:read".into()),
                    Value::Str("actions:execute".into()),
                    Value::Str("jobs:execute".into()),
                ]),
            );
            ada.attributes = Some(attributes);
            assert!(sql.register_user(&mut ada).await.success);
        }
        let mut config = meerschaum_server::ServerConfig::for_root("sql:local", root);
        configure(&mut config);
        let instance: Arc<dyn InstanceConnector> = sql;
        meerschaum_server::AppState::builder(config, instance)
            .secret(b"0123456789abcdef0123456789abcdef".to_vec())
            .build()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_callers_get_what_their_users_may_do() {
        let root = tempfile::tempdir().unwrap();
        let state = server_state(root.path(), |_| {}).await;
        // Page viewers: what `/login` would grant them; nothing for a guest or an unknown name.
        assert!(page_caller(&state, "").await.0.is_empty());
        assert!(page_caller(&state, "nobody").await.0.is_empty());
        let (admin_scopes, admin) = page_caller(&state, "root").await;
        assert_eq!(admin_scopes, ["*"]);
        let (ada_scopes, ada) = page_caller(&state, "ada").await;
        assert_eq!(
            ada_scopes,
            [
                "pipes:read",
                "actions:execute",
                "jobs:execute",
                "webterm:use"
            ]
        );
        assert!(ada.is_some());

        // A page uses the server's current role policy, not v4's all-scopes fallback.
        let mut viewer = User::new("viewer", Some("synthetic-pass-1"));
        assert!(state.instance().register_user(&mut viewer).await.success);
        let limited = server_state(root.path(), |config| {
            config.default_role = "viewer".into();
        })
        .await;
        let (scopes, _) = page_caller(&limited, "viewer").await;
        assert!(scopes.iter().any(|scope| scope == "pipes:read"));
        assert!(!scopes.iter().any(|scope| scope == "actions:execute"));
        let attrs = meerschaum_core::access::attributes_with_permissions::<&str>(&Map::new(), &[]);
        assert!(
            state
                .instance()
                .set_user_attributes(&viewer, &attrs)
                .await
                .success
        );
        limited.access_cache().forget_identities();
        assert!(page_caller(&limited, "viewer").await.0.is_empty());

        let config =
            meerschaum_core::config::Config::new(meerschaum_core::config::ConfigOptions::with_env(
                vec![("MRSM_ROOT_DIR".into(), root.path().display().to_string())],
            ))
            .unwrap();
        let env = HostEnv::new(
            crate::native::Session::with_runtime(tokio::runtime::Handle::current()),
            host_config(&config),
            Arc::new(NativePlugins::empty()),
            None,
        );
        let api = |state: &meerschaum_server::AppState,
                   scopes: &[String],
                   principal: &Option<meerschaum_server::auth::Principal>| {
            PluginHost::api(
                env.clone(),
                state.clone(),
                scopes.to_vec(),
                principal.clone(),
            )
        };
        // `exec_sql`: an administrator may; a user with `actions:execute` (and `jobs:execute`, as
        // `/actions` asks) only where the server lets non-administrators run actions, and never
        // under `--secure`; an anonymous caller never.
        assert!(
            api(&state, &admin_scopes, &admin)
                .may_execute()
                .await
                .is_ok()
        );
        let refused = api(&state, &ada_scopes, &ada)
            .may_execute()
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("not allowed users"), "{refused}");
        assert!(
            api(&state, &["*".into()], &None)
                .may_execute()
                .await
                .is_err()
        );
        let open = server_state(root.path(), |c| c.system_actions_non_admin = true).await;
        assert!(api(&open, &ada_scopes, &ada).may_execute().await.is_ok());
        let secure = server_state(root.path(), |c| {
            c.system_actions_non_admin = true;
            c.actions_non_admin = false;
        })
        .await;
        assert!(api(&secure, &ada_scopes, &ada).may_execute().await.is_err());
        let no_auth = server_state(root.path(), |c| c.no_auth = true).await;
        assert!(
            api(&no_auth, &["*".into()], &None)
                .may_execute()
                .await
                .is_ok()
        );

        // `host_scopes` add named scopes for every caller, never `*`.
        let guest = api(&state, &[], &None).with_host_scopes(&["pipes:read".into(), "*".into()]);
        assert!(guest.api_access("pipes:read", "read pipes").await.is_ok());
        assert!(guest.api_access("pipes:write", "sync pipes").await.is_err());
        assert!(
            guest
                .require_all("read the Meerschaum config")
                .await
                .is_err()
        );
        let writer = api(&state, &[], &None).with_host_scopes(&["pipes:write".into()]);
        assert!(writer.api_access("pipes:write", "sync pipes").await.is_ok());
        assert!(
            writer
                .api_access("pipes:register", "register pipes")
                .await
                .is_err()
        );
        // Without them a guest may do nothing.
        assert!(
            api(&state, &[], &None)
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );

        // A request's effective scope ceiling stays exact when the owner gains rights.
        let ada_user = ada.as_ref().unwrap().as_user().unwrap();
        let expanded = meerschaum_core::access::attributes_with_permissions(
            &Map::new(),
            &["pipes:read", "pipes:write", "pipes:register"],
        );
        assert!(
            state
                .instance()
                .set_user_attributes(ada_user, &expanded)
                .await
                .success
        );
        let capped = api(&state, &["pipes:write".into()], &ada);
        assert!(capped.api_access("pipes:write", "sync pipes").await.is_ok());
        assert!(
            capped
                .api_access("pipes:register", "register pipes")
                .await
                .is_err()
        );

        // Credentials are reloaded within the same plugin invocation, including narrowed
        // or revoked tokens and a user deleted after the request was authenticated.
        use meerschaum_core::users::{Expiration, Token};
        let mut token = Token::new(
            Some("host-caller"),
            Expiration::Never,
            Some(vec!["pipes:read".into(), "pipes:write".into()]),
            366,
            chrono::Utc::now(),
        );
        token.user_id = state
            .instance()
            .get_user_id(&User::new("root", None))
            .await
            .unwrap();
        token.user = Some(User::new("root", None));
        let registered = state.instance().register_token(&mut token).await;
        assert!(registered.success, "{registered}");
        let credential =
            meerschaum_server::auth::load_token(&state, &token.id.unwrap().to_string())
                .await
                .unwrap();
        let token_host = api(
            &state,
            &["pipes:read".into(), "pipes:write".into()],
            &credential,
        );
        assert!(
            token_host
                .api_access("pipes:write", "sync pipes")
                .await
                .is_ok()
        );
        let grants: Vec<String> = meerschaum_core::access::ALL_PERMISSIONS
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert!(
            token_host
                .clone()
                .with_host_scopes(&grants)
                .require_all("read config")
                .await
                .is_err()
        );
        token.scopes = vec!["pipes:read".into()];
        assert!(state.instance().edit_token(&token).await.success);
        assert!(
            token_host
                .api_access("pipes:write", "sync pipes")
                .await
                .is_err()
        );
        assert!(
            token_host
                .api_access("pipes:read", "read pipes")
                .await
                .is_ok()
        );
        assert!(state.instance().invalidate_token(&token).await.success);
        assert!(
            token_host
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );
        let read =
            meerschaum_core::access::attributes_with_permissions(&Map::new(), &["pipes:read"]);
        assert!(
            state
                .instance()
                .set_user_attributes(&viewer, &read)
                .await
                .success
        );
        let deleted = api(
            &state,
            &["pipes:read".into()],
            &Some(meerschaum_server::auth::Principal::User(viewer.clone())),
        );
        assert!(deleted.api_access("pipes:read", "read pipes").await.is_ok());
        assert!(state.instance().delete_user(&viewer).await.success);
        assert!(
            deleted
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );
    }
}
