//! The default schema is asked of the database (`current_schema()` / `SCHEMA_NAME()`), as
//! v4's `sqlalchemy.inspect(engine).default_schema_name` does: a role's `search_path` or a
//! login's default schema wins over `public` / `dbo`, and the instance methods use it.
mod common;
use common::*;
use meerschaum_connector::{DataQuery, InstanceConnector, InstanceProvider, SyncOptions};
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::Pipe;
use meerschaum_kernel::enforce::PipeInput;
use std::collections::HashMap;
use std::sync::Arc;

struct Static(HashMap<String, Arc<dyn InstanceConnector>>);

impl InstanceProvider for Static {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0.get(keys).cloned()
    }
}

/// `scheme://user:password@<rest of the admin URI>` for a test login.
fn uri_as(admin_uri: &str, scheme: &str, user: &str, password: &str) -> String {
    let rest = admin_uri.split_once('@').expect("a URI with credentials").1;
    format!("{scheme}://{user}:{password}@{rest}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_roles_search_path_is_the_default_schema_on_postgis() {
    let Some(admin) = ctx("postgis", "w16schema").await else {
        return;
    };
    let Ok(admin_uri) = std::env::var("MRSM_TEST_POSTGIS_URI") else {
        return;
    };
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let role = format!("w16role_{tag}");
    let schema = format!("w16schema_{tag}");
    for query in [
        format!("CREATE ROLE {role} LOGIN PASSWORD 'w16pass'"),
        format!("GRANT CREATE ON DATABASE testdb TO {role}"),
        format!("CREATE SCHEMA {schema} AUTHORIZATION {role}"),
        format!("ALTER ROLE {role} SET search_path = {schema}"),
    ] {
        assert!(admin.conn.exec(&query).await.is_some(), "{query}");
    }
    let outcome = std::panic::AssertUnwindSafe(async {
        let uri = uri_as(&admin_uri, "postgis", &role, "w16pass");
        let spec = || ConnectionSpec::from_uri(&uri, Some("w16role"), None).unwrap();

        // Built eagerly, the connector asks while connecting.
        let eager = SqlConnector::connect("w16role", spec()).await.unwrap();
        assert_eq!(eager.schema(), Some(schema.as_str()));

        // Built lazily, it answers the flavor default until it asks.
        let lazy = SqlConnector::connect_lazy(
            "w16role",
            spec(),
            InstanceSettings::defaults(spec().flavor),
        )
        .unwrap();
        assert_eq!(lazy.schema(), Some("public"));
        assert_eq!(
            lazy.discover_default_schema().await.unwrap().as_deref(),
            Some(schema.as_str())
        );
        assert_eq!(lazy.schema(), Some(schema.as_str()));
        assert_eq!(lazy.instance_schema(), Some(schema.as_str()));

        // A configured schema is never replaced.
        let mut configured = spec();
        configured.schema = Some("public".into());
        let fixed = SqlConnector::connect_lazy(
            "w16role",
            configured,
            InstanceSettings::defaults(spec().flavor),
        )
        .unwrap();
        assert_eq!(
            fixed.discover_default_schema().await.unwrap().as_deref(),
            Some("public")
        );

        // The instance methods use the discovered schema: the pipe's table lands in it.
        let c = Ctx::with_conn("postgis", lazy, "w16schema");
        let pipe = c.pipe(
            "test",
            "default_schema",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        ok(c.sync(
            &pipe,
            vec![doc(vec![("dt", ndt(2024, 1, 1, 0, 0)), ("id", i(1))])],
        )
        .await);
        let target = pipe.target(None).unwrap();
        assert!(admin.conn.table_exists(&target, Some(&schema)).await);
        assert!(!admin.conn.table_exists(&target, Some("public")).await);
        assert_eq!(
            c.get_data(&pipe, DataQuery::default())
                .await
                .unwrap()
                .height(),
            1
        );
        c.delete(&pipe).await;

        // A pipe on another instance, referenced from the admin's pipe through a provider that
        // builds the role's connector lazily (as the config and `MRSM_SQL_*` providers do):
        // the reference names the discovered schema, not `public`.
        let far = Pipe::builder("w16far", "far")
            .instance("sql:w16role")
            .parameters(params(r#"{"columns": {"datetime": "dt", "id": "id"}}"#))
            .build()
            .unwrap();
        let seeded = SqlConnector::connect("w16role", spec()).await.unwrap();
        ok(seeded
            .sync(
                &far,
                PipeInput::Docs(vec![doc(vec![("dt", ndt(2024, 1, 1, 0, 0)), ("id", i(1))])]),
                &SyncOptions::default(),
            )
            .await);
        let far_target = far.target(None).unwrap();
        let fresh = Arc::new(
            SqlConnector::connect_lazy(
                "w16role",
                spec(),
                InstanceSettings::defaults(spec().flavor),
            )
            .unwrap(),
        );
        assert!(!fresh.schema_known());
        assert_eq!(
            fresh.pipe_table_reference(&far),
            None,
            "an undiscovered schema names no table"
        );
        let mut instances: HashMap<String, Arc<dyn InstanceConnector>> = HashMap::new();
        instances.insert("sql:w16role".into(), fresh.clone());
        admin
            .conn
            .set_instance_provider(Arc::new(Static(instances)));
        let child = Pipe::builder("sql:rs_test_postgis", "w16child")
            .instance(&admin.conn.keys())
            .parameters(params(
                r#"{"sql": "SELECT * FROM {{ Pipe('w16far', 'far', instance='sql:w16role') }}"}"#,
            ))
            .build()
            .unwrap();
        let prepared = admin.conn.prepare_pipe(&child).await;
        let resolved = prepared.parameters()["sql"].py_str();
        assert!(
            resolved.contains(&format!("\"{schema}\".\"{far_target}\"")),
            "{resolved}"
        );
        assert!(!resolved.contains("public"), "{resolved}");
        assert!(fresh.schema_known());
        let _ = seeded.drop_pipe(&far).await;
        let _ = seeded.delete_pipe(&far).await;

        // A database that cannot be reached cannot be asked: nothing runs against the flavor
        // default; once it answers, the next call discovers first and then runs.
        let (host, port) = {
            let hostport = admin_uri
                .split_once('@')
                .unwrap()
                .1
                .split('/')
                .next()
                .unwrap();
            let (h, p) = hostport.split_once(':').unwrap();
            (h.to_string(), p.to_string())
        };
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_port = free.local_addr().unwrap().port();
        drop(free);
        let proxied = uri_as(
            &format!("x://x:x@127.0.0.1:{proxy_port}/testdb"),
            "postgis",
            &role,
            "w16pass",
        );
        let late = Arc::new(
            SqlConnector::connect_lazy(
                "w16late",
                ConnectionSpec::from_uri(&proxied, Some("w16late"), None).unwrap(),
                InstanceSettings::defaults(Flavor::Postgis),
            )
            .unwrap(),
        );
        let late_pipe = Pipe::builder("w16late", "late")
            .instance("sql:w16late")
            .parameters(params(r#"{"columns": {"datetime": "dt", "id": "id"}}"#))
            .build()
            .unwrap();
        let refused = InstanceConnector::register_pipe(late.as_ref(), &late_pipe).await;
        assert!(!refused.success);
        assert!(
            refused.message.contains("default schema"),
            "{}",
            refused.message
        );
        assert!(!late.schema_known());
        assert_eq!(late.schema(), Some("public"));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", proxy_port))
            .await
            .unwrap();
        let upstream = format!("{host}:{port}");
        let forwarder = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        let registered = InstanceConnector::register_pipe(late.as_ref(), &late_pipe).await;
        assert!(registered.success, "{}", registered.message);
        assert_eq!(late.schema(), Some(schema.as_str()));
        assert!(admin.conn.table_exists("mrsm_pipes", Some(&schema)).await);
        let _ = InstanceConnector::delete_pipe(late.as_ref(), &late_pipe).await;
        forwarder.abort();
    });
    let result = futures_util::FutureExt::catch_unwind(outcome).await;
    let _ = admin
        .conn
        .exec(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = admin.conn.exec(&format!("DROP OWNED BY {role}")).await;
    let _ = admin
        .conn
        .exec(&format!("DROP ROLE IF EXISTS {role}"))
        .await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_logins_default_schema_is_the_default_schema_on_mssql() {
    let Some(admin) = ctx("mssql", "w16schema").await else {
        return;
    };
    let Ok(admin_uri) = std::env::var("MRSM_TEST_MSSQL_URI") else {
        return;
    };
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let login = format!("w16login_{tag}");
    let schema = format!("w16schema_{tag}");
    let password = "W16-secret-pass-9!";
    for query in [
        format!("CREATE LOGIN {login} WITH PASSWORD = '{password}', CHECK_POLICY = OFF"),
        format!("CREATE USER {login} FOR LOGIN {login}"),
        format!("CREATE SCHEMA {schema} AUTHORIZATION {login}"),
        format!("ALTER USER {login} WITH DEFAULT_SCHEMA = {schema}"),
        format!("ALTER ROLE db_owner ADD MEMBER {login}"),
    ] {
        assert!(admin.conn.exec(&query).await.is_some(), "{query}");
    }
    let outcome = std::panic::AssertUnwindSafe(async {
        let uri = uri_as(&admin_uri, "mssql", &login, password);
        let spec = || ConnectionSpec::from_uri(&uri, Some("w16login"), None).unwrap();
        let eager = SqlConnector::connect("w16login", spec()).await.unwrap();
        assert_eq!(eager.schema(), Some(schema.as_str()));

        let lazy = SqlConnector::connect_lazy(
            "w16login",
            spec(),
            InstanceSettings::defaults(spec().flavor),
        )
        .unwrap();
        assert_eq!(lazy.schema(), Some("dbo"));
        assert_eq!(
            lazy.discover_default_schema().await.unwrap().as_deref(),
            Some(schema.as_str())
        );

        let c = Ctx::with_conn("mssql", lazy, "w16schema");
        let pipe = c.pipe(
            "test",
            "default_schema",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        ok(c.sync(
            &pipe,
            vec![doc(vec![("dt", ndt(2024, 1, 1, 0, 0)), ("id", i(1))])],
        )
        .await);
        let target = pipe.target(None).unwrap();
        assert!(admin.conn.table_exists(&target, Some(&schema)).await);
        assert!(!admin.conn.table_exists(&target, Some("dbo")).await);
        assert_eq!(
            c.get_data(&pipe, DataQuery::default())
                .await
                .unwrap()
                .height(),
            1
        );
        c.delete(&pipe).await;
    });
    let result = futures_util::FutureExt::catch_unwind(outcome).await;
    // The login's schema holds the instance tables its connector created.
    let _ = admin
        .conn
        .exec(&format!(
            "DECLARE @sql NVARCHAR(MAX) = (SELECT STRING_AGG( \
                 CAST('DROP TABLE [{schema}].[' + t.name + '];' AS NVARCHAR(MAX)), ' ') \
                 WITHIN GROUP (ORDER BY t.name) \
                 FROM sys.tables t WHERE t.schema_id = SCHEMA_ID('{schema}')); \
             IF @sql IS NOT NULL EXEC sp_executesql @sql"
        ))
        .await;
    for query in [
        format!("DROP SCHEMA IF EXISTS {schema}"),
        format!("DROP USER IF EXISTS {login}"),
        format!("DROP LOGIN {login}"),
    ] {
        let _ = admin.conn.exec(&query).await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
