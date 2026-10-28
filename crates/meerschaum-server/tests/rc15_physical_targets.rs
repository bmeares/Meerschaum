//! Physical SQL identifier aliases must not bypass the access of registered pipes.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::{SECRET, login, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::access::{guard, pipe_targets_on};
use meerschaum_server::auth::{Principal, Scopes};
use meerschaum_server::mcp::{Caller, McpServer};
use meerschaum_server::sql_guard::{HiddenTables, check_definition};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::json;

fn parameters(value: serde_json::Value) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

fn pipe(metric: &str) -> Pipe {
    Pipe::builder("plugin:aliases", metric)
        .instance("sql:aliases")
        .build()
        .unwrap()
}

#[tokio::test]
async fn postgres_physical_aliases_include_explicit_targets_and_utf8_byte_clipping() {
    // The lazy connector never opens a connection. These are its real SQL naming helpers.
    let spec = ConnectionSpec::from_uri(
        "postgresql://synthetic:synthetic@127.0.0.1:1/synthetic",
        Some("aliases"),
        None,
    )
    .unwrap();
    let conn = SqlConnector::connect_lazy(
        "aliases",
        spec,
        InstanceSettings::defaults(Flavor::Postgresql),
    )
    .unwrap();
    let prefix = "a".repeat(100);
    let private = pipe(&format!("{prefix}x"));
    let guessed = pipe(&format!("{prefix}y"));
    assert_ne!(private.target(None).unwrap(), guessed.target(None).unwrap());
    assert_eq!(
        conn.pipe_target_aliases(&private)[0],
        conn.pipe_target_aliases(&guessed)[0],
    );
    let hidden = HiddenTables::new(pipe_targets_on(&conn, &private));
    assert!(guard::check_pipe_target(&conn, &guessed, &hidden).is_err());

    let mut explicit = pipe("explicit");
    explicit.set_target(&format!("{prefix}x"));
    let mut alias = pipe("other");
    alias.set_target(&format!("{prefix}y"));
    // Explicit `target` skips Pipe's flavor shortening; SQL identifier quoting still applies it.
    assert_eq!(explicit.target(Some("postgresql")).unwrap().len(), 101);
    let targets = pipe_targets_on(&conn, &explicit);
    assert!(targets.contains(&explicit.target(None).unwrap()));
    assert!(targets.contains(&explicit.target_legacy()));
    let hidden =
        HiddenTables::new(&targets).with_identifier_byte_limit(conn.sql_identifier_byte_limit());
    assert!(guard::check_pipe_target(&conn, &alias, &hidden).is_err());
    let physical = &conn.pipe_target_aliases(&explicit)[0];
    assert_eq!(physical, &"a".repeat(63));
    assert!(check_definition(&format!("SELECT * FROM \"{physical}\""), &hidden).is_err());
    assert!(
        check_definition(
            &format!("SELECT * FROM \"{physical}different_suffix\""),
            &hidden
        )
        .is_err()
    );
    assert!(
        check_definition(
            &format!("SELECT * FROM \"public\".\"{physical}different_suffix\""),
            &hidden
        )
        .is_err()
    );
    assert!(guard::check_pipe_target(&conn, &alias, &hidden).is_err());

    // PostgreSQL truncates bytes, not Unicode characters, even below the client character limit.
    explicit.set_target(&format!("{}x", "界".repeat(30)));
    alias.set_target(&format!("{}different", "界".repeat(21)));
    let clipped = "界".repeat(21);
    assert_eq!(clipped.len(), 63);
    assert!(conn.pipe_target_aliases(&explicit).contains(&clipped));
    assert!(conn.pipe_target_aliases(&alias).contains(&clipped));
    let hidden = HiddenTables::new(pipe_targets_on(&conn, &explicit))
        .with_identifier_byte_limit(conn.sql_identifier_byte_limit());
    assert!(guard::check_pipe_target(&conn, &alias, &hidden).is_err());
    assert!(check_definition(&format!("SELECT * FROM \"{clipped}\""), &hidden).is_err());
    assert!(check_definition(&format!("SELECT * FROM \"{clipped}extra\""), &hidden).is_err());
    assert!(
        check_definition(
            &format!("SELECT * FROM \"public\".\"{clipped}extra\""),
            &hidden
        )
        .is_err()
    );
}

#[tokio::test]
async fn sqlite_http_rejects_colliding_reads_registration_and_definition_edits() {
    let root = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", root.path().join("aliases.db").display());
    let conn = Arc::new(SqlConnector::from_uri("aliases", &uri).await.unwrap());
    let mut bob = User::new("bob", Some("synthetic-pass"));
    assert!(conn.register_user(&mut bob).await.success);
    assert!(
        conn.set_user_attributes(&bob, &parameters(json!({"scopes": ["*"]})))
            .await
            .success
    );
    let prefix = "a".repeat(1100);
    let private = pipe(&format!("{prefix}x"));
    let guessed = pipe(&format!("{prefix}y"));
    let mut private = private;
    private.set_parameters(parameters(json!({
        "access": {"owner": "alice", "visibility": "private"},
    })));
    assert!(conn.register_pipe(&private).await.success);
    let physical = conn.pipe_table_reference(&private).unwrap();
    assert_eq!(physical, conn.pipe_table_reference(&guessed).unwrap());
    conn.driver()
        .exec(&format!("CREATE TABLE {physical} (secret_value INTEGER)"))
        .await
        .unwrap();
    conn.driver()
        .exec(&format!("INSERT INTO {physical} VALUES (73)"))
        .await
        .unwrap();
    let editable = pipe("editable");
    assert!(conn.register_pipe(&editable).await.success);
    let mut cfg = ServerConfig::for_root("sql:aliases", root.path());
    cfg.no_dash = true;
    cfg.no_webterm = true;
    let state = AppState::builder(cfg, conn.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let auth = login(&app, "bob", "synthetic-pass", Some("*")).await;
    let index = meerschaum_server::access::pipe_index(&state, None)
        .await
        .unwrap();
    assert!(index.pipes.iter().any(|p| p.targets.contains(&physical)));

    for suffix in ["data", "rowcount", "exists", "columns/types"] {
        let response = send(
            &app,
            req(
                "GET",
                &format!(
                    "/pipes/plugin:aliases/{}/None/{suffix}",
                    guessed.metric_key()
                ),
                Some(&auth),
                None,
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "{}",
            response.text()
        );
        assert!(!response.text().contains("73"));
    }
    let caller = Caller::api(Scopes::List(vec!["*".into()]), Some(Principal::User(bob)));
    let mcp = McpServer::for_state(&state);
    let cx = meerschaum_server::mcp::tools::ToolCx {
        state: &state,
        server: &mcp,
        caller: &caller,
    };
    for name in ["get_pipe_data", "get_pipe_stats", "get_pipe_attributes"] {
        let args = parameters(json!({
            "connector_keys": guessed.connector_keys(), "metric_key": guessed.metric_key(),
        }));
        let error = meerschaum_server::mcp::tools::call(name, &args, &cx)
            .await
            .unwrap_err();
        assert!(error.message.contains("was not found"), "{error:?}");
    }
    let candidate_target = guessed.target(None).unwrap();
    for (method, path, body) in [
        (
            "POST",
            "/pipes/plugin:aliases/new/None/register",
            json!({"target": candidate_target}),
        ),
        (
            "PATCH",
            "/pipes/plugin:aliases/editable/None/edit",
            json!({"target": candidate_target}),
        ),
        (
            "PATCH",
            "/pipes/plugin:aliases/editable/None/edit",
            json!({"sql": format!("SELECT * FROM {physical}")}),
        ),
    ] {
        let response = send(
            &app,
            req(
                method,
                path,
                Some(&auth),
                Some(("application/json", body.to_string().into_bytes())),
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{}",
            response.text()
        );
        assert!(
            response.text().contains("cannot see"),
            "{}",
            response.text()
        );
    }
    // New-pipe introspection remains available when its physical target is not hidden.
    let response = send(
        &app,
        req(
            "GET",
            "/pipes/plugin:aliases/safe/None/exists",
            Some(&auth),
            None,
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json(), json!(false));
}
