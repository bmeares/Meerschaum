//! Positive authenticated broker acceptance on an explicitly disposable local PostgreSQL.

use std::sync::Arc;

use meerschaum_cli::{native::Session, source_fetch_backend::NativeSqlSources};
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector};
use meerschaum_connector_api::source_fetch::{self, PageRequest, PageResponse, Request, Response};
use meerschaum_connector_sql::{SqlConnector, uri::ConnectionSpec};
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Config, Map, Pipe, Value, users::User};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn map(json: Json) -> Map {
    serde_json::from_value::<Value>(json).unwrap().to_map()
}

async fn reply<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let status = response.status();
    let path = response.url().path().to_owned();
    let body = response.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "{path} {status}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly disposable local PostgreSQL database"]
async fn registered_same_store_table_fetch_and_paging() {
    let uri = std::env::var("MEERSCHAUM_TEST_REGISTERED_SOURCE_PG_URI")
        .expect("Supply the owned local PostgreSQL fixture URI");
    let url = reqwest::Url::parse(&uri).expect("Invalid fixture URI");
    assert!(matches!(url.host_str(), Some("127.0.0.1" | "[::1]")));
    assert!(url.path().starts_with("/mrsm_registered_source_"));
    let root = tempfile::tempdir().unwrap();
    let schema = format!("fixture_{}", uuid::Uuid::new_v4().simple());
    let label = "registered";
    let keys = "sql:registered";
    let mut spec = ConnectionSpec::from_uri(&uri, Some(label), None).unwrap();
    spec.schema = Some(schema.clone());
    let db = Arc::new(SqlConnector::connect(label, spec).await.unwrap());
    for sql in [
        format!("CREATE SCHEMA {schema}"),
        format!(
            "CREATE TABLE {schema}.samples (id BIGINT PRIMARY KEY, station TEXT NOT NULL, payload BYTEA, measurement NUMERIC)"
        ),
        format!(
            "INSERT INTO {schema}.samples VALUES
             (1, 'north', decode('aa', 'hex'), 1.20),
             (2, 'north', decode('00ff', 'hex'), 2.3400),
             (3, 'south', decode('cc', 'hex'), 3.45),
             (4, 'north', decode('10ff00', 'hex'), 40000000000000000000.0500),
             (5, 'north', NULL, 5.60),
             (6, 'north', decode('ee', 'hex'), 6.70)"
        ),
    ] {
        db.driver().exec(&sql).await.unwrap();
    }
    let mut operator = User::new("operator", Some("synthetic-registered-source"));
    operator.attributes = Some(map(
        json!({"role":"custom","scopes":["pipes:read","sql:read"]}),
    ));
    assert!(db.register_user(&mut operator).await.success);
    let mut team = Team::new("StationReaders");
    team.add_member("operator", TeamRole::Member);
    team.attributes = map(json!({"isolated":true,"instance":keys,"connectors":[keys]}));
    assert!(db.register_team(&mut team).await.success);
    let pipe = Pipe::builder(keys, "samples")
        .instance(keys)
        .parameters(map(json!({
            "target":"samples",
            "schema":schema,
            "columns":{"datetime":"id"},
            "access":{"owner":"fixture_curator","team":"StationReaders","visibility":"team"}
        })))
        .build()
        .unwrap();
    let registration = db.register_pipe(&pipe).await;
    assert!(registration.success, "{}", registration.message);
    let id = db.get_pipe_id(&pipe).await.unwrap().unwrap().to_string();
    let attributes = json!({
        "flavor":"postgresql",
        "uri":uri,
        "schema":schema,
        "teams":["StationReaders"],
        "registered_source_tables":{(id):{"schema":schema,"table":"samples"}}
    });
    let config = Arc::new(
        Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![(
            "MRSM_ROOT_DIR".into(),
            root.path().display().to_string(),
        )]))
        .unwrap(),
    );
    config.set_config(map(json!({
        "meerschaum":{"instance":keys,"connectors":{"sql":{"registered":attributes}}}
    })));
    let session = Session::with_runtime(tokio::runtime::Handle::current());
    session.register_sql(keys, db.clone());
    let mut cfg = ServerConfig::for_root(keys, root.path());
    cfg.no_auth = false;
    cfg.allowed_instance_keys = vec![keys.into()];
    cfg.connectors = map(json!({"sql":{"registered":attributes}}));
    let sources = Arc::new(NativeSqlSources::new(session, config));
    let state = AppState::builder(cfg, db.clone())
        .secret(b"synthetic-registered-source-fixture-signing-key".to_vec())
        .source_fetch(sources)
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let serve = tokio::spawn(async move {
        axum::serve(listener, meerschaum_server::app(state))
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let login: Json = reply(
        client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("username=operator&password=synthetic-registered-source")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let bearer = login["access_token"].as_str().unwrap();
    let query = DataQuery {
        begin: Some(DateBound::Int(2)),
        end: Some(DateBound::Int(6)),
        order: Some("asc".into()),
        params: Some(map(json!({"station":"north"}))),
        select_columns: Some(vec!["id".into(), "payload".into(), "measurement".into()]),
        ..Default::default()
    };
    let mut request = Request::for_pipe(keys, &pipe, &query).unwrap();
    request.row_limit = 2;
    request.limit = Some(2);
    let data: Response = reply(
        client
            .post(format!("{origin}{}", source_fetch::path(&request.pipe)))
            .bearer_auth(bearer)
            .json(&request)
            .send()
            .await
            .unwrap(),
    )
    .await;
    data.validate(&request).unwrap();
    assert_eq!(data.rows.len(), 2);
    assert_eq!(data.rows[0]["id"], Value::Int(2));
    assert_eq!(data.rows[0]["payload"].as_str(), Some("AP8="));
    assert_eq!(data.rows[1]["id"], Value::Int(4));
    assert_eq!(
        data.rows[1]["measurement"].as_str(),
        Some("40000000000000000000.0500")
    );
    assert_eq!(
        data.dtypes.get("payload").map(String::as_str),
        Some("bytes")
    );
    assert_eq!(
        data.dtypes.get("measurement").map(String::as_str),
        Some("numeric")
    );

    request.limit = None;
    let url = format!("{origin}{}", source_fetch::page_path(&request.pipe));
    let mut page = PageRequest {
        version: source_fetch::PAGE_VERSION,
        request,
        cursor: None,
        close: false,
    };
    let first: PageResponse = reply(
        client
            .post(&url)
            .bearer_auth(bearer)
            .json(&page)
            .send()
            .await
            .unwrap(),
    )
    .await;
    first.validate(&page).unwrap();
    assert_eq!(first.data.rows, data.rows);
    assert!(!first.complete);
    page.cursor = first.cursor;
    assert!(page.cursor.is_some());
    // A normal writer can commit after capture. The next page is still the frozen result.
    db.driver()
        .exec(&format!(
            "UPDATE {schema}.samples SET measurement = 55.55 WHERE id = 5"
        ))
        .await
        .unwrap();
    let last: PageResponse = reply(
        client
            .post(&url)
            .bearer_auth(bearer)
            .json(&page)
            .send()
            .await
            .unwrap(),
    )
    .await;
    last.validate(&page).unwrap();
    assert!(last.complete);
    assert_eq!(last.data.rows.len(), 1);
    assert_eq!(last.data.rows[0]["id"], Value::Int(5));
    assert!(last.data.rows[0]["payload"].is_null());
    assert_eq!(last.data.rows[0]["measurement"].as_str(), Some("5.60"));
    serve.abort();
    db.driver().close().await;
}
