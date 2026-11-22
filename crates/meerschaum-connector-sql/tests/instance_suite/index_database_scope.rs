//! MySQL index metadata belongs to the selected database. Same-named tables in another
//! database must not make an unindexed table look ready for upserts.

use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::uri::ConnectionSpec;

#[tokio::test]
async fn mysql_index_metadata_does_not_leak_across_databases() {
    let Ok(uri) = std::env::var("MRSM_TEST_MYSQL_URI") else {
        eprintln!("skipped: MRSM_TEST_MYSQL_URI is not set");
        return;
    };
    let admin = SqlConnector::from_uri("index_admin", &uri).await.unwrap();
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let databases = [format!("mrsm_ix_{tag}_a"), format!("mrsm_ix_{tag}_b")];
    for database in &databases {
        admin
            .driver()
            .exec(&format!("CREATE DATABASE `{database}`"))
            .await
            .unwrap();
    }
    let mut connections = Vec::new();
    for database in &databases {
        let mut spec = ConnectionSpec::from_uri(&uri, Some("index_scope"), None).unwrap();
        spec.uri = None;
        spec.database = Some(database.clone());
        let conn = SqlConnector::connect("index_scope", spec).await.unwrap();
        conn.driver()
            .exec("CREATE TABLE shared_indices (id INTEGER, payload INTEGER)")
            .await
            .unwrap();
        connections.push(conn);
    }
    let a = &connections[0];
    let b = &connections[1];
    a.driver()
        .exec("CREATE UNIQUE INDEX idx_payload ON shared_indices (payload)")
        .await
        .unwrap();
    let a_indices = a.get_table_cols_indices("shared_indices", None).await;
    assert_eq!(a_indices.len(), 1, "{a_indices:?}");
    assert_eq!(a_indices["payload"][0].name.as_deref(), Some("idx_payload"));
    assert!(
        b.get_table_cols_indices("shared_indices", None)
            .await
            .is_empty(),
        "an unindexed table must not inherit another database's unique key"
    );
    assert!(
        a.get_table_cols_indices("shared_indices", Some(&databases[1]))
            .await
            .is_empty(),
        "an explicitly selected database must not fall back to the current database"
    );
    assert_eq!(
        b.get_table_cols_indices("shared_indices", Some(&databases[0]))
            .await,
        a_indices,
        "explicit schema selection takes precedence over the current database"
    );
    b.driver()
        .exec("CREATE INDEX idx_identity ON shared_indices (id)")
        .await
        .unwrap();
    let b_indices = b.get_table_cols_indices("shared_indices", None).await;
    assert_eq!(b_indices.len(), 1, "{b_indices:?}");
    assert_eq!(b_indices["id"][0].name.as_deref(), Some("idx_identity"));
    assert_eq!(
        a.get_table_cols_indices("shared_indices", None).await,
        a_indices
    );

    drop(connections);
    for database in &databases {
        admin
            .driver()
            .exec(&format!("DROP DATABASE `{database}`"))
            .await
            .unwrap();
    }
}
