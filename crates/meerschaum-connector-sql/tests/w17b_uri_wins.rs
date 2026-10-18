//! Wave 17b: a `uri` attribute decides where the connection goes (v4 builds its engine from the
//! URI alone), whatever `host` / `port` / `username` / `password` / `database` a label's default
//! config keeps as attributes. The flavor of a URI naming another driver family wins; a flavor of
//! the same family stays as configured.

use meerschaum_connector_sql::flavor::Flavor;
use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};

/// `sql:main`'s default config (the attributes a fresh root hands the connector).
fn main_defaults(uri: &str) -> ConnectorAttributes {
    ConnectorAttributes {
        label: Some("main".to_string()),
        flavor: Some(Flavor::TimescaledbHa),
        uri: Some(uri.to_string()),
        host: Some("192.0.2.1".to_string()),
        port: Some(5432),
        username: Some("mrsm".to_string()),
        password: Some("mrsm".to_string()),
        database: Some("meerschaum".to_string()),
        ..Default::default()
    }
}

#[test]
fn uri_replaces_every_connection_attribute() {
    let spec =
        ConnectionSpec::from_attributes(&main_defaults("postgresql://u:p@127.0.0.1:5559/dbx"))
            .unwrap();
    assert_eq!(spec.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(spec.port, Some(5559));
    assert_eq!(spec.username.as_deref(), Some("u"));
    assert_eq!(spec.password.as_deref(), Some("p"));
    assert_eq!(spec.database.as_deref(), Some("dbx"));
}

#[test]
fn attributes_the_uri_omits_are_not_inherited() {
    // No port, no password: the default config's `5432` / `mrsm` must not leak into the target.
    let spec = ConnectionSpec::from_attributes(&main_defaults("postgresql://other@db.invalid/dbx"))
        .unwrap();
    assert_eq!(spec.host.as_deref(), Some("db.invalid"));
    assert_eq!(
        spec.port,
        Some(5432),
        "the flavor default, not the configured port"
    );
    assert_eq!(spec.username.as_deref(), Some("other"));
    assert_eq!(spec.password, None);

    let mut attrs = main_defaults("postgresql://other@db.invalid/dbx");
    attrs.port = Some(5459);
    let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
    assert_eq!(
        spec.port,
        Some(5432),
        "a configured port never applies to a URI without one"
    );
}

#[test]
fn file_database_uri_drops_the_default_host() {
    for (uri, flavor) in [
        ("sqlite:////tmp/w17b-x.db", Flavor::Sqlite),
        ("duckdb:////tmp/w17b-x.duckdb", Flavor::Duckdb),
    ] {
        let spec = ConnectionSpec::from_attributes(&main_defaults(uri)).unwrap();
        assert_eq!(spec.flavor, flavor, "{uri}");
        assert_eq!(spec.host, None, "{uri}");
        assert_eq!(spec.port, None, "{uri}");
        assert_eq!(spec.username, None, "{uri}");
        assert_eq!(spec.password, None, "{uri}");
        assert!(
            spec.database.as_deref().unwrap().starts_with("/tmp/w17b-x"),
            "{uri}"
        );
    }
}

#[test]
fn same_family_keeps_the_configured_flavor() {
    // PARITY: `timescaledb-ha` stays for a `postgresql://` URI (v4 kept the label's flavor).
    for uri in [
        "postgresql://u:p@127.0.0.1:5559/dbx",
        "postgresql+psycopg://u:p@127.0.0.1:5559/dbx",
        "postgres://u:p@127.0.0.1:5559/dbx",
    ] {
        let spec = ConnectionSpec::from_attributes(&main_defaults(uri)).unwrap();
        assert_eq!(spec.flavor, Flavor::TimescaledbHa, "{uri}");
    }
    let mut attrs = main_defaults("mysql://u:p@127.0.0.1:3310/dbx");
    attrs.flavor = Some(Flavor::Mariadb);
    assert_eq!(
        ConnectionSpec::from_attributes(&attrs).unwrap().flavor,
        Flavor::Mariadb
    );
    let mut attrs = main_defaults("sqlite:////tmp/w17b-x.db");
    attrs.flavor = Some(Flavor::Geopackage);
    assert_eq!(
        ConnectionSpec::from_attributes(&attrs).unwrap().flavor,
        Flavor::Geopackage
    );
}

#[test]
fn cross_family_takes_the_flavor_of_the_uri() {
    for (uri, flavor) in [
        ("mysql://u:p@127.0.0.1:3310/dbx", Flavor::Mysql),
        ("mariadb://u:p@127.0.0.1:3309/dbx", Flavor::Mariadb),
        (
            "mssql+pyodbc://u:p@127.0.0.1:1439/dbx?driver=ODBC+Driver+18+for+SQL+Server",
            Flavor::Mssql,
        ),
        ("oracle://u:p@127.0.0.1:1529/xe", Flavor::Oracle),
        ("sqlite:////tmp/w17b-x.db", Flavor::Sqlite),
        ("duckdb:////tmp/w17b-x.duckdb", Flavor::Duckdb),
    ] {
        let spec = ConnectionSpec::from_attributes(&main_defaults(uri)).unwrap();
        assert_eq!(spec.flavor, flavor, "{uri}");
        if flavor != Flavor::Sqlite && flavor != Flavor::Duckdb {
            assert_eq!(spec.host.as_deref(), Some("127.0.0.1"), "{uri}");
        }
    }
}

#[test]
fn a_sqlite_label_with_a_server_uri_takes_the_server_flavor() {
    // `sql:local`'s default config is `sqlite` with `database: {SQLITE_DB_PATH}`.
    let attrs = ConnectorAttributes {
        label: Some("local".to_string()),
        flavor: Some(Flavor::Sqlite),
        uri: Some("postgresql://u:p@127.0.0.1:5559/dbx".to_string()),
        database: Some("{SQLITE_DB_PATH}".to_string()),
        ..Default::default()
    };
    let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
    assert_eq!(spec.flavor, Flavor::Postgresql);
    assert_eq!(spec.database.as_deref(), Some("dbx"));
    assert_eq!(spec.host.as_deref(), Some("127.0.0.1"));
}

#[test]
fn scheme_flavors_still_override_an_explicit_flavor() {
    // PARITY: `timescaledb://` sets the flavor even when another one was given
    // (`_SQLConnector.py:211-219`).
    let mut attrs = main_defaults("timescaledb://u:p@127.0.0.1:5439/dbx");
    attrs.flavor = Some(Flavor::Postgis);
    assert_eq!(
        ConnectionSpec::from_attributes(&attrs).unwrap().flavor,
        Flavor::Timescaledb
    );
}

#[test]
fn without_a_uri_the_attributes_are_the_connection() {
    let mut attrs = main_defaults("");
    attrs.uri = None;
    let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
    assert_eq!(spec.host.as_deref(), Some("192.0.2.1"));
    assert_eq!(spec.flavor, Flavor::TimescaledbHa);
    assert_eq!(spec.database.as_deref(), Some("meerschaum"));
}
