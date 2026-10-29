//! The front crate's public surface compiles, per feature, and carries the workspace version.

use meerschaum::prelude::*;

#[test]
fn the_prelude_names_resolve() {
    let pipe = Pipe::new("plugin:demo", "metric").unwrap();
    assert_eq!(pipe.connector_keys(), "plugin:demo");
    let ok: SuccessTuple = (true, "ok").into();
    assert!(ok.success);
    let value = Value::from("x");
    assert_eq!(value.as_str(), Some("x"));
    let _: Map = Map::new();
    let _ = meerschaum::static_config();
}

#[test]
fn the_version_is_the_workspace_version() {
    assert_eq!(meerschaum::VERSION, env!("CARGO_PKG_VERSION"));
    assert!(meerschaum::core::version::version_ge("5.0.0", "4.1.4"));
}

#[cfg(feature = "sql")]
#[test]
fn sql_is_reexported() {
    assert!(std::mem::size_of::<meerschaum::SqlConnector>() > 0);
    let _ = meerschaum::sql::Flavor::Sqlite;
}

#[cfg(feature = "api")]
#[test]
fn api_is_reexported() {
    assert!(std::mem::size_of::<meerschaum::APIConnector>() > 0);
}

#[cfg(feature = "valkey")]
#[test]
fn valkey_is_reexported() {
    assert!(std::mem::size_of::<meerschaum::ValkeyConnector>() > 0);
}

#[cfg(feature = "engine")]
#[test]
fn engine_is_reexported() {
    let _ = std::any::type_name::<meerschaum::Pipeline>();
    let _ = std::any::type_name::<meerschaum::engine::Plan>();
}

#[cfg(feature = "plugin")]
#[test]
fn plugin_sdk_is_reexported() {
    fn takes_plugin<P: meerschaum::Plugin>() {}
    let _ = takes_plugin::<DemoPlugin>;

    #[derive(Default)]
    struct DemoPlugin;
    impl meerschaum::Plugin for DemoPlugin {
        const NAME: &'static str = "demo";
        const VERSION: &'static str = "0";
        fn manifest(&self) -> meerschaum::plugin::serde_json::Value {
            meerschaum::plugin::serde_json::Value::Null
        }
    }
}
