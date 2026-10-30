//! Synthetic wave-7 console test plugin (Rust, native C ABI): a page-contract page embedding the
//! same Plotly figure JSON as the Python plugin `w7_pyfig`. Built by `tests/console/build_native.sh`
//! (a copy of the SDK example's shape, with only a page). All data is synthetic.
use meerschaum_plugin::{
    Error, Plugin, Reply, Request, Result,
    serde_json::{Value, json},
};

/// Must stay byte-identical to `FIGURE_JSON` in `tests/plugins/w7_pyfig/__init__.py`.
const FIGURE_JSON: &str = r#"{"data":[{"type":"bar","x":["a","b","c"],"y":[3,1,2],"name":"w7"}],"layout":{"title":{"text":"W7 figure"}}}"#;

#[derive(Default)]
struct Fig;

impl Plugin for Fig {
    const NAME: &'static str = "w7_native_fig";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> Value {
        json!({"schema_version":meerschaum_plugin::MANIFEST_SCHEMA_VERSION,"meerschaum_version":"5.0.0-rc.20","python_version":"","fingerprint":null,
          "plugins":{"w7_native_fig":{"name":"w7_native_fig","version":"1.0.0","data_functions":[],"actions":[],"arguments":[],"pre_sync_hooks":[],"post_sync_hooks":[],"connectors":[],"api_plugins":[],"web_pages":[{"path":"/dash/w7-native-figure","page_group":"W7 Rust","page_key":"w7-native-figure","login_required":true,"function":"page","mode":"html"}]}},
          "connector_types":{},"pre_sync_hooks":[],"post_sync_hooks":[]})
    }
    fn call(&self, r: &Request, _data: Option<meerschaum_plugin::Frame>) -> Result<Reply> {
        match r.method.as_str() {
            "page" => Ok(Reply::json(json!({
                "html": format!(
                    "<section id=\"w7-rs-page\"><h2>W7 Rust page</h2><div id=\"w7-figure\" data-mrsm-figure='{FIGURE_JSON}'></div></section>"
                )
            }))),
            _ => Err(Error::Unsupported(format!("unsupported method {}", r.method))),
        }
    }
}
meerschaum_plugin::export_plugin!(Fig);
