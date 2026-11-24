#[allow(dead_code)]
#[path = "../../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;
#[allow(dead_code)]
#[path = "../common/native_examples.rs"]
mod native_examples;
#[path = "../plugin_lifecycle_support/mod.rs"]
mod plugin_lifecycle_support;
#[path = "../common/process.rs"]
mod process;
#[allow(dead_code)]
#[path = "../python_support/mod.rs"]
mod python_support;

#[cfg(unix)]
mod entry_point_plugins;
mod native_api_mcp;
mod native_e2e;
#[cfg(unix)]
mod native_executor_stream;
#[cfg(unix)]
mod native_host_calls;
mod native_plugins;
mod native_single_file;
mod native_source_plugin;
mod plugin_lifecycle;
mod plugin_manifest_cache;
mod plugin_package_venv_actions;
#[cfg(unix)]
mod plugin_token_scopes;
#[cfg(unix)]
mod prune_venvs;
mod register_plugin_hook;
mod rust_plugin_admin;
#[cfg(unix)]
mod self_upgrade;
mod single_file_extensions;
mod single_file_parity;
#[cfg(unix)]
mod single_file_server;
mod venvs;
