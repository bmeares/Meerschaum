//! `meerschaum-py`: the PyO3 extension module `meerschaum._core` (design §4.2, D3, D12).
//!
//! The bridge:
//!
//! - `__version__` and `build_info()`: what was built, with which toolchain and features.
//! - `static_config()`: `crates/meerschaum-core/static.toml` (D15), parsed by `meerschaum-core`
//!   (embedded at build time; `HAS_STATIC_CONFIG` is always true).
//! - `roundtrip_arrow(obj)`: the Arrow PyCapsule stream boundary. Accepts any object implementing
//!   `__arrow_c_stream__` (or `__arrow_c_array__`) and returns an [`ArrowStream`] that exports the
//!   same data back through `__arrow_c_stream__`, without importing pyarrow.
//! - `polars_roundtrip(df)`: `polars.DataFrame` -> `polars::DataFrame` -> `polars.DataFrame` over the Arrow C Stream.
//!
//! The kernel ([`kernel_api`]): `enforce_dtypes`, `filter_unseen_df`, `query_df`, the detectors
//! (`get_json_cols`, `get_numeric_cols`, `get_uuid_cols`, `get_datetime_cols`, `get_bytes_cols`,
//! `get_geometry_cols`) and the dtype-string helpers (`are_dtypes_equal`, `to_pandas_dtype`,
//! `is_dtype_numeric`, `get_geometry_type_srid`, `get_pd_type_from_db_type`). DataFrames cross
//! through [`pandas_bridge`], scalars through [`convert`].
//!
//! The core ([`core_api`]): `get_config`, `replace_pipes_syntax`, `hash_password`,
//! `verify_password`.
//!
//! The Python-facing loader is `meerschaum/_core/__init__.py`; see this crate's README.

use pyo3::prelude::*;

pub mod access_api;
pub mod api_client_api;
pub mod arrow_bridge;
pub mod build_info;
pub mod compose_api;
pub mod config_api;
pub mod convert;
pub mod core_api;
pub mod d14_api;
pub mod dataframe_api;
mod duckdb_transport;
pub mod engine_api;
pub mod entry_api;
pub mod facade;
pub mod ffi_export;
pub mod ffi_import;
pub mod instance_features_api;
pub mod jobs_api;
pub mod kernel_api;
pub mod logging;
pub mod main_api;
pub mod metrics_api;
pub mod native_plugin_api;
pub mod pandas_bridge;
pub mod pipe_api;
pub mod pipe_bridge;
pub mod pipe_methods;
pub mod pipe_methods_api;
pub mod polars_bridge;
pub mod runtime;
pub mod sql_api;
pub mod sql_raw_api;
pub mod static_config;
pub mod status_api;
pub mod sync_api;
pub mod trace;
pub mod valkey_api;
pub mod verify_api;
pub mod version_api;
pub mod yaml_api;

/// The `meerschaum._core._native` extension module.
///
/// `gil_used = false` declares free-threading support (§12, R8): every exposed object is
/// immutable (`frozen` pyclasses) and no function keeps unsynchronized global state.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    logging::install();
    duckdb_transport::register(m)?;
    // Python owns this process's signals: no `mrsm` stop handler that could end the interpreter.
    meerschaum_cli::native::jobs::set_embedded(true);
    m.add("__version__", build_info::VERSION)?;
    m.add("HAS_STATIC_CONFIG", static_config::STATIC_TOML.is_some())?;

    m.add_class::<logging::Debugging>()?;
    m.add_function(wrap_pyfunction!(build_info::py_build_info, m)?)?;
    m.add_function(wrap_pyfunction!(static_config::py_static_config, m)?)?;

    m.add_class::<arrow_bridge::ArrowStream>()?;
    m.add_function(wrap_pyfunction!(arrow_bridge::roundtrip_arrow, m)?)?;

    m.add_function(wrap_pyfunction!(polars_bridge::polars_roundtrip, m)?)?;

    // The DataFrame library setting: `frame_from_arrow`, the root pin (rc15, Q54)
    dataframe_api::register(m)?;

    // meerschaum-kernel
    m.add_function(wrap_pyfunction!(kernel_api::enforce_dtypes, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::filter_unseen_df, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::query_df, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_json_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_numeric_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_uuid_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_datetime_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_bytes_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_geometry_cols, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::to_pandas_dtype, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::are_dtypes_equal, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::is_dtype_numeric, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_geometry_type_srid, m)?)?;
    m.add_function(wrap_pyfunction!(kernel_api::get_pd_type_from_db_type, m)?)?;

    // meerschaum-core
    // The config registry: `get_config`, `read_config`, `write_config`, backups, ... (wave 15, O)
    config_api::register(m)?;
    m.add_function(wrap_pyfunction!(core_api::replace_pipes_syntax, m)?)?;
    m.add_function(wrap_pyfunction!(core_api::hash_password, m)?)?;
    m.add_function(wrap_pyfunction!(core_api::verify_password, m)?)?;

    // PEP 440 requirement matching for the façade's dependency prechecks (V4-002)
    m.add_function(wrap_pyfunction!(version_api::version_satisfies, m)?)?;

    // meerschaum-engine (Syncing Engine, §5)
    m.add_function(wrap_pyfunction!(engine_api::engine_plan, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::extract_pipe_references, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::pipeline_mutation_lock, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::engine_run, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::engine_runs, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::engine_run_detail, m)?)?;
    m.add_function(wrap_pyfunction!(engine_api::engine_run_events, m)?)?;

    // D14 runway (wave 8)
    m.add_function(wrap_pyfunction!(d14_api::sql_item_name, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::build_where, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::d14_get_pipe_metadef, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::d14_sync_pipe_inplace, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::get_parameters, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::get_chunk_interval, m)?)?;
    m.add_function(wrap_pyfunction!(d14_api::get_chunk_bounds, m)?)?;

    // Pipe.verify() and the façade session (wave 9)
    m.add_function(wrap_pyfunction!(verify_api::verify_pipe, m)?)?;
    m.add_function(wrap_pyfunction!(verify_api::register_connector, m)?)?;
    m.add_function(wrap_pyfunction!(native_plugin_api::native_plugin_names, m)?)?;
    m.add_function(wrap_pyfunction!(
        native_plugin_api::native_connector_register,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        native_plugin_api::native_connector_types,
        m
    )?)?;
    m.add_class::<native_plugin_api::PyNativeConnector>()?;
    m.add_function(wrap_pyfunction!(verify_api::unregister_connector, m)?)?;

    // The native action dispatcher from Python (wave 16, P2)
    m.add_function(wrap_pyfunction!(entry_api::entry, m)?)?;
    m.add_function(wrap_pyfunction!(entry_api::run_action, m)?)?;
    m.add_function(wrap_pyfunction!(entry_api::core_actions, m)?)?;
    m.add_function(wrap_pyfunction!(entry_api::set_in_worker, m)?)?;
    m.add_function(wrap_pyfunction!(entry_api::plugin_import_begin, m)?)?;
    m.add_function(wrap_pyfunction!(entry_api::plugin_import_end, m)?)?;
    m.add_function(wrap_pyfunction!(compose_api::compose_init_env, m)?)?;
    m.add_function(wrap_pyfunction!(compose_api::compose_read_config, m)?)?;
    m.add_function(wrap_pyfunction!(compose_api::compose_env_dict, m)?)?;
    m.add_function(wrap_pyfunction!(compose_api::compose_dir_paths, m)?)?;
    m.add_function(wrap_pyfunction!(compose_api::compose_installed_plugins, m)?)?;

    // `Pipe.sync()` for explicit data (wave 13)
    m.add_function(wrap_pyfunction!(sync_api::sync_pipe, m)?)?;
    m.add_function(wrap_pyfunction!(sync_api::parse_sql_uri, m)?)?;

    m.add_class::<pipe_api::PyPipe>()?;

    // `Pipe` methods over an instance: get_data, filter_existing, deduplicate, ... (wave 15, L2)
    pipe_methods_api::register(m)?;

    // Protected connectors and `Pipe.verify_schema()` (rc15, Q20 / Q32)
    instance_features_api::register(m)?;

    // Per-pipe sync status: `pipe.get_sync_status()`, `mrsm.get_sync_statuses()` (rc15, C2)
    status_api::register(m)?;
    m.add_function(wrap_pyfunction!(metrics_api::engine_metrics, m)?)?;
    m.add_function(wrap_pyfunction!(metrics_api::initialize_engine_metrics, m)?)?;

    // The access model: permissions, roles, teams, pipe access and locks (rc15, K2)
    access_api::register(m)?;

    // `meerschaum.jobs.Job`, the executors and `utils.daemon.Daemon` over `meerschaum-jobs` (wave 16, S)
    m.add_function(wrap_pyfunction!(main_api::main, m)?)?;
    jobs_api::register(m)?;

    // PyYAML-compatible YAML codec with a lossless Python boundary (wave 13)
    yaml_api::register(m)?;

    // The `APIConnector` client and the `ValkeyConnector` (wave 16, S)
    api_client_api::register(m)?;
    valkey_api::register(m)?;

    // meerschaum-connector-sql (instance connector)
    m.add_class::<sql_api::PySqlConnector>()?;
    m.add_function(wrap_pyfunction!(sql_api::pipe_metadata_epoch, m)?)?;
    sql_raw_api::register(m)?;

    Ok(())
}
