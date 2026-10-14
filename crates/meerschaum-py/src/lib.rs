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
//! - `polars_roundtrip(df)`: the `pyo3-polars` path (feature `polars`).
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

pub mod arrow_bridge;
pub mod build_info;
pub mod convert;
pub mod core_api;
pub mod d14_api;
pub mod engine_api;
pub mod ffi_import;
pub mod kernel_api;
pub mod pandas_bridge;
pub mod pipe_api;
pub mod pipe_bridge;
#[cfg(feature = "polars")]
pub mod polars_bridge;
pub mod runtime;
pub mod sql_api;
pub mod static_config;
pub mod trace;
pub mod verify_api;

/// The `meerschaum._core._core` extension module.
///
/// `gil_used = false` declares free-threading support (§12, R8): every exposed object is
/// immutable (`frozen` pyclasses) and no function keeps unsynchronized global state.
#[pymodule(gil_used = false)]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", build_info::VERSION)?;
    m.add("HAS_STATIC_CONFIG", static_config::STATIC_TOML.is_some())?;
    m.add("HAS_POLARS", cfg!(feature = "polars"))?;

    m.add_function(wrap_pyfunction!(build_info::py_build_info, m)?)?;
    m.add_function(wrap_pyfunction!(static_config::py_static_config, m)?)?;

    m.add_class::<arrow_bridge::ArrowStream>()?;
    m.add_function(wrap_pyfunction!(arrow_bridge::roundtrip_arrow, m)?)?;

    #[cfg(feature = "polars")]
    m.add_function(wrap_pyfunction!(polars_bridge::polars_roundtrip, m)?)?;

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
    m.add_function(wrap_pyfunction!(core_api::get_config, m)?)?;
    m.add_function(wrap_pyfunction!(core_api::replace_pipes_syntax, m)?)?;
    m.add_function(wrap_pyfunction!(core_api::hash_password, m)?)?;
    m.add_function(wrap_pyfunction!(core_api::verify_password, m)?)?;

    // meerschaum-engine (Syncing Engine, §5)
    m.add_function(wrap_pyfunction!(engine_api::engine_plan, m)?)?;
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
    m.add_function(wrap_pyfunction!(verify_api::unregister_connector, m)?)?;

    m.add_class::<pipe_api::PyPipe>()?;

    // meerschaum-sql (instance connector)
    m.add_class::<sql_api::PySqlConnector>()?;
    m.add_function(wrap_pyfunction!(sql_api::pipe_metadata_epoch, m)?)?;

    Ok(())
}
