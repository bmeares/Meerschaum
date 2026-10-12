//! `meerschaum-py`: the PyO3 extension module `meerschaum._core` (design §4.2, D3, D12).
//!
//! This wave binds only the bridge itself, no kernel or core functions:
//!
//! - `__version__` and `build_info()`: what was built, with which toolchain and features.
//! - `static_config()`: `crates/meerschaum-core/static.toml` (D15) embedded at build time, or
//!   `None` when the file did not exist when the extension was compiled.
//! - `roundtrip_arrow(obj)`: the Arrow PyCapsule stream boundary. Accepts any object implementing
//!   `__arrow_c_stream__` (or `__arrow_c_array__`) and returns an [`ArrowStream`] that exports the
//!   same data back through `__arrow_c_stream__`, without importing pyarrow.
//! - `polars_roundtrip(df)`: the `pyo3-polars` path (feature `polars`).
//!
//! The Python-facing loader is `meerschaum/_core/__init__.py`; see this crate's README.

use pyo3::prelude::*;

pub mod arrow_bridge;
pub mod build_info;
pub mod ffi_import;
#[cfg(feature = "polars")]
pub mod polars_bridge;
pub mod static_config;

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

    Ok(())
}
