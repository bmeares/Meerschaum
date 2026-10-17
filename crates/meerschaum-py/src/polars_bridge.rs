//! The `pyo3-polars` path: Python `polars.DataFrame` <-> Rust `polars::DataFrame`.
//!
//! `pyo3-polars` 0.24 converts column by column through the Arrow C Data Interface. Note that its
//! extraction calls `Series.to_arrow()`, which needs **pyarrow** at call time (not at import time);
//! the stream path in [`crate::arrow_bridge`] has no such requirement. Kernel bindings that take
//! polars frames should prefer `__arrow_c_stream__` unless they need a `polars::DataFrame`
//! directly.

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

/// `_core.polars_roundtrip(df: polars.DataFrame) -> polars.DataFrame`.
///
/// Extracts the frame into a Rust `polars::DataFrame` (validating it on the way) and returns it
/// to Python as a new polars frame sharing the same buffers.
#[pyfunction]
pub fn polars_roundtrip(df: PyDataFrame) -> PyResult<PyDataFrame> {
    let df: polars::prelude::DataFrame = df.into();
    // Touch the frame on the Rust side so the round trip proves a usable `DataFrame`, not only a
    // pointer hand-off.
    let height = df.height();
    if df.get_columns().iter().any(|c| c.len() != height) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Columns of the extracted DataFrame have mismatched lengths.",
        ));
    }
    Ok(PyDataFrame(df))
}
