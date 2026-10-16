//! `_core.version_satisfies(version, specifiers) -> bool | None`: PEP 440 requirement matching.
//!
//! The façade's dependency prechecks (`meerschaum.utils.packages.need_update`) use it when
//! `MRSM_NO_AUTO_INSTALL` is set and no version parser (`semver`) is importable, so that the
//! base wheel never has to download one (V4-002).

use pyo3::prelude::*;

/// `True` / `False` when both sides parse as PEP 440, `None` when either does not.
#[pyfunction]
pub fn version_satisfies(version: &str, specifiers: &str) -> Option<bool> {
    meerschaum_core::version::version_satisfies(version, specifiers)
}
