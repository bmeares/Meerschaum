#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The `MRSM_CORE` dispatch gate. **Development-only, for the v5 parity phase.**

During the parity phase both implementations live in the tree: the v4 Python code and the Rust
crates bound in `meerschaum._core`. The functions the kernel and core crates replace first carry
a guard at the top:

```python
if (_rust := rust_impl('filter_unseen_df')) is not None:
    return _rust(old_df, new_df, ...)
```

`rust_impl()` returns `None` (run the Python body) unless the gate selects the feature **and** the
extension binds a symbol of that name. A symbol that is not bound yet (`AttributeError`) falls
through to Python; errors raised *inside* a bound Rust function propagate. If the gate selects
Rust but the extension is not built at all, `meerschaum._core.CoreNotBuiltError` is raised with the
build instructions.

`MRSM_CORE` is read once, here, at import time:

- `python` (default): always run the Python implementations.
- `rust`: prefer `meerschaum._core` for every gated feature.
- `rust:feature_a,feature_b`: prefer Rust only for the listed features.

D14: the shipped 5.0 package has **no** Python core and **no** `MRSM_CORE` switch; the parity
oracle is v4.1.x in a separate venv (§10.2). This module and every `rust_impl()` guard are
deleted when the façade switches to `_core`. Do not reference `MRSM_CORE` from user-facing code
or documentation.
"""

from __future__ import annotations

import os
import warnings
from typing import Any, Callable, FrozenSet, Optional, Tuple

ENV_VAR = 'MRSM_CORE'
PYTHON = 'python'
RUST = 'rust'

#: Features with a guard in the Python package, mapped to the module defining the guarded function.
GATED_FEATURES = {
    'filter_unseen_df': 'meerschaum.utils.dataframe',
    'enforce_dtypes': 'meerschaum.utils.dataframe',
    'query_df': 'meerschaum.utils.dataframe',
    'get_json_cols': 'meerschaum.utils.dataframe',
    'get_numeric_cols': 'meerschaum.utils.dataframe',
    'get_uuid_cols': 'meerschaum.utils.dataframe',
    'get_datetime_cols': 'meerschaum.utils.dataframe',
    'get_bytes_cols': 'meerschaum.utils.dataframe',
    'get_geometry_cols': 'meerschaum.utils.dataframe',
    'are_dtypes_equal': 'meerschaum.utils.dtypes',
    'to_pandas_dtype': 'meerschaum.utils.dtypes',
    'is_dtype_numeric': 'meerschaum.utils.dtypes',
    'get_geometry_type_srid': 'meerschaum.utils.dtypes',
    'get_pd_type_from_db_type': 'meerschaum.utils.dtypes.sql',
    'hash_password': 'meerschaum.core.User._User',
    'verify_password': 'meerschaum.core.User._User',
    'replace_pipes_syntax': 'meerschaum.utils.pipes',
    'get_config': 'meerschaum.config',
}

#: Classes bound in `meerschaum._core` whose methods replace a v4 class's methods, mapped to the
#: module defining the guarded class. The method guards call `rust_method(self, name)`, which asks
#: the Python object for its Rust counterpart (built once per object, see
#: `SQLConnector._rust_method`). The feature name for `MRSM_CORE=rust:...` is the lowercased
#: class name (`sqlconnector`).
GATED_CLASSES = {
    'SqlConnector': 'meerschaum.connectors.sql._SQLConnector',
}


def parse_mode(value: Optional[str]) -> Tuple[str, Optional[FrozenSet[str]]]:
    """
    Parse an `MRSM_CORE` value into `(mode, features)`.
    `features` is `None` when every feature is selected.
    Unrecognized values warn and fall back to `python`.
    """
    text = (value or '').strip().lower()
    if not text or text == PYTHON:
        return PYTHON, None
    if text == RUST:
        return RUST, None
    if text.startswith(RUST + ':'):
        features = frozenset(f.strip() for f in text[len(RUST) + 1:].split(',') if f.strip())
        return (RUST, features) if features else (PYTHON, None)

    warnings.warn(
        f"Ignoring unrecognized {ENV_VAR}={value!r} "
        f"(expected '{PYTHON}', '{RUST}', or '{RUST}:feature,...').",
        stacklevel=2,
    )
    return PYTHON, None


_MODE, _FEATURES = parse_mode(os.environ.get(ENV_VAR))


def mode() -> str:
    """Return the active mode (`'python'` or `'rust'`)."""
    return _MODE


def use_rust(feature: str) -> bool:
    """Return whether the gate prefers the Rust implementation of `feature`."""
    if _MODE == PYTHON:
        return False
    return _FEATURES is None or feature.lower() in _FEATURES


def rust_impl(feature: str, name: Optional[str] = None) -> Optional[Callable[..., Any]]:
    """
    Return the `meerschaum._core` callable for `feature` (or `name`), or `None` to run Python.

    Raises `meerschaum._core.CoreNotBuiltError` if the gate selects Rust but the extension is
    not built.
    """
    if _MODE == PYTHON or not use_rust(feature):
        return None

    from meerschaum import _core
    try:
        return getattr(_core, name or feature)
    except AttributeError:
        return None


def rust_method(obj: Any, name: str) -> Optional[Callable[..., Any]]:
    """
    Return the Rust method `name` bound to `obj`'s `_core` counterpart, or `None` to run Python.

    The guarded v4 methods are plain functions that tests and plugins also call with stub objects
    as `self` (duck typing, design §4.4), so an object without `_rust_method` runs Python.
    """
    if _MODE == PYTHON:
        return None
    getter = getattr(obj, '_rust_method', None)
    if getter is None:
        return None
    return getter(name)


def set_mode(value: Optional[str]) -> None:
    """Re-parse the gate from `value` (an `MRSM_CORE` string). For tests only."""
    global _MODE, _FEATURES
    _MODE, _FEATURES = parse_mode(value)


def reload() -> None:
    """Re-read `MRSM_CORE` from the environment. For tests only."""
    set_mode(os.environ.get(ENV_VAR))
