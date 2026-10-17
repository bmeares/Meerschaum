#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Loader for `meerschaum._core`, the Rust extension module of Meerschaum v5 (design §4.2, D3).

The compiled module lives next to this file as `_core.abi3.so` (`_core.pyd` on Windows) and is
built by `scripts/rust/build_py.sh` (see `crates/meerschaum-py/README.md`). It is **not** built
by the root `pyproject.toml` during the v5 parity phase.

Importing this package is cheap: the extension is loaded lazily on first attribute access
(PEP 562), so `import meerschaum` never pays for it unless something asks for it.

Attributes
----------
- `AVAILABLE`: `True` if the extension is built and loaded.
- `LOAD_ERROR`: the `ImportError` raised while loading it, or `None`.
- `EXTENSION_PATH`: the file the extension was loaded from, or `None`.
- Every public name of the extension (`__version__`, `build_info`, `static_config`,
  `roundtrip_arrow`, `polars_roundtrip`, `ArrowStream`, ...).

Using a symbol of the extension when it is not built raises `CoreNotBuiltError` (an
`ImportError`) naming the build script. That includes `hasattr()` / `getattr(..., default)`,
which only swallow `AttributeError`: probe `AVAILABLE` first. A name the built extension does not define raises
`AttributeError`, which the parity-phase dispatch guards (`meerschaum._core._gate.rust_impl`)
treat as "not bound yet" and fall through to Python.

`import meerschaum`, `import meerschaum.config` and the pure helpers (`meerschaum.utils.dtypes`)
never load the extension: the live config dictionary is created in Python
(`meerschaum/config/_store.py`) and the extension adopts it on first use. Anything that routes to
Rust (the `MRSM_CORE` gate, a `get_config` in rust mode, the CLI) loads it then.

An extension built before the config registry moved to Rust (it lacks `REQUIRED_SYMBOLS`) is
reported as not built, with the build script in the error.

Only the standard library may be imported here: `meerschaum.config` imports this package.
"""

from __future__ import annotations

import importlib
import threading
from typing import Any, Optional

__all__ = (
    'AVAILABLE',
    'LOAD_ERROR',
    'EXTENSION_PATH',
    'CoreNotBuiltError',
    'load',
)

BUILD_SCRIPT = 'scripts/rust/build_py.sh'

### Symbols a current build must define; an older build is reported as not built.
REQUIRED_SYMBOLS = ('config_dict', 'config_set', 'config_reset')

_lock = threading.RLock()
_loaded = False
_extension = None


class CoreNotBuiltError(ImportError):
    """Raised when a `meerschaum._core` symbol is used but the extension is not built."""


def load() -> Optional[Any]:
    """
    Load the extension module once (thread-safe) and return it, or `None` if it is not built.

    On success, the extension's public names are copied into this package's namespace so later
    lookups are plain attribute reads.
    """
    global _loaded, _extension
    if _loaded:
        return _extension

    with _lock:
        if _loaded:
            return _extension

        namespace = globals()
        try:
            # `import_module` (not `from meerschaum._core import _core`) so the lookup never
            # re-enters this package's `__getattr__`.
            extension = importlib.import_module(__name__ + '._core')
        except ImportError as e:
            namespace['AVAILABLE'] = False
            namespace['LOAD_ERROR'] = e
            namespace['EXTENSION_PATH'] = None
            extension = None
        else:
            missing = [name for name in REQUIRED_SYMBOLS if not hasattr(extension, name)]
            if missing:
                # Built before the config registry moved to Rust: unusable, and an
                # `AttributeError` from the middle of `meerschaum.config` helps nobody.
                namespace['AVAILABLE'] = False
                namespace['LOAD_ERROR'] = CoreNotBuiltError(
                    "the built extension predates the Rust config registry "
                    f"(missing: {', '.join(missing)}); rebuild it with `{BUILD_SCRIPT}`"
                )
                namespace['EXTENSION_PATH'] = None
                _extension = None
                _loaded = True
                return None
            for name in dir(extension):
                if name.startswith('_') and name != '__version__':
                    continue
                namespace[name] = getattr(extension, name)
            namespace['AVAILABLE'] = True
            namespace['LOAD_ERROR'] = None
            namespace['EXTENSION_PATH'] = getattr(extension, '__file__', None)

        _extension = extension
        _loaded = True
        return _extension


def __getattr__(name: str) -> Any:
    """Load the extension on first access and resolve `name` (PEP 562)."""
    if (name.startswith('__') and name != '__version__') or name == '_core':
        raise AttributeError(f"module 'meerschaum._core' has no attribute '{name}'")

    extension = load()
    namespace = globals()
    if name in namespace:
        return namespace[name]

    if extension is None:
        raise CoreNotBuiltError(
            f"meerschaum._core.{name} requires the Rust extension, which is not built "
            f"({LOAD_ERROR}).\n"
            f"Build it from the repository root with `{BUILD_SCRIPT}` "
            f"(or unset MRSM_CORE to use the Python implementations)."
        )

    raise AttributeError(f"module 'meerschaum._core' has no attribute '{name}'")


def __dir__():
    load()
    return sorted(set(globals()) | set(__all__))
