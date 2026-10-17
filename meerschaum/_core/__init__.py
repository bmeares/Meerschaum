#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Loader for `meerschaum._core`, the Rust extension module of Meerschaum v5 (design §4.2, D3).

The compiled module lives next to this file as `_native.abi3.so` (`_native.pyd` on Windows) and is
built by `scripts/rust/build_py.sh` (see `crates/meerschaum-py/README.md`); the `meerschaum`
wheel ships it. It is **required**: importing this package raises `CoreNotBuiltError` (an
`ImportError`) naming the build script when the file is missing.

Importing this package is cheap: the extension is loaded lazily on first attribute access
(PEP 562), so `import meerschaum` never pays for it unless something asks for it.

Attributes
----------
- `AVAILABLE`: `True` if the extension is built and loaded.
- `LOAD_ERROR`: the `ImportError` raised while loading it, or `None`.
- `EXTENSION_PATH`: the file the extension was loaded from, or `None`.
- Every public name of the extension (`__version__`, `build_info`, `static_config`,
  `roundtrip_arrow`, `polars_roundtrip`, `ArrowStream`, ...).

Using a symbol of the extension when it cannot be loaded raises `CoreNotBuiltError`. That includes
`hasattr()` / `getattr(..., default)`, which only swallow `AttributeError`: probe `AVAILABLE`
first. A name the loaded extension does not define raises `AttributeError`.

`import meerschaum` and `import meerschaum.config` never load the extension: the live config
dictionary is created in Python (`meerschaum/config/_store.py`) and the extension adopts it on
first use. Anything that routes to Rust (`get_config` through the registry, a `Pipe`, the CLI)
loads it then.

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
_BUILD_HELP = (
    f"Install the `meerschaum` wheel (`pip install meerschaum`) or, from a source checkout, "
    f"build the extension from the repository root with `{BUILD_SCRIPT}`."
)

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
            # `import_module` (not `from meerschaum._core import _native`) so the lookup never
            # re-enters this package's `__getattr__`.
            extension = importlib.import_module(__name__ + '._native')
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
    if (name.startswith('__') and name != '__version__') or name == '_native':
        raise AttributeError(f"module 'meerschaum._core' has no attribute '{name}'")

    extension = load()
    namespace = globals()
    if name in namespace:
        return namespace[name]

    if extension is None:
        raise CoreNotBuiltError(
            f"meerschaum._core.{name} requires the Rust extension, which is not built "
            f"({LOAD_ERROR}).\n"
            f"{_BUILD_HELP}"
        )

    raise AttributeError(f"module 'meerschaum._core' has no attribute '{name}'")


def __dir__():
    load()
    return sorted(set(globals()) | set(__all__))


def _require_extension() -> None:
    """Fail `import meerschaum` early, with the build instructions, if the file is missing."""
    import importlib.util
    try:
        found = importlib.util.find_spec(__name__ + '._native') is not None
    except (ImportError, ValueError):
        found = False
    if not found:
        raise CoreNotBuiltError(
            "The Meerschaum native extension (`meerschaum._core`) is not installed. "
            + _BUILD_HELP
        )


_require_extension()
