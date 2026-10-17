#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The live config dictionary (`meerschaum.config.config`).

Python code holds the dictionaries `get_config()` returns and mutates them, and expects the
change on the next call, so the process-wide tree is one `dict` object. It lives here, in plain
Python, so that `import meerschaum` and `import meerschaum.config` never load the native
extension (an import must stay as cheap as v4's). The config registry in `meerschaum._core`
(task O of wave 15) adopts this very object the first time it is used: `config_api.rs::store()`
reads `_dict` from this module when it creates its own handle, and `set_store()` /
`reset_store()` hand a replacement to an already loaded extension. There is one dictionary
whichever side answers `get_config()`.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, Optional

_dict: Dict[str, Any] = {}

### The name under which the loader imports the compiled module (`meerschaum/_core/__init__.py`).
_EXTENSION_MODULE = 'meerschaum._core._core'


def _loaded_extension() -> Optional[Any]:
    """Return the extension if something already loaded it; never loads it."""
    return sys.modules.get(_EXTENSION_MODULE)


def get_store() -> Dict[str, Any]:
    """Return the live config dictionary."""
    return _dict


def set_store(cf: Dict[str, Any]) -> Dict[str, Any]:
    """Make `cf` the live config dictionary (the object itself) and return it."""
    global _dict
    _dict = cf
    extension = _loaded_extension()
    if extension is not None and hasattr(extension, 'config_set'):
        extension.config_set(cf)
    return cf


def reset_store() -> Dict[str, Any]:
    """Start over from an empty dictionary (what reloading `meerschaum.config` does)."""
    return set_store({})
