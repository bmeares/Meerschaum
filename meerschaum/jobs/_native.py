#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The bridge to the native jobs (`meerschaum._core`, crate `meerschaum-jobs`).

Importing this module is cheap; the extension is loaded on first use.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

_lock = threading.Lock()
_binary_hinted = False

#: The attributes of an `api:<label>` connector the native client needs.
API_ATTRIBUTES = (
    'host', 'port', 'protocol', 'uri', 'username', 'password', 'client_id', 'client_secret',
    'api_key', 'verify', 'instance_keys',
)


def core():
    """
    Return the `meerschaum._core` module, telling it once where the `mrsm` binary is (jobs run
    `mrsm <sysargs>`; see `meerschaum._internal.binary.find_binary`).
    """
    global _binary_hinted
    from meerschaum import _core
    if not _binary_hinted:
        with _lock:
            if not _binary_hinted:
                path = None
                try:
                    from meerschaum._internal.binary import find_binary
                    path = find_binary()
                except Exception:
                    path = None
                if path:
                    _core.jobs_set_binary(path)
                _binary_hinted = True
    return _core


def api_attributes(executor_keys: Optional[str]) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    The attributes of the `api:<label>` connector behind `executor_keys` (a connector defined in
    this process need not be in the configuration), as `{keys: attributes}`, or `None`.
    """
    if not executor_keys or not str(executor_keys).startswith('api:'):
        return None
    try:
        import meerschaum as mrsm
        conn = mrsm.get_connector(executor_keys)
    except Exception:
        return None
    if conn is None:
        return None
    attributes = {
        key: conn.__dict__[key]
        for key in API_ATTRIBUTES
        if key in conn.__dict__
        and isinstance(conn.__dict__[key], (str, int, float, bool))
    }
    return {str(executor_keys): attributes} if attributes else None


def emit_warnings(messages) -> None:
    """Show the warnings the native operations collected."""
    if not messages:
        return
    from meerschaum.utils.warnings import warn
    for message in messages:
        warn(message, stack=False)
