#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The bridge to the native jobs (`meerschaum._core`, crate `meerschaum-jobs`).

Importing this module is cheap; the extension is loaded on first use.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

#: The attributes of an `api:<label>` connector the native client needs.
API_ATTRIBUTES = (
    'host', 'port', 'protocol', 'uri', 'username', 'password', 'client_id', 'client_secret',
    'api_key', 'verify', 'instance_keys',
)


def core():
    """
    Return the `meerschaum._core` module. Jobs run `<self> <sysargs>`, where `<self>` is this
    process's own command (`python -m meerschaum`, or the `MRSM_BIN` binary): the extension
    works that out itself (`meerschaum_core::self_exe`).
    """
    from meerschaum import _core
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
