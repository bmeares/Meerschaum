#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The configuration file that holds the connectors (`meerschaum:connectors`), as this API host
tracks it: a team's database connector written here (`meerschaum.api.dash.team_setup`) or by
another process (`bootstrap teams`) reaches the live config without a restart.
"""

from __future__ import annotations

import os
import threading

from meerschaum.utils.typing import Optional

#: Held while the connectors file or the live connectors config is written or read again.
CONFIG_LOCK = threading.RLock()
_connectors_mtime: Optional[float] = None


def connectors_file() -> Optional[str]:
    """The configuration file under the root's `config/` that holds the connectors, if any."""
    try:
        from meerschaum.config._read_config import get_keyfile_path
        path = get_keyfile_path('meerschaum')
        return str(path) if path is not None else None
    except Exception:
        pass
    return None


def remember_connectors_file() -> None:
    """Note the file's modification time after this process wrote it (nothing to read again)."""
    global _connectors_mtime
    path = connectors_file()
    try:
        _connectors_mtime = os.path.getmtime(path) if path else None
    except OSError:
        _connectors_mtime = None


def refresh_connectors() -> bool:
    """
    Pick up new connector labels and edited reservations from the file. Returns whether the
    live connector configuration changed.
    """
    global _connectors_mtime
    with CONFIG_LOCK:
        path = connectors_file()
        if path is None:
            return False
        try:
            mtime = os.path.getmtime(path)
            if mtime == _connectors_mtime:
                return False
            from meerschaum import _core
            from meerschaum.config import get_config
            get_config('meerschaum', 'connectors', warn=False)
            changed = _core.refresh_console_connectors()
            _connectors_mtime = mtime
            return changed
        except Exception:
            return False
