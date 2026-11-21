#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The configuration file that holds the connectors (`meerschaum:connectors`), as this API host
tracks it: a team's database connector written here (`meerschaum.api.dash.team_setup`) or by
another process (`bootstrap teams`) reaches the live config without a restart.
"""

from __future__ import annotations

import copy
import os
import threading

from meerschaum.utils.typing import Optional

#: Held while the connectors file or the live connectors config is written or read again.
CONFIG_LOCK = threading.RLock()
_connectors_mtime: Optional[float] = None


def connectors_file() -> Optional[str]:
    """The configuration file under the root's `config/` that holds the connectors, if any."""
    try:
        from meerschaum.config.paths import CONFIG_DIR_PATH
        for name in ('meerschaum.yaml', 'meerschaum.yml', 'meerschaum.json'):
            path = os.path.join(str(CONFIG_DIR_PATH), name)
            if os.path.exists(path):
                return path
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
    Pick up the connectors another process wrote to the configuration file since this one read it
    (a team's database made with `bootstrap teams`): new labels are added to the live config, and
    nothing in memory is replaced or removed. Returns whether anything was added. Cheap: one
    `stat` unless the file changed.
    """
    global _connectors_mtime
    path = connectors_file()
    if path is None:
        return False
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return False
    if _connectors_mtime is None:
        _connectors_mtime = mtime
        return False
    if mtime == _connectors_mtime:
        return False
    added = False
    with CONFIG_LOCK:
        _connectors_mtime = mtime
        try:
            from meerschaum.config import get_config
            from meerschaum.config._read_config import read_config
            disk = read_config(keys=['meerschaum'], write_missing=False, substitute=True)
            fresh = (disk.get('meerschaum', None) or {}).get('connectors', None) or {}
            live = get_config('meerschaum', 'connectors', warn=False)
            if not isinstance(live, dict):
                return False
            for typ, labels in fresh.items():
                if not isinstance(labels, dict):
                    continue
                for label, attributes in labels.items():
                    if label not in live.setdefault(typ, {}):
                        live[typ][label] = copy.deepcopy(attributes)
                        added = True
        except Exception:
            return False
    return added
