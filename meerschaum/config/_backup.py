#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Back up and restore configuration key files so that edits may be rolled back.

A snapshot of a key's on-disk file (e.g. `stack.yaml`) is taken before it is
edited. If an edit corrupts the file or overwrites values with defaults, the
previous state can be restored via `restore_config_backup()`.
"""

from __future__ import annotations

import pathlib

from meerschaum.utils.typing import Optional, List, SuccessTuple
from meerschaum import _core

### Timestamps are UTC and lexically sortable so filename order == chronological order.
_TIMESTAMP_FORMAT: str = '%Y%m%d-%H%M%S-%f'


def get_key_backups_dir(key: str, create: bool = False) -> pathlib.Path:
    """Return the directory holding backups for a config `key`."""
    _rust = _core.get_key_backups_dir
    return _rust(key, create=create)


def list_config_backups(key: str) -> List[pathlib.Path]:
    """Return existing backups for `key`, newest first."""
    _rust = _core.list_config_backups
    return _rust(key)


def backup_config_key(key: str, debug: bool = False) -> Optional[pathlib.Path]:
    """
    Copy the current on-disk file for `key` into the backups directory.

    Parameters
    ----------
    key: str
        The configuration key to back up (e.g. `'stack'`).

    Returns
    -------
    The path to the new backup, the most recent backup if the file is unchanged,
    or `None` if there was nothing to back up.
    """
    _rust = _core.backup_config_key
    return _rust(key, debug=debug)


def restore_config_backup(
    key: str,
    backup_path: Optional[pathlib.Path] = None,
    index: int = 0,
    reload: bool = True,
    debug: bool = False,
) -> SuccessTuple:
    """
    Restore a key's config file from a backup.

    The current (possibly broken) file is itself backed up first, so a restore
    can always be undone.

    Parameters
    ----------
    key: str
        The configuration key to restore (e.g. `'stack'`).

    backup_path: Optional[pathlib.Path], default None
        A specific backup file to restore.
        If `None`, restore the `index`-th most recent backup.

    index: int, default 0
        Which backup to restore when `backup_path` is not provided (0 = newest).

    reload: bool, default True
        If `True`, reload Meerschaum after restoring so the new config takes effect.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    success, message = _core.restore_config_backup(key, backup_path=backup_path, index=index, debug=debug)
    if success and reload:
        from meerschaum.utils.packages import reload_meerschaum
        reload_meerschaum(debug=debug)
    return success, message
