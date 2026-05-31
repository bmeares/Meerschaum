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
from datetime import datetime, timezone

from meerschaum.utils.typing import Optional, List, SuccessTuple

### Timestamps are UTC and lexically sortable so filename order == chronological order.
_TIMESTAMP_FORMAT: str = '%Y%m%d-%H%M%S-%f'


def get_max_backups_per_key() -> int:
    """Return the number of backups to retain per config key."""
    from meerschaum._internal.static import STATIC_CONFIG
    return STATIC_CONFIG['config'].get('max_backups_per_key', 10)


def get_key_backups_dir(key: str, create: bool = False) -> pathlib.Path:
    """Return the directory holding backups for a config `key`."""
    import meerschaum.config.paths as paths
    backups_dir = pathlib.Path(paths.CONFIG_BACKUPS_RESOURCES_PATH) / key
    if create:
        backups_dir.mkdir(parents=True, exist_ok=True)
    return backups_dir


def list_config_backups(key: str) -> List[pathlib.Path]:
    """Return existing backups for `key`, newest first."""
    backups_dir = get_key_backups_dir(key)
    if not backups_dir.exists():
        return []
    backups = [path for path in backups_dir.iterdir() if path.is_file()]
    return sorted(backups, key=lambda path: path.name, reverse=True)


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
    import shutil
    from meerschaum.config._read_config import get_keyfile_path
    from meerschaum.utils.debug import dprint

    keyfile_path = get_keyfile_path(key, create_new=False)
    if keyfile_path is None or not keyfile_path.exists():
        if debug:
            dprint(f"No config file to back up for '{key}'.")
        return None

    ### Skip redundant backups if the newest one is byte-for-byte identical.
    existing = list_config_backups(key)
    try:
        current_bytes = keyfile_path.read_bytes()
        if existing and existing[0].read_bytes() == current_bytes:
            if debug:
                dprint(f"'{key}' unchanged since last backup; skipping.")
            return existing[0]
    except Exception:
        pass

    timestamp = datetime.now(timezone.utc).strftime(_TIMESTAMP_FORMAT)
    backups_dir = get_key_backups_dir(key, create=True)
    backup_path = backups_dir / f"{timestamp}{keyfile_path.suffix}"
    try:
        shutil.copy2(keyfile_path, backup_path)
    except Exception as e:
        if debug:
            dprint(f"Failed to back up '{key}':\n{e}")
        return None

    if debug:
        dprint(f"Backed up '{key}' to '{backup_path}'.")

    _prune_backups(key)
    return backup_path


def _prune_backups(key: str) -> None:
    """Delete the oldest backups beyond the retention limit."""
    max_backups = get_max_backups_per_key()
    for old_backup in list_config_backups(key)[max_backups:]:
        try:
            old_backup.unlink()
        except Exception:
            pass


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
    import shutil
    from meerschaum.config._read_config import get_keyfile_path
    from meerschaum.utils.packages import reload_meerschaum

    backups = list_config_backups(key)
    if backup_path is None:
        if not backups:
            return False, f"No configuration backups found for '{key}'."
        try:
            backup_path = backups[index]
        except IndexError:
            return False, (
                f"No backup at index {index} for '{key}' "
                f"(found {len(backups)})."
            )

    backup_path = pathlib.Path(backup_path)
    if not backup_path.exists():
        return False, f"Backup '{backup_path}' does not exist."

    keyfile_path = get_keyfile_path(key, create_new=True)
    ### Restore using the backup's original filetype.
    target_path = keyfile_path.parent / f"{key}{backup_path.suffix}"

    ### Snapshot the current state before overwriting it.
    backup_config_key(key, debug=debug)

    try:
        shutil.copy2(backup_path, target_path)
    except Exception as e:
        return False, f"Failed to restore '{key}' from '{backup_path}':\n{e}"

    ### Remove stale key files with a different filetype to avoid ambiguity
    ### (e.g. a leftover `stack.json` when restoring `stack.yaml`).
    for other in keyfile_path.parent.glob(f"{key}.*"):
        if other.resolve() != target_path.resolve():
            try:
                other.unlink()
            except Exception:
                pass

    if reload:
        reload_meerschaum(debug=debug)

    return True, f"Restored '{key}' from backup '{backup_path.name}'."
