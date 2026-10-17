#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Record which plugin (or `meerschaum` itself) requested each package in a virtual environment
(design D20 / §12.1).

The ledger lives at `venvs/<name>/.mrsm_requests.json` and is read by the native
`mrsm show venvs` and `mrsm prune venvs` actions (`crates/meerschaum-cli`).

Schema (version 1)::

    {
      "schema_version": 1,
      "packages": {
        "<normalized install name>": {               # PEP 503: lowercase, runs of [-_.] -> '-'
          "requesters": {
            "<plugin name | 'meerschaum'>": {
              "spec": "pandas>=2.0",                  # the install spec that was requested
              "first_requested": "2026-10-13T12:00:00+00:00",
              "last_requested": "2026-10-13T12:00:00+00:00"
            }
          }
        }
      }
    }

Writes are atomic (temporary file + rename) and serialized across processes with an advisory
lock file (`.mrsm_requests.lock`) where the platform supports it.
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import re
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple

REQUESTS_FILE_NAME = '.mrsm_requests.json'
REQUESTS_LOCK_NAME = '.mrsm_requests.lock'
REQUESTS_SCHEMA_VERSION = 1
CORE_REQUESTER = 'meerschaum'

### `(ledger path, normalized name, requester)` triples already written by this process.
_recorded: Set[Tuple[str, str, str]] = set()


def normalize_package_name(install_name: str) -> str:
    """
    Return the PEP 503 normalized distribution name of an install spec
    (version specifiers and extras are dropped).
    """
    base = re.split(r'[\[=<>,!~; @]', install_name.strip(), maxsplit=1)[0]
    return re.sub(r'[-_.]+', '-', base).lower()


def get_requests_path(venv: str) -> pathlib.Path:
    """
    Return the path to the requests ledger of the virtual environment `venv`.
    """
    import meerschaum.config.paths as paths
    return pathlib.Path(paths.VIRTENV_RESOURCES_PATH) / venv / REQUESTS_FILE_NAME


def read_requests(venv: str) -> Dict[str, Any]:
    """
    Return the ledger for `venv` (an empty one if it is missing or unreadable).
    """
    return _read(get_requests_path(venv))


def _read(path: pathlib.Path) -> Dict[str, Any]:
    empty = {'schema_version': REQUESTS_SCHEMA_VERSION, 'packages': {}}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return empty
    if not isinstance(data, dict) or not isinstance(data.get('packages', None), dict):
        return empty
    return data


@contextlib.contextmanager
def _file_lock(path: pathlib.Path):
    lock_file = None
    try:
        try:
            import fcntl
            lock_file = open(path.parent / REQUESTS_LOCK_NAME, 'a+')
            fcntl.flock(lock_file, fcntl.LOCK_EX)
        except (ImportError, OSError):
            lock_file = None
        yield
    finally:
        if lock_file is not None:
            try:
                import fcntl
                fcntl.flock(lock_file, fcntl.LOCK_UN)
            except Exception:
                pass
            lock_file.close()


def _write_atomic(path: pathlib.Path, data: Dict[str, Any]) -> None:
    fd, temp_name = tempfile.mkstemp(dir=str(path.parent), prefix='.mrsm_requests.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write('\n')
        os.replace(temp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temp_name)
        raise


def record_request(
    venv: Optional[str],
    install_name: str,
    requester: str = CORE_REQUESTER,
    force: bool = False,
) -> bool:
    """
    Record that `requester` requested `install_name` in the virtual environment `venv`.
    Return `True` if the ledger was written.
    Failures (read-only root, missing directory) are swallowed: accounting must never
    break an import.

    Parameters
    ----------
    venv: Optional[str]
        The virtual environment. `None` (the system environment) is not tracked.

    install_name: str
        The install spec (e.g. `'pandas>=2.0'`).

    requester: str, default 'meerschaum'
        The plugin name, or `'meerschaum'` for core and extra installs.

    force: bool, default False
        If `True`, update `last_requested` even if this process already recorded the request.
    """
    if venv is None or not install_name:
        return False
    name = normalize_package_name(install_name)
    if not name:
        return False
    try:
        path = get_requests_path(venv)
        key = (str(path), name, requester)
        if key in _recorded and not force:
            return False
        if not path.parent.exists():
            ### Never create a venv directory just for accounting.
            return False
        now = datetime.now(timezone.utc).isoformat(timespec='seconds')
        with _file_lock(path):
            data = _read(path)
            packages = data.setdefault('packages', {})
            entry = packages.setdefault(name, {})
            requesters = entry.setdefault('requesters', {})
            existing = requesters.get(requester, {})
            requesters[requester] = {
                'spec': install_name,
                'first_requested': existing.get('first_requested', now),
                'last_requested': now,
            }
            data['schema_version'] = REQUESTS_SCHEMA_VERSION
            _write_atomic(path, data)
        _recorded.add(key)
        return True
    except Exception:
        return False


def get_requester(skip_prefixes: Tuple[str, ...] = ('meerschaum.utils.packages',)) -> str:
    """
    Return the plugin that is calling into `attempt_import`, or `'meerschaum'`.

    The requester is the module of the nearest caller outside of `skip_prefixes`:
    a v4-style plugin module is named `plugins.<name>[...]`. A plugin that reaches
    `attempt_import` through Meerschaum internals is attributed to `meerschaum`.
    """
    try:
        frame = sys._getframe(1)
    except Exception:
        return CORE_REQUESTER
    while frame is not None:
        module_name = frame.f_globals.get('__name__', '') or ''
        if module_name.startswith(skip_prefixes) or module_name.startswith('importlib'):
            frame = frame.f_back
            continue
        if module_name.startswith('plugins.'):
            plugin_name = module_name.split('.')[1]
            if plugin_name and not plugin_name.startswith('_'):
                return plugin_name
        return CORE_REQUESTER
    return CORE_REQUESTER


def forget_requests(venv: Optional[str], install_names: Any) -> bool:
    """
    Drop the ledger entries of the given packages (called after an uninstall).
    Return `True` if the ledger was rewritten.
    """
    if venv is None:
        return False
    try:
        path = get_requests_path(venv)
        if not path.exists():
            return False
        names = {normalize_package_name(name) for name in install_names}
        with _file_lock(path):
            data = _read(path)
            packages = data.get('packages', {})
            removed = [name for name in list(packages) if name in names]
            if not removed:
                return False
            for name in removed:
                del packages[name]
            _write_atomic(path, data)
        for key in [k for k in _recorded if k[0] == str(path) and k[1] in names]:
            _recorded.discard(key)
        return True
    except Exception:
        return False
