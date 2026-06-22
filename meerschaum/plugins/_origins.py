#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Track the origin repository of installed plugins.

Each plugins directory keeps its own ``.mrsm_origins.json`` manifest mapping
``plugin_name -> repo_keys`` (e.g. ``{"compose": "api:mrsm"}``). The manifest is
colocated with the plugins it describes so it travels with the directory when it
is mounted into another environment (e.g. a Docker image). Plugins authored
locally (not installed from a repository) have no entry and are skipped by the
update checker.
"""

from __future__ import annotations

import os
import json
import pathlib

import meerschaum.config.paths as paths
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.typing import Dict, Optional, Union


def get_origins_filename() -> str:
    """Return the manifest filename used within each plugins directory."""
    return STATIC_CONFIG['plugins']['origins_filename']


def get_origins_path(plugins_dir_path: Union[pathlib.Path, str]) -> pathlib.Path:
    """Return the path to the origins manifest within a plugins directory."""
    return pathlib.Path(plugins_dir_path) / get_origins_filename()


def _read_origins_file(plugins_dir_path: Union[pathlib.Path, str]) -> Dict[str, str]:
    """Return the origins mapping for a single plugins directory (empty if absent)."""
    origins_path = get_origins_path(plugins_dir_path)
    if not origins_path.exists():
        return {}
    try:
        with open(origins_path, 'r', encoding='utf-8') as f:
            origins = json.load(f)
    except Exception:
        return {}
    if not isinstance(origins, dict):
        return {}
    return {
        str(name): str(repo_keys)
        for name, repo_keys in origins.items()
        if isinstance(repo_keys, str) and repo_keys
    }


def read_plugin_origins(debug: bool = False) -> Dict[str, str]:
    """
    Return a merged ``plugin_name -> repo_keys`` mapping across all plugins directories.

    Manifests are read from every directory in `paths.PLUGINS_DIR_PATHS`. Because a
    plugin lives in exactly one directory, the merge is effectively a union; if the
    same name appears in multiple directories, the first directory wins (matching
    the precedence used when resolving installed plugins).
    """
    origins: Dict[str, str] = {}
    for plugins_dir_path in paths.PLUGINS_DIR_PATHS:
        for name, repo_keys in _read_origins_file(plugins_dir_path).items():
            origins.setdefault(name, repo_keys)
    return origins


def write_plugin_origin(
    name: str,
    repo_keys: str,
    plugins_dir_path: Union[pathlib.Path, str],
    debug: bool = False,
) -> bool:
    """
    Record a plugin's origin repository in its directory's manifest.

    Parameters
    ----------
    name: str
        The plugin name (without any repo separator).

    repo_keys: str
        The connector keys of the origin repository (e.g. ``'api:mrsm'``).

    plugins_dir_path:
        The directory the plugin was installed into.

    Returns
    -------
    A `bool` indicating success.
    """
    if not repo_keys:
        return False
    origins = _read_origins_file(plugins_dir_path)
    if origins.get(name) == repo_keys:
        return True
    origins[name] = repo_keys
    return _write_origins_file(plugins_dir_path, origins)


def remove_plugin_origin(
    name: str,
    plugins_dir_path: Optional[Union[pathlib.Path, str]] = None,
    debug: bool = False,
) -> bool:
    """
    Remove a plugin's origin entry. If `plugins_dir_path` is `None`, remove from
    every directory's manifest that contains it.
    """
    plugins_dir_paths = (
        [plugins_dir_path]
        if plugins_dir_path is not None
        else list(paths.PLUGINS_DIR_PATHS)
    )
    success = True
    for path in plugins_dir_paths:
        origins = _read_origins_file(path)
        if name not in origins:
            continue
        del origins[name]
        success = _write_origins_file(path, origins) and success
    return success


def _write_origins_file(
    plugins_dir_path: Union[pathlib.Path, str],
    origins: Dict[str, str],
) -> bool:
    """Persist the origins mapping for a single plugins directory."""
    origins_path = get_origins_path(plugins_dir_path)
    try:
        if not origins:
            if origins_path.exists():
                origins_path.unlink()
            return True
        origins_path.parent.mkdir(parents=True, exist_ok=True)
        with open(origins_path, 'w+', encoding='utf-8') as f:
            json.dump(origins, f, indent=4, sort_keys=True)
    except Exception:
        return False
    return True
