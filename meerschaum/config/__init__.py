#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message.
"""

from __future__ import annotations

import os
import shutil
import sys
import copy
import contextlib

import meerschaum.config.paths as paths
import types
from meerschaum.utils.typing import Any, Dict, Optional, Union
from meerschaum.utils.threading import RLock

from meerschaum.config._version import __version__
from meerschaum.config._store import get_store, set_store, reset_store
from meerschaum.config._edit import edit_config, write_config
from meerschaum.config._read_config import (
    search_and_substitute_config,
    revert_symlinks_config,
    get_possible_keys,
    get_keyfile_path,
    read_config,
)
from meerschaum._internal.static import STATIC_CONFIG

from meerschaum.config._patch import (
    apply_patch_to_config,
)
from meerschaum import _core

__all__ = (
    'get_plugin_config',
    'write_plugin_config',
    'get_config',
    'write_config',
    'edit_config',
    'set_config',
    'replace_config',
    'search_and_substitute_config',
    'revert_symlinks_config',
    'get_possible_keys',
    'get_keyfile_path',
    'apply_patch_to_config',
    'read_config',
    'environment',
    'paths',
    'STATIC_CONFIG',
)
__pdoc__ = {'static': False, 'resources': False, 'stack': False, }
_locks = {'config': RLock()}

### The live config dictionary is `meerschaum.config.config` (see `_store.py`): the config registry
### owns the object, so it is resolved on access (`from meerschaum.config import config` gives the
### current dictionary, as v4's module global did), and reloading this module starts it over.
reset_store()
_backup_config = None
_allow_write_missing: bool = True


def _config(
    *keys: str,
    reload: bool = False,
    substitute: bool = True,
    sync_files: bool = True,
    allow_replaced: bool = True,
    write_missing: bool = True,
) -> Dict[str, Any]:
    """
    Read and process the configuration file.
    """
    if reload:
        with _locks['config']:
            reset_store()

    config = get_store()
    if keys and keys[0] not in config:
        from meerschaum.config._sync import sync_files as _sync_files
        key_config = read_config(
            keys = [keys[0]],
            substitute = substitute,
            write_missing = write_missing and _allow_write_missing,
        )
        if keys[0] in key_config:
            config[keys[0]] = key_config[keys[0]]
            if sync_files and _allow_write_missing:
                _sync_files(keys=[keys[0] if keys else None])

    if not allow_replaced:
        return _backup_config if _backup_config is not None else config

    return config


def set_config(cf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Set the configuration dictionary.
    """
    if not isinstance(cf, dict):
        from meerschaum.utils.warnings import error
        error(f"Invalid value for config: {cf}")
    with _locks['config']:
        set_store(cf)
    return cf


def get_config(
    *keys: str,
    patch: bool = True,
    substitute: bool = True,
    sync_files: bool = True,
    write_missing: bool = True,
    as_tuple: bool = False,
    warn: bool = True,
    debug: bool = False
) -> Any:
    """
    Return the Meerschaum configuration dictionary.
    If positional arguments are provided, index by the keys.
    Raises a warning if invalid keys are provided.

    Parameters
    ----------
    keys: str:
        List of strings to index.

    patch: bool, default True
        If `True`, patch missing default keys into the config directory.
        Defaults to `True`.

    sync_files: bool, default True
        If `True`, sync files if needed.
        Defaults to `True`.

    write_missing: bool, default True
        If `True`, write default values when the main config files are missing.
        Defaults to `True`.

    substitute: bool, default True
        If `True`, subsitute 'MRSM{}' values.
        Defaults to `True`.

    as_tuple: bool, default False
        If `True`, return a tuple of type (success, value).
        Defaults to `False`.
        
    Returns
    -------
    The value in the configuration directory, indexed by the provided keys.

    Examples
    --------
    >>> get_config('meerschaum', 'instance')
    'sql:main'
    >>> get_config('does', 'not', 'exist')
    UserWarning: Invalid keys in config: ('does', 'not', 'exist')
    """
    return _core.get_config(
        *keys, patch=patch, substitute=substitute, sync_files=sync_files,
        write_missing=write_missing, as_tuple=as_tuple, warn=warn, debug=debug,
    )


def get_plugin_config(
    *keys: str,
    warn: bool = False,
    **kw: Any
) -> Optional[Any]:
    """
    This may only be called from within a Meerschaum plugin.
    See `meerschaum.config.get_config` for arguments.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.plugins import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error(
            "You may only call `get_plugin_config()` "
            "from within a Meerschaum plugin."
        )

    return _core.get_plugin_config(parent_plugin_name, *keys, warn=warn, **kw)


def write_plugin_config(
    config_dict: Dict[str, Any],
    **kw: Any
):
    """
    Write a plugin's configuration dictionary.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.plugins import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error("You may only call `get_plugin_config()` from within a Meerschaum plugin.")
    return _core.write_plugin_config(parent_plugin_name, config_dict, **kw)


@contextlib.contextmanager
def replace_config(config_: Union[Dict[str, Any], None]):
    """
    Temporarily override the Meerschaum config dictionary.

    Parameters
    ----------
    config_: Dict[str, Any]
        The new config dictionary to temporarily replace the canonical `config`.
    """
    if config_ is None:
        yield
        return

    global _backup_config, _allow_write_missing

    previous_config = _config()
    previous_backup_config = _backup_config
    previous_allow_write_missing = _allow_write_missing
    _backup_config = (
        previous_backup_config
        if previous_backup_config is not None
        else previous_config
    )
    _allow_write_missing = False
    set_config(config_)

    try:
        yield
    finally:
        set_config(previous_config)
        _backup_config = previous_backup_config
        _allow_write_missing = previous_allow_write_missing

### This need to be below get_config to avoid a circular import.
from meerschaum.config._read_config import read_config

### If environment variable MRSM_CONFIG or MRSM_PATCH is set, patch config before anything else.
from meerschaum.config.environment import (
    apply_environment_patches as _apply_environment_patches,
    apply_environment_uris as _apply_environment_uris,
)
_apply_environment_uris()
_apply_environment_patches()

patch_config = None
if paths.PATCH_DIR_PATH.exists():
    from meerschaum.utils.yaml import yaml, _yaml
    if _yaml is not None:
        patch_config = read_config(directory=paths.PATCH_DIR_PATH)

permanent_patch_config = None
if paths.PERMANENT_PATCH_DIR_PATH.exists():
    from meerschaum.utils.yaml import yaml, _yaml
    if _yaml is not None:
        permanent_patch_config = read_config(directory=paths.PERMANENT_PATCH_DIR_PATH)
### If patches exist, apply to config.
if patch_config is not None:
    set_config(apply_patch_to_config(_config(), patch_config))
    if paths.PATCH_DIR_PATH.exists():
        shutil.rmtree(paths.PATCH_DIR_PATH)

### if permanent_patch.yaml exists, apply patch to config, write config, and delete patch
if permanent_patch_config is not None and paths.PERMANENT_PATCH_DIR_PATH.exists():
    print(
        "Found permanent patch configuration. " +
        "Updating main config and deleting permanent patch..."
    )
    set_config(apply_patch_to_config(_config(), permanent_patch_config))
    write_config(_config())
    permanent_patch_config = None
    if paths.PERMANENT_PATCH_DIR_PATH.exists():
        shutil.rmtree(paths.PERMANENT_PATCH_DIR_PATH)
    if paths.DEFAULT_CONFIG_DIR_PATH.exists():
        shutil.rmtree(paths.DEFAULT_CONFIG_DIR_PATH)


### Make sure readline is available for the portable version.
environment_runtime = STATIC_CONFIG['environment']['runtime']
if environment_runtime in os.environ:
    if os.environ[environment_runtime] == 'portable':
        from meerschaum.utils.packages import ensure_readline
        if not paths.PORTABLE_CHECK_READLINE_PATH.exists():
            ensure_readline()
            paths.PORTABLE_CHECK_READLINE_PATH.touch()


### If interactive REPL, print welcome header.
__doc__ = f"Meerschaum v{__version__}"
try:
    interactive = False
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False
if interactive:
    msg = __doc__
    print(msg, file=sys.stderr)


class _ConfigModule(types.ModuleType):
    """
    The `meerschaum.config` module type: `config` is the live dictionary of the registry, so
    `meerschaum.config.config = {...}` replaces it (as `set_config()` does) instead of creating
    a second attribute that `get_config()` never sees.
    """

    @property
    def config(self) -> Dict[str, Any]:
        return get_store()

    @config.setter
    def config(self, cf: Dict[str, Any]) -> None:
        set_config(cf)


sys.modules[__name__].__class__ = _ConfigModule
