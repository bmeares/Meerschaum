#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message.
"""

from __future__ import annotations

import os, shutil, sys, pathlib, copy
from meerschaum.utils.typing import Any, Dict, Optional, Union
from meerschaum.utils.threading import RLock
from meerschaum.utils.warnings import warn

from meerschaum.config._version import __version__
from meerschaum.config._edit import edit_config, write_config
from meerschaum.config.static import STATIC_CONFIG

from meerschaum.config._paths import (
    PERMANENT_PATCH_DIR_PATH,
    CONFIG_DIR_PATH,
    DEFAULT_CONFIG_DIR_PATH,
)
from meerschaum.config._patch import (
    #  permanent_patch_config,
    #  patch_config,
    apply_patch_to_config,
)
__all__ = ('get_plugin_config', 'write_plugin_config', 'get_config', 'write_config', 'set_config',)
__pdoc__ = {'static': False, 'resources': False, 'stack': False, }
_locks = {'config': RLock()}

### apply config preprocessing (e.g. main to meta)
config = {}
def _config(
        *keys: str, reload: bool = False, substitute: bool = True,
        sync_files: bool = True, write_missing: bool = True,
    ) -> Dict[str, Any]:
    """
    Read and process the configuration file.
    """
    global config
    if config is None or reload:
        with _locks['config']:
            config = {}
    if keys and keys[0] not in config:
        from meerschaum.config._sync import sync_files as _sync_files
        key_config = read_config(
            keys=[keys[0]],
            substitute=substitute,
            write_missing=write_missing,
        )
        if keys[0] in key_config:
            config[keys[0]] = key_config[keys[0]]
            if sync_files:
                _sync_files(keys=[keys[0] if keys else None])
    return config


def set_config(cf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Set the configuration dictionary.
    """
    global config
    if not isinstance(cf, dict):
        from meerschaum.utils.warnings import error
        error(f"Invalid value for config: {cf}")
    with _locks['config']:
        config = cf
    return config


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
    import json

    symlinks_key = STATIC_CONFIG['config']['symlinks_key']
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"Indexing keys: {keys}", color=False)

    if len(keys) == 0:
        _rc = _config(substitute=substitute, sync_files=sync_files, write_missing=write_missing)
        if as_tuple:
            return True, _rc 
        return _rc
    
    ### Weird threading issues, only import if substitute is True.
    if substitute:
        from meerschaum.config._read_config import search_and_substitute_config
    ### Invalidate the cache if it was read before with substitute=False
    ### but there still exist substitutions.
    if (
        config is not None and substitute and keys[0] != symlinks_key
        and 'MRSM{' in json.dumps(config.get(keys[0]))
    ):
        try:
            _subbed = search_and_substitute_config({keys[0]: config[keys[0]]})
        except Exception as e:
            import traceback
            traceback.print_exc()
        config[keys[0]] = _subbed[keys[0]]
        if symlinks_key in _subbed:
            if symlinks_key not in config:
                config[symlinks_key] = {}
            if keys[0] not in config[symlinks_key]:
                config[symlinks_key][keys[0]] = {}
            config[symlinks_key][keys[0]] = apply_patch_to_config(
                _subbed,
                config[symlinks_key][keys[0]]
            )

    from meerschaum.config._sync import sync_files as _sync_files
    if config is None:
        _config(*keys, sync_files=sync_files)

    invalid_keys = False
    if keys[0] not in config and keys[0] != symlinks_key:
        single_key_config = read_config(
            keys=[keys[0]], substitute=substitute, write_missing=write_missing
        )
        if keys[0] not in single_key_config:
            invalid_keys = True
        else:
            config[keys[0]] = single_key_config.get(keys[0], None)
            if symlinks_key in single_key_config and keys[0] in single_key_config[symlinks_key]:
                if symlinks_key not in config:
                    config[symlinks_key] = {}
                config[symlinks_key][keys[0]] = single_key_config[symlinks_key][keys[0]]

            if sync_files:
                _sync_files(keys=[keys[0]])

    c = config
    if len(keys) > 0:
        for k in keys:
            try:
                c = c[k]
            except Exception as e:
                invalid_keys = True
                break
        if invalid_keys:
            ### Check if the keys are in the default configuration.
            from meerschaum.config._default import default_config
            in_default = True
            patched_default_config = (
                search_and_substitute_config(default_config)
                if substitute else copy.deepcopy(default_config)
            )
            _c = patched_default_config
            for k in keys:
                try:
                    _c = _c[k]
                except Exception as e:
                    in_default = False
            if in_default:
                c = _c
                invalid_keys = False
            warning_msg = f"Invalid keys in config: {keys}"
            if not in_default:
                try:
                    if warn:
                        from meerschaum.utils.warnings import warn as _warn
                        _warn(warning_msg, stacklevel=3, color=False)
                except Exception as e:
                    if warn:
                        print(warning_msg)
                if as_tuple:
                    return False, None
                return None

            ### Don't write keys that we haven't yet loaded into memory.
            not_loaded_keys = [k for k in patched_default_config if k not in config]
            for k in not_loaded_keys:
                patched_default_config.pop(k, None)

            set_config(
                apply_patch_to_config(
                    patched_default_config,
                    config,
                )
            )
            if patch and keys[0] != symlinks_key:
                #  print("Updating configuration, please wait...")
                if write_missing:
                    write_config(config, debug=debug)

    if as_tuple:
        return (not invalid_keys), c
    return c


def get_plugin_config(*keys : str, **kw : Any) -> Optional[Any]:
    """
    This may only be called from within a Meerschaum plugin.
    See `meerschaum.config.get_config` for arguments.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.plugins import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error(f"You may only call `get_plugin_config()` from within a Meerschaum plugin.")
    return get_config(*(['plugins', parent_plugin_name] + list(keys)), **kw)


def write_plugin_config(
        config_dict: Dict[str, Any],
        **kw : Any
    ):
    """
    Write a plugin's configuration dictionary.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.plugins import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error(f"You may only call `get_plugin_config()` from within a Meerschaum plugin.")
    plugins_cf = get_config('plugins', warn=False)
    if plugins_cf is None:
        plugins_cf = {}
    plugins_cf.update({parent_plugin_name: config_dict})
    cf = {'plugins' : plugins_cf}
    return write_config(cf, **kw)


### This need to be below get_config to avoid a circular import.
from meerschaum.config._read_config import read_config

### If environment variable MRSM_CONFIG or MRSM_PATCH is set, patch config before anything else.
from meerschaum.config._environment import apply_environment_patches, apply_environment_uris
apply_environment_uris()
apply_environment_patches()


from meerschaum.config._paths import PATCH_DIR_PATH, PERMANENT_PATCH_DIR_PATH
patch_config = None
if PATCH_DIR_PATH.exists():
    from meerschaum.utils.yaml import yaml, _yaml
    if _yaml is not None:
        patch_config = read_config(directory=PATCH_DIR_PATH)

permanent_patch_config = None
if PERMANENT_PATCH_DIR_PATH.exists():
    from meerschaum.utils.yaml import yaml, _yaml
    if _yaml is not None:
        permanent_patch_config = read_config(directory=PERMANENT_PATCH_DIR_PATH)
### If patches exist, apply to config.
if patch_config is not None:
    from meerschaum.config._paths import PATCH_DIR_PATH
    set_config(apply_patch_to_config(_config(), patch_config))
    if PATCH_DIR_PATH.exists():
        shutil.rmtree(PATCH_DIR_PATH)

### if permanent_patch.yaml exists, apply patch to config, write config, and delete patch
if permanent_patch_config is not None and PERMANENT_PATCH_DIR_PATH.exists():
    print(
        "Found permanent patch configuration. " +
        "Updating main config and deleting permanent patch..."
    )
    set_config(apply_patch_to_config(_config(), permanent_patch_config))
    write_config(_config())
    permanent_patch_config = None
    if PERMANENT_PATCH_DIR_PATH.exists():
        shutil.rmtree(PERMANENT_PATCH_DIR_PATH)
    if DEFAULT_CONFIG_DIR_PATH.exists():
        shutil.rmtree(DEFAULT_CONFIG_DIR_PATH)


### Make sure readline is available for the portable version.
environment_runtime = STATIC_CONFIG['environment']['runtime']
if environment_runtime in os.environ:
    if os.environ[environment_runtime] == 'portable':
        from meerschaum.utils.packages import ensure_readline
        from meerschaum.config._paths import PORTABLE_CHECK_READLINE_PATH
        if not PORTABLE_CHECK_READLINE_PATH.exists():
            ensure_readline()
            PORTABLE_CHECK_READLINE_PATH.touch()


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
