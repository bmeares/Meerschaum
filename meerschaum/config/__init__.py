#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message.
"""

from __future__ import annotations
import os, shutil, sys, pathlib
from meerschaum.utils.typing import Any, Dict, Optional, Union

from meerschaum.config._version import __version__
from meerschaum.config._edit import write_config
from meerschaum.config.static import _static_config

from meerschaum.config._paths import (
    PERMANENT_PATCH_DIR_PATH,
    CONFIG_DIR_PATH,
    DEFAULT_CONFIG_DIR_PATH,
)
from meerschaum.config._patch import (
    permanent_patch_config,
    patch_config,
    apply_patch_to_config,
)

### apply config preprocessing (e.g. main to meta)
config = None
def _config(
        *keys : str, reload : bool = False, substitute : bool = True,
        sync_files : bool = True, write_missing : bool = True,
    ) -> Dict[str, Any]:
    """
    Read and process the configuration file.
    """
    global config
    if config is None or reload:
        from meerschaum.config._read_config import read_config
        from meerschaum.config._sync import sync_files as _sync_files
        config = read_config(keys=keys, substitute=substitute, write_missing=write_missing)
        if sync_files:
            _sync_files(keys=[keys[0] if keys else None])
    return config

def set_config(cf : Dict[str, Any]) -> Dict[str, Any]:
    """
    Set the configuration dictionary to a dictionary
    """
    global config
    if not isinstance(cf, dict):
        from meerschaum.utils.warnings import error
        error(f"Invalid value for config: {cf}")
    config = cf
    return config

def get_config(
        *keys : str,
        patch : bool = True,
        substitute : bool = True,
        sync_files : bool = True,
        write_missing : bool = True,
        as_tuple : bool = False,
        warn : bool = True,
        debug = False
    ) -> Any:
    """
    Return the Meerschaum configuration dictionary.
    If positional arguments are provided, index by the keys.
    Raises a warning if invalid keys are provided.

    :param keys:
        List of strings to index.

    :param patch:
        If True, patch missing default keys into the config directory.
        Defaults to True.

    :param sync_files:
        If True, sync files if needed.
        Defaults to True.

    :param write_missing:
        If True, write default values when the main config files are missing.
        Defaults to True.

    :param substitute:
        If True, subsitute 'MRSM{}' values.
        Defaults to True.

    :param as_tuple:
        If True, return a tuple of type (success, value).
        Defaults to False.

    E.g. get_config('meerschaum', 'connectors') == config['meerschaum']['connectors']
    """
    global config
    import json

    symlinks_key = _static_config()['config']['symlinks_key']
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"Indexing keys: {keys}")

    if len(keys) == 0:
        _rc = _config(substitute=substitute, sync_files=sync_files, write_missing=write_missing)
        if as_tuple:
            return True, _rc 
        return _rc

    from meerschaum.config._read_config import read_config, search_and_substitute_config
    ### Invalidate the cache if it was read before with substitute=False
    ### but there still exist substitutions.
    if (
        config is not None and substitute and keys[0] != symlinks_key
        and 'MRSM{' in json.dumps(config.get(keys[0]))
    ):
        _subbed = search_and_substitute_config({keys[0] : config[keys[0]]})
        config[keys[0]] = _subbed[keys[0]]
        if symlinks_key in _subbed:
            if symlinks_key not in config:
                config[symlinks_key] = {}
            config[symlinks_key][keys[0]] = _subbed

    from meerschaum.config._sync import sync_files as _sync_files
    if config is None:
        config = read_config(keys=[keys[0]], substitute=substitute, write_missing=write_missing)
        if sync_files:
            _sync_files(keys=[keys[0]])

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
                if substitute else default_config
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
                        _warn(warning_msg, stacklevel=3)
                except Exception as e:
                    if warn:
                        print(warning_msg)
                if as_tuple:
                    return False, None
                return None

            config = apply_patch_to_config(patched_default_config, config)
            if patch and keys[0] != symlinks_key:
                print("Updating configuration, please wait...")
                write_config(config, debug=debug)
    if as_tuple:
        return (not invalid_keys), c
    return c

def get_plugin_config(*keys : str, **kw : Any) -> Optional[Any]:
    """
    Return the configuration for the calling plugin.
    This function is only mean to be used from within a plugin.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.actions import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error(f"You may only call `get_plugin_config()` from within a Meerschaum plugin.")
    return get_config(*(['plugins', parent_plugin_name] + list(keys)), **kw)

def write_plugin_config(
        config_dict : Dict[str, Any],
        **kw : Any
    ):
    """
    Write a plugin's configuration dictionary.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.actions import _get_parent_plugin
    parent_plugin_name = _get_parent_plugin(2)
    if parent_plugin_name is None:
        error(f"You may only call `get_plugin_config()` from within a Meerschaum plugin.")
    plugins_cf = get_config('plugins', warn=False)
    if plugins_cf is None:
        plugins_cf = {}
    plugins_cf.update({parent_plugin_name : config_dict})
    cf = {'plugins' : plugins_cf}
    return write_config(cf, **kw)


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

### If environment variable MRSM_CONFIG is set, patch config before anything else.
environment_config = _static_config()['environment']['config']
environment_patch = _static_config()['environment']['patch']

def _apply_environment_config(env_var):
    if env_var in os.environ:
        from meerschaum.utils.misc import string_to_dict
        try:
            _patch = string_to_dict(str(os.environ[env_var]))
        except Exception as e:
            _patch = None
        error_msg = (
            f"Environment variable {env_var} is set but cannot be parsed.\n"
            f"Unset {env_var} or change to JSON or simplified dictionary format " +
            "(see --help, under params for formatting)\n" +
            f"{env_var} is set to:\n{os.environ[env_var]}\n"
            f"Skipping patching os environment into config..."
        )

        if not isinstance(_patch, dict):
            print(error_msg)
        else:
            ### Load and patch config files.
            for k in _patch:
                try:
                    _valid, _key_config = get_config(
                        k, write_missing=False, as_tuple=True, warn=False
                    )
                    to_set = (
                        apply_patch_to_config({k: _key_config}, _patch) if _valid
                        else _patch
                    )
                    set_config(to_set)
                except Exception as e:
                    print(e)
                    print(error_msg)
_apply_environment_config(environment_config)
_apply_environment_config(environment_patch)

environment_root_dir = _static_config()['environment']['root']
if environment_root_dir in os.environ:
    from meerschaum.config._paths import set_root
    root_dir_path = pathlib.Path(os.environ[environment_root_dir]).absolute()
    if not root_dir_path.exists():
        print(
            f"Invalid root directory '{str(root_dir_path)}' set for " +
            f"environment variable '{environment_root_dir}'.\n" +
            f"Please enter a valid path for {environment_root_dir}."
        )
        sys.exit(1)
    set_root(root_dir_path)


#  environment_runtime = _static_config()['environment']['runtime']
#  if environment_runtime in os.environ:
    #  if os.environ[environment_runtime] == 'portable':
        #  import platform
        #  from meerschaum.config._paths import PORTABLE_CHECK_READLINE_PATH
        #  from meerschaum.utils.packages import attempt_import, pip_install
        #  if not PORTABLE_CHECK_READLINE_PATH.exists():
            #  rl_name = "gnureadline" if platform.system() != 'Windows' else "pyreadline"
            #  try:
                #  rl = attempt_import(
                    #  rl_name,
                    #  lazy = False,
                    #  install = True,
                    #  venv = None,
                #  )
            #  except ImportError:
                #  if not pip_install(rl_name, args=['--upgrade', '--ignore-installed'], venv=None):
                    #  print(f"Unable to import {rl_name}!")
            #  PORTABLE_CHECK_READLINE_PATH.touch()


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

