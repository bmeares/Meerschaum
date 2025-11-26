#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Patch the runtime configuration from environment variables.
"""

import os
import re
import json
import contextlib
import copy
import pathlib

from meerschaum.utils.typing import List, Union, Dict, Any, Optional
from meerschaum._internal.static import STATIC_CONFIG


def apply_environment_patches(env: Optional[Dict[str, Any]] = None) -> None:
    """
    Apply patches defined in `MRSM_CONFIG` and `MRSM_PATCH`.
    """
    config_var = STATIC_CONFIG['environment']['config']
    patch_var = STATIC_CONFIG['environment']['patch']
    apply_environment_config(config_var, env=env)
    apply_environment_config(patch_var, env=env)


def apply_environment_config(env_var: str, env: Optional[Dict[str, Any]] = None) -> None:
    """
    Parse a dictionary (simple or JSON) from an environment variable
    and apply it to the current configuration.
    """
    from meerschaum.config import get_config, set_config, _config
    from meerschaum.config._patch import apply_patch_to_config

    env = env if env is not None else os.environ

    if env_var not in env:
        return

    from meerschaum.utils.misc import string_to_dict
    try:
        _patch = string_to_dict(str(os.environ[env_var]).lstrip())
    except Exception:
        _patch = None

    error_msg = (
        f"Environment variable {env_var} is set but cannot be parsed.\n"
        f"Unset {env_var} or change to JSON or simplified dictionary format "
        "(see --help, under params for formatting)\n"
        f"{env_var} is set to:\n{os.environ[env_var]}\n"
        f"Skipping patching os environment into config..."
    )

    if not isinstance(_patch, dict):
        print(error_msg)
        return

    valids = []

    def load_key(key: str) -> Union[Dict[str, Any], None]:
        try:
            c = get_config(key, warn=False)
        except Exception:
            c = None
        return c

    ### This was multi-threaded, but I ran into all sorts of locking issues.
    keys = list(_patch.keys())
    for key in keys:
        _ = load_key(key)

    ### Load and patch config files.
    set_config(
        apply_patch_to_config(
            _config(),
            _patch,
        )
    )


def apply_environment_uris(env: Optional[Dict[str, Any]] = None) -> None:
    """
    Patch temporary connectors defined in environment variables which start with
    `MRSM_SQL_` or `MRSM_API_`.
    """
    for env_var in get_connector_env_vars(env=env):
        apply_connector_uri(env_var, env=env)


def get_connector_env_regex() -> str:
    """
    Return the regex pattern for valid environment variable names for instance connectors.
    """
    return STATIC_CONFIG['environment']['uri_regex']


def get_connector_env_vars(env: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Get the names of the environment variables which match the Meerschaum connector regex.

    Examples
    --------
    >>> get_connector_environment_vars()
    ['MRSM_SQL_FOO']
    """
    uri_regex = get_connector_env_regex()
    env_vars = []

    env = env if env is not None else os.environ

    for env_var in env:
        matched = re.match(uri_regex, env_var)
        if matched is None:
            continue
        if env_var in STATIC_CONFIG['environment'].values():
            continue
        env_vars.append(env_var)

    return env_vars


def apply_connector_uri(env_var: str, env: Optional[Dict[str, Any]] = None) -> None:
    """
    Parse and validate a URI obtained from an environment variable.
    """
    from meerschaum.config import get_config, set_config, _config
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.config._read_config import search_and_substitute_config
    from meerschaum.utils.warnings import warn

    env = env if env is not None else os.environ

    if env_var not in env:
        return

    uri_regex = get_connector_env_regex()
    matched = re.match(uri_regex, env_var)
    groups = matched.groups()
    typ, label = groups[0].lower(), groups[1].lower()
    if not typ or not label:
        return

    uri = env[env_var]

    if uri.lstrip().startswith('{') and uri.rstrip().endswith('}'):
        try:
            conn_attrs = json.loads(uri)
        except Exception:
            warn(f"Unable to parse JSON for environment connector '{typ}:{label}'.")
            conn_attrs = {'uri': uri}
    else:
        conn_attrs = {'uri': uri}

    set_config(
        apply_patch_to_config(
            {'meerschaum': get_config('meerschaum')},
            {'meerschaum': {'connectors': {typ: {label: conn_attrs}}}},
        )
    )


def get_env_vars(env: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Return all environment variables which begin with `'MRSM_'`.
    """
    prefix = STATIC_CONFIG['environment']['prefix']
    env = env if env is not None else os.environ
    return {
        env_var: env_val
        for env_var, env_val in env.items()
        if env_var.startswith(prefix)
    }


def get_daemon_env_vars(env: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """
    Return the daemon-specific environment vars in the current environment.
    """
    env = env if env is not None else os.environ

    daemon_env_var_names = (
        STATIC_CONFIG['environment']['systemd_log_path'],
        STATIC_CONFIG['environment']['systemd_result_path'],
        STATIC_CONFIG['environment']['systemd_delete_job'],
        STATIC_CONFIG['environment']['systemd_stdin_path'],
        STATIC_CONFIG['environment']['daemon_id'],
    )
    return {
        env_var: env.get(env_var, '')
        for env_var in daemon_env_var_names
        if env_var in env
    }


@contextlib.contextmanager
def replace_env(env: Union[Dict[str, Any], None]):
    """
    Temporarily replace environment variables and current configuration.

    Parameters
    ----------
    env: Dict[str, Any]
        The new environment dictionary to be patched on `os.environ`.
    """
    if env is None:
        try:
            yield
        except Exception:
            pass
        return

    from meerschaum.config import _config, set_config
    from meerschaum.config.paths import (
        set_root,
        set_plugins_dir_paths,
        set_venvs_dir_path,
        set_config_dir_path,
        ROOT_DIR_PATH,
        PLUGINS_DIR_PATHS,
        VIRTENV_RESOURCES_PATH,
        CONFIG_DIR_PATH,
    )

    old_environ = dict(os.environ)
    old_config = copy.deepcopy(_config())
    old_root_dir_path = ROOT_DIR_PATH
    old_plugins_dir_paths = PLUGINS_DIR_PATHS
    old_venvs_dir_path = VIRTENV_RESOURCES_PATH
    old_config_dir_path = CONFIG_DIR_PATH

    os.environ.update(env)

    root_dir_env_var = STATIC_CONFIG['environment']['root']
    plugins_dir_env_var = STATIC_CONFIG['environment']['plugins']
    config_dir_env_var = STATIC_CONFIG['environment']['config_dir']
    venvs_dir_env_var = STATIC_CONFIG['environment']['venvs']

    replaced_root = False
    if root_dir_env_var in env:
        root_dir_path = pathlib.Path(env[root_dir_env_var])
        set_root(root_dir_path)
        replaced_root = True

    replaced_plugins = False
    if plugins_dir_env_var in env:
        plugins_dir_paths = env[plugins_dir_env_var]
        set_plugins_dir_paths(plugins_dir_paths)
        replaced_plugins = True

    replaced_venvs = False
    if venvs_dir_env_var in env:
        venv_dir_path = pathlib.Path(env[venvs_dir_env_var])
        set_venvs_dir_path(venv_dir_path)
        replaced_venvs = True

    replaced_config_dir = False
    if config_dir_env_var in env:
        config_dir_path = pathlib.Path(env[config_dir_env_var])
        set_config_dir_path(config_dir_path)
        replaced_config_dir = True

    apply_environment_patches(env)
    apply_environment_uris(env)

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

        if replaced_root:
            set_root(old_root_dir_path)

        if replaced_plugins:
            set_plugins_dir_paths(old_plugins_dir_paths)

        if replaced_venvs:
            set_venvs_dir_path(old_venvs_dir_path)

        if replaced_config_dir:
            set_config_dir_path(old_config_dir_path)

        _config().clear()
        set_config(old_config)
