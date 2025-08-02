#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Patch the runtime configuration from environment variables.
"""

import os
import re
import json

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


def get_env_vars(env: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Return all environment variables which begin with `'MRSM_'`.
    """
    prefix = STATIC_CONFIG['environment']['prefix']
    env = env if env is not None else os.environ
    return sorted([env_var for env_var in env if env_var.startswith(prefix)])
