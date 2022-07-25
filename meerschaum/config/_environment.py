#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Patch the runtime configuration from environment variables.
"""

import os
import re
from meerschaum.config.static import _static_config

def apply_environment_patches() -> None:
    """
    Apply patches defined in `MRSM_CONFIG` and `MRSM_PATCH`.
    """
    config_var = _static_config()['environment']['config']
    patch_var = _static_config()['environment']['patch']
    apply_environment_config(config_var)
    apply_environment_config(patch_var)


def apply_environment_config(env_var: str) -> None:
    """
    Parse a dictionary (simple or JSON) from an environment variable
    and apply it to the current configuration.
    """
    from meerschaum.config import get_config, set_config
    from meerschaum.config._patch import apply_patch_to_config
    if env_var not in os.environ:
        return
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
        return
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


def apply_environment_uris() -> None:
    """
    Patch temporary connectors defined in environment variables which start with
    `MRSM_SQL_` or `MRSM_API_`.
    """
    uri_regex = _static_config()['environment']['uri_regex']
    env_vars = []
    for env_var in os.environ:
        matched = re.match(uri_regex, env_var)
        if matched is None:
            continue
        env_vars.append(env_var)
    for env_var in env_vars:
        apply_connector_uri(env_var)


def apply_connector_uri(env_var: str) -> None:
    """
    Parse and validate a URI obtained from an environment variable.
    """
    from meerschaum.connectors import get_connector
    from meerschaum.config import get_config, set_config
    from meerschaum.config._patch import apply_patch_to_config
    uri_regex = _static_config()['environment']['uri_regex']
    matched = re.match(uri_regex, env_var)
    groups = matched.groups()
    typ, label, uri = groups[0].lower(), groups[1].lower(), os.environ[env_var]
    msg = f"An invalid URI was set for environment variable '{env_var}'."
    try:
        conn = get_connector(typ, label, uri=os.environ[env_var])
    except Exception as e:
        print(msg)
        return
    m_config = get_config('meerschaum')
    try:
        uri = conn.DATABASE_URL if typ == 'sql' else os.environ[env_var]
    except Exception as e:
        print(msg)
    set_config(apply_patch_to_config(
        {'meerschaum': m_config},
        {'meerschaum': {'connectors': {typ: {label: {'uri': uri}}}}}, 
    ))

