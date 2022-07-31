#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Patch the runtime configuration from environment variables.
"""

import os
import re
from meerschaum.utils.typing import List, Union, Dict, Any
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
    from meerschaum.config import get_config, set_config, _config
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

    valids = []

    from meerschaum.utils.pool import get_pool_executor
    executor = get_pool_executor()

    def load_key(key: str) -> Union[Dict[str, Any], None]:
        try:
            c = get_config(key, warn=False)
        except Exception as e:
            c = None
        return c

    keys = list(_patch.keys())
    with executor:
        keys_configs = list(executor.map(load_key, keys))

    ### Load and patch config files.
    set_config(
        apply_patch_to_config(
            _config(),
            _patch,
        )
    )


def apply_environment_uris() -> None:
    """
    Patch temporary connectors defined in environment variables which start with
    `MRSM_SQL_` or `MRSM_API_`.
    """
    for env_var in get_connector_env_vars():
        apply_connector_uri(env_var)


def get_connector_env_vars() -> List[str]:
    """
    Get the names of the environment variables which match the Meerschaum connector regex.

    Examples
    --------
    >>> get_connector_environment_vars()
    ['MRSM_SQL_FOO']
    """
    uri_regex = _static_config()['environment']['uri_regex']
    env_vars = []
    for env_var in os.environ:
        matched = re.match(uri_regex, env_var)
        if matched is None:
            continue
        env_vars.append(env_var)
    return env_vars


def apply_connector_uri(env_var: str) -> None:
    """
    Parse and validate a URI obtained from an environment variable.
    """
    from meerschaum.connectors import get_connector
    from meerschaum.config import get_config, set_config, _config
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
    try:
        uri = conn.DATABASE_URL if typ == 'sql' else os.environ[env_var]
    except Exception as e:
        print(msg)

    _config()['meerschaum']['connectors'][typ][label] = {'uri': uri}
    #  m_config = get_config('meerschaum')
    #  m_config['connectors'][typ][label] = {'uri': uri}
    #  set_config(
        #  apply_patch_to_config(
            #  _config(),
            #  {'meerschaum': m_config},
        #  ),
    #  )


def get_env_vars() -> List[str]:
    """
    Return all environment variables which begin with `'MRSM_'`.
    """
    prefix = _static_config()['environment']['prefix']
    return sorted([env_var for env_var in os.environ if env_var.startswith(prefix)])
