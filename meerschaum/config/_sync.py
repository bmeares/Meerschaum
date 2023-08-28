#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize across config files
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List, Tuple

def sync_yaml_configs(
        keys: List[str],
        sub_path: pathlib.Path,
        substitute: bool = True,
        permissions: Optional[int] = None,
        replace_tuples: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
    """Synchronize sub-configuration with main configuration file.
    
    Parameters
    ----------
    keys: List[str]
        The config keys to read via `get_config()`.

    sub_path: pathlib.Path
        The derivative file to write.

    substitute: bool, default True
        If `True`, parse `MRSM{}` syntax and substitute values.
        See `get_config()` for more information.

    permissions: Optional[int], default None
        If not `None`, set permissions of the derivative file.

    replace_tuples: Optional[List[Tuple[str]]], default None
        If provided, iterate through a list of tuples,
        replacing the old string (index 0) with the new string (index 1).
    """
    import os, sys
    try:
        from meerschaum.utils.yaml import yaml, _yaml
    except Exception as e:
        return
    from meerschaum.config._patch import apply_patch_to_config
    import meerschaum.config
    from meerschaum.utils.packages import reload_package
    from meerschaum.config._read_config import search_and_substitute_config, get_keyfile_path

    def _read_yaml_config(path):
        """Read YAML file with header comment."""
        if not path.exists():
            return "", {}
        header_comment = ""
        with open(path, 'r') as f:
            if _yaml is not None:
                config = yaml.load(f)
            else:
                print("PyYAML not installed!")
                sys.exit(1)
            f.seek(0)
            for line in f:
                if not line.startswith('#') and not line == '\n':
                    break
                header_comment += line
        return header_comment, config

    from meerschaum.config import get_config
    config_path = get_keyfile_path(keys[0], create_new=True)
    c = get_config(*keys, substitute=substitute, sync_files=False)

    config_header, config = _read_yaml_config(config_path)
    sub_header, sub_config = _read_yaml_config(sub_path)

    if substitute:
        sub_config = search_and_substitute_config(sub_config)

    sub_config = c
    new_config_text = yaml.dump(c, sort_keys=False)
    if replace_tuples:
        for replace_tuple in replace_tuples:
            new_config_text = new_config_text.replace(replace_tuple[0], replace_tuple[1])
    new_header = sub_header
    new_path = sub_path

    ### write changes
    with open(new_path, 'w+') as f:
        f.write(new_header)
        f.write(new_config_text)
    if permissions is not None:
        os.chmod(new_path, permissions)

def sync_files(keys: Optional[List[str]] = None):
    if keys is None:
        keys = []

    def _stack():
        import os
        from meerschaum.config._paths import (
            CONFIG_DIR_PATH,
            STACK_ENV_PATH,
            STACK_COMPOSE_PATH,
            STACK_COMPOSE_FILENAME,
            STACK_ENV_FILENAME,
            GRAFANA_DATASOURCE_PATH,
            GRAFANA_DASHBOARD_PATH,
        )
        from meerschaum.config.static import STATIC_CONFIG

        sync_yaml_configs(
            ['stack', STACK_COMPOSE_FILENAME],
            STACK_COMPOSE_PATH,
            substitute = True,
            replace_tuples = [('$', '$$'), ('<DOLLAR>', '$')],
        )
        sync_yaml_configs(
            ['stack', 'grafana', 'datasource'],
            GRAFANA_DATASOURCE_PATH,
            substitute = True,
        )
        sync_yaml_configs(
            ['stack', 'grafana', 'dashboard'],
            GRAFANA_DASHBOARD_PATH,
            substitute = True,
        )


    key_functions = {
        'stack': _stack,
    }
    if keys is None:
        keys = list(key_functions.keys())
    for k in keys:
        if k in key_functions:
            key_functions[k]()

