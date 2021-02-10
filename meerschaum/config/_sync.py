#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize across config files
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List

def sync_yaml_configs(
        config_path : pathlib.Path,
        keys : List[str],
        sub_path : pathlib.Path,
        substitute : bool = True,
    ) -> None:
    """
    Synchronize sub-configuration with main configuration file.

    NOTE: This function might need refactoring to work better with the new
    `read_config` system.
    """
    import os, sys
    try:
        from meerschaum.utils.yaml import yaml, _yaml
    except:
        return
    from meerschaum.config._patch import apply_patch_to_config
    import meerschaum.config
    from meerschaum.utils.packages import reload_package
    from meerschaum.config._read_config import search_and_substitute_config
    if not os.path.isfile(config_path) or not os.path.isfile(sub_path):
        return

    def _read_config(path):
        """
        Read YAML file with header comment
        """
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

    config_header, config = _read_config(config_path)
    sub_header, sub_config = _read_config(sub_path)

    from meerschaum.config import get_config
    c = get_config(*keys, substitute=substitute)
    if substitute:
        sub_config = search_and_substitute_config(sub_config)

    if c != sub_config:
        config_time = os.path.getmtime(config_path)
        sub_time = os.path.getmtime(sub_path)

        sub_config = c
        new_config_text = yaml.dump(c, sort_keys=False)
        new_header = sub_header
        new_path = sub_path

        ### write changes
        with open(new_path, 'w+') as f:
            f.write(new_header)
            f.write(new_config_text)

        try:
            reload_package('meerschaum')
        except:
            print("Restart Meerschaum to reload config.")
            import sys
            sys.exit(1)

