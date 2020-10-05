#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize across config files
"""

def sync_configs(
        config_path : 'Path',
        keys : list,
        sub_path : 'Path'
    ) -> None:
    """
    Synchronize sub-configuration with main configuration file
    """
    import os, sys
    try:
        import yaml
    except ImportError:
        return
    from meerschaum.config._patch import apply_patch_to_config
    import meerschaum.config
    from meerschaum.utils.misc import reload_package, search_and_substitute_config
    if not os.path.isfile(config_path) or not os.path.isfile(sub_path): return

    def read_config(path):
        """
        Read YAML file with header comment
        """
        header_comment = ""
        with open(path, 'r') as f:
            if yaml:
                config = yaml.safe_load(f)
            else:
                print("PyYAML not installed!")
                sys.exit()
            f.seek(0)
            for line in f:
                #  print(len(line))
                if not line.startswith('#') and not line == '\n':
                    break
                header_comment += line
        return header_comment, config

    config_header, config = read_config(config_path)
    sub_header, sub_config = read_config(sub_path)

    config = search_and_substitute_config(config)
    sub_config = search_and_substitute_config(sub_config)
    c = config
    for k in keys:
        c_parent = c
        c = c[k]

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

        reload_package(meerschaum.config)
        reload_package(meerschaum.config)

