#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any

def read_config(directory : Optional[Dict[str, Any]] = None, keys : Optional[List[str]] = None):
    import sys, shutil, os, json, itertools
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.yaml import yaml, _yaml
    from meerschaum.config._edit import copy_default_to_config
    from meerschaum.config._paths import CONFIG_DIR_PATH
    if directory is None:
        directory = CONFIG_DIR_PATH

    if _yaml is None:
        print('Could not import YAML! Reverting to default configuration.')
        from meerschaum.config._default import default_config
        return default_config

    ### Each key corresponds to a YAML or JSON file.
    config = {}

    default_filetype = _static_config()['config']['default_filetype']
    filetype_loaders = {
        'yml' : yaml.load,
        'yaml' : yaml.load,
        'json' : json.load,
    }

    ### Construct filekeys (files to parse).
    filenames = os.listdir(directory)
    if keys is None:
        filekeys = filenames
    else:
        filekeys = []
        for k in keys:
            for ft in filetype_loaders:
                if str(k) + '.' + str(ft) in filenames:
                    filekeys.append(str(k) + '.' + str(ft))

    ### Check for duplicate files.
    ### Found help on StackOverflow:
    ### https://stackoverflow.com/questions/26618688/python-iterate-over-a-list-of-files-finding-same-filenames-but-different-exten
    keygroups = {
        key: set(value)
        for key, value in itertools.groupby(
            sorted(mylist, key = lambda e: os.path.splitext(e)[0]),
            key = lambda e: os.path.splitext(e)[0]
        )
    }
    for k, v in keygroups.items():
        fn = v[0]
        if len(v) > 1:
            if default_filetype in v:
                fn = k + default_filetype
            print(f"Found multiple config files named '{k}'. Will attempt to parse '{fn}' for key '{k}'.")
        filekeys.append(fn)

    _seen_keys = []
    for filename in filekeys:
        _parts = filename.split('.')
        _type = _parts[-1]
        key = '.'.join(_parts[:-1])
        ### Check if we've seen this key before (e.g. test.yaml, test.yml, test.json).
        if key in _seen_keys:
            print(f"Multiple files with the name '{key}' found in '{str(directory)}'. Reading from '{filename}'.")
        if len(_parts) < 2 or _type not in filetype_loaders:
            print(f"Unknown file '{filename}' in '{str(directory)}'. Skipping...")

        if _yaml is not None:
            try:
                with open(os.path.join(directory, filename), 'r') as f:
                    config[key] = filetype_loaders[_type](f)
            except FileNotFoundError:
                if directory == CONFIG_DIR_PATH:
                    print(f"NOTE: Configuration file is missing. Falling back to default configuration.")
                    print(f"You can edit the configuration with `edit config` or replace the file {CONFIG_PATH}")
                    ### TODO Restructure default
                    copy_default_to_config()
            except Exception as e:
                print(f"Unable to parse {filename}!")
                print(e)
                input(f"Press [Enter] to open {filename} and fix formatting errors.")
                from meerschaum.config._default import default_system_config
                from meerschaum.utils.misc import edit_file
                edit_file(CONFIG_PATH)
                sys.exit(1)
        else:
            from meerschaum.config._default import default_config
            config = default_config

    return config

