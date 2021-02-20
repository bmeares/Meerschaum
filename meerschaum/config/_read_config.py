#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, List, Tuple

def read_config(
        directory : Optional[Dict[str, Any]] = None,
        keys : Optional[List[str]] = None,
        write_missing : bool = True,
        substitute : bool = True,
        with_filenames : bool = False,
    ) -> Union[Dict[str, Any], Tuple[Dict[str, Any], List[str]]]:
    """
    Read the configuration directory.

    :param directory:
        The directory with configuration files (.json and .yaml).

    :param keys:
        Which configuration files to read.

    :param write_missing:
        If a keyfile does not exist but is defined in the default configuration,
        write the file to disk.
        Defauls to True.

    :param substitute:
        Replace `MRSM{}` syntax with configuration values.
        Defaults to True.

    :param with_filename:
        If True, return a tuple of the configuration dictionary with a list of read filenames.
        Defaults to False.

        Example:
        ```
        >>> read_config(keys='meerschaum', with_filename=True)
        >>> ({...}, ['meerschaum.yaml'])

        ```
    """
    import sys, shutil, os, json, itertools
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.yaml import yaml, _yaml
    #  from meerschaum.config._edit import copy_default_to_config
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.config.static import _static_config
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
    filekeys = []
    filenames = os.listdir(directory)
    missing_keys, found_keys = set(), set()
    if keys is None:
        _filekeys = filenames
    else:
        _filekeys = []
        for k in keys:
            for ft in filetype_loaders:
                if str(k) + '.' + str(ft) in filenames:
                    _filekeys.append(str(k) + '.' + str(ft))
                    found_keys.add(k)
                    if k in missing_keys:
                        missing_keys.remove(k)
                elif k not in found_keys:
                    missing_keys.add(k)

    ### Check for missing files with default keys.
    config_to_write = {}
    if len(missing_keys) > 0:
        from meerschaum.config._default import default_config
        for mk in missing_keys:
            if mk in default_config:
                _default_mk = (
                    search_and_substitute_config({'default' : default_config})['default'][mk] if substitute
                    else default_config[mk]
                )
                config[mk] = _default_mk
                config_to_write[mk] = config[mk]

    ### Write missing keys if necessary.
    if len(config_to_write) > 0 and write_missing:
        from meerschaum.config._edit import write_config
        write_config(config_to_write, directory)

    ### Check for duplicate files.
    ### Found help on StackOverflow:
    ### https://stackoverflow.com/questions/26618688/python-iterate-over-a-list-of-files-finding-same-filenames-but-different-exten
    keygroups = {
        key: list(value)
        for key, value in itertools.groupby(
            sorted(_filekeys, key = lambda e: os.path.splitext(e)[0]),
            key = lambda e: os.path.splitext(e)[0]
        )
    }
    for k, v in keygroups.items():
        fn = v[0]
        if len(v) > 1:
            if k + '.' + default_filetype in v:
                fn = k + '.' + default_filetype
            print(f"Found multiple config files named '{k}'. Will attempt to parse '{fn}' for key '{k}'.")
        filekeys.append(fn)

    _seen_keys = []
    for filename in filekeys:
        filepath = os.path.join(directory, filename)
        _parts = filename.split('.')
        _type = _parts[-1]
        key = '.'.join(_parts[:-1])
        ### Check if we've seen this key before (e.g. test.yaml, test.yml, test.json).
        if key in _seen_keys:
            print(f"Multiple files with the name '{key}' found in '{str(directory)}'. Reading from '{filename}'.")
        if len(_parts) < 2 or _type not in filetype_loaders:
            print(f"Unknown file '{filename}' in '{str(directory)}'. Skipping...")

        while True:
            try:
                with open(filepath, 'r') as f:
                    _config_key = filetype_loaders[_type](f)
                    config[key] = (
                        search_and_substitute_config({key : _config_key})[key] if substitute
                        else _config_key
                    )
                break
            except Exception as e:
                print(f"Unable to parse {filename}!")
                print(e)
                input(f"Press [Enter] to open '{filename}' and fix formatting errors.")
                from meerschaum.utils.misc import edit_file
                edit_file(filepath)

    if with_filenames:
        return config, filekeys
    return config

def search_and_substitute_config(
        config : dict,
        leading_key : str = "MRSM",
        delimiter : str = ":",
        begin_key : str = "{",
        end_key : str = "}"
    ) -> dict:
    """
    Search the config for Meerschaum substitution syntax and substite with value of keys

    Example:
        MRSM{meerschaum:connectors:main:host} => cf['meerschaum']['connectors']['main']['host']
    """
    import json
    needle = leading_key + begin_key
    haystack = json.dumps(config)
    from meerschaum.config import get_config

    haystack = json.dumps(config)
    mod_haystack = list(str(haystack))
    buff = str(needle)
    max_index = len(haystack) - len(buff)

    patterns = dict()

    begin, end, floor = 0, 0, 0
    while needle in haystack[floor:]:
        ### extract the keys
        hs = haystack[floor:]

        ### the first character of the keys
        ### MRSM{value1:value2}
        ###      ^
        begin = hs.find(needle) + len(needle)

        ### number of characters to end of keys
        ### (really it's the index of the beginning of the end_key relative to the beginning
        ###     but the math works out)
        ### MRSM{value1}
        ###      ^     ^  => 6
        length = hs[begin:].find(end_key)

        ### index of the end_key (end of `length` characters)
        end = begin + length

        ### advance the floor to find the next leading key
        floor += end + len(end_key)
        keys = hs[begin:end].split(delimiter)

        ### follow the pointers to the value
        valid, value = get_config(*keys, substitute=False, as_tuple=True)
        if valid:
            ### pattern to search and replace
            pattern = leading_key + begin_key + delimiter.join(keys) + end_key

            ### store patterns and values
            patterns[pattern] = value

    ### replace the patterns with the values
    for pattern, value in patterns.items():
        haystack = haystack.replace(pattern, str(value))

    ### parse back into dict
    return json.loads(haystack)

def get_possible_keys() -> List[str]:
    """
    Return a list of possible configuration keys.
    """
    import os
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.config._default import default_config
    keys = set()
    for key in default_config:
        keys.add(key)
    for filename in os.listdir(CONFIG_DIR_PATH):
        keys.add('.'.join(filename.split('.')[:-1]))
    return sorted(list(keys))

def get_keyfile_path(key : str, create_new : bool = False) -> Optional[pathlib.Path]:
    """
    Determine a key's file path.
    """
    import os, pathlib
    from meerschaum.config._paths import CONFIG_DIR_PATH

    try:
        return pathlib.Path(os.path.join(
            CONFIG_DIR_PATH,
            read_config(keys=[key], with_filenames=True)[1][0]
        ))
    except IndexError as e:
        if create_new:
            from meerschaum.config.static import _static_config
            default_filetype = _static_config()['config']['default_filetype']
            return pathlib.Path(os.path.join(CONFIG_DIR_PATH, key + '.' + default_filetype))
        return None

