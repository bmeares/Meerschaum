#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, List, Tuple, Union
from meerschaum.config import get_config

def read_config(
        directory: Optional[str] = None,
        keys: Optional[List[str]] = None,
        write_missing : bool = True,
        substitute : bool = True,
        with_filenames : bool = False,
    ) -> Union[Dict[str, Any], Tuple[Dict[str, Any], List[str]]]:
    """
    Read the configuration directory.

    Parameters
    ----------
    directory: Optional[str], default None
        The directory with configuration files (.json and .yaml).

    keys: Optional[List[str]], default None
        Which configuration files to read.

    write_missing: bool, default True
        If a keyfile does not exist but is defined in the default configuration,
        write the file to disk.

    substitute: bool, default True
        Replace `MRSM{}` syntax with configuration values.

    with_filename: bool, default False
        If `True`, return a tuple of the configuration dictionary with a list of read filenames.
        
    Examples
    --------
    >>> read_config(keys=['meerschaum'], with_filename=True)
    >>> ({...}, ['meerschaum.yaml'])
    """
    import sys, shutil, os, json, itertools
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.yaml import yaml, _yaml
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.config._patch import apply_patch_to_config
    if directory is None:
        directory = CONFIG_DIR_PATH

    if _yaml is None:
        print('Could not import YAML! Reverting to default configuration.')
        from meerschaum.config._default import default_config
        return default_config

    ### Each key corresponds to a YAML or JSON file.
    symlinks_key = STATIC_CONFIG['config']['symlinks_key']
    config = {}
    config_to_write = {}

    default_filetype = STATIC_CONFIG['config']['default_filetype']
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
    if len(missing_keys) > 0:
        from meerschaum.config._default import default_config
        for mk in missing_keys:
            if mk not in default_config:
                continue
            _default_dict = (
                search_and_substitute_config(default_config) if substitute
                else default_config
            )
            ### If default config contains symlinks, add them to the config to write.
            try:
                _default_symlinks = _default_dict[symlinks_key][mk]
            except Exception as e:
                _default_symlinks = {}
            config[mk] = _default_dict[mk]
            if _default_symlinks:
                if symlinks_key not in config:
                    config[symlinks_key] = {}
                if mk not in config[symlinks_key]:
                    config[symlinks_key][mk] = {}
                config[symlinks_key][mk] = apply_patch_to_config(
                    config[symlinks_key][mk], 
                    _default_symlinks
                )
                if symlinks_key not in config_to_write:
                    config_to_write[symlinks_key] = {}
                config_to_write[symlinks_key][mk] = config[symlinks_key][mk]

            ### Write the default key.
            config_to_write[mk] = config[mk]

    ### Write missing keys if necessary.
    if len(config_to_write) > 0 and write_missing:
        from meerschaum.config._edit import write_config
        write_config(config_to_write, directory)

    ### Check for duplicate files.
    ### Found help on StackOverflow:
    ### https://stackoverflow.com/questions/26618688/python-iterate-over-a-list-
    ### of-files-finding-same-filenames-but-different-exten
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
            print(
                f"Found multiple config files named '{k}'. " +
                f"Will attempt to parse '{fn}' for key '{k}'."
            )
        filekeys.append(fn)

    _seen_keys = []
    for filename in filekeys:
        filepath = os.path.join(directory, filename)
        _parts = filename.split('.')
        _type = _parts[-1]
        key = '.'.join(_parts[:-1])
        ### Check if we've seen this key before (e.g. test.yaml, test.yml, test.json).
        if key in _seen_keys:
            print(
                f"Multiple files with the name '{key}' found in '{str(directory)}'. " +
                f"Reading from '{filename}'."
            )
        if len(_parts) < 2 or _type not in filetype_loaders:
            print(f"Unknown file '{filename}' in '{str(directory)}'. Skipping...")

        while True:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    try:
                        _config_key = filetype_loaders[_type](f)
                    except Exception as e:
                        print(f"Error processing file: {filepath}")
                        import traceback
                        traceback.print_exc()
                        _config_key = {}
                _single_key_config = (
                    search_and_substitute_config({key: _config_key}) if substitute
                    else {key: _config_key}
                )
                config[key] = _single_key_config[key]
                if (
                    symlinks_key in _single_key_config
                    and key in _single_key_config[symlinks_key]
                ):
                    if symlinks_key not in config:
                        config[symlinks_key] = {}
                    config[symlinks_key][key] = _single_key_config[symlinks_key][key]
                break
            except Exception as e:
                print(f"Unable to parse {filename}!")
                import traceback
                traceback.print_exc()
                input(f"Press [Enter] to open '{filename}' and fix formatting errors.")
                from meerschaum.utils.misc import edit_file
                edit_file(filepath)

    if with_filenames:
        return config, filekeys
    return config


def search_and_substitute_config(
        config: Dict[str, Any],
        leading_key: str = "MRSM",
        delimiter: str = ":",
        begin_key: str = "{",
        end_key: str = "}",
        literal_key: str = '!',
        keep_symlinks: bool = True,
    ) -> Dict[str, Any]:
    """Search the config for Meerschaum substitution syntax and substite with value of keys.

    Parameters
    ----------
    config: Dict[str, Any]
        The Meerschaum configuration dictionary to search through.

    leading_key: str, default 'MRSM'
        The string with which to start the search.

    begin_key: str, default '{'
        The string to start the keys list.

    end_key: str, default '}'
        The string to end the keys list.

    literal_key: str, default '!'
        The string to force an literal interpretation of a value.
        When the string is isolated, a literal interpreation is assumed and the surrounding
        quotes are replaced.
        
        E.g. Suppose a:b:c produces a dictionary {'d': 1}.
        - 'MRSM{a:b:c}'    => {'d': 1}        : isolated
        - ' MRSM{a:b:c} '  => ' "{\'d\': 1}"' : not isolated
        - ' MRSM{!a:b:c} ' => ' {"d": 1}'     : literal

    keep_symlinks :
        If True, include the symlinks under the top-level key '_symlinks' (never written to a file).
        Defaults to True.
        
        Example:

        ```
        MRSM{meerschaum:connectors:main:host} => cf['meerschaum']['connectors']['main']['host']
        ``` 
    """

    _links = []
    def _find_symlinks(d, _keys: Optional[List[str]] = None):
        if _keys is None:
            _keys = []
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            if isinstance(v, dict):
                _find_symlinks(v, _keys + [k])
            elif (leading_key + begin_key) in str(v):
                _links.append((_keys + [k], v))

    _find_symlinks(config)

    import json
    needle = leading_key + begin_key
    haystack = json.dumps(config, separators=(',', ':'))
    mod_haystack = list(str(haystack))
    buff = str(needle)
    max_index = len(haystack) - len(buff)

    patterns = {}
    isolated_patterns = {}
    literal_patterns = {}

    begin, end, floor = 0, 0, 0
    while needle in haystack[floor:]:
        ### extract the keys
        hs = haystack[floor:]

        ### the first character of the keys
        ### MRSM{key1:key2}
        ###      ^
        begin = hs.find(needle) + len(needle)

        ### The character behind the needle.
        ### "MRSM{key1:key2}"
        ### ^
        prior = haystack[(floor + begin) - (len(needle) + 1)]

        ### number of characters to end of keys
        ### (really it's the index of the beginning of the end_key relative to the beginning
        ###     but the math works out)
        ### MRSM{key1}
        ###      ^   ^  => 4
        length = hs[begin:].find(end_key)

        ### index of the end_key (end of `length` characters)
        end = begin + length

        ### The character after the end_key.
        after = haystack[floor + end + 1]

        ### advance the floor to find the next leading key
        floor += end + len(end_key)
        pattern_keys = hs[begin:end].split(delimiter)

        ### Check for isolation key and empty keys (MRSM{}).
        force_literal = False
        keys = [k for k in pattern_keys]
        if str(keys[0]).startswith(literal_key):
            keys[0] = str(keys[0])[len(literal_key):]
            force_literal = True
        if len(keys) == 1 and keys[0] == '':
            keys = []

        ### Evaluate the parsed keys to extract the referenced value.
        ### TODO This needs to be recursive for chaining symlinks together.
        try:
            valid, value = get_config(
                *keys,
                substitute = False,
                as_tuple = True,
                write_missing = False,
                sync_files = False,
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            valid = False
        if not valid:
            continue

        ### pattern to search and replace
        pattern = leading_key + begin_key + delimiter.join(pattern_keys) + end_key

        ### store patterns and values
        patterns[pattern] = value

        ### Determine whether the pattern occured inside a string or is an isolated, direct symlink.
        isolated_patterns[pattern] = (prior == '"' and after == '"')

        literal_patterns[pattern] = force_literal

    ### replace the patterns with the values
    for pattern, value in patterns.items():
        if isolated_patterns[pattern]:
            haystack = haystack.replace(
                json.dumps(pattern),
                json.dumps(value),
            )
        elif literal_patterns[pattern]:
            haystack = haystack.replace(
                pattern,
                (
                    json.dumps(value)
                    .replace("\\", "\\\\")
                    .replace('"', '\\"')
                    .replace("'", "\\'")
                )
            )
        else:
            haystack = haystack.replace(pattern, str(value))

    ### parse back into dict
    parsed_config = json.loads(haystack) or {}

    symlinks = {}
    if keep_symlinks:
        ### Keep track of symlinks for writing back to a file.
        for _keys, _pattern in _links:
            s = symlinks
            for k in _keys[:-1]:
                if k not in s:
                    s[k] = {}
                s = s[k]
            s[_keys[-1]] = _pattern

        from meerschaum.config._patch import apply_patch_to_config
        from meerschaum.config.static import STATIC_CONFIG
        symlinks_key = STATIC_CONFIG['config']['symlinks_key']
        if symlinks_key not in parsed_config:
            parsed_config[symlinks_key] = {}
        parsed_config[symlinks_key] = apply_patch_to_config(parsed_config[symlinks_key], symlinks)

    return parsed_config


def get_possible_keys() -> List[str]:
    """
    Return a list of possible top-level keys.
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


def get_keyfile_path(
        key: str,
        create_new: bool = False,
        directory: Union[pathlib.Path, str, None] = None,
    ) -> Union[pathlib.Path, None]:
    """Determine a key's file path."""
    import os, pathlib
    if directory is None:
        from meerschaum.config._paths import CONFIG_DIR_PATH
        directory = CONFIG_DIR_PATH

    try:
        return pathlib.Path(
            os.path.join(
                directory,
                read_config(
                    keys = [key],
                    with_filenames = True,
                    write_missing = False,
                    substitute = False,
                )[1][0]
            )
        )
    except IndexError as e:
        if create_new:
            from meerschaum.config.static import STATIC_CONFIG
            default_filetype = STATIC_CONFIG['config']['default_filetype']
            return pathlib.Path(os.path.join(directory, key + '.' + default_filetype))
        return None
