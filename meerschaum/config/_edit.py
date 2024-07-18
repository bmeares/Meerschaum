#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

from __future__ import annotations
import sys
import pathlib
from meerschaum.utils.typing import Optional, Any, SuccessTuple, Mapping, Dict, List, Union

def edit_config(
        keys : Optional[List[str]] = None,
        params : Optional[Dict[str, Any]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """Edit the configuration files."""
    from meerschaum.config import get_config, config
    from meerschaum.config._read_config import get_keyfile_path
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.utils.packages import reload_meerschaum
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.debug import dprint

    if keys is None:
        keys = []

    for k in keys:
        ### If defined in default, create the config file.
        if isinstance(config, dict) and k in config:
            del config[k]
        get_config(k, write_missing=True, warn=False)
        edit_file(get_keyfile_path(k, create_new=True))

    reload_meerschaum(debug=debug)
    return (True, "Success")


def write_config(
        config_dict: Optional[Dict[str, Any]] = None,
        directory: Union[str, pathlib.Path, None] = None,
        debug: bool = False,
        **kw : Any
    ) -> bool:
    """Write YAML and JSON files to the configuration directory.

    Parameters
    ----------
    config_dict: Optional[Dict[str, Any]], default None
        A dictionary of keys to dictionaries of configuration.
        Each key corresponds to a .yaml or .json config file.
        Writing config to a directory with different keys
        does not affect existing keys in that directory.
        If not provided, use the currently loaded config dictionary.

    directory: Union[str, pathlib.Path, None], default None
        The directory to which the keys are written.
        If not provided, use the default config path (`~/.config/meerschaum/config/`).

    Returns
    -------
    A bool indicating success.

    """
    if directory is None:
        from meerschaum.config._paths import CONFIG_DIR_PATH
        directory = CONFIG_DIR_PATH
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.config._default import default_header_comment
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.config._read_config import get_keyfile_path
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.yaml import yaml
    from meerschaum.utils.misc import filter_keywords
    import json, os
    if config_dict is None:
        from meerschaum.config import _config
        cf = _config()
        config_dict = cf

    default_filetype = STATIC_CONFIG['config']['default_filetype']
    filetype_dumpers = {
        'yml' : yaml.dump,
        'yaml' : yaml.dump,
        'json' : json.dump,
    }

    symlinks_key = STATIC_CONFIG['config']['symlinks_key']
    symlinks = config_dict.pop(symlinks_key) if symlinks_key in config_dict else {}
    config_dict = apply_patch_to_config(config_dict, symlinks)

    def determine_filetype(k, v):
        if k == 'meerschaum':
            return 'yaml'
        if isinstance(v, dict) and 'filetype' in v:
            return v['filetype']
        path = get_keyfile_path(k, create_new=False, directory=directory)
        if path is None:
            return default_filetype
        filetype = path.suffix[1:]
        if not isinstance(filetype, str) or filetype not in filetype_dumpers:
            print(f"Invalid filetype '{filetype}' for '{k}'. Assuming {default_filetype}...")
            filetype = default_filetype
        return filetype

    for k, v in config_dict.items():
        filetype = determine_filetype(k, v)        
        filename = str(k) + '.' + str(filetype)
        filepath = os.path.join(directory, filename)
        pathlib.Path(filepath).parent.mkdir(exist_ok=True)
        with open(filepath, 'w+') as f:
            try:
                if k == 'meerschaum':
                    f.write(default_header_comment)
                filetype_dumpers[filetype](
                    v, f,
                    **filter_keywords(
                        filetype_dumpers[filetype],
                        sort_keys = False,
                        indent = 2
                    )
                )
                success = True
            except Exception as e:
                success = False
                print(f"FAILED TO WRITE!")
                print(e)
                print(filter_keywords(
                    filetype_dumpers[filetype],
                    sort_keys=False,
                    indent = 2
                ))

            if not success:
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                except Exception as e:
                    print(f"Failed to write '{k}'")
                return False

    return True

def general_write_yaml_config(
        files: Optional[Dict[pathlib.Path, Dict[str, Any]]] = None,
        debug: bool = False
    ):
    """
    Write configuration dictionaries to file paths with optional headers.

    Parameters
    ----------
    files: Optional[Dict[str, pathlib.Path]], default None
        Dictionary of paths -> dict or tuple of format (dict, header).
        If item is a tuple, the header will be written at the top of the file.
    """

    from meerschaum.utils.debug import dprint

    if files is None:
        files = {}

    for fp, value in files.items():
        config = value
        header = None
        if isinstance(value, tuple):
            config, header = value
        path = pathlib.Path(fp)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        with open(path, 'w+', encoding='utf-8') as f:
            if header is not None:
                if debug:
                    dprint(f"Header detected, writing to {path}...")
                f.write(header)
            if isinstance(config, str):
                if debug:
                    dprint(f"Config is a string. Writing to {path}...")
                f.write(config)
            elif isinstance(config, dict):
                if debug:
                    dprint(f"Config is a dict. Writing to {path}...")
                from meerschaum.utils.yaml import yaml
                yaml.dump(config, stream=f, sort_keys=False)

    return True

def general_edit_config(
        action: Optional[List[str]] = None,
        files: Optional[Dict[str, Union[str, pathlib.Path]]] = None,
        default: Optional[str] = None,
        debug: bool = False
    ):
    """Prompt the user to edit any config files."""
    if default is None:
        raise Exception("Provide a default choice for which file to edit")
    import os
    from subprocess import call
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.debug import dprint

    if files is None:
        files = {}
    if action is None:
        action = []
    file_to_edit = files[default]
    if len(action) > 1 and action[1] in files:
        file_to_edit = files[action[1]]

    edit_file(file_to_edit)

    return True, "Success"

def copy_default_to_config(debug : bool = False):
    """Copy the default config directory to the main config directory.
    NOTE: This function is now depreciated in favor of the new patch system.
    """
    from meerschaum.config._paths import DEFAULT_CONFIG_DIR_PATH, CONFIG_DIR_PATH
    import shutil
    try:
        shutil.copytree(DEFAULT_CONFIG_DIR_PATH, CONFIG_DIR_PATH)
    except FileNotFoundError:
        write_default_config(debug=debug)
        return copy_default_to_config(debug=debug)
    except Exception as e:
        print("exception:", e)
    return True

def write_default_config(
        debug : bool = False,
        **kw
    ):
    """Write the default configuration files."""
    import os
    from meerschaum.config._paths import DEFAULT_CONFIG_DIR_PATH
    from meerschaum.config._default import default_config, default_header_comment
    return write_config(default_config, directory=DEFAULT_CONFIG_DIR_PATH)
