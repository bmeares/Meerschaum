#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

#  from meerschaum.utils.debug import dprint
from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, SuccessTuple, Mapping, Dict
import sys

def edit_config(
        params : Optional[Mapping[str, Any]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit the configuration file

    :param params:
        patch to apply. Depreciated / replaced by --config (at least in this case)
    """
    import tempfile, os, importlib
    import meerschaum
    from meerschaum.config import _config, set_config; cf = _config()
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.packages import reload_package
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.debug import dprint

    if params is not None:
        from meerschaum.utils import apply_patch_to_config
        set_config(apply_patch_to_config(cf, params))
        if not write_config(cf, debug=debug):
            return False, "Failed to update config!"
    else:
        edit_file(CONFIG_PATH, debug=debug)

    if debug: dprint("Reloading configuration...")
    reload_package(meerschaum, debug=debug, **kw)
    # reload_package(meerschaum.config, debug=debug, **kw)

    return (True, "Success")

def write_config(
        config_dict : Optional[Dict[str, Any]] = None,
        directory : Optional[Union[str, pathlib.Path]] = None,
        debug : bool = False,
        **kw : Any
    ) -> bool:
    """
    Write YAML and JSON files to the configuration directory.
    """
    from meerschaum.config._paths import CONFIG_DIR_PATH
    if directory is None:
        directory = CONFIG_DIR_PATH
    from meerschaum.config.static import _static_config
    from meerschaum.config._default import default_header_comment
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.yaml import yaml
    from meerschaum.utils.misc import filter_keywords
    import json, os
    if config_dict is None:
        from meerschaum.config import _config; cf = _config()
        config_dict = cf
    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Writing configuration to {CONFIG_PATH:}")
        pprint(config_dict, stream=sys.stderr)

    default_filetype = _static_config()['config']['default_filetype']
    filetype_dumpers = {
        'yml' : yaml.dump,
        'yaml' : yaml.dump,
        'json' : json.dump,
    }

    for k, v in config_dict:
        filetype = v.get('filetype', default_filetype)
        if k == 'meerschaum':
            filetype = 'yaml'
        if not isinstance(filetype, str) or filetype not in filetype_dumpers:
            print(f"Invalid filetype '{filetype}' for '{k}'. Assuming {default_filetype}...")
            filetype = default_filetype
        filename = str(k) + '.' + str(filetype)
        filepath = os.path.join(directory, filename)
        with open(filepath, 'w+') as f:
            try:
                if k == 'meerschaum':
                    f.write(default_header_comment)
                filetype_dumpers[filetype](v, f, **filter_keywords(sort_keys=False, indent=2))
                success = True
            except:
                success = False

            if not success:
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                except Exception as e:
                    print(f"Failed to write '{k}'")
                return False

    return True

def general_write_config(
        files : dict = {},
        debug : bool = False
    ):
    """
    Write configuration dictionaries to file paths with optional headers.

    files : dict
        Dictionary of paths -> dict or tuple of format (dict, header).
        If item is a tuple, the header will be written at the top of the file.
    """

    from meerschaum.utils.debug import dprint
    from pathlib import Path

    for fp, value in files.items():
        config = value
        header = None
        if isinstance(value, tuple):
            config, header = value
        path = Path(fp)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        with open(path, 'w+') as f:
            if header is not None:
                if debug: dprint(f"Header detected, writing to {path}...")
                f.write(header)
            if isinstance(config, str):
                if debug: dprint(f"Config is a string. Writing to {path}...")
                f.write(config)
            elif isinstance(config, dict):
                if debug: dprint(f"Config is a dict. Writing to {path}...")
                #  import yaml
                from meerschaum.utils.yaml import yaml
                yaml.dump(config, stream=f, sort_keys=False)

    return True

def general_edit_config(
        action : list = [''],
        files : dict = {},
        default : str = None,
        debug : bool = False
    ):
    """
    Edit any config files
    """
    if default is None:
        raise Exception("Provide a default choice for which file to edit")
    import os
    from subprocess import call
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.debug import dprint

    file_to_edit = files[default]
    if len(action) > 1:
        if action[1] in files:
            file_to_edit = files[action[1]]

    edit_file(file_to_edit)

    return True, "Success"

def copy_default_to_config(debug : bool = False):
    """
    Copy the default config file to the main config file
    """
    from meerschaum.config._paths import DEFAULT_CONFIG_DIR_PATH, CONFIG_DIR_PATH
    import shutil
    try:
        shutil.copytree(DEFAULT_CONFIG_DIR_PATH, CONFIG_DIR_PATH)
    except FileNotFoundError:
        write_default_config(debug=debug)
        return copy_default_to_config(debug=debug)
    return True

def write_default_config(
        debug : bool = False,
        **kw
    ):
    """
    Overwrite the existing default_config.yaml.
    """
    from meerschaum.utils.yaml import yaml
    import os
    from meerschaum.config._paths import PATCH_PATH, DEFAULT_CONFIG_DIR_PATH
    from meerschaum.config._default import default_config, default_header_comment
    from meerschaum.utils.debug import dprint
    #  if os.path.isfile(DEFAULT_CONFIG_PATH): os.remove(DEFAULT_CONFIG_PATH)
    #  if os.path.isfile(PATCH_PATH): os.remove(PATCH_PATH)

    return write_config(default_config, directory=DEFAULT_CONFIG_PATH)

    #  config_copy = dict()
    #  config_copy['meerschaum'] = default_config['meerschaum'].copy()

    #  ### write meerschaum config first
    #  if debug: dprint(f"Writing default Meerschaum configuration to {DEFAULT_CONFIG_PATH}...")
    #  with open(DEFAULT_CONFIG_PATH, 'w') as f:
        #  f.write(default_header_comment)
        #  yaml.dump(config_copy, stream=f, sort_keys=False)
        #  f.write("\n\n")

    #  config_copy = default_config.copy()
    #  del config_copy['meerschaum']

    #  ### write the rest of the configuration
    #  if debug: dprint(f"Writing remaining default configuration to {DEFAULT_CONFIG_PATH}...")
    #  with open(DEFAULT_CONFIG_PATH, 'a+') as f:
        #  yaml.dump(config_copy, stream=f, sort_keys=False)

    #  return True
