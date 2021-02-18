#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

#  from meerschaum.utils.debug import dprint
from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, SuccessTuple, Mapping, Dict, List
import sys

def edit_config(
        keys : List[str] = [],
        params : Optional[Mapping[str, Any]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit the configuration file

    :param params:
        patch to apply. Depreciated / replaced by --config (at least in this case)
    """
    from meerschaum.config._read_config import get_keyfile_path
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.utils.packages import reload_package
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.debug import dprint

    for k in keys:
        fp = get_keyfile_path(k, create_new=True)
        edit_file(fp)

    if debug: dprint("Reloading configuration...")
    reload_package('meerschaum', debug=debug, **kw)
    reload_package('meerschaum', debug=debug, **kw)

    return (True, "Success")

def write_config(
        config_dict : Optional[Dict[str, Any]] = None,
        directory : Optional[Union[str, pathlib.Path]] = None,
        debug : bool = False,
        **kw : Any
    ) -> bool:
    """
    Write YAML and JSON files to the configuration directory.

    :param config_dict:
        A dictionary of keys to dictionaries of configuration.
        Each key corresponds to a .yaml or .json config file.
        Writing config to a directory with different keys
        does not affect existing keys in that directory.

    :param directory:
        The directory to which the keys are written.
    """
    if directory is None:
        from meerschaum.config._paths import CONFIG_DIR_PATH
        directory = CONFIG_DIR_PATH
    from meerschaum.config.static import _static_config
    from meerschaum.config._default import default_header_comment
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.yaml import yaml
    from meerschaum.utils.misc import filter_keywords
    import json, os, pathlib
    if config_dict is None:
        from meerschaum.config import _config; cf = _config()
        config_dict = cf
    #  if debug:
        #  from meerschaum.utils.formatting import pprint
        #  print(f"Writing configuration to {directory}")
        #  pprint(config_dict, stream=sys.stderr)

    default_filetype = _static_config()['config']['default_filetype']
    filetype_dumpers = {
        'yml' : yaml.dump,
        'yaml' : yaml.dump,
        'json' : json.dump,
    }

    for k, v in config_dict.items():
        filetype = v.get('filetype', default_filetype)
        if k == 'meerschaum':
            filetype = 'yaml'
        if not isinstance(filetype, str) or filetype not in filetype_dumpers:
            print(f"Invalid filetype '{filetype}' for '{k}'. Assuming {default_filetype}...")
            filetype = default_filetype
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
    Copy the default config directory to the main config directory.
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
    """
    Write the default configuration files.
    """
    #  from meerschaum.utils.yaml import yaml
    import os
    from meerschaum.config._paths import DEFAULT_CONFIG_DIR_PATH
    from meerschaum.config._default import default_config, default_header_comment
    #  from meerschaum.utils.debug import dprint
    #  if os.path.isfile(DEFAULT_CONFIG_PATH): os.remove(DEFAULT_CONFIG_PATH)
    #  if os.path.isfile(PATCH_PATH): os.remove(PATCH_PATH)

    return write_config(default_config, directory=DEFAULT_CONFIG_DIR_PATH)

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
