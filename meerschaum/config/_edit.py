#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

#  from meerschaum.utils.debug import dprint
import sys

def edit_config(
        params : dict = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Edit the configuration file

    params: patch to apply. Depreciated / replaced by --config (at least in this case)
    """
    import tempfile, os, importlib
    import meerschaum.config
    from meerschaum.config import config as cf
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.misc import reload_package, edit_file
    from meerschaum.utils.debug import dprint

    if params is not None:
        from meerschaum.utils import apply_patch_to_config
        cf = apply_patch_to_config(cf, params)
        if not write_config(cf, debug=debug):
            return False, "Failed to update config!"
    else:
        edit_file(CONFIG_PATH, debug=debug)

    if debug: dprint("Reloading configuration...")
    reload_package(meerschaum.config, debug=debug, **kw)
    reload_package(meerschaum.config, debug=debug, **kw)

    return (True, "Success")

def write_config(
        config_dict : dict = None,
        debug : bool = False,
        **kw
    ) -> bool:
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.config import config
    from meerschaum.config._default import default_header_comment
    from meerschaum.utils.debug import dprint
    import yaml
    if config_dict is None:
        config_dict = config

    if debug:
        from pprintpp import pprint
        dprint(f"Writing configuration to {CONFIG_PATH:}")
        pprint(config_dict, stream=sys.stderr)
    with open(CONFIG_PATH, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(config_dict, f, sort_keys=False)

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
                import yaml
                yaml.dump(config, f)

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
    from meerschaum.config._paths import DEFAULT_CONFIG_PATH, CONFIG_PATH
    import shutil
    try:
        shutil.copyfile(DEFAULT_CONFIG_PATH, CONFIG_PATH)
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
    import yaml, os
    from meerschaum.config._paths import PATCH_PATH, DEFAULT_CONFIG_PATH
    from meerschaum.config._default import default_config, default_header_comment
    from meerschaum.utils.debug import dprint
    if os.path.isfile(DEFAULT_CONFIG_PATH): os.remove(DEFAULT_CONFIG_PATH)
    if os.path.isfile(PATCH_PATH): os.remove(PATCH_PATH)
    if debug:
        from pprintpp import pprint
        pprint(default_config, stream=sys.stderr)

    config_copy = dict()
    config_copy['meerschaum'] = default_config['meerschaum'].copy()

    ### write meerschaum config first
    if debug: dprint(f"Writing default Meerschaum configuration to {DEFAULT_CONFIG_PATH}...")
    with open(DEFAULT_CONFIG_PATH, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(config_copy, f, sort_keys=False)
        f.write("\n\n")

    config_copy = default_config.copy()
    del config_copy['meerschaum']

    ### write the rest of the configuration
    if debug: dprint(f"Writing remaining default configuration to {DEFAULT_CONFIG_PATH}...")
    with open(DEFAULT_CONFIG_PATH, 'a+') as f:
        yaml.dump(config_copy, f)

    return True
