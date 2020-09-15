#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

def edit_config(
        params : dict = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    import sys, tempfile, os, importlib
    from subprocess import call
    import meerschaum.config
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.misc import reload_package

    if params is not None:
        meerschaum.config.config.update(params)
        if not write_config(meerschaum.config.config, debug=debug):
            return False, "Failed to update config!"
    else:
        ### get editor from environment
        EDITOR = os.environ.get('EDITOR', meerschaum.config.system_config['shell']['default_editor'])

        if debug: print(f"Opening file '{CONFIG_PATH}' with editor '{EDITOR}'") 

        ### prompt user to edit config.yaml
        call([EDITOR, CONFIG_PATH])

    if debug: print("Reloading configuration...")
    reload_package(meerschaum.config, debug=debug, **kw)
    reload_package(meerschaum.config, debug=debug, **kw)

    return (True, "Success")

def write_config(
        config_dict : dict = None,
        debug : bool = False,
        **kw
    ) -> bool:
    from meerschaum.config import config_path, config
    from meerschaum.config._default import default_header_comment
    import yaml
    if config_dict is None:
        config_dict = config

    with open(config_path, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(config_dict, f)

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
                if debug: print(f"Header detected, writing to {path}...")
                f.write(header)
            if isinstance(config, str):
                if debug: print(f"Config is a string. Writing to {path}...")
                f.write(config)
            elif isinstance(config, dict):
                if debug: print(f"Config is a dict. Writing to {path}...")
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
    from meerschaum.config import system_config
    import os
    from subprocess import call
    EDITOR = os.environ.get('EDITOR', system_config['shell']['default_editor'])

    file_to_edit = files[default]
    if len(action) > 1:
        if action[1] in files:
            file_to_edit = files[action[1]]

    if debug: print(f"Opening file '{file_to_edit}' with editor '{EDITOR}'") 

    ### open editor
    call([EDITOR, file_to_edit])

    return True, "Success"

def copy_default_to_config():
    """
    Copy the default config file to the main config file
    """
    from meerschaum.config._paths import DEFAULT_CONFIG_PATH, CONFIG_PATH
    import shutil
    try:
        shutil.copyfile(DEFAULT_CONFIG_PATH, CONFIG_PATH)
    except FileNotFoundError:
        write_default_config()
        return copy_default_to_config()
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
    if os.path.isfile(DEFAULT_CONFIG_PATH): os.remove(DEFAULT_CONFIG_PATH)
    if os.path.isfile(PATCH_PATH): os.remove(PATCH_PATH)
    if debug:
        from pprintpp import pprint
        pprint(default_config)

    config_copy = dict()
    config_copy['meerschaum'] = dict(default_config['meerschaum'])

    ### write meerschaum config first
    if debug: print(f"Writing default Meerschaum configuration to {DEFAULT_CONFIG_PATH}...")
    with open(DEFAULT_CONFIG_PATH, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(config_copy, f)
        f.write("\n\n")

    config_copy = dict(default_config)
    del config_copy['meerschaum']

    ### write the rest of the configuration
    if debug: print(f"Writing remaining default configuration to {DEFAULT_CONFIG_PATH}...")
    with open(DEFAULT_CONFIG_PATH, 'a+') as f:
        yaml.dump(config_copy, f)

    return True
