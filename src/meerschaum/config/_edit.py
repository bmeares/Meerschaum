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
    from meerschaum.utils.misc import reload_package

    if params is not None:
        meerschaum.config.config.update(params)
        if not write_config(meerschaum.config.config, debug=debug):
            return False, "Failed to update config!"
    else:
        ### get editor from environment
        EDITOR = os.environ.get('EDITOR', meerschaum.config.system_config['shell']['default_editor'])

        if debug: print(f"Opening file '{meerschaum.config.config_path}' with editor '{EDITOR}'") 

        ### prompt user to edit config.yaml
        call([EDITOR, meerschaum.config.config_path])


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
    import yaml, os
    if config_dict is None:
        config_dict = config

    with open(config_path, 'w') as f:
        f.write(default_header_comment)
        yaml.dump(config_dict, f)

    return True

