#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

def edit_config(debug=False, **kw):
    import sys, tempfile, os, importlib
    from subprocess import call
    import meerschaum.config
    from meerschaum.utils.misc import reload_package

    ### get editor from environment
    EDITOR = os.environ.get('EDITOR', meerschaum.config.system_config['shell']['default_editor'])

    if debug: print(f"Opening file '{meerschaum.config.config_path}' with editor '{EDITOR}'") 

    ### prompt user to edit config.yaml
    call([EDITOR, meerschaum.config.config_path])

    if debug: print("Reloading configuration...")
    reload_package(meerschaum.config, debug=debug, **kw)
    reload_package(meerschaum.config, debug=debug, **kw)

    return (True, "Success")
