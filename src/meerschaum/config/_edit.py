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

    ### get editor from environment
    EDITOR = os.environ.get('EDITOR', 'vim')

    if debug: print(f"Opening file '{config_path}' with editor '{EDITOR}'") 

    ### prompt user to edit config.yaml
    call([EDITOR, meerschaum.config.config_path])

    importlib.reload(meerschaum.config)

    return (True, "Success")
