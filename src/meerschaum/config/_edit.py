#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

def edit_config(debug=False, **kw):
    import sys, tempfile, os
    from subprocess import call
    from meerschaum.config import config_path

    EDITOR = os.environ.get('EDITOR', 'vim')

    if debug: print(f"Opening file '{config_path}' with editor '{EDITOR}'") 

    call([EDITOR, config_path])

    return (True, "Success")
