#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
and if interactive, print the welcome message
"""

import yaml, sys, os
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

from meerschaum._version import __version__ as version

### parse the yaml file
config_filename = 'config.yaml'
config_text = pkg_resources.read_text('meerschaum', config_filename)
try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f'Unable to parse {config_filename}')
    print(e)
    sys.exit()

### if interactive shell, print welcome header
header = "Hello, World!"
try:
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False
if interactive:
    print(header)
