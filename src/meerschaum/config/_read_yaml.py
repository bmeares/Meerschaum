#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

import yaml, sys
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

### parse the yaml file
config_filename = 'config.yaml'
try:
    config_text = pkg_resources.read_text('meerschaum.resources', config_filename)
except FileNotFoundError:
    print(f"Config file '{config_filename}' cannot be found.")
    print(f"Copy '{config_filename}' to meerschaum/resources/ and reinstall.")
    sys.exit()

### get full path of config file
context_manager = pkg_resources.path('meerschaum.resources', config_filename)
with context_manager as file_path:
    config_path = file_path

try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f'Unable to parse {config_filename}')
    print(e)
    sys.exit()

