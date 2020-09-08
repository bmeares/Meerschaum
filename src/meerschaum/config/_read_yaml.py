#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

import yaml, sys, shutil
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

### parse the yaml file
default_filename = 'default_config.yaml'
config_filename = 'config.yaml'

### get full path of default config file
default_context_manager = pkg_resources.path('meerschaum.resources', default_filename)
with default_context_manager as file_path:
    default_path = file_path

try:
    config_text = pkg_resources.read_text('meerschaum.resources', config_filename)
except FileNotFoundError:
    print(f"Config file '{config_filename}' cannot be found.")
    print(f"Copying {default_filename} to {config_filename}.")
    print("You can edit the configuration with `edit config`")
    src_file = default_path
    dest_file = str(default_path).replace(default_filename, config_filename)
    shutil.copyfile(src_file, dest_file)
    #  print(f"Copy '{config_filename}' to meerschaum/resources/ and reinstall.")
    #  sys.exit()

config_text = pkg_resources.read_text('meerschaum.resources', config_filename)

### get full path of config file
config_context_manager = pkg_resources.path('meerschaum.resources', config_filename)
with config_context_manager as file_path:
    config_path = file_path

try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f'Unable to parse {config_filename}')
    print(e)
    sys.exit()

