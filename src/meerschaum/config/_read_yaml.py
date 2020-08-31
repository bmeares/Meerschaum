#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

import yaml
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

### parse the yaml file
config_filename = 'config.yaml'
config_text = pkg_resources.read_text('meerschaum.resources', config_filename)
try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f'Unable to parse {config_filename}')
    print(e)
    sys.exit()

