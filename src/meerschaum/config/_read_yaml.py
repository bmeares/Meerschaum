#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

import yaml, sys, shutil, os
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

from meerschaum.config._default import default_filename, default_path, copy_default_to_config, resources_path

### get full path of default config file
config_filename = 'config.yaml'
config_path = os.path.join(resources_path, config_filename)

try:
    config_text = pkg_resources.read_text('meerschaum.config.resources', config_filename)
except FileNotFoundError:
    print(f"NOTE: Configuration file is missing. Falling back to default configuration.")
    print(f"You can edit the configuration with `edit config` or replace the file {config_path}")
    copy_default_to_config(config_filename)

config_text = pkg_resources.read_text('meerschaum.config.resources', config_filename)

### parse the yaml file
try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f'Unable to parse {config_filename}')
    print(e)
    sys.exit()

### apply preprocessing (e.g. main -> meta (if empty))
from meerschaum.config._preprocess import preprocess_config
config = preprocess_config(config)

### if patch.yaml exists, patch config
from meerschaum.config._patch import patch, patch_config
if patch is not None: config = patch_config(config, patch)

### if environment variable MEERSCHAUM_CONFIG is set, , patch config
from meerschaum.utils.misc import string_to_dict
environment_config = 'MEERSCHAUM_CONFIG'
if environment_config in os.environ:
    try:
        config = patch_config(config, string_to_dict(str(os.environ[environment_config])))
    except Exception as e:
        print(
            f"Environment variable {environment_config} is set but cannot be parsed.\n"
            f"Unset {environment_config} or change to JSON or simplified dictionary format (see --help, under params for formatting)\n"
            f"{environment_config} is set to:\n{os.environ[environment_config]}\n"
            f"Skipping patching os environment into config..."
        )
