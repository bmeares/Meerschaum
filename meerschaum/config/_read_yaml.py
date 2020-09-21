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

from meerschaum.config._edit import copy_default_to_config
from meerschaum.config._paths import CONFIG_PATH, CONFIG_FILENAME

try:
    with open(CONFIG_PATH, 'r') as f:
        config_text = f.read()
except FileNotFoundError:
    print(f"NOTE: Configuration file is missing. Falling back to default configuration.")
    print(f"You can edit the configuration with `edit config` or replace the file {CONFIG_PATH}")
    copy_default_to_config()

config_text = pkg_resources.read_text('meerschaum.config.resources', CONFIG_FILENAME)

### parse the yaml file
try:
    ### cf dictionary
    config = yaml.safe_load(config_text)
except Exception as e:
    print(f"Unable to parse {CONFIG_FILENAME}!")
    print(e)
    input(f"Press [Enter] to open {CONFIG_FILENAME} and fix formatting errors.")
    from meerschaum.config._default import default_system_config
    from meerschaum.utils.misc import edit_file
    edit_file(CONFIG_PATH)
    sys.exit()

### apply preprocessing (e.g. main -> meta (if empty))
from meerschaum.config._preprocess import preprocess_config
config = preprocess_config(config)

### if patch.yaml exists, apply patch to config
from meerschaum.config._patch import patch_config, apply_patch_to_config
if patch_config is not None: config = apply_patch_to_config(config, patch_config)

### if environment variable MEERSCHAUM_CONFIG is set, , patch config
from meerschaum.utils.misc import string_to_dict
environment_config = 'MEERSCHAUM_CONFIG'
if environment_config in os.environ:
    try:
        config = apply_patch_to_config(config, string_to_dict(str(os.environ[environment_config])))
    except Exception as e:
        print(
            f"Environment variable {environment_config} is set but cannot be parsed.\n"
            f"Unset {environment_config} or change to JSON or simplified dictionary format (see --help, under params for formatting)\n"
            f"{environment_config} is set to:\n{os.environ[environment_config]}\n"
            f"Skipping patching os environment into config..."
        )
