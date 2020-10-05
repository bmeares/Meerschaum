#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message
"""

from meerschaum.config._version import __version__
from meerschaum.config._read_yaml import config

### developer-specified values
system_config = config['system']

### apply config preprocessing (e.g. main to meta)
from meerschaum.config._preprocess import preprocess_config
config = preprocess_config(config)

### if patch.yaml exists, apply patch to config
from meerschaum.config._patch import patch_config, apply_patch_to_config
if patch_config is not None:
    config = apply_patch_to_config(config, patch_config)

### if environment variable MEERSCHAUM_CONFIG is set, , patch config
from meerschaum.utils.misc import string_to_dict
import os
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


### if interactive shell, print welcome header
import sys
__doc__ = f"Meerschaum v{__version__}"
try:
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False
if interactive:
    msg = __doc__
    if config['system']['formatting']['ansi']:
        try:
            from more_termcolor import colored
        except:
            pass
        else:
            msg = colored(msg, 'bright blue', attrs=['bold'])
    print(msg, file=sys.stderr)

