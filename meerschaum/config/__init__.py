#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message
"""

from meerschaum.config._version import __version__
from meerschaum.config._read_yaml import config

from meerschaum.utils.debug import dprint
import os

### developer-specified values
system_config = config['system']

### apply config preprocessing (e.g. main to meta)
from meerschaum.config._preprocess import preprocess_config
config = preprocess_config(config)

### if patch.yaml exists, apply patch to config
from meerschaum.config._patch import permanent_patch_config, patch_config, apply_patch_to_config
if patch_config is not None:
    from meerschaum.config._paths import PATCH_PATH
    config = apply_patch_to_config(config, patch_config)
    if PATCH_PATH.exists(): os.remove(PATCH_PATH)

### if permanent_patch.yaml exists, apply patch to config, write config, and delete patch
if permanent_patch_config is not None:
    from meerschaum.config._edit import write_config
    dprint("Found permanent patch configuration. Updating main config and deleting permanent patch...")
    config = apply_patch_to_config(config, permanent_patch_config)
    write_config(config)
    from meerschaum.config._paths import PERMANENT_PATCH_PATH
    if PERMANENT_PATCH_PATH.exists(): os.remove(PERMANENT_PATCH_PATH)

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

def get_config(*keys, patch=False, debug=False):
    """
    Return the Meerschaum configuration dictionary.
    If positional arguments are provided, index by the keys.
    Raises a warning if invalid keys are provided.

    E.g. get_config('system', 'shell') == config['system']['shell']
    """
    from meerschaum.utils.debug import dprint
    if debug: dprint(f"Indexing keys: {keys}")
    c = config
    if len(keys) > 0:
        for k in keys:
            try:
                c = c[k]
            except KeyError:
                from meerschaum.utils.warnings import warn
                warn(f"Invalid keys in config: {keys}")
                if patch:
                    from meerschaum.config._paths import PERMANENT_PATCH_PATH, CONFIG_PATH
                    import shutil, sys
                    if debug: dprint(f"Moving {CONFIG_PATH} to {PERMANENT_PATCH_PATH}. Restart Meerschaum to patch configuration with new defaults.")
                    shutil.move(CONFIG_PATH, PERMANENT_PATCH_PATH)
                    sys.exit()
    return c

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

