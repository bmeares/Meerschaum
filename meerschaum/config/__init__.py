#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Dict, Optional, Union

from meerschaum.config._version import __version__
from meerschaum.config._edit import write_config
from meerschaum.config.static import _static_config

import os, shutil

### apply config preprocessing (e.g. main to meta)
config = None
def _config(*keys : str, reload : bool = False) -> Dict[str, Any]:
    """
    Read and process the configuration file.
    """
    global config
    if config is None or reload:
        from meerschaum.config._read_config import read_config
        config = read_config(keys=keys)
    return config

def set_config(cf : Dict[str, Any]) -> Dict[str, Any]:
    """
    Set the configuration dictionary to a dictionary
    """
    global config
    if not isinstance(cf, dict):
        from meerschaum.utils.warnings import error
        error(f"Invalid value for config: {cf}", stacklevel=3)
    config = cf
    return config

def get_config(
        *keys : str,
        patch : bool = False,
        substitute : bool = True,
        as_tuple : bool = False,
        warn : bool = True,
        debug = False
    ) -> Any:
    """
    Return the Meerschaum configuration dictionary.
    If positional arguments are provided, index by the keys.
    Raises a warning if invalid keys are provided.

    :param keys:
        List of strings to index.

    :param patch:
        If invalid keys are detected and `patch` is True, rename the `config` directory
        to `permanent_patch_config`, which preserves configuration across installs.
        With the new config system, this is less necessary.
        
    :param substitute:
        If True, subsitute 'MRSM{}' values.
        Defaults to True.

    :param as_tuple:
        If True, return a tuple of type (success, value).
        Defaults to False.

    E.g. get_config('system', 'shell') == config['system']['shell']
    """
    global config

    from meerschaum.utils.debug import dprint
    if debug: dprint(f"Indexing keys: {keys}")

    if len(keys) == 0:
        return _config()

    from meerschaum.config._read_config import read_config
    if config is None:
        config = read_config(keys=[keys[0]], substitute=substitute)

    if keys[0] not in config:
        config.update(read_config(keys=[keys[0]], substitute=substitute))

    c = config
    invalid_keys = False
    if len(keys) > 0:
        for k in keys:
            try:
                c = c[k]
            except:
                invalid_keys = True
                break
        if invalid_keys:
            from meerschaum.config._paths import PERMANENT_PATCH_DIR_PATH, CONFIG_DIR_PATH
            warning_msg = f"Invalid keys in config: {keys}"
            debug_msg = f"Moving {CONFIG_DIR_PATH} to {PERMANENT_PATCH_DIR_PATH}. Restart Meerschaum to patch configuration with new defaults."
            try:
                if warn:
                    from meerschaum.utils.warnings import warn as _warn
                    _warn(warning_msg, stacklevel=3)
            except:
                if warn:
                    print(f"Invalid keys in config: {keys}")
            if patch:
                import shutil, sys
                try:
                    dprint(debug_msg, color=False)
                except:
                    print(debug_msg)
                shutil.move(CONFIG_DIR_PATH, PERMANENT_PATCH_DIR_PATH)
                sys.exit(1)
    if as_tuple:
        return (not invalid_keys), c
    return c

### If patches exist, apply to config.
from meerschaum.config._paths import PERMANENT_PATCH_DIR_PATH
from meerschaum.config._patch import permanent_patch_config, patch_config, apply_patch_to_config
if patch_config is not None:
    from meerschaum.config._paths import PATCH_DIR_PATH
    set_config(apply_patch_to_config(_config(), patch_config))
    if PATCH_DIR_PATH.exists():
        shutil.rmtree(PATCH_DIR_PATH)

### if permanent_patch.yaml exists, apply patch to config, write config, and delete patch
if permanent_patch_config is not None and PERMANENT_PATCH_DIR_PATH.exists():
    from meerschaum.config._edit import write_config
    from meerschaum.utils.debug import dprint
    print("Found permanent patch configuration. Updating main config and deleting permanent patch...")
    set_config(apply_patch_to_config(_config(), permanent_patch_config))
    write_config(_config())
    from meerschaum.config._paths import PERMANENT_PATCH_DIR_PATH, DEFAULT_CONFIG_DIR_PATH
    permanent_patch_config = None
    if PERMANENT_PATCH_DIR_PATH.exists():
        shutil.rmtree(PERMANENT_PATCH_DIR_PATH)
    if DEFAULT_CONFIG_DIR_PATH.exists():
        shutil.rmtree(DEFAULT_CONFIG_DIR_PATH)

### If environment variable MEERSCHAUM_CONFIG is set, patch config before anything else.
from meerschaum.utils.misc import string_to_dict
import os
environment_config = _static_config()['config']['environment_key']
if environment_config in os.environ:
    try:
        set_config(
            apply_patch_to_config(
                _config(),
                string_to_dict(
                    str(os.environ[environment_config])
                )
            )
        )
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
    #  if config['system']['formatting']['ansi']:
        #  try:
            #  from more_termcolor import colored
        #  except:
            #  pass
        #  else:
            #  msg = colored(msg, 'bright blue', attrs=['bold'])
    print(msg, file=sys.stderr)

