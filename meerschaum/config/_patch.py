#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""
import os
try:
    import yaml
except ImportError:
    yaml = None
from meerschaum.config._paths import PATCH_FILENAME, PATCH_PATH, PERMANENT_PATCH_PATH
patch_config = None
if os.path.isfile(PATCH_PATH):
    if yaml:
        with open(PATCH_PATH, 'r') as f:
            patch_text = f.read()
        patch_config = yaml.safe_load(patch_text)

from meerschaum.utils.misc import search_and_substitute_config
permanent_patch_config = None
if PERMANENT_PATCH_PATH.exists():
    if yaml:
        with open(PERMANENT_PATCH_PATH, 'r') as f:
            permanent_patch_text = f.read()
        permanent_patch_config = search_and_substitute_config(
            yaml.safe_load(permanent_patch_text)
        )
else:
    permanent_patch_config = None


def apply_patch_to_config(
        config : dict,
        patch : dict
    ):
    """
    Patch the config dict with a new dict (cascade patching)
    """
    from meerschaum.utils.packages import attempt_import
    cascadict = attempt_import("cascadict", warn=True, install=True)
    base = cascadict.CascaDict(config)
    new = base.cascade(patch)
    return new.copy_flat()

def write_patch(
        patch : dict,
        debug : bool = False
    ):
    """
    Write patch dict to yaml
    """
    if os.path.isfile(PATCH_PATH):
        if debug: print(f"Removing existing patch: {PATCH_PATH}", file=sys.stderr)
        os.remove(PATCH_PATH)
    if debug:
        from meerschaum.utils.formatting import pprint
        print(f"Writing configuration to {PATCH_PATH}:", file=sys.stderr)
        pprint(patch, stream=sys.stderr)
    if yaml:
        with open(PATCH_PATH, 'w') as f:
            yaml.dump(patch, f, sort_keys=False)
