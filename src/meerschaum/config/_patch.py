#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

import os, yaml
from meerschaum.config._paths import PATCH_FILENAME, PATCH_PATH
patch_config = None
if os.path.isfile(PATCH_PATH):
    patch = yaml.safe_load(
        pkg_resources.read_text('meerschaum.config.resources', PATCH_FILENAME)
    )
def apply_patch_to_config(
        config : dict,
        patch : dict
    ):
    """
    Patch the config dict with a new dict (cascade patching)
    """
    from cascadict import CascaDict
    base = CascaDict(config)
    new = base.cascade(patch)
    return new.copy_flat()

def write_patch(
        patch : dict
    ):
    """
    Write patch dict to yaml
    """
    if os.path.isfile(PATCH_PATH):
        os.remove(PATCH_PATH)
    with open(PATCH_PATH, 'w') as f:
        yaml.dump(patch, f)
