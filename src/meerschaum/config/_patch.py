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
from meerschaum.config._default import resources_path
patch_filename = "patch.yaml"
patch_path = os.path.join(resources_path, patch_filename)
patch = None
if os.path.isfile(patch_path):
    patch = yaml.safe_load(
                pkg_resources.read_text('meerschaum.config.resources', patch_filename)
            )
def patch_config(
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
    if os.path.isfile(patch_path):
        os.remove(patch_path)
    with open(patch_path, 'w') as f:
        yaml.dump(patch, f)
