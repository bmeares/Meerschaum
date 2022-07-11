#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""

def apply_patch_to_config(
        config: dict,
        patch: dict
    ):
    """Patch the config dict with a new dict (cascade patching)."""
    from meerschaum.utils.packages import cascadict
    base = cascadict.CascaDict(config)
    new = base.cascade(patch)
    return new.copy_flat()

def write_patch(
        patch: dict,
        debug: bool = False
    ):
    """Write patch dict to yaml."""
    from meerschaum.config._edit import write_config
    if debug:
        from meerschaum.utils.formatting import pprint
        print(f"Writing configuration to {PATCH_DIR_PATH}:", file=sys.stderr)
        pprint(patch, stream=sys.stderr)
    write_config(patch, directory=PATCH_DIR_PATH)
