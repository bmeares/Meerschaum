#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""

import sys
from meerschaum.utils.typing import Dict, Any

def apply_patch_to_config(
        config: Dict[str, Any],
        patch: Dict[str, Any],
    ):
    """Patch the config dict with a new dict (cascade patching)."""
    ### Weird threading issues: Must import class, not parent module.
    from meerschaum.utils.packages.cascadict import CascaDict
    base = CascaDict(config)
    new = base.cascade(patch)
    return new.copy_flat()


def write_patch(
        patch: Dict[str, Any],
        debug: bool = False
    ):
    """Write patch dict to yaml."""
    from meerschaum.utils.debug import dprint
    from meerschaum.config._paths import PATCH_DIR_PATH
    from meerschaum.config._edit import write_config
    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Writing configuration to {PATCH_DIR_PATH}:")
        pprint(patch, stream=sys.stderr)
    write_config(patch, directory=PATCH_DIR_PATH)
