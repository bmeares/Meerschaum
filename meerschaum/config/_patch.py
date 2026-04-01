#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""

import sys
import copy
from meerschaum.utils.typing import Dict, Any
from meerschaum.utils.warnings import warn as _warn

def apply_patch_to_config(
    config: Dict[str, Any],
    patch: Dict[str, Any],
    warn: bool = False,
) -> Dict[str, Any]:
    """Patch the config dict with a new dict (cascade patching)."""
    if not isinstance(patch, dict) or not patch:
        return config
    if not isinstance(config, dict) or not config:
        return copy.deepcopy(patch)

    base = config.copy()

    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = apply_patch_to_config(base[key], value, warn=warn)
        elif isinstance(value, dict):
            if warn and key in base:
                _warn(f"Overwriting the value {base[key]} with a dictionary:\n{value}")
            base[key] = copy.deepcopy(value)
        else:
            base[key] = value

    return base


def write_patch(
    patch: Dict[str, Any],
    debug: bool = False
) -> None:
    """Write patch dict to yaml."""
    from meerschaum.utils.debug import dprint
    from meerschaum.config._paths import PATCH_DIR_PATH
    from meerschaum.config._edit import write_config
    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Writing configuration to {PATCH_DIR_PATH}:")
        pprint(patch, stream=sys.stderr)
    write_config(patch, directory=PATCH_DIR_PATH)
