#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for patching the configuration dictionary
"""

import sys
import copy
from meerschaum.utils.typing import Dict, Any
from meerschaum.utils.warnings import warn

def apply_patch_to_config(
        config: Dict[str, Any],
        patch: Dict[str, Any],
    ) -> Dict[str, Any]:
    """Patch the config dict with a new dict (cascade patching)."""
    _base = copy.deepcopy(config) if isinstance(config, dict) else {}
    if not isinstance(patch, dict):
        return config

    def update_dict(base, patch):
        if base is None:
            return {}
        if not isinstance(base, dict):
            warn(f"Overwriting the value {base} with a dictionary:\n{patch}")
            base = {}
        for key, value in patch.items():
            if isinstance(value, dict):
                base[key] = update_dict(base.get(key, {}), value)
            else:
                base[key] = value
        return base

    return update_dict(_base, patch)


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
