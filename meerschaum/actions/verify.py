#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the states of pipes, pacakages, and more.
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def verify(
        action: Optional[List[str]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Verify the states of pipes, packages, and more.
    """
    from meerschaum.utils.misc import choose_subaction
    if action is None:
        action = []
    options = {
        'pipes' : _verify_pipes,
        'packages' : _verify_packages,
    }
    return choose_subaction(action, options, **kw)

def _verify_pipes(**kw) -> SuccessTuple:
    """
    Verify the contents of pipes.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum import get_pipes
    pipes = get_pipes(as_list=True, **kw)
    for pipe in pipes:
        info(f"Verifying the contents of {pipe}.")
        #  success, msg = pipe.verify()
    return False, "Not implemented."


def _verify_packages(
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Verify the versions of packages.
    """
    from meerschaum.utils.packages import (
        attempt_import, need_update, all_packages, is_installed, venv_contains_package,
    )
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.debug import dprint

    venv_packages, base_packages, miss_packages = [], [], []

    ### Verify the system dependencies.
    for import_name, install_name in all_packages.items():
        _where_list = (
            venv_packages if venv_contains_package(
                import_name, split=False, venv='mrsm', debug=debug
            ) else (
                base_packages if is_installed(import_name, venv=None)
                else miss_packages
            )
        )
        _where_list.append(import_name)

    attempt_import(
        *(base_packages + venv_packages),
        split=False, venv='mrsm', check_update=True, lazy=False, debug=debug,
    )


    ### Verify the plugins dependencies.
    return True, f"Verified {len(base_packages) + len(venv_packages)} packages."

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
verify.__doc__ += _choices_docstring('verify')
