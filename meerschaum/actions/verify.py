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
        'pipes': _verify_pipes,
        'packages': _verify_packages,
        'venvs': _verify_venvs,
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
        venv: Optional[str] = 'mrsm',
        **kw
    ) -> SuccessTuple:
    """
    Verify the versions of packages.
    """
    from meerschaum.utils.packages import (
        attempt_import, need_update, all_packages, is_installed, venv_contains_package,
        _monkey_patch_get_distribution, manually_import_module,
    )
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.debug import dprint

    venv_packages, base_packages, miss_packages = [], [], []

    ### Verify the system dependencies.
    for import_name, install_name in all_packages.items():
        _where_list = (
            venv_packages if venv_contains_package(
                import_name, split=False, venv=venv, debug=debug
            ) else (
                base_packages if is_installed(import_name, venv=None)
                else miss_packages
            )
        )
        _where_list.append(import_name)

    if 'flask_compress' in venv_packages or 'dash' in venv_packages:
        flask_compress = attempt_import('flask_compress', lazy=False, debug=debug)
        _monkey_patch_get_distribution('flask-compress', flask_compress.__version__)
        if 'flask_compress' in venv_packages:
            venv_packages.remove('flask_compress')

    for import_name in base_packages:
        manually_import_module(import_name, debug=debug, venv=None)
    for import_name in venv_packages:
        manually_import_module(import_name, debug=debug, venv=venv)

    ### Verify the plugins dependencies.
    return True, f"Verified {len(base_packages) + len(venv_packages)} packages."


def _verify_venvs(
        action: Optional[List[str]],
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Verify your virtual environments.
    """
    from meerschaum.utils.venv import get_venvs, verify_venv
    for venv in get_venvs():
        verify_venv(venv)
    return True, "Success"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
verify.__doc__ += _choices_docstring('verify')
