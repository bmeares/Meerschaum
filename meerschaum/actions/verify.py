#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the states of pipes, pacakages, and more.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, SuccessTuple, Optional, List


def verify(
    action: Optional[List[str]] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Verify the states of pipes, packages, and more.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _verify_pipes,
        'packages': _verify_packages,
        'venvs': _verify_venvs,
        'plugins': _verify_plugins,
        'rowcounts': _verify_rowcounts,
    }
    return choose_subaction(action, options, **kwargs)


def _verify_pipes(**kwargs) -> SuccessTuple:
    """
    Verify the contents of pipes, syncing across their entire datetime intervals.
    """
    from meerschaum.actions.sync import _sync_pipes
    kwargs['verify'] = True
    return _sync_pipes(**kwargs)


def _verify_rowcounts(**kwargs) -> SuccessTuple:
    """
    Verify the contents of pipes, syncing across their entire datetime intervals.
    """
    from meerschaum.actions.sync import _sync_pipes
    kwargs['verify'] = True
    kwargs['check_rowcounts_only'] = True
    return _sync_pipes(**kwargs)


def _verify_packages(
    debug: bool = False,
    venv: Optional[str] = 'mrsm',
    **kw
) -> SuccessTuple:
    """
    Verify the versions of packages.
    """
    from meerschaum.utils.packages import (
        attempt_import, all_packages, is_installed, venv_contains_package,
        _monkey_patch_get_distribution, manually_import_module,
    )

    venv_packages, base_packages, miss_packages = [], [], []

    ### Verify the system dependencies.
    for import_name, install_name in all_packages.items():
        _where_list = (
            venv_packages if venv_contains_package(
                import_name,
                split=False,
                venv=venv,
                debug=debug,
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


def _verify_plugins(
    action: Optional[List[str]] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Verify that all of the available plugins are able to be imported as expected.
    """
    from meerschaum.utils.formatting import print_options, print_tuple
    from meerschaum.plugins import get_plugins_names, Plugin
    from meerschaum.utils.misc import items_str

    plugins_names_to_verify = action or get_plugins_names()
    if not plugins_names_to_verify:
        if not action:
            return True, "There are no installed plugins; nothing to do."
        return False, f"Unable to verify plugins {items_str(action)}."

    header = f"Verifying {len(plugins_names_to_verify)} plugins"
    print_options(
        plugins_names_to_verify,
        header = header,
    )

    failed_to_import = []
    for plugin_name in plugins_names_to_verify:
        plugin = Plugin(plugin_name)
        plugin_success = plugin.module is not None
        if not plugin_success:
            failed_to_import.append(plugin)
        plugin_msg = (
            (f"Imported plugin '{plugin}'.")
            if plugin_success
            else (f"Failed to import plugin '{plugin}'.")
        )
        print_tuple((plugin_success, plugin_msg), calm=True)

    success = len(failed_to_import) == 0
    message = (
        f"Successfully imported {len(plugins_names_to_verify)} plugins."
        if success
        else (
            "Failed to import plugin"
            + ('s' if len(failed_to_import) != 1 else '')
            + f" {items_str(failed_to_import)}."
        )
    )
    return success, message


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
verify.__doc__ += _choices_docstring('verify')
