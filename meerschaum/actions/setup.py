#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Set up Meerschaum plugins.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional

def setup(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Run the `setup()` function for Meerschaum plugins.

    Usage:
        setup plugins noaa
    """
    from meerschaum.actions import choose_subaction
    options = {
        'plugins'  : _setup_plugins,
    }
    return choose_subaction(action, options, **kw)


def _complete_setup(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []
    options = {
        'plugin' : _complete_setup_plugins,
        'plugins' : _complete_setup_plugins,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['setup'] + action), **kw)


def _setup_plugins(
        action: Optional[List[str]] = None,
        repository: Optional[str] = None,
        **kw
    ) -> SuccessTuple:
    """
    Run the `setup()` function for Meerschaum plugins.

    Example:
        `setup plugins noaa covid`
    """
    from meerschaum.core import Plugin
    from meerschaum.utils.warnings import warn
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.plugins import get_plugins_names
    repo_connector = parse_repo_keys(repository)

    success_count = 0
    fail_count = 0

    plugins_names = action or get_plugins_names()

    for plugin_name in plugins_names:
        plugin = Plugin(plugin_name, repo_connector=repo_connector)
        if not plugin.is_installed():
            warn(f"Plugin '{plugin}' is not installed. Skipping...", stack=False)
            continue
        _st = plugin.setup(**kw)
        if isinstance(_st, tuple):
            success = _st[0]
            if not success:
                print_tuple(_st)
        elif isinstance(_st, bool):
            success = _st
        else:
            success = False
        if not success:
            warn(f"Failed to set up plugin '{plugin}'.", stack=False)
            fail_count += 1
        else:
            success_count += 1

    success = success_count > 0
    msg = (
        f"Completed setting up {success_count + fail_count} plugins\n    "
        + f"({success_count} succeeded, {fail_count} failed)."
    )
    return success, msg


def _complete_setup_plugins(*args, **kw) -> List[str]:
    from meerschaum.actions.uninstall import _complete_uninstall_plugins
    return _complete_uninstall_plugins(*args, **kw)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
setup.__doc__ += _choices_docstring('setup')
