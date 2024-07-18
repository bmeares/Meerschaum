#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Uninstall plugins and pip packages.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional, Union

def uninstall(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Uninstall Meerschaum plugins or Python packages.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'plugins'  : _uninstall_plugins,
        'packages' : _uninstall_packages,
    }
    return choose_subaction(action, options, **kw)


def _complete_uninstall(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []
    options = {
        'plugin': _complete_uninstall_plugins,
        'plugins': _complete_uninstall_plugins,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['uninstall'] + action), **kw)


def _uninstall_plugins(
    action: Optional[List[str]] = None,
    repository: Optional[str] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Remove installed plugins. Does not affect repository registrations.
    """
    import meerschaum.actions
    from meerschaum.plugins import get_plugins_names, get_plugins_modules, reload_plugins
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.utils.prompt import yes_no
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.core import Plugin
    import os, shutil
    repo_connector = parse_repo_keys(repository)

    if action is None:
        action = []

    plugins_to_uninstall = []
    potential_plugins = [Plugin(pl, repo_connector=repo_connector) for pl in action]
    for plugin in potential_plugins:
        if force or plugin.is_installed():
            plugins_to_uninstall.append(plugin)
        else:
            warn(f"Plugin '{plugin}' is not installed. Add `--force` to try anyway.", stack=False)

    if not plugins_to_uninstall:
        return False, "No plugins to uninstall."

    ### verify that the user absolutely wants to do this (skips on --force)
    question = "Are you sure you want to remove these plugins?\n"
    for plugin in plugins_to_uninstall:
        question += f" - {plugin}" + "\n"
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', yes=yes, noask=noask)
    if not answer:
        return False, "No plugins were uninstalled."

    success_count, fail_count = 0, 0
    for plugin in plugins_to_uninstall:
        success, msg = plugin.uninstall(debug=debug)
        if not success:
            warn(msg, stack=False)
            fail_count += 1
        else:
            success_count += 1

    success = success_count > 0
    msg = (
        f"Done uninstalling {success_count + fail_count} plugins\n    "
        + f"({success_count} succeeded, {fail_count} failed)."
    )
    return success, msg


    ### delete the folders or files
    for name, m in modules_to_delete.items():
        ### __init__.py might be missing
        if m.__file__ is None:
            try:
                shutil.rmtree(os.path.join(PLUGINS_RESOURCES_PATH, name))
            except Exception as e:
                return False, str(e)
            continue
        try:
            if '__init__.py' in m.__file__:
                shutil.rmtree(m.__file__.replace('__init__.py', ''))
            else:
                os.remove(m.__file__)
        except Exception as e:
            return False, f"Could not remove plugin '{name}'."

    reload_plugins(debug=debug)
    return True, "Success"


def _complete_uninstall_plugins(action: Optional[List[str]] = None, **kw) -> List[str]:
    from meerschaum.actions.edit import _complete_edit_plugins
    return _complete_edit_plugins(action=action, **kw)


def _uninstall_packages(
    action: Optional[List[str]] = None,
    sub_args: Optional[List[str]] = None,
    venv: Optional[str] = 'mrsm',
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Uninstall PyPI packages from the Meerschaum virtual environment.
    
    Example:
        `uninstall packages pandas numpy`
    """
    if not action:
        return False, f"No packages to uninstall."

    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import pip_uninstall
    from meerschaum.utils.misc import items_str

    if not (yes or force) and noask:
        return False, "Skipping uninstallation. Add `-y` or `-f` to agree to the uninstall prompt."

    if pip_uninstall(
        *action,
        args = sub_args + (['-y'] if (yes or force) else []),
        venv = venv,
        debug = debug,
    ):
        return True, (
            f"Successfully removed packages from virtual environment '{venv}':\n"
            + items_str(action)
        )

    return False, f"Failed to uninstall packages:\n" + ', '.join(action)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
uninstall.__doc__ += _choices_docstring('uninstall')
