#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Install plugins and pip packages.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional, Union

def install(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Install Meerschaum plugins or Python packages.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'plugins'  : _install_plugins,
        'packages' : _install_packages,
        'required' : _install_required,
    }
    return choose_subaction(action, options, **kw)


def _complete_install(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []
    options = {
        'plugin' : _complete_install_plugins,
        'plugins' : _complete_install_plugins,
        'package': _complete_install_packages,
        'packages': _complete_install_packages,
        'required': _complete_install_required,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['install'] + action), **kw)


def _install_plugins(
    action: Optional[List[str]] = None,
    repository: Optional[str] = None,
    skip_deps: bool = False,
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Install a plugin.
    
    By default, install from the main Meerschaum repository (mrsm.io).
    Use a private repository by specifying the API label after the plugin.
    NOTE: the --instance flag is ignored!
    
    Usage:
        install plugins [plugin]
    
    Examples:
        - install plugins noaa
        - install plugins noaa --repo mrsm  (mrsm is the default instance)
        - install plugins noaa --repo mycustominstance
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum.plugins import reload_plugins
    from meerschaum.connectors.parse import parse_repo_keys
    import meerschaum.actions
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.core import Plugin
    from meerschaum.connectors.api import APIConnector

    if not action:
        return False, "No plugins to install."

    repo_connector = parse_repo_keys(repository)

    for name in action:
        info(f"Installing plugin '{name}' from Meerschaum repository '{repo_connector}'...")
        success, msg = repo_connector.install_plugin(
            name,
            force = force,
            skip_deps = skip_deps,
            debug = debug,
        )
        print_tuple((success, msg))

    reload_plugins(debug=debug)
    return True, "Success"


def _complete_install_plugins(
    action: Optional[List[str]] = None,
    repository: Optional[str] = None,
    **kw: Any
) -> List[str]:
    """
    Search for plugins to autocomplete command line text.
    NOTE: Disabled for the time being so we don't interrupt the user typing.
    """
    return []
    from meerschaum.actions import get_shell
    if action is None:
        action = []
    if len(action) == 0:
        search_term = None
    else:
        search_term = action[-1]

    ### Don't start searching unless a key has been pressed.
    if search_term is None or len(search_term) == 0:
        return []

    ### In case we're using the Shell (which we have to in order for _complete to work),
    ### get the current repository.
    if repository is None:
        repository = get_shell().repo_keys

    from meerschaum.connectors.parse import parse_repo_keys
    try:
        repo_connector = parse_repo_keys(repository)
    except Exception as e:
        return []
    results = repo_connector.get_plugins(search_term=search_term)
    if len(results) == 1 and results[0] == search_term:
        return []
    return sorted(results)

class NoVenv:
    pass

def _install_packages(
    action: Optional[List[str]] = None,
    sub_args: Optional[List[str]] = None,
    venv: Union[str, NoVenv] = NoVenv,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Install PyPI packages into the Meerschaum virtual environment.
    
    Example:
        `install packages pandas numpy`
    """
    if not action:
        return False, f"No packages to install."
    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import pip_install
    from meerschaum.utils.misc import items_str
    if venv is NoVenv:
        venv = 'mrsm'

    if pip_install(
        *action,
        args = ['--upgrade'] + sub_args,
        venv = venv,
        debug = debug,
    ):
        return True, (
            "Successfully installed package" + ("s" if len(action) != 1 else '')
            + f" {items_str(action)}"
            + f" into the virtual environment '{venv}'."
        )
    return False, (
        "Failed to install package" + ("s" if len(action) != 1 else '') + f" {items_str(action)}."
    )


def _complete_install_packages(**kw : Any) -> List[str]:
    return []


def _install_required(
    action: Optional[List[str]] = None,
    repository: Optional[str] = None,
    force: bool = False,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Install the required packages for Meerschaum plugins.
    Each plugin's packages will be installed into its virtual environment.

    Example:
        `install required noaa covid`
    """
    from meerschaum.core import Plugin
    from meerschaum.utils.warnings import warn, info
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.plugins import get_plugins_names
    repo_connector = parse_repo_keys(repository)

    plugins_names = action or get_plugins_names()

    success_count = 0
    fail_count = 0

    for plugin_name in plugins_names:
        plugin = Plugin(plugin_name, repo_connector=repo_connector)
        if not plugin.is_installed():
            warn(f"Plugin '{plugin}' is not installed. Skipping...", stack=False)
            continue
        info(f"Installing required packages for plugin '{plugin}'...")
        success = plugin.install_dependencies(force=force, debug=debug)
        if not success:
            warn(f"Failed to install required packages for plugin '{plugin}'.", stack=False)
            fail_count += 1
        else:
            success_count += 1

    success = fail_count == 0
    msg = (
        f"Installed packages for {success_count + fail_count} plugins\n    "
        + f"({success_count} succeeded, {fail_count} failed)."
    )
    return success, msg


def _complete_install_required(*args, **kw) -> List[str]:
    from meerschaum.actions.uninstall import _complete_uninstall_plugins
    return _complete_uninstall_plugins(*args, **kw)


def _install_systemd(
    action: Optional[List[str]] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Install the Meerschaum job monitor as a systemd service.
    """
    import sys


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
install.__doc__ += _choices_docstring('install')
