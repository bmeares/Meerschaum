#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Install plugins and pip packages.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional

def install(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Install Meerschaum plugins or Python packages.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'plugins'  : _install_plugins,
        'packages' : _install_packages,
    }
    return choose_subaction(action, options, **kw)

def _complete_install(
        action : Optional[List[str]] = None,
        **kw : Any
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
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum.actions.shell import default_action_completer
    return default_action_completer(action=(['install'] + action), **kw)

def _install_plugins(
        action : Optional[List[str]] = None,
        repository : Optional[str] = None,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Install a plugin.

    By default, install from the main Meerschaum repository (mrsm.io).
    Use a private repository by specifying the API label after the plugin.
    NOTE: the --instance flag is ignored!

    Usage:
        install plugins [plugin]

    Examples:
        install plugins noaa
        install plugins noaa --repo mrsm  (mrsm is the default instance)
        install plugins noaa --repo mycustominstance
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum.actions.plugins import reload_plugins
    from meerschaum.connectors.parse import parse_repo_keys
    import meerschaum.actions
    from meerschaum.utils.formatting import print_tuple
    from meerschaum._internal import Plugin
    from meerschaum.connectors.api import APIConnector

    if not action:
        return False, "No plugins to install."

    repo_connector = parse_repo_keys(repository)

    successes = dict()
    for name in action:
        info(f"Installing plugin '{name}' from Meerschaum repository '{repo_connector}'...")
        success, msg = repo_connector.install_plugin(name, force=force, debug=debug)
        successes[name] = (success, msg)
        print_tuple((success, msg))

    reload_plugins(debug=debug)
    return True, "Success"

def _complete_install_plugins(
        action : Optional[List[str]] = None,
        repository : Optional[str] = None,
        **kw : Any
    ) -> List[str]:
    """
    Search for plugins to autocomplete command line text.
    """
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

def _install_packages(
        action : Optional[List[str]] = None,
        sub_args : Optional[List[str]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Install PyPI packages into the Meerschaum virtual environment.

    Example:
        `install packages pandas numpy`
    """
    if not action:
        return False, f"No packages to install"
    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import pip_install
    if pip_install(*action, args=['--upgrade'] + sub_args, debug=debug):
        return True, f"Successfully installed packages to virtual environment 'mrsm':\n{action}"
    return False, f"Failed to install packages:\n{action}"

def _complete_install_packages(
        **kw : Any
    ) -> List[str]:
    return []


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
install.__doc__ += _choices_docstring('install')
