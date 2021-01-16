#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Install plugins
"""

def install(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Install Meerschaum plugins or Python packages
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'plugins'  : _install_plugins,
        'packages' : _install_packages,
    }
    return choose_subaction(action, options, **kw)

def _install_plugins(
        action : list = [],
        repository : str = None,
        debug : bool = None,
        **kw
    ) -> tuple:
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
    from meerschaum.utils.packages import reload_package
    from meerschaum.utils.misc import parse_repo_keys
    import meerschaum.actions
    from meerschaum.utils.formatting import print_tuple
    from meerschaum import Plugin
    from meerschaum.connectors.api import APIConnector

    if action == [''] or len(action) == 0: return False, "No plugins to install"

    repo_connector = parse_repo_keys(repository)

    successes = dict()
    for name in action:
        info(f"Installing plugin '{name}' from Meerschaum repository '{repo_connector}'")
        success, msg = repo_connector.install_plugin(name, debug=debug)
        successes[name] = (success, msg)
        print_tuple((success, msg))

    reload_package(meerschaum.actions)
    return True, "Success"

def _install_packages(
        action : list = [],
        debug : bool = False,
        **kw
    ) -> tuple:
    if len(action) == 0: return False, f"No packages to install"
    from meerschaum.utils.warnings import info
    from meerschaum.config._paths import MRSM_VIRTENV_PATH
    import sys
    if str(MRSM_VIRTENV_PATH) not in sys.path:
        sys.path.insert(1, str(MRSM_VIRTENV_PATH))

    info(f"Will install the following plugins to '{str(MRSM_VIRTENV_PATH)}':\n{action}")

    from meerschaum.utils.packages import pip_install
    if pip_install(action, debug=debug):
        return True, f"Successfully installed packages to virtual environment 'mrsm':\n{action}"
    return False, f"Failed to install packages:\n{action}"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
install.__doc__ += _choices_docstring('install')

