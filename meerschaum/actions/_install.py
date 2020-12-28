#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Install plugins
"""

def install(
        action : list = [''],
        repository : str = None,
        debug : bool = False,
        **kw
    ):
    """
    Install a plugin.

    By default, install from the main Meerschaum repository (mrsm.io).
    Use a private repository by specifying the API label after the plugin.
    NOTE: the --instance flag is ignored!

    Usage:
        install [plugin] {API label}

    Examples:
        install apex
        install apex --repo mrsm  (mrsm is the default instance)
        install apex --repo mycustominstance
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum.utils.misc import parse_repo_keys, reload_package
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
