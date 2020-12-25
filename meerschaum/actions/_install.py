#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Install plugins
"""

def install(
        action : list = [''],
        mrsm_instance : str = None,
        debug : bool = False,
        **kw
    ):
    """
    Install a plugin.

    By default, install from the main Meerschaum repository (mrsm.io).
    Use a private repository with `--mrsm-instance` or `-I`.

    Usage:
        install [plugin]

        install [plugin] -I api:mycustominstance
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import parse_instance_keys
    from meerschaum.utils.formatting import print_tuple
    from meerschaum import Plugin
    from meerschaum.connectors.api import APIConnector
    if mrsm_instance is None or str(mrsm_instance).split(':')[0] != 'api': mrsm_instance = 'api:mrsm'
    instance_connector = parse_instance_keys(mrsm_instance)

    if action == ['']: return False, "No plugins to install"

    successes = dict()
    for name in action:
        success, msg = instance_connector.install_plugin(name, debug=debug)
        successes[name] = (success, msg)
        print_tuple((success, msg + '\n'))


    return True, "Success"
