#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""


def show(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Show elements of a certain type.
    
    Command:
        `show {option}`

    Example:
        `show pipes`
    """
    
    from meerschaum.utils.misc import choose_subaction
    show_options = {
        'actions'    : _show_actions,
        'pipes'      : _show_pipes,
        'config'     : _show_config,
        'modules'    : _show_modules,
        'version'    : _show_version,
        'connectors' : _show_connectors,
        'arguments'  : _show_arguments,
        'data'       : _show_data,
        'plugins'    : _show_plugins,
        'help'       : _show_help,
        'users'      : _show_users,
    }
    return choose_subaction(action, show_options, **kw)

def _show_actions(**kw) -> tuple:
    """
    Show available actions
    """
    from meerschaum.actions import actions
    from meerschaum.utils.misc import print_options
    print_options(options=actions, name='actions', **kw)
    return True, "Success"

def _show_help(**kw) -> tuple:
    """
    Print the --help menu from argparse
    """
    from meerschaum.actions.arguments._parser import parser
    print(parser.format_help())
    return True, "Success"

def _show_config(
        action : list = [''],
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Show the configuration dictionary.
    Sub-actions defined in the action list are index in the config dictionary.

    E.g. `show config pipes` -> cf['pipes']
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.config import get_config
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.debug import dprint
    if debug: dprint(f"Configuration loaded from {CONFIG_PATH}")

    keys = list(action)
    if keys == ['']: keys = []

    pprint(get_config(*keys))
    return (True, "Success")

def _show_modules(**kw) -> tuple:
    """
    Show the currently imported modules
    """
    import sys
    from meerschaum.utils.formatting import pprint
    pprint(list(sys.modules.keys()))
    return (True, "Success")

def _show_pipes(
        nopretty : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum import get_pipes
    from meerschaum.utils.misc import flatten_pipes_dict
    pipes = get_pipes(debug=debug, **kw)
    from meerschaum.utils.formatting import ANSI, pprint_pipes

    if len(flatten_pipes_dict(pipes)) == 1:
        return flatten_pipes_dict(pipes)[0].show(debug=debug, nopretty=nopretty, **kw)

    if not nopretty:
        pprint_pipes(pipes)
    else:
        pipes_list = flatten_pipes_dict(pipes)
        for p in pipes_list:
            print(p)

    return (True, "Success")

def _show_version(nopretty : bool = False, **kw) -> tuple:
    """
    Show the Meerschaum doc string
    """
    from meerschaum import __doc__ as doc, __version__ as version
    from meerschaum.utils.warnings import info
    _print = print
    if nopretty:
        msg = version
    else:
        msg = doc
        _print = info
    _print(msg)
    return (True, "Success")

def _show_connectors(
        action : list = [''],
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.connectors import connectors
    from meerschaum.config import config
    from meerschaum.utils.formatting import make_header
    from meerschaum.utils.formatting import pprint
    print(make_header("\nConfigured connectors:"))
    pprint(config['meerschaum']['connectors'])
    print(make_header("\nActive connectors:"))
    pprint(connectors)

    from meerschaum.utils.misc import parse_instance_keys
    if action != []:
        conn = parse_instance_keys(action[0], debug=debug)
        if conn:
            print(make_header("\n" + f"Attributes for connector '{conn}':"))
            pprint(conn.__dict__)

    return True, "Success"

def _show_arguments(
        **kw
    ) -> tuple:
    from meerschaum.utils.formatting import pprint
    pprint(kw)
    return True, "Success"

def _show_data(
        gui : bool = False,
        debug : bool = False,
        **kw
    ):
    import sys
    from meerschaum import get_pipes
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn, info
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    backtrack_minutes = 1440
    for p in pipes:
        try:
            df = p.get_backtrack_data(backtrack_minutes=backtrack_minutes, debug=debug)
        except:
            df = None
        if df is None:
            warn(f"Failed to fetch data for pipe '{p}'.", stack=False)
            continue
        info(f"Last {backtrack_minutes} minutes of data for Pipe '{p}'")
        print(df, file=sys.stderr)
        if gui:
            pandasgui = attempt_import('pandasgui')
            try:
                pandasgui.show(df)
            except:
                df.plot()
    return True, "Success"

def _show_plugins(
        action : list = [''],
        repository : str = 'api:mrsm',
        debug : bool = False,
        **kw
    ):
    from meerschaum.actions import _plugins_names
    from meerschaum.utils.misc import print_options
    from meerschaum.utils.misc import parse_repo_keys
    from meerschaum.utils.warnings import info
    from meerschaum import User
    repo_connector = parse_repo_keys(repository)

    if action == [''] or len(action) == 0:
        _to_print = _plugins_names
        header = "Installed plugins:"
        info("To see all installable plugins, run `show plugins all`")
        info("To see plugins created by a certain user, run `show plugins [username]`")
    elif action[0] in ('all'):
        _to_print = repo_connector.get_plugins(debug=debug)
        header = f"Available plugins from Meerschaum repository '{repo_connector}':"
    else:
        username = action[0]
        user_id = repo_connector.get_user_id(User(username, ''))
        _to_print = repo_connector.get_plugins(user_id=user_id, debug=debug)
        header = f"Plugins from user '{username}' at Meerschaum repository '{repo_connector}':"

    print()
    print_options(_to_print, header=header, debug=debug, **kw)
    print()

    return True, "Success"

def _show_users(
        repository : str = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.config import get_config
    from meerschaum.utils.misc import parse_repo_keys, print_options
    try:
        repo_connector = parse_repo_keys(repository)
        users_list = repo_connector.get_users(debug=debug)
    except:
        return False, f"Failed to get users from repository '{repository}'"
    print_options(users_list, header=f"Registered users for repository '{repo_connector}':")
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
show.__doc__ += _choices_docstring('show')

