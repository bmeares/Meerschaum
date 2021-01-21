#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""


def edit(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Edit an existing element.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'config'  : _edit_config,
        'pipes'   : _edit_pipes,
        'stack'   : _edit_stack,
        'grafana' : _edit_grafana,
        'users'   : _edit_users,
    }
    return choose_subaction(action, options, **kw)

def _edit_stack(*args, **kw):
    from meerschaum.config.stack import edit_stack
    return edit_stack(*args, **kw)

def _edit_config(*args, **kw):
    from meerschaum.config._edit import edit_config
    return edit_config(*args, **kw)

def _edit_grafana(*args, **kw):
    from meerschaum.config.stack.grafana import edit_grafana
    return edit_grafana(*args, **kw)

def _edit_pipes(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    from meerschaum import get_pipes
    from prompt_toolkit import prompt
    from meerschaum.actions.shell.Shell import input_with_sigint
    from meerschaum.utils.misc import print_options
    _input = input_with_sigint(input)

    pipes = get_pipes(debug=debug, as_list=True, **kw)
    print_options(pipes, header=f'Pipes to be edited:')

    try:
        prompt("Press [Enter] to begin editing the above pipes or [CTRL-C] to cancel: ")
    except KeyboardInterrupt:
        return False, f"No pipes changed."

    for p in pipes:
        text = _input(f"Press [Enter] to edit '{p}' or [CTRL-C] to skip: ")
        ### 
        if text != 'pass':
            p.edit(debug=debug, **kw)
    return (True, "Success")

def _edit_users(
        action : list = [''],
        repository : str = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.config import get_config
    from meerschaum import get_connector
    from meerschaum.utils.misc import parse_repo_keys
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    from prompt_toolkit import prompt
    repo_connector = parse_repo_keys(repository)

    if len(action) == 0 or action == ['']: return False, "No users to edit."

    success = dict()
    for username in action:
        password = prompt(f"Password for user '{username}': ")
        email = prompt(f"Email for user '{username}' (empty to omit): ")
        if len(email) == 0: email = None
        user = User(username, password, email=email)
        info(f"Editing user '{user}' on Meerschaum instance '{repo_connector}'...")
        result_tuple = repo_connector.edit_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r: succeeded += 1
        else: failed += 1

    msg = (
        f"Finished editing {len(action)} users." + '\n' +
        f"  {succeeded} succeeded, {failed} failed."
    )
    info(msg)
    return True, msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
edit.__doc__ += _choices_docstring('edit')

