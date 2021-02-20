#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple

def edit(
        action : List[str] = [],
        **kw : Any
    ) -> SuccessTuple:
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

def _complete_edit(
        action : List[str] = [],
        **kw : Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    options = {
        'config' : _complete_edit_config,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum.actions.shell import default_action_completer
    return default_action_completer(action=(['edit'] + action), **kw)

def _edit_stack(*args, **kw) -> SuccessTuple:
    from meerschaum.config.stack import edit_stack
    return edit_stack(*args, **kw)

def _edit_config(action : List[str] = [], **kw) -> SuccessTuple:
    """
    Edit Meerschaum configuration files.

    Specify a specific configuration key to edit.
    Defaults to editing `meerschaum` configuration (connectors, instance, etc.).

    Examples:
        ```
        ### Edit the main 'meerschaum' configuration.
        edit config

        ### Edit 'system' configuration.
        edit config system

        ### Create a new configuration file called 'myconfig'.
        edit config myconfig

        ```
    """
    from meerschaum.config._edit import edit_config
    if len(action) == 0:
        action.append('meerschaum')
    return edit_config(keys=action, **kw)

def _complete_edit_config(action : List[str] = [], **kw : Any):
    from meerschaum.config._read_config import get_possible_keys
    keys = get_possible_keys()
    if len(action) == 0:
        return keys
    possibilities = []
    for key in keys:
        if key.startswith(action[0]) and action[0] != key:
            possibilities.append(key)
    return possibilities

def _edit_grafana(*args, **kw) -> SuccessTuple:
    from meerschaum.config.stack.grafana import edit_grafana
    return edit_grafana(*args, **kw)

def _edit_pipes(
        action : List[str] = [],
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Open and edit pipes' configuration files.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import prompt
    from meerschaum.actions.shell.Shell import input_with_sigint
    from meerschaum.utils.misc import print_options
    _input = input_with_sigint(input)

    pipes = get_pipes(debug=debug, as_list=True, **kw)
    print_options(pipes, header=f'Pipes to be edited:')

    try:
        prompt("Press [Enter] to begin editing the above pipes or [CTRL-C] to cancel: ", icon=False)
    except KeyboardInterrupt:
        return False, f"No pipes changed."

    for p in pipes:
        text = _input(f"Press [Enter] to edit '{p}' or [CTRL-C] to skip: ")
        ### 
        if text != 'pass':
            p.edit(debug=debug, **kw)
    return (True, "Success")

def _edit_users(
        action : List[str] = [],
        repository : str = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit users' registration information.
    """
    from meerschaum.config import get_config
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal.User import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.prompt import prompt, yes_no, get_password, get_email
    from meerschaum.utils.misc import edit_file
    from meerschaum.config._paths import USERS_CACHE_RESOURCES_PATH
    from meerschaum.utils.yaml import yaml
    import os, pathlib
    repo_connector = parse_repo_keys(repository)

    def build_user(username : str):
        ### Change the password
        password = ''
        if yes_no(f"Change the password for user '{username}'?", default='n'):
            password = get_password(username, minimum_length=7)

        ## Make an admin
        _type = None
        if yes_no(f"Change the user type for user '{username}'?", default='n'):
            is_admin = yes_no(f"Make user '{username}' an admin?", default='n')
            _type = 'admin' if is_admin else None

        ### Change the email
        email = ''
        if yes_no(f"Change the email for user '{username}'?", default='n'):
            email = get_email(username)

        ### Change the attributes
        attributes = None
        if yes_no(f"Edit the attributes YAML file for user '{username}'?", default='n'):
            attr_path = pathlib.Path(os.path.join(USERS_CACHE_RESOURCES_PATH, f'{username}.yaml'))
            try:
                existing_attrs = repo_connector.get_user_attributes(User(username), debug=debug)
                with open(attr_path, 'w+') as f:
                    yaml.dump(existing_attrs, stream=f)
                edit_file(attr_path)
                with open(attr_path, 'r') as f:
                    attributes = yaml.load(f)
            except Exception as e:
                warn(
                    f"Unable to set attributes for user '{username}' due to exception:\n" + f"{e}" +
                    "\nSkipping attributes...",
                    stack = False
                )
                attributes = None

        ### Submit changes
        return User(username, password, email=email, type=_type, attributes=attributes)

    if len(action) == 0 or action == ['']:
        return False, "No users to edit."

    success = dict()
    for username in action:
        try:
            user = build_user(username)
        except Exception as e:
            print(e)
            info(f"Skipping editing user '{username}'...")
            continue
        info(f"Editing user '{user}' on Meerschaum instance '{repo_connector}'...")
        result_tuple = repo_connector.edit_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r: succeeded += 1
        else: failed += 1

    msg = (
        f"Finished editing {len(action)} users" + '\n' +
        f"    ({succeeded} succeeded, {failed} failed)."
    )
    info(msg)
    return True, msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
edit.__doc__ += _choices_docstring('edit')

