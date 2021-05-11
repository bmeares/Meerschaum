#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional

def edit(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit an existing element.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'config'  : _edit_config,
        'pipes'   : _edit_pipes,
        'users'   : _edit_users,
    }
    return choose_subaction(action, options, **kw)

def _complete_edit(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    options = {
        'config' : _complete_edit_config,
    }

    if action is None:
        action = []

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum.actions.shell import default_action_completer
    return default_action_completer(action=(['edit'] + action), **kw)

def _edit_config(action : Optional[List[str]] = None, **kw : Any) -> SuccessTuple:
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
    if action is None:
        action = []
    if len(action) == 0:
        action.append('meerschaum')
    return edit_config(keys=action, **kw)

def _complete_edit_config(action : Optional[List[str]] = None, **kw : Any) -> List[str]:
    from meerschaum.config._read_config import get_possible_keys
    keys = get_possible_keys()
    if not action:
        return keys
    possibilities = []
    for key in keys:
        if key.startswith(action[0]) and action[0] != key:
            possibilities.append(key)
    return possibilities

def _edit_pipes(
        action : Optional[List[str]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Open and edit pipes' configuration files.

    If 'definition' is specified, open a definition file (e.g. `.sql` for `sql` pipes).

    Usage:
        ```
        ### Edit all pipes.
        edit pipes

        ### Edit a SQL definition for the pipe `sql_main_mymetric`.
        edit pipes definition -c sql:main -m mymetric
        ```
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import prompt
    from meerschaum.utils.misc import print_options

    if action is None:
        action = []

    edit_definition = (len(action) > 0 and action[0] == 'definition')

    pipes = get_pipes(debug=debug, as_list=True, **kw)
    if pipes:
        print_options(pipes, header=f'Pipes to be edited:')
    else:
        return False, "No pipes to edit."

    if len(pipes) > 1:
        try:
            prompt(
                f"Press [Enter] to begin editing the above {len(pipes)} pipe" +
                ("s" if len(pipes) != 1 else "") +
                " or [CTRL-C] to cancel:", icon=False
            )
        except KeyboardInterrupt:
            return False, f"No pipes changed."

    for p in pipes:
        try:
            text = prompt(f"Press [Enter] to edit '{p}' or [CTRL-C] to skip:", icon=False)
            if text == 'pass':
                continue
        except KeyboardInterrupt:
            continue
        if edit_definition:
            p.edit_definition(debug=debug, **kw)
        else:
            p.edit(interactive=True, debug=debug, **kw)
    return (True, "Success")

def _edit_users(
        action : Optional[List[str]] = None,
        mrsm_instance : Optional[str] = None,
        yes : bool = False,
        noask : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit users' registration information.
    """
    from meerschaum.config import get_config
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_instance_keys
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
    instance_connector = parse_instance_keys(mrsm_instance)
    
    if action is None:
        action = []

    def build_user(username : str):
        ### Change the password
        password = ''
        if yes_no(f"Change the password for user '{username}'?", default='n', yes=yes, noask=noask):
            password = get_password(username, minimum_length=7)

        ## Make an admin
        _type = ''
        if yes_no(f"Change the user type for user '{username}'?", default='n', yes=yes, noask=noask):
            is_admin = yes_no(
                f"Make user '{username}' an admin?", default='n', yes=yes, noask=noask
            )
            _type = 'admin' if is_admin else None

        ### Change the email
        email = ''
        if yes_no(f"Change the email for user '{username}'?", default='n', yes=yes, noask=noask):
            email = get_email(username)

        ### Change the attributes
        attributes = None
        if yes_no(
                f"Edit the attributes YAML file for user '{username}'?",
                default='n', yes=yes, noask=noask
        ):
            attr_path = pathlib.Path(os.path.join(USERS_CACHE_RESOURCES_PATH, f'{username}.yaml'))
            try:
                existing_attrs = instance_connector.get_user_attributes(User(username), debug=debug)
                with open(attr_path, 'w+') as f:
                    yaml.dump(existing_attrs, stream=f, sort_keys=False)
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

    if not action:
        return False, "No users to edit."

    success = dict()
    for username in action:
        try:
            user = build_user(username)
        except Exception as e:
            print(e)
            info(f"Skipping editing user '{username}'...")
            continue
        info(f"Editing user '{user}' on Meerschaum instance '{instance_connector}'...")
        result_tuple = instance_connector.edit_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r:
            succeeded += 1
        else:
            failed += 1

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
