#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""

from __future__ import annotations
import meerschaum as mrsm 
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional, Dict

def edit(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Edit an existing element.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'config'    : _edit_config,
        'pipes'     : _edit_pipes,
        'definition': _edit_definition,
        'users'     : _edit_users,
        'plugins'   : _edit_plugins,
    }
    return choose_subaction(action, options, **kw)


def _complete_edit(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    options = {
        'config': _complete_edit_config,
        'plugin': _complete_edit_plugins,
        'plugins': _complete_edit_plugins,
    }

    if action is None:
        action = []

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
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

def _complete_edit_config(action: Optional[List[str]] = None, **kw: Any) -> List[str]:
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
        action: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        yes: bool = False,
        force: bool = False,
        noask: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Open and edit pipes' configuration files.
    
    If `fetch:definition` is specified, open a definition file (e.g. `.sql` for `sql` pipes).
    
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
    from meerschaum.utils.warnings import info
    from meerschaum.utils.formatting import pprint
    from meerschaum.config._patch import apply_patch_to_config

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
            if not (yes or force or noask):
                prompt(
                    f"Press [Enter] to begin editing the above {len(pipes)} pipe"
                    + ("s" if len(pipes) != 1 else "")
                    + " or [CTRL-C] to cancel:",
                    icon = False,
                )
        except KeyboardInterrupt:
            return False, f"No pipes changed."

    interactive = (not bool(params))
    success, msg = True, ""
    for pipe in pipes:
        try:
            if not (yes or force or noask):
                text = prompt(f"Press [Enter] to edit {pipe} or [CTRL-C] to skip:", icon=False)
                if text == 'pass':
                    continue
        except KeyboardInterrupt:
            continue

        if params:
            info(f"Will patch the following parameters into {pipe}:")
            pprint(params)
            pipe.parameters = apply_patch_to_config(pipe.parameters, params)

        edit_success, edit_msg = (
            pipe.edit_definition(debug=debug, **kw)
            if edit_definition
            else pipe.edit(interactive=interactive, debug=debug, **kw)
        )
        success = success and edit_success
        if not edit_success:
            msg += f"\n{pipe}: {edit_msg}"

    msg = "Success" if success else msg[1:]
    return success, msg


def _edit_definition(
    action: Optional[List[str]] = None,
    **kw
) -> SuccessTuple:
    """
    Edit pipes' definitions.
    Alias for `edit pipes definition`.
    """
    return _edit_pipes(['definition'], **kw)


def _edit_users(
    action: Optional[List[str]] = None,
    mrsm_instance: Optional[str] = None,
    yes: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Edit users' registration information.
    """
    from meerschaum.config import get_config
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.core.User import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.prompt import prompt, yes_no, get_password, get_email
    from meerschaum.utils.misc import edit_file
    from meerschaum.config._paths import USERS_CACHE_RESOURCES_PATH
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.yaml import yaml
    import os, pathlib
    instance_connector = parse_instance_keys(mrsm_instance)
    
    if action is None:
        action = []

    def build_user(username : str):
        ### Change the password
        password = ''
        if yes_no(f"Change the password for user '{username}'?", default='n', yes=yes, noask=noask):
            password = get_password(
                username,
                minimum_length = STATIC_CONFIG['users']['min_password_length'],
            )

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

    success = {}
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


def _edit_plugins(
    action: Optional[List[str]] = None,
    debug: bool = False,
    **kwargs: Any
):
    """
    Edit a plugin's source code.
    """
    import pathlib
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.packages import reload_meerschaum
    from meerschaum.actions import actions

    if not action:
        return False, "Specify which plugin to edit."

    for plugin_name in action:
        plugin = mrsm.Plugin(plugin_name)

        if not plugin.is_installed():
            warn(f"Plugin '{plugin_name}' is not installed.", stack=False)

            if not yes_no(
                f"Would you like to create a new plugin '{plugin_name}'?",
                **kwargs
            ):
                return False, f"Plugin '{plugin_name}' does not exist."

            actions['bootstrap'](
                ['plugins', plugin_name],
                debug = debug,
                **kwargs
            )
            continue

        plugin_file_path = pathlib.Path(plugin.__file__).resolve()

        try:
            _ = prompt(f"Press [Enter] to open '{plugin_file_path}':", icon=False)
        except (KeyboardInterrupt, Exception):
            continue

        edit_file(plugin_file_path)
        reload_meerschaum(debug=debug)

    return True, "Success"


def _complete_edit_plugins(
    action: Optional[List[str]] = None,
    line: Optional[str] = None,
    **kw: Any
) -> List[str]:
    from meerschaum.plugins import get_plugins_names
    plugins_names = get_plugins_names(try_import=False)
    if not action:
        return plugins_names

    last_word = action[-1]
    if last_word in plugins_names and (line or '')[-1] == ' ':
        return [
            plugin_name
            for plugin_name in plugins_names
            if plugin_name not in action
        ]

    possibilities = []
    for plugin_name in plugins_names:
        if (
            plugin_name.startswith(last_word)
            and plugin_name not in action
        ):
            possibilities.append(plugin_name)
    return possibilities


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
edit.__doc__ += _choices_docstring('edit')
