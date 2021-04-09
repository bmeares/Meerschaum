#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for deleting elements
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, Union, Optional, List

def delete(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete an element.
    """
    from meerschaum.utils.misc import choose_subaction
    from meerschaum.utils.debug import dprint
    options = {
        'config'     : _delete_config, 
        'pipes'      : _delete_pipes,
        'plugins'    : _delete_plugins,
        'users'      : _delete_users,
        'connectors' : _delete_connectors,
    }
    return choose_subaction(action, options, **kw)

def _complete_delete(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []
    options = {
        'connectors': _complete_delete_connectors,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum.actions.shell import default_action_completer
    return default_action_completer(action=(['delete'] + action), **kw)

def _delete_pipes(
        debug : bool = False,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Drop pipes and delete their registrations.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.warnings import warn
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to delete."
    question = "Are you sure you want to delete these Pipes? THIS CANNOT BE UNDONE!\n"
    for p in pipes:
        question += f" - {p}" + "\n"
    answer = force
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)
    if not answer:
        return False, "No pipes deleted."

    successes, fails = 0, 0
    success_dict = {}

    for p in pipes:
        success_tuple = p.delete(debug=debug)
        success_dict[p] = success_tuple[1]
        if success_tuple[0]:
            successes += 1
        else:
            fails += 1
            warn(success_tuple[1], stack=False)

    msg = (
        f"Finished deleting {len(pipes)} pipes.\n" +
        f"    ({successes} succeeded, {fails} failed)"
    )
    
    return successes > 0, msg

def _delete_config(
        action : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete configuration files.
    """
    import os, shutil
    from meerschaum.utils.prompt import yes_no
    from meerschaum.config._paths import CONFIG_DIR_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_DIR_PATH
    from meerschaum.config._read_config import get_possible_keys, get_keyfile_path
    from meerschaum.utils.debug import dprint
    paths = [p for p in [STACK_COMPOSE_PATH, DEFAULT_CONFIG_DIR_PATH] if p.exists()]
    if action is None:
        action = []
    keys = get_possible_keys() if len(action) == 0 else action
    for k in keys:
        _p = get_keyfile_path(k, create_new=False)
        if _p is not None:
            paths.append(_p)

    if force or not paths:
        answer = True
    else:
        sep = '\n' + '  - '
        answer = yes_no(
            f"Are you sure you want to delete the following configuration files?" +
            f"{sep + sep.join([str(p) for p in paths])}\n",
            default='n', noask=noask, yes=yes
        )

    if answer or force:
        for path in paths:
            if debug:
                dprint(f"Removing {path}...")
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        success, msg = True, "Success"
    else:
        success, msg = False, "Nothing deleted."
    
    return success, msg

def _delete_plugins(
        action : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Remove installed plugins. Does not affect repository registrations.
    """
    import meerschaum.actions
    from meerschaum.actions.plugins import get_plugins_names, get_plugins_modules
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.actions.plugins import reload_plugins
    from meerschaum.utils.prompt import yes_no
    import os, shutil

    if action is None:
        action = []

    ### parse the provided plugins and link them to their modules
    modules_to_delete = dict()
    for plugin in action:
        if plugin not in get_plugins_names():
            info(f"Plugin '{plugin}' is not installed. Ignoring...")
        else:
            for m in get_plugins_modules():
                if plugin == m.__name__.split('.')[-1]:
                    modules_to_delete[plugin] = m
                    break
    if len(modules_to_delete) == 0:
        return False, "No plugins to delete."

    ### verify that the user absolutely wants to do this (skips on --force)
    question = "Are you sure you want to delete these plugins?\n"
    for plugin in modules_to_delete:
        question += f" - {plugin}" + "\n"
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', yes=yes, noask=noask)
    if not answer:
        return False, "No plugins deleted."

    ### delete the folders or files
    for name, m in modules_to_delete.items():
        ### __init__.py might be missing
        if m.__file__ is None:
            try:
                shutil.rmtree(os.path.join(PLUGINS_RESOURCES_PATH, name))
            except Exception as e:
                return False, str(e)
            continue
        try:
            if '__init__.py' in m.__file__:
                shutil.rmtree(m.__file__.replace('__init__.py', ''))
            else:
                os.remove(m.__file__)
        except Exception as e:
            return False, f"Could not remove plugin '{name}'"

    reload_plugins(debug=debug)
    return True, "Success"

def _delete_users(
        action : Optional[List[str]] = None,
        mrsm_instance : Optional[str] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        shell : bool = False,
        debug : bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Delete users from a Meerschaum instance. Adequate permissions are required.
    """
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal.User import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    instance_connector = parse_instance_keys(mrsm_instance)

    if action is None:
        action = []

    registered_users = []
    for username in action:
        user = User(username=username)
        user_id = instance_connector.get_user_id(user, debug=debug)
        if user_id is None:
            info(f"User '{user}' does not exist. Skipping...")
            continue
        user.user_id = user_id
        registered_users.append(user)

    if len(registered_users) == 0:
        return False, "No users to delete."

    ### verify that the user absolutely wants to do this (skips on --force)
    question = f"Are you sure you want to delete these users from Meerschaum instance '{instance_connector}'?\n"
    for username in registered_users:
        question += f" - {username}" + "\n"
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)
    if not answer:
        return False, "No users deleted."

    success = dict()
    for user in registered_users:
        info(f"Deleting user '{user}' from Meerschaum instance '{instance_connector}'...")
        result_tuple = instance_connector.delete_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r:
            succeeded += 1
        else:
            failed += 1

    msg = (
        f"Finished deleting {len(action)} users." + '\n' +
        f"    ({succeeded} succeeded, {failed} failed)"
    )
    return True, msg

def _delete_connectors(
        action : Optional[List[str]] = None,
        connector_keys : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete configured connectors.

    Example:
        `delete connectors -c sql:test`
    """
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.connectors.parse import parse_connector_keys
    from meerschaum.config import _config
    from meerschaum.config._edit import write_config
    from meerschaum.utils.warnings import info, warn
    import os
    cf = _config()
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    _keys = list(set(action + connector_keys))

    if not _keys:
        return False, "No connectors to delete."

    to_delete = []
    for ck in _keys:
        try:
            conn = parse_connector_keys(ck, debug=debug)
        except Exception as e:
            warn(f"Could not parse connector '{ck}'. Skipping...", stack=False)
            continue

        if not force:
            if not yes_no(
                f"Are you sure you want to delete connector '{conn}' from the configuration file?",
                default = 'n',
                yes = yes,
                noask = noask,
            ):
                info(f"Skipping connector '{conn}'...")
                continue
        to_delete.append(conn)

    if len(to_delete) == 0:
        return False, "No changes made to the configuration file."
    for c in to_delete:
        try:
            if c.flavor == 'sqlite':
                if force or yes_no(f"Detected sqlite database '{c.database}'. Delete this file?", default='n', noask=noask, yes=yes):
                    try:
                        os.remove(c.database)
                    except Exception as e:
                        warn(f"Failed to delete database file for connector '{c}'. Ignoring...", stack=False)
        except Exception as e:
            pass
        try:
            del cf['meerschaum']['connectors'][c.type][c.label]
        except Exception as e:
            warn(f"Failed to delete connector '{c}' from configuration. Skipping...", stack=False)

    write_config(cf, debug=debug)
    return True, "Success"

def _complete_delete_connectors(action : Optional[List[str]] = None, **kw : Any) -> List[str]:
    from meerschaum.config import get_config
    from meerschaum.utils.misc import get_connector_labels
    types = list(get_config('meerschaum', 'connectors').keys())
    search_term = action[-1] if action else ''
    return get_connector_labels(*types, search_term=search_term)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
delete.__doc__ += _choices_docstring('delete')
