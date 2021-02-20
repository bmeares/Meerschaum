#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for deleting elements
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, Union, Optional, List

def delete(
        action : List[str] = [],
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

def _delete_pipes(
        debug : bool = False,
        yes : bool = False,
        force : bool = False,
        **kw
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
    if not yes and not force:
        answer = yes_no(question, default='n')
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
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete configuration files.
    """
    import os, shutil
    from meerschaum.utils.prompt import yes_no
    from meerschaum.config._paths import CONFIG_DIR_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_DIR_PATH
    from meerschaum.utils.debug import dprint
    paths = [CONFIG_DIR_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_DIR_PATH]
    answer = False
    if not yes:
        sep = '\n' + '  - '
        answer = yes_no(f"Delete files and directories?{sep + sep.join([str(p) for p in paths])}\n", default='n')

    if answer or force:
        for path in paths:
            if debug: dprint(f"Removing {path}...")
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
    else:
        msg = "Nothing deleted."
        if debug: dprint(msg)
        return False, msg
    
    return True, "Successfully deleted configuration files"

def _delete_plugins(
        action : List[str] = [],
        yes : bool = False,
        force : bool = False,
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
    from meerschaum.utils.misc import reload_plugins
    from meerschaum.utils.prompt import yes_no
    import os, shutil

    ### parse the provided plugins and link them to their modules
    modules_to_delete = dict()
    for plugin in action:
        if plugin not in get_plugins_names(): info(f"Plugin '{plugin}' is not installed. Ignoring...")
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
    answer = force
    if not yes and not force:
        answer = yes_no(question, default='n')
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

    reload_plugins()
    return True, "Success"

def _delete_users(
        action : List[str] = [],
        repository : str = None,
        yes : bool = False,
        force : bool = False,
        shell : bool = False,
        debug : bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Delete plugins from a repository. Adequate permissions are required.
    """
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal.User import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    repo_connector = parse_repo_keys(repository)

    registered_users = []
    for username in action:
        user = User(username=username, password='')
        user_id = repo_connector.get_user_id(user, debug=debug)
        if user_id is None:
            info(f"User '{user}' does not exist. Skipping...")
            continue
        user.user_id = user_id
        registered_users.append(user)

    if len(registered_users) == 0:
        return False, "No users to delete."

    ### verify that the user absolutely wants to do this (skips on --force)
    question = f"Are you sure you want to delete these users from Meerschaum instance '{repo_connector}'?\n"
    for username in registered_users:
        question += f" - {username}" + "\n"
    answer = force
    if not yes and not force:
        answer = yes_no(question, default='n')
    if not answer:
        return False, "No users deleted."

    success = dict()
    for user in registered_users:
        info(f"Deleting user '{user}' from Meerschaum repository '{repo_connector}'...")
        result_tuple = repo_connector.delete_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r: succeeded += 1
        else: failed += 1

    msg = (
        f"Finished deleting {len(action)} users." + '\n' +
        f"    ({succeeded} succeeded, {failed} failed)"
    )
    if shell: info(msg)
    return True, msg

def _delete_connectors(
        connector_keys : List[str] = [],
        yes : bool = False,
        force : bool = False,
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
    from meerschaum.config import _config; cf = _config()
    from meerschaum.config._edit import write_config
    from meerschaum.utils.warnings import info, warn
    import os

    if len(connector_keys) == 0:
        return False, "No connector keys provided. Run again with `-c` to list connector keys."

    to_delete = []
    for ck in connector_keys:
        try:
            conn = parse_connector_keys(ck, debug=debug)
        except:
            warn(f"Could not parse connector '{ck}'. Skipping...", stack=False)
            continue

        if not force:
            if yes or not yes_no(
                f"Are you sure you want to delete connector '{conn}' from the configuration file?",
                default='n'
            ):
                info(f"Skipping connector '{conn}'...")
                continue
        to_delete.append(conn)

    if len(to_delete) == 0:
        return False, "No changes made to the configuration file."
    for c in to_delete:
        try:
            if c.flavor == 'sqlite':
                if force or yes_no(f"Detected sqlite database '{c.database}'. Delete this file?", default='n'):
                    try:
                        os.remove(c.database)
                    except:
                        warn(f"Failed to delete database file for connector '{c}'. Ignoring...", stack=False)
        except:
            pass
        try:
            del cf['meerschaum']['connectors'][c.type][c.label]
        except:
            warn(f"Failed to delete connector '{c}' from configuration. Skipping...")

    write_config(cf, debug=debug)
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
delete.__doc__ += _choices_docstring('delete')
