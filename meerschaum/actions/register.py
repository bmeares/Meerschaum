#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes. Requires the API to be running.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional

def register(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Register new items (pipes, plugins, users).

    Pipes and users reside on instances (`-i`), and plugins reside on repositories (`-r`).
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'     : _register_pipes,
        'plugins'   : _register_plugins,
        'users'     : _register_users,
    }
    return choose_subaction(action, options, **kw)

def _complete_register(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []
    options = {
        'plugin' : _complete_register_plugins,
        'plugins' : _complete_register_plugins,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['register'] + action), **kw)

def _register_pipes(
        connector_keys: Optional[List[str]] = None,
        metric_keys: Optional[List[str]] = None,
        location_keys: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Create and register Pipe objects.

    Required:
        `connector_keys` and `metric_keys`.
        If `location_keys` is empty, assume [`None`].
    """
    from meerschaum import get_pipes, get_connector
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    if params is None:
        params = {}

    if (
        len(connector_keys) == 0 or
        len(metric_keys) == 0
    ):
        warn(
            "You must provide connector keys (-c) and metrics (-m) to register pipes.\n\n" +
            "Run `bootstrap pipe` for an interactive guide that creates pipes.",
            stack = False
        )
        return False, "Missing connector keys or metrics"

    pipes = get_pipes(
        connector_keys = connector_keys,
        metric_keys = metric_keys,
        location_keys = location_keys,
        params = params,
        as_list = True,
        method = 'explicit',
        debug = debug,
        **kw
    )

    success, message = True, "Success"
    failed_message = ""
    for p in pipes:
        if debug:
            dprint(f"Registering {p}...")
        ss, msg = p.register(debug=debug)
        if not ss:
            warn(f"{msg}", stack=False)
            success = False
            failed_message += f"{p}, "

    if len(failed_message) > 0:
        message = "Failed to register pipes: " + failed_message[:(-1 * len(', '))]

    return success, message

def _register_plugins(
        action: Optional[List[str]] = None,
        repository: Optional[str] = None,
        shell: bool = False,
        debug: bool = False,
        yes: bool = False,
        noask: bool = False,
        force: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    from meerschaum.utils.debug import dprint
    from meerschaum.plugins import reload_plugins, get_plugins_names
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.core import Plugin
    from meerschaum import get_connector
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.prompt import prompt, yes_no

    if action is None:
        action = []

    reload_plugins(debug=debug)

    repo_connector = parse_repo_keys(repository)
    if repo_connector.__dict__.get('type', None) != 'api':
        return False, (
            f"Can only upload plugins to the Meerschaum API." +
            f"Connector '{repo_connector}' is of type " +
            f"'{repo_connector.get('type', type(repo_connector))}'."
        )

    if len(action) == 0 or action == ['']:
        return False, "No plugins to register."

    plugins_to_register = {}
    plugins_names = get_plugins_names()
    for p in action:
        if p not in plugins_names:
            warn(
                f"Plugin '{p}' is not installed and cannot be registered. Ignoring...",
                stack=False
            )
        else:
            plugins_to_register[p] = Plugin(p)

    successes = {}

    for name, plugin in plugins_to_register.items():
        desc = None
        plugin.attributes = repo_connector.get_plugin_attributes(plugin, debug=debug)
        if plugin.attributes is None:
            plugin.attributes = {}
        question = f"Would you like to add a description to plugin '{name}'?"
        if plugin.attributes.get('description', None):
            info(f"Found existing description for plugin '{plugin}':")
            print(plugin.attributes['description'])
            question = (
                "Would you like to overwrite this description?\n"
                + "To edit the existing text, visit /dash/plugins for this repository."
            )
        if not noask and not force and yes_no(
            question,
            default='n',
            yes=yes
        ):
            info('Press (Esc + Enter) to submit the description, (CTRL + C) to cancel.')
            try:
                desc = prompt('', multiline=True, icon=False)
            except KeyboardInterrupt:
                desc = None
            if desc == '':
                desc = None
        if desc is not None:
            plugin.attributes = {'description': desc}
        info(f"Registering plugin '{plugin}' to Meerschaum API '{repo_connector}'..." + '\n')
        success, msg = repo_connector.register_plugin(plugin, debug=debug)
        print_tuple((success, msg + '\n'))
        successes[name] = (success, msg)

    total_success, total_fail = 0, 0
    for p, tup in successes.items():
        if tup[0]:
            total_success += 1
        else:
            total_fail += 1

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint("Return values for each plugin:")
        pprint(successes)

    msg = (
        f"Finished registering {len(plugins_to_register)} plugins" + '\n' +
        f"    ({total_success} succeeded, {total_fail} failed)."
    )
    reload_plugins(debug=debug)
    return total_success > 0, msg

def _complete_register_plugins(*args, **kw):
    from meerschaum.actions.uninstall import _complete_uninstall_plugins
    return _complete_uninstall_plugins(*args, **kw)

def _register_users(
        action: Optional[List[str]] = None,
        mrsm_instance: Optional[str] = None,
        shell: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Register a new user to a Meerschaum instance.
    """
    from meerschaum.config import get_config
    from meerschaum.config.static import _static_config
    from meerschaum import get_connector
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.core import User
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.prompt import prompt, get_password, get_email
    if mrsm_instance is None:
        mrsm_instance = get_config('meerschaum', 'instance')
    instance_connector = parse_instance_keys(mrsm_instance)

    if not action:
        return False, "No users to register."

    ### filter out existing users
    nonregistered_users = []
    for username in action:
        min_len = _static_config()['users']['min_username_length']
        if len(username) < min_len:
            warn(
                f"Username '{username}' is too short (less than {min_len} characters). Skipping...",
                stack = False
            )
            continue
        user = User(username=username, instance=instance_connector)
        user_id = instance_connector.get_user_id(user, debug=debug)
        if user_id is not None:
            warn(f"User '{user}' already exists. Skipping...", stack=False)
            continue
        nonregistered_users.append(user)

    ### prompt for passwords and emails, then try to register
    success = dict()
    successfully_registered_users = set()
    for _user in nonregistered_users:
        try:
            username = _user.username
            password = get_password(
                username,
                minimum_length = _static_config()['users']['min_password_length']
            )
            email = get_email(username, allow_omit=True)
        except Exception as e:
            return False, (
                "Aborted registering users " +
                ', '.join(
                    [
                        str(u) for u in nonregistered_users
                            if u not in successfully_registered_users
                    ]
                )
            )
        if len(email) == 0:
            email = None
        user = User(username, password, email=email)
        info(f"Registering user '{user}' to Meerschaum instance '{instance_connector}'...")
        result_tuple = instance_connector.register_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]
        if success[username]:
            successfully_registered_users.add(user)

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r:
            succeeded += 1
        else:
            failed += 1

    msg = (
        f"Finished registering {succeeded + failed} users." + '\n' +
        f"  ({succeeded} succeeded, {failed} failed)"
    )
    return succeeded > 0, msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
register.__doc__ += _choices_docstring('register')
