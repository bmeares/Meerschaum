#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes. Requires the API to be running.
"""

def register(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Register new elements.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'     : _register_pipes,
        'metrics'   : _register_metrics,
        'locations' : _register_locations,
        'plugins'   : _register_plugins,
        'users'     : _register_users,
    }
    return choose_subaction(action, options, **kw)

def _register_pipes(
        connector_keys : list = [],
        metric_keys : list = [],
        location_keys : list = [],
        params : dict = dict(),
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Create and register Pipe objects.
    Required: connector_keys and metric_keys. If location_keys is empty, assume [None]
    """
    from meerschaum import get_pipes, get_connector
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info

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
        if debug: dprint(f"Registering pipe '{p}'...")
        ss, msg = p.register(debug=debug)
        if not ss:
            warn(f"{msg}", stack=False)
            success = False
            failed_message += f"{p}, "

    if len(failed_message) > 0:
        message = "Failed to register pipes: " + failed_message[:(-1 * len(', '))]

    return success, message


def _register_metrics(**kw):
    pass

def _register_locations(**kw):
    pass

def _register_plugins(
        action : list = [],
        repository : str = None,
        shell : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import parse_repo_keys, reload_plugins
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal import Plugin
    from meerschaum.connectors.api import APIConnector
    from meerschaum import get_connector
    from meerschaum.utils.formatting import print_tuple

    reload_plugins(debug=debug)

    repo_connector = parse_repo_keys(repository)
    if not isinstance(repo_connector, APIConnector):
        return False, f"Can only upload plugins to the Meerschaum API. Connector '{repo_connector}' is of type '{repo_connector.type}'"

    if len(action) == 0 or action == ['']: return False, "No plugins to register"

    plugins_to_register = dict()
    from meerschaum.actions import _plugins_names
    for p in action:
        if p not in _plugins_names:
            warn(f"Plugin '{p}' is not installed and cannot be registered. Ignoring...", stack=False)
        else:
            plugins_to_register[p] = Plugin(p)

    successes = dict()

    for name, plugin in plugins_to_register.items():
        info(f"Registering plugin '{plugin}' to Meerschaum API '{repo_connector}'..." + '\n')
        success, msg = repo_connector.register_plugin(plugin, debug=debug)
        print_tuple((success, msg + '\n'))
        successes[name] = (success, msg)

    total_success, total_fail = 0, 0
    for p, tup in successes.items():
        if tup[0]: total_success += 1
        else: total_fail += 1

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint("Return values for each plugin:")
        pprint(successes)

    msg = (
        f"Finished registering {len(plugins_to_register)} plugins." + '\n' +
        f"    ({total_success} succeeded, {total_fail} failed)"
    )
    if shell: info(msg)
    reload_plugins(debug=debug)
    return True, msg

def _register_users(
        action : list = [],
        repository : str = None,
        shell : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.config import get_config
    from meerschaum import get_connector
    from meerschaum.utils.misc import parse_repo_keys, is_valid_email
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum._internal import User
    from meerschaum.connectors.api import APIConnector
    from meerschaum.utils.formatting import print_tuple
    from prompt_toolkit import prompt
    from getpass import getpass
    repo_connector = parse_repo_keys(repository)

    if len(action) == 0 or action == ['']: return False, "No users to register."

    ### filter out existing users
    nonregistered_users = []
    for username in action:
        user = User(username=username, password='')
        user_id = repo_connector.get_user_id(user, debug=debug)
        if user_id is not None:
            info(f"User '{user}' already exists. Skipping...")
            continue
        nonregistered_users.append(user)

    def get_password(username):
        while True:
            password = getpass(prompt=f"Password for user '{username}': ")
            _password = getpass(prompt=f"Confirm password for user '{username}': ")
            if password != _password:
                warn(f"Passwords do not match! Please try again.", stack=False)
                continue
            else:
                return password

    def get_email():
        while True:
            email = prompt(f"Email for user '{username}' (empty to omit): ")
            if email == '' or is_valid_email(email): return email
            else: warn(f"Invalid email! Please try again.", stack=False)

    ### prompt for passwords and emails, then try to register
    success = dict()
    successfully_registered_users = set()
    for _user in nonregistered_users:
        try:
            username = _user.username
            password = get_password(username)
            email = get_email()
        except:
            return False, f"Aborted registering users {', '.join([str(u) for u in nonregistered_users if u not in successfully_registered_users])}"
        if len(email) == 0: email = None
        user = User(username, password, email=email)
        info(f"Registering user '{user}' to Meerschaum repository '{repo_connector}'...")
        result_tuple = repo_connector.register_user(user, debug=debug)
        print_tuple(result_tuple)
        success[username] = result_tuple[0]
        if success[username]: successfully_registered_users.add(user)

    succeeded, failed = 0, 0
    for username, r in success.items():
        if r: succeeded += 1
        else: failed += 1

    msg = (
        f"Finished registering {succeeded + failed} users." + '\n' +
        f"  ({succeeded} succeeded, {failed} failed)"
    )
    if shell: info(msg)
    return True, msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
register.__doc__ += _choices_docstring('register')

