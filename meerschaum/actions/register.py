#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes. Requires the API to be running.
"""

from __future__ import annotations
import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional, Dict

def register(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Register new items (pipes, plugins, users).

    Pipes and users reside on instances (`-i`), and plugins reside on repositories (`-r`).
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'     : _register_pipes,
        'plugins'   : _register_plugins,
        'users'     : _register_users,
        'connectors': _register_connectors,
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
        'plugin': _complete_register_plugins,
        'plugins': _complete_register_plugins,
        'connector': _complete_register_connectors,
        'connectors': _complete_register_connectors,
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
    tags: Optional[List[str]] = None,
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
    from meerschaum.utils.misc import items_str
    from meerschaum.config._patch import apply_patch_to_config

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    if params is None:
        params = {}
    if tags:
        params['tags'] = tags

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
        tags = tags,
        as_list = True,
        method = 'explicit',
        debug = debug,
        **kw
    )

    if params:
        for pipe in pipes:
            pipe._attributes['parameters'] = apply_patch_to_config(
                params,
                pipe._attributes.get('parameters', {})
            )

    success, message = True, "Success"
    failed_pipes = []
    success_pipes = []
    for p in pipes:
        if debug:
            dprint(f"Registering {p}...")
        ss, msg = p.register(debug=debug)
        if not ss:
            warn(f"{msg}", stack=False)
            failed_pipes.append(p)
        else:
            success_pipes.append(p)

    message = ""
    if success_pipes:
        message += (
            f"Successfully registered {len(success_pipes)} pipe"
            + ('s' if len(success_pipes) != 1 else '') + "."
        )
        if failed_pipes:
            message += "\n"

    if failed_pipes:
        message += (
            f"Failed to register {len(failed_pipes)} pipe"
            + ('s' if len(failed_pipes) != 1 else '') + "."
        )

    return len(success_pipes) > 0, message


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
    """
    Upload plugins to an API instance (repository).
    """
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
        plugin.attributes = repo_connector.get_plugin_attributes(plugin, debug=debug)
        if plugin.attributes is None:
            plugin.attributes = {}

        try:
            description_text = plugin.attributes.get('description', '')
            doc_text = plugin.module.__doc__.lstrip().rstrip()
        except Exception:
            description_text = ''
            doc_text = ''

        desc = description_text or doc_text or ''

        question = f"Would you like to add a description to plugin '{name}'?"
        if desc:
            info(f"Found existing description for plugin '{plugin}':")
            print(desc)
            question = (
                "Would you like to overwrite this description?\n"
                + "To edit the existing text, visit /dash/plugins for this repository."
            )
        if not noask and not force and yes_no(
            question,
            default='n',
            yes=yes
        ):
            info('Press (Esc + Enter) to submit, (CTRL + C) to cancel.')
            try:
                desc = prompt(
                    '',
                    multiline=True,
                    icon=False,
                    default_editable=desc.lstrip().rstrip(),
                )
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
    from meerschaum.config.static import STATIC_CONFIG
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
        min_len = STATIC_CONFIG['users']['min_username_length']
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
    success = {}
    successfully_registered_users = set()
    for _user in nonregistered_users:
        try:
            username = _user.username
            password = get_password(
                username,
                minimum_length = STATIC_CONFIG['users']['min_password_length']
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
        f"    ({succeeded} succeeded, {failed} failed)"
    )
    return succeeded > 0, msg


def _register_connectors(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Create new connectors programmatically with `--params`.
    See `bootstrap connector`.

    Examples:

    mrsm register connector sql:tmp --params 'uri:sqlite:////tmp/tmp.db'

    mrsm register connector -c sql:new --params '{"database": "/tmp/new.db"}'
    """
    from meerschaum.config import get_config, write_config
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn
    all_keys = (action or []) + (connector_keys or [])
    if len(all_keys) != 1:
        return (
            False,
            "Provide one pair of keys for the connector to be registered."
        )

    keys = all_keys[0]

    if keys.count(':') != 1:
        return False, "Connector keys must be in the format `type:label`."

    type_, label = keys.split(':', maxsplit=1)
    mrsm_config = get_config('meerschaum')
    if 'connectors' not in mrsm_config:
        mrsm_config['connectors'] = {}

    if type_ not in mrsm_config['connectors']:
        mrsm_config['connectors'] = {}

    is_new = True
    if label in mrsm_config['connectors'][type_]:
        rich_table, rich_json, rich_box = mrsm.attempt_import(
            'rich.table',
            'rich.json',
            'rich.box',
        )
        existing_params = mrsm_config['connectors'][type_][label]
        if existing_params == params:
            return True, "Connector exists, nothing to do."

        table = rich_table.Table(box=rich_box.MINIMAL)
        table.add_column('Existing Parameters')
        table.add_column('New Parameters')
        table.add_row(
            rich_json.JSON.from_data(existing_params),
            rich_json.JSON.from_data(params or {}),
        )

        mrsm.pprint(table)
        warn(f"Connector '{keys}' already exists.", stack=False)
        if not yes_no(
            f"Do you want to overwrite connector '{keys}'?",
            default='n',
            **kwargs
        ):
            return False, "Nothing was changed."

        is_new = False

    mrsm_config['connectors'][type_][label] = params
    if not write_config({'meerschaum': mrsm_config}):
        return False, "Failed to update configuration."

    msg = (
        "Successfully "
        + ("registered" if is_new else "updated")
        + f" connector '{keys}'."
    )
    return True, msg


def _complete_register_connectors(
    action: Optional[List[str]] = None, **kw: Any
) -> List[str]:
    from meerschaum.actions.show import _complete_show_connectors
    return _complete_show_connectors(action)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
register.__doc__ += _choices_docstring('register')
