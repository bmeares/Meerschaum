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
        'jobs'      : _edit_jobs,
    }
    return choose_subaction(action, options, **kw)


def _complete_edit(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.delete import _complete_delete_jobs

    if action is None:
        action = []

    options = {
        'config': _complete_edit_config,
        'plugin': _complete_edit_plugins,
        'plugins': _complete_edit_plugins,
        'job': _complete_delete_jobs,
        'jobs': _complete_delete_jobs,
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


def _edit_config(action: Optional[List[str]] = None, **kw : Any) -> SuccessTuple:
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
        print_options(pipes, header='Pipes to be edited:')
    else:
        return False, "No pipes to edit."

    if len(pipes) > 1:
        try:
            if not (yes or force or noask):
                prompt(
                    f"Press [Enter] to begin editing the above {len(pipes)} pipe"
                    + ("s" if len(pipes) != 1 else "")
                    + " or [CTRL-C] to cancel:",
                    icon=False,
                )
        except KeyboardInterrupt:
            return False, "No pipes changed."

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


def _edit_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Edit existing jobs.
    """
    import shlex
    from meerschaum.jobs import get_filtered_jobs
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum._internal.arguments import (
        split_pipeline_sysargs,
        split_chained_sysargs,
        parse_arguments,
    )
    from meerschaum.utils.formatting import make_header, print_options
    from meerschaum.utils.warnings import info
    from meerschaum._internal.shell.ShellCompleter import ShellCompleter
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.formatting._shell import clear_screen

    jobs = get_filtered_jobs(executor_keys, action, debug=debug)
    if not jobs:
        return False, "No jobs to edit."

    num_edited = 0
    for name, job in jobs.items():
        try:
            sub_args_line = None
            pipeline_args_line = None
            if job.sysargs[:2] == ['start', 'pipeline']:
                job_args = parse_arguments(job.sysargs)
                mrsm.pprint(job_args)
                sub_args_line = job_args['params']['sub_args_line']
                params_index = job.sysargs[2:].index('-P')
                indices_to_skip = (params_index, params_index + 1)
                pipeline_args_line = shlex.join(
                    [a for i, a in enumerate(job.sysargs[2:]) if i not in indices_to_skip]
                )
        except (ValueError, IndexError):
            sub_args_line = None

        sysargs_str = (
            f"{sub_args_line} : {pipeline_args_line}"
            if sub_args_line is not None and pipeline_args_line is not None
            else shlex.join(job.sysargs)
        )
        clear_screen(debug=debug)
        info(
            f"Editing arguments for job '{name}'.\n"
            "    Press [Esc + Enter] to submit, [CTRL + C] to exit.\n\n"
            "    Tip: join actions with `+`, manage pipeline with `:`.\n"
            "    https://meerschaum.io/reference/actions/#chaining-actions\n"
        )

        try:
            new_sysargs_str = prompt(
                "",
                default_editable=job.label,
                multiline=True,
                icon=False,
                completer=ShellCompleter(),
            )
        except KeyboardInterrupt:
            return True, "Nothing was changed."

        if new_sysargs_str.strip() == sysargs_str.strip():
            continue

        new_sysargs = shlex.split(new_sysargs_str)
        new_sysargs, pipeline_args = split_pipeline_sysargs(new_sysargs)
        chained_sysargs = split_chained_sysargs(new_sysargs)

        clear_screen(debug=debug)
        if len(chained_sysargs) > 1:
            print_options(
                [
                    shlex.join(step_sysargs)
                    for step_sysargs in chained_sysargs
                ],
                header=f"\nSteps in Job '{name}':",
                number_options=True,
                **kwargs
            )
        else:
            print('\n' + make_header(f"Action for Job '{name}':"))
            print(shlex.join(new_sysargs))

        if pipeline_args:
            print('\n' + make_header("Pipeline Arguments:"))
            print(shlex.join(pipeline_args))
            print()

        if not yes_no(
            (
                f"Are you sure you want to recreate job '{name}' with the above arguments?\n"
                + "    The job will be started if you continue."
            ),
            default='n',
            **kwargs
        ):
            return True, "Nothing was changed."

        delete_success, delete_msg = job.delete()
        if not delete_success:
            return delete_success, delete_msg

        new_job = mrsm.Job(name, new_sysargs_str, executor_keys=executor_keys)
        start_success, start_msg = new_job.start()
        if not start_success:
            return start_success, start_msg
        num_edited += 1

    msg = (
        "Successfully edit job"
        + ('s' if len(jobs) != 1 else '')
        + ' '
        + items_str(list(jobs.keys()))
        + '.'
        ) if num_edited > 0 else "Nothing was edited."
    return True, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
edit.__doc__ += _choices_docstring('edit')
