#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Attempt to create a pipe's requirements in one method.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, Dict, Any

def bootstrap(
        self,
        debug: bool = False,
        yes: bool = False,
        force: bool = False,
        noask: bool = False,
        shell: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Prompt the user to create a pipe's requirements all from one method.
    This method shouldn't be used in any automated scripts because it interactively
    prompts the user and therefore may hang.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    yes: bool, default False:
        Print the questions and automatically agree.

    force: bool, default False:
        Skip the questions and agree anyway.

    noask: bool, default False:
        Print the questions but go with the default answer.

    shell: bool, default False:
        Used to determine if we are in the interactive shell.
        
    Returns
    -------
    A `SuccessTuple` corresponding to the success of this procedure.

    """

    from meerschaum.utils.warnings import warn, info, error
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.utils.formatting import pprint
    from meerschaum.config import get_config
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.actions import actions
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    _clear = get_config('shell', 'clear_screen', patch=True)

    if self.get_id(debug=debug) is not None:
        delete_tuple = self.delete(debug=debug)
        if not delete_tuple[0]:
            return delete_tuple

    if _clear:
        clear_screen(debug=debug)

    _parameters = _get_parameters(self, debug=debug)
    self.parameters = _parameters
    pprint(self.parameters)
    try:
        prompt(
            f"\n    Press [Enter] to register {self} with the above configuration:",
            icon = False
        )
    except KeyboardInterrupt as e:
        return False, f"Aborting bootstrapping {self}."

    with Venv(get_connector_plugin(self.instance_connector)):
        register_tuple = self.instance_connector.register_pipe(self, debug=debug)

    if not register_tuple[0]:
        return register_tuple

    if _clear:
        clear_screen(debug=debug)

    try:
        if yes_no(
            f"Would you like to edit the definition for {self}?", yes=yes, noask=noask
        ):
            edit_tuple = self.edit_definition(debug=debug)
            if not edit_tuple[0]:
                return edit_tuple

        if yes_no(f"Would you like to try syncing {self} now?", yes=yes, noask=noask):
            sync_tuple = actions['sync'](
                ['pipes'],
                connector_keys = [self.connector_keys],
                metric_keys = [self.metric_key],
                location_keys = [self.location_key],
                mrsm_instance = str(self.instance_connector),
                debug = debug,
                shell = shell,
            )
            if not sync_tuple[0]:
                return sync_tuple
    except Exception as e:
        return False, f"Failed to bootstrap {self}:\n" + str(e)

    print_tuple((True, f"Finished bootstrapping {self}!"))
    info(
        f"You can edit this pipe later with `edit pipes` "
        + "or set the definition with `edit pipes definition`.\n"
        + "    To sync data into your pipe, run `sync pipes`."
    )

    return True, "Success"


def _get_parameters(pipe, debug: bool = False) -> Dict[str, Any]:
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.utils.warnings import warn, info
    from meerschaum.config import get_config
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    _clear = get_config('shell', 'clear_screen', patch=True)
    _types_defaults = {
        'sql': {
            'fetch': {
                'definition': None,
            },
            'parents': [
                {
                    'connector': None,
                    'metric': None,
                    'location': None,
                    'instance': None,
                },
            ],
        },
        'api': {
            'fetch': {
                'connector': None,
                'metric': None,
                'location': None,
            },
            'parents': [
                {
                    'connector': None,
                    'metric': None,
                    'location': None,
                    'instance': None,
                },
            ],
        },
    }
    try:
        conn_type = pipe.connector.type
    except Exception as e:
        conn_type = None
    _parameters = _types_defaults.get(conn_type, {})

    if conn_type == 'plugin':
        if pipe.connector.register is not None:
            with Venv(get_connector_plugin(pipe.connector)):
                _params = pipe.connector.register(pipe)
            if not isinstance(_params, dict):
                warn(
                    f"Plugin '{pipe.connector_keys[len('plugin:'):]}' "
                    + "did not return a dictionary of attributes.", stack=False
                )
            else:
                _parameters = apply_patch_to_config(_parameters, _params)
     
    ### If the plugin's `register()` function returns columns, skip asking for columns.
    ask_for_cols = _parameters.get('columns', {}).get('datetime', None) is None
    if ask_for_cols:
        _parameters['columns'] = _ask_for_columns(pipe, debug=debug)

    ### Ask to change the target.
    if _clear:
        clear_screen(debug=debug)
    info(
        f"You have the option to change the target table name for {pipe}.\n    "
        + "To keep the default, press [Enter].\n"
    )
    _parameters['target'] = prompt(f"Target table name for {pipe}:", default=pipe.target)

    if _clear:
        clear_screen(debug=debug)

    ### Ask for tags.
    try:
        info(
            "In addition to the key flags (-c, -m, -l), you can select pipes with `--tags` (-t).\n"
            + "    Separate tags with commas (e.g. tag1,tag2).\n"
        )
        _parameters['tags'] = [
            t.strip() for t in prompt(f"Tags for {pipe} (empty to omit):").split(',') if t
        ]
    except Exception as e:
        _parameters['tags'] = []

    return _parameters


def _ask_for_columns(pipe, debug: bool=False) -> Dict[str, str]:
    """
    Prompt the user for the column names.
    """
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.prompt import prompt

    info(f"Please enter column names for {pipe}:")
    while True:
        try:
            datetime_name = prompt(f"Datetime column (empty to omit):", icon=False)
        except KeyboardInterrupt:
            return False, f"Cancelled bootstrapping {pipe}."
        if datetime_name == '':
            datetime_name = None

        try:
            id_name = prompt(f"ID column (empty to omit):", icon=False)
        except KeyboardInterrupt:
            return False, f"Cancelled bootstrapping {pipe}."
        if id_name == '':
            id_name = None

        break

    return {
        'datetime': datetime_name,
        'id': id_name,
    }
