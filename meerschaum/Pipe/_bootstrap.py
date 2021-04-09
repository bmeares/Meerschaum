#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Attempt to create a pipe's requirements in one method.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional

def bootstrap(
        self,
        debug : bool = False,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Prompt the user to create a pipe's requirements all from one method.
    This method shouldn't be used in any automated scripts because it interactively
    prompts the user and therefore may hang.
    """

    from meerschaum.utils.warnings import warn, info, error
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.utils.formatting import pprint
    from meerschaum.config import get_config
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.formatting import print_tuple

    _clear = get_config('shell', 'clear_screen', patch=True)

    if self.get_id(debug=debug) is not None:
        delete_tuple = self.delete(debug=debug)
        if not delete_tuple[0]:
            return delete_tuple

    info(f"Please enter the following column names for pipe '{self}':")
    while True:
        try:
            datetime_name = prompt(f"Datetime:", icon=False)
        except KeyBoardError:
            return False, "Failed to bootstrap pipe '{self}':\n" + str(e)
        if datetime_name == '':
            warn(f"Please enter a datetime column.", stack=False)
            continue

        try:
            id_name = prompt(f"ID (empty to omit):", icon=False)
        except KeyBoardError:
            return False, f"Failed to bootstrap pipe '{self}':\n" + str(e)
        if id_name == '':
            id_name = None
        break

    if _clear:
        clear_screen(debug=debug)

    _parameters = {
        'columns' : {
            'datetime' : datetime_name,
            'id' : id_name,
        },
    }

    parameters_update_types = {
        'sql'  : {
            'fetch' : {
                'definition' : None,
            },
        },
        'api'  : {
            'fetch' : {
                'connector_keys' : None,
                'metric_key' : None,
                'location_key' : None,
            },
        },
        'mqtt' : {
            'fetch' : {
                'topic' : '#',
            },
        },
    }

    _parameters.update(parameters_update_types.get(self.connector.type, {}))
    self.parameters = _parameters
    pprint(self.parameters)
    prompt(f"\nPress [Enter] to register pipe '{self}' with the above configuration:", icon=False)
    register_tuple = self.register(debug=debug)
    if not register_tuple[0]:
        return register_tuple

    if _clear:
        clear_screen(debug=debug)

    try:
        if yes_no(
            f"Would you like to edit the definition for pipe '{self}'?", yes=yes, noask=noask
        ):
            edit_tuple = self.edit_definition(debug=debug)
            if not edit_tuple[0]:
                return edit_tuple

            if yes_no(f"Would you like to sync pipe '{self}' now?", yes=yes, noask=noask):
                sync_tuple = self.sync(debug=debug)
                if not sync_tuple[0]:
                    return sync_tuple
    except Exception as e:
        return False, f"Failed to bootstrap pipe '{self}':\n" + str(e)

    print_tuple((True, f"Finished bootstrapping pipe '{self}'!"))
    info(
        f"You can edit this pipe later with `edit pipes` or set the definition with `edit pipes definition`.\n" +
        "To sync data into your pipe, run `sync pipes`."
    )

    return True, "Success"
