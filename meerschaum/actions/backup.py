#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Backup the stack database.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional, Union

def backup(
        action: Optional[List[str]] = None,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Backup the stack database.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'database'        : _backup_database,
    }
    return choose_subaction(action, options, **kwargs)


def _backup_database(
        action: Optional[List[str]] = None,
    ) -> SuccessTuple:
    """
    Backup the stack's database to a sql file.
    """
    from meerschaum.actions import get_action
    from meerschaum.config.paths import BACKUP_RESOURCES_PATH
    do_stack = get_action('stack')
    cmd_list = ['exec', 'db', 'pg']
    stack_success, stack_msg = do_stack(['exec'])
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
backup.__doc__ += _choices_docstring('backup')
