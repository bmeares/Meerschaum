#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
NOTE: This action may be a huge security vulnerability
    if not handled appropriately!
"""

from meerschaum.utils.typing import SuccessTuple, List, Any

def bash(
        action : List[str] = [],
        sub_args : List[str] = [],
        use_bash : bool = True,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Launch a bash shell as a Meershaum action
    """
    import subprocess
    import sys
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint

    if use_bash:
        command_list = ["bash"]
        if len(action) == 0:
            command_list += ["-c", " ".join(action + sub_args)]
    else:
        command_list = action + sub_args
        if len(action) == 0: command_list = 'bash'

    if debug:
        dprint(f'action  : {action}')
        dprint(f'sub-args: {sub_args}')
        dprint(command_list)

    try:
        process = subprocess.Popen(
            command_list,
            shell = False,
        )
        exit_code = process.wait()
    except FileNotFoundError:
        msg = f"Invalid commands: '{command_list}'"
        return False, msg

    if exit_code != 0:
        return (False, f"Returned exit code: {exit_code}")
    return (True, "Success")
