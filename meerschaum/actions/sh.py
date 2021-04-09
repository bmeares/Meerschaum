#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
NOTE: This action may be a huge security vulnerability
    if not handled appropriately!
"""

from meerschaum.utils.typing import SuccessTuple, List, Any, Optional

def sh(
        action : Optional[List[str]] = None,
        sub_args : Optional[List[str]] = None,
        sysargs : Optional[List[str]] = None,
        use_bash : bool = True,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Execute system commands.
    """
    import subprocess
    import sys, os
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint

    if action is None:
        action = []
    if sub_args is None:
        sub_args = []
    if sysargs is None:
        sysargs = []

    _shell = os.environ.get('SHELL', 'bash')

    cmd_list = []

    if len(sysargs) > 1:
        if sysargs[0] == 'sh':
            del sysargs[0]
        elif sysargs[0].startswith('!'):
            sysargs[0] = sysargs[0][1:]
        cmd_list = sysargs
    else:
        cmd_list = action
    if sub_args:
        cmd_list += sub_args

    command_list = cmd_list
    if use_bash:
        command_list = ["bash"]
        if len(action) == 0:
            command_list += ["-c", " ".join(cmd_list)]
    else:
        if len(action) == 0:
            command_list = _shell

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
