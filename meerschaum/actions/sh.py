#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
NOTE: This action may be a huge security vulnerability
    if not handled appropriately!
"""

from meerschaum.utils.typing import SuccessTuple, List, Any, Optional


def sh(
    action: Optional[List[str]] = None,
    sub_args: Optional[List[str]] = None,
    filtered_sysargs: Optional[List[str]] = None,
    use_bash: bool = True,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Execute system commands.
    """
    import subprocess
    import sys, os, shlex
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint

    if action is None:
        action = []
    if sub_args is None:
        sub_args = []
    if filtered_sysargs is None:
        filtered_sysargs = []

    _shell = os.environ.get('SHELL', 'bash')

    if action:
        if action[0].startswith('!'):
            action[0] = action[0][1:]
        elif action[0] == 'sh':
            action = action[1:]
    cmd_list = action

    if sub_args:
        cmd_list += sub_args

    command_list = cmd_list
    if use_bash:
        command_list = ["bash"]
        if len(action) != 0:
            try:
                command_list += ["-c", shlex.join(cmd_list)]
            except Exception:
                command_list += ["-c", ' '.join(cmd_list)]
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
            shell=False,
            env=os.environ,
        )
        exit_code = process.wait()
    except FileNotFoundError:
        msg = f"Invalid commands: '{command_list}'"
        return False, msg
    except KeyboardInterrupt:
        return True, "Success"

    if exit_code != 0:
        return (False, f"Returned exit code: {exit_code}")
    return True, "Success"
