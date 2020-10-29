#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
NOTE: This action may be a huge security vulnerability
    if not handled appropriately!
"""

def bash(
        action : list = [],
        sub_args : list = [],
        debug : bool = False,
        **kw
    ):
    """
    Launch a bash shell as a Meershaum action
    """
    from meerschaum.config import system_config

    import subprocess
    import sys
    from meerschaum.utils.debug import dprint

    command_list = ["bash"]

    if action[0] != '':
        command_list += ["-c", " ".join(action + sub_args)]

    if debug:
        dprint(f'action  : {action}')
        dprint(f'sub-args: {sub_args}')
        dprint(command_list)

    process = subprocess.Popen(
        command_list,
        shell = False,
    )

    exit_code = process.wait()

    if exit_code != 0:
        return (False, f"Returned exit code: {exit_code}")
    return (True, "Success")
