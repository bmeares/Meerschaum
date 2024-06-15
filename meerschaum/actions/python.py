#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Similar to the `bash` action, run Python commands from the Meerschaum shell
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional

def python(
        action: Optional[List[str]] = None,
        venv: Optional[str] = 'mrsm',
        sub_args: Optional[List[str]] = None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Launch a virtual environment's Python interpreter with Meerschaum imported.
    You may pass flags to the Python binary by surrounding each flag with `[]`.
    
    Usage:
        `python {commands}`
    
    Examples:
        mrsm python
        mrsm python --venv noaa
        mrsm python [-i] [-c 'print("hi")']
    """
    import sys, subprocess, os
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.process import run_process
    from meerschaum.utils.venv import venv_executable
    from meerschaum.config import __version__ as _version
    from meerschaum.config.paths import VIRTENV_RESOURCES_PATH

    if action is None:
        action = []

    if venv == 'None':
        venv = None

    joined_actions = ['import meerschaum as mrsm']
    line = ""
    for i, a in enumerate(action):
        if a == '':
            continue
        line += a + " "
        if a.endswith(';') or i == len(action) - 1:
            joined_actions.append(line[:-1])
            line = ""

    ### ensure meerschaum is imported
    if debug:
        dprint(joined_actions)

    print_command = 'import sys; print("""'
    ps1 = ">>> "
    for i, a in enumerate(joined_actions):
        line = ps1 + f"{a}".replace(';', '\n')
        if '\n' not in line and i != len(joined_actions) - 1:
            line += "\n"
        print_command += line
    print_command += '""")'

    command = ""
    for a in joined_actions:
        command += a
        if not a.endswith(';'):
            command += ';'
        command += ' '

    command += print_command

    if debug:
        dprint(f"command:\n{command}")

    env_dict = os.environ.copy()
    venv_path = (VIRTENV_RESOURCES_PATH / venv) if venv is not None else None
    if venv_path is not None:
        env_dict.update({'VIRTUAL_ENV': venv_path.as_posix()})

    args_to_run = (
        [venv_executable(venv), '-i', '-c', command]
        if not sub_args
        else [venv_executable(venv)] + sub_args
    )

    try:
        return_code = run_process(
            args_to_run,
            foreground = True,
            env = env_dict,
        )
    except KeyboardInterrupt:
        return_code = 1
    return return_code == 0, (
        "Success" if return_code == 0
        else f"Python interpreter returned {return_code}."
    )
