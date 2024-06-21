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
    from meerschaum.utils.venv import venv_executable
    from meerschaum.utils.misc import generate_password
    from meerschaum.config import __version__ as _version
    from meerschaum.config.paths import VIRTENV_RESOURCES_PATH, PYTHON_RESOURCES_PATH
    from meerschaum.utils.packages import run_python_package, attempt_import

    if action is None:
        action = []

    if venv == 'None':
        venv = None

    joined_actions = ["import meerschaum as mrsm"]
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
        dprint(str(joined_actions))

    ### TODO: format the pre-executed code using the pygments lexer.
    print_command = (
        'from meerschaum.utils.packages import attempt_import; '
        + 'ptft = attempt_import("prompt_toolkit.formatted_text", lazy=False); '
        + 'pts = attempt_import("prompt_toolkit.shortcuts"); '
        + 'ansi = ptft.ANSI("""'
    )
    ps1 = "\\033[1m>>> \\033[0m"
    for i, a in enumerate(joined_actions):
        line = ps1 + f"{a}".replace(';', '\n')
        if '\n' not in line and i != len(joined_actions) - 1:
            line += "\n"
        print_command += line
    print_command += (
        '"""); '
        + 'pts.print_formatted_text(ansi); '
    )

    command = print_command
    for a in joined_actions:
        command += a
        if not a.endswith(';'):
            command += ';'
        command += ' '

    if debug:
        dprint(f"command:\n{command}")

    init_script_path = PYTHON_RESOURCES_PATH / (generate_password(8) + '.py')
    with open(init_script_path, 'w', encoding='utf-8') as f:
        f.write(command)

    env_dict = os.environ.copy()
    venv_path = (VIRTENV_RESOURCES_PATH / venv) if venv is not None else None
    if venv_path is not None:
        env_dict.update({'VIRTUAL_ENV': venv_path.as_posix()})

    try:
        ptpython = attempt_import('ptpython', venv=venv, allow_outside_venv=False)
        return_code = run_python_package(
            'ptpython',
            sub_args or ['--dark-bg', '-i', init_script_path.as_posix()],
            venv = venv,
            foreground = True,
            env = env_dict,
        )
    except KeyboardInterrupt:
        return_code = 1

    try:
        if init_script_path.exists():
            init_script_path.unlink()
    except Exception as e:
        warn(f"Failed to clean up tempory file '{init_script_path}'.")

    return return_code == 0, (
        "Success" if return_code == 0
        else f"Python interpreter returned {return_code}."
    )
