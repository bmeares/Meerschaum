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
    sub_args: Optional[List[str]] = None,
    nopretty: bool = False,
    noask: bool = False,
    venv: Optional[str] = None,
    executor_keys: Optional[str] = None,
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
        mrsm python [-m pip -V]

    Flags:
        `--nopretty`
        Open a plain Pyhthon REPL rather than ptpython.

        `--noask`
        Run the supplied Python code and do not open a REPL.

        `--venv`
        Run the Python interpreter from a virtual environment.
        Will not have Meercshaum imported.

        `--sub-args` (or flags surrounded by `[]`)
        Rather than run Python code, execute the interpreter with the given sub-flags
        (e.g. `mrsm python [-V]`)
    """
    import sys, subprocess, os
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.venv import venv_executable
    from meerschaum.utils.misc import generate_password
    from meerschaum.config import __version__ as _version
    from meerschaum.config.paths import VIRTENV_RESOURCES_PATH, PYTHON_RESOURCES_PATH
    from meerschaum.utils.packages import run_python_package, attempt_import
    from meerschaum.utils.process import run_process

    if executor_keys and executor_keys.startswith('api:'):
        warn("Cannot open a Python REPL remotely, falling back to local...", stack=False)

    if action is None:
        action = []

    if noask:
        nopretty = True

    joined_actions = (
        ["import meerschaum as mrsm"]
        if venv is None and not sub_args
        else []
    )
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
    if nopretty or venv is not None or sub_args:
        print_command = ""

    if sub_args:
        nopretty = True

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

    try:
        ptpython = attempt_import('ptpython', venv=None)
        return_code = run_python_package(
            'ptpython',
            sub_args or ['--dark-bg', '-i', init_script_path.as_posix()],
            foreground = True,
            venv = venv,
        ) if not nopretty else run_process(
            (
                [venv_executable(venv)] + (
                    sub_args or
                    (
                        (
                            ['-i']
                            if not noask
                            else []
                        ) + [init_script_path.as_posix()]
                    )
                )
            ),
            foreground = True,
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
