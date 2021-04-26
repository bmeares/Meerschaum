#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Similar to the `bash` action, run Python commands from the Meerschaum shell
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional

def python(
        action : Optional[List[str]] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Launch a Python interpreter with Meerschaum imported. Commands are optional.
    Note that quotes must be escaped and commands must be separated by semicolons

    Usage:
        `python {commands}`

    Example:
        `python print(\\'Hello, World!\\'); pipes = mrsm.get_pipes()`

        ```
        Hello, World!
        >>> import meerschaum as mrsm
        >>> print('Hello, World!')
        >>> pipes = mrsm.get_pipes()
        ```
    """
    import sys, subprocess
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.process import run_process

    if action is None:
        action = []

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
    #  joined_actions = ['import meerschaum as mrsm;'] + joined_actions
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
    try:
        return_code = run_process([sys.executable, '-i', '-c', command], foreground=True)
    except KeyboardInterrupt:
        return_code = 1
    return return_code == 0, (
        "Success" if return_code == 0
        else f"Python interpreter returned {return_code}."
    )
