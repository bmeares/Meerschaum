#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Similar to the `bash` action, run Python commands from the Meerschaum shell
"""

def python(
        action : list = [''],
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Launch a Python interpreter with Meerschaum imported. Commands are optional.
    Note that quotes must be escaped and commands must be separated by semicolons

    Usage:
        `python {commands}`

    Example:
        `python print(\\'Hello, World!\\'); pipes = mrsm.get_pipes()`

    Result:
        Hello, World!
        >>> import meerschaum as mrsm
        >>> print('Hello, World!')
        >>> pipes = mrsm.get_pipes()
    """
    import sys, subprocess
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error

    joined_actions = ['import meerschaum as mrsm']
    line = ""
    for i, a in enumerate(action):
        if a == '': continue
        line += a + " "
        if a.endswith(';') or i == len(action) - 1:
            joined_actions.append(line[:-1])
            line = ""

    ### ensure meerschaum is imported
    #  joined_actions = ['import meerschaum as mrsm;'] + joined_actions
    if debug: dprint(joined_actions)

    #  for a in joined_actions:

    
    #  end_with_semicolon = False
    #  end_with_semicolon = joined_actions[-1].endswith(';')
    print_command = 'print("""'
    for i, a in enumerate(joined_actions):
        line = ">>> " + f"{a}".replace(';', '\n')
        if '\n' not in line and i != len(joined_actions) - 1: line += "\n"
        print_command += line
    print_command += '""")'

    command = ""
    for a in joined_actions:
        command += a
        if not a.endswith(';'): command += ';'
        command += ' '

    #  if not end_with_semicolon:
        #  command += ";"

    command += print_command

    if debug: dprint(f"command:\n{command}")
    return_code = subprocess.call([sys.executable, '-i', '-c', command])
    return return_code == 0, "Success" if return_code == 0 else f"Python interpreter returned {return_code}"
