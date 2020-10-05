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
    Launch a Python interpreter or execute Python commands
    """
    import sys, subprocess
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    #  exec("print('memes')")
    command = " ".join(action)
    if debug: dprint(command)
    try:
        result = exec(command)
    except Exception as e:
        warn(str(e))

    return True, "Success"
