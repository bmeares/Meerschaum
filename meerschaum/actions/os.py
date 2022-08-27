#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Open subprocesses and read their output
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional

def os(
        action: Optional[List[str]] = None,
        sub_args: Optional[List[str]] = None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Launch a subprocess and read its output to stdout.
    """
    import os as _os
    from meerschaum.config import get_config

    import subprocess
    import sys
    from meerschaum.utils.debug import dprint

    if action is None:
        action = []

    ### determine system encoding
    encoding = sys.getdefaultencoding()

    command_list = []

    ### where to redirect stdout (default None)
    capture_stdout, capture_stderr = None, None
    timeout = None

    ### if actions are provided, don't launch interactive shell
    ### and read stdout, stderr and exit code
    if len(action) > 0:
        capture_stdout = subprocess.PIPE
        capture_stderr = subprocess.PIPE
        command_list += action + sub_args
        timeout = get_config('shell', 'timeout', patch=True)
    else:
        return False, os.__doc__

    if debug:
        dprint(command_list)

    process = subprocess.Popen(
        command_list,
        shell = False,
        stdout = capture_stdout,
        stderr = capture_stderr,
        env = _os.environ,
    )

    try:
        output_data, error_output_data = process.communicate(timeout=timeout)
    except Exception as e:
        print(e)
        process.kill()
        output_data, error_output_data = process.communicate()

    exit_code = process.wait()

    output = None
    if output_data is not None:
        output = output_data.decode(encoding)

    if output is not None:
        print(output, end="")
    else:
        output = "Success"

    error_output = "Error"
    if error_output_data is not None:
        error_output = error_output_data.decode(encoding)

    if debug:
        dprint("stdout:\n" + f"{output}")
        dprint("stderr:\n" + f"{error_output}")
        dprint(f"exit code: {exit_code}")

    if exit_code != 0:
        return (False, error_output)
    return (True, output)
