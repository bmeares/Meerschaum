#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
NOTE: This action may be a huge security vulnerability
    if not handled appropriately!
"""
from meerschaum.config import system_config

def bash(
        action : list = [],
        sub_args : list = [],
        debug : bool = False,
        **kw
    ):
    """
    Launch a bash shell as a Meershaum action
    """
    import subprocess
    import sys
    from meerschaum.utils.debug import dprint

    ### determine system encoding
    encoding = sys.getdefaultencoding()

    command_list = ["bash"]

    ### where to redirect stdout (default None)
    capture_stdout, capture_stderr = None, None
    timeout = None

    ### if actions are provided, don't launch interactive shell
    ### and read stdout, stderr and exit code
    if action[0] != '':
        capture_stdout = subprocess.PIPE
        capture_stderr = subprocess.PIPE
        command_list += ["-c", " ".join(action + sub_args)]
        timeout = system_config['shell']['timeout']

    if debug:
        dprint('action:', action)
        dprint('sub-args', sub_args)
        dprint(command_list)

    process = subprocess.Popen(
                command_list,
                shell=False,
                stdout=capture_stdout,
                stderr=capture_stderr
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
        dprint("stdout:")
        dprint(output)
        dprint("\nstderr:")
        dprint(error_output)
        dprint("\nexit code:")
        dprint(exit_code)

    if exit_code != 0:
        return (False, error_output)
    return (True, output)
