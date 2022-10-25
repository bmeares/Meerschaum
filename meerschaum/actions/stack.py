#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for running the Docker Compose stack
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional

def stack(
        action: Optional[List[str]] = None,
        sysargs: Optional[List[str]] = None,
        sub_args: Optional[List[str]] = None,
        yes: bool = False,
        noask: bool = False,
        force: bool = False,
        debug: bool = False,
        _capture_output: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Control the Meerschaum stack with Docker Compose.
    Usage: `stack {command}`
    
    Command: action[0]: default 'up'
        Docker Compose command to run. E.g. 'config' will print Docker Compose configuration
    """
    import subprocess
    import contextlib
    import io
    import os
    import sys
    import pathlib
    import meerschaum.config.stack
    from meerschaum.config.stack import NECESSARY_FILES, write_stack
    from meerschaum.config._paths import STACK_COMPOSE_PATH
    from meerschaum.utils.prompt import yes_no
    import meerschaum.config
    from meerschaum.utils.packages import (
        attempt_import, run_python_package, venv_contains_package,
        pip_install,
    )
    from meerschaum.config import get_config
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import ANSI
    from meerschaum.utils.misc import is_docker_available

    if action is None:
        action = []
    if sysargs is None:
        sysargs = []
    if sub_args is None:
        sub_args = []

    bootstrap = False
    for path in NECESSARY_FILES:
        if not path.exists():
            bootstrap = True
            break
    ### if bootstrap flag was set, create files
    if bootstrap:
        write_stack(debug=debug)

    compose_command = ['up']
    ### default: alias stack as docker-compose
    if len(action) > 0 and action[0] != '':
        compose_command = action

    ### define project name when starting containers
    project_name_list = ['--project-name', get_config(
        'stack', 'project_name', patch=True, substitute=True)
    ]
    
    ### disable ANSI if the user sets ANSI mode to False
    ansi_list = [] if ANSI else ['--no-ansi']
    ### Debug list used to include --log-level DEBUG, but the flag is not supported on Windows (?)
    debug_list = []

    ### prepend settings before the docker-compose action
    settings_list = project_name_list + ansi_list + debug_list
    if not is_docker_available():
        warn("Could not connect to Docker. Is the Docker service running?", stack=False)
        print(
            "To start the Docker service, run `sudo systemctl start docker` or `sudo dockerd`.\n"
            + "On Windows or MacOS, make sure Docker Desktop is running.",
            file = sys.stderr,
        )
        return False, "Failed to connect to the Docker engine."

    try:
        has_builtin_compose = subprocess.call(
            ['docker', 'compose'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        ) == 0
    except Exception as e:
        has_builtin_compose = False

    if not has_builtin_compose:
        _compose_venv = 'mrsm'
        compose = attempt_import('compose', lazy=False, venv=_compose_venv, debug=debug)

        ### If docker-compose is installed globally, don't use the `mrsm` venv.
        if not venv_contains_package('compose', _compose_venv):
            _compose_venv = None

        if not venv_contains_package('packaging', _compose_venv):
            if not pip_install('packaging', venv=_compose_venv, debug=debug):
                warn_(f"Unable to install `packaging` into venv '{_compose_venv}'.")

        if not venv_contains_package('yaml', _compose_venv):
            if not pip_install('pyyaml', venv=_compose_venv, debug=debug):
                warn_(f"Unable to install `pyyaml` into venv '{_compose_venv}'.")

    cmd_list = [
        _arg for _arg in (
            settings_list
            + compose_command
            + (
                sysargs[2:] if len(sysargs) > 2 and not sub_args
                else sub_args
            )
        )
        if _arg != '--debug'
    ]
    if debug:
        dprint(cmd_list)

    stdout = None if not _capture_output else subprocess.PIPE
    stderr = stdout

    has_binary_compose = pathlib.Path('/usr/bin/docker-compose').exists()
    proc = subprocess.Popen(
        (
            ['docker', 'compose'] if has_builtin_compose
            else ['docker-compose']
        ) + cmd_list,
        cwd = STACK_COMPOSE_PATH.parent,
        stdout = stdout,
        stderr = stderr,
        env = os.environ,
    ) if (has_builtin_compose or has_binary_compose) else run_python_package(
        'compose',
        args = cmd_list,
        cwd = STACK_COMPOSE_PATH.parent,
        venv = _compose_venv,
        capture_output = _capture_output,
        as_proc = True,
    )
    try:
        rc = proc.wait() if proc is not None else 1
    except KeyboardInterrupt:
        rc = 0
    if _capture_output and proc is not None:
        captured_stdout, captured_stderr = proc.communicate()
        captured_stdout = captured_stdout.decode()
        captured_stderr = captured_stderr.decode()
    success = rc == 0
    msg = (
        "Success" if success else f"Failed to execute commands:\n{cmd_list}"
    ) if not _capture_output else captured_stdout

    return success, msg
