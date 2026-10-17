#! /usr/bin/env python3

"""Utilities for the built-in Compose action."""

import pathlib
import shlex
from typing import List, Dict, Any, Optional, Union

import meerschaum as mrsm
from meerschaum.compose.utils.debug import get_debug_args
from meerschaum.compose.utils.config import get_env_dict
from meerschaum.compose.utils.stack import get_project_name


def run_mrsm_command(
    args: Union[List[str], str],
    compose_config: Dict[str, Any],
    capture_output: bool = False,
    debug: bool = False,
    _subprocess: Optional[bool] = None,
    _replace: bool = True,
    **kw
) -> mrsm.SuccessTuple:
    """
    Run a Meerschaum command in the Compose environment: the native `mrsm` binary, started with
    the project's environment (`_replace`) and the project's tag.
    """
    import os
    import subprocess
    import sys
    from meerschaum._internal.binary import find_binary, MISSING_MESSAGE

    project_name = get_project_name(compose_config)
    if isinstance(args, str):
        args = shlex.split(args)

    sysargs = (
        args
        + (get_debug_args(debug) if '--debug' not in args else [])
        + (
            ['--tags', project_name]
            if '--tags' not in args and '-t' not in args and not ' '.join(args).startswith('stack ')
            else []
        )
        + (['--no-daemon'] if compose_config.get('daemon', None) is False else [])
    )
    binary = find_binary()
    if binary is None:
        return False, MISSING_MESSAGE

    env = (
        {key: ('' if value is None else str(value)) for key, value in get_env_dict(compose_config).items()}
        if _replace
        else dict(os.environ)
    )
    env.setdefault('MRSM_PYTHON', sys.executable)
    process = subprocess.run([binary, *sysargs], env=env, capture_output=capture_output)
    if process.returncode == 0:
        return True, "Success"
    return False, f"Failed to execute sysargs:\n{sysargs}"


def init(
    file: Optional[pathlib.Path] = None,
    env_file: Optional[pathlib.Path] = None,
    isolated: bool = False,
    debug: bool = False,
    **kw: Any
) -> Dict[str, Any]:
    """Read a Compose file and initialize its root directory."""
    from meerschaum.compose.utils.config import (
        infer_compose_file_path,
        init_env,
        init_root,
        read_compose_config,
    )
    compose_file_path = infer_compose_file_path(file)
    if compose_file_path is None:
        raise FileNotFoundError(
            "No compose file could be found.\n    "
            + "Create a file mrsm-compose.yaml or specify a path with `--file`."
        )

    init_env(compose_file_path, env_file)
    compose_config = read_compose_config(
        compose_file_path,
        env_file=env_file,
        isolated=isolated,
        debug=debug,
    )
    init_root(compose_config, debug=debug)
    return compose_config
