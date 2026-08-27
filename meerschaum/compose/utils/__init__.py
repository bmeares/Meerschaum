#! /usr/bin/env python3

"""Utilities for the built-in Compose action."""

import copy
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
    """Run a Meerschaum command in the Compose environment."""
    from meerschaum.config.environment import replace_env
    from meerschaum.utils.packages import run_python_package
    from meerschaum.config import replace_config
    import meerschaum.config.paths as paths
    from meerschaum._internal.entry import entry

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
    if _subprocess is None:
        _subprocess = compose_config.get('isolation', None) == 'subprocess'
    if _subprocess:
        _replace = True

    config = copy.deepcopy(compose_config.get('config', {})) if _replace else None
    env = get_env_dict(compose_config) if _replace else None
    root_dir_path = compose_config.get('root_dir', paths.ROOT_DIR_PATH) if _replace else None

    if capture_output or _subprocess:
        success = run_python_package(
            'meerschaum',
            sysargs,
            env=env,
            capture_output=capture_output,
            as_proc=False,
            venv=None,
            foreground=True,
            debug=debug,
            **kw
        ) == 0
        return (True, "Success") if success else (False, f"Failed to execute sysargs:\n{sysargs}")

    with paths.replace_root_dir(root_dir_path):
        with replace_config(config):
            with replace_env(env):
                return entry(sysargs, _use_cli_daemon=True)


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
