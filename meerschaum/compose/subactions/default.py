#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pass all other subactions to `mrsm`.
"""

from meerschaum.utils.typing import SuccessTuple, Any, Optional, List, Dict
from meerschaum.utils.warnings import info


def _compose_default(
    compose_config: Dict[str, Any],
    action: Optional[List[str]] = None,
    sysargs: Optional[List[str]] = None,
    debug: bool = False,
    **kw: Any,
) -> SuccessTuple:
    """
    Execute Meerschaum actions in the isolated environment.
    """
    try:
        from meerschaum.compose.utils import run_mrsm_command
    except ImportError:
        return (
            False,
            (
                f"Failed to execute `{' '.join(action or [])}`."
                + "\nRun `mrsm compose init` before proceeding."
            )
        )

    from meerschaum.compose.utils.stack import get_project_name

    project_name = get_project_name(compose_config)

    isolated_sysargs = []
    found_file = False
    if sysargs:
        for arg in sysargs[1:]:
            if arg in ('--file', '--env-file'):
                found_file = True
                continue
            if found_file:
                found_file = False
                continue
            isolated_sysargs.append(arg)

    ### Run in a SUBPROCESS only for commands that WRITE to venvs (`install`, `setup`).
    ### In-process execution cannot redirect Meerschaum's import-time path resolution
    ### (notably the venvs dir): re-pointing the root mid-process does not move where
    ### `install required` / `setup plugins` write, so in-process they land in the HOST's
    ### `~/.config/meerschaum/venvs` instead of the project's `root/venvs` (the package
    ### installs, but the project venvs stay empty). A fresh subprocess reads the compose
    ### env (absolute MRSM_ROOT_DIR) at import, so venvs resolve under the project root.
    ### Everything else runs in-process per the configured isolation (much faster — no
    ### per-command interpreter spin-up); the shell (no action) still needs a subprocess.
    _venv_writing_actions = {'install', 'setup'}
    _subprocess = (
        (compose_config.get('isolation', None) == 'subprocess')
        or (not action)
        or (action[0] in _venv_writing_actions)
    )
    if action:
        info(f"Running '{' '.join(action)}' in compose project '{project_name}'...")

    success, msg = run_mrsm_command(
        isolated_sysargs,
        compose_config,
        debug=debug,
        _subprocess=_subprocess,
        _replace=False,
    )
    return success, msg
