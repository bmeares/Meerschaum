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

    scoped_success, scoped_result = _scope_job_action(
        compose_config,
        action or [],
        isolated_sysargs,
        debug=debug,
    )
    if not scoped_success:
        return False, scoped_result
    if scoped_result is None:
        return True, f"No jobs in compose project '{project_name}'."
    isolated_sysargs = scoped_result

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
    else:
        info(f"Starting a shell in compose project '{project_name}'...")

    success, msg = run_mrsm_command(
        isolated_sysargs,
        compose_config,
        debug=debug,
        _subprocess=_subprocess,
        _replace=False,
    )
    return success, msg


### Job actions which change jobs' states. Several Compose projects may share one root
### directory, so these must never fall through to every job in the root.
_SCOPED_JOB_ACTIONS = ('start', 'stop', 'pause', 'restart', 'delete')


def _scope_job_action(
    compose_config: Dict[str, Any],
    action: List[str],
    sysargs: List[str],
    debug: bool = False,
) -> SuccessTuple:
    """
    Scope job actions (e.g. `start jobs`) to this Compose project's jobs.

    Returns
    -------
    A tuple of `(True, sysargs)` with the sysargs to run (`None` if the project has no jobs),
    or `(False, msg)` if the command targets another project's jobs.
    """
    if (
        len(action) < 2
        or action[0] not in _SCOPED_JOB_ACTIONS
        or action[1] not in ('job', 'jobs')
    ):
        return True, sysargs

    from meerschaum.jobs import get_jobs
    from meerschaum.compose.utils.stack import get_project_name
    from meerschaum.compose.utils.jobs import get_project_job_names, get_job_project_name

    project_name = get_project_name(compose_config)
    jobs = get_jobs(debug=debug)
    job_names = action[2:]

    if job_names:
        ### Running another project's job here would apply this project's config to it.
        foreign_job_names = [
            job_name
            for job_name in job_names
            if job_name in jobs
            and get_job_project_name(jobs[job_name]) not in (None, project_name)
        ]
        if foreign_job_names:
            return False, (
                f"Refusing to {action[0]} job"
                + ('s' if len(foreign_job_names) != 1 else '')
                + f" owned by another Compose project: {', '.join(foreign_job_names)}"
                + f"\nRun `{action[0]} jobs` from the owning project's compose file."
            )
        return True, sysargs

    project_job_names = get_project_job_names(compose_config, jobs)
    if not project_job_names:
        return True, None

    ### Insert the project's job names right after the action words (e.g. `start jobs`).
    for ix in range(len(sysargs) - 1):
        if sysargs[ix:ix + 2] == action[:2]:
            return True, sysargs[:ix + 2] + project_job_names + sysargs[ix + 2:]

    return False, (
        f"Could not scope `{' '.join(action)}` to compose project '{project_name}'."
        + "\nName the jobs explicitly."
    )
