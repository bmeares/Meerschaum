#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Higher-level utilities for managing `meerschaum.utils.daemon.Daemon`.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Optional, List, SuccessTuple

from meerschaum.jobs import _native
from meerschaum.jobs._Job import Job, StopMonitoringLogs
from meerschaum.jobs._Executor import Executor
from meerschaum.jobs._access import (
    filter_visible_jobs,
    can_see_job,
    job_access,
    ownership_properties,
    get_team_jobs,
    move_team_jobs,
)

__all__ = (
    'Job',
    'StopMonitoringLogs',
    'systemd',
    'get_jobs',
    'get_filtered_jobs',
    'get_restart_jobs',
    'get_running_jobs',
    'get_stopped_jobs',
    'get_paused_jobs',
    'get_restart_jobs',
    'get_executor_keys_from_context',
    'make_executor',
    'Executor',
    'check_restart_jobs',
    'start_check_jobs_thread',
    'stop_check_jobs_thread',
    'filter_visible_jobs',
    'can_see_job',
    'job_access',
    'ownership_properties',
    'get_team_jobs',
    'move_team_jobs',
    'report_lap',
)

executor_types: List[str] = ['api', 'local']


#: Set by the worker while it runs a plugin action for the Rust host (`report_lap`).
_lap_reporter = None


def report_lap(
    success: bool = True,
    message: str = '',
    next_in: Optional[float] = None,
) -> SuccessTuple:
    """
    Record a lap of a plugin action that repeats its work by itself (its function takes `loop`,
    so `--loop` is left to it): the job's health then shows how its laps go, and the Overview's
    Next runs when the next one is due.

    Call it once at the end of each iteration. Outside a job (a foreground run, a script) it does
    nothing. An action without a `loop` parameter does not need it: with `--loop` or `-s`, the
    host runs it in laps and records them itself.

    Parameters
    ----------
    success: bool, default True
        Whether this lap succeeded (a failed lap counts toward `consecutive_failures`).

    message: str, default ''
        What the lap did, shown with the job's health.

    next_in: Optional[float], default None
        Seconds until the next lap begins (the action's own `min_seconds`). It sets the cadence
        the job is expected at (stalled after three times it) and `job.health['next_run']`.

    Returns
    -------
    A `SuccessTuple`: whether the lap was recorded.

    Examples
    --------
    >>> import time
    >>> import meerschaum as mrsm
    >>> def heartbeat_ack(loop: bool = False, min_seconds: float = 20, **kwargs):
    ...     while True:
    ...         success, msg = ack_once()
    ...         mrsm.jobs.report_lap(success, msg, next_in=min_seconds)
    ...         if not loop:
    ...             return success, msg
    ...         time.sleep(min_seconds)
    """
    reporter = _lap_reporter
    if reporter is None:
        return True, "Not running in a job's plugin action: the lap was not recorded."
    lap = {'success': bool(success), 'message': str(message or '')}
    if next_in is not None:
        lap['next_in'] = float(next_in)
    value = reporter(lap)
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return bool(value[0]), str(value[1])
    return False, f"Unexpected answer from the host: {value!r}"


def _rows_to_jobs(rows) -> Dict[str, Job]:
    """Build the `Job`s of a native listing."""
    jobs = {}
    for row in rows:
        keys = row['executor_keys']
        jobs[row['name']] = Job(
            row['name'],
            None if keys == 'local' else row['sysargs'],
            executor_keys=keys,
        )
    return jobs


def get_jobs(
    executor_keys: Optional[str] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of the existing jobs.

    Parameters
    ----------
    executor_keys: Optional[str], default None
        If provided, return remote jobs on the given API instance.
        Otherwise return local jobs.

    include_hidden: bool, default False
        If `True`, include jobs with the `hidden` attribute.

    Returns
    -------
    A dictionary mapping job names to jobs.
    """
    from meerschaum.utils.warnings import warn
    native = _native.core()
    try:
        rows, warnings = native.jobs_get_jobs(
            executor_keys,
            include_hidden,
            combine_local_and_systemd,
            api_attributes=_native.api_attributes(executor_keys),
        )
    except RuntimeError as e:
        ### An unreachable or unauthorized executor is not "no jobs".
        warn(f"Could not get the jobs of executor '{executor_keys}': {e}", stack=False)
        return {}

    _native.emit_warnings(warnings)
    return _rows_to_jobs(rows)


def get_filtered_jobs(
    executor_keys: Optional[str] = None,
    filter_list: Optional[List[str]] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    warn: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a list of jobs filtered by the user.
    """
    from meerschaum.utils.warnings import warn as _warn
    native = _native.core()
    try:
        rows, listing_warnings, missing = native.jobs_get_filtered_jobs(
            executor_keys,
            filter_list or [],
            include_hidden,
            combine_local_and_systemd,
            api_attributes=_native.api_attributes(executor_keys),
        )
    except RuntimeError as e:
        _warn(f"Could not get the jobs of executor '{executor_keys}': {e}", stack=False)
        return {}

    _native.emit_warnings(listing_warnings)
    if warn:
        _native.emit_warnings(missing)
    return _rows_to_jobs(rows)


def get_restart_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return jobs which were created with `--restart` or `--loop`.
    """
    if jobs is None:
        jobs = get_jobs(
            executor_keys,
            include_hidden=include_hidden,
            combine_local_and_systemd=combine_local_and_systemd,
            debug=debug,
        )

    return {
        name: job
        for name, job in jobs.items()
        if job.restart
    }


def get_running_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of running jobs.
    """
    if jobs is None:
        jobs = get_jobs(
            executor_keys,
            include_hidden=include_hidden,
            combine_local_and_systemd=combine_local_and_systemd,
            debug=debug,
        )

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'running'
    }


def get_paused_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of paused jobs.
    """
    if jobs is None:
        jobs = get_jobs(
            executor_keys,
            include_hidden=include_hidden,
            combine_local_and_systemd=combine_local_and_systemd,
            debug=debug,
        )

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'paused'
    }


def get_stopped_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    combine_local_and_systemd: bool = True,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of stopped jobs.
    """
    if jobs is None:
        jobs = get_jobs(
            executor_keys,
            include_hidden=include_hidden,
            combine_local_and_systemd=combine_local_and_systemd,
            debug=debug,
        )

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'stopped'
    }


def make_executor(cls):
    """
    Register a class as an `Executor`.
    """
    import re
    from meerschaum.connectors import make_connector
    suffix_regex = r'executor$'
    typ = re.sub(suffix_regex, '', cls.__name__.lower())
    if typ not in executor_types:
        executor_types.append(typ)
    return make_connector(cls, _is_executor=True)


def check_restart_jobs(
    executor_keys: Optional[str] = 'local',
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = True,
    silent: bool = False,
    debug: bool = False,
) -> SuccessTuple:
    """
    Restart any stopped jobs which were created with `--restart`.

    Parameters
    ----------
    executor_keys: Optional[str], default None
        If provided, check jobs on the given remote API instance.
        Otherwise check local jobs.

    include_hidden: bool, default True
        If `True`, include hidden jobs in the check.

    silent: bool, default False
        If `True`, do not print the restart success message.
    """
    from meerschaum.utils.misc import items_str

    if jobs is None:
        jobs = get_jobs(
            executor_keys,
            include_hidden=include_hidden,
            combine_local_and_systemd=False,
            debug=debug,
        )

    if not jobs:
        return True, "No jobs to restart."

    results = {}
    for name, job in jobs.items():
        check_success, check_msg = job.check_restart()
        results[job.name] = (check_success, check_msg)
        if not silent:
            mrsm.pprint((check_success, check_msg))

    success_names = [name for name, (check_success, check_msg) in results.items() if check_success]
    fail_names = [name for name, (check_success, check_msg) in results.items() if not check_success]
    success = len(success_names) == len(jobs)
    msg = (
        (
            "Successfully restarted job"
            + ('s' if len(success_names) != 1 else '')
            + ' ' + items_str(success_names) + '.'
        )
        if success
        else (
            "Failed to restart job"
            + ('s' if len(success_names) != 1 else '')
            + ' ' + items_str(fail_names) + '.'
        )
    )
    return success, msg


def _check_restart_jobs_against_lock(*args, **kwargs):
    import fcntl
    import meerschaum.config.paths as paths
    paths.CHECK_JOBS_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(paths.CHECK_JOBS_LOCK_PATH, 'a+', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            check_restart_jobs(*args, **kwargs)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


_check_loop_stop_thread = None
def start_check_jobs_thread():
    """
    Start a thread to regularly monitor jobs.
    """
    import atexit
    from functools import partial
    from meerschaum.utils.threading import RepeatTimer
    from meerschaum._internal.static import STATIC_CONFIG

    global _check_loop_stop_thread
    sleep_seconds = STATIC_CONFIG['jobs']['check_restart_seconds']

    _check_loop_stop_thread = RepeatTimer(
        sleep_seconds,
        partial(
            _check_restart_jobs_against_lock,
            silent=True,
        )
    )
    _check_loop_stop_thread.daemon = True
    atexit.register(stop_check_jobs_thread)

    _check_loop_stop_thread.start()
    return _check_loop_stop_thread


def stop_check_jobs_thread():
    """
    Stop the job monitoring thread.
    """
    import meerschaum.config.paths as paths
    from meerschaum.utils.warnings import warn
    if _check_loop_stop_thread is None:
        return

    _check_loop_stop_thread.cancel()

    try:
        if paths.CHECK_JOBS_LOCK_PATH.exists():
            paths.CHECK_JOBS_LOCK_PATH.unlink()
    except Exception as e:
        warn(f"Failed to remove check jobs lock file:\n{e}")


def get_executor_keys_from_context() -> str:
    """
    Return the configured executor (`meerschaum:executor`).
    Otherwise, on a host with `systemd`, return `'systemd'` (for any root directory) unless
    Meerschaum runs from a source checkout or a development build.
    Otherwise return `'local'`.
    """
    return _native.core().jobs_executor_keys_from_context()


def _install_healthcheck_job() -> SuccessTuple:
    """
    Install the systemd job which checks local jobs.
    """
    from meerschaum.config import get_config

    enable_healthcheck = get_config('system', 'experimental', 'systemd_healthcheck')
    if not enable_healthcheck:
        return False, "Local healthcheck is disabled."

    if get_executor_keys_from_context() != 'systemd':
        return False, "Not running systemd."

    job = Job(
        '.local-healthcheck',
        ['restart', 'jobs', '-e', 'local', '--loop', '--min-seconds', '60'],
        executor_keys='systemd',
    )
    return job.start()
