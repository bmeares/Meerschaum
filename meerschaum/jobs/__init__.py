#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Higher-level utilities for managing `meerschaum.utils.daemon.Daemon`.
"""

import pathlib

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Optional, List, Callable, Any, SuccessTuple

from meerschaum.jobs._Job import Job, StopMonitoringLogs
from meerschaum.jobs._Executor import Executor

__all__ = (
    'Job',
    'get_jobs',
    'get_filtered_jobs',
    'get_restart_jobs',
    'get_running_jobs',
    'get_stopped_jobs',
    'get_paused_jobs',
    'get_restart_jobs',
    'Executor',
    'make_executor',
    'check_restart_jobs',
    'start_check_jobs_thread',
    'stop_check_jobs_thread',
)

executor_types: List[str] = ['api']


def get_jobs(
    executor_keys: Optional[str] = None,
    include_hidden: bool = False,
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
    from meerschaum.connectors.parse import parse_connector_keys
    if executor_keys == 'local':
        executor_keys = None

    if executor_keys is not None:
        try:
            _ = parse_connector_keys(executor_keys, construct=False)
            conn = mrsm.get_connector(executor_keys)
            return conn.get_jobs(debug=debug)
        except Exception:
            return {}

    from meerschaum.utils.daemon import get_daemons
    daemons = get_daemons()
    jobs = {
        daemon.daemon_id: Job(name=daemon.daemon_id)
        for daemon in daemons
    }
    return {
        name: job
        for name, job in jobs.items()
        if include_hidden or not job.hidden
    }


def get_filtered_jobs(
    executor_keys: Optional[str] = None,
    filter_list: Optional[List[str]] = None,
    include_hidden: bool = False,
    warn: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a list of jobs filtered by the user.
    """
    from meerschaum.utils.warnings import warn as _warn
    jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

    if not filter_list:
        return jobs

    jobs_to_return = {}
    for name in filter_list:
        job = jobs.get(name, None)
        if job is None:
            if warn:
                _warn(
                    f"Job '{name}' does not exist.",
                    stack=False,
                )
            continue
        jobs_to_return[name] = job

    return jobs_to_return


def get_restart_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return jobs which were created with `--restart` or `--loop`.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.restart
    }


def get_running_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of running jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'running'
    }


def get_paused_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of paused jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'paused'
    }


def get_stopped_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    include_hidden: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of stopped jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

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
    executor_keys: Optional[str] = None,
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
        jobs = get_jobs(executor_keys, include_hidden=include_hidden, debug=debug)

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
    from meerschaum.config.paths import CHECK_JOBS_LOCK_PATH
    fasteners = mrsm.attempt_import('fasteners')
    lock = fasteners.InterProcessLock(CHECK_JOBS_LOCK_PATH)
    with lock:
        check_restart_jobs(*args, **kwargs)


_check_loop_stop_thread = None
def start_check_jobs_thread():
    """
    Start a thread to regularly monitor jobs.
    """
    import atexit
    from functools import partial
    from meerschaum.utils.threading import RepeatTimer
    from meerschaum.config.static import STATIC_CONFIG

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
    from meerschaum.config.paths import CHECK_JOBS_LOCK_PATH
    from meerschaum.utils.warnings import warn
    if _check_loop_stop_thread is None:
        return

    _check_loop_stop_thread.cancel()

    try:
        if CHECK_JOBS_LOCK_PATH.exists():
            CHECK_JOBS_LOCK_PATH.unlink()
    except Exception as e:
        warn(f"Failed to remove check jobs lock file:\n{e}")
