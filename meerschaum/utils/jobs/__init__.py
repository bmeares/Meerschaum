#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Higher-level utilities for managing `meerschaum.utils.daemon.Daemon`.
"""

import pathlib

import meerschaum as mrsm
from meerschaum.utils.jobs._Job import Job
from meerschaum.utils.typing import Dict, Optional, List, Callable, Any


def get_jobs(
    executor_keys: Optional[str] = None,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of the existing jobs.

    Parameters
    ----------
    executor_keys: Optional[str], default None
        If provided, return remote jobs on the given API instance.
        Otherwise return local jobs.

    Returns
    -------
    A dictionary mapping job names to jobs.
    """
    if executor_keys == 'local':
        executor_keys = None

    if executor_keys is not None:
        conn = mrsm.get_connector(executor_keys)
        return conn.get_jobs(debug=debug)

    from meerschaum.utils.daemon import get_daemons
    daemons = get_daemons()
    return {
        daemon.daemon_id: Job(name=daemon.daemon_id)
        for daemon in daemons
    }


def get_filtered_jobs(
    executor_keys: Optional[str] = None,
    filter_list: Optional[List[str]] = None,
    warn: bool = False,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a list of jobs filtered by the user.
    """
    from meerschaum.utils.warnings import warn as _warn
    jobs = get_jobs(executor_keys, debug=debug)

    if not filter_list:
        return {
            name: job
            for name, job in jobs.items()
            if not job.hidden
        }

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
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return jobs which were created with `--restart` or `--loop`.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.restart
    }


def get_running_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of running jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'running'
    }


def get_paused_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of paused jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'paused'
    }


def get_stopped_jobs(
    executor_keys: Optional[str] = None,
    jobs: Optional[Dict[str, Job]] = None,
    debug: bool = False,
) -> Dict[str, Job]:
    """
    Return a dictionary of stopped jobs.
    """
    if jobs is None:
        jobs = get_jobs(executor_keys, debug=debug)

    return {
        name: job
        for name, job in jobs.items()
        if job.status == 'stopped'
    }


def check_restart_jobs(
    executor_keys: Optional[str] = None,
    silent: bool = False,
) -> None:
    """
    Restart any stopped jobs which were created with `--restart`.

    Parameters
    ----------
    executor_keys: Optional[str], default None
        If provided, check jobs on the given remote API instance.
        Otherwise check local jobs.

    silent: bool, default False
        If `True`, do not print the restart success message.
    """
    jobs = get_jobs(executor_keys)
    for name, job in jobs.items():
        success, msg = job.check_restart()
        if not silent:
            mrsm.pprint((success, msg))


_check_loop_stop_thread = None
def start_check_jobs_thread():
    """
    Start a thread to regularly monitor jobs.
    """
    import atexit
    from functools import partial
    from meerschaum.utils.threading import RepeatTimer
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.config.paths import CHECK_LOGS_LOCK_PATH

    if CHECK_LOGS_LOCK_PATH.exists():
        return

    try:
        CHECK_LOGS_LOCK_PATH.touch()
    except Exception:
        pass

    global _check_loop_stop_thread
    sleep_seconds = STATIC_CONFIG['jobs']['check_restart_seconds']

    _check_loop_stop_thread = RepeatTimer(
        sleep_seconds,
        partial(
            check_restart_jobs,
            silent=True,
        )
    )
    _check_loop_stop_thread.daemon = True
    atexit.register(stop_check_jobs_thread)

    _check_loop_stop_thread.start()


def stop_check_jobs_thread():
    """
    Stop the job monitoring thread.
    """
    from meerschaum.config.paths import CHECK_LOGS_LOCK_PATH
    if _check_loop_stop_thread is None:
        return

    _check_loop_stop_thread.cancel()

    try:
        CHECK_LOGS_LOCK_PATH.unlink()
    except Exception:
        pass
