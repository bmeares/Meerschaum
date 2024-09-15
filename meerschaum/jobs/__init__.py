#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Higher-level utilities for managing `meerschaum.utils.daemon.Daemon`.
"""

import pathlib

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Optional, List, SuccessTuple

from meerschaum.jobs._Job import Job, StopMonitoringLogs
from meerschaum.jobs._Executor import Executor

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
    'make_executor',
    'Executor',
    'check_restart_jobs',
    'start_check_jobs_thread',
    'stop_check_jobs_thread',
)

executor_types: List[str] = ['api', 'local']


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
    from meerschaum.connectors.parse import parse_executor_keys
    executor_keys = executor_keys or get_executor_keys_from_context()

    include_local_and_system = (
        combine_local_and_systemd
        and str(executor_keys).split(':', maxsplit=1)[0] in ('None', 'local', 'systemd')
        and get_executor_keys_from_context() == 'systemd'
    )

    def _get_local_jobs():
        from meerschaum.utils.daemon import get_daemons
        daemons = get_daemons()
        jobs = {
            daemon.daemon_id: Job(name=daemon.daemon_id, executor_keys='local')
            for daemon in daemons
        }
        return {
            name: job
            for name, job in jobs.items()
            if (include_hidden or not job.hidden) and not job._is_externally_managed

        }

    def _get_systemd_jobs():
        conn = mrsm.get_connector('systemd')
        jobs = conn.get_jobs(debug=debug)
        return {
            name: job
            for name, job in jobs.items()
            if include_hidden or not job.hidden
        }

    if include_local_and_system:
        local_jobs = _get_local_jobs()
        systemd_jobs = _get_systemd_jobs()
        shared_jobs = set(local_jobs) & set(systemd_jobs)
        if shared_jobs:
            from meerschaum.utils.misc import items_str
            from meerschaum.utils.warnings import warn
            warn(
                "Job"
                + ('s' if len(shared_jobs) != 1 else '')
                + f" {items_str(list(shared_jobs))} "
                + "exist"
                + ('s' if len(shared_jobs) == 1 else '')
                + " in both `local` and `systemd`.",
                stack=False,
            )
        return {**local_jobs, **systemd_jobs}

    if executor_keys == 'local':
        return _get_local_jobs()

    if executor_keys == 'systemd':
        return _get_systemd_jobs()

    try:
        _ = parse_executor_keys(executor_keys, construct=False)
        conn = parse_executor_keys(executor_keys)
        jobs = conn.get_jobs(debug=debug)
        return {
            name: job
            for name, job in jobs.items()
            if include_hidden or not job.hidden
        }
    except Exception:
        return {}


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
    jobs = get_jobs(
        executor_keys,
        include_hidden=True,
        combine_local_and_systemd=combine_local_and_systemd,
        debug=debug,
    )
    if not filter_list:
        return {
            name: job
            for name, job in jobs.items()
            if include_hidden or not job.hidden
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


_context_keys = None
def get_executor_keys_from_context() -> str:
    """
    If we are running on the host with the default root, default to `'systemd'`.
    Otherwise return `'local'`.
    """
    global _context_keys

    if _context_keys is not None:
        return _context_keys

    from meerschaum.config import get_config
    from meerschaum.config.paths import ROOT_DIR_PATH, DEFAULT_ROOT_DIR_PATH
    from meerschaum.utils.misc import is_systemd_available

    configured_executor = get_config('meerschaum', 'executor', warn=False)
    if configured_executor is not None:
        return configured_executor

    _context_keys = (
        'systemd'
        if is_systemd_available() and ROOT_DIR_PATH == DEFAULT_ROOT_DIR_PATH
        else 'local'
    )
    return _context_keys


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
