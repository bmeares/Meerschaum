#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage jobs via the Meerschaum API (the Rust client, `meerschaum._core.ApiConnector`).
"""

import asyncio
import inspect
from datetime import datetime

from meerschaum.utils.typing import Dict, Any, SuccessTuple, List, Union, Callable, Optional


def get_jobs(self, debug: bool = False) -> Dict[str, 'Job']:
    """
    Return a dictionary of remote jobs.
    """
    from meerschaum.jobs import Job
    return {
        name: Job(
            name,
            job_meta['sysargs'],
            executor_keys=str(self),
            _properties=job_meta['daemon']['properties'],
        )
        for name, job_meta in self._rust_client().get_jobs_metadata().items()
    }


def get_job(self, name: str, debug: bool = False) -> 'Job':
    """
    Return a single Job object.
    """
    from meerschaum.jobs import Job
    metadata = self.get_job_metadata(name, debug=debug)
    if not metadata:
        raise ValueError(f"Job '{name}' does not exist.")

    return Job(
        name,
        metadata['sysargs'],
        executor_keys=str(self),
        _properties=metadata['daemon']['properties'],
    )


def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the metadata for a single job.
    """
    return self._rust_client().get_job_metadata(name)


def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the daemon properties for a single job.
    """
    return self._rust_client().get_job_properties(name)


def get_job_status(self, name: str, debug: bool = False) -> str:
    """
    Return the job's status.
    """
    return self._rust_client().get_job_status(name)


def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `began` timestamp, if it exists.
    """
    return self._rust_client().get_job_began(name)


def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `ended` timestamp, if it exists.
    """
    return self._rust_client().get_job_ended(name)


def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `paused` timestamp, if it exists.
    """
    return self._rust_client().get_job_paused(name)


def get_job_exists(self, name: str, debug: bool = False) -> bool:
    """
    Return whether a job exists.
    """
    return self._rust_client().get_job_exists(name)


def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Delete a job.
    """
    return self._rust_client().delete_job(name)


def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Start a job.
    """
    return self._rust_client().start_job(name)


def create_job(
    self,
    name: str,
    sysargs: List[str],
    properties: Optional[Dict[str, str]] = None,
    debug: bool = False,
) -> SuccessTuple:
    """
    Create a job.
    """
    return self._rust_client().create_job(name, sysargs, properties)


def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Stop a job.
    """
    return self._rust_client().stop_job(name)


def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Pause a job.
    """
    return self._rust_client().pause_job(name)


def get_logs(self, name: str, debug: bool = False) -> str:
    """
    Return the logs for a job.
    """
    return self._rust_client().get_logs(name)


def get_job_stop_time(self, name: str, debug: bool = False) -> Union[datetime, None]:
    """
    Return the job's manual stop time.
    """
    data = self._rust_client().get_job_stop_time(name)
    if data is None:
        return None

    return datetime.fromisoformat(data)


class _LoopCallback:
    """
    Run a callback (a function or a coroutine function) on the event loop's thread and wait for it,
    so the Rust client's thread sees the callback's result or exception.
    """

    def __init__(self, loop, callback):
        self.loop = loop
        self.callback = callback
        self.is_async = inspect.iscoroutinefunction(callback)

    def __call__(self, *args):
        if not self.is_async:
            return self.callback(*args)
        future = asyncio.run_coroutine_threadsafe(self.callback(*args), self.loop)
        return future.result()


async def monitor_logs_async(
    self,
    name: str,
    callback_function: Callable[[Any], Any],
    input_callback_function: Callable[[], str],
    stop_callback_function: Callable[[SuccessTuple], str],
    stop_on_exit: bool = False,
    strip_timestamps: bool = False,
    accept_input: bool = True,
    debug: bool = False,
):
    """
    Monitor a job's log files and await a callback with the changes.

    The stream runs in the Rust client; coroutine callbacks are awaited on this event loop.
    Raise `meerschaum.jobs.StopMonitoringLogs` from a callback to stop monitoring.
    """
    import threading
    loop = asyncio.get_running_loop()
    client = self._rust_client()
    cancelled = threading.Event()
    try:
        await loop.run_in_executor(
            None,
            lambda: client.monitor_logs(
                name,
                _LoopCallback(loop, callback_function),
                input_callback_function=(
                    _LoopCallback(loop, input_callback_function)
                    if input_callback_function is not None
                    else None
                ),
                stop_callback_function=(
                    _LoopCallback(loop, stop_callback_function)
                    if stop_callback_function is not None
                    else None
                ),
                stop_on_exit=stop_on_exit,
                strip_timestamps=strip_timestamps,
                stop_event=cancelled,
            ),
        )
    finally:
        ### A cancelled task ends the stream (the worker thread checks every 100 ms).
        cancelled.set()


def monitor_logs(
    self,
    name: str,
    callback_function: Callable[[Any], Any],
    input_callback_function: Callable[[None], str],
    stop_callback_function: Callable[[None], str],
    stop_on_exit: bool = False,
    strip_timestamps: bool = False,
    accept_input: bool = True,
    debug: bool = False,
):
    """
    Monitor a job's log files and execute a callback with the changes.
    """
    return asyncio.run(
        self.monitor_logs_async(
            name,
            callback_function,
            input_callback_function=input_callback_function,
            stop_callback_function=stop_callback_function,
            stop_on_exit=stop_on_exit,
            strip_timestamps=strip_timestamps,
            accept_input=accept_input,
            debug=debug
        )
    )


def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
    """
    Return whether a remote job is blocking on stdin.
    """
    return self._rust_client().get_job_is_blocking_on_stdin(name)


def get_job_prompt_kwargs(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the kwargs to the blocking `prompt()`, if available.
    """
    return self._rust_client().get_job_prompt_kwargs(name)
