#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the Meerschaum abstraction atop daemons.

`Job` is a façade over the native jobs (crate `meerschaum-jobs`, bound as
`meerschaum._core.Job`): the on-disk layout, the supervision and the status rules are Rust.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import pathlib
import shlex
import sys
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import (
    List, Optional, Union, SuccessTuple, Any, Dict, Callable, TYPE_CHECKING,
)
from meerschaum.utils.warnings import warn
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.jobs import _native

if TYPE_CHECKING:
    from meerschaum.jobs._Executor import Executor
    from meerschaum.utils.daemon import Daemon

BANNED_CHARS: List[str] = [
    ',', ';', "'", '"', '$', '#', '=', '*', '&', '!', '`', '~',
]
RESTART_FLAGS: List[str] = [
    '-s',
    '--restart',
    '--loop',
    '--schedule',
    '--cron',
]
STOP_TOKEN: str = STATIC_CONFIG['jobs']['stop_token']
CLEAR_TOKEN: str = STATIC_CONFIG['jobs']['clear_token']
FLUSH_TOKEN: str = STATIC_CONFIG['jobs']['flush_token']


class StopMonitoringLogs(Exception):
    """
    Raise this exception to stop the logs monitoring.
    """


def _clear_screen() -> None:
    """Clear the terminal (v4 `clear_screen()`)."""
    try:
        from meerschaum.utils.formatting._shell import clear_screen
        clear_screen()
        return
    except Exception:
        pass
    print('\033[2J\033[H', end='', flush=True)


def _flush_stdout() -> None:
    print('', end='', flush=True)


def _default_stdout_callback(line: str):
    if line == '\n' or line.startswith(FLUSH_TOKEN):
        _flush_stdout()
        return

    if CLEAR_TOKEN in line:
        _clear_screen()
        return

    if STOP_TOKEN in line:
        return

    print(line, end='', flush=True)


def _parse_dt(text: Optional[str]) -> Union[datetime, None]:
    return datetime.fromisoformat(text) if text else None


class Job:
    """
    Manage a job, locally, with `systemd`, or remotely via the API.
    """

    def __init__(
        self,
        name: str,
        sysargs: Union[List[str], str, None] = None,
        env: Optional[Dict[str, str]] = None,
        executor_keys: Optional[str] = None,
        delete_after_completion: bool = False,
        refresh_seconds: Union[int, float, None] = None,
        _properties: Optional[Dict[str, Any]] = None,
        _status_hook: Optional[Callable[[], str]] = None,
        _result_hook: Optional[Callable[[], SuccessTuple]] = None,
        _externally_managed: bool = False,
    ):
        """
        Create a new job.

        Parameters
        ----------
        name: str
            The name of the job to be created.
            This will also be used as the Daemon ID.

        sysargs: Union[List[str], str, None], default None
            The sysargs of the command to be executed, e.g. 'start api'.

        env: Optional[Dict[str, str]], default None
            If provided, set these environment variables in the job's process.

        executor_keys: Optional[str], default None
            If provided, execute the job remotely on an API instance, e.g. 'api:main'
            (or with `'systemd'`). Defaults to `'local'`, or the context's executor.

        delete_after_completion: bool, default False
            If `True`, delete this job when it has finished executing.

        refresh_seconds: Union[int, float, None], default None
            The number of seconds to sleep between refreshes.
            Defaults to the configured value `system.cli.refresh_seconds`.

        _properties: Optional[Dict[str, Any]], default None
            If provided, use this to patch the daemon's properties.
        """
        native = _native.core()
        self._core = native.Job(
            name,
            sysargs,
            env=env,
            executor_keys=executor_keys,
            delete_after_completion=delete_after_completion,
            refresh_seconds=refresh_seconds,
            properties=_properties,
            externally_managed=_externally_managed,
            api_attributes=_native.api_attributes(executor_keys),
        )
        _native.emit_warnings(self._core.take_warnings())
        self.name = name
        self.executor_keys = self._core.executor_keys
        self.refresh_seconds = (
            refresh_seconds
            if refresh_seconds is not None
            else mrsm.get_config('system', 'cli', 'refresh_seconds')
        )
        if _status_hook is not None:
            self._status_hook = _status_hook

        if _result_hook is not None:
            self._result_hook = _result_hook

        self._externally_managed = _externally_managed

    def _warn(self) -> None:
        """Show the warnings the last native operation produced."""
        _native.emit_warnings(self._core.take_warnings())

    @staticmethod
    def from_pid(pid: int, executor_keys: Optional[str] = None) -> Job:
        """
        Build a `Job` from the PID of a running Meerschaum process.

        Parameters
        ----------
        pid: int
            The PID of the process.

        executor_keys: Optional[str], default None
            The executor keys to assign to the job.
        """
        import os
        try:
            os.kill(pid, 0)
        except ProcessLookupError as e:
            warn(f"Process with PID {pid} does not exist.", stack=False)
            raise e
        except PermissionError:
            pass

        native = _native.core()
        daemon_ids = native.jobs_daemon_ids()
        for daemon_id in daemon_ids:
            job = Job(daemon_id, executor_keys=executor_keys)
            if job.pid == pid:
                return job

        cmdline_path = pathlib.Path('/proc') / str(pid) / 'cmdline'
        if not cmdline_path.exists():
            raise EnvironmentError(f"Is process {pid} a Meerschaum job?")

        command_args = [
            arg
            for arg in cmdline_path.read_bytes().decode('utf-8', errors='replace').split('\0')
            if arg
        ]
        # During startup the PID may change from the supervisor to its worker between
        # the caller's PID read and the registry lookup above. Scope its persistent marker
        # to this registry: another root may have a different Job with the same name.
        if '--jobs-dir' in command_args:
            import meerschaum.config.paths as paths
            index = command_args.index('--jobs-dir') + 1
            if (
                index >= len(command_args)
                or pathlib.Path(command_args[index]).resolve() != paths.DAEMON_RESOURCES_PATH.resolve()
            ):
                raise EnvironmentError(f"Process {pid} belongs to another Meerschaum jobs directory.")
            for arg in command_args:
                if arg.startswith("daemon_id='") and arg.endswith("'"):
                    daemon_id = arg[len("daemon_id='"):-1]
                    if daemon_id in daemon_ids:
                        return Job(daemon_id, executor_keys=executor_keys)
            if '__mrsm_job_supervisor__' in command_args or '__mrsm_run_daemon__' in command_args:
                if '--daemon-id' in command_args:
                    index = command_args.index('--daemon-id') + 1
                    if index < len(command_args) and command_args[index] in daemon_ids:
                        return Job(command_args[index], executor_keys=executor_keys)

        if len(command_args) > 1 and command_args[1] == '-c':
            raise EnvironmentError(f"Is process {pid} a running job?")

        mrsm_ix = 0
        for i, arg in enumerate(command_args):
            if 'mrsm' in arg or 'meerschaum' in arg.lower():
                mrsm_ix = i
                break

        sysargs = command_args[mrsm_ix + 1:]
        name = None
        for flag in ('--name', '--job-name'):
            if flag in sysargs and sysargs.index(flag) + 1 < len(sysargs):
                name = sysargs[sysargs.index(flag) + 1]
                break
        if name is None:
            name = native.jobs_new_daemon_name()
        return Job(name, sysargs, executor_keys=executor_keys)

    def start(self, debug: bool = False) -> SuccessTuple:
        """
        Start the job's daemon.
        """
        result = self._core.start()
        self._warn()
        return result

    def stop(
        self,
        timeout_seconds: Union[int, float, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Stop the job's daemon.
        """
        result = self._core.stop(timeout_seconds)
        self._warn()
        return result

    def pause(
        self,
        timeout_seconds: Union[int, float, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Pause the job's daemon.
        """
        result = self._core.pause(timeout_seconds)
        self._warn()
        return result

    def resume(
        self,
        timeout_seconds: Union[int, float, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Resume a paused job.
        """
        result = self._core.resume(timeout_seconds)
        self._warn()
        return result

    def delete(self, debug: bool = False) -> SuccessTuple:
        """
        Delete the job and its daemon.
        """
        result = self._core.delete()
        self._warn()
        return result

    def is_running(self) -> bool:
        """
        Determine whether the job's daemon is running.
        """
        return self.status == 'running'

    def exists(self, debug: bool = False) -> bool:
        """
        Determine whether the job exists.
        """
        return self._core.exists()

    def get_logs(self) -> Union[str, None]:
        """
        Return the output text of the job's daemon.
        """
        return self._core.get_logs()

    def monitor_logs(
        self,
        callback_function: Callable[[str], None] = _default_stdout_callback,
        input_callback_function: Optional[Callable[[], str]] = None,
        stop_callback_function: Optional[Callable[[SuccessTuple], None]] = None,
        stop_event: Optional[asyncio.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
        accept_input: bool = True,
        debug: bool = False,
        _wait_if_stopped: bool = True,
    ):
        """
        Monitor the job's log files and execute a callback on new lines.

        Parameters
        ----------
        callback_function: Callable[[str], None], default partial(print, end='')
            The callback to execute as new data comes in.
            Defaults to printing the output directly to `stdout`.

        input_callback_function: Optional[Callable[[], str]], default None
            If provided, execute this callback when the daemon is blocking on stdin.
            Defaults to `sys.stdin.readline()`.

        stop_callback_function: Optional[Callable[[SuccessTuple]], str], default None
            If provided, execute this callback when the daemon stops.
            The job's SuccessTuple will be passed to the callback.

        stop_event: Optional[asyncio.Event], default None
            If provided, stop monitoring when this event is set.
            You may instead raise `meerschaum.jobs.StopMonitoringLogs`
            from within `callback_function` to stop monitoring.

        stop_on_exit: bool, default False
            If `True`, stop monitoring when the job stops.

        strip_timestamps: bool, default False
            If `True`, remove leading timestamps from lines.

        accept_input: bool, default True
            If `True`, accept input when the daemon blocks on stdin.
        """
        return self._monitor(
            callback_function,
            input_callback_function,
            stop_callback_function,
            stop_event,
            stop_on_exit,
            strip_timestamps,
            accept_input,
            _wait_if_stopped,
            loop=None,
            stop_flag=None,
        )

    async def monitor_logs_async(
        self,
        callback_function: Callable[[str], None] = _default_stdout_callback,
        input_callback_function: Optional[Callable[[], str]] = None,
        stop_callback_function: Optional[Callable[[SuccessTuple], None]] = None,
        stop_event: Optional[asyncio.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
        accept_input: bool = True,
        debug: bool = False,
        _wait_if_stopped: bool = True,
    ):
        """
        Monitor the job's log files and await a callback on new lines.

        Parameters
        ----------
        callback_function: Callable[[str], None], default _default_stdout_callback
            The callback to execute as new data comes in.
            Defaults to printing the output directly to `stdout`.

        input_callback_function: Optional[Callable[[], str]], default None
            If provided, execute this callback when the daemon is blocking on stdin.
            Defaults to `sys.stdin.readline()`.

        stop_callback_function: Optional[Callable[[SuccessTuple]], str], default None
            If provided, execute this callback when the daemon stops.
            The job's SuccessTuple will be passed to the callback.

        stop_event: Optional[asyncio.Event], default None
            If provided, stop monitoring when this event is set.
            You may instead raise `meerschaum.jobs.StopMonitoringLogs`
            from within `callback_function` to stop monitoring.

        stop_on_exit: bool, default False
            If `True`, stop monitoring when the job stops.

        strip_timestamps: bool, default False
            If `True`, remove leading timestamps from lines.

        accept_input: bool, default True
            If `True`, accept input when the daemon blocks on stdin.
        """
        loop = asyncio.get_running_loop()
        stop_flag = _native.core().StopFlag()
        run = functools.partial(
            self._monitor,
            callback_function,
            input_callback_function,
            stop_callback_function,
            stop_event,
            stop_on_exit,
            strip_timestamps,
            accept_input,
            _wait_if_stopped,
            loop=loop,
            stop_flag=stop_flag,
        )
        try:
            await loop.run_in_executor(None, run)
        except asyncio.CancelledError:
            stop_flag.set()
            raise
        finally:
            stop_flag.set()

    def _monitor(
        self,
        callback_function,
        input_callback_function,
        stop_callback_function,
        stop_event,
        stop_on_exit,
        strip_timestamps,
        accept_input,
        wait_if_stopped,
        loop,
        stop_flag,
    ):
        """
        Follow the logs with the native monitor, running the callbacks on this thread's helper
        (coroutine functions are awaited on `loop`, or on a private loop when there is none).
        """
        def sync(function):
            if function is None or not inspect.iscoroutinefunction(function):
                return function

            def call(*args):
                coroutine = function(*args)
                if loop is not None:
                    return asyncio.run_coroutine_threadsafe(coroutine, loop).result()
                return asyncio.run(coroutine)

            return call

        def default_input_callback_function():
            from meerschaum.utils.prompt import prompt
            prompt_kwargs = self.get_prompt_kwargs()
            if prompt_kwargs:
                answer = prompt(**prompt_kwargs)
                return answer + '\n'
            return sys.stdin.readline()

        input_callback = sync(input_callback_function) or default_input_callback_function
        self._core.monitor_logs(
            sync(callback_function),
            input_callback=input_callback if accept_input else None,
            stop_callback=sync(stop_callback_function),
            stop_event=stop_event,
            stop_flag=stop_flag,
            stop_on_exit=stop_on_exit,
            strip_timestamps=strip_timestamps,
            accept_input=accept_input,
            wait_if_stopped=wait_if_stopped,
            stop_exception=StopMonitoringLogs,
            clear_callback=_clear_screen,
            warn=lambda message: warn(message),
        )

    def is_blocking_on_stdin(self, debug: bool = False) -> bool:
        """
        Return whether a job's daemon is blocking on stdin.
        """
        return self._core.is_blocking_on_stdin()

    def get_prompt_kwargs(self, debug: bool = False) -> Dict[str, Any]:
        """
        Return the kwargs to the blocking `prompt()`, if available.
        """
        return self._core.get_prompt_kwargs()

    def write_stdin(self, data):
        """
        Write to a job's daemon's `stdin`.
        """
        self._core.write_stdin(data)

    @property
    def executor(self) -> Union[Executor, None]:
        """
        If the job is not local, return the connector to its executor
        (the remote API instance, `systemd`, a plugin's executor).
        """
        if self.executor_keys == 'local':
            return None
        from meerschaum.jobs._native import is_python_executor_type
        if is_python_executor_type(self.executor_keys.split(':', maxsplit=1)[0]):
            return mrsm.get_connector(self.executor_keys)
        ### An executor type of a native (Rust) plugin: no Python connector exists.
        from meerschaum.jobs._NativeExecutor import NativeExecutor
        return NativeExecutor(self.executor_keys)

    @property
    def status(self) -> str:
        """
        Return the running status of the job's daemon.
        """
        if '_status_hook' in self.__dict__:
            return self._status_hook()

        return self._core.status

    @property
    def pid(self) -> Union[int, None]:
        """
        Return the PID of the job's dameon.
        """
        return self._core.pid

    @property
    def restart(self) -> bool:
        """
        Return whether to restart a stopped job.
        """
        return self._core.restarts

    @property
    def result(self) -> SuccessTuple:
        """
        Return the `SuccessTuple` when the job has terminated.
        """
        if '_result_hook' in self.__dict__:
            if self.is_running():
                return True, f"{self} is running."
            return self._result_hook()

        return self._core.result

    @property
    def sysargs(self) -> List[str]:
        """
        Return the sysargs to use for the Daemon.
        """
        return self._core.sysargs

    @property
    def health(self) -> Dict[str, Any]:
        """
        Return how the job's laps go (a job that repeats its work: `--loop`, `-s`, `--listen`).

        Returns
        -------
        A dictionary with the keys:

        - `state`: `'healthy'` (the last lap succeeded), `'failing'` (the last lap failed),
          `'stalled'` (no lap for three times the expected interval, and at least a minute;
          an hour without a sync for `--listen`; at least an hour for a first lap),
          `'stopped'` (not running, or paused) or `'unknown'` (nothing recorded yet, e.g.
          `'first sync in progress'`).
        - `last_success`, `last_lap`: ISO-8601 UTC timestamps (or `None`).
        - `consecutive_failures`: laps that failed in a row.
        - `expected_interval_seconds`: how often laps are expected (`--min-seconds`, the
          schedule's period, an hour for `--listen`), or `None`.
        - `message`: what to show next to the state, e.g. `'last good sync 2 min ago'`.

        Examples
        --------
        >>> job = Job('weather', 'sync pipes -c plugin:noaa --loop')
        >>> success, msg = job.start()
        >>> job.health['state']  # Before its first lap ends.
        'unknown'
        """
        return self._core.health()

    def get_daemon_properties(self) -> Dict[str, Any]:
        """
        Return the `properties` dictionary for the job's daemon.
        """
        return self._core.get_daemon_properties()

    @property
    def daemon(self) -> 'Daemon':
        """
        Return the daemon which this job manages.
        """
        from meerschaum.utils.daemon import Daemon
        if self.executor_keys == 'local' and self.exists():
            return Daemon(daemon_id=self.name)

        return Daemon(
            daemon_id=self.name,
            sysargs=self.sysargs,
            label=shlex.join(self.sysargs),
            properties=self.get_daemon_properties(),
        )

    @property
    def began(self) -> Union[datetime, None]:
        """
        The datetime when the job began running.
        """
        return _parse_dt(self._core.began)

    @property
    def ended(self) -> Union[datetime, None]:
        """
        The datetime when the job stopped running.
        """
        return _parse_dt(self._core.ended)

    @property
    def paused(self) -> Union[datetime, None]:
        """
        The datetime when the job was suspended while running.
        """
        return _parse_dt(self._core.paused)

    @property
    def stop_time(self) -> Union[datetime, None]:
        """
        Return the timestamp when the job was manually stopped.
        """
        return _parse_dt(self._core.stop_time)

    @property
    def hidden(self) -> bool:
        """
        Return a bool indicating whether this job should be displayed.
        """
        return self._core.hidden

    def check_restart(self) -> SuccessTuple:
        """
        If `restart` is `True` and the daemon is not running,
        restart the job.
        Do not restart if the job was manually stopped.
        """
        result = self._core.check_restart()
        self._warn()
        return result

    @property
    def label(self) -> str:
        """
        Return the job's Daemon label (joined sysargs).
        """
        return self._core.label

    @property
    def _externally_managed_file(self) -> pathlib.Path:
        """
        Return the path to the externally managed file.
        """
        return pathlib.Path(self._core.paths()['externally_managed'])

    def _set_externally_managed(self):
        """
        Set this job as externally managed.
        """
        self._externally_managed = True
        self._core.set_externally_managed()

    @property
    def _is_externally_managed(self) -> bool:
        """
        Return whether this job is externally managed.
        """
        return self._core.is_externally_managed

    @property
    def env(self) -> Dict[str, str]:
        """
        Return the environment variables to set for the job's process.
        """
        return self._core.env

    @property
    def delete_after_completion(self) -> bool:
        """
        Return whether this job is configured to delete itself after completion.
        """
        return self._core.delete_after_completion

    def __str__(self) -> str:
        return self._core.describe()

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash(self.name)
