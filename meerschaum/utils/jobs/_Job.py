#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the Meerschaum abstraction atop daemons.
"""

import shlex
import asyncio
import threading
import json
import pathlib
import os
import sys
import traceback
from functools import partial
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import List, Optional, Union, SuccessTuple, Any, Dict, Callable
from meerschaum._internal.entry import entry
from meerschaum.utils.warnings import warn
from meerschaum.config.paths import LOGS_RESOURCES_PATH
from meerschaum.config import get_config

BANNED_CHARS: List[str] = [
    ',', ';', "'", '"',
]
RESTART_FLAGS: List[str] = [
    '-s',
    '--restart',
    '--loop',
    '--schedule',
    '--cron',
]

class StopMonitoringLogs(Exception):
    """
    Raise this exception to stop the logs monitoring.
    """


class Job:
    """
    Manage a `meerschaum.utils.daemon.Daemon`, locally or remotely via the API.
    """

    def __init__(
        self,
        name: str,
        sysargs: Union[List[str], str, None] = None,
        executor_keys: Optional[str] = None,
        _properties: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a new job to manage a `meerschaum.utils.daemon.Daemon`.

        Parameters
        ----------
        name: str
            The name of the job to be created.
            This will also be used as the Daemon ID.

        sysargs: Union[List[str], str, None], default None
            The sysargs of the command to be executed, e.g. 'start api'.

        executor_keys: Optional[str], default None
            If provided, execute the job remotely on an API instance, e.g. 'api:main'.

        _properties: Optional[Dict[str, Any]], default None
            If provided, use this to patch the daemon's properties.
        """
        from meerschaum.utils.daemon import Daemon
        for char in BANNED_CHARS:
            if char in name:
                raise ValueError(f"Invalid name: ({char}) is not allowed.")

        if isinstance(sysargs, str):
            sysargs = shlex.split(sysargs)

        if executor_keys == 'local':
            executor_keys = None
        self.executor_keys = executor_keys
        self.name = name
        try:
            self._daemon = (
                Daemon(daemon_id=name)
                if executor_keys is not None
                else None
            )
        except Exception:
            self._daemon = None

        self._properties_patch = _properties or {}

        daemon_sysargs = (
            self._daemon.properties.get('target', {}).get('args', [None])[0]
            if self._daemon is not None
            else None
        )

        if daemon_sysargs and sysargs and daemon_sysargs != sysargs:
            warn("Given sysargs differ from existing sysargs.")

        self._sysargs = [
            arg
            for arg in (daemon_sysargs or sysargs or [])
            if arg not in ('-d', '--daemon')
        ]
        for restart_flag in RESTART_FLAGS:
            if restart_flag in self._sysargs:
                self._properties_patch.update({'restart': True})
                break

    def start(self, debug: bool = False) -> SuccessTuple:
        """
        Start the job's daemon.
        """
        if self.executor is not None:
            if not self.exists(debug=debug):
                return self.executor.create_job(self.name, self.sysargs, debug=debug)
            return self.executor.start_job(self.name, debug=debug)

        if self.is_running():
            return True, f"{self} is already running."

        success, msg = self.daemon.run(
            keep_daemon_output=True,
            allow_dirty_run=True,
        )
        if not success:
            return success, msg

        return success, f"Started {self}."

    def stop(self, timeout_seconds: Optional[int] = None, debug: bool = False) -> SuccessTuple:
        """
        Stop the job's daemon.
        """
        if self.executor is not None:
            return self.executor.stop_job(self.name, debug=debug)

        if self.daemon.status == 'stopped':
            if not self.restart:
                return True, f"{self} is not running."

        quit_success, quit_msg = self.daemon.quit(timeout=timeout_seconds)
        if quit_success:
            return quit_success, f"Stopped {self}."

        warn(
            f"Failed to gracefully quit {self}.",
            stack=False,
        )
        kill_success, kill_msg = self.daemon.kill(timeout=timeout_seconds)
        if not kill_success:
            return kill_success, kill_msg

        return kill_success, f"Killed {self}."

    def pause(self, timeout_seconds: Optional[int] = None, debug: bool = False) -> SuccessTuple:
        """
        Pause the job's daemon.
        """
        if self.executor is not None:
            return self.executor.pause_job(self.name, debug=debug)

        pause_success, pause_msg = self.daemon.pause(timeout=timeout_seconds)
        if not pause_success:
            return pause_success, pause_msg

        return pause_success, f"Paused {self}."

    def delete(self, debug: bool = False) -> SuccessTuple:
        """
        Delete the job and its daemon.
        """
        if self.executor is not None:
            return self.executor.delete_job(self.name, debug=debug)

        if self.is_running():
            stop_success, stop_msg = self.stop()
            if not stop_success:
                return stop_success, stop_msg

        cleanup_success, cleanup_msg = self.daemon.cleanup()
        if not cleanup_success:
            return cleanup_success, cleanup_msg

        return cleanup_success, f"Deleted {self}."

    def is_running(self) -> bool:
        """
        Determine whether the job's daemon is running.
        """
        return self.status == 'running'

    def exists(self, debug: bool = False) -> bool:
        """
        Determine whether the job exists.
        """
        if self.executor is not None:
            return self.executor.get_job_exists(self.name, debug=debug)

        return self.daemon.path.exists()

    def get_logs(self) -> Union[str, None]:
        """
        Return the output text of the job's daemon.
        """
        if self.executor is not None:
            return self.executor.get_logs(self.name)

        return self.daemon.log_text

    def monitor_logs(
        self,
        callback_function: Callable[[str], None] = partial(print, end=''),
        stop_event: Optional[threading.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
    ):
        """
        Monitor the job's log files and execute a callback on new lines.

        Parameters
        ----------
        callback_function: Callable[[str], None], default partial(print, end='')
            The callback to execute as new data comes in.
            Defaults to printing the output directly to `stdout`.

        stop_event: Optional[asyncio.Event], default None
            If provided, stop monitoring when this event is set.
            You may instead raise `meerschaum.utils.jobs.StopMonitoringLogs`
            from within `callback_function` to stop monitoring.

        stop_on_exit: bool, default False
            If `True`, stop monitoring when the job stops.

        strip_timestamps: bool, default False
            If `True`, remove leading timestamps from lines.

        """
        if self.executor is not None:
            self.executor.monitor_logs(self.name, callback_function)
            return

        monitor_logs_coroutine = self.monitor_logs_async(
            callback_function=callback_function,
            stop_event=stop_event,
            stop_on_exit=stop_on_exit,
            strip_timestamps=strip_timestamps,
        )
        nest_asyncio = mrsm.attempt_import('nest_asyncio')
        nest_asyncio.apply()
        return asyncio.run(monitor_logs_coroutine)


    async def monitor_logs_async(
        self,
        callback_function: Callable[[str], None] = partial(print, end=''),
        stop_event: Optional[asyncio.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
    ):
        """
        Monitor the job's log files and await a callback on new lines.

        Parameters
        ----------
        callback_function: Callable[[str], None], default partial(print, end='')
            The callback to execute as new data comes in.
            Defaults to printing the output directly to `stdout`.

        stop_event: Optional[asyncio.Event], default None
            If provided, stop monitoring when this event is set.
            You may instead raise `meerschaum.utils.jobs.StopMonitoringLogs`
            from within `callback_function` to stop monitoring.

        stop_on_exit: bool, default False
            If `True`, stop monitoring when the job stops.

        strip_timestamps: bool, default False
            If `True`, remove leading timestamps from lines.
        """
        if self.executor is not None:
            await self.executor.monitor_logs_async(self.name, callback_function)
            return

        from meerschaum.utils.prompt import prompt
        from meerschaum.utils.formatting._jobs import strip_timestamp_from_line

        events = {
            'user': stop_event,
            'stopped': (asyncio.Event() if stop_on_exit else None),
        }
        combined_event = asyncio.Event()
        emitted_text = False

        async def check_job_status():
            nonlocal emitted_text
            stopped_event = events.get('stopped', None)
            if stopped_event is None:
                return
            sleep_time = 0.1
            while sleep_time < 60:
                if self.status == 'stopped':
                    if not emitted_text:
                        await asyncio.sleep(sleep_time)
                        sleep_time = round(sleep_time * 1.1, 2)
                        continue
                    events['stopped'].set()
                    break
                await asyncio.sleep(0.1)

        async def check_blocking_on_input():
            while True:
                if not emitted_text or not self.is_blocking_on_stdin():
                    try:
                        await asyncio.sleep(0.1)
                    except asyncio.exceptions.CancelledError:
                        break
                    continue

                if not self.is_running():
                    break

                await emit_latest_lines()

                ### TODO parametrize stdin callback
                try:
                    print('Waiting for input...')
                    data = await asyncio.get_event_loop().run_in_executor(None, prompt, '', {'icon': False})
                    print(f'Input received: {data}')
                except KeyboardInterrupt:
                    break
                if not data.endswith('\n'):
                    data += '\n'
                self.daemon.stdin_file.write(data)
                await asyncio.sleep(0.1)

        async def combine_events():
            event_tasks = [
                asyncio.create_task(event.wait())
                for event in events.values()
                if event is not None
            ]
            if not event_tasks:
                return

            try:
                await asyncio.wait(
                    event_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.exceptions.CancelledError:
                pass
            finally:
                combined_event.set()

        check_job_status_task = asyncio.create_task(check_job_status())
        check_blocking_on_input_task = asyncio.create_task(check_blocking_on_input())
        combine_events_task = asyncio.create_task(combine_events())

        log = self.daemon.rotating_log
        lines_to_show = get_config('jobs', 'logs', 'lines_to_show')

        async def emit_latest_lines():
            nonlocal emitted_text
            lines = log.readlines()
            for line in lines[(-1 * lines_to_show):]:
                if stop_event is not None and stop_event.is_set():
                    return

                if strip_timestamps:
                    line = strip_timestamp_from_line(line)

                try:
                    if asyncio.iscoroutinefunction(callback_function):
                        await callback_function(line)
                    else:
                        callback_function(line)
                    emitted_text = True
                except StopMonitoringLogs:
                    return
                except Exception:
                    warn(f"Error in logs callback:\n{traceback.format_exc()}")

        await emit_latest_lines()
        try:
            asyncio.gather(check_job_status_task, check_blocking_on_input_task, combine_events_task)
        except Exception as e:
            warn(f"Failed to run async checks:\n{traceback.format_exc()}")

        watchfiles = mrsm.attempt_import('watchfiles')
        async for changes in watchfiles.awatch(
            LOGS_RESOURCES_PATH,
            stop_event=combined_event,
        ):
            for change in changes:
                file_path_str = change[1]
                file_path = pathlib.Path(file_path_str)
                latest_subfile_path = log.get_latest_subfile_path()
                if latest_subfile_path != file_path:
                    continue

                lines = log.readlines()
                for line in lines:
                    if strip_timestamps:
                        line = strip_timestamp_from_line(line)
                    try:
                        if asyncio.iscoroutinefunction(callback_function):
                            await callback_function(line)
                        else:
                            callback_function(line)
                        emitted_text = True
                    except RuntimeError:
                        return
                    except StopMonitoringLogs:
                        return
                    except Exception:
                        warn(f"Error in logs callback:\n{traceback.format_exc()}")
                        return

        await emit_latest_lines()

    def is_blocking_on_stdin(self, debug: bool = False) -> bool:
        """
        Return whether a job's daemon is blocking on stdin.
        """
        if self.executor is not None:
            return self.executor.get_job_is_blocking_on_stdin(self.name, debug=debug)

        return self.is_running() and self.daemon.blocking_stdin_file_path.exists()

    def write_stdin(self, data):
        """
        Write to a job's daemon's `stdin`.
        """
        if self.executor is not None:
            pass

        self.daemon.stdin_file.write(data)

    @property
    def executor(self) -> Union['APIConnector', None]:
        """
        If the job is remote, return the connector to the remote API instance.
        """
        return (
            mrsm.get_connector(self.executor_keys)
            if self.executor_keys is not None
            else None
        )

    @property
    def status(self) -> str:
        """
        Return the running status of the job's daemon.
        """
        if self.executor is not None:
            return self.executor.get_job_metadata(
                self.name
            ).get('daemon', {}).get('status', 'stopped')

        return self.daemon.status

    @property
    def pid(self) -> Union[int, None]:
        """
        Return the PID of the job's dameon.
        """
        if self.executor is not None:
            return self.executor.get_job_metadata(self.name).get('daemon', {}).get('pid', None)

        return self.daemon.pid

    @property
    def restart(self) -> bool:
        """
        Return whether to restart a stopped job.
        """
        return self.daemon.properties.get('restart', False)

    @property
    def result(self) -> SuccessTuple:
        """
        Return the `SuccessTuple` when the job has terminated.
        """
        if self.is_running():
            return True, f"{self} is running."

        _result = self.daemon.properties.get('result', None)
        if _result is None:
            return False, "No result available."

        return tuple(_result)

    @property
    def sysargs(self) -> List[str]:
        """
        Return the sysargs to use for the Daemon.
        """
        if self._sysargs:
            return self._sysargs

        #  target_args = self.daemon.properties.get('target', {}).get('args', None)
        target_args = self.daemon.target_args
        if target_args is None:
            return []
        self._sysargs = target_args[0] if len(target_args) > 0 else []
        return self._sysargs

    @property
    def daemon(self) -> 'Daemon':
        """
        Return the daemon which this job manages.
        """
        from meerschaum.utils.daemon import Daemon
        if self._daemon is not None and self.executor is None and self._sysargs:
            return self._daemon

        remote_properties = (
            {}
            if self.executor is None
            else self.executor.get_job_properties(self.name)
        )
        properties = {**remote_properties, **self._properties_patch}

        self._daemon = Daemon(
            target=entry,
            target_args=[self._sysargs],
            target_kw={},
            daemon_id=self.name,
            label=shlex.join(self._sysargs),
            properties=properties,
        )

        return self._daemon

    @property
    def began(self) -> Union[datetime, None]:
        """
        The datetime when the job began running.
        """
        began_str = self.daemon.properties.get('process', {}).get('began', None)
        if began_str is None:
            return None

        return datetime.fromisoformat(began_str)

    @property
    def ended(self) -> Union[datetime, None]:
        """
        The datetime when the job stopped running.
        """
        ended_str = self.daemon.properties.get('process', {}).get('ended', None)
        if ended_str is None:
            return None

        return datetime.fromisoformat(ended_str)

    @property
    def paused(self) -> Union[datetime, None]:
        """
        The datetime when the job was suspended while running.
        """
        paused_str = self.daemon.properties.get('process', {}).get('paused', None)
        if paused_str is None:
            return None

        return datetime.fromisoformat(paused_str)

    @property
    def stop_time(self) -> Union[datetime, None]:
        """
        Return the timestamp when the job was manually stopped.
        """
        if self.executor is not None:
            return self.executor.get_job_stop_time(self.name)

        if not self.daemon.stop_path.exists():
            return None

        try:
            with open(self.daemon.stop_path, 'r', encoding='utf-8') as f:
                stop_data = json.load(f)
        except Exception as e:
            warn(f"Failed to read stop file for {self}:\n{e}")
            return None

        stop_time_str = stop_data.get('stop_time', None)
        if not stop_time_str:
            warn(f"Could not read stop time for {self}.")
            return None

        return datetime.fromisoformat(stop_time_str)

    @property
    def hidden(self) -> bool:
        """
        Return a bool indicating whether this job should be displayed.
        """
        return self.name.startswith('_') or self.name.startswith('.')

    def check_restart(self) -> SuccessTuple:
        """
        If `restart` is `True` and the daemon is not running,
        restart the job.
        Do not restart if the job was manually stopped.
        """
        if self.is_running():
            return True, f"{self} is running."

        if not self.restart:
            return True, f"{self} does not need to be restarted."

        if self.stop_time is not None:
            return True, f"{self} was manually stopped."

        return self.start()

    @property
    def label(self) -> str:
        """
        Return the job's Daemon label (joined sysargs).
        """
        return shlex.join(self.sysargs)

    def __str__(self) -> str:
        sysargs = self.sysargs
        sysargs_str = shlex.join(sysargs) if sysargs else ''
        job_str = f'Job("{self.name}"'
        if sysargs_str:
            job_str += f', "{sysargs_str}"'

        job_str += ')'
        return job_str

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash(self.name)
