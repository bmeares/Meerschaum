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
import traceback
from functools import partial
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import List, Optional, Union, SuccessTuple, Any, Dict, Callable
from meerschaum._internal.entry import entry
from meerschaum._internal.arguments._parse_arguments import parse_arguments
from meerschaum.utils.warnings import warn
from meerschaum.utils.misc import is_int
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
    ):
        """
        Monitor the job's log files and execute a callback on new lines.

        Parameters
        ----------
        callback_function: Callable[[str], None], default partial(print, end='')
            The callback to execute as new data comes in.
            Defaults to printing the output directly to `stdout`.

        stop_event: Optional[threading.Event], default None
            If provided, stop monitoring when this event is set.
            You may instead raise `meerschaum.utils.jobs.StopMonitoringLogs`
            from within `callback_function` to stop monitoring.
        """
        if self.executor is not None:
            self.executor.monitor_logs(self.name, callback_function)
            return

        log = self.daemon.rotating_log
        lines = log.readlines()
        lines_to_show = get_config('jobs', 'logs', 'lines_to_show')
        for line in lines[(-1 * lines_to_show):]:
            try:
                callback_function(line)
            except StopMonitoringLogs:
                return
            except Exception:
                warn(f"Error in logs callback:\n{traceback.format_exc()}")

        watchfiles = mrsm.attempt_import('watchfiles')
        for changes in watchfiles.watch(
            LOGS_RESOURCES_PATH,
            stop_event=stop_event,
        ):
            for change in changes:
                file_path_str = change[1]
                file_path = pathlib.Path(file_path_str)
                latest_subfile_path = log.get_latest_subfile_path()
                if latest_subfile_path != file_path:
                    continue

                text = log.read()
                try:
                    callback_function(text)
                except StopMonitoringLogs:
                    return
                except Exception:
                    warn(f"Error in logs callback:\n{traceback.format_exc()}")


    async def monitor_logs_async(
        self,
        callback_function: Callable[[str], None] = partial(print, end=''),
        stop_event: Optional[asyncio.Event] = None,
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
        """
        if self.executor is not None:
            await self.executor.monitor_logs_async(self.name, callback_function)
            return

        log = self.daemon.rotating_log
        lines = log.readlines()
        lines_to_show = get_config('jobs', 'logs', 'lines_to_show')
        for line in lines[(-1 * lines_to_show):]:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                if asyncio.iscoroutinefunction(callback_function):
                    await callback_function(line)
                else:
                    callback_function(line)
            except StopMonitoringLogs:
                return
            except Exception:
                warn(f"Error in logs callback:\n{traceback.format_exc()}")

        watchfiles = mrsm.attempt_import('watchfiles')
        async for changes in watchfiles.awatch(
            LOGS_RESOURCES_PATH,
            stop_event=stop_event,
        ):
            for change in changes:
                file_path_str = change[1]
                file_path = pathlib.Path(file_path_str)
                latest_subfile_path = log.get_latest_subfile_path()
                if latest_subfile_path != file_path:
                    continue

                lines = log.readlines()
                for line in lines:
                    try:
                        if asyncio.iscoroutinefunction(callback_function):
                            await callback_function(line)
                        else:
                            callback_function(line)
                    except StopMonitoringLogs:
                        return
                    except Exception:
                        warn(f"Error in logs callback:\n{traceback.format_exc()}")
                        return

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

        self._sysargs = self.daemon.properties.get('target', {}).get('args', [[]])[0]
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
