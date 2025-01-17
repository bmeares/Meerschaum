#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the Meerschaum abstraction atop daemons.
"""

from __future__ import annotations

import shlex
import asyncio
import pathlib
import sys
import traceback
from functools import partial
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import (
    List, Optional, Union, SuccessTuple, Any, Dict, Callable, TYPE_CHECKING,
)
from meerschaum._internal.entry import entry
from meerschaum.utils.warnings import warn
from meerschaum.config.paths import LOGS_RESOURCES_PATH
from meerschaum.config import get_config
from meerschaum.config.static import STATIC_CONFIG

if TYPE_CHECKING:
    from meerschaum.jobs._Executor import Executor

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
        env: Optional[Dict[str, str]] = None,
        executor_keys: Optional[str] = None,
        delete_after_completion: bool = False,
        _properties: Optional[Dict[str, Any]] = None,
        _rotating_log=None,
        _stdin_file=None,
        _status_hook: Optional[Callable[[], str]] = None,
        _result_hook: Optional[Callable[[], SuccessTuple]] = None,
        _externally_managed: bool = False,
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

        env: Optional[Dict[str, str]], default None
            If provided, set these environment variables in the job's process.

        executor_keys: Optional[str], default None
            If provided, execute the job remotely on an API instance, e.g. 'api:main'.

        delete_after_completion: bool, default False
            If `True`, delete this job when it has finished executing.

        _properties: Optional[Dict[str, Any]], default None
            If provided, use this to patch the daemon's properties.
        """
        from meerschaum.utils.daemon import Daemon
        for char in BANNED_CHARS:
            if char in name:
                raise ValueError(f"Invalid name: ({char}) is not allowed.")

        if isinstance(sysargs, str):
            sysargs = shlex.split(sysargs)

        and_key = STATIC_CONFIG['system']['arguments']['and_key']
        escaped_and_key = STATIC_CONFIG['system']['arguments']['escaped_and_key']
        if sysargs:
            sysargs = [
                (arg if arg != escaped_and_key else and_key)
                for arg in sysargs
            ]

        ### NOTE: 'local' and 'systemd' executors are being coalesced.
        if executor_keys is None:
            from meerschaum.jobs import get_executor_keys_from_context
            executor_keys = get_executor_keys_from_context()

        self.executor_keys = executor_keys
        self.name = name
        try:
            self._daemon = (
                Daemon(daemon_id=name)
                if executor_keys == 'local'
                else None
            )
        except Exception:
            self._daemon = None

        ### Handle any injected dependencies.
        if _rotating_log is not None:
            self._rotating_log = _rotating_log
            if self._daemon is not None:
                self._daemon._rotating_log = _rotating_log

        if _stdin_file is not None:
            self._stdin_file = _stdin_file
            if self._daemon is not None:
                self._daemon._stdin_file = _stdin_file
                self._daemon._blocking_stdin_file_path = _stdin_file.blocking_file_path

        if _status_hook is not None:
            self._status_hook = _status_hook

        if _result_hook is not None:
            self._result_hook = _result_hook

        self._externally_managed = _externally_managed
        self._properties_patch = _properties or {}
        if _externally_managed:
            self._properties_patch.update({'externally_managed': _externally_managed})

        if env:
            self._properties_patch.update({'env': env})

        if delete_after_completion:
            self._properties_patch.update({'delete_after_completion': delete_after_completion})

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
        from meerschaum.config.paths import DAEMON_RESOURCES_PATH

        psutil = mrsm.attempt_import('psutil')
        try:
            process = psutil.Process(pid)
        except psutil.NoSuchProcess as e:
            warn(f"Process with PID {pid} does not exist.", stack=False)
            raise e

        command_args = process.cmdline()
        is_daemon = command_args[1] == '-c'

        if is_daemon:
            daemon_id = command_args[-1].split('daemon_id=')[-1].split(')')[0].replace("'", '')
            root_dir = process.environ().get(STATIC_CONFIG['environment']['root'], None)
            if root_dir is None:
                from meerschaum.config.paths import ROOT_DIR_PATH
                root_dir = ROOT_DIR_PATH
            else:
                root_dir = pathlib.Path(root_dir)
            jobs_dir = root_dir / DAEMON_RESOURCES_PATH.name
            daemon_dir = jobs_dir / daemon_id
            pid_file = daemon_dir / 'process.pid'

            if pid_file.exists():
                with open(pid_file, 'r', encoding='utf-8') as f:
                    daemon_pid = int(f.read())

                if pid != daemon_pid:
                    raise EnvironmentError(f"Differing PIDs: {pid=}, {daemon_pid=}")
            else:
                raise EnvironmentError(f"Is job '{daemon_id}' running?")

            return Job(daemon_id, executor_keys=executor_keys)

        from meerschaum._internal.arguments._parse_arguments import parse_arguments
        from meerschaum.utils.daemon import get_new_daemon_name

        mrsm_ix = 0
        for i, arg in enumerate(command_args):
            if 'mrsm' in arg or 'meerschaum' in arg.lower():
                mrsm_ix = i
                break

        sysargs = command_args[mrsm_ix+1:]
        kwargs = parse_arguments(sysargs)
        name = kwargs.get('name', get_new_daemon_name())
        return Job(name, sysargs, executor_keys=executor_keys)

    def start(self, debug: bool = False) -> SuccessTuple:
        """
        Start the job's daemon.
        """
        if self.executor is not None:
            if not self.exists(debug=debug):
                return self.executor.create_job(
                    self.name,
                    self.sysargs,
                    properties=self.daemon.properties,
                    debug=debug,
                )
            return self.executor.start_job(self.name, debug=debug)

        if self.is_running():
            return True, f"{self} is already running."

        success, msg = self.daemon.run(
            keep_daemon_output=(not self.delete_after_completion),
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
            elif self.stop_time is not None:
                return True, f"{self} will not restart until manually started."

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

        _ = self.daemon._properties.pop('result', None)
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
        input_callback_function: Optional[Callable[[], str]] = None,
        stop_callback_function: Optional[Callable[[SuccessTuple], None]] = None,
        stop_event: Optional[asyncio.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
        accept_input: bool = True,
        debug: bool = False,
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
        def default_input_callback_function():
            return sys.stdin.readline()

        if input_callback_function is None:
            input_callback_function = default_input_callback_function

        if self.executor is not None:
            self.executor.monitor_logs(
                self.name,
                callback_function,
                input_callback_function=input_callback_function,
                stop_callback_function=stop_callback_function,
                stop_on_exit=stop_on_exit,
                accept_input=accept_input,
                strip_timestamps=strip_timestamps,
                debug=debug,
            )
            return

        monitor_logs_coroutine = self.monitor_logs_async(
            callback_function=callback_function,
            input_callback_function=input_callback_function,
            stop_callback_function=stop_callback_function,
            stop_event=stop_event,
            stop_on_exit=stop_on_exit,
            strip_timestamps=strip_timestamps,
            accept_input=accept_input,
        )
        return asyncio.run(monitor_logs_coroutine)

    async def monitor_logs_async(
        self,
        callback_function: Callable[[str], None] = partial(print, end='', flush=True),
        input_callback_function: Optional[Callable[[], str]] = None,
        stop_callback_function: Optional[Callable[[SuccessTuple], None]] = None,
        stop_event: Optional[asyncio.Event] = None,
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
        accept_input: bool = True,
        _logs_path: Optional[pathlib.Path] = None,
        _log=None,
        _stdin_file=None,
        debug: bool = False,
    ):
        """
        Monitor the job's log files and await a callback on new lines.

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
        def default_input_callback_function():
            return sys.stdin.readline()

        if input_callback_function is None:
            input_callback_function = default_input_callback_function

        if self.executor is not None:
            await self.executor.monitor_logs_async(
                self.name,
                callback_function,
                input_callback_function=input_callback_function,
                stop_callback_function=stop_callback_function,
                stop_on_exit=stop_on_exit,
                strip_timestamps=strip_timestamps,
                accept_input=accept_input,
                debug=debug,
            )
            return

        from meerschaum.utils.formatting._jobs import strip_timestamp_from_line

        events = {
            'user': stop_event,
            'stopped': asyncio.Event(),
        }
        combined_event = asyncio.Event()
        emitted_text = False
        stdin_file = _stdin_file if _stdin_file is not None else self.daemon.stdin_file

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

                    if stop_callback_function is not None:
                        try:
                            if asyncio.iscoroutinefunction(stop_callback_function):
                                await stop_callback_function(self.result)
                            else:
                                stop_callback_function(self.result)
                        except asyncio.exceptions.CancelledError:
                            break
                        except Exception:
                            warn(traceback.format_exc())

                    if stop_on_exit:
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

                try:
                    print('', end='', flush=True)
                    if asyncio.iscoroutinefunction(input_callback_function):
                        data = await input_callback_function()
                    else:
                        data = input_callback_function()
                except KeyboardInterrupt:
                    break
                if not data.endswith('\n'):
                    data += '\n'

                stdin_file.write(data)
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
                done, pending = await asyncio.wait(
                    event_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
            except asyncio.exceptions.CancelledError:
                pass
            finally:
                combined_event.set()

        check_job_status_task = asyncio.create_task(check_job_status())
        check_blocking_on_input_task = asyncio.create_task(check_blocking_on_input())
        combine_events_task = asyncio.create_task(combine_events())

        log = _log if _log is not None else self.daemon.rotating_log
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

        tasks = (
            [check_job_status_task]
            + ([check_blocking_on_input_task] if accept_input else [])
            + [combine_events_task]
        )
        try:
            _ = asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.exceptions.CancelledError:
            raise
        except Exception:
            warn(f"Failed to run async checks:\n{traceback.format_exc()}")

        watchfiles = mrsm.attempt_import('watchfiles')
        async for changes in watchfiles.awatch(
            _logs_path or LOGS_RESOURCES_PATH,
            stop_event=combined_event,
        ):
            for change in changes:
                file_path_str = change[1]
                file_path = pathlib.Path(file_path_str)
                latest_subfile_path = log.get_latest_subfile_path()
                if latest_subfile_path != file_path:
                    continue

                await emit_latest_lines()

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
        self.daemon.stdin_file.write(data)

    @property
    def executor(self) -> Union[Executor, None]:
        """
        If the job is remote, return the connector to the remote API instance.
        """
        return (
            mrsm.get_connector(self.executor_keys)
            if self.executor_keys != 'local'
            else None
        )

    @property
    def status(self) -> str:
        """
        Return the running status of the job's daemon.
        """
        if '_status_hook' in self.__dict__:
            return self._status_hook()

        if self.executor is not None:
            return self.executor.get_job_status(self.name)

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
        if self.executor is not None:
            return self.executor.get_job_metadata(self.name).get('restart', False)

        return self.daemon.properties.get('restart', False)

    @property
    def result(self) -> SuccessTuple:
        """
        Return the `SuccessTuple` when the job has terminated.
        """
        if self.is_running():
            return True, f"{self} is running."

        if '_result_hook' in self.__dict__:
            return self._result_hook()

        if self.executor is not None:
            return (
                self.executor.get_job_metadata(self.name)
                .get('result', (False, "No result available."))
            )

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

        if self.executor is not None:
            return self.executor.get_job_metadata(self.name).get('sysargs', [])

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
        if '_rotating_log' in self.__dict__:
            self._daemon._rotating_log = self._rotating_log

        if '_stdin_file' in self.__dict__:
            self._daemon._stdin_file = self._stdin_file
            self._daemon._blocking_stdin_file_path = self._stdin_file.blocking_file_path

        return self._daemon

    @property
    def began(self) -> Union[datetime, None]:
        """
        The datetime when the job began running.
        """
        if self.executor is not None:
            began_str = self.executor.get_job_began(self.name)
            if began_str is None:
                return None
            return (
                datetime.fromisoformat(began_str)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )

        began_str = self.daemon.properties.get('process', {}).get('began', None)
        if began_str is None:
            return None

        return datetime.fromisoformat(began_str)

    @property
    def ended(self) -> Union[datetime, None]:
        """
        The datetime when the job stopped running.
        """
        if self.executor is not None:
            ended_str = self.executor.get_job_ended(self.name)
            if ended_str is None:
                return None
            return (
                datetime.fromisoformat(ended_str)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )

        ended_str = self.daemon.properties.get('process', {}).get('ended', None)
        if ended_str is None:
            return None

        return datetime.fromisoformat(ended_str)

    @property
    def paused(self) -> Union[datetime, None]:
        """
        The datetime when the job was suspended while running.
        """
        if self.executor is not None:
            paused_str = self.executor.get_job_paused(self.name)
            if paused_str is None:
                return None
            return (
                datetime.fromisoformat(paused_str)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )

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

        stop_data = self.daemon._read_stop_file()
        if not stop_data:
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
        return (
            self.name.startswith('_')
            or self.name.startswith('.')
            or self._is_externally_managed
        )

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
        from meerschaum._internal.arguments import compress_pipeline_sysargs
        sysargs = compress_pipeline_sysargs(self.sysargs)
        return shlex.join(sysargs).replace(' + ', '\n+ ').replace(' : ', '\n: ').lstrip().rstrip()

    @property
    def _externally_managed_file(self) -> pathlib.Path:
        """
        Return the path to the externally managed file.
        """
        return self.daemon.path / '.externally-managed'

    def _set_externally_managed(self):
        """
        Set this job as externally managed.
        """
        self._externally_managed = True
        try:
            self._externally_managed_file.parent.mkdir(exist_ok=True, parents=True)
            self._externally_managed_file.touch()
        except Exception as e:
            warn(e)

    @property
    def _is_externally_managed(self) -> bool:
        """
        Return whether this job is externally managed.
        """
        return self.executor_keys in (None, 'local') and (
            self._externally_managed or self._externally_managed_file.exists()
        )

    @property
    def env(self) -> Dict[str, str]:
        """
        Return the environment variables to set for the job's process.
        """
        if '_env' in self.__dict__:
            return self.__dict__['_env']

        _env = self.daemon.properties.get('env', {})
        default_env = {
            'PYTHONUNBUFFERED': '1',
            'LINES': str(get_config('jobs', 'terminal', 'lines')),
            'COLUMNS': str(get_config('jobs', 'terminal', 'columns')),
            STATIC_CONFIG['environment']['noninteractive']: 'true',
        }
        self._env = {**default_env, **_env}
        return self._env

    @property
    def delete_after_completion(self) -> bool:
        """
        Return whether this job is configured to delete itself after completion.
        """
        if '_delete_after_completion' in self.__dict__:
            return self.__dict__.get('_delete_after_completion', False)

        self._delete_after_completion = self.daemon.properties.get('delete_after_completion', False)
        return self._delete_after_completion

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
