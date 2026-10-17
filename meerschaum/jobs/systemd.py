#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Manage `meerschaum.jobs.Job` via `systemd`.

`SystemdExecutor` is a façade over the native executor (crate `meerschaum-jobs`, bound as
`meerschaum._core.Executor`): the unit files, the `systemctl --user` calls and the job state are
Rust.
"""

import asyncio
import pathlib
import shlex
from datetime import datetime
from functools import partial

import meerschaum as mrsm
from meerschaum.jobs import Job, Executor, make_executor, _native
from meerschaum.utils.typing import Dict, Any, List, SuccessTuple, Union, Optional
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn, dprint

JOB_METADATA_CACHE_SECONDS: int = STATIC_CONFIG['api']['jobs']['metadata_cache_seconds']


@make_executor
class SystemdExecutor(Executor):
    """
    Execute Meerschaum jobs via `systemd`.
    """

    @property
    def _native_executor(self):
        """The native executor (built once per object)."""
        executor = self.__dict__.get('_native_executor_object', None)
        if executor is None:
            executor = _native.core().Executor('systemd')
            self.__dict__['_native_executor_object'] = executor
        return executor

    def _warn(self) -> None:
        _native.emit_warnings(self._native_executor.take_warnings())

    def get_job_names(self, debug: bool = False) -> List[str]:
        """
        Return a list of existing jobs, including hidden ones.
        """
        return self._native_executor.get_job_names()

    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """
        return self._native_executor.get_job_exists(name)

    def get_jobs(self, debug: bool = False) -> Dict[str, Job]:
        """
        Return a dictionary of `systemd` Jobs (including hidden jobs).
        """
        return {
            name: Job(name, executor_keys=str(self))
            for name in self.get_job_names(debug=debug)
        }

    def get_service_name(self, name: str, debug: bool = False) -> str:
        """
        Return a job's service name.
        """
        return self._native_executor.get_service_name(name)

    def get_service_job_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to the job's service directory.
        """
        return pathlib.Path(self._native_executor.get_service_job_path(name))

    def get_service_symlink_file_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to where to create the service symlink.
        """
        return pathlib.Path(self._native_executor.get_service_symlink_file_path(name))

    def get_service_file_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to a job's service file.
        """
        return pathlib.Path(self._native_executor.get_service_file_path(name))

    def get_service_logs_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to direct service logs to.
        """
        return pathlib.Path(self._native_executor.get_service_logs_path(name))

    def get_socket_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to the FIFO file.
        """
        return pathlib.Path(self._native_executor.get_socket_path(name))

    def get_result_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to the result file.
        """
        return pathlib.Path(self._native_executor.get_result_path(name))

    def get_service_file_text(self, name: str, sysargs: List[str], debug: bool = False) -> str:
        """
        Return the contents of the unit file.
        """
        return self._native_executor.get_service_file_text(name, sysargs)

    def get_socket_file_text(self, name: str, debug: bool = False) -> str:
        """
        Return the contents of the socket file.
        """
        return self._native_executor.get_socket_file_text(name)

    def get_hidden_job(
        self,
        name: str,
        sysargs: Optional[List[str]] = None,
        properties: Optional[Dict[str, Any]] = None,
        debug: bool = False,
    ):
        """
        Return the hidden "sister" job to store a job's parameters.
        """
        job = Job(
            name,
            sysargs,
            executor_keys='local',
            _properties=properties,
            _status_hook=partial(self.get_job_status, name),
            _result_hook=partial(self.get_job_result, name),
            _externally_managed=True,
        )
        return job

    def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return metadata about a job.
        """
        return self._native_executor.get_job_metadata(name)

    def get_job_restart(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job restarts.
        """
        return self._native_executor.get_job_restart(name)

    def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return the properties for a job.
        """
        return self._native_executor.get_job_properties(name)

    def get_job_status(self, name: str, debug: bool = False) -> str:
        """
        Return the job's service status.
        """
        return self._native_executor.get_job_status(name)

    def get_job_pid(self, name: str, debug: bool = False) -> Union[int, None]:
        """
        Return the job's service PID.
        """
        return self._native_executor.get_job_pid(name)

    def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job began running.
        """
        return self._native_executor.get_job_began(name)

    def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job stopped running.
        """
        return self._native_executor.get_job_ended(name)

    def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job was paused.
        """
        return self._native_executor.get_job_paused(name)

    def get_job_result(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Return the job's result SuccessTuple.
        """
        result = self._native_executor.get_job_result(name)
        if result is None:
            return False, "No result available."
        return result

    def get_job_sysargs(self, name: str, debug: bool = False) -> Union[List[str], None]:
        """
        Return the sysargs from the service file.
        """
        return self._native_executor.get_job_sysargs(name)

    def run_command(
        self,
        command_args: List[str],
        as_output: bool = False,
        debug: bool = False,
    ) -> Union[SuccessTuple, str]:
        """
        Run a `systemd` command and return success.

        Parameters
        ----------
        command_args: List[str]
            The command to pass to `systemctl --user`.

        as_output: bool, default False
            If `True`, return the process stdout output.
            Defaults to a `SuccessTuple`.

        Returns
        -------
        A `SuccessTuple` indicating success or a str for the process output.
        """
        if command_args[:2] != ['systemctl', '--user']:
            command_args = ['systemctl', '--user'] + command_args

        if debug:
            dprint(shlex.join(command_args))

        return self._native_executor.run_command(list(command_args[2:]), as_output)

    def get_job_stdin_file(self, name: str, debug: bool = False):
        """
        Return a `StdinFile` for the job.
        """
        from meerschaum.utils.daemon import StdinFile
        if '_stdin_files' not in self.__dict__:
            self._stdin_files: Dict[str, StdinFile] = {}

        if name not in self._stdin_files:
            socket_path = self.get_socket_path(name, debug=debug)
            socket_path.parent.mkdir(parents=True, exist_ok=True)
            self._stdin_files[name] = StdinFile(socket_path)

        return self._stdin_files[name]

    def create_job(
        self,
        name: str,
        sysargs: List[str],
        properties: Optional[Dict[str, Any]] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Create a job as a service to be run by `systemd`.
        """
        result = self._native_executor.create_job(name, sysargs, properties)
        self._warn()
        return result

    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Start a job.
        """
        result = self._native_executor.start_job(name)
        self._warn()
        return result

    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job.
        """
        result = self._native_executor.stop_job(name)
        self._warn()
        return result

    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Pause a job.
        """
        result = self._native_executor.pause_job(name)
        self._warn()
        return result

    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Delete a job's service.
        """
        result = self._native_executor.delete_job(name)
        self._warn()
        return result

    def get_logs(self, name: str, debug: bool = False) -> str:
        """
        Return a job's journal logs.
        """
        return self._native_executor.get_logs(name) or ''

    def get_job_stop_time(self, name: str, debug: bool = False) -> Union[datetime, None]:
        """
        Return a job's stop time.
        """
        stop_time = self._native_executor.get_job_stop_time(name)
        return datetime.fromisoformat(stop_time) if stop_time else None

    def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job is blocking on stdin.
        """
        return self._native_executor.get_job_is_blocking_on_stdin(name)

    def get_job_prompt_kwargs(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return the kwargs to the blocking prompt.
        """
        return self._native_executor.get_job_prompt_kwargs(name)

    def get_job_rotating_file(self, name: str, debug: bool = False):
        """
        Return a `RotatingFile` for the job's log output.
        """
        from meerschaum.utils.daemon import RotatingFile
        service_logs_path = self.get_service_logs_path(name, debug=debug)
        return RotatingFile(service_logs_path)

    async def monitor_logs_async(
        self,
        name: str,
        *args,
        debug: bool = False,
        **kwargs
    ):
        """
        Monitor a job's output.
        """
        job = Job(name, executor_keys=str(self))
        kwargs.update({'debug': debug})
        await job.monitor_logs_async(*args, **kwargs)

    def monitor_logs(self, *args, **kwargs):
        """
        Monitor a job's output.
        """
        asyncio.run(self.monitor_logs_async(*args, **kwargs))
