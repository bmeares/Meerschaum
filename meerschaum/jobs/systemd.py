#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Manage `meerschaum.jobs.Job` via `systemd`.
"""

import os
import pathlib
import shlex
import sys
import asyncio
import json
import time
import traceback
import shutil
from datetime import datetime, timezone
from functools import partial

import meerschaum as mrsm
from meerschaum.jobs import Job, Executor, make_executor
from meerschaum.utils.typing import Dict, Any, List, SuccessTuple, Union, Optional, Callable
from meerschaum.config import get_config
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn, dprint
from meerschaum._internal.arguments._parse_arguments import parse_arguments

JOB_METADATA_CACHE_SECONDS: int = STATIC_CONFIG['api']['jobs']['metadata_cache_seconds']


@make_executor
class SystemdExecutor(Executor):
    """
    Execute Meerschaum jobs via `systemd`.
    """

    def get_job_names(self, debug: bool = False) -> List[str]:
        """
        Return a list of existing jobs, including hidden ones.
        """
        from meerschaum.config.paths import SYSTEMD_USER_RESOURCES_PATH
        return [
            service_name[len('mrsm-'):(-1 * len('.service'))]
            for service_name in os.listdir(SYSTEMD_USER_RESOURCES_PATH)
            if (
                service_name.startswith('mrsm-')
                and service_name.endswith('.service')
                ### Check for broken symlinks.
                and (SYSTEMD_USER_RESOURCES_PATH / service_name).exists()
            )
        ]

    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """
        user_services = self.get_job_names(debug=debug)
        if debug:
            dprint(f'Existing services: {user_services}')
        return name in user_services

    def get_jobs(self, debug: bool = False) -> Dict[str, Job]:
        """
        Return a dictionary of `systemd` Jobs (including hidden jobs).
        """
        user_services = self.get_job_names(debug=debug)
        jobs = {
            name: Job(name, executor_keys=str(self))
            for name in user_services
        }
        return {
            name: job
            for name, job in jobs.items()
        }

    def get_service_name(self, name: str, debug: bool = False) -> str:
        """
        Return a job's service name.
        """
        return f"mrsm-{name.replace(' ', '-')}.service"

    def get_service_job_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path for the job's files under the root directory.
        """
        from meerschaum.config.paths import SYSTEMD_JOBS_RESOURCES_PATH
        return SYSTEMD_JOBS_RESOURCES_PATH / name

    def get_service_symlink_file_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to where to create the service symlink.
        """
        from meerschaum.config.paths import SYSTEMD_USER_RESOURCES_PATH
        return SYSTEMD_USER_RESOURCES_PATH / self.get_service_name(name, debug=debug)

    def get_service_file_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to a Job's service file.
        """
        return (
            self.get_service_job_path(name, debug=debug)
            / self.get_service_name(name, debug=debug)
        )

    def get_service_logs_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to direct service logs to.
        """
        from meerschaum.config.paths import SYSTEMD_LOGS_RESOURCES_PATH
        return SYSTEMD_LOGS_RESOURCES_PATH / (self.get_service_name(name, debug=debug) + '.log')

    def get_socket_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to the FIFO file.
        """
        return (
            self.get_service_job_path(name, debug=debug)
            / (self.get_service_name(name, debug=debug) + '.stdin')
        )

    def get_result_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to the result file.
        """
        return (
            self.get_service_job_path(name, debug=debug)
            / (self.get_service_name(name, debug=debug) + '.result.json')
        )

    def get_service_file_text(self, name: str, sysargs: List[str], debug: bool = False) -> str:
        """
        Return the contents of the unit file.
        """
        service_logs_path = self.get_service_logs_path(name, debug=debug)
        socket_path = self.get_socket_path(name, debug=debug)
        result_path = self.get_result_path(name, debug=debug)
        job = self.get_hidden_job(name, debug=debug)

        sysargs_str = shlex.join(sysargs)
        exec_str = f'{sys.executable} -m meerschaum {sysargs_str}'
        mrsm_env_var_names = set([var for var in STATIC_CONFIG['environment'].values()])
        mrsm_env_vars = {
            key: val
            for key, val in os.environ.items()
            if key in mrsm_env_var_names
        }

        ### Add new environment variables for the service process.
        mrsm_env_vars.update({
            STATIC_CONFIG['environment']['daemon_id']: name,
            STATIC_CONFIG['environment']['systemd_log_path']: service_logs_path.as_posix(),
            STATIC_CONFIG['environment']['systemd_result_path']: result_path.as_posix(),
            STATIC_CONFIG['environment']['systemd_stdin_path']: socket_path.as_posix(),
            STATIC_CONFIG['environment']['systemd_delete_job']: (
                '1'
                if job.delete_after_completion
                else '0',
            ),
        })

        ### Allow for user-defined environment variables.
        mrsm_env_vars.update(job.env)

        environment_lines = [
            f"Environment={key}={shlex.quote(str(val))}"
            for key, val in mrsm_env_vars.items()
        ]
        environment_str = '\n'.join(environment_lines)
        service_name = self.get_service_name(name, debug=debug)

        service_text = (
            "[Unit]\n"
            f"Description=Run the job '{name}'\n"
            "\n"
            "[Service]\n"
            f"ExecStart={exec_str}\n"
            "KillSignal=SIGTERM\n"
            "Restart=always\n"
            "RestartPreventExitStatus=0\n"
            f"SyslogIdentifier={service_name}\n"
            f"{environment_str}\n"
            "\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        )
        return service_text

    def get_socket_file_text(self, name: str, debug: bool = False) -> str:
        """
        Return the contents of the socket file.
        """
        service_name = self.get_service_name(name, debug=debug)
        socket_path = self.get_socket_path(name, debug=debug)
        socket_text = (
            "[Unit]\n"
            f"BindsTo={service_name}\n"
            "\n"
            "[Socket]\n"
            f"ListenFIFO={socket_path.as_posix()}\n"
            "FileDescriptorName=stdin\n"
            "RemoveOnStop=true\n"
            "SocketMode=0660\n"
        )
        return socket_text

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
            _rotating_log=self.get_job_rotating_file(name, debug=debug),
            _stdin_file=self.get_job_stdin_file(name, debug=debug),
            _status_hook=partial(self.get_job_status, name),
            _result_hook=partial(self.get_job_result, name),
            _externally_managed=True,
        )
        return job


    def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return metadata about a job.
        """
        now = time.perf_counter()

        if '_jobs_metadata' not in self.__dict__:
            self._jobs_metadata: Dict[str, Any] = {}

        if name in self._jobs_metadata:
            ts = self._jobs_metadata[name].get('timestamp', None)

            if ts is not None and (now - ts) <= JOB_METADATA_CACHE_SECONDS:
                if debug:
                    dprint(f"Retuning cached metadata for job '{name}'.")
                return self._jobs_metadata[name]['metadata']

        metadata = {
            'sysargs': self.get_job_sysargs(name, debug=debug),
            'result': self.get_job_result(name, debug=debug),
            'restart': self.get_job_restart(name, debug=debug),
            'daemon': {
                'status': self.get_job_status(name, debug=debug),
                'pid': self.get_job_pid(name, debug=debug),
                'properties': self.get_job_properties(name, debug=debug),
            },
        }
        self._jobs_metadata[name] = {
            'timestamp': now,
            'metadata': metadata,
        }
        return metadata

    def get_job_restart(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job restarts.
        """
        from meerschaum.jobs._Job import RESTART_FLAGS
        sysargs = self.get_job_sysargs(name, debug=debug)
        if not sysargs:
            return False

        for flag in RESTART_FLAGS:
            if flag in sysargs:
                return True

        return False

    def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return the properties for a job.
        """
        job = self.get_hidden_job(name, debug=debug)
        return {
            k: v for k, v in job.daemon.properties.items()
            if k != 'externally_managed'
        }

    def get_job_process(self, name: str, debug: bool = False):
        """
        Return a `psutil.Process` for the job's PID.
        """
        pid = self.get_job_pid(name, debug=debug)
        if pid is None:
            return None

        psutil = mrsm.attempt_import('psutil')
        try:
            return psutil.Process(pid)
        except Exception:
            return None

    def get_job_status(self, name: str, debug: bool = False) -> str:
        """
        Return the job's service status.
        """
        output = self.run_command(
            ['is-active', self.get_service_name(name, debug=debug)],
            as_output=True,
            debug=debug,
        )

        if output == 'activating':
            return 'running'

        if output == 'active':
            process = self.get_job_process(name, debug=debug)
            if process is None:
                return 'stopped'

            try:
                if process.status() == 'stopped':
                    return 'paused'
            except Exception:
                return 'stopped'

            return 'running'

        return 'stopped'

    def get_job_pid(self, name: str, debug: bool = False) -> Union[int, None]:
        """
        Return the job's service PID.
        """
        from meerschaum.utils.misc import is_int

        output = self.run_command(
            [
                'show',
                self.get_service_name(name, debug=debug),
                '--property=MainPID',
            ],
            as_output=True,
            debug=debug,
        )
        if not output.startswith('MainPID='):
            return None

        pid_str = output[len('MainPID='):]
        if pid_str == '0':
            return None

        if is_int(pid_str):
            return int(pid_str)
        
        return None

    def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job began running.
        """
        output = self.run_command(
            [
                'show',
                self.get_service_name(name, debug=debug),
                '--property=ActiveEnterTimestamp'
            ],
            as_output=True,
            debug=debug,
        )
        if not output.startswith('ActiveEnterTimestamp'):
            return None

        dt_str = output.split('=')[-1]
        if not dt_str:
            return None

        dateutil_parser = mrsm.attempt_import('dateutil.parser')
        try:
            dt = dateutil_parser.parse(dt_str)
        except Exception as e:
            warn(f"Cannot parse '{output}' as a datetime:\n{e}")
            return None

        return dt.astimezone(timezone.utc).isoformat()

    def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job began running.
        """
        output = self.run_command(
            [
                'show',
                self.get_service_name(name, debug=debug),
                '--property=InactiveEnterTimestamp'
            ],
            as_output=True,
            debug=debug,
        )
        if not output.startswith('InactiveEnterTimestamp'):
            return None

        dt_str = output.split('=')[-1]
        if not dt_str:
            return None

        dateutil_parser = mrsm.attempt_import('dateutil.parser')

        try:
            dt = dateutil_parser.parse(dt_str)
        except Exception as e:
            warn(f"Cannot parse '{output}' as a datetime:\n{e}")
            return None
        return dt.astimezone(timezone.utc).isoformat()

    def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job was paused.
        """
        job = self.get_hidden_job(name, debug=debug)
        if self.get_job_status(name, debug=debug) != 'paused':
            return None

        stop_time = job.stop_time
        if stop_time is None:
            return None

        return stop_time.isoformat()

    def get_job_result(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Return the job's result SuccessTuple.
        """
        result_path = self.get_result_path(name, debug=debug)
        if not result_path.exists():
            return False, "No result available."

        try:
            with open(result_path, 'r', encoding='utf-8') as f:
                result = json.load(f)
        except Exception:
            return False, f"Could not read result for Job '{name}'."

        return tuple(result)

    def get_job_sysargs(self, name: str, debug: bool = False) -> Union[List[str], None]:
        """
        Return the sysargs from the service file.
        """
        service_file_path = self.get_service_file_path(name, debug=debug)
        if not service_file_path.exists():
            return []

        with open(service_file_path, 'r', encoding='utf-8') as f:
            service_lines = f.readlines()

        for line in service_lines:
            if line.startswith('ExecStart='):
                sysargs_str = line.split(' -m meerschaum ')[-1].split('<')[0]
                return shlex.split(sysargs_str)

        return []

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
        from meerschaum.utils.process import run_process

        if command_args[:2] != ['systemctl', '--user']:
            command_args = ['systemctl', '--user'] + command_args

        if debug:
            dprint(shlex.join(command_args))

        proc = run_process(
            command_args,
            foreground=False,
            as_proc=True,
            capture_output=True,
            text=True,
        )
        stdout, stderr = proc.communicate()
        if debug:
            dprint(f"{stdout}")

        if as_output:
            return stdout.strip()
            
        command_success = proc.wait() == 0
        command_msg = (
            "Success"
            if command_success
            else f"Failed to execute command `{shlex.join(command_args)}`."
        )
        return command_success, command_msg

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
        from meerschaum.utils.misc import make_symlink
        service_name = self.get_service_name(name, debug=debug)
        service_file_path = self.get_service_file_path(name, debug=debug)
        service_symlink_file_path = self.get_service_symlink_file_path(name, debug=debug)
        socket_stdin = self.get_job_stdin_file(name, debug=debug)
        _ = socket_stdin.file_handler

        ### Init the externally_managed file.
        ### NOTE: We must write the pickle file in addition to the properties file.
        job = self.get_hidden_job(name, sysargs=sysargs, properties=properties, debug=debug)
        job._set_externally_managed()
        pickle_success, pickle_msg = job.daemon.write_pickle()
        if not pickle_success:
            return pickle_success, pickle_msg
        properties_success, properties_msg = job.daemon.write_properties()
        if not properties_success:
            return properties_success, properties_msg

        service_file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(service_file_path, 'w+', encoding='utf-8') as f:
            f.write(self.get_service_file_text(name, sysargs, debug=debug))

        symlink_success, symlink_msg = make_symlink(service_file_path, service_symlink_file_path)
        if not symlink_success:
            return symlink_success, symlink_msg

        commands = [
            ['daemon-reload'],
            ['enable', service_name],
            ['start', service_name],
        ]

        fails = 0
        for command_list in commands:
            command_success, command_msg = self.run_command(command_list, debug=debug)
            if not command_success:
                fails += 1

        if fails > 1:
            return False, "Failed to reload systemd."

        return True, f"Started job '{name}' via systemd."

    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job's service.
        """
        job = self.get_hidden_job(name, debug=debug)
        job.daemon._remove_stop_file()

        status = self.get_job_status(name, debug=debug)
        if status == 'paused':
            return self.run_command(
                ['kill', '-s', 'SIGCONT', self.get_service_name(name, debug=debug)],
                debug=debug,
            )

        return self.run_command(
            ['start', self.get_service_name(name, debug=debug)],
            debug=debug,
        )

    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job's service.
        """
        job = self.get_hidden_job(name, debug=debug)
        job.daemon._write_stop_file('quit')
        sigint_success, sigint_msg = self.run_command(
            ['kill', '-s', 'SIGINT', self.get_service_name(name, debug=debug)],
            debug=debug,
        )

        check_timeout_interval = get_config('jobs', 'check_timeout_interval_seconds')
        loop_start = time.perf_counter()
        timeout_seconds = get_config('jobs', 'timeout_seconds')
        while (time.perf_counter() - loop_start) < timeout_seconds:
            if self.get_job_status(name, debug=debug) == 'stopped':
                return True, 'Success'

            time.sleep(check_timeout_interval)

        return self.run_command(
            ['stop', self.get_service_name(name, debug=debug)],
            debug=debug,
        )

    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Pause a job's service.
        """
        job = self.get_hidden_job(name, debug=debug)
        job.daemon._write_stop_file('pause')
        return self.run_command(
            ['kill', '-s', 'SIGSTOP', self.get_service_name(name, debug=debug)],
            debug=debug,
        )

    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Delete a job's service.
        """
        from meerschaum.config.paths import SYSTEMD_LOGS_RESOURCES_PATH
        job = self.get_hidden_job(name, debug=debug)

        if not job.delete_after_completion:
            _ = self.stop_job(name, debug=debug)
            _ = self.run_command(
                ['disable', self.get_service_name(name, debug=debug)],
                debug=debug,
            )

        service_job_path = self.get_service_job_path(name, debug=debug)
        try:
            if service_job_path.exists():
                shutil.rmtree(service_job_path)
        except Exception as e:
            warn(e)
            return False, str(e)

        service_logs_path = self.get_service_logs_path(name, debug=debug)
        logs_paths = [
            (SYSTEMD_LOGS_RESOURCES_PATH / name)
            for name in os.listdir(SYSTEMD_LOGS_RESOURCES_PATH)
            if name.startswith(service_logs_path.name + '.')
        ]
        paths = [
            self.get_service_file_path(name, debug=debug),
            self.get_service_symlink_file_path(name, debug=debug),
            self.get_socket_path(name, debug=debug),
            self.get_result_path(name, debug=debug),
        ] + logs_paths

        for path in paths:
            if path.exists():
                try:
                    path.unlink()
                except Exception as e:
                    warn(e)
                    return False, str(e)

        _ = job.delete()

        return self.run_command(['daemon-reload'], debug=debug)

    def get_logs(self, name: str, debug: bool = False) -> str:
        """
        Return a job's journal logs.
        """
        rotating_file = self.get_job_rotating_file(name, debug=debug)
        return rotating_file.read()

    def get_job_stop_time(self, name: str, debug: bool = False) -> Union[datetime, None]:
        """
        Return a job's stop time.
        """
        job = self.get_hidden_job(name, debug=debug)
        return job.stop_time

    def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job is blocking on stdin.
        """
        socket_path = self.get_socket_path(name, debug=debug)
        blocking_path = socket_path.parent / (socket_path.name + '.block')
        return blocking_path.exists()

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
        from meerschaum.config.paths import SYSTEMD_LOGS_RESOURCES_PATH
        job = self.get_hidden_job(name, debug=debug)
        kwargs.update({
            '_logs_path': SYSTEMD_LOGS_RESOURCES_PATH,
            '_log': self.get_job_rotating_file(name, debug=debug),
            '_stdin_file': self.get_job_stdin_file(name, debug=debug),
            'debug': debug,
        })
        await job.monitor_logs_async(*args, **kwargs)

    def monitor_logs(self, *args, **kwargs):
        """
        Monitor a job's output.
        """
        asyncio.run(self.monitor_logs_async(*args, **kwargs))
