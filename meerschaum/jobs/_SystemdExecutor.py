#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Manage `meerschaum.jobs.Job` via `systemd`.
"""

import os
import pathlib
import shlex
import sys

from meerschaum.jobs import Job, Executor, make_executor
from meerschaum.utils.typing import Dict, Any, List, SuccessTuple
from meerschaum.config.static import STATIC_CONFIG


@make_executor
class SystemdExecutor(Executor):
    """
    Execute Meerschaum jobs via `systemd`.
    """

    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """
        from meerschaum.config.paths import SYSTEMD_USER_RESOURCES_PATH
        user_services = [
            name
            for name in os.listdir(SYSTEMD_USER_RESOURCES_PATH)
            if name.startswith('mrsm-')
        ]
        return name in user_services
    
    def get_jobs(self) -> Dict[str, Job]:
        """
        Return a dictionary of `systemd` Jobs.
        """
        from meerschaum.config.paths import SYSTEMD_USER_RESOURCES_PATH
        user_services = [
            name
            for name in os.listdir(SYSTEMD_USER_RESOURCES_PATH)
            if name.startswith('mrsm-')
        ]
        return {
            name: Job(name, executor_keys=str(self))
            for name in user_services
        }

    def get_service_name(self, name: str, debug: bool = False) -> str:
        """
        Return a job's service name.
        """
        return f"mrsm-{name.replace(' ', '-')}.service"

    def get_service_file_path(self, name: str, debug: bool = False) -> pathlib.Path:
        """
        Return the path to a Job's service file.
        """
        from meerschaum.config.paths import SYSTEMD_USER_RESOURCES_PATH
        return SYSTEMD_USER_RESOURCES_PATH / self.get_service_name(name, debug=debug)

    def get_service_file_text(self, name: str, sysargs: List[str], debug: bool = False) -> str:
        """
        Return the contents of the unit file.
        """
        sysargs_str = shlex.join(sysargs)
        mrsm_env_var_names = set([var for var in STATIC_CONFIG['environment'].values()])
        mrsm_env_vars = {
            key: val
            for key, val in os.environ.items()
            if key in mrsm_env_var_names
        }

        ### Add new environment variables for the service process.
        mrsm_env_vars.update({
            'MRSM_DAEMON_ID': name,
            'PYTHONUNBUFFERED': '1',

        })
        environment_lines = [
            f"Environment={key}={val}"
            for key, val in mrsm_env_vars.items()
        ]
        environment_str = '\n'.join(environment_lines)

        service_text = (
            "[Unit]\n"
            f"Description=Run the job '{name}'\n"
            "\n"
            "[Service]\n"
            f"ExecStart={sys.executable} -m meerschaum {sysargs_str}\n"
            "Restart=always\n"
            f"{environment_str}\n"
            "\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        )
        return service_text

    def run_command(self, command_args: List[str]) -> SuccessTuple:
        """
        Run a `systemd` command and return success.
        """
        from meerschaum.utils.process import run_process
        if len(command_args) < 2:
            return False, "Not enough arguments."

        if command_args[:2] != ['systemctl', '--user']:
            command_args = ['systemctl', '--user'] + command_args

        command_success = run_process(
            command_args,
            foreground=True,
            capture_output=False,
        ) == 0
        command_msg = (
            "Success"
            if command_success
            else f"Failed to execute command `{shlex.join(command_args)}`."
        )
        return command_success, command_msg

    def create_job(self, name: str, sysargs: List[str], debug: bool = False) -> SuccessTuple:
        """
        Create a job as a service to be run by `systemd`.
        """
        service_name = self.get_service_name(name, debug=debug)
        service_file_path = self.get_service_file_path(name, debug=debug)

        with open(service_file_path, 'w+', encoding='utf-8') as f:
            f.write(self.get_service_file_text(name, sysargs))

        commands = [
            ['daemon-reload'],
            ['enable', service_name],
            ['start', service_name],
        ]

        fails = 0
        for command_list in commands:
            command_success, command_msg = self.run_command(command_list)
            if not command_success:
                fails += 1

        if fails > 1:
            return False, "Failed to reload systemd."

        return True, f"Started {self} via systemd."

    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job's service.
        """
        return self.run_command(['start', self.get_service_name(name, debug=debug)])

    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job's service.
        """
        return self.run_command(['stop', self.get_service_name(name, debug=debug)])

    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Pause a job's service.
        """
        return False, "Not implemented."

    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Delete a job's service.
        """
        stop_success, stop_msg = self.stop_job(name, debug=debug)
        if not stop_success:
            return stop_success, stop_msg

        disable_success, disable_msg = self.run_command(
            ['disable', self.get_service_name(name, debug=debug)],
        )
        if not disable_success:
            return disable_success, disable_msg

        service_file_path = self.get_service_file_path(name, debug=debug)
        if service_file_path.exists():
            try:
                service_file_path.unlink()
            except Exception as e:
                return False, str(e)

        return self.run_command(['daemon-reload'])

    def get_logs(self, name: str, debug: bool = False) -> str:
        """
        Return a job's journal logs.
        """
        ### TODO read with cysystemd
        return ""
