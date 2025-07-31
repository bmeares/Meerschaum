#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define utilities for managing the workers jobs.
"""

import os
import pathlib
import time
import json
import asyncio
from typing import List

import meerschaum as mrsm
from meerschaum.utils.daemon import StdinFile
from meerschaum.utils.warnings import warn
from meerschaum.jobs import Job
from meerschaum._internal.static import STATIC_CONFIG

STOP_TOKEN: str = STATIC_CONFIG['jobs']['stop_token']


def get_worker_input_file_path(ix: int) -> pathlib.Path:
    """
    Return the file path to the worker's input `StdinFile`.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"worker-{ix}.input"


def get_worker_input_file(ix: int) -> StdinFile:
    """
    Return the `StdinFile` which will receive input commands to the worker job.
    """
    return StdinFile(get_worker_input_file_path(ix))


def get_worker_output_file_path(ix: int) -> pathlib.Path:
    """
    Return the file path to the worker's output `StdinFile`.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"worker-{ix}.output"


def get_worker_output_file(ix: int) -> StdinFile:
    """
    Return the `StdinFile` which will output status payloads.
    """
    return StdinFile(get_worker_output_file_path(ix))


def get_worker_stop_path(ix: int) -> pathlib.Path:
    """
    Return the file path to the worker's stop file.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"worker-{ix}.stop"


class ActionWorker:
    """
    The process loop which will accept commands to be run.
    """

    def __init__(self, ix: int):
        self.ix = ix

    @property
    def input_file(self) -> StdinFile:
        if '_input_file' in self.__dict__:
            return self._input_file

        self._input_file = get_worker_input_file(self.ix)
        return self._input_file

    @property
    def output_file(self) -> StdinFile:
        if '_output_file' in self.__dict__:
            return self._output_file

        self._output_file = get_worker_output_file(self.ix)
        return self._output_file

    @property
    def job(self) -> Job:
        """
        Return the job associated with this worker.
        """
        return Job(
            f'.cli.{self.ix}.worker',
            sysargs=['start', 'worker', str(self.ix)],
            executor_keys='local',
            delete_after_completion=True,
            _properties={
                'logs': {
                    'write_timestamps': False,
                    'refresh_files_seconds': 31557600,
                    'max_file_size': 10_000_000,
                    'num_files_to_keep': 1,
                    'redirect_streams': True,
                },
            },
        )

    @property
    def stop_event(self) -> asyncio.Event:
        """
        Return the stop event to set to exit the logs monitoring.
        """
        if '_stop_event' in self.__dict__:
            return self._stop_event

        self._stop_event = asyncio.Event()
        return self._stop_event

    @property
    def stop_path(self) -> pathlib.Path:
        """
        Return the path to the worker's stop file.
        """
        if '_stop_path' in self.__dict__:
            return self._stop_path

        self._stop_path = get_worker_stop_path(self.ix)
        return self._stop_path

    def check_stop_status(self) -> None:
        """
        Check for the stop file, and if it exists, set the stop event.
        """
        if self.stop_path.exists():
            self.stop_event.set()

    def set_stop_status(self) -> None:
        """
        Create the stop file and set the stop event.
        """
        self.stop_path.touch()
        self.stop_event.set()

    def send_signal(self, signalnum: int):
        """
        Send a signal to the running job.
        """
        pid = self.job.pid
        if pid is None:
            raise EnvironmentError("Job is not running.")

        os.kill(pid, signalnum)

    def run(self) -> mrsm.SuccessTuple:
        """
        Run the worker's process loop.
        """
        from meerschaum._internal.entry import entry
        from meerschaum.utils.misc import set_env

        while True:
            try:
                if self.stop_path.exists():
                    self.stop_path.unlink()
            except Exception as e:
                warn(f"Could not remove stop path:\n{e}")
                return False, f"Failed to clean up stop path:\n{e}"

            try:
                input_data_str = self.input_file.readline()
            except Exception as e:
                warn(f"Could not parse input data in worker:\n{e}")
                continue

            if not input_data_str:
                time.sleep(0.1)
                continue

            try:
                input_data = json.loads(input_data_str)
            except Exception as e:
                input_data = {'error': str(e)}

            if 'error' in input_data:
                return False, input_data['error']

            sysargs = input_data['sysargs']
            session_id = input_data['session_id']
            action_id = input_data['action_id']
            patch_args = input_data.get('patch_args', None)
            env = input_data.get('env', {})

            with set_env(env):
                action_success, action_msg = entry(
                    sysargs,
                    _use_cli_daemon=False,
                    _patch_args=patch_args,
                )

            print(STOP_TOKEN, flush=True, end='')

            output_data = {
                'state': 'completed',
                'session_id': session_id,
                'action_id': action_id,
                'success': action_success,
                'message': action_msg,
            }

            try:
                self.output_file.write(json.dumps(output_data, separators=(',', ':')) + '\n')
            except Exception as e:
                warn(f"Failed to write output payload from worker:\n{e}")

            self.set_stop_status()

            try:
                self.job.daemon.rotating_log.increment_subfiles()
                if self.job.daemon.log_offset_path.exists():
                    self.job.daemon.log_offset_path.unlink()
            except Exception as e:
                warn(f"Failed to refresh log files:\n{e}")

        return True, "Success"

    def monitor_callback(self, data: str):
        print(data, flush=True, end='')
        self.check_stop_status()

    def cleanup(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Delete the worker's job and any existing files.
        """
        delete_success, delete_msg = self.job.delete(debug=debug)
        if not delete_success:
            return delete_success, delete_msg

        paths = [
            get_worker_stop_path(self.ix),
            get_worker_input_file_path(self.ix),
            get_worker_output_file_path(self.ix),
        ]

        try:
            for path in paths:
                if path.exists():
                    path.unlink()
        except Exception as e:
            return False, f"Failed to clean up worker files:\n{e}"

        return True, "Success"


def get_existing_cli_worker_indices() -> List[int]:
    """
    Get a list of the existing CLI workers' indices.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    from meerschaum.utils.misc import is_int

    return sorted(list({
        int(worker_ix)
        for filename in os.listdir(CLI_RESOURCES_PATH)
        if (
            filename.startswith('worker-')
            and is_int(
                (worker_ix := filename.split('.', maxsplit=1)[0].replace('worker-', ''))
            )
        )
    }))


def get_existing_cli_workers() -> List[ActionWorker]:
    """
    Get a list of the existing CLI workers.
    """
    return [ActionWorker(ix) for ix in get_existing_cli_worker_indices()]
