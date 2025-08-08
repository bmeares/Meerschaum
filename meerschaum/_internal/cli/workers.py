#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define utilities for managing the workers jobs.
"""

import os
import pathlib
import json
import asyncio
import time
from typing import List, Dict, Any, Union, TextIO

import meerschaum as mrsm
from meerschaum.utils.warnings import warn
from meerschaum.jobs import Job
from meerschaum.utils.threading import Thread
from meerschaum._internal.static import STATIC_CONFIG

STOP_TOKEN: str = STATIC_CONFIG['jobs']['stop_token']


def get_worker_input_file_path(ix: int) -> pathlib.Path:
    """
    Return the file path to the worker's input named pipe file.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"worker-{ix}.input"


def get_worker_output_file_path(ix: int) -> pathlib.Path:
    """
    Return the file path to the worker's output `StdinFile`.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"worker-{ix}.output"


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

    def __init__(self, ix: int, refresh_seconds: Union[int, float, None] = None):
        self.ix = ix
        self.refresh_seconds = (
            refresh_seconds
            if refresh_seconds is not None
            else mrsm.get_config('system', 'cli', 'refresh_seconds')
        )
        self.refresh_logs_stop_event = asyncio.Event()

    @property
    def input_file_path(self) -> pathlib.Path:
        """
        Return the path to the input file.
        """
        return get_worker_input_file_path(self.ix)

    @property
    def output_file_path(self) -> pathlib.Path:
        """
        Return the path to the output file.
        """
        return get_worker_output_file_path(self.ix)

    @property
    def lock_path(self) -> pathlib.Path:
        """
        Return the lock path for this CLI worker.
        """
        if '_lock_path' in self.__dict__:
            return self._lock_path

        from meerschaum._internal.cli.daemons import get_cli_lock_path
        self._lock_path = get_cli_lock_path(self.ix)
        return self._lock_path

    @property
    def job(self) -> Job:
        """
        Return the job associated with this worker.
        """
        from meerschaum.config.paths import CLI_LOGS_RESOURCES_PATH
        log_path = CLI_LOGS_RESOURCES_PATH / f'cli.{self.ix}.worker.log'

        return Job(
            f'.cli.{self.ix}.worker',
            sysargs=['start', 'worker', str(self.ix)],
            executor_keys='local',
            delete_after_completion=False,
            _properties={
                'logs': {
                    'path': log_path.as_posix(),
                    'write_timestamps': False,
                    'refresh_files_seconds': 31557600,
                    'max_file_size': 10_000_000,
                    'num_files_to_keep': 1,
                    'redirect_streams': True,
                    'lines_to_show': 0,
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

    def clear_stop_status(self) -> None:
        """
        Remove the stop file and clear the stop event.
        """
        try:
            self.stop_event.clear()
            if self.stop_path.exists():
                self.stop_path.unlink()
        except Exception as e:
            warn(f"Failed to clear stop status:\n{e}")

    def set_lock(self) -> None:
        """
        Create the lock file.
        """
        self.lock_path.touch()

    def release_lock(self) -> None:
        """
        Delete the lock file.
        """
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        except Exception as e:
            warn(f"Failed to release lock for {self}:\n{e}")

    def send_signal(self, signalnum):
        """
        Send a signal to the running job.
        """
        pid = self.job.pid
        if pid is None:
            raise EnvironmentError("Job is not running.")

        os.kill(pid, signalnum)

    def _read_data(
        self,
        file_to_read: TextIO,
    ) -> Dict[str, Any]:
        """
        Common logic for reading data from a pipe.
        """
        try:
            data_str = file_to_read.readline()
        except Exception as e:
            warn(f"Could not read data:\n{e}")
            return {}

        if not data_str:
            return {}

        try:
            data = json.loads(data_str)
        except Exception as e:
            return {'error': str(e)}

        return data

    @staticmethod
    def _write_data(
        file_to_write: TextIO,
        data: Dict[str, Any],
    ) -> None:
        """
        Write a data dictionary to a pipe file.
        """
        try:
            file_to_write.write(json.dumps(data, separators=(',', ':')) + '\n')
        except Exception as e:
            warn(f"Failed to write data:\n{e}")

    def read_input_data(self) -> Dict[str, Any]:
        """
        Read input data from the input pipe file.
        This method is called from within the worker's daemon context.
        """
        with open(self.input_file_path, 'r') as f:
            return self._read_data(f)

    def write_output_data(self, output_data: Dict[str, Any]) -> None:
        """
        Write the output data dictionary to the output pipe file.
        This method is called from within the worker's daemon context.
        """
        with open(self.output_file_path, 'w') as f:
            self._write_data(f, output_data)

    def write_input_data(self, input_data: Dict[str, Any]) -> None:
        """
        Write the input data dictionary to the input pipe file.
        This method is called from the client entry context.
        """
        with open(self.input_file_path, 'w') as f:
            self._write_data(f, input_data)

    def read_output_data(self) -> Dict[str, Any]:
        """
        Read output data from the output pipe file.
        This method is called from the client entry context.
        """
        with open(self.output_file_path, 'r') as f:
            return self._read_data(f)

    def increment_log(self) -> None:
        """
        Increment the rotating log to clear output for the next action.
        """
        try:
            self.job.daemon.rotating_log.increment_subfiles()
            if self.job.daemon.log_offset_path.exists():
                self.job.daemon.log_offset_path.unlink()
        except Exception as e:
            warn(f"Failed to refresh log files:\n{e}")

    def is_ready(self) -> bool:
        """
        Return whether the CLI worker is ready to accept input.
        """
        return (
            not self.lock_path.exists()
            and self.input_file_path.exists()
            and not self.stop_path.exists()
        )

    def create_fifos(self) -> mrsm.SuccessTuple:
        """
        Create the named pipes (FIFO files) for input and output.
        """
        paths = [self.input_file_path, self.output_file_path]
        for path in paths:
            try:
                os.mkfifo(path)
            except FileExistsError:
                pass
            except Exception as e:
                return False, f"Failed to create FIFO file for {self}:\n{e}"

        return True, "Success"

    def run(self) -> mrsm.SuccessTuple:
        """
        Run the worker's process loop.
        """
        from meerschaum._internal.entry import entry
        from meerschaum.config import replace_config
        from meerschaum.config.environment import replace_env

        self.create_fifos()

        while True:
            self.release_lock()
            self.clear_stop_status()

            input_data = self.read_input_data()
            self.set_lock()

            if 'error' in input_data:
                warn(input_data['error'])
                continue

            if input_data.get('increment', False):
                self.increment_log()
                continue

            sysargs = input_data.get('sysargs', None)
            session_id = input_data.get('session_id', None)
            action_id = input_data.get('action_id', None)
            patch_args = input_data.get('patch_args', None)
            env = input_data.get('env', {})
            config = input_data.get('config', {})
            self.write_output_data({
                'state': 'accepted',
                'session_id': session_id,
                'action_id': action_id,
            })

            with replace_config(config):
                with replace_env(env):
                    action_success, action_msg = entry(
                        sysargs,
                        _use_cli_daemon=False,
                        _patch_args=patch_args,
                    )
                    print(STOP_TOKEN, flush=True, end='\n')

            self.write_output_data({
                'state': 'completed',
                'session_id': session_id,
                'action_id': action_id,
                'success': action_success,
                'message': action_msg,
            })
            self.set_stop_status()

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

    def touch_cli_logs_loop(self):
        """
        Touch the CLI daemon's logs to refresh the logs monitoring.
        """
        while not self.refresh_logs_stop_event.is_set():
            self.job.daemon.rotating_log.touch()
            time.sleep(self.refresh_seconds)

    def start_cli_logs_refresh_thread(self):
        """
        Spin up a daemon thread to refresh the CLI's logs.
        """
        self._logs_refresh_thread = Thread(
            target=self.touch_cli_logs_loop,
            daemon=True,
        )
        self._logs_refresh_thread.start()

    def stop_cli_logs_refresh_thread(self):
        """
        Stop the logs refresh thread.
        """
        self.refresh_logs_stop_event.set()
        thread = self.__dict__.pop('_logs_refresh_thread', None)
        if thread is None:
            return

        thread.join()

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f'ActionWorker({self.ix})'


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
