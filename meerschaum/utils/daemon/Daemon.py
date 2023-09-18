#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage running daemons via the Daemon class.
"""

from __future__ import annotations
import os, pathlib, json, shutil, datetime, signal, sys
from meerschaum.utils.typing import Optional, Dict, Any, SuccessTuple, Callable, List, Union
from meerschaum.config import get_config
from meerschaum.config._paths import DAEMON_RESOURCES_PATH, LOGS_RESOURCES_PATH
from meerschaum.config._patch import apply_patch_to_config
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.packages import attempt_import, venv_exec
from meerschaum.utils.daemon._names import get_new_daemon_name
from meerschaum.utils.daemon.RotatingFile import RotatingFile
from meerschaum.utils.daemon.Log import Log
from meerschaum.utils.threading import RepeatTimer
from meerschaum.__main__ import _close_pools

class Daemon:
    """Manage running daemons via the Daemon class."""

    def __new__(
        cls,
        *args,
        daemon_id : Optional[str] = None,
        **kw
    ):
        """
        If a daemon_id is provided and already exists, read from its pickle file.
        """
        instance = super(Daemon, cls).__new__(cls)
        if daemon_id is not None:
            instance.daemon_id = daemon_id
            if instance.pickle_path.exists():
                instance = instance.read_pickle()
        return instance

    def __init__(
        self,
        target: Optional[Callable[[Any], Any]] = None,
        target_args: Optional[List[str]] = None,
        target_kw: Optional[Dict[str, Any]] = None,
        daemon_id: Optional[str] = None,
        label: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        """
        :param target:
            The function to execute in a child process.

        :param target_args:
            Positional arguments to pass to the target function.
            Defaults to `None`.

        :param target_kw:
            Keyword arguments to pass to the target function.
            Defaults to `None`.

        :param daemon_id:
            Build a `Daemon` from an existing `daemon_id`.
            If `daemon_id` is provided, other arguments are ignored and are derived
            from the existing pickled `Daemon`.

        :param label:
            Label string to help identifiy a daemon.
            If `None`, use the function name instead.
            Defaults to `None`.

        :param properties:
            Override reading from the properties JSON by providing an existing dictionary.
            Defaults to `None`.
        """
        _pickle = self.__dict__.get('_pickle', False)
        if daemon_id is not None:
            self.daemon_id = daemon_id
            if not self.pickle_path.exists() and not target and ('target' not in self.__dict__):
                raise Exception(
                    f"Daemon '{self.daemon_id}' does not exist. "
                    + "Pass a target to create a new Daemon."
                )
        if 'target' not in self.__dict__:
            if target is None:
                error(f"Cannot create a Daemon without a target.")
            self.target = target
        if 'target_args' not in self.__dict__:
            self.target_args = target_args if target_args is not None else []
        if 'target_kw' not in self.__dict__:
            self.target_kw = target_kw if target_kw is not None else {}
        if 'label' not in self.__dict__:
            if label is None:
                label = (
                    self.target.__name__ if '__name__' in self.target.__dir__()
                        else str(self.target)
                )
            self.label = label
        if 'daemon_id' not in self.__dict__:
            self.daemon_id = get_new_daemon_name()
        if '_properties' not in self.__dict__:
            self._properties = properties
        if self._properties is None:
            self._properties = {}
        self._properties.update({'label' : self.label})
        ### Instantiate the process and if it doesn't exist, make sure the PID is removed.
        _ = self.process


    def _run_exit(
            self,
            keep_daemon_output: bool = True,
            allow_dirty_run: bool = False,
        ) -> Any:
        """Run the daemon's target function.
        NOTE: This WILL EXIT the parent process!

        Parameters
        ----------
        keep_daemon_output: bool, default True
            If `False`, delete the daemon's output directory upon exiting.
            
        allow_dirty_run :
            If `True`, run the daemon, even if the `daemon_id` directory exists.
            This option is dangerous because if the same `daemon_id` runs twice,
            the last to finish will overwrite the output of the first.

        Returns
        -------
        Nothing — this will exit the parent process.
        """
        import platform, sys, os
        daemon = attempt_import('daemon')

        if platform.system() == 'Windows':
            return False, "Windows is no longer supported."

        self._setup(allow_dirty_run)

        self._daemon_context = daemon.DaemonContext(
            pidfile = self.pid_lock,
            stdout = self.rotating_log,
            stderr = self.rotating_log,
            working_directory = os.getcwd(),
            detach_process = True,
            files_preserve = list(self.rotating_log.subfile_objects.values()),
            signal_map = {
                signal.SIGINT: self._handle_interrupt,
                signal.SIGTERM: self._handle_sigterm,
            },
        )

        log_refresh_seconds = get_config('jobs', 'logs', 'refresh_files_seconds')
        self._log_refresh_timer = RepeatTimer(log_refresh_seconds, self.rotating_log.refresh_files)

        with self._daemon_context:
            try:
                with open(self.pid_path, 'w+') as f:
                    f.write(str(os.getpid()))

                self._log_refresh_timer.start()
                result = self.target(*self.target_args, **self.target_kw)
            except Exception as e:
                warn(e, stacklevel=3)
                result = e
            finally:
                self._log_refresh_timer.cancel()
                self.rotating_log.close()
                if self.pid_path.exists():
                    self.pid_path.unlink()

            if keep_daemon_output:
                self.properties['process']['ended'] = datetime.datetime.utcnow().isoformat()
                self.properties['result'] = result
                self.write_properties()
            else:
                self.cleanup()
            return result


    def run(
            self,
            keep_daemon_output: bool = True,
            allow_dirty_run: bool = False,
            debug: bool = False,
        ) -> SuccessTuple:
        """Run the daemon as a child process and continue executing the parent.

        Parameters
        ----------
        keep_daemon_output: bool, default True
            If `False`, delete the daemon's output directory upon exiting.

        allow_dirty_run: bool, default False
            If `True`, run the daemon, even if the `daemon_id` directory exists.
            This option is dangerous because if the same `daemon_id` runs twice,
            the last to finish will overwrite the output of the first.

        Returns
        -------
        A SuccessTuple indicating success.

        """
        import platform
        self.mkdir_if_not_exists(allow_dirty_run)
        _write_pickle_success_tuple = self.write_pickle()
        if not _write_pickle_success_tuple[0]:
            return _write_pickle_success_tuple

        if platform.system() == 'Windows':
            return False, "Cannot run background jobs on Windows."

        _launch_daemon_code = (
            "from meerschaum.utils.daemon import Daemon; "
            + f"daemon = Daemon(daemon_id='{self.daemon_id}'); "
            + f"daemon._run_exit(keep_daemon_output={keep_daemon_output}, "
            + f"allow_dirty_run=True)"
        )
        _launch_success_bool = venv_exec(_launch_daemon_code, debug=debug, venv=None)
        msg = "Success" if _launch_success_bool else f"Failed to start daemon '{self.daemon_id}'."
        return _launch_success_bool, msg


    def kill(self, timeout: Optional[int] = 3) -> SuccessTuple:
        """Forcibly terminate a running daemon.
        Sends a SIGTERM signal to the process.

        Parameters
        ----------
        timeout: Optional[int], default 3
            How many seconds to wait for the process to terminate.

        Returns
        -------
        A SuccessTuple indicating success.
        """
        success, msg = self._send_signal(signal.SIGTERM, timeout=timeout)
        if success:
            return success, msg
        process = self.process
        if process is None or not process.is_running():
            return True, "Process has already stopped."
        try:
            process.terminate()
            process.kill()
            process.wait(timeout=timeout)
        except Exception as e:
            return False, f"Failed to kill job {self} with exception: {e}"
        return True, "Success"


    def quit(self, timeout: Optional[int] = 3) -> SuccessTuple:
        """Gracefully quit a running daemon."""
        return self._send_signal(signal.SIGINT, timeout=timeout)


    def _handle_interrupt(self, signal_number: int, stack_frame: 'frame') -> None:
        """
        Handle `SIGINT` within the Daemon context.
        This method is injected into the `DaemonContext`.
        """
        timer = self.__dict__.get('_log_refresh_timer', None)
        if timer is not None:
            timer.cancel()

        daemon_context = self.__dict__.get('_daemon_context', None)
        if daemon_context is not None:
            daemon_context.close()

        _close_pools()

        ### NOTE: SystemExit() does not work here.
        sys.exit(0)


    def _handle_sigterm(self, signal_number: int, stack_frame: 'frame') -> None:
        """
        Handle `SIGTERM` within the `Daemon` context.
        This method is injected into the `DaemonContext`.
        """
        timer = self.__dict__.get('_log_refresh_timer', None)
        if timer is not None:
            timer.cancel()

        daemon_context = self.__dict__.get('_daemon_context', None)
        if daemon_context is not None:
            daemon_context.close()

        _close_pools()

        raise SystemExit()

 
    def _send_signal(
            self,
            signal_to_send,
            timeout: Optional[Union[float, int]] = 3,
            check_timeout_interval: float = 0.1,
        ) -> SuccessTuple:
        """Send a signal to the daemon process.

        Parameters
        ----------
        signal_to_send:
            The signal the send to the daemon, e.g. `signals.SIGINT`.

        timeout:
            The maximum number of seconds to wait for a process to terminate.
            Defaults to 3.

        check_timeout_interval: float, default 0.1
            The number of seconds to wait between checking if the process is still running.
            Defaults to 0.1.

        Returns
        -------
        A SuccessTuple indicating success.
        """
        import time

        try:
            os.kill(int(self.pid), signal_to_send)
        except Exception as e:
            return False, str(e)
        if timeout is None:
            return True, f"Successfully sent '{signal}' to daemon '{self.daemon_id}'."
        begin = time.perf_counter()
        while (time.perf_counter() - begin) < timeout:
            if not self.pid_path.exists():
                return True, f"Successfully stopped daemon '{self.daemon_id}'."
            time.sleep(check_timeout_interval)

        return False, (
            f"Failed to stop daemon '{self.daemon_id}' within {timeout} second"
            + ('s' if timeout != 1 else '') + '.'
        )


    def mkdir_if_not_exists(self, allow_dirty_run: bool = False):
        """Create the Daemon's directory.
        If `allow_dirty_run` is `False` and the directory already exists,
        raise a `FileExistsError`.
        """
        try:
            self.path.mkdir(parents=True, exist_ok=False)
            _already_exists = False
        except FileExistsError:
            _already_exists = True

        if _already_exists and not allow_dirty_run:
            error(
                f"Daemon '{self.daemon_id}' already exists. " +
                f"To allow this daemon to run, do one of the following:\n"
                + "  - Execute `daemon.cleanup()`.\n"
                + f"  - Delete the directory '{self.path}'.\n"
                + "  - Pass `allow_dirty_run=True` to `daemon.run()`.\n",
                FileExistsError,
            )

    @property
    def process(self) -> Union['psutil.Process', None]:
        """
        Return the psutil process for the Daemon.
        """
        psutil = attempt_import('psutil')
        pid = self.pid
        if pid is None:
            return None
        if '_process' not in self.__dict__ or self.__dict__['_process'].pid != int(pid):
            try:
                self._process = psutil.Process(int(pid))
            except Exception as e:
                if self.pid_path.exists():
                    self.pid_path.unlink()
                return None
        return self._process


    @property
    def path(self) -> pathlib.Path:
        """
        Return the path for this Daemon's directory.
        """
        return DAEMON_RESOURCES_PATH / self.daemon_id

    @property
    def properties_path(self) -> pathlib.Path:
        """
        Return the `propterties.json` path for this Daemon.
        """
        return self.path / 'properties.json'


    @property
    def log_path(self) -> pathlib.Path:
        """
        Return the log path.
        """
        return LOGS_RESOURCES_PATH / (self.daemon_id + '.log')


    @property
    def log_offset_path(self) -> pathlib.Path:
        """
        Return the log offset file path.
        """
        return LOGS_RESOURCES_PATH / ('.' + self.daemon_id + '.log.offset')

    
    @property
    def rotating_log(self) -> RotatingFile:
        if '_rotating_log' in self.__dict__:
            return self._rotating_log

        self._rotating_log = RotatingFile(self.log_path, redirect_streams=True)
        return self._rotating_log


    @property
    def log_text(self) -> Optional[str]:
        """Read the log files and return their contents.
        Returns `None` if the log file does not exist.
        """
        new_rotating_log = RotatingFile(
            self.rotating_log.file_path,
            num_files_to_keep = self.rotating_log.num_files_to_keep,
            max_file_size = self.max_file_size,
        )
        return new_rotating_log.read()


    def readlines(self) -> List[str]:
        """
        Read the next log lines, persisting the cursor for later use.
        Note this will alter the cursor of `self.rotating_log`.
        """
        self.rotating_log._cursor = self._read_log_offset()
        lines = self.rotating_log.readlines()
        self._write_log_offset()
        return lines


    def _read_log_offset(self) -> Tuple[int, int]:
        """
        Return the current log offset cursor.

        Returns
        -------
        A tuple of the form (`subfile_index`, `position`).
        """
        if not self.log_offset_path.exists():
            return 0, 0

        with open(self.log_offset_path, 'r', encoding='utf-8') as f:
            cursor_text = f.read()
        cursor_parts = cursor_text.split(' ')
        subfile_index, subfile_position = int(cursor_parts[0]), int(cursor_parts[1])
        return subfile_index, subfile_position


    def _write_log_offset(self) -> None:
        """
        Write the current log offset file.
        """
        with open(self.log_offset_path, 'w+', encoding='utf-8') as f:
            subfile_index = self.rotating_log._cursor[0]
            subfile_position = self.rotating_log._cursor[1]
            f.write(f"{subfile_index} {subfile_position}")


    @property
    def log(self) -> List['meerschaum.utils.daemon.Log']:
        """
        Return a `meerschaum.utils.daemon.Log` object for this daemon.
        """
        if '_log' in self.__dict__:
            return self._log
        self._log = Log(self.rotating_log, self.log_offset_path)
        return self._log


    @property
    def pid(self) -> str:
        """Read the PID file and return its contents.
        Returns `None` if the PID file does not exist.
        """
        if not self.pid_path.exists():
            return None
        try:
            with open(self.pid_path, 'r') as f:
                text = f.read()
        except Exception as e:
            warn(e)
            text = None
        return text.rstrip('\n') if text is not None else text


    @property
    def pid_path(self) -> pathlib.Path:
        """
        Return the path to a file containing the PID for this Daemon.
        """
        return self.path / 'process.pid'


    @property
    def pid_lock(self) -> 'fasteners.InterProcessLock':
        """
        Return the process lock context manager.
        """
        if '_pid_lock' in self.__dict__:
            return self._pid_lock

        fasteners = attempt_import('fasteners')
        self._pid_lock = fasteners.InterProcessLock(self.pid_path)
        return self._pid_lock


    @property
    def pickle_path(self) -> pathlib.Path:
        """
        Return the path for the pickle file.
        """
        return self.path / 'pickle.pkl'


    def read_properties(self) -> Optional[Dict[str, Any]]:
        """Read the properties JSON file and return the dictionary."""
        if not self.properties_path.exists():
            return None
        try:
            with open(self.properties_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            return {}


    def read_pickle(self) -> Daemon:
        """Read a Daemon's pickle file and return the `Daemon`."""
        import pickle, traceback
        if not self.pickle_path.exists():
            error(f"Pickle file does not exist for daemon '{self.daemon_id}'.")
        try:
            with open(self.pickle_path, 'rb') as pickle_file:
                daemon = pickle.load(pickle_file)
            success, msg = True, 'Success'
        except Exception as e:
            success, msg = False, str(e)
            daemon = None
            traceback.print_exception(type(e), e, e.__traceback__)
            try:
                if self.pickle_path.exists():
                    self.pickle_path.unlink()
            except Exception as ue:
                traceback.print_exception(type(ue), ue, ue.__traceback__)
        if not success:
            error(msg)
        return daemon


    @property
    def properties(self) -> Optional[Dict[str, Any]]:
        """
        Return the contents of the properties JSON file.
        """
        _file_properties = self.read_properties()
        if not self._properties:
            self._properties = _file_properties
        if self._properties is None:
            self._properties = {}
        if _file_properties is not None:
            self._properties = apply_patch_to_config(self._properties, _file_properties)
        return self._properties


    @property
    def hidden(self) -> bool:
        """
        Return a bool indicating whether this Daemon should be displayed.
        """
        return self.daemon_id.startswith('_') or self.daemon_id.startswith('.')


    def write_properties(self) -> SuccessTuple:
        """Write the properties dictionary to the properties JSON file
        (only if self.properties exists).
        """
        success, msg = False, f"No properties to write for daemon '{self.daemon_id}'."
        if self.properties is not None:
            try:
                self.path.mkdir(parents=True, exist_ok=True)
                with open(self.properties_path, 'w+') as properties_file:
                    json.dump(self.properties, properties_file)
                success, msg = True, 'Success'
            except Exception as e:
                success, msg = False, str(e)
        return success, msg


    def write_pickle(self) -> SuccessTuple:
        """Write the pickle file for the daemon."""
        import pickle, traceback
        try:
            self.path.mkdir(parents=True, exist_ok=True)
            with open(self.pickle_path, 'wb+') as pickle_file:
                pickle.dump(self, pickle_file)
            success, msg = True, "Success"
        except Exception as e:
            success, msg = False, str(e)
            traceback.print_exception(type(e), e, e.__traceback__)
        return success, msg


    def _setup(
            self,
            allow_dirty_run: bool = False,
        ) -> None:
        """
        Update properties before starting the Daemon.
        """
        began = datetime.datetime.utcnow()
        if self.properties is None:
            self._properties = {}

        self._properties.update({
            'target': {
                'name': self.target.__name__,
                'module': self.target.__module__,
                'args': self.target_args,
                'kw': self.target_kw,
            },
            'process': {
                'began': began.isoformat(),
            },
        })
        self.mkdir_if_not_exists(allow_dirty_run)
        _write_properties_success_tuple = self.write_properties()
        if not _write_properties_success_tuple[0]:
            error(_write_properties_success_tuple[1])

        _write_pickle_success_tuple = self.write_pickle()
        if not _write_pickle_success_tuple[0]:
            error(_write_pickle_success_tuple[1])


    def cleanup(self, keep_logs: bool = False) -> None:
        """Remove a daemon's directory after execution.

        Parameters
        ----------
        keep_logs: bool, default False
            If `True`, skip deleting the daemon's log files.
        """
        if self.path.exists():
            try:
                shutil.rmtree(self.path)
            except Exception as e:
                warn(e)
        if not keep_logs:
            self.rotating_log.delete()


    def __getstate__(self):
        dill = attempt_import('dill')
        return {
            'target': dill.dumps(self.target),
            'target_args': self.target_args,
            'target_kw': self.target_kw,
            'daemon_id': self.daemon_id,
            'label': self.label,
            'properties': self.properties,
        }

    def __setstate__(self, _state : Dict[str, Any]):
        dill = attempt_import('dill')
        _state['target'] = dill.loads(_state['target'])
        self._pickle = True
        self.__init__(**_state) 

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.daemon_id

    def __eq__(self, other):
        if not isinstance(other, Daemon):
            return False
        return self.daemon_id == other.daemon_id

    def __hash__(self):
        return hash(self.daemon_id)       
