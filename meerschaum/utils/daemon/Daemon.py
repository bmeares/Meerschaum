#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage running daemons via the Daemon class.
"""

from __future__ import annotations
import os, pathlib, threading, json, shutil, datetime
from meerschaum.utils.typing import Optional, Dict, Any, SuccessTuple, Callable, List, Union
from meerschaum.config._paths import DAEMON_RESOURCES_PATH, LOGS_RESOURCES_PATH
from meerschaum.config._patch import apply_patch_to_config
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.packages import attempt_import, venv_exec
from meerschaum.utils.daemon._names import get_new_daemon_name

class Daemon:
    """
    Manage running daemons via the Daemon class.
    """

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
        target : Optional[Callable[[Any], Any]] = None,
        target_args : Optional[List[str]] = None,
        target_kw : Optional[Dict[str, Any]] = None,
        daemon_id : Optional[str] = None,
        label : Optional[str] = None,
        properties : Optional[Dict[str, Any]] = None,
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
                error(
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

    def _run_exit(
            self,
            keep_daemon_output : bool = True,
            allow_dirty_run : bool = False,
        ) -> Any:
        """
        Run the daemon's target function.
        NOTE: This WILL EXIT the parent process!

        :param keep_daemon_output:
            If `False`, delete the daemon's output directory upon exiting.
            Defaults to `True`.

        :param allow_dirty_run:
            If `True`, run the daemon, even if the `daemon_id` directory exists.
            This option is dangerous because if the same `daemon_id` runs twice,
            the last to finish will overwrite the output of the first.
            Defaults to `False`.
        """

        daemoniker = attempt_import('daemoniker')

        began = datetime.datetime.utcnow()
        if self.properties is None:
            self._properties = {}
        self._properties.update({
            'target' : {
                'name' : self.target.__name__,
                'args' : self.target_args,
                'kw' : self.target_kw,
            },
            'process' : {
                'began' : began.isoformat(),
            },
        })
        self.mkdir_if_not_exists(allow_dirty_run)
        _write_properties_success_tuple = self.write_properties()
        if not _write_properties_success_tuple[0]:
            error(_write_properties_success_tuple[1])

        _write_pickle_success_tuple = self.write_pickle()
        if not _write_pickle_success_tuple[0]:
            error(_write_pickle_success_tuple[1])

        with daemoniker.Daemonizer() as (is_setup, daemonizer):
            is_parent, self = daemonizer(
                str(self.pid_path),
                self,
                stdout_goto = str(self.stdout_path),
                stderr_goto = str(self.stderr_path),
                strip_cmd_args = True
            )

        self.sighandler.start()

        try:
            result = self.target(*self.target_args, **self.target_kw)
        except Exception as e:
            warn(e, stacklevel=3)
            result = e

        if keep_daemon_output:
            self.properties['process']['ended'] = datetime.datetime.utcnow().isoformat()
            self.write_properties()
        else:
            self.cleanup()
        return result

    def run(
            self,
            keep_daemon_output : bool = True,
            allow_dirty_run : bool = False,
            debug : bool = False,
        ) -> SuccessTuple:
        """
        Run the daemon as a child process and continue executing the parent.

        :param keep_daemon_output:
            If `False`, delete the daemon's output directory upon exiting.
            Defaults to `True`.

        :param allow_dirty_run:
            If `True`, run the daemon, even if the `daemon_id` directory exists.
            This option is dangerous because if the same `daemon_id` runs twice,
            the last to finish will overwrite the output of the first.
            Defaults to `False`.

        """
        self.mkdir_if_not_exists(allow_dirty_run)
        _write_pickle_success_tuple = self.write_pickle()
        if not _write_pickle_success_tuple[0]:
            return _write_pickle_success_tuple

        _launch_daemon_code = (
            "from meerschaum.utils.daemon import Daemon; "
            + f"daemon = Daemon(daemon_id='{self.daemon_id}'); "
            + f"daemon._run_exit(keep_daemon_output={keep_daemon_output}, "
            + f"allow_dirty_run=True)"
        )
        _launch_success_bool = venv_exec(_launch_daemon_code, debug=debug, venv=None)
        msg = "Success" if _launch_success_bool else f"Failed to start daemon '{self.daemon_id}'."
        return _launch_success_bool, msg

    def kill(self, timeout : Optional[int] = 3) -> SuccessTuple:
        """
        Forcibly terminate a running daemon.
        Sends a SIGTERM signal to the process.
        """
        daemoniker = attempt_import('daemoniker')
        return self._send_signal(daemoniker.SIGTERM, timeout=timeout)

    def quit(self, timeout : Optional[int] = 3) -> SuccessTuple:
        """
        Gracefully quit a running daemon.
        Sends a SIGINT signal the to process.
        """
        daemoniker = attempt_import('daemoniker')
        return self._send_signal(daemoniker.SIGINT, timeout=timeout)

    def _send_signal(
            self,
            signal : daemoniker.DaemonikerSignal,
            timeout : Optional[Union[float, int]] = 3,
            check_timeout_interval : float = 0.1,
        ):
        """
        Send a signal to the daemon process.

        :param signal:
            The signal the send to the daemon.
            Examples include `daemoniker.SIGINT` and `daemoniker.SIGTERM`.

        :param timeout:
            The maximum number of seconds to wait for a process to terminate.
            Defaults to 3.

        :param check_timeout_interval:
            The number of seconds to wait between checking if the process is still running.
            Defaults to 0.1.
        """
        import time
        daemoniker = attempt_import('daemoniker')

        try:
            daemoniker.send(str(self.pid_path), daemoniker.SIGINT)
        except Exception as e:
            return False, str(e)
        if timeout is None:
            return True, f"Successfully sent '{signal}' to daemon '{self.daemon_id}'."
        begin = time.time()
        while (time.time() - begin) < timeout:
            if not self.pid_path.exists():
                return True, f"Successfully stopped daemon '{self.daemon_id}'."
            time.sleep(check_timeout_interval)
        return False, (
            f"Failed to stop daemon '{self.daemon_id}' within {timeout} second"
            + ('s' if timeout != 1 else '') + '.'
        )

    @property
    def sighandler(self) -> Optional[daemoniker.SignalHandler1]:
        """
        Return the signal handler for the daemon.

        If the process is not running, return `None`.
        """
        if not self.pid_path.exists():
            return None

        def _quit(*args, **kw):
            from meerschaum.__main__ import _exit
            _exit()
        daemoniker = attempt_import('daemoniker')
        if '_sighandler' not in self.__dict__:
            self._sighandler = daemoniker.SignalHandler1(
                str(self.pid_path),
                sigint = _quit,
                sigterm = _quit,
                sigabrt = _quit,
            )
        return self._sighandler

    def mkdir_if_not_exists(self, allow_dirty_run : bool = False):
        """
        Create the Daemon's directory.
        
        If `allow_dirty_run` is False and the directory already exists,
        raise an error.
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
    def path(self) -> pathlib.Path:
        """
        Return the daemon's directory path.
        """
        return DAEMON_RESOURCES_PATH / self.daemon_id

    @property
    def properties_path(self):
        """
        Return the path for the properties JSON file.
        """
        return self.path / 'properties.json'

    @property
    def stdout_path(self):
        """
        Return the path for the stdout text file.
        """
        return self.log_path
        #  return self.path / 'stdout.txt'

    @property
    def stderr_path(self):
        """
        Return the path for the stderr text file.
        """
        return self.log_path
        #  return self.path / 'stderr.txt'

    @property
    def log_path(self):
        """
        Return the path for the output log file.
        """
        return LOGS_RESOURCES_PATH / (self.daemon_id + '.log')

    @property
    def log_offset_path(self):
        return self.path / (self.daemon_id + '.log.offset')

    @property
    def log_text(self) -> Optional[str]:
        """
        Read the log file and return its contents.
        Returns `None` if the log file does not exist.
        """
        if not self.log_path.exists():
            return None
        try:
            with open(self.log_path, 'r') as f:
                text = f.read()
        except Exception as e:
            warn(e)
            text = None
        return text

    @property
    def pid(self) -> str:
        """
        Read the PID file and return its contents.
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
        Return the path for the pid file.
        """
        return self.path / 'process.pid'

    @property
    def pickle_path(self) -> pathlib.Path:
        """
        Return the path for the pickle file.
        """
        return self.path / 'pickle.pkl'

    def read_properties(self) -> Optional[Dict[str, Any]]:
        """
        Read the properties JSON file and return the dictionary.
        """
        if not self.properties_path.exists():
            return None
        try:
            with open(self.properties_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            return {}

    def read_pickle(self) -> Daemon:
        """
        Read a Daemon's pickle file and return the `Daemon`.
        """
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
        if not success:
            error(msg)
        return daemon

    @property
    def properties(self) -> Optional[Dict[str, Any]]:
        """
        Return the properties dictionary. If none was provided, attempt to
        read the properties JSON file.
        """
        _file_properties = self.read_properties()
        if not self._properties:
            self._properties = _file_properties
        if self._properties is None:
            self._properties = {}
        if _file_properties is not None:
            self._properties = apply_patch_to_config(self._properties, _file_properties)
        return self._properties

    def write_properties(self) -> SuccessTuple:
        """
        Write the properties dictionary to the properties JSON file
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
        """
        Write the pickle file for the daemon.
        """
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

    def cleanup(self, keep_logs : bool = False):
        """
        Remove a daemon's directory after execution.

        :param keep_logs:
            If `True`, skip deleting the daemon's log file.
            Defaults to `False`.
        """
        if self.path.exists():
            try:
                shutil.rmtree(self.path)
            except Exception as e:
                warn(e)
        if self.log_path.exists() and not keep_logs:
            try:
                os.remove(self.log_path)
            except Exception as e:
                warn(e)

    def __getstate__(self):
        dill = attempt_import('dill')
        return {
            'target' : dill.dumps(self.target),
            'target_args' : self.target_args,
            'target_kw' : self.target_kw,
            'daemon_id' : self.daemon_id,
            'label' : self.label,
            'properties' : self.properties,
        }

    def __setstate__(self, _state : Dict[str, Any]):
        dill = attempt_import('dill')
        _state['target'] = dill.loads(_state['target'])
        self._pickle = True
        self.__init__(**_state) 

    def __repr__(self):
        return str(self)

    def __str__(self):
        #  return str(self.daemon_id) + '\n    (' + str(self.label) + ')'
        return self.daemon_id

    def __eq__(self, other):
        if not isinstance(other, Daemon):
            return False
        return self.daemon_id == other.daemon_id

    def __hash__(self):
        return hash(self.daemon_id)
