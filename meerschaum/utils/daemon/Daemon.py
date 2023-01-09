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
        daemoniker = attempt_import('daemoniker')

        if platform.system() == 'Windows':
            success, msg = self._run_windows(
                keep_daemon_output = keep_daemon_output,
                allow_dirty_run = allow_dirty_run,
            )
            rc = 0 if success else 1
            os._exit(rc)

        self._setup(allow_dirty_run)

        with daemoniker.Daemonizer() as (is_setup, daemonizer):
            if is_setup:
                pass
            is_parent = daemonizer(
                str(self.pid_path.as_posix()),
                stdout_goto = str(self.stdout_path.as_posix()),
                stderr_goto = str(self.stderr_path.as_posix()),
                strip_cmd_args = (platform.system() != 'Windows'),
            )
            if is_parent:
                pass

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


    def _run_windows(
            self,
            keep_daemon_output: bool = True,
            allow_dirty_run: bool = False,
            debug: bool = False,
        ) -> SuccessTuple:
        """
        Run the Daemon from Windows.
        """
        import sys
        from meerschaum.utils.process import run_process
        from meerschaum.config._paths import PACKAGE_ROOT_PATH
        self._setup(allow_dirty_run)
        target_module = attempt_import(self.target.__module__, lazy=False, install=False)
        target_root_module_name = target_module.__name__.split('.')[0]
        target_root_module = attempt_import(target_root_module_name, lazy=False, install=False)
        target_root_module_path = pathlib.Path(target_root_module.__file__)
        target_parent_path = (
            target_root_module_path.parent.parent
            if target_root_module_path.name == '__init__.py'
            else target_root_module.parent
        )

        temp_script_path = self.path / 'entry.py'
        temp_script_path.parent.mkdir(exist_ok=True)
        code_to_write = (
            "import sys\n"
            + "import pathlib\n"
            + "\n"
            + f"pid_path = '{self.pid_path.as_posix()}'\n"
            + f"stdout_path = '{self.stdout_path.as_posix()}'\n"
            + f"stderr_path = '{self.stderr_path.as_posix()}'\n"
            + f"args = {json.dumps(self.target_args)}\n"
            + f"kw = {json.dumps(self.target_kw)}\n"
            + "\n"
            + f"sys.path[0] = '{PACKAGE_ROOT_PATH.parent.as_posix()}'\n"
            + "from meerschaum.utils.packages import attempt_import\n"
            + "daemoniker = attempt_import('daemoniker')\n"
            + "\n"
            + "with daemoniker.Daemonizer() as (is_setup, daemonizer):\n"
            + "    if is_setup:\n"
            + "        pass\n"
            + "    is_parent, args, kw = daemonizer(\n"
            + "        pid_path,\n"
            + "        args,\n"
            + "        kw,\n"
            + "        stdout_goto = stdout_path,\n"
            + "        stderr_goto = stderr_path,\n"
            + ")\n"
            + "    if is_parent:\n"
            + "        pass"
            + "\n"
            + "sighandler = daemoniker.SignalHandler1(pid_path)\n"
            + "sighandler.start()\n"
            + "\n"
            + f"sys.path.insert(0, '{target_parent_path.as_posix()}')\n"
            + "try:\n"
            + f"    from {self.target.__module__} import {self.target.__name__}\n"
            + "    imported = True\n"
            + "except Exception as e:\n"
            + "    print(f'Failed to import job module with exception: {e}')\n"
            + "    imported = False\n"
            + "if imported:\n"
            + "    " + self.target.__name__ + "(*args, **kw)\n"
            + "\n"
            + "import datetime\n"
            + "from meerschaum.utils.daemon import Daemon\n"
            + f"daemon = Daemon(daemon_id='{self.daemon_id}')\n"
            + f"keep_daemon_output = {keep_daemon_output}\n"
            + "if keep_daemon_output:\n"
            + "    now = datetime.datetime.utcnow()\n"
            + "    daemon.properties['process']['ended'] = now.isoformat()\n"
            + "    daemon.write_properties()\n"
            + "else:\n"
            + "    daemon.cleanup()\n"
        )
        with open(temp_script_path, 'w', encoding='utf-8') as f:
            f.write(code_to_write)

        return_code = run_process([sys.executable, temp_script_path.as_posix()])
        success = return_code == 0
        msg = (
            f"Succesfully started Daemon '{self.daemon_id}'." 
            if success
            else f"Failed to start Daemon '{self.daemon_id}'."
        )
        return success, msg


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
            return self._run_windows(
                keep_daemon_output = keep_daemon_output,
                allow_dirty_run = True,
                debug = debug,
            )

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
        timeout: Optional[int] :
             (Default value = 3)

        Returns
        -------
        A SuccessTuple indicating success.
        """
        daemoniker, psutil = attempt_import('daemoniker', 'psutil')
        success, msg = self._send_signal(daemoniker.SIGTERM, timeout=timeout)
        if success:
            return success, msg
        process = self.process
        if process is None or not process.is_running():
            return True, "Process has already stopped."
        try:
            process.terminate()
            process.kill()
            process.wait(timeout=10)
        except Exception as e:
            return False, f"Failed to kill job {self} with exception: {e}"
        return True, "Success"


    def quit(self, timeout: Optional[int] = 3) -> SuccessTuple:
        """Gracefully quit a running daemon.
        Sends a SIGINT signal the to process.

        Parameters
        ----------
        timeout: Optional[int] :
             (Default value = 3)

        Returns
        -------

        """
        daemoniker, psutil = attempt_import('daemoniker', 'psutil')
        return self._send_signal(daemoniker.SIGINT, timeout=timeout)

    def _send_signal(
            self,
            signal,
            timeout: Optional[Union[float, int]] = 3,
            check_timeout_interval: float = 0.1,
        ) -> SuccessTuple:
        """Send a signal to the daemon process.

        Parameters
        ----------
        signal:
            The signal the send to the daemon.
            Examples include `daemoniker.SIGINT` and `daemoniker.SIGTERM`.

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
        daemoniker = attempt_import('daemoniker')

        try:
            daemoniker.send(str(self.pid_path.as_posix()), signal)
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

    @property
    def sighandler(self) -> Optional[daemoniker.SignalHandler1]:
        """

        Parameters
        ----------

        Returns
        -------
        type
            If the process is not running, return `None`.

        """
        # if not self.pid_path.exists():
        #     return None

        def _quit(*args, **kw):
            from meerschaum.__main__ import _exit
            _exit()
        daemoniker = attempt_import('daemoniker')
        if '_sighandler' not in self.__dict__:
            self._sighandler = daemoniker.SignalHandler1(
                str(self.pid_path.as_posix()),
                sigint = _quit,
                sigterm = _quit,
                sigabrt = _quit,
            )
        return self._sighandler

    def mkdir_if_not_exists(self, allow_dirty_run : bool = False):
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
    def properties_path(self):
        """
        Return the `propterties.json` path for this Daemon.
        """
        return self.path / 'properties.json'

    @property
    def stdout_path(self):
        """
        Return the path to redirect stdout into.
        """
        return self.log_path

    @property
    def stderr_path(self):
        """
        Return the path to redirect stderr into.
        """
        return self.log_path

    @property
    def log_path(self):
        """
        Return the log path.
        """
        return LOGS_RESOURCES_PATH / (self.daemon_id + '.log')

    @property
    def log_offset_path(self):
        return self.path / (self.daemon_id + '.log.offset')

    @property
    def log_text(self) -> Optional[str]:
        """Read the log file and return its contents.
        Returns `None` if the log file does not exist.
        """
        if not self.log_path.exists():
            return None
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            warn(e)
            text = None
        return text

    
    @property
    def log(self) -> 'meerschaum.utils.daemon.Log':
        """
        Return a `meerschaum.utils.daemon.Log` object for this daemon.
        """
        if self.__dict__.get('_log', None) is not None:
            return self.__dict__['_log']
        from meerschaum.utils.daemon import Log
        self._log = Log(self.log_path, self.log_offset_path)
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
            If `True`, skip deleting the daemon's log file.
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
        return self.daemon_id

    def __eq__(self, other):
        if not isinstance(other, Daemon):
            return False
        return self.daemon_id == other.daemon_id

    def __hash__(self):
        return hash(self.daemon_id)
