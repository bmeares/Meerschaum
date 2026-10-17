#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage a job's daemon: its directory, its process and its logs.

`Daemon` is a read and control façade over the native daemon (crate `meerschaum-jobs`, bound as
`meerschaum._core.Daemon`). A daemon runs a Meerschaum command (`Daemon(sysargs=[...])`, what
`Job` manages); running an arbitrary Python callable as a daemon (`Daemon(target=...)`) is not
supported in Meerschaum v5.
"""

from __future__ import annotations

import pathlib
import shlex

from meerschaum.utils.typing import Any, Callable, Dict, List, Optional, SuccessTuple, Union

_REMOVED = (
    "Running a Python callable as a daemon is not supported in Meerschaum v5. "
    "Use `meerschaum.jobs.Job(name, sysargs)` to run a Meerschaum command in the background "
    "(see the migration guide, 'Removed in 5.0')."
)

#: `Daemon.run()` results of this process, by daemon ID (kept for v4's `_results`).
_results: Dict[str, SuccessTuple] = {}


class Daemon:
    """
    Daemonize Python functions into background processes (v5: Meerschaum commands).
    """

    def __init__(
        self,
        target: Optional[Callable[[Any], Any]] = None,
        target_args: Optional[List[Any]] = None,
        target_kw: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        daemon_id: Optional[str] = None,
        label: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        pickle: bool = True,
        *,
        sysargs: Optional[List[str]] = None,
    ):
        """
        Open an existing daemon (`Daemon(daemon_id='name')`) or describe a new one
        (`Daemon(daemon_id='name', sysargs=[...])`).

        Parameters
        ----------
        target: Optional[Callable[[Any], Any]], default None
            Only `meerschaum._internal.entry.entry` (a Meerschaum command, with the sysargs as
            its first argument) is supported.

        target_args: Optional[List[Any]], default None
            The arguments of `target`: `[sysargs]`.

        env: Optional[Dict[str, str]], default None
            Environment variables to set in the daemon's process.

        daemon_id: Optional[str], default None
            The name of the daemon (and of the job).

        label: Optional[str], default None
            The label to show (the joined sysargs).

        properties: Optional[Dict[str, Any]], default None
            Patch the daemon's properties.
        """
        from meerschaum.jobs import _native
        core = _native.core()
        if target is not None:
            if getattr(target, '__name__', None) != 'entry':
                raise NotImplementedError(_REMOVED)
            sysargs = list((target_args or [[]])[0] or [])

        if target_kw:
            raise NotImplementedError(_REMOVED)

        if daemon_id is None:
            if sysargs is None:
                raise ValueError("Pass a `daemon_id` to open an existing daemon.")
            daemon_id = core.jobs_new_daemon_name()

        if env:
            properties = {**(properties or {}), 'env': dict(env)}

        if sysargs is not None and label is None:
            label = shlex.join(sysargs)

        self.daemon_id = daemon_id
        self._core = core.Daemon(daemon_id, sysargs, label, properties)

    @classmethod
    def from_properties_file(cls, daemon_id: str) -> 'Daemon':
        """Open the daemon `daemon_id` from its `properties.json`."""
        return cls(daemon_id=daemon_id)

    @property
    def label(self) -> str:
        return self._core.label

    @property
    def target(self) -> None:
        return None

    @property
    def target_args(self) -> List[List[str]]:
        """The arguments of the daemon's target: `[sysargs]`."""
        return [self._core.sysargs]

    @property
    def target_kw(self) -> Dict[str, Any]:
        return {}

    @property
    def hidden(self) -> bool:
        return self._core.hidden

    @property
    def status(self) -> str:
        """Return `'running'`, `'paused'` or `'stopped'`."""
        return self._core.status

    @property
    def pid(self) -> Union[int, None]:
        """Return the PID of the daemon's process, if it has one."""
        return self._core.pid

    @property
    def process(self) -> Union[int, None]:
        """Return the PID if the process exists (v4 returned a `psutil.Process`)."""
        return self._core.process

    @property
    def properties(self) -> Dict[str, Any]:
        """Return the daemon's properties (`properties.json` patched with this object's)."""
        return self._core.properties()

    @properties.setter
    def properties(self, patch: Dict[str, Any]) -> None:
        self._core.update_properties(patch)

    def read_properties(self) -> Optional[Dict[str, Any]]:
        """Read `properties.json`."""
        return self._core.read_properties()

    def write_properties(self) -> SuccessTuple:
        """Write `properties.json`."""
        return self._core.write_properties()

    def _paths(self) -> Dict[str, str]:
        return self._core.paths()

    @property
    def path(self) -> pathlib.Path:
        """The daemon's directory (`jobs/<daemon_id>`)."""
        return pathlib.Path(self._paths()['dir'])

    @property
    def properties_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['properties'])

    @property
    def pid_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['pid'])

    @property
    def pickle_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['pickle'])

    @property
    def stdin_file_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['stdin'])

    @property
    def blocking_stdin_file_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['stdin_block'])

    @property
    def prompt_kwargs_file_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['prompt_kwargs'])

    @property
    def stop_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['stop'])

    @property
    def log_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['log'])

    @property
    def log_offset_path(self) -> pathlib.Path:
        return pathlib.Path(self._paths()['log_offset'])

    @property
    def stdin_file(self):
        """A `StdinFile` for the daemon's stdin FIFO."""
        from meerschaum.utils.daemon.StdinFile import StdinFile
        return StdinFile(self.stdin_file_path, self.blocking_stdin_file_path)

    @property
    def rotating_log(self):
        """A `RotatingFile` over the daemon's logs."""
        from meerschaum.utils.daemon.RotatingFile import RotatingFile
        logs = self.properties.get('logs', {}) or {}
        return RotatingFile(
            self.log_path,
            num_files_to_keep=logs.get('num_files_to_keep'),
            max_file_size=logs.get('max_file_size'),
            write_timestamps=bool(logs.get('write_timestamps', False)),
            timestamp_format=logs.get('timestamp_format'),
        )

    @property
    def log_text(self) -> Optional[str]:
        """The text of the daemon's logs."""
        return self._core.log_text()

    def readlines(self) -> List[str]:
        """The log lines since the last call (the cursor is persisted)."""
        return self._core.readlines()

    def _read_stop_file(self) -> Optional[Dict[str, Any]]:
        return self._core.read_stop_file()

    def mkdir_if_not_exists(self, allow_dirty_run: bool = False) -> SuccessTuple:
        """Create the daemon's directory."""
        return self._core.mkdir_if_not_exists(allow_dirty_run)

    def run(
        self,
        keep_daemon_output: bool = True,
        allow_dirty_run: bool = False,
        wait: bool = False,
        timeout: Union[int, float] = 4,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Run the daemon's command in the background.

        Parameters
        ----------
        keep_daemon_output: bool, default True
            If `False`, delete the daemon's directory and logs when it finishes.

        allow_dirty_run: bool, default False
            If `True`, run although the daemon's directory exists.

        wait: bool, default False
            If `True`, wait up to `timeout` seconds until the daemon's process is running.

        timeout: Union[int, float], default 4
            How long to wait when `wait` is `True`.
        """
        result = self._core.run(keep_daemon_output, allow_dirty_run)
        if wait and result[0]:
            import time
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline and self.status == 'stopped':
                time.sleep(0.05)
            if self.status == 'stopped' and 'result' not in (self.read_properties() or {}):
                result = False, f"Daemon '{self.daemon_id}' did not start within {timeout} seconds."
        _results[self.daemon_id] = result
        return result

    def kill(
        self,
        timeout: Union[int, float, None] = 8,
        debug: bool = False,
    ) -> SuccessTuple:
        """Forcibly terminate the daemon."""
        return self._core.kill(timeout)

    def quit(
        self,
        timeout: Union[int, float, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """Gracefully quit the daemon (`SIGINT`)."""
        return self._core.quit(timeout)

    def pause(
        self,
        timeout: Union[int, float, None] = None,
        check_timeout_interval: Union[float, int, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """Suspend the daemon's process."""
        return self._core.pause(timeout, check_timeout_interval)

    def resume(
        self,
        timeout: Union[int, float, None] = None,
        check_timeout_interval: Union[float, int, None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """Resume the daemon's process."""
        return self._core.resume(timeout, check_timeout_interval)

    def cleanup(self, keep_logs: bool = False, debug: bool = False) -> SuccessTuple:
        """Remove the daemon's directory (and its logs unless `keep_logs`)."""
        return self._core.cleanup(keep_logs)

    def get_timeout_seconds(self, timeout: Union[int, float, None] = None) -> Union[int, float]:
        """Return `timeout`, or the configured `jobs:timeout_seconds`."""
        return self._core.get_timeout_seconds(timeout)

    def get_check_timeout_interval_seconds(
        self,
        check_timeout_interval: Union[int, float, None] = None,
    ) -> Union[int, float]:
        """Return the interval, or the configured `jobs:check_timeout_interval_seconds`."""
        return self._core.get_check_timeout_interval_seconds(check_timeout_interval)

    def __eq__(self, other) -> bool:
        return isinstance(other, Daemon) and other.daemon_id == self.daemon_id

    def __hash__(self) -> int:
        return hash(self.daemon_id)

    def __str__(self) -> str:
        return self.daemon_id

    def __repr__(self) -> str:
        return str(self)
