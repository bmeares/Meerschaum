#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the Meerschaum abstraction atop daemons.
"""

import shlex
import asyncio
from datetime import datetime

from meerschaum.utils.typing import List, Optional, Union, SuccessTuple
from meerschaum._internal.entry import entry
from meerschaum._internal.arguments._parse_arguments import parse_arguments
from meerschaum.utils.warnings import warn

BANNED_CHARS: List[str] = [
    ',', ';', "'", '"',
]


class Job:
    """
    Manage a `meerschaum.utils.daemon.Daemon`.
    """

    def __init__(
        self,
        name: str,
        sysargs: Union[List[str], str, None] = None,
        restart: Optional[bool] = None,
        executor_keys: Optional[str] = None,
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

        restart: Optional[bool], default None
            If `True`, restart a stopped job (unless manually stopped).

        executor_keys: Optional[str], default None
            If provided, execute the job remotely on an API instance, e.g. 'api:main'.
        """
        from meerschaum.utils.daemon import Daemon
        for char in BANNED_CHARS:
            if char in name:
                raise ValueError(f"Invalid name: ({char}) is not allowed.")

        if isinstance(sysargs, str):
            sysargs = shlex.split(sysargs)

        self.name = name
        try:
            self._daemon = Daemon(daemon_id=name)
        except Exception:
            self._daemon = None

        self._properties_patch = {}
        if restart is not None:
            self._properties_patch.update({'restart': restart})

        daemon_sysargs = (
            self._daemon.properties.get('target', {}).get('args', [None])[0]
            if self._daemon is not None
            else None
        )

        if daemon_sysargs and sysargs and daemon_sysargs != sysargs:
            warn("Given sysargs differ from existing sysargs.")

        _sysargs = (
            [
                arg
                for arg in (daemon_sysargs or sysargs or [])
                if arg not in ('-d', '--daemon')
            ]
        )
        self._sysargs = _sysargs

    def start(self) -> SuccessTuple:
        """
        Start the job's daemon.
        """
        if self.is_running():
            return True, f"{self} is already running."

        return self.daemon.run(
            keep_daemon_output=True,
            allow_dirty_run=True,
        )

    def stop(self, timeout_seconds: Optional[int] = None) -> SuccessTuple:
        """
        Stop the job's daemon.
        """
        if self.daemon.status == 'stopped':
            return True, f"{self} is not running."

        quit_success, quit_msg = self.daemon.quit(timeout=timeout_seconds)
        if quit_success:
            return quit_success, quit_msg

        warn(
            f"Failed to gracefully quit {self}.",
            stack=False,
        )
        return self.daemon.kill(timeout=timeout_seconds)

    def pause(self, timeout_seconds: Optional[int] = None) -> SuccessTuple:
        """
        Pause the job's daemon.
        """
        return self.daemon.pause(timeout=timeout_seconds)

    def delete(self) -> SuccessTuple:
        """
        Delete the job and its daemon.
        """
        if self.is_running():
            stop_success, stop_msg = self.stop()
            if not stop_success:
                return stop_success, stop_msg

        return self.daemon.cleanup()

    def is_running(self) -> bool:
        """
        Determine whether the job's daemon is running.
        """
        return self.daemon.status == 'running'

    def exists(self) -> bool:
        """
        Determine whether the job exists.
        """
        return self.daemon.path.exists()

    @property
    def restart(self) -> bool:
        """
        Return whether to restart a stopped job.
        """
        return self.daemon.properties.get('restart', False)

    @property
    def result(self) -> SuccessTuple:
        """
        Return the `SuccessTuple` when the job has terminated.
        """
        if self.is_running():
            return True, f"{self} is running."

        _result = self.daemon.properties.get('result', None)
        if _result is None:
            return False, "No result available."

        return tuple(_result)

    @property
    def sysargs(self) -> List[str]:
        """
        Return the sysargs to use for the Daemon.
        """
        return self._sysargs

    @property
    def daemon(self) -> 'Daemon':
        """
        Return the daemon which this job manages.
        """
        from meerschaum.utils.daemon import Daemon
        if self._daemon is not None:
            return self._daemon

        self._daemon = Daemon(
            target=entry,
            target_args=[self.sysargs],
            daemon_id=self.name,
            label=self.sysargs,
            properties=self._properties_patch,
        )

        return self._daemon

    @property
    def began(self) -> Union[datetime, None]:
        """
        The datetime when the job began running.
        """
        began_str = self.daemon.properties.get('process', {}).get('began', None)
        if began_str is None:
            return None

        return datetime.fromisoformat(began_str)

    @property
    def ended(self) -> Union[datetime, None]:
        """
        The datetime when the job stopped running.
        """
        ended_str = self.daemon.properties.get('process', {}).get('ended', None)
        if ended_str is None:
            return None

        return datetime.fromisoformat(ended_str)

    @property
    def paused(self) -> Union[datetime, None]:
        """
        The datetime when the job was suspended while running.
        """
        paused_str = self.daemon.properties.get('process', {}).get('paused', None)
        if paused_str is None:
            return None

        return datetime.fromisoformat(paused_str)
    
    @property
    def log_text(self) -> Union[str, None]:
        """
        Return the output text of the job's daemon.
        """
        return self.daemon.log_text

    def __str__(self) -> str:
        return f'Job("{self.name}", "{shlex.join(self.sysargs)}")'

    def __repr__(self) -> str:
        return str(self)

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

        if self.daemon.stop_path.exists():
            return True, f"{self} was manually stopped."

        return self.start()
