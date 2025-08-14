#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the base class for a Job executor.
"""

from __future__ import annotations

from abc import abstractmethod

from meerschaum.connectors import Connector
from meerschaum.utils.typing import (
    List, Dict, SuccessTuple, TYPE_CHECKING, Optional, Any, Union, Callable,
)

if TYPE_CHECKING:
    from meerschaum.jobs import Job
    from datetime import datetime

class Executor(Connector):
    """
    Define the methods for managing jobs.
    """

    @abstractmethod
    def get_job_names(self, debug: bool = False) -> List[str]:
        """
        Return a list of existing jobs, including hidden ones.
        """

    @abstractmethod
    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """

    @abstractmethod
    def get_jobs(self, debug: bool = False) -> Dict[str, Job]:
        """
        Return a dictionary of existing jobs.
        """

    @abstractmethod
    def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return a job's metadata.
        """

    @abstractmethod
    def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return the underlying daemon's properties.
        """
    @abstractmethod
    def get_job_status(self, name: str, debug: bool = False) -> str:
        """
        Return the job's status.
        """

    @abstractmethod
    def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job began running.
        """

    @abstractmethod
    def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return when a job stopped running.
        """

    @abstractmethod
    def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
        """
        Return a job's `paused` timestamp, if it exists.
        """
    
    @abstractmethod
    def create_job(
        self,
        name: str,
        sysargs: List[str],
        properties: Optional[Dict[str, Any]] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Create a new job.
        """

    @abstractmethod
    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Start a job.
        """

    @abstractmethod
    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job.
        """

    @abstractmethod
    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Pause a job.
        """

    @abstractmethod
    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Delete a job.
        """

    @abstractmethod
    def get_logs(self, name: str, debug: bool = False) -> str:
        """
        Return a job's log output.
        """

    @abstractmethod
    def get_job_stop_time(self, name: str, debug: bool = False) -> Union[datetime, None]:
        """
        Return the job's manual stop time.
        """

    @abstractmethod
    async def monitor_logs_async(
        self,
        name: str,
        callback_function: Callable[[Any], Any],
        input_callback_function: Callable[[], str],
        stop_callback_function: Callable[[SuccessTuple], str],
        stop_on_exit: bool = False,
        strip_timestamps: bool = False,
        accept_input: bool = True,
        debug: bool = False,
    ):
        """
        Monitor a job's log files and await a callback with the changes.
        """

    @abstractmethod
    def monitor_logs(self, *args, **kwargs):
        """
        Monitor a job's log files.
        """

    @abstractmethod
    def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job is blocking on stdin.
        """

    @abstractmethod
    def get_job_prompt_kwargs(self, name: str, debug: bool = False) -> Dict[str, Any]:
        """
        Return the kwargs to the blocking prompt.
        """
