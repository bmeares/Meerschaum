#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the base class for a Job executor.
"""

from __future__ import annotations

from abc import abstractmethod

from meerschaum.connectors import Connector
from meerschaum.utils.typing import List, Dict, SuccessTuple, TYPE_CHECKING, Optional, Any

if TYPE_CHECKING:
    from meerschaum.jobs import Job

class Executor(Connector):
    """
    Define the methods for managing jobs.
    """

    @abstractmethod
    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """
    
    @abstractmethod
    def get_jobs(self) -> Dict[str, Job]:
        """
        Return a dictionary of names -> Jobs.
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
