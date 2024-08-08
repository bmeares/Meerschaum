#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Run jobs locally.
"""

from meerschaum.utils.typing import Dict, Any, List, SuccessTuple
from meerschaum.jobs import Job, Executor, make_executor
from meerschaum.utils.daemon import Daemon, get_daemons


#  @make_executor
class LocalExecutor(Executor):
    """
    Run jobs locally as Unix daemons.
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
    def create_job(self, name: str, sysargs: List[str], debug: bool = False) -> SuccessTuple:
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
