#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Run jobs locally.
"""

from meerschaum.utils.typing import Dict, Any, List, SuccessTuple, Union
from meerschaum.jobs import Job, Executor, make_executor
from meerschaum.utils.daemon import Daemon, get_daemons
from meerschaum._internal.entry import entry


@make_executor
class LocalExecutor(Executor):
    """
    Run jobs locally as Unix daemons.
    """

    def get_job_daemon(
        self,
        name: str,
        #  sysargs: Opt
        debug: bool = False,
    ) -> Union[Daemon, None]:
        """
        Return a job's daemon if it exists.
        """
        try:
            daemon = Daemon(name)
        except Exception:
            daemon = None

        return daemon
    
    def get_daemon_syargs(self, name: str, debug: bool = False) -> Union[List[str], None]:
        """
        Return the list of sysargs from the job's daemon.
        """
        daemon = self.get_job_daemon(name, debug=debug)

        if daemon is None:
            return None

        return daemon.properties.get('target', {}).get('args', [None])[0]

    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        """
        Return whether a job exists.
        """
        daemon = self.get_job_daemon(name, debug=debug)
        if daemon is None:
            return False
    
    def get_jobs(self) -> Dict[str, Job]:
        """
        Return a dictionary of names -> Jobs.
        """

    def create_job(self, name: str, sysargs: List[str], debug: bool = False) -> SuccessTuple:
        """
        Create a new job.
        """

    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Start a job.
        """

    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Stop a job.
        """

    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Pause a job.
        """

    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        """
        Delete a job.
        """

    def get_logs(self, name: str, debug: bool = False) -> str:
        """
        Return a job's log output.
        """
