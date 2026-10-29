#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Python view of a native plugin's job executor (`<type>:<label>` keys a Rust plugin declares
under `executor_types`): `Job.executor` for such keys. Every method goes through the native jobs
API (`meerschaum._core`), which calls the plugin; there is no Python implementation.
"""

from __future__ import annotations

from meerschaum.jobs._Executor import Executor
from meerschaum.utils.typing import Any, Dict, List, SuccessTuple, Union


class NativeExecutor(Executor):
    """
    A native plugin's executor, reached through the native jobs API.
    """

    def __init__(self, executor_keys: str):
        typ, _, label = str(executor_keys).partition(':')
        self.__dict__['type'] = typ
        super().__init__(type=typ, label=(label or 'main'))
        self.executor_keys = f"{typ}:{label or 'main'}"

    def _job(self, name: str, sysargs=None, properties=None):
        from meerschaum.jobs import Job
        return Job(name, sysargs, executor_keys=self.executor_keys, _properties=properties)

    def get_job_names(self, debug: bool = False) -> List[str]:
        return list(self.get_jobs(debug=debug))

    def get_job_exists(self, name: str, debug: bool = False) -> bool:
        return self._job(name).exists()

    def get_jobs(self, debug: bool = False) -> Dict[str, Any]:
        from meerschaum.jobs import get_jobs
        return get_jobs(executor_keys=self.executor_keys, include_hidden=True, debug=debug)

    def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
        job = self._job(name)
        if not job.exists():
            return {}
        return {
            'sysargs': job.sysargs,
            'result': list(job.result),
            'restart': job.restart,
            'daemon': {
                'status': job.status,
                'pid': job.pid,
                'properties': job.get_daemon_properties(),
            },
        }

    def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
        return self._job(name).get_daemon_properties()

    def get_job_status(self, name: str, debug: bool = False) -> str:
        return self._job(name).status

    def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
        return self._job(name)._core.began

    def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
        return self._job(name)._core.ended

    def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
        return self._job(name)._core.paused

    def create_job(
        self,
        name: str,
        sysargs: List[str],
        properties: Union[Dict[str, Any], None] = None,
        debug: bool = False,
    ) -> SuccessTuple:
        return self._job(name, sysargs, properties).start()

    def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
        return self._job(name).start()

    def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
        return self._job(name).stop()

    def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
        return self._job(name).pause()

    def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
        return self._job(name).delete()

    def get_logs(self, name: str, debug: bool = False) -> Union[str, None]:
        return self._job(name).get_logs()

    def get_job_stop_time(self, name: str, debug: bool = False):
        return self._job(name).stop_time

    def monitor_logs(self, name: str, *args, **kwargs):
        return self._job(name).monitor_logs(*args, **kwargs)

    async def monitor_logs_async(self, name: str, *args, **kwargs):
        return await self._job(name).monitor_logs_async(*args, **kwargs)

    def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
        return self._job(name).is_blocking_on_stdin()

    def get_job_prompt_kwargs(self, name: str, debug: bool = False) -> Dict[str, Any]:
        return self._job(name).get_prompt_kwargs()
