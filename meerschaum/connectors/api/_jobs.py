#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage jobs via the Meerschaum API.
"""

import asyncio
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any, SuccessTuple, List, Union, Callable
from meerschaum.utils.jobs import Job
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn

JOBS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['jobs']
LOGS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['logs']
JOBS_STDIN_MESSAGE: str = STATIC_CONFIG['api']['jobs']['stdin_message']


def get_jobs(self, debug: bool = False) -> Dict[str, Job]:
    """
    Return a dictionary of remote jobs.
    """
    response = self.get(JOBS_ENDPOINT, debug=debug)
    if not response:
        warn(f"Failed to get remote jobs from {self}.")
        return {}
    return {
        name: Job(
            name,
            job_meta['sysargs'],
            executor_keys=str(self),
            _properties=job_meta['daemon']['properties']
        )
        for name, job_meta in response.json().items()
    }


def get_job(self, name: str, debug: bool = False) -> Job:
    """
    Return a single Job object.
    """
    metadata = self.get_job_metadata(name, debug=debug)
    if not metadata:
        raise ValueError(f"Job '{name}' does not exist.")

    return Job(
        name,
        metadata['sysargs'],
        executor_keys=str(self),
        _properties=metadata['daemon']['properties'],
    )


def get_job_metadata(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the metadata for a single job.
    """
    response = self.get(JOBS_ENDPOINT + f"/{name}", debug=debug)
    if not response:
        if debug:
            msg = (
                response.json()['detail']
                if 'detail' in response.text
                else response.text
            )
            warn(f"Failed to get metadata for job '{name}':\n{msg}")
        return {}

    return response.json()


def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the daemon properties for a single job.
    """
    metadata = self.get_job_metadata(name, debug=debug)
    return metadata.get('daemon', {}).get('properties', {})


def get_job_exists(self, name: str, debug: bool = False) -> bool:
    """
    Return whether a job exists.
    """
    response = self.get(JOBS_ENDPOINT + f'/{name}/exists', debug=debug)
    if not response:
        warn(f"Failed to determine whether job '{name}' exists.")
        return False

    return response.json()


def delete_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Delete a job.
    """
    response = self.delete(JOBS_ENDPOINT + f"/{name}", debug=debug)
    if not response:
        if 'detail' in response.text:
            return False, response.json()['detail']

        return False, response.text

    return tuple(response.json())


def start_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Start a job.
    """
    response = self.post(JOBS_ENDPOINT + f"/{name}/start", debug=debug)
    if not response:
        if 'detail' in response.text:
            return False, response.json()['detail']
        return False, response.text

    return tuple(response.json())


def create_job(self, name: str, sysargs: List[str], debug: bool = False) -> SuccessTuple:
    """
    Create a job.
    """
    response = self.post(JOBS_ENDPOINT + f"/{name}", json=sysargs, debug=debug)
    if not response:
        if 'detail' in response.text:
            return False, response.json()['detail']
        return False, response.text

    return tuple(response.json())


def stop_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Stop a job.
    """
    response = self.post(JOBS_ENDPOINT + f"/{name}/stop", debug=debug)
    if not response:
        if 'detail' in response.text:
            return False, response.json()['detail']
        return False, response.text

    return tuple(response.json())


def pause_job(self, name: str, debug: bool = False) -> SuccessTuple:
    """
    Pause a job.
    """
    response = self.post(JOBS_ENDPOINT + f"/{name}/pause", debug=debug)
    if not response:
        if 'detail' in response.text:
            return False, response.json()['detail']
        return False, response.text

    return tuple(response.json())


def get_logs(self, name: str, debug: bool = False) -> str:
    """
    Return the logs for a job.
    """
    response = self.get(LOGS_ENDPOINT + f"/{name}")
    if not response:
        raise ValueError(f"Cannot fetch logs for job '{name}':\n{response.text}")

    return response.json()


def get_job_stop_time(self, name: str, debug: bool = False) -> Union[datetime, None]:
    """
    Return the job's manual stop time.
    """
    response = self.get(JOBS_ENDPOINT + f"/{name}/stop_time")
    if not response:
        warn(f"Failed to get stop time for job '{name}':\n{response.text}")
        return None

    data = response.json()
    if data is None:
        return None

    return datetime.fromisoformat(data)


async def monitor_logs_async(
    self,
    name: str,
    callback_function: Callable[[Any], Any],
    input_callback_function: Callable[[], str],
    strip_timestamps: bool = False,
    accept_input: bool = True,
    debug: bool = False,
):
    """
    Monitor a job's log files and await a callback with the changes.
    """
    from meerschaum.utils.formatting._jobs import strip_timestamp_from_line

    websockets, websockets_exceptions = mrsm.attempt_import('websockets', 'websockets.exceptions')
    protocol = 'ws' if self.URI.startswith('http://') else 'wss'
    port = self.port if 'port' in self.__dict__ else ''
    uri = f"{protocol}://{self.host}:{port}{LOGS_ENDPOINT}/{name}/ws"

    async with websockets.connect(uri) as websocket:
        try:
            await websocket.send(self.token or 'no-login')
        except websockets_exceptions.ConnectionClosedOK:
            pass

        while True:
            try:
                response = await websocket.recv()
                if response == JOBS_STDIN_MESSAGE:
                    if asyncio.iscoroutinefunction(input_callback_function):
                        data = await input_callback_function()
                    else:
                        data = input_callback_function()

                    await websocket.send(data)
                    continue

                if strip_timestamps:
                    response = strip_timestamp_from_line(response)

                if asyncio.iscoroutinefunction(callback_function):
                    await callback_function(response)
                else:
                    callback_function(response)
            except KeyboardInterrupt:
                await websocket.close()
                break

def monitor_logs(
    self,
    name: str,
    callback_function: Callable[[Any], Any],
    input_callback_function: Callable[[None], str],
    strip_timestamps: bool = False,
    accept_input: bool = True,
    debug: bool = False,
):
    """
    Monitor a job's log files and execute a callback with the changes.
    """
    return asyncio.run(
        self.monitor_logs_async(
            name,
            callback_function,
            input_callback_function=input_callback_function,
            strip_timestamps=strip_timestamps,
            accept_input=accept_input,
            debug=debug
        )
    )

def get_job_is_blocking_on_stdin(self, name: str, debug: bool = False) -> bool:
    """
    Return whether a remote job is blocking on stdin.
    """
    response = self.get(JOBS_ENDPOINT + f'/{name}/is_blocking_on_stdin', debug=debug)
    if not response:
        return False

    return response.json()
