#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage jobs via the Meerschaum API.
"""

import asyncio
import time
import json
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any, SuccessTuple, List, Union, Callable, Optional
from meerschaum.jobs import Job
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn, dprint

JOBS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['jobs']
LOGS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['logs']
JOBS_STDIN_MESSAGE: str = STATIC_CONFIG['api']['jobs']['stdin_message']
JOBS_STOP_MESSAGE: str = STATIC_CONFIG['api']['jobs']['stop_message']
JOB_METADATA_CACHE_SECONDS: int = STATIC_CONFIG['api']['jobs']['metadata_cache_seconds']


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
    now = time.perf_counter()
    _job_metadata_cache = self.__dict__.get('_job_metadata_cache', None)
    _job_metadata_timestamp = (
        _job_metadata_cache.get(name, {}).get('timestamp', None)
    ) if _job_metadata_cache is not None else None

    if (
        _job_metadata_timestamp is not None
        and (now - _job_metadata_timestamp) < JOB_METADATA_CACHE_SECONDS
    ):
        if debug:
            dprint(f"Returning cached metadata for job '{name}'.")
        return _job_metadata_cache[name]['metadata']

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

    metadata = response.json()
    if _job_metadata_cache is None:
        self._job_metadata_cache = {}

    self._job_metadata_cache[name] = {
        'timestamp': now,
        'metadata': metadata,
    }
    return metadata

def get_job_properties(self, name: str, debug: bool = False) -> Dict[str, Any]:
    """
    Return the daemon properties for a single job.
    """
    metadata = self.get_job_metadata(name, debug=debug)
    return metadata.get('daemon', {}).get('properties', {})

def get_job_status(self, name: str, debug: bool = False) -> str:
    """
    Return the job's status.
    """
    metadata = self.get_job_metadata(name, debug=debug)
    return metadata.get('status', 'stopped')

def get_job_began(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `began` timestamp, if it exists.
    """
    properties = self.get_job_properties(name, debug=debug)
    began_str = properties.get('daemon', {}).get('began', None)
    if began_str is None:
        return None

    return began_str

def get_job_ended(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `ended` timestamp, if it exists.
    """
    properties = self.get_job_properties(name, debug=debug)
    ended_str = properties.get('daemon', {}).get('ended', None)
    if ended_str is None:
        return None

    return ended_str

def get_job_paused(self, name: str, debug: bool = False) -> Union[str, None]:
    """
    Return a job's `paused` timestamp, if it exists.
    """
    properties = self.get_job_properties(name, debug=debug)
    paused_str = properties.get('daemon', {}).get('paused', None)
    if paused_str is None:
        return None

    return paused_str

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


def create_job(
    self,
    name: str,
    sysargs: List[str],
    properties: Optional[Dict[str, str]] = None,
    debug: bool = False,
) -> SuccessTuple:
    """
    Create a job.
    """
    response = self.post(
        JOBS_ENDPOINT + f"/{name}",
        json={
            'sysargs': sysargs,
            'properties': properties,
        },
        debug=debug,
    )
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
    stop_callback_function: Callable[[SuccessTuple], str],
    stop_on_exit: bool = False,
    strip_timestamps: bool = False,
    accept_input: bool = True,
    debug: bool = False,
):
    """
    Monitor a job's log files and await a callback with the changes.
    """
    import traceback
    from meerschaum.jobs import StopMonitoringLogs
    from meerschaum.utils.formatting._jobs import strip_timestamp_from_line

    websockets, websockets_exceptions = mrsm.attempt_import('websockets', 'websockets.exceptions')
    protocol = 'ws' if self.URI.startswith('http://') else 'wss'
    port = self.port if 'port' in self.__dict__ else ''
    uri = f"{protocol}://{self.host}:{port}{LOGS_ENDPOINT}/{name}/ws"

    async def _stdin_callback(client):
        if input_callback_function is None:
            return

        if asyncio.iscoroutinefunction(input_callback_function):
            data = await input_callback_function()
        else:
            data = input_callback_function()

        await client.send(data)

    async def _stop_callback(client):
        try:
            result = tuple(json.loads(await client.recv()))
        except Exception as e:
            warn(traceback.format_exc())
            result = False, str(e)

        if stop_callback_function is not None:
            if asyncio.iscoroutinefunction(stop_callback_function):
                await stop_callback_function(result)
            else:
                stop_callback_function(result)

        if stop_on_exit:
            raise StopMonitoringLogs

    message_callbacks = {
        JOBS_STDIN_MESSAGE: _stdin_callback,
        JOBS_STOP_MESSAGE: _stop_callback,
    }

    async with websockets.connect(uri) as websocket:
        try:
            await websocket.send(self.token or 'no-login')
        except websockets_exceptions.ConnectionClosedOK:
            pass

        while True:
            try:
                response = await websocket.recv()
                callback = message_callbacks.get(response, None)
                if callback is not None:
                    await callback(websocket)
                    continue

                if strip_timestamps:
                    response = strip_timestamp_from_line(response)

                if asyncio.iscoroutinefunction(callback_function):
                    await callback_function(response)
                else:
                    callback_function(response)
            except (KeyboardInterrupt, StopMonitoringLogs):
                await websocket.close()
                break


def monitor_logs(
    self,
    name: str,
    callback_function: Callable[[Any], Any],
    input_callback_function: Callable[[None], str],
    stop_callback_function: Callable[[None], str],
    stop_on_exit: bool = False,
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
            stop_callback_function=stop_callback_function,
            stop_on_exit=stop_on_exit,
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
