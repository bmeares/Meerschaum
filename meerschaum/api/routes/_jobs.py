#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage jobs via the Meerschaum API.
"""

from __future__ import annotations

import os
import select
import asyncio
import traceback
from datetime import datetime
from collections import defaultdict
from functools import partial

from fastapi import WebSocket, WebSocketDisconnect

from meerschaum.utils.typing import Dict, Any, SuccessTuple, List, Optional, Union
from meerschaum.utils.jobs import get_jobs as _get_jobs, Job, StopMonitoringLogs
from meerschaum.utils.warnings import warn

from meerschaum.api import (
    fastapi,
    app,
    endpoints,
    manager,
    debug,
    no_auth,
    private,
)
from meerschaum.config.static import STATIC_CONFIG

JOBS_STDIN_MESSAGE: str = STATIC_CONFIG['api']['jobs']['stdin_message']


@app.get(endpoints['jobs'], tags=['Jobs'])
def get_jobs(
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, Dict[str, Any]]:
    """
    Return metadata about the current jobs.
    """
    jobs = _get_jobs()
    return {
        name: {
            'sysargs': job.sysargs,
            'result': job.result,
            'daemon': {
                'status': job.daemon.status,
                'pid': job.daemon.pid,
                'properties': job.daemon.properties,
            },
        }
        for name, job in jobs.items()
    }


@app.get(endpoints['jobs'] + '/{name}', tags=['Jobs'])
def get_job(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, Any]:
    """
    Return metadata for a single job.
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} doesn't exist."
        )

    return {
        'sysargs': job.sysargs,
        'result': job.result,
        'daemon': {
            'status': job.daemon.status,
            'pid': job.daemon.pid,
            'properties': job.daemon.properties,
        },
    }


@app.post(endpoints['jobs'] + '/{name}', tags=['Jobs'])
def create_job(
    name: str,
    sysargs: List[str],
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Create and start a new job.
    """
    job = Job(name, sysargs)
    if job.exists():
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"{job} already exists."
        )

    return job.start()


@app.delete(endpoints['jobs'] + '/{name}', tags=['Jobs'])
def delete_job(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Delete a job.
    """
    job = Job(name)
    return job.delete()


@app.get(endpoints['jobs'] + '/{name}/exists', tags=['Jobs'])
def get_job_exists(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> bool:
    """
    Return whether a job exists.
    """
    job = Job(name)
    return job.exists()


@app.get(endpoints['logs'] + '/{name}', tags=['Jobs'])
def get_logs(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Union[str, None]:
    """
    Return a job's log text.
    To stream log text, connect to the WebSocket endpoint `/logs/{name}/ws`.
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} does not exist.",
        )

    return job.get_logs()


@app.post(endpoints['jobs'] + '/{name}/start', tags=['Jobs'])
def start_job(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Start a job if stopped.
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} does not exist."
        )
    return job.start()


@app.post(endpoints['jobs'] + '/{name}/stop', tags=['Jobs'])
def stop_job(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Stop a job if running.
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} does not exist."
        )
    return job.stop()


@app.post(endpoints['jobs'] + '/{name}/pause', tags=['Jobs'])
def pause_job(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Pause a job if running.
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} does not exist."
        )
    return job.pause()


@app.get(endpoints['jobs'] + '/{name}/stop_time', tags=['Jobs'])
def get_stop_time(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Union[datetime, None]:
    """
    Get the timestamp when the job was manually stopped.
    """
    job = Job(name)
    return job.stop_time


@app.get(endpoints['jobs'] + '/{name}/is_blocking_on_stdin', tags=['Jobs'])
def get_is_blocking_on_stdin(
    name: str,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> bool:
    """
    Return whether a job is blocking on stdin.
    """
    job = Job(name)
    return job.is_blocking_on_stdin()


_job_clients = defaultdict(lambda: [])
_job_stop_events = defaultdict(lambda: asyncio.Event())
async def notify_clients(name: str, content: str):
    """
    Write the given content to all connected clients.
    """
    if not _job_clients[name]:
        _job_stop_events[name].set()

    async def _notify_client(client):
        try:
            await client.send_text(content)
        except WebSocketDisconnect:
            if client in _job_clients[name]:
                _job_clients[name].remove(client)
        except Exception:
            pass

    notify_tasks = [
        asyncio.create_task(_notify_client(client))
        for client in _job_clients[name]
    ]
    await asyncio.wait(notify_tasks)


async def get_input_from_clients(name):
    """
    When a job is blocking on input, return input from the first client which provides it.
    """
    print('GET INPUT FROM CLIENTS')
    if not _job_clients[name]:
        print('NO CLIENTS')
        return ''

    async def _read_client(client):
        try:
            await client.send_text(JOBS_STDIN_MESSAGE)
            data = await client.receive_text()
        except WebSocketDisconnect:
            if client in _job_clients[name]:
                _job_clients[name].remove(client)
        except Exception:
            pass
        return data

    read_tasks = [
        asyncio.create_task(_read_client(client))
        for client in _job_clients[name]
    ]
    done, pending = await asyncio.wait(read_tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in done:
        return task.result()


@app.websocket(endpoints['logs'] + '/{name}/ws')
async def logs_websocket(name: str, websocket: WebSocket):
    """
    Stream logs from a job over a websocket.
    """
    await websocket.accept()
    job = Job(name)
    _job_clients[name].append(websocket)

    async def monitor_logs():
        await job.monitor_logs_async(
            partial(notify_clients, name),
            input_callback_function=partial(get_input_from_clients, name),
            stop_event=_job_stop_events[name],
        )

    try:
        token = await websocket.receive_text()
        user = await manager.get_current_user(token) if not no_auth else None
        if user is None and not no_auth:
            raise fastapi.HTTPException(
                status_code=401,
                detail="Invalid credentials.",
            )
        monitor_task = asyncio.create_task(monitor_logs())
        await monitor_task
    except fastapi.HTTPException:
        await websocket.send_text("Invalid credentials.")
        await websocket.close()
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    except Exception:
        warn(f"Error in logs websocket:\n{traceback.format_exc()}")
    finally:
        if websocket in _job_clients[name]:
            _job_clients[name].remove(websocket)
        _job_stop_events[name].clear()
