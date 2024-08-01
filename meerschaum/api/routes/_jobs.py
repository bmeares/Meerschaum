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
from meerschaum.utils.jobs import get_jobs as _get_jobs, Job
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
def delete_job(name: str) -> SuccessTuple:
    """
    Delete a job.
    """
    job = Job(name)
    return job.delete()


@app.get(endpoints['jobs'] + '/{name}/exists', tags=['Jobs'])
def get_job_exists(name: str) -> bool:
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
    """
    job = Job(name)
    if not job.exists():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"{job} does not exist.",
        )

    return job.get_logs()


@app.post(endpoints['jobs'] + '/{name}/start', tags=['Jobs'])
def start_job(name: str) -> SuccessTuple:
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
def stop_job(name: str) -> SuccessTuple:
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
def pause_job(name: str) -> SuccessTuple:
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
def get_stop_time(name: str) -> Union[datetime, None]:
    """
    Get the timestamp when the job was manually stopped.
    """
    job = Job(name)
    return job.stop_time


_job_clients = defaultdict(lambda: [])
async def notify_clients(name: str, content: str):
    """
    Write the given content to all connected clients.
    """
    for client in [c for c in _job_clients[name]]:
        try:
            await client.send_text(content)
        except WebSocketDisconnect:
            _job_clients[name].remove(client)
        except Exception:
            pass


@app.websocket(endpoints['logs'] + '/{name}/ws')
async def logs_websocket(name: str, websocket: WebSocket):
    """
    Stream logs from a job over a websocket.
    """
    print(f"{name=}")
    await websocket.accept()
    job = Job(name)
    _job_clients[name].append(websocket)

    stop_event = asyncio.Event()

    async def monitor_logs():
        await job.monitor_logs_async(partial(notify_clients, name), stop_event=stop_event)
        print(f"{stop_event.is_set()=}")
        print('hi')

    monitor_task = asyncio.create_task(monitor_logs())

    try:
        token = await websocket.receive_text()
        await monitor_task
        #  await job.monitor_logs_async(partial(notify_clients, name))
    #  except KeyboardInterrupt:
        #  print('keyboard')
        #  monitor_task.cancel()
        #  await websocket.close()
        #  _job_clients[name].remove(websocket)
    except WebSocketDisconnect:
        print('disconnected')
        monitor_task.cancel()
        stop_event.set()
        pass
    except asyncio.CancelledError:
        print('cancelled')
    except Exception:
        warn("Error in logs websocket:\n{traceback.format_exc()}")
    finally:
        #  print('cancel')
        monitor_task.cancel()

        #  print('setting stop event')
        stop_event.set()

        if websocket in _job_clients[name]:
            _job_clients[name].remove(websocket)

        try:
            print('trying to await')
            await monitor_task
        except asyncio.CancelledError:
            print('is cancelled')
            pass
        except Exception:
            warn(f"Exception when cancelling:\n{traceback.format_exc()}")

        print('done')
