#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute Meerschaum Actions via the API
"""

from __future__ import annotations
import asyncio
import traceback
import shlex
from functools import partial
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

from meerschaum.utils.misc import generate_password
from meerschaum.utils.jobs import Job
from meerschaum.utils.warnings import warn
from meerschaum.utils.typing import SuccessTuple, Union, List, Dict
from meerschaum.api import (
    fastapi, app, endpoints, get_api_connector, debug, manager, private, no_auth
)
from meerschaum.actions import actions
import meerschaum.core
from meerschaum.config import get_config
from meerschaum._internal.arguments._parse_arguments import parse_dict_to_sysargs, parse_arguments

actions_endpoint = endpoints['actions']

def is_user_allowed_to_execute(user) -> SuccessTuple:
    if user is None:
        return False, "Could not load user."

    if user.type == 'admin':
        return True, "Success"

    allow_non_admin = get_config(
        'system', 'api', 'permissions', 'actions', 'non_admin', patch=True
    )
    if not allow_non_admin:
        return False, (
            "The administrator for this server has not allowed users to perform actions.\n\n"
            + "Please contact the system administrator, or if you are running this server, "
            + "open the configuration file with `edit config system` "
            + "and search for 'permissions'. "
            + "\nUnder the keys 'api:permissions:actions', "
            + "you can allow non-admin users to perform actions."
        )

    return True, "Success"


@app.get(actions_endpoint, tags=['Actions'])
def get_actions(
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> List[str]:
    """
    Return a list of the available actions.
    """
    return list(actions)


async def notify_client(client, content: str):
    """
    Send a line of text to a client.
    """
    try:
        await client.send_text(content)
    except WebSocketDisconnect:
        pass


@app.websocket(actions_endpoint + '/ws')
async def do_action_websocket(websocket: WebSocket):
    """
    Execute an action and stream the output to the client.
    """
    await websocket.accept()

    stop_event = asyncio.Event()

    async def monitor_logs(job):
        success, msg = job.start()
        await job.monitor_logs_async(
            partial(notify_client, websocket),
            stop_event=stop_event,
            stop_on_exit=True,
        )

    try:
        token = await websocket.receive_text()
        user = await manager.get_current_user(token) if not no_auth else None
        if user is None and not no_auth:
            raise fastapi.HTTPException(
                status_code=401,
                detail="Invalid credentials.",
            )

        auth_success, auth_msg = is_user_allowed_to_execute(user)
        auth_payload = {
            'is_authenticated': auth_success,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        await websocket.send_json(auth_payload)
        if not auth_success:
            await websocket.close()

        sysargs = await websocket.receive_json()
        kwargs = parse_arguments(sysargs)
        _ = kwargs.pop('executor_keys', None)
        _ = kwargs.pop('shell', None)
        sysargs = parse_dict_to_sysargs(kwargs)

        job_name = '.-' + shlex.join(sysargs) + '-' + generate_password(12)
        job = Job(
            job_name,
            sysargs,
            _properties={
                'logs': {
                    'write_timestamps': False,
                },
            },
        )
        monitor_task = asyncio.create_task(monitor_logs(job))
        await monitor_task
        await websocket.close()
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
        job.stop()
        stop_event.set()
