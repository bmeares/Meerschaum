#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to interact with /mrsm/actions
"""

from __future__ import annotations

import json
import asyncio
from functools import partial

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, List, Callable
from meerschaum.config.static import STATIC_CONFIG

ACTIONS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['actions']


def get_actions(self):
    """Get available actions from the API instance."""
    return self.get(ACTIONS_ENDPOINT)


def do_action(self, sysargs: List[str]) -> SuccessTuple:
    """
    Execute a Meerschaum action remotely.
    """
    return asyncio.run(self.do_action_async(sysargs))


async def do_action_async(
    self,
    sysargs: List[str],
    callback_function: Callable[[str], None] = partial(print, end=''),
) -> SuccessTuple:
    """
    Monitor a job's log files and await a callback with the changes.
    """
    websockets, websockets_exceptions = mrsm.attempt_import('websockets', 'websockets.exceptions')
    protocol = 'ws' if self.URI.startswith('http://') else 'wss'
    port = self.port if 'port' in self.__dict__ else ''
    uri = f"{protocol}://{self.host}:{port}{ACTIONS_ENDPOINT}/ws"
    if sysargs and sysargs[0] == 'api' and len(sysargs) > 2:
        sysargs = sysargs[2:]

    sysargs_str = json.dumps(sysargs)

    async with websockets.connect(uri) as websocket:
        try:
            await websocket.send(self.token or 'no-login')
            response = await websocket.recv()
            init_data = json.loads(response)
            if not init_data.get('is_authenticated'):
                return False, "Cannot authenticate with actions endpoint."

            await websocket.send(sysargs_str)
        except websockets_exceptions.ConnectionClosedOK:
            return False, "Connection was closed."

        while True:
            try:
                line = await websocket.recv()
                if asyncio.iscoroutinefunction(callback_function):
                    await callback_function(line)
                else:
                    callback_function(line)
            except KeyboardInterrupt:
                await websocket.close()
                break
            except websockets_exceptions.ConnectionClosedOK:
                break

    return True, "Success"
