#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to interact with /mrsm/actions (the Rust client, `meerschaum._core.ApiConnector`).
"""

from __future__ import annotations

import asyncio
from functools import partial

from meerschaum.utils.typing import SuccessTuple, List, Callable, Optional


def get_actions(self):
    """
    Get available actions from the API instance.
    Returns the response (v4 does: `.json()` is the list of action names).
    """
    from meerschaum._internal.static import STATIC_CONFIG
    return self.get(STATIC_CONFIG['api']['endpoints']['actions'])


def do_action(self, sysargs: List[str]) -> SuccessTuple:
    """
    Execute a Meerschaum action remotely.
    """
    return self._rust_client().do_action(sysargs)


async def do_action_async(
    self,
    sysargs: List[str],
    callback_function: Callable[[str], None] = partial(print, end=''),
) -> SuccessTuple:
    """
    Execute an action as a temporary remote job.
    """
    from meerschaum.connectors.api._jobs import _LoopCallback
    loop = asyncio.get_running_loop()
    client = self._rust_client()
    return await loop.run_in_executor(
        None,
        lambda: client.do_action(sysargs, _LoopCallback(loop, callback_function)),
    )


def do_action_legacy(
    self,
    action: Optional[List[str]] = None,
    sysargs: Optional[List[str]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    NOTE: This method is deprecated.
    Please use `do_action()` or `do_action_async()`.

    Execute a Meerschaum action remotely.

    If `sysargs` are provided, parse those instead.
    Otherwise infer everything from keyword arguments.

    Examples
    --------
    >>> conn = mrsm.get_connector('api:main')
    >>> conn.do_action(['show', 'pipes'])
    (True, "Success")
    >>> conn.do_action(['show', 'arguments'], name='test')
    (True, "Success")
    """
    if action is None:
        action = []

    if sysargs is not None and action and action[0] == '':
        from meerschaum._internal.arguments import parse_arguments
        json_dict = parse_arguments(sysargs)
        action = list(json_dict.pop('action', None) or [])
        kw = json_dict
    else:
        action = list(action)
        if debug:
            kw['debug'] = debug

    return self._rust_client().do_action_legacy(action, kw)
