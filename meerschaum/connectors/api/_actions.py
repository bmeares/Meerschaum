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
from meerschaum.utils.typing import SuccessTuple, List, Callable, Optional
from meerschaum.config.static import STATIC_CONFIG

ACTIONS_ENDPOINT: str = STATIC_CONFIG['api']['endpoints']['actions']
TEMP_PREFIX: str = STATIC_CONFIG['api']['jobs']['temp_prefix']


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
    Execute an action as a temporary remote job.
    """
    from meerschaum._internal.arguments import remove_api_executor_keys
    from meerschaum.utils.misc import generate_password
    sysargs = remove_api_executor_keys(sysargs)

    job_name = TEMP_PREFIX + generate_password(12)
    job = mrsm.Job(job_name, sysargs, executor_keys=str(self))

    start_success, start_msg = job.start()
    if not start_success:
        return start_success, start_msg

    await job.monitor_logs_async(
        callback_function=callback_function,
        stop_on_exit=True,
        strip_timestamps=True,
    )

    success, msg = job.result
    job.delete()
    return success, msg


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
    import sys, json
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.misc import json_serialize_datetime
    if action is None:
        action = []

    if sysargs is not None and action and action[0] == '':
        from meerschaum._internal.arguments import parse_arguments
        if debug:
            dprint(f"Parsing sysargs:\n{sysargs}")
        json_dict = parse_arguments(sysargs)
    else:
        json_dict = kw
        json_dict['action'] = action
        if 'noask' not in kw:
            json_dict['noask'] = True
        if 'yes' not in kw:
            json_dict['yes'] = True
        if debug:
            json_dict['debug'] = debug

    root_action = json_dict['action'][0]
    del json_dict['action'][0]
    r_url = f"{STATIC_CONFIG['api']['endpoints']['actions']}/{root_action}"
    
    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending data to '{self.url + r_url}':")
        pprint(json_dict, stream=sys.stderr)

    response = self.post(
        r_url,
        data = json.dumps(json_dict, default=json_serialize_datetime),
        debug = debug,
    )
    try:
        response_list = json.loads(response.text)
        if isinstance(response_list, dict) and 'detail' in response_list:
            return False, response_list['detail']
    except Exception as e:
        print(f"Invalid response: {response}")
        print(e)
        return False, response.text
    if debug:
        dprint(response)
    try:
        return response_list[0], response_list[1]
    except Exception as e:
        return False, f"Failed to parse result from action '{root_action}'"
