#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Start subsystems (API server, logging daemon, etc.).
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def start(
        action : Optional[List[str]] = None,
        **kw : Any,
    ) -> SuccessTuple:
    """
    Start subsystems (API server, logging daemon, etc.).
    """

    from meerschaum.utils.misc import choose_subaction
    options = {
        'api' : _start_api,
        'job' : _start_job,
    }
    return choose_subaction(action, options, **kw)

def _start_api(action : Optional[List[str]] = None, **kw):
    """
    Start the API server.

    Usage:
        `start api {options}`

    Options:
        - `-p, --port {number}`
            Port to bind the API server to.

        - `-w, --workers {number}`
            How many worker threads to run.
            Defaults to the number of CPU cores or 1 on Android.
    """
    from meerschaum.actions import actions
    return actions['api'](action=['start'], **kw)

def _start_job(
        action : Optional[List[str]] = None,
        name : Optional[str] = None,
        **kw
    ) -> SuccessTuple:
    """
    Run a Meerschaum action as a background job.

    To create a new job, pass the command arguments after `start job`.
    To start a stopped job, pass the job name after `start job`.

    You may also run a background job with the `-d` or `--daemon` flags.

    Examples:

        Create new jobs:

          - `start job sync pipes --loop`
                Run the action `sync pipes --loop` as a background job.
                Generates a random name; e.g. 'happy_seal'.
          
          - `start api --daemon --name api_server`
                Run the action `start api` as a background job, and assign the job
                the name 'api_server'.

        Start stopped jobs:

          - `start job happy_seal`
                Start the job 'happy_seal'.

          - `start job --name happy_seal`
                Start the job 'happy_seal' but via the `--name` flag.
                This only applies when no text follows the words 'start job'.
    """
    import textwrap
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.daemon import daemon_action, Daemon, get_daemon_ids
    from meerschaum.utils.daemon._names import get_new_daemon_name
    from meerschaum.actions.arguments._parse_arguments import parse_arguments
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no

    daemon_ids = get_daemon_ids()

    new_job = True

    if action:
        if len(action) == 1 and action[0] in daemon_ids and not name:
            new_job = False
            name = action[0]
        ### Cannot find dameon_id
        else:
            pass

    ### No action provided; start job if possible
    elif name is not None:
        new_job = False

    ### No action or name provided. Return error.
    else:
        print(textwrap.dedent(_start_job.__doc__))
        return False, "Nothing to do. Provide action arguments or a job name to start."

    def _run_new_job(name : Optional[str] = None):
        kw['action'] = action
        if not name:
            name = get_new_daemon_name()
        kw['name'] = name
        _action_success_tuple = daemon_action(daemon_id=name, **kw)
        return _action_success_tuple, name

    def _run_existing_job(name : Optional[str] = None):
        daemon = Daemon(daemon_id=name)

        if name not in daemon_ids:
            warn(f"There isn't a job with the name '{name}'.", stack=False)
            print(
                f"You can start a new job named '{name}' with `start job "
                + "{options}" + f" --name {name}`"
            )
            return (False, f"Job '{name}' does not exist."), daemon.daemon_id

        if not yes_no(
            f"Would you like to overwrite the logs and run the job '{daemon.daemon_id}'?",
            default = 'n',
            yes = kw.get('yes', False),
            force = kw.get('force', False),
            nopretty = kw.get('nopretty', False),
            noask = kw.get('noask', False),
        ):
            return (False, "Nothing was started."), daemon.daemon_id
        daemon.cleanup()
        _daemon_sysargs = daemon.properties['target']['args'][0]
        _daemon_kw = parse_arguments(_daemon_sysargs)
        _daemon_kw['name'] = daemon.daemon_id
        _action_success_tuple = daemon_action(
            **_daemon_kw
        )
        if not _action_success_tuple[0]:
            return _action_success_tuple, daemon.daemon_id
        return (True, f"Success"), daemon.daemon_id

    success_tuple, _name = _run_new_job(name) if new_job else _run_existing_job(name)
    if success_tuple[0]:
        return True, (
            f"Successfully started job '{_name}'." if not kw.get('nopretty', False) else _name
        )
    return success_tuple


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
