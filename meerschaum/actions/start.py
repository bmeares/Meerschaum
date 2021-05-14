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

def _complete_start(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """

    if action is None:
        action = []

    options = {
        'job' : _complete_start_job,
    }

    if len(action) > 0 and action[0] in options:
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum.actions.shell import default_action_completer
    return default_action_completer(action=(['start'] + action), **kw)



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
        nopretty : bool = False,
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
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.daemon import daemon_action, Daemon, get_daemon_ids, get_daemons
    from meerschaum.utils.daemon._names import get_new_daemon_name
    from meerschaum.actions.arguments._parse_arguments import parse_arguments
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting._jobs import pprint_job

    names = []
    daemon_ids = get_daemon_ids()
    daemons = get_daemons()

    new_job = True
    _potential_jobs = {'known' : [], 'unknown' : []}

    if action:
        for a in action:
            _potential_jobs[('known' if a in daemon_ids else 'unknown')].append(a)
        
        ### Check if the job is named after an action.
        if (
            _potential_jobs['known']
                and _potential_jobs['unknown']
                and _potential_jobs['known'][0] == action[0]
                and _potential_jobs['known'][0] in actions
        ):
            _potential_jobs['uknown'].insert(0, _potential_jobs['known'][0])
            del _potential_jobs['known'][0]

        ### Only spawn a new job if we don't don't find any jobs.
        new_job = (len(_potential_jobs['known']) == 0)
        if not new_job and _potential_jobs['unknown']:
            warn(
                (
                    "Unknown job" + ("s" if len(_potential_jobs['unknown']) > 1 else '') + " "
                    #  + ("'" if len(_potential_jobs['unknown']) == 1 else '')
                    + "'"
                    + "', '".join(_potential_jobs['unknown'][:])
                    + "'"
                    #  + ("'" if len(_potential_jobs['unknown']) == 1 else '')
                    + " will be ignored."
                ),
                stack = False
            )
        if not new_job and not name:
            name = _potential_jobs['known'][0]
        #  names = _potential_jobs['']

        #  if len(action) == 1 and action[0] in daemon_ids and not name:
            #  new_job = False
            #  name = action[0]
        ### Cannot find dameon_id
        else:
            pass

    ### No action provided; start job if possible
    elif name is not None:
        new_job = False

    ### No action or name provided. Return error.
    else:
        #  print(textwrap.dedent(_start_job.__doc__))
        if not kw.get('force', False):
            print_options(daemon_ids, nopretty=(kw.get('nopretty', False)), header='Jobs to start')
            if not yes_no("Would you like to start all jobs?", default='n'):
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

        if not kw.get('force', False):
            pprint_job(daemon, nopretty=nopretty)
            if not yes_no(
                f"Would you like to overwrite the logs and run the job '{daemon.daemon_id}'?",
                default = 'n',
                yes = kw.get('yes', False),
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

def _complete_start_job(action : Optional[List[str]] = None, **kw) -> List[str]:
    from meerschaum.utils.daemon import get_daemon_ids
    daemon_ids = get_daemon_ids()
    if not action:
        return daemon_ids
    possibilities = []
    for daemon_id in daemon_ids:
        if daemon_id.startswith(action[0]) and action[0] != daemon_id:
            possibilities.append(daemon_id)
    return possibilities


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
