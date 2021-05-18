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
        'jobs' : _start_jobs,
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
        'job' : _complete_start_jobs,
        'jobs' : _complete_start_jobs,
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

def _start_jobs(
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
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.daemon import (
        daemon_action, Daemon, get_daemon_ids, get_daemons, get_filtered_daemons,
        get_stopped_daemons, get_running_daemons
    )
    from meerschaum.utils.daemon._names import get_new_daemon_name
    from meerschaum.actions.arguments._parse_arguments import parse_arguments
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.formatting._jobs import pprint_job, pprint_jobs
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.misc import items_str

    names = []
    daemon_ids = get_daemon_ids()

    new_job = len(list(action)) > 0
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
            _potential_jobs['unknown'].insert(0, _potential_jobs['known'][0])
            del _potential_jobs['known'][0]

        ### Only spawn a new job if we don't don't find any jobs.
        new_job = (len(_potential_jobs['known']) == 0)
        if not new_job and _potential_jobs['unknown']:
            if not kw.get('nopretty', False):
                warn(
                    (
                        "Unknown job" + ("s" if len(_potential_jobs['unknown']) > 1 else '') + " "
                        + items_str(_potential_jobs['unknown'])
                        + " will be ignored."
                    ),
                    stack = False
                )

        ### Determine the `names` list.
        if new_job:
            names = [get_new_daemon_name()]
        elif not new_job and not name:
            names = _potential_jobs['known']
        ### Cannot find dameon_id
        else:
            msg = (
                f"Unknown job" + ('s' if len(action) > 1 else '') + ' '
                + items_str(action, and_str='or') + '.\n'
            )
            return False, msg

    ### No action provided but a --name was. Start job if possible.
    ### E.g. `start job --myjob`
    elif name is not None:
        new_job = False
        names = [name]

    ### No action or --name was provided. Ask to start all stopped jobs.
    else:
        _stopped_daemons = get_stopped_daemons()
        if not _stopped_daemons:
            return False, "No jobs to start."
        #  if not kw.get('force', False):
            #  pprint_jobs(_stopped_daemons, nopretty=kw.get('nopretty', False))
            #  if not yes_no(
                #  "Would you like to start all stopped jobs?",
                #  yes=kw.get('yes', False), noask=kw.get('noask', False), default='n'
            #  ):
                #  return False, "Nothing to do. Provide action arguments or a job name to start."
            #  if not kw.get('nopretty', False):
                #  clear_screen(debug=kw.get('debug', False))
        names = [d.daemon_id for d in get_stopped_daemons()]

    def _run_new_job(name : Optional[str] = None):
        kw['action'] = action
        if not name:
            name = get_new_daemon_name()
        kw['name'] = name
        _action_success_tuple = daemon_action(daemon_id=name, **kw)
        return _action_success_tuple, name

    def _run_existing_job(name : Optional[str] = None):
        daemon = Daemon(daemon_id=name)

        if not daemon.path.exists():
            if not kw.get('nopretty', False):
                warn(f"There isn't a job with the name '{name}'.", stack=False)
                print(
                    f"You can start a new job named '{name}' with `start job "
                    + "{options}" + f" --name {name}`"
                )
            return (False, f"Job '{name}' does not exist."), daemon.daemon_id

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

    if not names:
        return False, "No jobs to start."

    ### Get user permission to clear logs.
    if not kw.get('force', False):
        pprint_jobs([Daemon(daemon_id=n) for n in names], nopretty=kw.get('nopretty', False))
        if not yes_no(
            (
                f"Would you like to overwrite the logs and run the job"
                + ("s" if len(names) > 1 else '') + " " + items_str(names) + "?"
            ),
            default = 'n',
            yes = kw.get('yes', False),
            nopretty = kw.get('nopretty', False),
            noask = kw.get('noask', False),
        ):
            return (False, "Nothing was started.")


    _successes, _failures = [], []
    for name in names:
        success_tuple, _name = _run_new_job(name) if new_job else _run_existing_job(name)
        if success_tuple[0]:
            if kw.get('nopretty', False):
                print_tuple(True, f"Successfully started job '{_name}'.")
            _successes.append(name)
        else:
            _failures.append(name)

    msg = (
        (("Succesfully started job" + ("s" if len(_successes) > 1 else '')
            + f" {items_str(_successes)}." + ('\n' if _failures else ''))
            if _successes else '')
        + ("Failed to start job" + ("s" if len(_failures) > 1 else '')
            + f" {items_str(_failures)}." if _failures else '')
    )
    return len(_successes) > 0, msg

def _complete_start_jobs(action : Optional[List[str]] = None, **kw) -> List[str]:
    from meerschaum.utils.daemon import get_daemon_ids
    daemon_ids = get_daemon_ids()
    if not action:
        return daemon_ids
    possibilities = []
    if action[-1] in daemon_ids:
        return daemon_ids
    for daemon_id in daemon_ids:
        if daemon_id.startswith(action[-1]):
            possibilities.append(daemon_id)
    return possibilities


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
