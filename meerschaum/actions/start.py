#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Start subsystems (API server, logging daemon, etc.).
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def start(
        action: Optional[List[str]] = None,
        **kw: Any,
    ) -> SuccessTuple:
    """
    Start subsystems (API server, background job, etc.).
    """

    from meerschaum.utils.misc import choose_subaction
    options = {
        'api': _start_api,
        'jobs': _start_jobs,
        'gui': _start_gui,
        'webterm': _start_webterm,
    }
    return choose_subaction(action, options, **kw)

def _complete_start(
        action: Optional[List[str]] = None,
        **kw: Any
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

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['start'] + action), **kw)


def _start_api(action: Optional[List[str]] = None, **kw):
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
        action: Optional[List[str]] = None,
        name: Optional[str] = None,
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
            names = [(get_new_daemon_name() if not name else name)]
        elif not new_job and not name:
            names = _potential_jobs['known']
        ### Cannot find dameon_id
        else:
            msg = (
                f"Unknown job" + ('s' if len(action) != 1 else '') + ' '
                + items_str(action, and_str='or') + '.'
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
        names = [d.daemon_id for d in _stopped_daemons]

    def _run_new_job(name: Optional[str] = None):
        kw['action'] = action
        if not name:
            name = get_new_daemon_name()
        kw['name'] = name
        _action_success_tuple = daemon_action(daemon_id=name, **kw)
        return _action_success_tuple, name

    def _run_existing_job(name: Optional[str] = None):
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
        try:
            _daemon_sysargs = daemon.properties['target']['args'][0]
        except KeyError:
            return False, "Failed to get arguments for daemon '{dameon.daemon_id}'."
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
    _filtered_daemons = get_filtered_daemons(names)
    if not kw.get('force', False) and _filtered_daemons:
        _filtered_running_daemons = get_running_daemons(_filtered_daemons)
        if _filtered_running_daemons:
            pprint_jobs(_filtered_running_daemons)
            if yes_no(
                "The above jobs are still running. Do you want to first stop these jobs?",
                default = 'n',
                yes = kw.get('yes', False),
                noask = kw.get('noask', False)
            ):
                stop_success_tuple = actions['stop'](
                    action = ['jobs'] + [d.daemon_id for d in _filtered_running_daemons],
                    force = True,
                )
                if not stop_success_tuple[0]:
                    warn(
                        "Failed to stop job" + ("s" if len(_filtered_running_daemons) != 1 else '')
                        + items_str([d.daemon_id for d in _filtered_running_daemons])
                        + ".",
                        stack = False
                    )
                    for d in _filtered_running_daemons:
                        names.remove(d.daemon_id)
                        _filtered_daemons.remove(d)
            else:
                info(
                    "Skipping already running job"
                    + ("s" if len(_filtered_running_daemons) != 1 else '') + ' '
                    + items_str([d.daemon_id for d in _filtered_running_daemons]) + '.'
                )
                for d in _filtered_running_daemons:
                    names.remove(d.daemon_id)
                    _filtered_daemons.remove(d)

        if not _filtered_daemons:
            return False, "No jobs to start."
        pprint_jobs(_filtered_daemons, nopretty=kw.get('nopretty', False))
        if not yes_no(
            (
                f"Would you like to overwrite the logs and run the job"
                + ("s" if len(names) != 1 else '') + " " + items_str(names) + "?"
            ),
            default = 'n',
            yes = kw.get('yes', False),
            nopretty = kw.get('nopretty', False),
            noask = kw.get('noask', False),
        ):
            return (False, "Nothing was started.")


    _successes, _failures = [], []
    for _name in names:
        success_tuple, __name = _run_new_job(_name) if new_job else _run_existing_job(_name)
        if not kw.get('nopretty', False):
            print_tuple(success_tuple)
        _successes.append(_name) if success_tuple[0] else _failures.append(_name)

    msg = (
        (("Successfully started job" + ("s" if len(_successes) != 1 else '')
            + f" {items_str(_successes)}." + ('\n' if _failures else ''))
            if _successes else '')
        + ("Failed to start job" + ("s" if len(_failures) != 1 else '')
            + f" {items_str(_failures)}." if _failures else '')
    )
    return len(_successes) > 0, msg

def _complete_start_jobs(
        action: Optional[List[str]] = None,
        line: str = '',
        **kw
    ) -> List[str]:
    from meerschaum.utils.daemon import get_daemon_ids
    daemon_ids = get_daemon_ids()
    if not action:
        return daemon_ids
    possibilities = []
    _line_end = line.split(' ')[-1]
    for daemon_id in daemon_ids:
        if daemon_id in action:
            continue
        if _line_end == '':
            possibilities.append(daemon_id)
            continue
        if daemon_id.startswith(action[-1]):
            possibilities.append(daemon_id)
    return possibilities


def _start_gui(
        action: Optional[List[str]] = None,
        mrsm_instance: Optional[str] = None,
        port: Optional[int] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Start the Meerschaum GUI application.
    """
    from meerschaum.utils.daemon import Daemon
    from meerschaum.utils.process import run_process
    from meerschaum.utils.packages import venv_exec, run_python_package, attempt_import
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.networking import find_open_ports, is_port_in_use
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum._internal.term.tools import is_webterm_running
    webview = attempt_import('webview')
    requests = attempt_import('requests')
    import json
    import time

    success, msg = True, "Success"
    host = '127.0.0.1'
    if port is None:
        port = 8765

    if not is_webterm_running(host, port):
        port = next(find_open_ports(port, 9000))


    api_kw = {
        'action': ['webterm'],
        'no_auth': True,
        'port': port,
        'mrsm_instance': str(parse_instance_keys(mrsm_instance)),
        'debug': debug,
        'host': host,
    }
    api_kw_str = json.dumps(json.dumps(api_kw))
    start_tornado_code = (
        "from meerschaum.actions import actions; "
        "import json; "
        f"actions['start'](**json.loads({api_kw_str}))"
    )
    if debug:
        print(start_tornado_code)
    base_url = 'http://' + api_kw['host'] + ':' + str(api_kw['port'])

    process = venv_exec(
        start_tornado_code, as_proc=True, venv=None, debug=debug, capture_output=(not debug)
    )
    timeout = 10
    start = time.perf_counter()
    starting_up = True
    while starting_up:
        starting_up = (time.perf_counter() - start) < timeout
        time.sleep(0.1)
        try:
            request = requests.get(base_url)
            if request:
                break
        except Exception as e:
            if debug:
                dprint(e)
            continue
    if starting_up is False:
        return False, f"The webterm failed to start within {timeout} seconds."

    try:
        webview.create_window('Meerschaum Shell', f'http://127.0.0.1:{port}', height=650, width=1000)
        webview.start(debug=debug)
    except Exception as e:
        import traceback
        traceback.print_exc()
        success, msg = False, str(e)
    finally:
        process.kill()
    return success, msg


def _start_webterm(
        port: Optional[int] = None,
        host: Optional[str] = None,
        force: bool = False,
        nopretty: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Start the Meerschaum Web Terminal.
    
    Options:
        - `-p`, `--port`
            The port to which the webterm binds.
            Defaults to 8765, and `--force` will choose the next available port.
    
        - `--host`
            The host interface to which the webterm binds.
            Defaults to '127.0.0.1'.
    """
    from meerschaum._internal.term import tornado_app, tornado, term_manager, tornado_ioloop
    from meerschaum._internal.term.tools import is_webterm_running
    from meerschaum.utils.networking import find_open_ports, is_port_in_use
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import info

    if host is None:
        host = '127.0.0.1'
    if port is None:
        port = 8765

    if is_webterm_running(host, port):
        if force:
            port = next(find_open_ports(port + 1, 9000))
        else:
            return False, (
                f"The webterm is already running at http://{host}:{port}\n\n"
                + "    Include `-f` to start another server on a new port\n"
                + "    or specify a different port with `-p`."
            )

    if not nopretty:
        info(f"Starting the webterm at http://{host}:{port} ...\n    Press CTRL+C to quit.")
    tornado_app.listen(port, host)
    loop = tornado_ioloop.IOLoop.instance()
    try:
        loop.start()
    except KeyboardInterrupt:
        if not nopretty:
            print()
            info("Shutting down webterm...")
        term_manager.shutdown()
    finally:
        loop.close()

    return True, "Success"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
