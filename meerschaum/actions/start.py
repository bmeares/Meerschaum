#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Start subsystems (API server, logging daemon, etc.).
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any, Union, Dict

def start(
    action: Optional[List[str]] = None,
    **kw: Any,
) -> SuccessTuple:
    """
    Start subsystems (API server, background job, etc.).
    """
    from meerschaum.actions import choose_subaction
    options = {
        'api': _start_api,
        'jobs': _start_jobs,
        'gui': _start_gui,
        'webterm': _start_webterm,
        'connectors': _start_connectors,
        'pipeline': _start_pipeline,
    }
    return choose_subaction(action, options, **kw)


def _complete_start(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.delete import _complete_delete_jobs
    from functools import partial

    if action is None:
        action = []

    _complete_start_jobs = partial(
        _complete_delete_jobs,
        _get_job_method=['stopped', 'paused'],
    )

    options = {
        'job': _complete_start_jobs,
        'jobs': _complete_start_jobs,
        'connector': _complete_start_connectors,
        'connectors': _complete_start_connectors,
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
    sysargs: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    rm: bool = False,
    debug: bool = False,
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
    
          - `start api --daemon --name api-server`
                Run the action `start api` as a background job, and assign the job
                the name 'api-server'.
    
        Start stopped jobs:
    
          - `start job happy_seal`
                Start the job 'happy_seal'.
    
          - `start job --name happy_seal`
                Start the job 'happy_seal' but via the `--name` flag.
    """
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.daemon._names import get_new_daemon_name
    from meerschaum.jobs import (
        Job,
        get_filtered_jobs,
        get_stopped_jobs,
        get_running_jobs,
        get_paused_jobs,
        _install_healthcheck_job,
    )
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.formatting._jobs import pprint_jobs
    from meerschaum.utils.misc import items_str

    names = []
    jobs = get_filtered_jobs(executor_keys, action, debug=debug)

    new_job = len(list(action)) > 0
    _potential_jobs = {'known': [], 'unknown': []}

    if action:
        for a in action:
            _potential_jobs[(
                'known'
                if a in jobs
                else 'unknown'
            )].append(a)

        ### Check if the job is named after an action.
        if (
            _potential_jobs['known']
                and _potential_jobs['unknown']
                and _potential_jobs['known'][0] == action[0]
                and _potential_jobs['known'][0] in actions
        ):
            _potential_jobs['unknown'].insert(0, _potential_jobs['known'][0])
            del _potential_jobs['known'][0]

        ### Only spawn a new job if we don't find any jobs.
        new_job = (len(_potential_jobs['known']) == 0)
        if not new_job and _potential_jobs['unknown']:
            if not kw.get('nopretty', False):
                warn(
                    (
                        "Unknown job" + ("s" if len(_potential_jobs['unknown']) > 1 else '') + " "
                        + items_str(_potential_jobs['unknown'])
                        + " will be ignored."
                    ),
                    stack=False
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
    ### E.g. `start job --name myjob`
    elif name is not None:
        new_job = False
        names = [name]

    ### No action or --name was provided. Ask to start all stopped jobs.
    else:
        running_jobs = get_running_jobs(executor_keys, jobs, debug=debug)
        paused_jobs = get_paused_jobs(executor_keys, jobs, debug=debug)
        stopped_jobs = get_stopped_jobs(executor_keys, jobs, debug=debug)

        if not stopped_jobs and not paused_jobs:
            if not running_jobs:
                return False, "No jobs to start"
            return True, "All jobs are running."

        names = [
            name
            for name in list(stopped_jobs) + list(paused_jobs)
        ]

    def _run_new_job(name: Optional[str] = None):
        name = name or get_new_daemon_name()
        job = Job(name, sysargs, executor_keys=executor_keys, delete_after_completion=rm)
        return job.start(debug=debug), name

    def _run_existing_job(name: str):
        job = Job(name, executor_keys=executor_keys)
        return job.start(debug=debug), name

    if not names:
        return False, "No jobs to start."

    ### Get user permission to clear logs.
    _filtered_jobs = get_filtered_jobs(executor_keys, names, debug=debug)
    if not kw.get('force', False) and _filtered_jobs:
        _filtered_running_jobs = get_running_jobs(executor_keys, _filtered_jobs, debug=debug)
        _skipped_jobs = []
        if _filtered_running_jobs:
            pprint_jobs(_filtered_running_jobs)
            if yes_no(
                "Do you want to first stop these jobs?",
                default='n',
                yes=kw.get('yes', False),
                noask=kw.get('noask', False)
            ):
                stop_success_tuple = actions['stop'](
                    action=['jobs'] + [_name for _name in _filtered_running_jobs],
                    force=True,
                    executor_keys=executor_keys,
                    debug=debug,
                )
                if not stop_success_tuple[0]:
                    warn(
                        (
                            "Failed to stop job"
                            + ("s" if len(_filtered_running_jobs) != 1 else '')
                            + items_str([_name for _name in _filtered_running_jobs])
                            + "."
                        ),
                        stack=False
                    )
                    for _name in _filtered_running_jobs:
                        names.remove(_name)
                        _filtered_jobs.pop(_name)
            else:
                info(
                    "Skipping already running job"
                    + ("s" if len(_filtered_running_jobs) != 1 else '')
                    + ' '
                    + items_str([_name for _name in _filtered_running_jobs])
                    + '.'
                )
                for _name in _filtered_running_jobs:
                    names.remove(_name)
                    _filtered_jobs.pop(_name)
                    _skipped_jobs.append(_name)

        if not _filtered_jobs:
            return len(_skipped_jobs) > 0, "No jobs to start."

        pprint_jobs(_filtered_jobs, nopretty=kw.get('nopretty', False))
        info(
            "Starting the job"
            + ("s" if len(names) != 1 else "")
            + " " + items_str(names)
            + "..."
        )

    _successes, _failures = [], []
    for _name in names:
        success_tuple, __name = (
            _run_new_job(_name)
            if new_job
            else _run_existing_job(_name)
        )
        if not kw.get('nopretty', False):
            print_tuple(success_tuple)

        if success_tuple[0]:
            _successes.append(_name)
        else:
            _failures.append(_name)

    msg = (
        (("Successfully started job" + ("s" if len(_successes) != 1 else '')
            + f" {items_str(_successes)}." + ('\n' if _failures else ''))
            if _successes else '')
        + ("Failed to start job" + ("s" if len(_failures) != 1 else '')
            + f" {items_str(_failures)}." if _failures else '')
    )
    _install_healthcheck_job()
    return len(_failures) == 0, msg


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
    from meerschaum.utils.venv import venv_exec
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.networking import find_open_ports
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum._internal.term.tools import is_webterm_running
    import platform
    webview, requests = attempt_import('webview', 'requests')
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
        start_tornado_code, as_proc=True, debug=debug, capture_output=(not debug)
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
        webview.create_window(
            'Meerschaum Shell', 
            f'http://127.0.0.1:{port}',
            height = 650,
            width = 1000
        )
        webview.start(debug=debug)
    except Exception as e:
        import traceback
        traceback.print_exc()
        success, msg = False, str(e)
    except KeyboardInterrupt:
        success, msg = True, "Success"
    finally:
        process.kill()
    return success, msg


def _start_webterm(
    port: Optional[int] = None,
    host: Optional[str] = None,
    mrsm_instance: Optional[str] = None,
    force: bool = False,
    nopretty: bool = False,
    sysargs: Optional[List[str]] = None,
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

        - `-i`, '--instance'
            The default instance to use for the Webterm shell.
    """
    from meerschaum._internal.term import get_webterm_app_and_manager, tornado_ioloop
    from meerschaum._internal.term.tools import is_webterm_running
    from meerschaum.utils.networking import find_open_ports
    from meerschaum.utils.warnings import info

    if host is None:
        host = '127.0.0.1'
    if port is None:
        port = 8765
    if sysargs is None:
        sysargs = ['start', 'webterm']
    tornado_app, term_manager = get_webterm_app_and_manager(instance_keys=mrsm_instance)

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
    finally:
        term_manager.shutdown()
        loop.close()

    return True, "Success"


def _start_connectors(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    min_seconds: int = 3,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Start polling connectors to verify a connection can be made.
    """
    from meerschaum.connectors.poll import retry_connect
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.pool import get_pool
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import items_str
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    unique_keys = list(set(action + connector_keys))
    valid_conns = []
    for keys in unique_keys:
        try:
            conn = parse_instance_keys(keys)
        except Exception:
            warn(f"Invalid connector keys: '{keys}'. Skipping...", stack=False)
            continue
        valid_conns.append(conn)

    if not valid_conns:
        return False, "No valid connector keys were provided."

    connected = {}
    def connect(conn):
        success = retry_connect(
            conn,
            retry_wait=min_seconds,
            enforce_chaining=False,
            enforce_login=False,
            print_on_connect=True,
            debug=debug,
        )
        connected[conn] = success
        return success

    pool = get_pool()
    try:
        pool.map(connect, valid_conns)
    except KeyboardInterrupt:
        pass

    ### If a KeyboardInterrupt stopped a connection, mark as `False`.
    for conn in valid_conns:
        if conn not in connected:
            connected[conn] = False

    successes = [conn for conn, success in connected.items() if success]
    fails = [conn for conn, success in connected.items() if not success]

    success = len(fails) == 0
    msg = (
        "Successfully started connector" + ('s' if len(successes) != 1 else '')
        + ' ' + items_str(successes) + '.'
    ) if success else f"Failed to start {len(fails)} connectors."
    if len(fails) > 0:
        msg += (
            "\n    Failed to start connector" + ('s' if len(fails) != 1 else '')
            + ' ' + items_str(fails) + '.'
        )

    return success, msg


def _complete_start_connectors(**kw) -> List[str]:
    """
    Return a list of connectors.
    """
    from meerschaum.actions.show import _complete_show_connectors
    return _complete_show_connectors(**kw)


def _start_pipeline(
    action: Optional[List[str]] = None,
    loop: bool = False,
    min_seconds: Union[float, int, None] = 1.0,
    params: Optional[Dict[str, Any]] = None,
    **kwargs
) -> SuccessTuple:
    """
    Run a series of Meerschaum commands as a single action.

    Add `:` to the end of chained arguments to apply additional flags to the pipeline.

    Examples
    --------

    `sync pipes -i sql:local + sync pipes -i sql:main :: -s 'daily'`

    `show version + show arguments :: --loop`

    """
    import time
    from meerschaum._internal.entry import entry
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.misc import is_int

    do_n_times = (
        int(action[0].lstrip('x'))
        if action and is_int(action[0].lstrip('x'))
        else 1
    )

    params = params or {}
    sub_args_line = params.get('sub_args_line', None)
    patch_args = params.get('patch_args', None)

    if not sub_args_line:
        return False, "Nothing to do."

    if min_seconds is None:
        min_seconds = 1.0

    ran_n_times = 0
    success, msg = False, "Did not run pipeline."
    def run_loop():
        nonlocal ran_n_times, success, msg
        while True:
            success, msg = entry(sub_args_line, _patch_args=patch_args)
            ran_n_times += 1

            if not loop and do_n_times == 1:
                break

            if min_seconds != 0 and ran_n_times != do_n_times:
                info(f"Sleeping for {min_seconds} seconds...")
                time.sleep(min_seconds)

            if loop:
                continue

            if ran_n_times >= do_n_times:
                break

    try:
        run_loop()
    except KeyboardInterrupt:
        warn("Cancelled pipeline.", stack=False)

    if do_n_times != 1:
        info(f"Ran pipeline {ran_n_times} time" + ('s' if ran_n_times != 1 else '') + '.')
    return success, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
