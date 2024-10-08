#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""

from __future__ import annotations

from datetime import datetime
import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Union, Sequence, Any, Optional, List, Dict, Tuple

def show(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """Show elements of a certain type.
    
    Command:
        `show {option}`
    
    Example:
        `show pipes`
    """
    from meerschaum.actions import choose_subaction
    show_options = {
        'actions'    : _show_actions,
        'pipes'      : _show_pipes,
        'config'     : _show_config,
        'environment': _show_environment,
        'version'    : _show_version,
        'connectors' : _show_connectors,
        'arguments'  : _show_arguments,
        'data'       : _show_data,
        'columns'    : _show_columns,
        'rowcounts'  : _show_rowcounts,
        'plugins'    : _show_plugins,
        'packages'   : _show_packages,
        'help'       : _show_help,
        'users'      : _show_users,
        'jobs'       : _show_jobs,
        'logs'       : _show_logs,
        'tags'       : _show_tags,
        'schedules'  : _show_schedules,
        'venvs'      : _show_venvs,
    }
    return choose_subaction(action, show_options, **kw)


def _complete_show(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.delete import _complete_delete_jobs

    if action is None:
        action = []

    options = {
        'connector' : _complete_show_connectors,
        'connectors': _complete_show_connectors,
        'config'    : _complete_show_config,
        'package'   : _complete_show_packages,
        'packages'  : _complete_show_packages,
        'job'       : _complete_delete_jobs,
        'jobs'      : _complete_delete_jobs,
        'log'       : _complete_delete_jobs,
        'logs'      : _complete_delete_jobs,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['show'] + action), **kw)


def _show_actions(**kw: Any) -> SuccessTuple:
    """
    Show available actions.
    """
    from meerschaum.actions import actions
    from meerschaum.utils.misc import print_options
    from meerschaum._internal.shell.Shell import hidden_commands
    _actions = [ _a for _a in actions if _a not in hidden_commands ]
    print_options(
        options=_actions,
        name='actions',
        actions=False,
        sort_options=True,
        **kw
    )
    return True, "Success"


def _show_help(**kw: Any) -> SuccessTuple:
    """
    Print the --help menu from `argparse`.
    """
    from meerschaum._internal.arguments._parser import parser
    print(parser.format_help())
    return True, "Success"


def _show_config(
        action: Optional[List[str]] = None,
        debug: bool = False,
        nopretty: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Show the configuration dictionary.
    Sub-actions defined in the action list are recursive indices in the config dictionary.
    
    Example:
        `show config pipes` -> cf['pipes']
    """
    import json
    from meerschaum.utils.formatting import pprint
    from meerschaum.config import get_config
    from meerschaum.config._paths import CONFIG_DIR_PATH
    from meerschaum.utils.debug import dprint

    if action is None:
        action = []

    valid, config = get_config(*action, as_tuple=True, warn=False)
    if not valid:
        return False, f"Invalid configuration keys '{action}'."
    if nopretty:
        print(json.dumps(config))
    else:
        pprint(config)
    return (True, "Success")


def _complete_show_config(action: Optional[List[str]] = None, **kw : Any):
    from meerschaum.config._read_config import get_possible_keys
    keys = get_possible_keys()
    if not action:
        return keys
    possibilities = []
    for key in keys:
        if key.startswith(action[0]) and action[0] != key:
            possibilities.append(key)
    return possibilities


def _show_pipes(
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Print a stylized tree of available Meerschaum pipes.
    Respects global ANSI and UNICODE settings.
    """
    import json
    from meerschaum import get_pipes
    from meerschaum.utils.misc import flatten_pipes_dict
    from meerschaum.utils.formatting import ANSI, pprint_pipes
    pipes = get_pipes(debug=debug, **kw)

    if len(pipes) == 0:
        return True, "No pipes to show."

    if len(flatten_pipes_dict(pipes)) == 1:
        return flatten_pipes_dict(pipes)[0].show(debug=debug, nopretty=nopretty, **kw)

    if not nopretty:
        pprint_pipes(pipes)
    else:
        pipes_list = flatten_pipes_dict(pipes)
        for p in pipes_list:
            print(json.dumps(p.__getstate__()))

    return (True, "Success")


def _show_version(nopretty: bool = False, **kw : Any) -> SuccessTuple:
    """
    Show the Meerschaum doc string.

    Examples:
        - `show version`
        - `show version --nopretty`
    """
    from meerschaum import __version__ as version
    _print = print
    if nopretty:
        msg = version
    else:
        from meerschaum.utils.warnings import info
        msg = "Meerschaum v" + version
        _print = info
    _print(msg)
    return (True, "Success")


def _show_connectors(
        action: Optional[List[str]] = None,
        nopretty: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Show connectors configuration and, if requested, specific connector attributes.
    
    Examples:
        `show connectors`
            Display the connectors configuration.
    
        `show connectors sql:main`
            Show the connectors configuration and the attributes for the connector 'sql:main'.
    """
    from meerschaum.connectors import connectors
    from meerschaum.config import get_config
    from meerschaum.utils.formatting import make_header
    from meerschaum.utils.formatting import pprint

    conn_type = action[0].split(':')[0] if action else None

    if not nopretty:
        print(make_header(
            f"""\nConfigured {"'" + (conn_type + "' ") if conn_type else ''}Connectors:"""
        ))

    keys = ['meerschaum', 'connectors']
    if conn_type:
        keys.append(conn_type)
    pprint(get_config(*keys), nopretty=nopretty)
    if not nopretty and not conn_type:
        print(make_header("\nActive connectors:"))
        pprint(connectors, nopretty=nopretty)

    from meerschaum.connectors.parse import parse_instance_keys
    if action and ':' in action[0]:
        attr, keys = parse_instance_keys(action[0], construct=False, as_tuple=True, debug=debug)
        if attr:
            if not nopretty:
                print(make_header("\n" + f"Attributes for connector '{keys}':"))
            pprint(attr, nopretty=nopretty)

    return True, "Success"


def _complete_show_connectors(
    action: Optional[List[str]] = None, **kw: Any
) -> List[str]:
    from meerschaum.utils.misc import get_connector_labels
    _text = action[0] if action else ""
    return get_connector_labels(search_term=_text, ignore_exact_match=True)


def _show_arguments(
    **kw: Any
) -> SuccessTuple:
    """
    Show the parsed keyword arguments.
    """
    from meerschaum.utils.formatting import pprint
    pprint(kw)
    return True, "Success"


def _show_data(
    action: Optional[List[str]] = None,
    gui: bool = False,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = -1,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show pipes data as Pandas DataFrames.
    
    Usage:
        - Use --gui to open an interactive window.
    
        - `show data all` to grab all data for the chosen Pipes.
          WARNING: This may be dangerous!
    
        - `show data 60` to grab the last 60 (or any number) minutes of data for all pipes.
    
        - `show data --begin 2020-01-01 --end 2021-01-01` to specify date rangers.
          **NOTE:** You must specify to at least the day, otherwise the date parser will assume
          you mean today's date.
    
        - Regular pipes parameters (-c, -m, -l, etc.)
    
    Examples:
        - show data -m weather --gui
            Open an interactive pandasgui window for the last 1440 minutes of data
            for all pipes of metric 'weather'.
    """
    import sys, json
    from meerschaum import get_pipes
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.dataframe import to_json
    pd = import_pandas()

    if action is None:
        action = []

    pipes = get_pipes(as_list=True, params=params, debug=debug, **kw)
    try:
        backtrack_minutes = float(action[0])
    except Exception as e:
        backtrack_minutes = (
            1440 if (
                begin is None
                and end is None
                and (
                    not action
                    or (action and action[0] != 'all')
                )
            ) else None
        )

    for p in pipes:
        try:
            if backtrack_minutes is not None:
                df = p.get_backtrack_data(
                    backtrack_minutes = backtrack_minutes,
                    chunksize = chunksize,
                    params = params,
                    debug = debug,
                )
            else:
                df = p.get_data(
                    begin = begin,
                    end = end,
                    params = params,
                    chunksize = chunksize,
                    debug = debug,
                )
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            df = None

        if df is None:
            warn(f"Failed to fetch data for {p}.", stack=False)
            continue

        info_msg = (
            (
                f"Last {backtrack_minutes} minute"
                + ('s' if backtrack_minutes != 1 else '')
                + f" of data for {p}:"
            ) if backtrack_minutes is not None
            else (
                f"Data for {p}" +
                    (f" from {begin}" if begin is not None else '') +
                    (f" to {end}" if end is not None else '') + ':'
            )
        )

        if not nopretty:
            info(info_msg)
        else:
            print(json.dumps(p.__getstate__()))
            df = to_json(df, orient='columns')

        pprint(df, nopretty=nopretty)

    return True, "Success"


def _show_columns(
    action: Optional[List[str]] = None,
    debug: bool = False,
    nopretty: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the registered and table columns for pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.formatting._pipes import pprint_pipe_columns
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    for p in pipes:
        pprint_pipe_columns(p, nopretty=nopretty, debug=debug)

    return True, "Success"


def _show_rowcounts(
    action: Optional[List[str]] = None,
    workers: Optional[int] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the rowcounts for pipes.

    To see remote rowcounts (execute `COUNT(*)` on the source server),
    execute `show rowcounts remote`.
    """
    from meerschaum.utils.misc import print_options
    from meerschaum.utils.pool import get_pool
    from meerschaum import get_pipes

    if action is None:
        action = []
    remote = action and action[0] == 'remote'

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    pool = get_pool(workers=workers)
    def _get_rc(_pipe):
        return _pipe.get_rowcount(
            begin=begin,
            end=end,
            params=params,
            remote=remote,
            debug=debug
        )

    rowcounts = pool.map(_get_rc, pipes) if pool is not None else [_get_rc(p) for p in pipes]

    rc_dict = {}
    for i, p in enumerate(pipes):
        rc_dict[p] = rowcounts[i]

    msgs = []
    for p, rc in rc_dict.items():
        msgs.append(f'{p}\n{rc}\n')

    header = "Remote row-counts:" if remote else "Pipe row-counts:"

    print_options(msgs, header=header, **kw)

    return True, "Success"

def _show_plugins(
    action: Optional[List[str]] = None,
    repository: Optional[str] = None,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the installed plugins.
    """
    from meerschaum.plugins import import_plugins, get_plugins_names
    from meerschaum.utils.misc import print_options
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.warnings import info
    from meerschaum.core import User
    repo_connector = parse_repo_keys(repository)

    if action is None:
        action = []

    if action == [''] or len(action) == 0:
        _to_print = get_plugins_names()
        header = "Installed plugins:"
        if not nopretty:
            info(
                f"To see all installable plugins from repository '{repo_connector}', "
                + "run `show plugins all`"
            )
            info("To see plugins created by a certain user, run `show plugins [username]`")
    elif action[0] in ('all'):
        _to_print = repo_connector.get_plugins(debug=debug)
        header = f"Available plugins from Meerschaum repository '{repo_connector}':"
    else:
        username = action[0]
        user_id = repo_connector.get_user_id(User(username, ''))
        _to_print = repo_connector.get_plugins(user_id=user_id, debug=debug)
        header = f"Plugins from user '{username}' at Meerschaum repository '{repo_connector}':"

    print_options(_to_print, header=header, debug=debug, nopretty=nopretty, **kw)

    return True, "Success"

def _show_users(
    mrsm_instance: Optional[str] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the registered users in a Meerschaum instance (default is the current instance).
    """
    from meerschaum.config import get_config
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.misc import print_options
    instance_connector = parse_instance_keys(mrsm_instance)
    users_list = instance_connector.get_users(debug=debug)

    try:
        users_list = instance_connector.get_users(debug=debug)
    except Exception as e:
        return False, f"Failed to get users from instance '{mrsm_instance}'"

    print_options(users_list, header=f"Registered users for instance '{instance_connector}':")

    return True, "Success"

def _show_packages(
    action: Optional[List[str]] = None,
    nopretty: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the packages in dependency groups, or as a list with `--nopretty`.
    """
    from meerschaum.utils.packages import packages
    from meerschaum.utils.warnings import warn

    if action is None:
        action = []

    if not nopretty:
        from meerschaum.utils.formatting import pprint

    def _print_packages(_packages):
        for import_name, install_name in _packages.items():
            print(install_name)

    _print_func = pprint if not nopretty else _print_packages

    key = 'full' if len(action) == 0 else action[0]

    try:
        _print_func(packages[key])
    except KeyError:
        warn(f"'{key}' is not a valid group.", stack=False)

    return True, "Success"

def _complete_show_packages(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    from meerschaum.utils.packages import packages
    if not action:
        return sorted(list(packages.keys()))
    possibilities = []

    for key in packages:
        if key.startswith(action[0]) and action[0] != key:
            possibilities.append(key)

    return possibilities

def _show_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the currently running and stopped jobs.
    """
    from meerschaum.jobs import get_filtered_jobs
    from meerschaum.utils.formatting._jobs import pprint_jobs

    jobs = get_filtered_jobs(executor_keys, action, debug=debug)
    if not jobs:
        if not action and not nopretty:
            from meerschaum.utils.warnings import info
            info('No running or stopped jobs.')
            print(
                f"    You can start a background job with `-d` or `--daemon`,\n" +
                "    or run the command `start job` before action commands.\n\n" +
                "    Examples:\n" +
                "      - start api -d\n" +
                "      - start job sync pipes --loop"
            )
        return True, "No jobs to show."

    pprint_jobs(jobs, nopretty=nopretty)
    return True, "Success"


def _show_logs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    nopretty: bool = False,
    **kw
) -> SuccessTuple:
    """
    Show the logs for jobs.
    
    You may specify jobs to only print certain logs.
    To print the entire log file, add the `--nopretty` flag.
    
    Usage:
        `show logs`
        `show logs --nopretty`
        `show logs myjob myotherjob`
    """
    import os, pathlib, random, asyncio
    from functools import partial
    from datetime import datetime, timezone
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.jobs import get_filtered_jobs, Job
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import get_console, ANSI, UNICODE
    from meerschaum.utils.misc import tail
    from meerschaum.config._paths import LOGS_RESOURCES_PATH
    from meerschaum.config import get_config
    rich = import_rich()
    rich_text = attempt_import('rich.text')

    if not ANSI:
        info = print
    colors = get_config('jobs', 'logs', 'colors')
    timestamp_format = get_config('jobs', 'logs', 'timestamps', 'format')
    follow_timestamp_format = get_config('jobs', 'logs', 'timestamps', 'follow_format')

    jobs = get_filtered_jobs(executor_keys, action)
    now = datetime.now(timezone.utc)
    now_str = now.strftime(timestamp_format)
    now_follow_str = now.strftime(follow_timestamp_format)

    def build_buffer_spaces(_jobs) -> Dict[str, str]:
        max_len_id = (
            max(len(name) for name in _jobs) + 1
        ) if _jobs else 0
        buffer_len = max(
            get_config('jobs', 'logs', 'min_buffer_len'),
            max_len_id
        )
        return {
            name: ' ' * (buffer_len - len(name))
            for name in _jobs
        }

    def build_job_colors(_jobs, _old_job_colors=None) -> Dict[str, str]:
        return {name: colors[i % len(colors)] for i, name in enumerate(_jobs)}

    buffer_spaces = build_buffer_spaces(jobs)
    job_colors = build_job_colors(jobs)

    def get_buffer_spaces(name):
        nonlocal buffer_spaces, jobs
        if name not in buffer_spaces:
            if name not in jobs:
                jobs = get_filtered_jobs(executor_keys, action)
            buffer_spaces = build_buffer_spaces(jobs)
        return buffer_spaces[name] or ' '

    def get_job_colors(name):
        nonlocal job_colors, jobs
        if name not in job_colors:
            if name not in jobs:
                jobs = get_filtered_jobs(executor_keys, action)
            job_colors = build_job_colors(jobs)
        return job_colors[name]

    previous_line_timestamp = None
    def print_job_line(job, line):
        nonlocal previous_line_timestamp
        date_prefix_str = line[:len(now_str)]
        try:
            line_timestamp = datetime.strptime(date_prefix_str, timestamp_format)
            previous_line_timestamp = line_timestamp
        except Exception as e:
            line_timestamp = None
        if line_timestamp:
            line = line[(len(now_str) + 3):]
        else:
            line_timestamp = previous_line_timestamp

        if len(line) == 0 or line == '\n':
            return

        text = rich_text.Text(job.name)
        line_prefix = (
            get_buffer_spaces(job.name)
            + (line_timestamp.strftime(follow_timestamp_format) if line_timestamp else '')
            + ' | '
        )
        text.append(line_prefix + (line[:-1] if line[-1] == '\n' else line))
        if ANSI:
            text.stylize(
                get_job_colors(job.name),
                0,
                len(job.name) + len(line_prefix)
            )
        get_console().print(text)

    stop_event = asyncio.Event()
    job_tasks = {}
    job_stop_events = {}

    async def refresh_job_tasks():
        nonlocal job_tasks, jobs
        while not stop_event.is_set():
            jobs = get_filtered_jobs(executor_keys, action)
            for name, job in jobs.items():
                if name not in job_tasks:
                    job_stop_events[name] = asyncio.Event()
                    job_tasks[name] = asyncio.create_task(
                        job.monitor_logs_async(
                            partial(print_job_line, job),
                            stop_event=job_stop_events[name],
                            accept_input=False,
                            stop_on_exit=False,
                        )
                    )

            for name, task in [(k, v) for k, v in job_tasks.items()]:
                if name not in jobs:
                    job_stop_events[name].set()
                    task.cancel()

                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    finally:
                        _ = job_tasks.pop(name, None)
                        _ = job_stop_events.pop(name, None)

            await asyncio.sleep(1)

    async def gather_tasks():
        tasks = [refresh_job_tasks()] + list(job_tasks.values())
        await asyncio.gather(*tasks)

    if not nopretty:
        info("Watching logs...")
        try:
            asyncio.run(gather_tasks())
        except KeyboardInterrupt:
            pass
    else:
        for name, job in jobs.items():
            print(f'\n-*-\nMRSM_JOB: {name}\n-*-')
            print(job.get_logs())
    return True, "Success"


def _show_environment(
    nopretty: bool = False,
    **kw
) -> SuccessTuple:
    """
    Show all of the current environment variables with begin with `'MRSM_'`.
    """
    import os
    from meerschaum.utils.formatting import pprint
    from meerschaum.config._environment import get_env_vars
    pprint(
        {
            env_var: os.environ[env_var]
            for env_var in get_env_vars()
        },
        nopretty = nopretty,
    )
    return True, "Success"


def _show_tags(
    action: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    workers: Optional[int] = None,
    nopretty: bool = False,
    **kwargs
) -> SuccessTuple:
    """
    Show the existing tags and their associated pipes.
    """
    import json
    from collections import defaultdict
    import meerschaum as mrsm
    from meerschaum.utils.formatting import pipe_repr, UNICODE, ANSI
    from meerschaum.utils.pool import get_pool
    from meerschaum.config import get_config
    rich_tree, rich_panel, rich_text, rich_console, rich_columns = (
        mrsm.attempt_import('rich.tree', 'rich.panel', 'rich.text', 'rich.console', 'rich.columns')
    )
    panel = rich_panel.Panel.fit('Tags')
    tree = rich_tree.Tree(panel)
    action = action or []
    tags = action + (tags or [])
    pipes = mrsm.get_pipes(as_list=True, tags=tags, **kwargs)
    if not pipes:
        return False, f"No pipes were found with the given tags."

    pool = get_pool(workers=workers)
    tag_prefix = get_config('formatting', 'pipes', 'unicode', 'icons', 'tag') if UNICODE else ''
    tag_style = get_config('formatting', 'pipes', 'ansi', 'styles', 'tags') if ANSI else None

    tags_pipes = defaultdict(lambda: [])
    gather_pipe_tags = lambda pipe: (pipe, (pipe.tags or []))

    pipes_tags = dict(pool.map(gather_pipe_tags, pipes))

    for pipe, tags in pipes_tags.items():
        for tag in tags:
            if action and tag not in action:
                continue
            tags_pipes[tag].append(pipe)

    columns = []
    sorted_tags = sorted([tag for tag in tags_pipes])
    for tag in sorted_tags:
        _pipes = tags_pipes[tag]
        tag_text = (
            rich_text.Text(tag_prefix)
            + rich_text.Text(
                tag,
                style = tag_style,
            )
        )
        pipes_texts = [
            pipe_repr(pipe, as_rich_text=True)
            for pipe in _pipes
        ]
        tag_group = rich_console.Group(*pipes_texts)
        tag_panel = rich_panel.Panel(tag_group, title=tag_text, title_align='left')
        columns.append(tag_panel)

    if len(columns) == 0:
        return False, "No pipes have been tagged."

    if not nopretty:
        mrsm.pprint(
            rich_columns.Columns(
                columns,
                equal = True,
            ),
        )
    else:
        for tag, _pipes in tags_pipes.items():
            print(tag)
            for pipe in _pipes:
                print(json.dumps(pipe.meta))

    return True, "Success"


def _show_schedules(
    action: Optional[List[str]] = None,
    nopretty: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Print the upcoming timestamps according to the given schedule.

    Examples:
        show schedule 'daily starting 00:00'
        show schedule 'every 12 hours and mon-fri starting 2024-01-01'
    """
    from meerschaum.utils.schedule import parse_schedule
    from meerschaum.utils.misc import is_int
    from meerschaum.utils.formatting import print_options
    if not action:
        return False, "Provide a schedule to be parsed."
    schedule = action[0]
    default_num_timestamps = 5
    num_timestamps_str = action[1] if len(action) >= 2 else str(default_num_timestamps)
    num_timestamps = (
        int(num_timestamps_str)
        if is_int(num_timestamps_str)
        else default_num_timestamps
    )
    try:
        trigger = parse_schedule(schedule)
    except ValueError as e:
        return False, str(e)

    next_datetimes = []
    for _ in range(num_timestamps):
        try:
            next_dt = trigger.next()
            next_datetimes.append(next_dt)
        except Exception as e:
            break

    print_options(
        next_datetimes,
        num_cols = 1,
        nopretty = nopretty,
        header = (
            f"Next {min(num_timestamps, len(next_datetimes))} timestamps "
            + f"for schedule '{schedule}':"
        ),
    )

    return True, "Success"
        

def _show_venvs(
    **kwargs: Any    
):
    """
    Print the available virtual environments in the current MRSM_ROOT_DIR.
    """
    import os
    import pathlib
    from meerschaum.config.paths import VIRTENV_RESOURCES_PATH
    from meerschaum.utils.venv import venv_exists
    from meerschaum.utils.misc import print_options

    venvs = [
        _venv
        for _venv in os.listdir(VIRTENV_RESOURCES_PATH)
        if venv_exists(_venv)
    ]
    print_options(
        venvs,
        name = 'Venvs:',
        **kwargs
    )

    return True, "Success"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
show.__doc__ += _choices_docstring('show')
