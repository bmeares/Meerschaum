#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Union, Sequence, Any, Optional, List

def show(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """Show elements of a certain type.
    
    Command:
        `show {option}`
    
    Example:
        `show pipes`
    """

    from meerschaum.utils.misc import choose_subaction
    show_options = {
        'actions'    : _show_actions,
        'pipes'      : _show_pipes,
        'config'     : _show_config,
        'modules'    : _show_modules,
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
        'gui'        : _show_gui,
    }
    return choose_subaction(action, show_options, **kw)

def _complete_show(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.start import _complete_start_jobs

    if action is None:
        action = []

    options = {
        'connector' : _complete_show_connectors,
        'connectors': _complete_show_connectors,
        'config'    : _complete_show_config,
        'package'   : _complete_show_packages,
        'packages'  : _complete_show_packages,
        'job'       : _complete_start_jobs,
        'jobs'      : _complete_start_jobs,
        'log'       : _complete_start_jobs,
        'logs'      : _complete_start_jobs,
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
    print_options(options=_actions, name='actions', actions=False, **kw)
    return True, "Success"

def _show_help(**kw: Any) -> SuccessTuple:
    """
    Print the --help menu from `argparse`.
    """
    from meerschaum.actions.arguments._parser import parser
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

def _show_modules(**kw: Any) -> SuccessTuple:
    """
    Show the currently imported modules.
    """
    import sys
    from meerschaum.utils.formatting import pprint
    pprint(list(sys.modules.keys()), **kw)
    return (True, "Success")

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
        return False, "No pipes to show."

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
    if not nopretty:
        print(make_header("\nConfigured connectors:"))
    pprint(get_config('meerschaum', 'connectors'), nopretty=nopretty)
    if not nopretty:
        print(make_header("\nActive connectors:"))
        pprint(connectors, nopretty=nopretty)

    from meerschaum.connectors.parse import parse_instance_keys
    if action:
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
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
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
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import pprint

    if action is None:
        action = []

    pipes = get_pipes(as_list=True, params=params, debug=debug, **kw)
    try:
        backtrack_minutes = float(action[0])
    except Exception as e:
        backtrack_minutes = (
            1440 if (
                begin is None and end is None and (not action or (action and action[0] != 'all'))
            ) else None
        )

    for p in pipes:
        try:
            if backtrack_minutes is not None:
                df = p.get_backtrack_data(
                    backtrack_minutes=backtrack_minutes,
                    chunksize=chunksize,
                    params=params,
                    debug=debug
                )
            else:
                df = p.get_data(
                    begin=begin, end=end, debug=debug, chunksize=chunksize,
                    params=params,
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
            df = df.to_json(orient='columns')
        pprint(df, nopretty=nopretty)
        if gui and not nopretty:
            pandasgui = attempt_import('pandasgui')
            try:
                pandasgui.show(df)
            except Exception as e:
                df.plot()
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
        return _pipe.get_rowcount(remote=remote, debug=debug)

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
        nopretty: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Show the currently running and stopped jobs.
    """
    from meerschaum.utils.daemon import get_filtered_daemons
    from meerschaum.utils.formatting._jobs import pprint_jobs
    daemons = get_filtered_daemons(action)
    if not daemons:
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
        return False, "No jobs to show."
    pprint_jobs(daemons, nopretty=nopretty)
    return True, "Success"

def _show_logs(
        action: Optional[List[str]] = None,
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
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.utils.daemon import get_filtered_daemons, Daemon, Log
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import get_console, ANSI, UNICODE
    from meerschaum.utils.misc import tail
    from meerschaum.config._paths import LOGS_RESOURCES_PATH
    from meerschaum.config import get_config
    colors = get_config('jobs', 'logs', 'colors')
    daemons = get_filtered_daemons(action)

    def _build_buffer_spaces(daemons) -> Dict[str, str]:
        _max_len_id = max([len(d.daemon_id) for d in daemons]) if daemons else 0
        _buffer_len = max(get_config('jobs', 'logs', 'min_buffer_len'), _max_len_id + 2)
        return {
            d.daemon_id: ''.join([' ' for i in range(_buffer_len - len(d.daemon_id))])
            for d in daemons
        }

    def _build_job_colors(daemons, _old_job_colors = None) -> Dict[str, str]:
        return {d.daemon_id: colors[i % len(colors)] for i, d in enumerate(daemons)}

    _buffer_spaces = _build_buffer_spaces(daemons)
    _job_colors = _build_job_colors(daemons)

    def _get_buffer_spaces(daemon_id):
        nonlocal _buffer_spaces, daemons
        if daemon_id not in _buffer_spaces:
            d = Daemon(daemon_id=daemon_id)
            if d not in daemons:
                #  daemons.append(Daemon(daemon_id=daemon_id))
                daemons = get_filtered_daemons(action)
            _buffer_spaces = _build_buffer_spaces(daemons)
        return _buffer_spaces[daemon_id]

    def _get_job_colors(daemon_id):
        nonlocal _job_colors, daemons
        if daemon_id not in _job_colors:
            d = Daemon(daemon_id=daemon_id)
            if d not in daemons:
                daemons = get_filtered_daemons(action)
                #  daemons.append(Daemon(daemon_id=daemon_id))
            _job_colors = _build_job_colors(daemons)
        return _job_colors[daemon_id]

    def _follow_pretty_print():
        watchgod = attempt_import('watchgod')
        rich = import_rich()
        rich_text = attempt_import('rich.text')
        _watch_daemon_ids = set([d.daemon_id for d in daemons])
        info("Watching log files...") if ANSI else print('Watching log files...')

        def _print_job_line(daemon, line):
            text = rich_text.Text(daemon.daemon_id)
            text.append(
                _get_buffer_spaces(daemon.daemon_id) + '| '
                + (line[:-1] if line[-1] == '\n' else line)
            )
            if ANSI:
                text.stylize(
                    _get_job_colors(daemon.daemon_id),
                    0,
                    len(daemon.daemon_id) + len(_get_buffer_spaces(daemon.daemon_id)) + 1
                )
            get_console().print(text)


        def _print_log_lines(daemon):
            if not daemon.log_path.exists():
                return
            for line in Log(daemon.log_path, offset_file_path=daemon.log_offset_path):
                _print_job_line(daemon, line)

        def _seek_back_offset(d) -> bool:
            if not d.log_path.exists():
                return False
            if not d.log_offset_path.exists():
                Log(d.log_path, offset_file_path=d.log_offset_path).read()
            if not d.log_offset_path.exists():
                return False
            log_text = d.log_text
            if log_text is None:
                return False
            lines_to_show = get_config('jobs', 'logs', 'lines_to_show')
            begin_index, end_index = 0, len(log_text)
            backup_index = 0
            for i in range(lines_to_show):
                nl_index = log_text.rfind('\n', begin_index, end_index)
                if nl_index == -1:
                    break
                end_index = nl_index
            backup_index = ((len(log_text) - nl_index) - 1) if end_index not in (-1, 0) else 0

            with open(d.log_offset_path, 'r') as f:
                offset_lines = f.readlines()
                char_index = int(offset_lines[-1].rstrip('\n'))
                new_offset_lines = [
                    offset_lines[0],
                    str((len(log_text) - backup_index) if char_index > backup_index else 0) + '\n'
                ]
            with open(d.log_offset_path, 'w') as f:
                f.writelines(new_offset_lines)
            return True

        for d in daemons:
            _seek_back_offset(d)
            _print_log_lines(d)

        _quit = False
        async def _watch_logs():
            async for changes in watchgod.awatch(LOGS_RESOURCES_PATH):
                if _quit:
                    return
                for change in changes:
                    file_path_str = change[1]
                    if '.log' not in file_path_str:
                        continue
                    file_path = pathlib.Path(file_path_str)
                    if not file_path.exists():
                        continue
                    daemon_id = file_path.name.replace('.log', '')
                    if daemon_id not in _watch_daemon_ids and action:
                        continue
                    daemon = Daemon(daemon_id=daemon_id)
                    if daemon.log_path.exists():
                        _print_log_lines(daemon)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_watch_logs())
        except KeyboardInterrupt:
            _quit = True

    def _print_nopretty_log_text():
        for d in daemons:
            log_text = d.log_text
            print(d.daemon_id)
            print(log_text)

    _print_log_text = _follow_pretty_print if not nopretty else _print_nopretty_log_text
    _print_log_text()

    return True, "Success"


def _show_gui(**kw) -> SuccessTuple:
    """
    Start the Meerschaum GUI application.
    """
    from meerschaum.actions import actions
    kw.pop('action', None)
    return actions['start'](['gui'], **kw)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
show.__doc__ += _choices_docstring('show')
