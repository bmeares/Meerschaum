#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Miscellaneous functions go here
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Mapping, Any, Callable, Optional, List, Dict, SuccessTuple, Iterable, PipesDict
)

def add_method_to_class(
        func : Callable[[Any], Any],
        class_def : 'Class',
        method_name : Optional[str] = None,
        keep_self : Optional[bool] = None,
    ) -> Callable[[Any], Any]:
    """
    Add function `func` to class `class_def`.

    :param func:
        Function to be added as a method of the class

    :param class_def:
        Class we are modifying

    :param method_name:
        New name of the method. None will use func.__name__ (default).
    """
    from functools import wraps

    is_class = isinstance(class_def, type)
    
    @wraps(func)
    def wrapper(self, *args, **kw):
        print(self, args, kw)
        return func(*args, **kw)

    if method_name is None:
        method_name = func.__name__

    setattr(class_def, method_name, (
            wrapper if ((is_class and keep_self is None) or keep_self is False) else func
        )
    )

    return func

def choose_subaction(
        action : Optional[List[str]] = None,
        options : Optional[Dict[str, Any]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Given a dictionary of options and the standard Meerschaum actions list,
    check if choice is valid and execute chosen function, else show available
    options and return False

    :param action:
        subactions (e.g. `show pipes` -> ['pipes'])
    
    :param options:
        Available options to execute.
        option (key) -> function (value)
        Functions must accept **kw keyword arguments
        and return a tuple of success bool and message.
    """
    from meerschaum.utils.warnings import warn, info
    import inspect
    if action is None:
        action = []
    if options is None:
        options = {}
    parent_action = inspect.stack()[1][3]
    if len(action) == 0:
        action = ['']
    choice = action[0]

    def valid_choice(_choice : str, _options : dict):
        if _choice in _options:
            return _choice
        if (_choice + 's') in options:
            return _choice + 's'
        return None

    parsed_choice = valid_choice(choice, options)
    if parsed_choice is None:
        warn(f"Cannot {parent_action} '{choice}'. Choose one:", stack=False)
        for option in sorted(options):
            print(f"  - {parent_action} {option}")
        return (False, f"Invalid choice '{choice}'")
    ### remove parent sub-action
    kw['action'] = list(action)
    del kw['action'][0]
    return options[parsed_choice](**kw)

def generate_password(
        length : int = 12
    ) -> str:
    """
    Generate a secure password of given length.
    """
    import secrets, string
    return ''.join((secrets.choice(string.ascii_letters) for i in range(length)))

def is_int(s : str) -> bool:
    """
    Check if string is an int.
    """
    try:
        float(s)
    except Exception as e:
        return False
    
    return float(s).is_integer()

def get_options_functions():
    """
    Get options functions from parent module
    """
    import inspect
    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    print(parent_package)

def string_to_dict(
        params_string : str
    ) -> Dict[str, Any]:
    """
    Parse a string into a dictionary

    If the string begins with '{', parse as JSON. Else use simple parsing

    """
    if params_string == "":
        return dict()

    if str(params_string).startswith('{'):
        import json
        return json.loads(params_string)

    import ast
    params_dict = dict()
    for param in params_string.split(","):
        _keys = param.split(":")
        keys = _keys[:-1]
        try:
            val = _keys[-1]
        except Exception as e:
            val = str(_keys[-1])

        c = params_dict
        for _k in keys[:-1]:
            try:
                k = ast.literal_eval(_k)
            except Exception as e:
                k = str(_k)
            if k not in c:
                c[k] = {}
            c = c[k]

        c[keys[-1]] = val

    return params_dict

def parse_config_substitution(
        value : str,
        leading_key : str = 'MRSM',
        begin_key : str = '{',
        end_key : str = '}',
        delimeter : str = ':'
    ):
    """
    Parse Meerschaum substitution syntax
    E.g. MRSM{value1:value2} => ['value1', 'value2']
    NOTE: Not currently used. See `search_and_substitute_config` in `meerschaum.config._read_yaml`.
    """
    if not value.beginswith(leading_key):
        return value

    return leading_key[len(leading_key):][len():-1].split(delimeter)

def edit_file(
        path : Union[pathlib.Path, str],
        default_editor : str = 'pyvim',
        debug : bool = False
    ) -> bool:
    """
    Open a file for editing.

    Attempt to launch the user's defined $EDITOR, otherwise use pyvim.
    """
    import os
    from subprocess import call
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import run_python_package, attempt_import, package_venv
    try:
        EDITOR = os.environ.get('EDITOR', default_editor)
        if debug:
            dprint(f"Opening file '{path}' with editor '{EDITOR}'")
        rc = call([EDITOR, path])
    except Exception as e: ### can't open with default editors
        if debug:
            dprint(e)
            dprint('Failed to open file with system editor. Falling back to pyvim...')
        pyvim = attempt_import('pyvim', lazy=False)
        rc = run_python_package('pyvim', [path], venv=package_venv(pyvim), debug=debug)
    return rc == 0

def is_pipe_registered(
        pipe : Union['meerschaum.Pipe', 'meerschaum.Pipe.MetaPipe'],
        pipes : dict,
        debug : bool = False
    ):
    """
    Check if a Pipe or MetaPipe is inside the pipes dictionary.
    """
    from meerschaum.utils.debug import dprint
    ck, mk, lk = pipe.connector_keys, pipe.metric_key, pipe.location_key
    if debug:
        dprint(f'{ck}, {mk}, {lk}')
        dprint(f'{pipe}, {pipes}')
    return ck in pipes and mk in pipes[ck] and lk in pipes[ck][mk]

def _get_subaction_names(action : str, globs : dict = None) -> List[str]:
    """
    NOTE: Don't use this function. You should use `meerschaum.actions.get_subactions()` instead.
    This only exists for internal use.

    Return a list of function pointers to all subactions for a given action.
    """
    if globs is None:
        import importlib
        module = importlib.import_module(f'meerschaum.actions.{action}')
        globs = vars(module)
    subactions = []
    for item in globs:
        if f'_{action}' in item and 'complete' not in item.lstrip('_'):
            subactions.append(globs[item])
    return subactions

def choices_docstring(action : str, globs : Optional[Dict[str, Any]] = None) -> str:
    """
    Append the an action's available options to the module docstring.
    This function is to be placed at the bottom of each action module.
    """
    options_str = f"\n    Options:\n        `{action} "
    subactions = _get_subaction_names(action, globs=globs)
    options_str += "["
    sa_names = []
    for sa in subactions:
        try:
            sa_names.append(sa.__name__[len(f"_{action}") + 1:])
        except Exception as e:
            print(e)
            return ""
    for sa_name in sorted(sa_names):
        options_str += f"{sa_name}, "
    options_str = options_str[:-2] + "]`"
    return options_str

def print_options(
        options : Optional[Dict[str, Any]] = None,
        nopretty : bool = False,
        no_rich : bool = False,
        name : str = 'options',
        header : Optional[str] = None,
        actions : bool = False,
        num_cols : int = 8,
        **kw
    ) -> None:
    """
    Show available options from an iterable
    """
    from meerschaum.utils.packages import import_rich
    from meerschaum.utils.formatting import make_header
    from meerschaum.actions import actions as _actions

    if options is None:
        options = {}
    _options = []
    for o in options:
        _options.append(str(o))
    _header = f"Available {name}" if header is None else header

    def _print_options_no_rich():
        if not nopretty:
            print()
            print(make_header(_header))
        ### print actions
        for option in sorted(_options):
            if not nopretty:
                print("  - ", end="")
            print(option)
        if not nopretty:
            print()

    rich = import_rich()
    if rich is None or nopretty or no_rich:
        _print_options_no_rich()
        return None

    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.packages import attempt_import
    rich_columns = attempt_import('rich.columns')
    rich_panel = attempt_import('rich.panel')
    rich_table = attempt_import('rich.table')
    from rich import box
    Panel = rich_panel.Panel
    Columns = rich_columns.Columns
    Table = rich_table.Table

    if _header is not None:
        table = Table(
            title = '\n' + _header,
            box = box.SIMPLE,
            show_header = False,
            show_footer = False,
            title_style = ''
        )
    else:
        table = Table.grid(padding=(0, 2))
    for i in range(num_cols):
        table.add_column()

    chunks = iterate_chunks(sorted(_options), num_cols, fillvalue='')
    for c in chunks:
        table.add_row(*c)

    cols = Columns([
        o for o in sorted(_options)
    ], expand=True, equal=True, title=header, padding=(0, 0))
    rich.print(table)
    return None

def iterate_chunks(iterable, chunksize : int, fillvalue : Optional[Any] = None):
    """
    Iterate over a list in chunks.
    Found here:
        https://stackoverflow.com/questions/
        434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks
    """
    from itertools import zip_longest
    args = [iter(iterable)] * chunksize
    return zip_longest(*args, fillvalue=fillvalue)

def sorted_dict(d : dict) -> dict:
    """
    Sort a dictionary's keys and values and return a new dictionary
    """
    try:
        return {key: value for key, value in sorted(d.items(), key=lambda item: item[1])}
    except Exception as e:
        return d

def flatten_pipes_dict(pipes_dict : dict) -> list:
    """
    Convert the standard pipes dictionary into a list
    """
    pipes_list = []
    for ck in pipes_dict.values():
        for mk in ck.values():
            pipes_list += list(mk.values())
    return pipes_list

def round_time(
        dt : 'datetime.datetime' = None,
        date_delta : 'datetime.timedelta' = None,
        to : 'str' = 'down'
    ) -> 'datetime.datetime':
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    Function found from:
    http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    import datetime
    if date_delta is None:
        date_delta = datetime.timedelta(minutes=1)
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.datetime.utcnow()
    seconds = (dt.replace(tzinfo=None) - dt.min.replace(tzinfo=None)).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)

def parse_df_datetimes(
        df : 'pd.DataFrame',
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Parse a pandas DataFrame for datetime columns and cast as datetimes
    """
    from meerschaum.utils.packages import import_pandas
    ### import pandas (or pandas replacement)
    from meerschaum.utils.debug import dprint
    pd = import_pandas()

    ### if df is a dict, build DataFrame
    if not isinstance(df, pd.DataFrame):
        if debug:
            dprint(f"df is not a DataFrame. Casting to DataFrame...")
        df = pd.DataFrame(df)

    ### skip parsing if DataFrame is empty
    if len(df) == 0:
        if debug:
            dprint(f"df is empty. Returning original DataFrame without casting datetime columns...")
        return df

    ### apply regex to columns to determine which are ISO datetimes
    iso_dt_regex = r'\d{4}-\d{2}-\d{2}.\d{2}\:\d{2}\:\d{2}'
    dt_mask = df.astype(str).apply(
        lambda s : s.str.match(iso_dt_regex).all()
    )

    ### list of datetime column names
    datetimes = list(df.loc[:, dt_mask])
    if debug:
        dprint("Converting columns to datetimes: " + str(datetimes))

    ### apply to_datetime
    df[datetimes] = df[datetimes].apply(pd.to_datetime)

    ### strip timezone information
    for dt in datetimes:
        df[dt] = df[dt].dt.tz_localize(None)

    return df

def timed_input(
        seconds : int = 10,
        timeout_message : str = "",
        prompt : str = "",
        icon : bool = False,
        **kw
    ) -> Optional[str]:
    """
    Accept user input only for a brief period of time.
    """
    from meerschaum.utils.prompt import prompt as _prompt
    import signal

    class TimeoutExpired(Exception):
        """
        Raise an exception when the timer runs out.
        """

    def alarm_handler(signum, frame):
        raise TimeoutExpired

    # set signal handler
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(seconds) # produce SIGALRM in `timeout` seconds

    try:
        #  return _prompt(prompt, icon=icon, **kw)
        return input(prompt)
    except TimeoutExpired:
        return None
    finally:
        signal.alarm(0) # cancel alarm

def retry_connect(
        connector : Union[
            meerschaum.connectors.sql.SQLConnector,
            meerschaum.connectors.api.APIConnector,
            None
        ] = None,
        max_retries : int = 40,
        retry_wait : int = 3,
        workers : int = 1,
        warn : bool = True,
        debug : bool = False,
    ):
    """
    Keep trying to connect to the database.
    """
    from meerschaum.utils.warnings import warn as _warn, error, info
    from meerschaum.utils.debug import dprint
    from meerschaum import get_connector
    from meerschaum.utils.packages import attempt_import
    from meerschaum.connectors.sql._tools import test_queries
    from functools import partial
    import time

    ### Get default connector if None is provided.
    if connector is None:
        connector = get_connector()

    if connector.type not in ('sql', 'api'):
        return None

    retries = 0
    connected, chaining_status = False, None
    while retries < max_retries:
        if debug:
            dprint(f"Trying to connect to '{connector}'...")
            dprint(f"Attempt ({retries + 1} / {max_retries})")

        if connector.type == 'sql':

            def _connect(_connector):
                ### Test queries like `SELECT 1`.
                connect_query = test_queries.get(connector.flavor, test_queries['default'])
                if _connector.exec(connect_query) is None:
                    raise Exception("Failed to connect.")

            try:
                _connect(connector)
                connected = True
            except Exception as e:
                print(e)
                connected = False

        elif connector.type == 'api':
            ### If the remote instance does not allow chaining, don't even try logging in.
            if not isinstance(chaining_status, bool):
                chaining_status = connector.get_chaining_status(debug=debug)
                if chaining_status is None:
                    connected = None
                elif chaining_status is False:
                    if warn:
                        _warn(
                            f"Meerschaum instance '{connector}' does not allow chaining " +
                            "and cannot be used as the parent for this instance.",
                            stack = False
                        )
                    return False
            if chaining_status:
                connected = connector.login(debug=debug)[0]
                if not connected and warn:
                    _warn(f"Unable to login to '{connector}'!", stack=False)

        if connected:
            if debug:
                dprint("Connection established!")
            return True

        if warn:
            _warn(
                f"Connection failed. Press [Enter] to retry or wait {retry_wait} seconds.",
                stack = False
            )
            info(
                f"To quit, press CTRL-C, then 'q' + Enter for each worker" +
                (f" ({workers})." if workers is not None else ".")
            )
        try:
            if retry_wait > 0:
                text = timed_input(retry_wait)
                if text in ('q', 'quit', 'pass', 'exit', 'stop'):
                    return None
        except KeyboardInterrupt:
            return None
        retries += 1

    return False

def df_from_literal(
        pipe : Optional['meerschaum.Pipe'] = None,
        literal : str = None,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Parse a literal (if nessary), and use a Pipe's column names to generate a DataFrame.
    """
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.warnings import error, warn
    from meerschaum.utils.debug import dprint

    if pipe is None or literal is None:
        error("Please provide a Pipe and a literal value")
    ### this will raise an error if the columns are undefined
    dt_name, val_name = pipe.get_columns('datetime', 'value')

    val = literal
    if isinstance(literal, str):
        if debug:
            dprint(f"Received literal string: '{literal}'")
        import ast
        try:
            val = ast.literal_eval(literal)
        except Exception as e:
            warn(
                "Failed to parse value from string:\n" + f"{literal}" +
                "\n\nWill cast as a string instead."\
            )
            val = literal

    ### NOTE: we do everything in UTC if possible.
    ### In dealing with timezones / Daylight Savings lies madness.
    import datetime
    now = datetime.datetime.utcnow()

    pd = import_pandas()
    return pd.DataFrame({dt_name : [now], val_name : [val]})

def filter_unseen_df(
        old_df : 'pd.DataFrame',
        new_df : 'pd.DataFrame',
        dtypes : dict = None,
        custom_nan : str = 'mrsm_NaN',
        debug : bool = False,
    ) -> 'pd.DataFrame':
    """
    Left join two DataFrames to find the newest unseen data.

    I have scoured the web for the best way to do this.
    My intuition was to join on datetime and id, but the code below accounts for values as well
    without needing to define expicit columns or indices.

    The logic below is based off this StackOverflow question, with an index reset thrown on top:
    https://stackoverflow.com/questions/
    48647534/python-pandas-find-difference-between-two-data-frames#48647840

    Also, NaN apparently does not equal NaN, so I am temporarily replacing instances of NaN with a
    custom string, per this StackOverflow question:
    https://stackoverflow.com/questions/31833635/pandas-checking-for-nan-not-working-using-isin

    Lastly, use the old DataFrame's columns for the new DataFrame,
    because order matters when checking equality.
    """
    if old_df is None:
        return new_df
    old_cols = list(old_df.columns)
    try:
        new_df = new_df[old_cols]
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(
            "Was not able to cast old columns onto new DataFrame. " +
            f"Are both DataFrames the same shape? Error:\n{e}"
        )
        return None

    ### assume the old_df knows what it's doing, even if it's technically wrong.
    if dtypes is None:
        dtypes = dict(old_df.dtypes)
    new_df = new_df.astype(dtypes)

    if len(old_df) == 0:
        return new_df

    return new_df[
        ~new_df.fillna(custom_nan).apply(tuple, 1).isin(old_df.fillna(custom_nan).apply(tuple, 1))
    ].reset_index(drop=True)

def change_dict(d : Dict[Any, Any], func : 'function') -> None:
    """
    Originally was local, moving to global for multiprocessing debugging
    """
    for k, v in d.items():
        if isinstance(v, dict):
            change_dict(v, func)
        else:
            d[k] = func(v)

def replace_pipes_in_dict(
        pipes : Optional[PipesDict] = None,
        func : 'function' = str,
        debug : bool = False,
        **kw
    ) -> PipesDict:
    """
    Replace the Pipes in a Pipes dict with the result of another function
    """
    if pipes is None:
        from meerschaum import get_pipes
        pipes = get_pipes(debug=debug, **kw)

    result = pipes.copy()
    change_dict(result, func)
    return result

def enforce_gevent_monkey_patch():
    """
    Check if gevent monkey patching is enabled, and if not, then apply patching
    """
    from meerschaum.utils.packages import attempt_import
    import socket
    gevent, gevent_socket, gevent_monkey = attempt_import(
        'gevent', 'gevent.socket', 'gevent.monkey'
    )
    if not socket.socket is gevent_socket.socket:
        gevent_monkey.patch_all()

def is_valid_email(email : str) -> bool:
    """
    Check whether a string is a valid email
    """
    import re
    regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    return re.search(regex, email)

def string_width(string : str) -> int:
    """
    Calculate the width of a string after its last newline
    """
    found_newline = False
    width = 0
    for c in reversed(string):
        if c == '\n' and found_newline:
            break
        if c == '\n':
            found_newline = True
            continue
        if found_newline:
            width += 1
    return width

def _pyinstaller_traverse_dir(
        directory : str,
        ignore_patterns : Iterable[str] = ('.pyc', 'dist', 'build', '.git', '.log'),
        include_dotfiles : bool = False
    ) -> list:
    """
    Recursively traverse a directory and return a list of its contents.
    """
    import os, pathlib
    paths = []
    _directory = pathlib.Path(directory)

    def _found_pattern(name : str):
        for pattern in ignore_patterns:
            if pattern.replace('/', os.path.sep) in str(name):
                return True
        return False

    for root, dirs, files in os.walk(_directory):
        _root = str(root)[len(str(_directory.parent)):]
        if _root.startswith(os.path.sep):
            _root = _root[len(os.path.sep):]
        if _root.startswith('.') and not include_dotfiles:
            continue
        ### ignore certain patterns
        if _found_pattern(_root):
            continue

        for filename in files:
            if filename.startswith('.') and not include_dotfiles:
                continue
            path = os.path.join(root, filename)
            if _found_pattern(path):
                continue

            _path = str(path)[len(str(_directory.parent)):]
            if _path.startswith(os.path.sep):
                _path = _path[len(os.path.sep):]
            _path = os.path.sep.join(_path.split(os.path.sep)[:-1])

            paths.append((path, _path))
    return paths

def replace_password(d : dict) -> dict:
    """
    Recursively replace passwords in a dictionary.
    """
    _d = d.copy()
    for k, v in d.items():
        if isinstance(v, dict):
            _d[k] = replace_password(v)
        elif 'password' in str(k).lower():
            _d[k] = ''.join(['*' for char in str(v)])
    return _d

def filter_keywords(
        func : Callable[[Any], Any],
        **kw : Any
    ) -> Mapping[str, Any]:
    """
    Filter out unsupported keywords.

    :param func:
        The function to inspect.
    """
    import inspect
    func_params = inspect.signature(func).parameters
    ### If the function has a **kw method, skip filtering.
    for param, _type in func_params.items():
        if '**' in str(_type):
            return kw
    func_kw = dict()
    for k, v in kw.items():
        if k in func_params:
            func_kw[k] = v
    return func_kw

def dict_from_od(od : collections.OrderedDict) -> Dict[Any, Any]:
    """
    Convert an ordered dict to a dict.
    Does not mutate the original OrderedDict.
    """
    from collections import OrderedDict
    _d = dict(od)
    for k, v in od.items():
        if isinstance(v, OrderedDict) or (
            issubclass(type(v), OrderedDict)
        ):
            _d[k] = dict_from_od(v)
    return _d

def remove_ansi(s : str) -> str:
    """
    Remove ANSI escape characters from a string.
    """
    import re
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', s)

def get_connector_labels(
        *types : str,
        search_term : str = '',
        ignore_exact_match = True,
    ) -> List[str]:
    """
    Read connector lables from config.
    """
    from meerschaum.config import get_config
    connectors = get_config('meerschaum', 'connectors')

    _types = list(types)
    if len(_types) == 0:
        _types = list(connectors.keys()) + ['plugin']

    conns = []
    for t in _types:
        if t == 'plugin':
            from meerschaum.actions.plugins import get_data_plugins
            conns += [f'{t}:' + m.__name__.split('.')[-1] for m in get_data_plugins()]
            continue
        conns += [ f'{t}:{label}' for label in connectors.get(t, {}) if label != 'default' ]

    possibilities = [
        c for c in conns
            if c.startswith(search_term)
                and c != (
                    search_term if ignore_exact_match else ''
                )
    ]
    return sorted(possibilities)

def json_serialize_datetime(dt : datetime.datetime) -> Optional[str]:
    """
    Serialize a datetime.datetime object into JSON (ISO format string).
    """
    import datetime
    if isinstance(dt, datetime.datetime):
        return dt.isoformat() + 'Z'
    return None

def wget(
        url : str,
        dest : Optional[Union[str, pathlib.Path]] = None,
        color : bool = True,
        debug : bool = False,
        **kw : Any
    ) -> pathlib.Path:
    """
    Mimic wget with requests.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.debug import dprint
    import os, pathlib, re, urllib.request
    if not color:
        dprint = print
    if debug:
        dprint(f"Downloading from '{url}'...")
    try:
        response = urllib.request.urlopen(url)
    except Exception as e:
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context
        try:
            response = urllib.request.urlopen(url)
        except Exception as _e:
            print(_e)
            response = None
    if response is None or response.code != 200:
        error_msg = f"Failed to download from '{url}'."
        if color:
            error(error_msg)
        else:
            print(error_msg)
            import sys
            sys.exit(1)

    d = response.headers.get('content-disposition', None)
    fname = (
        re.findall("filename=(.+)", d)[0].strip('"') if d is not None
        else url.split('/')[-1]
    )

    if dest is None:
        dest = pathlib.Path(os.path.join(os.getcwd(), fname))
    elif isinstance(dest, str):
        dest = pathlib.Path(dest)

    with open(dest, 'wb') as f:
        f.write(response.fp.read())

    if debug:
        dprint(f"Downloaded file '{dest}'.")

    return dest

def async_wrap(func):
    """
    Run a synchronous function as async.
    Found at
    https://dev.to/0xbf/turn-sync-function-to-async-python-tips-58nn
    """
    import asyncio
    from functools import wraps, partial

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run 

def debug_trace(browser : bool = True):
    from meerschaum.utils.packages import attempt_import
    heartrate = attempt_import('heartrate')
    heartrate.trace(files=heartrate.files.all, browser=browser)

def items_str(
        items : List[Any],
        quotes : bool = True,
        quote_str : str = "'",
        commas : bool = True,
        comma_str : str = ',',
        and_ : bool = True,
        and_str : str = 'and',
        oxford_comma : bool = True,
        spaces : bool = True,
        space_str = ' ',
    ) -> 'str':
    """
    Return a formatted string if list items separated by commas.

    :param quotes:
        If `True`, wrap items in quotes.
        Defaults to `True`.

    :param quote_str:
        If `quotes` is `True`, prepend and append each item with this string.
        Defaults to "'" (single quote).

    :param and_:
        If `True`, include the word 'and' before the final item in the list.
        Defaults to `True`.

    :param and_str:
        If `and_` is True, insert this string where 'and' normally would in and English list. 
        Defaults to 'and'.

    :param oxford_comma:
        If `True`, include the Oxford Comma (comma before the final 'and').
        Only applies when `and_` is `True`.
        Defaults to `True`.

    :param spaces:
        If `True`, separate items with `space_str`
        Defaults to `True`.

    :param space_str:
        If `spaces` is `True`, separate items with this string.
        Defaults to ' '.
    """
    if not items:
        return ''
    
    q = quote_str if quotes else ''
    s = space_str if spaces else ''
    a = and_str if and_ else ''
    c = comma_str if commas else ''

    if len(items) == 1:
        return q + str(items[0]) + q

    if len(items) == 2:
        return q + str(items[0]) + q + s + a + s + q + str(items[1]) + q

    sep = q + c + s + q
    output = q + sep.join(str(i) for i in items[:-1]) + q
    if oxford_comma:
        output += c
    output += s + a + (s if and_ else '') + q + str(items[-1]) + q
    return output
