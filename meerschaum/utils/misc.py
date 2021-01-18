#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Miscellaneous functions go here
"""

import sys

def add_method_to_class(
        func : 'function',
        class_def : 'class', 
        method_name : str = None
    ) -> 'function':
    """
    Add function `func` to class `class_def`
    func - function :
        function to be added as a method of the class
    class_def - class :
        class we are modifying
    method_name - str (default None) :
        new name of the method. None will use func.__name__
    """
    from functools import wraps

    @wraps(func)
    def wrapper(self, *args, **kw):
        return func(*args, **kw)

    if method_name is None: method_name = func.__name__
    setattr(class_def, method_name, wrapper)
    return func

def choose_subaction(
        action : list = [''],
        options : dict = {},
        **kw
    ) -> tuple:
    """
    Given a dictionary of options and the standard Meerschaum actions list,
    check if choice is valid and execute chosen function, else show available
    options and return False
    
    action - list:
        subactions (e.g. `show pipes` -> ['pipes'])
    options - dict:
        Available options to execute
        option (key) -> function (value)
        Functions must accept **kw keyword arguments
        and return a tuple of success code and message
    """
    import inspect
    parent_action = inspect.stack()[1][3]
    if len(action) == 0: action = ['']
    choice = action[0]
    if choice not in options:
        print(f"Cannot {parent_action} '{choice}'. Choose one:")
        for option in sorted(options):
            print(f"  - {parent_action} {option}")
        return (False, f"Invalid choice '{choice}'")
    ### remove parent sub-action
    kw['action'] = list(action)
    del kw['action'][0]
    return options[choice](**kw)

def generate_password(
        length : int = 12
    ):
    """
    Generate a secure password of given length.
    """
    import secrets, string
    return ''.join((secrets.choice(string.ascii_letters) for i in range(length)))

def yes_no(
        question : str = '',
        options : list = ['y', 'n'],
        default : str = 'y',
        wrappers : tuple = ('[', ']'),
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input
    
    Returns bool (answer)
    """
    from meerschaum.utils.warnings import error
    from prompt_toolkit import prompt
    ending = f" {wrappers[0]}" + "/".join(
                [ o.upper() if o.lower() == default.lower() else o.lower() for o in options ]
                ) + f"{wrappers[1]} "
    try:
        answer = prompt(question + ending)
    except:
        error(f"Error getting response.", stack=False)
    if answer == "": answer = default
    return answer.lower() == options[0].lower()

def is_int(s):
    """
    Check if string is an int
    """
    try:
        float(s)
    except ValueError:
        return False
    else:
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
    ) -> dict:
    """
    Parse a string into a dictionary

    If the string begins with '{', parse as JSON. Else use simple parsing

    """
    if params_string == "": return dict()

    if str(params_string)[0] == '{':
        import json
        return json.loads(params_string)

    import ast
    params_dict = dict()
    for param in params_string.split(","):
        values = param.split(":")
        try:
            key = ast.literal_eval(values[0])
        except:
            key = str(values[0])

        for value in values[1:]:
            try:
                params_dict[key] = ast.literal_eval(value)
            except:
                params_dict[key] = str(value)
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
    NOTE: Not currently used. See `search_and_substitute_config` below
    """
    if not value.beginswith(leading_key):
        return value
    
    return leading_key[len(leading_key):][len():-1].split(delimeter)
    
def search_and_substitute_config(
        config : dict,
        leading_key : str = "MRSM",
        delimiter : str = ":",
        begin_key : str = "{",
        end_key : str = "}"
    ) -> dict:
    """
    Search the config for Meerschaum substitution syntax and substite with value of keys

    Example:
        MRSM{meerschaum:connectors:main:host} => cf['meerschaum']['connectors']['main']['host']
    """
    try:
        import yaml
    except ImportError:
        return config
    needle = leading_key
    haystack = yaml.dump(config)
    mod_haystack = list(str(haystack))
    buff = str(needle)
    max_index = len(haystack) - len(buff)

    patterns = dict()

    begin, end, floor = 0, 0, 0
    while needle in haystack[floor:]:
        ### extract the keys
        hs = haystack[floor:]

        ### the first character of the keys
        ### MRSM{value1:value2}
        ###      ^
        begin = hs.find(needle) + len(needle) + len(begin_key)

        ### number of characters to end of keys
        ### (really it's the index of the beginning of the end_key relative to the beginning
        ###     but the math works out)
        ### MRSM{value1}
        ###      ^     ^  => 6
        length = hs[begin:].find(end_key)

        ### index of the end_key (end of `length` characters)
        end = begin + length

        ### advance the floor to find the next leading key
        floor += end + len(end_key)
        keys = hs[begin:end].split(delimiter)

        ### follow the pointers to the value
        c = config
        for k in keys:
            try:
                c = c[k]
            except KeyError:
                from meerschaum.utils.warnings import warn
                warn(f"Invalid keys in config: {keys}")
        value = c

        ### pattern to search and replace
        pattern = leading_key + begin_key + delimiter.join(keys) + end_key
        ### store patterns and values
        patterns[pattern] = value

    ### replace the patterns with the values
    for pattern, value in patterns.items():
        haystack = haystack.replace(pattern, str(value))

    ### parse back into dict
    return yaml.safe_load(haystack)

def edit_file(
        path : 'pathlib.Path',
        default_editor : str = 'pyvim',
        debug : bool = False
    ):
    """
    Open a file for editing. Attempts to use the user's defined EDITOR,
    otherwise uses pyvim.
    """
    import os
    from subprocess import call
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import run_python_package
    try:
        EDITOR = os.environ.get('EDITOR', default_editor)
        if debug: dprint(f"Opening file '{path}' with editor '{EDITOR}'") 
        call([EDITOR, path])
    except Exception as e: ### can't open with default editors
        if debug: dprint(e)
        if debug: dprint('Failed to open file with system editor. Falling back to pyvim...')
        run_python_package('pyvim', [path], debug=debug)

def parse_connector_keys(keys : str, construct : bool = True, **kw) -> 'meerschaum.connectors.Connector':
    """
    Parse connector keys and return Connector object
    """
    from meerschaum.connectors import get_connector
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import error
    keys = str(keys)
    vals = keys.split(':')
    if construct:
        conn = get_connector(type=vals[0], label=vals[1], **kw)
        if conn is None:
            error(f"Unable to parse connector keys '{keys}'", stack=False)
    else: conn = get_config('meerschaum', 'connectors', vals[0], vals[1])

    #  try:
        #  vals = keys.split(':')
        #  if construct: conn = get_connector(type=vals[0], label=vals[1], **kw)
        #  else: conn = get_config('meerschaum', 'connectors', vals[0], vals[1])
    #  except Exception as e:
        #  from meerschaum.utils.warnings import warn, error
        #  warn(str(e))
        #  return None
    return conn

def parse_instance_keys(keys : str, construct : bool = True, **kw):
    """
    Parse the Meerschaum instance value into a Connector object
    """
    from meerschaum.config import get_config
    if keys is None: keys = get_config('meerschaum', 'instance')
    keys = str(keys)
    if ':' not in keys: keys += ':'
    if keys.endswith(':'): keys += 'main'
    return parse_connector_keys(keys, construct=construct, **kw)

def parse_repo_keys(keys : str = None, **kw):
    """
    Parse the Meerschaum repository value into a Connector object
    """
    from meerschaum.config import get_config
    if keys is None: keys = get_config('meerschaum', 'default_repository', patch=True)
    keys = str(keys)
    if ':' not in keys: keys = 'api:' + keys
    return parse_connector_keys(keys, **kw)

def is_pipe_registered(
        pipe : 'Pipe or MetaPipe',
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

def get_subactions(action : str, globs : dict = None) -> list:
    """
    Return a list of function pointers to all subactions for a given action
    """
    if globs is None:
        import importlib
        module = importlib.import_module(f'meerschaum.actions._{action}')
        globs = vars(module)
    subactions = []
    for item in globs:
        if f'_{action}' in item:
            subactions.append(globs[item])
    return subactions

def choices_docstring(action : str, globs : dict = None):
    options_str = f"\n    Options:\n        `{action} "
    subactions = get_subactions(action, globs=globs)
    options_str += "["
    sa_names = []
    for sa in subactions:
        sa_names.append(sa.__name__[len(f"_{action}") + 1:])
    for sa_name in sorted(sa_names):
        options_str += f"{sa_name}, "
    options_str = options_str[:-2] + "]`"
    return options_str

def print_options(
        options : dict = {},
        nopretty : bool = False,
        name : str = 'options',
        header : str = None,
        **kw
    ) -> None:
    """
    Show available options from an iterable
    """
    from meerschaum.utils.formatting import make_header
    _options = []
    for o in options: _options.append(str(o))

    print()
    if not nopretty:
        if header is None: _header = f"Available {name}:"
        else: _header = header
        print(make_header(_header))
        ### calculate underline length
        #  underline_len = len(_header)
        #  for o in _options:
            #  if len(str(o)) + 4 > underline_len:
                #  underline_len = len(str(o)) + 4
        #  ### print underline
        #  for i in range(underline_len): print('-', end="")
        #  print("\n", end="")
    ### print actions
    for option in sorted(_options):
        if not nopretty: print("  - ", end="")
        print(option)

    print()

def sorted_dict(d : dict) -> dict:
    """
    Sort a dictionary's keys and values and return a new dictionary
    """
    try:
        return {key: value for key, value in sorted(d.items(), key=lambda item: item[1])}
    except:
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
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    import datetime
    if date_delta is None: date_delta = datetime.timedelta(minutes=1)
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
        if debug: dprint(f"df is not a DataFrame. Casting to DataFrame...")
        df = pd.DataFrame(df)

    ### skip parsing if DataFrame is empty
    if len(df) == 0:
        if debug: dprint(f"df is empty. Returning original DataFrame without casting datetime columns...")
        return df

    ### apply regex to columns to determine which are ISO datetimes
    iso_dt_regex = r'\d{4}-\d{2}-\d{2}.\d{2}\:\d{2}\:\d{2}'
    dt_mask = df.astype(str).apply(
        lambda s : s.str.match(iso_dt_regex).all()
    )

    ### list of datetime column names
    datetimes = list(df.loc[:, dt_mask])
    if debug: dprint("Converting columns to datetimes: " + str(datetimes))

    ### apply to_datetime
    df[datetimes] = df[datetimes].apply(pd.to_datetime)

    ### strip timezone information
    for dt in datetimes:
        df[dt] = df[dt].dt.tz_localize(None)

    return df

async def retry_connect(
        connector : 'meerschaum.connectors.SQLConnector or databases.Database' = None,
        max_retries : int = 40,
        retry_wait : int = 3,
        debug : bool = False,
    ):
    """
    Keep trying to connect to the database.
    Use wait_for_connection for non-async
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum import get_connector
    from meerschaum.connectors.sql import SQLConnector
    import time

    ### get default connector if None is provided
    if connector is None:
        connector = get_connector()

    database = connector
    if isinstance(connector, SQLConnector):
        database = connector.db


    retries = 0
    while retries < max_retries:
        if debug:
            dprint(f"Trying to connect to the database")
            dprint(f"Attempt ({retries + 1} / {max_retries})")
        try:
            await database.connect()

        except Exception as e:
            dprint(f"Connection failed. Retrying in {retry_wait} seconds...")
            time.sleep(retry_wait)
            retries += 1
        else:
            if debug: dprint("Connection established!")
            break

def wait_for_connection(**kw):
    """
    Block until a connection to the SQL database is made.
    """
    import asyncio
    asyncio.run(retry_connect(**kw))

def sql_item_name(s : str, flavor : str) -> str:
    """
    Parse SQL items depending on the flavor
    """
    if flavor in {'timescaledb', 'postgresql'}: s = pg_capital(s)
    elif flavor == 'sqlite': s = "\"" + s + "\""
    return s

def pg_capital(s : str) -> str:
    """
    If string contains a capital letter, wrap it in double quotes

    returns: string
    """
    if '"' in s: return s
    needs_quotes = False
    for c in str(s):
        if c.isupper():
            needs_quotes = True
            break
    if needs_quotes:
        return '"' + s + '"'
    return s

def df_from_literal(
        pipe : 'meerschaum.Pipe' = None,
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
        if debug: dprint(f"Received literal string: '{literal}'")
        import ast
        try:
            val = ast.literal_eval(literal)
        except:
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
    https://stackoverflow.com/questions/48647534/python-pandas-find-difference-between-two-data-frames#48647840

    Also, NaN apparently does not equal NaN, so I am temporarily replacing instances of NaN with a
    custom string, per this StackOverflow question:
    https://stackoverflow.com/questions/31833635/pandas-checking-for-nan-not-working-using-isin

    Lastly, use the old DataFrame's columns for the new DataFrame, because order matters when checking equality.
    """
    if old_df is None: return new_df
    old_cols = list(old_df.columns)
    try:
        new_df = new_df[old_cols]
    except Exception as e:
        warn(f"Was not able to cast old columns onto new DataFrame. Are both DataFrames the same shape? Error:\n{e}")
        return None

    ### assume the old_df knows what it's doing, even if it's technically wrong.
    if dtypes is None: dtypes = dict(old_df.dtypes)
    new_df = new_df.astype(dtypes)

    if len(old_df) == 0: return new_df

    return new_df[~new_df.fillna(custom_nan).apply(tuple, 1).isin(old_df.fillna(custom_nan).apply(tuple, 1))].reset_index(drop=True)

def change_dict(d : dict, func : 'function'):
    """
    Originally was local, moving to global for multiprocessing debugging
    """
    for k, v in d.items():
        if isinstance(v, dict):
            change_dict(v, func)
        else:
            d[k] = func(v)

def replace_pipes_in_dict(
        pipes : dict = None,
        func : 'function' = str,
        debug : bool = False,
        **kw
    ) -> dict:
    """
    Replace the Pipes in a Pipes dict with the result of another function
    """
    if pipes is None:
        from meerschaum import get_pipes
        pipes = get_pipes(debug=debug, **kw)

    result = pipes.copy()
    change_dict(result, func)
    return result

def build_where(parameters : dict):
    """
    Build the WHERE clause based on the input criteria
    """
    where = ""
    leading_and = "\n    AND "
    for key, value in parameters.items():
        ### search across a list (i.e. IN syntax)
        if isinstance(value, list):
            where += f"{leading_and}{key} IN ("
            for item in value:
                where += f"'{item}', "
            where = where[:-2] + ")"
            continue

        ### search a dictionary
        elif isinstance(value, dict):
            import json
            where += (f"{leading_and}CAST({key} AS TEXT) = '" + json.dumps(value) + "'")
            continue

        where += f"{leading_and}{key} " + ("IS NULL" if value is None else f"= '{value}'")
    if len(where) > 1: where = "\nWHERE\n    " + where[len(leading_and):]
    return where

def enforce_gevent_monkey_patch():
    """
    Check if gevent monkey patching is enabled, and if not, then apply patching
    """
    from meerschaum.utils.packages import attempt_import
    import socket
    gevent, gevent_socket, gevent_monkey = attempt_import('gevent', 'gevent.socket', 'gevent.monkey')
    if not socket.socket is gevent_socket.socket:
        gevent_monkey.patch_all()

def reload_plugins(debug : bool = False):
    """
    Convenience method for reloading the actions package (which loads plugins)
    """
    import meerschaum.actions
    from meerschaum.utils.packages import reload_package
    reload_package(meerschaum.actions, debug=debug)

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
        if c == '\n' and found_newline: break
        elif c == '\n':
            found_newline = True
            continue
        if found_newline:
            width += 1
    return width

def _pyinstaller_traverse_dir(
        directory : str,
        ignore_patterns : list = ['.pyc', 'dist', 'build', '.git', '.log'],
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
