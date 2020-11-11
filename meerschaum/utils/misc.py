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

def get_modules_from_package(
        package : 'package',
        names : bool = False,
        recursive : bool = False,
        lazy : bool = False,
        debug : bool = False
    ):
    """
    Find and import all modules in a package.

    Returns: either list of modules or tuple of lists
    
    names = False (default) : modules
    names = True            : (__all__, modules)
    """
    from meerschaum.utils.debug import dprint
    from os.path import dirname, join, isfile, isdir, basename
    import glob, importlib

    if recursive: pattern = '*'
    else: pattern = '*.py'
    module_names = glob.glob(join(dirname(package.__file__), pattern), recursive=recursive)
    _all = [
        basename(f)[:-3] if isfile(f) else basename(f)
            for f in module_names
                if (isfile(f) or isdir(f))
                    and not f.endswith('__init__.py')
                    and not f.endswith('__pycache__')
    ]

    if debug: dprint(_all)
    modules = []
    for module_name in [package.__name__ + "." + mod_name for mod_name in _all]:
        ### there's probably a better way than a try: catch but it'll do for now
        try:
            if lazy:
                modules.append(lazy_import(module_name))
            else:
                modules.append(importlib.import_module(module_name))
        except Exception as e:
            if debug: dprint(e)
            pass
    if names:
        return _all, modules
    return modules

def import_children(
        package : 'package' = None,
        package_name : str = None,
        types : list = ['method', 'builtin', 'function', 'class', 'module'],
        lazy : bool = True,
        recursive : bool = False,
        debug : bool = False
    ) -> list:
    """
    Import all functions in a package to its __init__.
    package : package (default None)
        Package to import its functions into.
        If None (default), use parent
    
    package_name : str (default None)
        Name of package to import its functions into
        If None (default), use parent

    types : list
        types of members to return.
        Default : ['method', 'builtin', 'class', 'function', 'package', 'module']

    Returns: list of members
    """
    import sys, inspect
    from meerschaum.utils.debug import dprint
    
    ### if package_name and package are None, use parent
    if package is None and package_name is None:
        package_name = inspect.stack()[1][0].f_globals['__name__']

    ### populate package or package_name from other other
    if package is None:
        package = sys.modules[package_name]
    elif package_name is None:
        package_name = package.__name__

    ### Set attributes in sys module version of package.
    ### Kinda like setting a dictionary
    ###   functions[name] = func
    modules = get_modules_from_package(package, recursive=recursive, lazy=lazy, debug=debug)
    _all, members = [], []
    for module in modules:
        objects = []
        for ob in inspect.getmembers(module):
            for t in types:
                ### ob is a tuple of (name, object)
                if getattr(inspect, 'is' + t)(ob[1]):
                    objects.append(ob)

        if 'module' in types:
            objects.append((module.__name__.split('.')[0], module))
    for ob in objects:
        setattr(sys.modules[package_name], ob[0], ob[1])
        _all.append(ob[0])
        members.append(ob[1])

    if debug: dprint(_all)
    ### set __all__ for import *
    setattr(sys.modules[package_name], '__all__', _all)
    return members

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
    ending = f" {wrappers[0]}" + "/".join(
                [ o.upper() if o.lower() == default.lower() else o.lower() for o in options ]
                ) + f"{wrappers[1]} "
    print(question, end=ending, flush=True)
    answer = input()
    if answer == "": answer = default
    return answer.lower() == options[0].lower()

def reload_package(
        package : 'package',
        lazy : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Recursively load a package's subpackages, even if they were not previously loaded
    """
    import os, types, importlib
    from meerschaum.utils.debug import dprint
    assert(hasattr(package, "__package__"))
    fn = package.__file__
    fn_dir = os.path.dirname(fn) + os.sep
    module_visit = {fn}
    del fn

    def reload_recursive_ex(module):
        ### forces import of lazily-imported modules
        module = importlib.import_module(module.__name__)
        importlib.reload(module)

        for module_child in get_modules_from_package(module, recursive=True, lazy=lazy):
            if isinstance(module_child, types.ModuleType) and hasattr(module_child, '__name__'):
                fn_child = getattr(module_child, "__file__", None)
                if (fn_child is not None) and fn_child.startswith(fn_dir):
                    if fn_child not in module_visit:
                        if debug: dprint(f"reloading: {fn_child} from {module}")
                        module_visit.add(fn_child)
                        reload_recursive_ex(module_child)

    return reload_recursive_ex(package)

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

def is_installed(
        name : str
    ) -> bool:
    """
    Check whether a package is installed.
    name : str
        Name of the package in question
    """
    import importlib.util
    return importlib.util.find_spec(name) is None

def attempt_import(
        *names : list,
        lazy : bool = True,
        warn : bool = True
    ) -> 'module or tuple of modules':
    """
    Raise a warning if packages are not installed; otherwise import and return modules.
    If lazy = True, return lazy-imported modules.

    Returns tuple of modules if multiple names are provided, else returns one module.

    Examples:
        pandas, sqlalchemy = attempt_import('pandas', 'sqlalchemy')
        pandas = attempt_import('pandas')
    """
    from meerschaum.utils.warnings import warn as warn_function
    import importlib, importlib.util

    modules = []
    for name in names:
        if importlib.util.find_spec(name) is None and warn:
            warn_function(
                (f"\n\nMissing package '{name}'; features will not work correctly. "
                f"\n\nRun `pip install {name}`.\n"),
                ImportWarning,
                stacklevel = 3
            )
            modules.append(None)
        else: ### package is installed but might not be available (e.g. virtualenv)
            ### determine the import method (lazy vs normal)
            if not lazy: import_method = importlib.import_module if not lazy else lazy_import
            try:
                mod = importlib.import_module(name)
            except:
                mod = None

            modules.append(mod)
    modules = tuple(modules)
    if len(modules) == 1: return modules[0]
    return modules

def lazy_import(
        name : str,
        local_name : str = None
    ):
    """
    Lazily import a package
    Uses the tensorflow LazyLoader implementation (Apache 2.0 License)
    """
    from meerschaum.utils.lazy_loader import LazyLoader
    if local_name is None:
        local_name = name
    return LazyLoader(local_name, globals(), name)

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
    try:
        EDITOR = os.environ.get('EDITOR', default_editor)
        if debug: dprint(f"Opening file '{path}' with editor '{EDITOR}'") 
        call([EDITOR, path])
    except Exception as e: ### can't open with default editors
        if debug: dprint(e)
        if debug: dprint('Failed to open file with system editor. Falling back to pyvim...')
        run_python_package('pyvim', [path])

def run_python_package(
        package_name : str,
        args : list = []
    ):
    """
    Runs an installed python package.
    E.g. Translates to `/usr/bin/python -m [package]`
    """
    import sys
    from subprocess import call
    command = [sys.executable, '-m', package_name] + args
    return call(command)

def parse_connector_keys(keys : str, **kw) -> 'meerschaum.connectors.Connector':
    """
    Parse connector keys and return Connector object
    """
    from meerschaum.connectors import get_connector
    try:
        vals = keys.split(':')
        conn = get_connector(type=vals[0], label=vals[1], **kw)
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(str(e))
        return None
    return conn

def parse_instance_keys(keys : str, **kw):
    """
    Parse the Meerschaum instance value into a Connector object
    """
    if ':' not in keys: keys += ':'
    if keys.endswith(':'): keys += 'main'
    return parse_connector_keys(keys)

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
        options={},
        nopretty=False,
        **kw
    ) -> None:
    """
    Show available options from an iterable
    """
    from meerschaum.actions import actions
    if not nopretty:
        header = "Available options:"
        print("\n" + header)
        ### calculate underline length
        underline_len = len(header)
        for a in actions:
            if len(a) + 4 > underline_len:
                underline_len = len(a) + 4
        ### print underline
        for i in range(underline_len): print('-', end="")
        print("\n", end="")
    ### print actions
    for action in sorted(actions):
        if not nopretty: print("  - ", end="")
        print(action)

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

def import_pandas() -> 'module':
    """
    Quality-of-life function to attempt to import the configured version of pandas
    """
    from meerschaum.config import get_config
    pandas_module_name = get_config('system', 'connectors', 'all', 'pandas', patch=True)
    ### NOTE: modin does NOT currently work!
    if pandas_module_name == 'modin':
        pandas_module_name = 'modin.pandas'
    return attempt_import(pandas_module_name)

def df_from_literal(
        pipe : 'meerschaum.Pipe' = None,
        literal : str = None,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Parse a literal (if nessary), and use a Pipe's column names to generate a DataFrame.
    """
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
    """
    return new_df[~new_df.fillna(custom_nan).apply(tuple, 1).isin(old_df.fillna(custom_nan).apply(tuple, 1))].reset_index(drop=True)

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

    def change_dict(d):
        for k, v in d.items():
            if isinstance(v, dict):
                change_dict(v)
            else:
                d[k] = func(v)
    result = pipes.copy()
    change_dict(result)
    return result

