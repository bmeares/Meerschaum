#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Miscellaneous functions go here
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Mapping, Any, Callable, Optional, List, Dict, SuccessTuple, Iterable, PipesDict, Tuple,
    InstanceConnector, Hashable
)
import meerschaum as mrsm

def add_method_to_class(
        func: Callable[[Any], Any],
        class_def: 'Class',
        method_name: Optional[str] = None,
        keep_self: Optional[bool] = None,
    ) -> Callable[[Any], Any]:
    """
    Add function `func` to class `class_def`.

    Parameters
    ----------
    func: Callable[[Any], Any]
        Function to be added as a method of the class

    class_def: Class
        Class to be modified.

    method_name: Optional[str], default None
        New name of the method. None will use func.__name__ (default).

    Returns
    -------
    The modified function object.

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
        action: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Given a dictionary of options and the standard Meerschaum actions list,
    check if choice is valid and execute chosen function, else show available
    options and return False

    Parameters
    ----------
    action: Optional[List[str]], default None
        A list of subactions (e.g. `show pipes` -> ['pipes']).

    options: Optional[Dict[str, Any]], default None
        Available options to execute.
        option (key) -> function (value)
        Functions must accept **kw keyword arguments
        and return a tuple of success bool and message.

    Returns
    -------
    The return value of the chosen subaction (assumed to be a `SuccessTuple`).

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
        return (False, f"Invalid choice '{choice}'.")
    ### remove parent sub-action
    kw['action'] = list(action)
    del kw['action'][0]
    return options[parsed_choice](**kw)


def generate_password(length: int = 12) -> str:
    """Generate a secure password of given length.

    Parameters
    ----------
    length : int, default 12
        The length of the password.

    Returns
    -------
    A random password string.

    """
    import secrets, string
    return ''.join((secrets.choice(string.ascii_letters) for i in range(length)))

def is_int(s : str) -> bool:
    """
    Check if string is an int.

    Parameters
    ----------
    s: str
        The string to be checked.
        
    Returns
    -------
    A bool indicating whether the string was able to be cast to an integer.

    """
    try:
        float(s)
    except Exception as e:
        return False
    
    return float(s).is_integer()


def string_to_dict(
        params_string: str
    ) -> Dict[str, Any]:
    """
    Parse a string into a dictionary.
    
    If the string begins with '{', parse as JSON. Otherwise use simple parsing.

    Parameters
    ----------
    params_string: str
        The string to be parsed.
        
    Returns
    -------
    The parsed dictionary.

    Examples
    --------
    >>> string_to_dict("a:1,b:2") 
    {'a': 1, 'b': 2}
    >>> string_to_dict('{"a": 1, "b": 2}')
    {'a': 1, 'b': 2}

    """
    if params_string == "":
        return {}

    import json

    ### Kind of a weird edge case.
    ### In the generated compose file, there is some weird escaping happening,
    ### so the string to be parsed starts and ends with a single quote.
    if (
        isinstance(params_string, str)
        and len(params_string) > 4
        and params_string[1] == "{"
        and params_string[-2] == "}"
    ):
        return json.loads(params_string[1:-1])
    if str(params_string).startswith('{'):
        return json.loads(params_string)

    import ast
    params_dict = {}
    for param in params_string.split(","):
        _keys = param.split(":")
        keys = _keys[:-1]
        try:
            val = ast.literal_eval(_keys[-1])
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
        value: str,
        leading_key: str = 'MRSM',
        begin_key: str = '{',
        end_key: str = '}',
        delimeter: str = ':'
    ) -> List[Any]:
    """
    Parse Meerschaum substitution syntax
    E.g. MRSM{value1:value2} => ['value1', 'value2']
    NOTE: Not currently used. See `search_and_substitute_config` in `meerschaum.config._read_yaml`.
    """
    if not value.beginswith(leading_key):
        return value

    return leading_key[len(leading_key):][len():-1].split(delimeter)

def edit_file(
        path: Union[pathlib.Path, str],
        default_editor: str = 'pyvim',
        debug: bool = False
    ) -> bool:
    """
    Open a file for editing.
    
    Attempt to launch the user's defined `$EDITOR`, otherwise use `pyvim`.

    Parameters
    ----------
    path: Union[pathlib.Path, str]
        The path to the file to be edited.
        
    default_editor: str, default 'pyvim'
        If `$EDITOR` is not set, use this instead.
        If `pyvim` is not installed, it will install it from PyPI.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating the file was successfully edited.

    """
    import os
    from subprocess import call
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import run_python_package, attempt_import, package_venv
    try:
        EDITOR = os.environ.get('EDITOR', default_editor)
        if debug:
            dprint(f"Opening file '{path}' with editor '{EDITOR}'...")
        rc = call([EDITOR, path])
    except Exception as e: ### can't open with default editors
        if debug:
            dprint(e)
            dprint('Failed to open file with system editor. Falling back to pyvim...')
        pyvim = attempt_import('pyvim', lazy=False)
        rc = run_python_package('pyvim', [path], venv=package_venv(pyvim), debug=debug)
    return rc == 0


def is_pipe_registered(
        pipe: 'meerschaum.Pipe',
        pipes: PipesDict,
        debug: bool = False
    ) -> bool:
    """
    Check if a Pipe is inside the pipes dictionary.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to see if it's in the dictionary.
        
    pipes: PipesDict
        The dictionary to search inside.
        
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating whether the pipe is inside the dictionary.
    """
    from meerschaum.utils.debug import dprint
    ck, mk, lk = pipe.connector_keys, pipe.metric_key, pipe.location_key
    if debug:
        dprint(f'{ck}, {mk}, {lk}')
        dprint(f'{pipe}, {pipes}')
    return ck in pipes and mk in pipes[ck] and lk in pipes[ck][mk]


def _get_subaction_names(action : str, globs : dict = None) -> List[str]:
    """NOTE: Don't use this function. You should use `meerschaum.actions.get_subactions()` instead.
    This only exists for internal use.
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


def choices_docstring(action: str, globs : Optional[Dict[str, Any]] = None) -> str:
    """
    Append the an action's available options to the module docstring.
    This function is to be placed at the bottom of each action module.

    Parameters
    ----------
    action: str
        The name of the action module (e.g. 'install').
        
    globs: Optional[Dict[str, Any]], default None
        An optional dictionary of global variables.

    Returns
    -------
    The generated docstring for the module.

    Examples
    --------
    >>> from meerschaum.utils.misc import choices_docstring as _choices_docstring
    >>> install.__doc__ += _choices_docstring('install')

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
        options: Optional[Dict[str, Any]] = None,
        nopretty: bool = False,
        no_rich: bool = False,
        name: str = 'options',
        header: Optional[str] = None,
        num_cols: Optional[int] = None,
        adjust_cols: bool = True,
        **kw
    ) -> None:
    """
    Print items in an iterable as a fancy table.

    Parameters
    ----------
    options: Optional[Dict[str, Any]], default None
        The iterable to be printed.

    nopretty: bool, default False
        If `True`, don't use fancy formatting.

    no_rich: bool, default False
        If `True`, don't use `rich` to format the output.

    name: str, default 'options'
        The text in the default header after `'Available'`.

    header: Optional[str], default None
        If provided, override `name` and use this as the header text.

    num_cols: Optional[int], default None
        How many columns in the table. Depends on the terminal size. If `None`, use 8.

    adjust_cols: bool, default True
        If `True`, adjust the number of columns depending on the terminal size.

    """
    import os
    from meerschaum.utils.packages import import_rich
    from meerschaum.utils.formatting import make_header, highlight_pipes
    from meerschaum.actions import actions as _actions


    if options is None:
        options = {}
    _options = []
    for o in options:
        _options.append(str(o))
    _header = f"Available {name}" if header is None else header

    if num_cols is None:
        num_cols = 8

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

    ### Prevent too many options from being truncated on small terminals.
    if adjust_cols and _options:
        _cols, _lines = get_cols_lines()
        while num_cols > 1:
            cell_len = int(((_cols - 4) - (3 * (num_cols - 1))) / num_cols)
            num_too_big = sum([(1 if string_width(o) > cell_len else 0) for o in _options])
            if num_too_big > int(len(_options) / 3):
                num_cols -= 1
                continue
            break

    from meerschaum.utils.formatting import pprint, get_console
    from meerschaum.utils.packages import attempt_import
    rich_columns = attempt_import('rich.columns')
    rich_panel = attempt_import('rich.panel')
    rich_table = attempt_import('rich.table')
    Text = attempt_import('rich.text').Text
    box = attempt_import('rich.box')
    Panel = rich_panel.Panel
    Columns = rich_columns.Columns
    Table = rich_table.Table

    if _header is not None:
        table = Table(
            title = '\n' + _header,
            box = box.SIMPLE,
            show_header = False,
            show_footer = False,
            title_style = '',
            expand = True,
        )
    else:
        table = Table.grid(padding=(0, 2))
    for i in range(num_cols):
        table.add_column()

    chunks = iterate_chunks(
        [Text.from_ansi(highlight_pipes(o)) for o in sorted(_options)],
        num_cols,
        fillvalue=''
    )
    for c in chunks:
        table.add_row(*c)

    get_console().print(table)
    return None


def get_cols_lines(default_cols: int = 100, default_lines: int = 120) -> Tuple[int, int]:
    """
    Determine the columns and lines in the terminal.
    If they cannot be determined, return the default values (100 columns and 120 lines).

    Parameters
    ----------
    default_cols: int, default 100
        If the columns cannot be determined, return this value.

    default_lines: int, default 120
        If the lines cannot be determined, return this value.

    Returns
    -------
    A tuple if integers for the columns and lines.
    """
    import os
    try:
        size = os.get_terminal_size()
        _cols, _lines = size.columns, size.lines
    except Exception as e:
        _cols, _lines = (
            int(os.environ.get('COLUMNS', str(default_cols))),
            int(os.environ.get('LINES', str(default_lines))),
        )
    return _cols, _lines


def iterate_chunks(iterable, chunksize: int, fillvalue: Optional[Any] = None):
    """
    Iterate over a list in chunks.
    https://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks

    Parameters
    ----------
    iterable: Iterable[Any]
        The iterable to iterate over in chunks.
        
    chunksize: int
        The size of chunks to iterate with.
        
    fillvalue: Optional[Any], default None
        If the chunks do not evenly divide into the iterable, pad the end with this value.

    Returns
    -------
    A generator of tuples of size `chunksize`.

    """
    from itertools import zip_longest
    args = [iter(iterable)] * chunksize
    return zip_longest(*args, fillvalue=fillvalue)

def sorted_dict(d: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Sort a dictionary's values and return a new dictionary.

    Parameters
    ----------
    d: Dict[Any, Any]
        The dictionary to be sorted.

    Returns
    -------
    A sorted dictionary.

    Examples
    --------
    >>> sorted_dict({'b': 1, 'a': 2})
    {'b': 1, 'a': 2}
    >>> sorted_dict({'b': 2, 'a': 1})
    {'a': 1, 'b': 2}

    """
    try:
        return {key: value for key, value in sorted(d.items(), key=lambda item: item[1])}
    except Exception as e:
        return d

def flatten_pipes_dict(pipes_dict: PipesDict) -> List[Pipe]:
    """
    Convert the standard pipes dictionary into a list.

    Parameters
    ----------
    pipes_dict: PipesDict
        The pipes dictionary to be flattened.

    Returns
    -------
    A list of `Pipe` objects.

    """
    pipes_list = []
    for ck in pipes_dict.values():
        for mk in ck.values():
            pipes_list += list(mk.values())
    return pipes_list


def round_time(
        dt: Optional['datetime.datetime'] = None,
        date_delta: Optional['datetime.timedelta'] = None,
        to: 'str' = 'down'
    ) -> 'datetime.datetime':
    """
    Round a datetime object to a multiple of a timedelta.
    http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python

    NOTE: This function strips timezone information!

    Parameters
    ----------
    dt: 'datetime.datetime', default None
        If `None`, grab the current UTC datetime.

    date_delta: 'datetime.timedelta', default None
        If `None`, use a delta of 1 minute.

    to: 'str', default 'down'
        Available options are `'up'`, `'down'`, and `'closest'`.

    Returns
    -------
    A rounded `datetime.datetime` object.

    Examples
    --------
    >>> round_time(datetime.datetime(2022, 1, 1, 12, 15, 57, 200))
    datetime.datetime(2022, 1, 1, 12, 15)
    >>> round_time(datetime.datetime(2022, 1, 1, 12, 15, 57, 200), to='up')
    datetime.datetime(2022, 1, 1, 12, 16)
    >>> round_time(datetime.datetime(2022, 1, 1, 12, 15, 57, 200), datetime.timedelta(hours=1))
    datetime.datetime(2022, 1, 1, 12, 0)
    >>> round_time(
    ...   datetime.datetime(2022, 1, 1, 12, 15, 57, 200),
    ...   datetime.timedelta(hours=1),
    ...   to='closest'
    ... )
    datetime.datetime(2022, 1, 1, 12, 0)
    >>> round_time(
    ...   datetime.datetime(2022, 1, 1, 12, 45, 57, 200),
    ...   datetime.timedelta(hours=1),
    ...   to='closest'
    ... )
    datetime.datetime(2022, 1, 1, 13, 0)

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
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)


def parse_df_datetimes(
        df: 'pd.DataFrame',
        debug: bool = False
    ) -> 'pd.DataFrame':
    """
    Parse a pandas DataFrame for datetime columns and cast as datetimes.

    Parameters
    ----------
    df: pd.DataFrame
        The pandas DataFrame to parse.
        
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A new pandas DataFrame with the determined datetime columns (usually ISO strings) cast as datetimes.

    Examples
    --------
    ```python
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': ['2022-01-01 00:00:00']}) 
    >>> df.dtypes
    a    object
    dtype: object
    >>> df = parse_df_datetimes(df)
    >>> df.dtypes
    a    datetime64[ns]
    dtype: object

    ```

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
    iso_dt_regex = r'\d{4}-\d{2}-\d{2}.\d{2}\:\d{2}\:\d+'
    dt_mask = df.astype(str).apply(
        lambda s: s.str.match(iso_dt_regex).all()
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
        seconds: int = 10,
        timeout_message: str = "",
        prompt: str = "",
        icon: bool = False,
        **kw
    ) -> Union[str, None]:
    """
    Accept user input only for a brief period of time.

    Parameters
    ----------
    seconds: int, default 10
        The number of seconds to wait.

    timeout_message: str, default ''
        The message to print after the window has elapsed.

    prompt: str, default ''
        The prompt to print during the window.

    icon: bool, default False
        If `True`, print the configured input icon.


    Returns
    -------
    The input string entered by the user.

    """
    import signal, time

    class TimeoutExpired(Exception):
        """Raise this exception when the timeout is reached."""

    def alarm_handler(signum, frame):
        raise TimeoutExpired

    # set signal handler
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(seconds) # produce SIGALRM in `timeout` seconds

    try:
        return input(prompt)
    except TimeoutExpired:
        return None
    except EOFError:
        try:
            print(prompt)
            time.sleep(seconds)
        except TimeoutExpired:
            return None
    finally:
        signal.alarm(0) # cancel alarm


def df_from_literal(
        pipe: Optional['meerschaum.Pipe'] = None,
        literal: str = None,
        debug: bool = False
    ) -> 'pd.DataFrame':
    """
    Construct a dataframe from a literal value, using the pipe's datetime and value column names.

    Parameters
    ----------
    pipe: Optional['meerschaum.Pipe'], default None
        The pipe which will consume the literal value.

    literal : str :
         (Default value = None)
    debug : bool :
         (Default value = False)

    Returns
    -------
    A 1-row pandas DataFrame from with the current UTC timestamp as the datetime columns and the literal as the value.

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


def add_missing_cols_to_df(df: 'pd.DataFrame', dtypes: Dict[str, Any]) -> pd.DataFrame:
    """
    Add columns from the dtypes dictionary as null columns to a new DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe we should copy and add null columns.

    dtypes:
        The data types dictionary which may contain keys not present in `df.columns`.

    Returns
    -------
    A new `DataFrame` with the keys from `dtypes` added as null columns.
    If `df.dtypes` is the same as `dtypes`, then return a reference to `df`.
    NOTE: This will not ensure that dtypes are enforced!

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame([{'a': 1}])
    >>> dtypes = {'b': 'Int64'}
    >>> add_missing_cols_to_df(df, dtypes)
       a     b
       0  1  <NA>
    >>> add_missing_cols_to_df(df, dtypes).dtypes
    a    int64
    b    Int64
    dtype: object
    >>> add_missing_cols_to_df(df, {'a': 'object'}).dtypes
    a    int64
    dtype: object
    >>> 
    """
    if set(df.columns) == set(dtypes):
        return df

    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    
    df = df.copy()
    for col, typ in dtypes.items():
        if col in df.columns:
            continue
        if typ == 'int64':
            typ = 'Int64'
        df[col] = pd.Series([None] * len(df), dtype=typ)
    
    return df


def filter_unseen_df(
        old_df: 'pd.DataFrame',
        new_df: 'pd.DataFrame',
        primary_key_col: Optional[str] = None,
        dtypes: Optional[Dict[str, Any]] = None,
        custom_nan: str = 'mrsm_NaN',
        debug: bool = False,
    ) -> 'pd.DataFrame':
    """
    Left join two DataFrames to find the newest unseen data.

    Parameters
    ----------
    old_df: 'pd.DataFrame'
        The original (target) dataframe. Acts as a filter on the `new_df`.
        
    new_df: 'pd.DataFrame'
        The fetched (source) dataframe. Rows that are contained in `old_df` are removed.
        
    dtypes: Optional[Dict[str, Any]], default None
        Optionally specify the datatypes of the dataframe.

    custom_nan: str, default 'mrsm_NaN'
        Fill in `NaN` cells with this string during filtering (later replaced with `NaN`).

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pandas dataframe of the new, unseen rows in `new_df`.

    Examples
    --------
    ```python
    >>> import pandas as pd
    >>> df1 = pd.DataFrame({'a': [1,2]})
    >>> df2 = pd.DataFrame({'a': [2,3]})
    >>> filter_unseen_df(df1, df2)
       a
    0  3

    ```

    """
    if old_df is None:
        return new_df

    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas(debug=debug)

    new_df_dtypes = dict(new_df.dtypes)
    old_df_dtypes = dict(old_df.dtypes)

    same_cols = set(new_df.columns) == set(old_df.columns)
    if not same_cols:
        new_df = add_missing_cols_to_df(new_df, old_df_dtypes)
        old_df = add_missing_cols_to_df(old_df, new_df_dtypes)

    try:
        ### Order matters when checking equality.
        new_df = new_df[old_df.columns]
    except Exception as e:
        warn(
            "Was not able to cast old columns onto new DataFrame. " +
            f"Are both DataFrames the same shape? Error:\n{e}",
            stacklevel = 3,
        )
        return new_df[list(new_df_dtypes.keys())]

    ### assume the old_df knows what it's doing, even if it's technically wrong.
    if dtypes is None:
        dtypes = {col: str(typ) for col, typ in old_df.dtypes.items()}

    dtypes = {
        col: (
            (
                str(typ)
                if str(typ) not in ('str', 'json')
                else 'object'
            )
            if str(typ) != 'int64'
            else 'Int64'
        ) for col, typ in dtypes.items()
        if col in new_df_dtypes and col in old_df_dtypes
    }
    for col, typ in new_df_dtypes.items():
        if col not in dtypes:
            dtypes[col] = typ
    
    for col, typ in {k: v for k, v in dtypes.items()}.items():
        if new_df_dtypes.get(col, None) != old_df_dtypes.get(col, None):
            ### Fallback to object if the types don't match.
            dtypes[col] = 'object'

    cast_cols = True
    try:
        new_df = new_df.astype(dtypes)
        cast_cols = False
    except Exception as e:
        warn(
            f"Was not able to cast the new DataFrame to the given dtypes.\n{e}"
        )
    if cast_cols:
        for col, dtype in dtypes.items():
            if col in new_df.columns:
                try:
                    new_df[col] = new_df[col].astype(dtype)
                except Exception as e:
                    warn(f"Was not able to cast column '{col}' to dtype '{dtype}'.\n{e}")

    return new_df[
        ~new_df.fillna(pd.NA).apply(tuple, 1).isin(old_df.fillna(pd.NA).apply(tuple, 1))
    ].reset_index(drop=True)[new_df_dtypes.keys()]


def replace_pipes_in_dict(
        pipes : Optional[PipesDict] = None,
        func: 'function' = str,
        debug: bool = False,
        **kw
    ) -> PipesDict:
    """
    Replace the Pipes in a Pipes dict with the result of another function.

    Parameters
    ----------
    pipes: Optional[PipesDict], default None
        The pipes dict to be processed.

    func: Callable[[Any], Any], default str
        The function to be applied to every pipe.
        Defaults to the string constructor.

    debug: bool, default False
        Verbosity toggle.
    

    Returns
    -------
    A dictionary where every pipe is replaced with the output of a function.

    """
    import copy
    def change_dict(d : Dict[Any, Any], func : 'function') -> None:
        for k, v in d.items():
            if isinstance(v, dict):
                change_dict(v, func)
            else:
                d[k] = func(v)

    if pipes is None:
        from meerschaum import get_pipes
        pipes = get_pipes(debug=debug, **kw)

    result = copy.deepcopy(pipes)
    change_dict(result, func)
    return result

def enforce_gevent_monkey_patch():
    """
    Check if gevent monkey patching is enabled, and if not, then apply patching.
    """
    from meerschaum.utils.packages import attempt_import
    import socket
    gevent, gevent_socket, gevent_monkey = attempt_import(
        'gevent', 'gevent.socket', 'gevent.monkey'
    )
    if not socket.socket is gevent_socket.socket:
        gevent_monkey.patch_all()

def is_valid_email(email: str) -> Union['re.Match', None]:
    """
    Check whether a string is a valid email.

    Parameters
    ----------
    email: str
        The string to be examined.
        
    Returns
    -------
    None if a string is not in email format, otherwise a `re.Match` object, which is truthy.

    Examples
    --------
    >>> is_valid_email('foo')
    >>> is_valid_email('foo@foo.com')
    <re.Match object; span=(0, 11), match='foo@foo.com'>

    """
    import re
    regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    return re.search(regex, email)

def string_width(string: str, widest: bool = True) -> int:
    """
    Calculate the width of a string, either by its widest or last line.

    Parameters
    ----------
    string: str:
        The string to be examined.
        
    widest: bool, default True
        No longer used because `widest` is always assumed to be true.

    Returns
    -------
    An integer for the text's visual width.

    Examples
    --------
    >>> string_width('a')
    1
    >>> string_width('a\nbc\nd')
    2

    """
    def _widest():
        words = string.split('\n')
        max_length = 0
        for w in words:
            length = len(w)
            if length > max_length:
                max_length = length
        return max_length

    return _widest()

def _pyinstaller_traverse_dir(
        directory: str,
        ignore_patterns: Iterable[str] = ('.pyc', 'dist', 'build', '.git', '.log'),
        include_dotfiles: bool = False
    ) -> list:
    """
    Recursively traverse a directory and return a list of its contents.
    """
    import os, pathlib
    paths = []
    _directory = pathlib.Path(directory)

    def _found_pattern(name: str):
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


def replace_password(d: Dict[str, Any], replace_with: str = '*') -> Dict[str, Any]:
    """
    Recursively replace passwords in a dictionary.

    Parameters
    ----------
    d: Dict[str, Any]
        The dictionary to search through.

    replace_with: str, default '*'
        The string to replace each character of the password with.

    Returns
    -------
    Another dictionary where values to the keys `'password'`
    are replaced with `replace_with` (`'*'`).

    Examples
    --------
    >>> replace_password({'a': 1})
    {'a': 1}
    >>> replace_password({'password': '123'})
    {'password': '***'}
    >>> replace_password({'nested': {'password': '123'}})
    {'nested': {'password': '***'}}
    >>> replace_password({'password': '123'}, replace_with='!')
    {'password': '!!!'}

    """
    import copy
    _d = copy.deepcopy(d)
    for k, v in d.items():
        if isinstance(v, dict):
            _d[k] = replace_password(v)
        elif 'password' in str(k).lower():
            _d[k] = ''.join([replace_with for char in str(v)])
        elif str(k).lower() == 'uri':
            from meerschaum.connectors.sql import SQLConnector
            try:
                uri_params = SQLConnector.parse_uri(v)
            except Exception as e:
                uri_params = None
            if not uri_params:
                continue
            if not 'username' in uri_params or not 'password' in uri_params:
                continue
            _d[k] = v.replace(
                uri_params['username'] + ':' + uri_params['password'],
                uri_params['username'] + ':' + ''.join(
                    [replace_with for char in str(uri_params['password'])]
                )
            )
    return _d


def filter_keywords(
        func: Callable[[Any], Any],
        **kw: Any
    ) -> Dict[str, Any]:
    """
    Filter out unsupported keywords.

    Parameters
    ----------
    func: Callable[[Any], Any]
        The function to inspect.
        
    **kw: Any
        The arguments to be filtered and passed into `func`.

    Returns
    -------
    A dictionary of keyword arguments accepted by `func`.
    
    Examples
    --------
    ```python
    >>> def foo(a=1, b=2):
    ...     return a * b
    >>> filter_keywords(foo, a=2, b=4, c=6)
    {'a': 2, 'b': 4}
    >>> foo(**filter_keywords(foo, **{'a': 2, 'b': 4, 'c': 6}))
    8
    ```

    """
    import inspect
    func_params = inspect.signature(func).parameters
    ### If the function has a **kw method, skip filtering.
    for param, _type in func_params.items():
        if '**' in str(_type):
            return kw
    return {k: v for k, v in kw.items() if k in func_params}


def dict_from_od(od: collections.OrderedDict) -> Dict[Any, Any]:
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

    Parameters
    ----------
    s: str:
        The string to be cleaned.

    Returns
    -------
    A string with the ANSI characters removed.

    Examples
    --------
    >>> remove_ansi("\x1b[1;31mHello, World!\x1b[0m")
    'Hello, World!'

    """
    import re
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', s)


def get_connector_labels(
        *types: str,
        search_term: str = '',
        ignore_exact_match = True,
    ) -> List[str]:
    """
    Read connector labels from the configuration dictionary.

    Parameters
    ----------
    *types: str
        The connector types.
        If none are provided, use the defined types (`'sql'` and `'api'`) and `'plugin'`.

    search_term: str, default ''
        A filter on the connectors' labels.

    ignore_exact_match: bool, default True
        If `True`, skip a connector if the search_term is an exact match.

    Returns
    -------
    A list of the keys of defined connectors.

    """
    from meerschaum.config import get_config
    connectors = get_config('meerschaum', 'connectors')

    _types = list(types)
    if len(_types) == 0:
        _types = list(connectors.keys()) + ['plugin']

    conns = []
    for t in _types:
        if t == 'plugin':
            from meerschaum.plugins import get_data_plugins
            conns += [
                f'{t}:' + plugin.module.__name__.split('.')[-1]
                for plugin in get_data_plugins()
            ]
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


def json_serialize_datetime(dt: 'datetime.datetime') -> Union[str, None]:
    """
    Serialize a datetime.datetime object into JSON (ISO format string).
    
    Examples
    --------
    >>> import json, datetime
    >>> json.dumps({'a': datetime.datetime(2022, 1, 1)}, default=json_serialize_datetime)
    '{"a": "2022-01-01T00:00:00Z"}'

    """
    import datetime

    if isinstance(dt, datetime.datetime):
        return dt.isoformat() + 'Z'
    return None


def wget(
        url: str,
        dest: Optional[Union[str, 'pathlib.Path']] = None,
        headers: Optional[Dict[str, Any]] = None,
        color: bool = True,
        debug: bool = False,
        **kw: Any
    ) -> 'pathlib.Path':
    """
    Mimic `wget` with `requests`.

    Parameters
    ----------
    url: str
        The URL to the resource to be downloaded.
        
    dest: Optional[Union[str, pathlib.Path]], default None
        The destination path of the downloaded file.
        If `None`, save to the current directory.

    color: bool, default True
        If `debug` is `True`, print color output.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    The path to the downloaded file.

    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.debug import dprint
    import os, pathlib, re, urllib.request
    if headers is None:
        headers = {}
    request = urllib.request.Request(url, headers=headers)
    if not color:
        dprint = print
    if debug:
        dprint(f"Downloading from '{url}'...")
    try:
        response = urllib.request.urlopen(request)
    except Exception as e:
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context
        try:
            response = urllib.request.urlopen(request)
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

def debug_trace(browser: bool = True):
    """
    Open a web-based debugger to trace the execution of the program.
    """
    from meerschaum.utils.packages import attempt_import
    heartrate = attempt_import('heartrate')
    heartrate.trace(files=heartrate.files.all, browser=browser)

def items_str(
        items: List[Any],
        quotes: bool = True,
        quote_str: str = "'",
        commas: bool = True,
        comma_str: str = ',',
        and_: bool = True,
        and_str: str = 'and',
        oxford_comma: bool = True,
        spaces: bool = True,
        space_str = ' ',
    ) -> str:
    """
    Return a formatted string if list items separated by commas.

    Parameters
    ----------
    items: [List[Any]]
        The items to be printed as an English list.

    quotes: bool, default True
        If `True`, wrap items in quotes.

    quote_str: str, default "'"
        If `quotes` is `True`, prepend and append each item with this string.

    and_: bool, default True
        If `True`, include the word 'and' before the final item in the list.

    and_str: str, default 'and'
        If `and_` is True, insert this string where 'and' normally would in and English list.

    oxford_comma: bool, default True
        If `True`, include the Oxford Comma (comma before the final 'and').
        Only applies when `and_` is `True`.

    spaces: bool, default True
        If `True`, separate items with `space_str`

    space_str: str, default ' '
        If `spaces` is `True`, separate items with this string.

    Returns
    -------
    A string of the items as an English list.

    Examples
    --------
    >>> items_str([1,2,3])
    "'1', '2', and '3'"
    >>> items_str([1,2,3], quotes=False)
    '1, 2, and 3'
    >>> items_str([1,2,3], and_=False)
    "'1', '2', '3'"
    >>> items_str([1,2,3], spaces=False, and_=False)
    "'1','2','3'"
    >>> items_str([1,2,3], oxford_comma=False)
    "'1', '2' and '3'"
    >>> items_str([1,2,3], quote_str=":")
    ':1:, :2:, and :3:'
    >>> items_str([1,2,3], and_str="or")
    "'1', '2', or '3'"
    >>> items_str([1,2,3], space_str="_")
    "'1',_'2',_and_'3'"

    """
    if not items:
        return ''
    
    q = quote_str if quotes else ''
    s = space_str if spaces else ''
    a = and_str if and_ else ''
    c = comma_str if commas else ''

    if len(items) == 1:
        return q + str(list(items)[0]) + q

    if len(items) == 2:
        return q + str(list(items)[0]) + q + s + a + s + q + str(list(items)[1]) + q

    sep = q + c + s + q
    output = q + sep.join(str(i) for i in items[:-1]) + q
    if oxford_comma:
        output += c
    output += s + a + (s if and_ else '') + q + str(items[-1]) + q
    return output


def is_docker_available() -> bool:
    """Check if we can connect to the Docker engine."""
    import subprocess
    try:
        has_docker = subprocess.call(
            ['docker', 'ps'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        ) == 0
    except Exception as e:
        has_docker = False
    return has_docker


def get_last_n_lines(file_name: str, N: int):
    """
    https://thispointer.com/python-get-last-n-lines-of-a-text-file-like-tail-command/
    """
    import os
    # Create an empty list to keep the track of last N lines
    list_of_lines = []
    # Open file for reading in binary mode
    with open(file_name, 'rb') as read_obj:
        # Move the cursor to the end of the file
        read_obj.seek(0, os.SEEK_END)
        # Create a buffer to keep the last read line
        buffer = bytearray()
        # Get the current position of pointer i.e eof
        pointer_location = read_obj.tell()
        # Loop till pointer reaches the top of the file
        while pointer_location >= 0:
            # Move the file pointer to the location pointed by pointer_location
            read_obj.seek(pointer_location)
            # Shift pointer location by -1
            pointer_location = pointer_location -1
            # read that byte / character
            new_byte = read_obj.read(1)
            # If the read byte is new line character then it means one line is read
            if new_byte == b'\n':
                # Save the line in list of lines
                list_of_lines.append(buffer.decode()[::-1])
                # If the size of list reaches N, then return the reversed list
                if len(list_of_lines) == N:
                    return list(reversed(list_of_lines))
                # Reinitialize the byte array to save next line
                buffer = bytearray()
            else:
                # If last read character is not eol then add it in buffer
                buffer.extend(new_byte)
        # As file is read completely, if there is still data in buffer, then its first line.
        if len(buffer) > 0:
            list_of_lines.append(buffer.decode()[::-1])
    # return the reversed list
    return list(reversed(list_of_lines))


def tail(f, n, offset=None):
    """
    https://stackoverflow.com/a/692616/9699829
    
    Reads n lines from f with an offset of offset lines.  The return
    value is a tuple in the form ``(lines, has_more)`` where `has_more` is
    an indicator that is `True` if there are more lines in the file.
    """
    avg_line_length = 74
    to_read = n + (offset or 0)

    while True:
        try:
            f.seek(-(avg_line_length * to_read), 2)
        except IOError:
            # woops.  apparently file is smaller than what we want
            # to step back, go to the beginning instead
            f.seek(0)
        pos = f.tell()
        lines = f.read().splitlines()
        if len(lines) >= to_read or pos == 0:
            return lines[-to_read:offset and -offset or None], \
                   len(lines) > to_read or pos > 0
        avg_line_length *= 1.3


def truncate_string_sections(item: str, delimeter: str = '_', max_len: int = 128) -> str:
    """
    Remove characters from each section of a string until the length is within the limit.

    Parameters
    ----------
    item: str
        The item name to be truncated.

    delimeter: str, default '_'
        Split `item` by this string into several sections.

    max_len: int, default 128
        The max acceptable length of the truncated version of `item`.

    Returns
    -------
    The truncated string.

    Examples
    --------
    >>> truncate_string_sections('abc_def_ghi', max_len=10)
    'ab_de_gh'

    """
    if len(item) < max_len:
        return item

    def _shorten(s: str) -> str:
        return s[:-1] if len(s) > 1 else s

    sections = list(enumerate(item.split('_')))
    sorted_sections = sorted(sections, key=lambda x: (-1 * len(x[1])))
    available_chars = max_len - len(sections)

    _sections = [(i, s) for i, s in sorted_sections]
    _sections_len = sum([len(s) for i, s in _sections])
    _old_sections_len = _sections_len
    while _sections_len > available_chars:
        _sections = [(i, _shorten(s)) for i, s in _sections]
        _old_sections_len = _sections_len
        _sections_len = sum([len(s) for i, s in _sections])
        if _old_sections_len == _sections_len:
            raise Exception(f"String could not be truncated: '{item}'")

    new_sections = sorted(_sections, key=lambda x: x[0])
    return delimeter.join([s for i, s in new_sections])


def separate_negation_values(
        vals: List[str],
        negation_prefix: Optional[str] = None,
    ) -> Tuple[List[str], List[str]]:
    """
    Separate the negated values from the positive ones.
    Return two lists: positive and negative values.

    Parameters
    ----------
    vals: List[str]
        A list of strings to parse.

    negation_prefix: Optional[str], default None
        Include values that start with this string in the second list.
        If `None`, use the system default (`_`).
    """
    if negation_prefix is None:
        from meerschaum.config.static import STATIC_CONFIG
        negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    _in_vals, _ex_vals = [], []
    for v in vals:
        if str(v).startswith(negation_prefix):
            _ex_vals.append(str(v)[len(negation_prefix):])
        else:
            _in_vals.append(v)

    return _in_vals, _ex_vals


def flatten_list(list_: List[Any]) -> List[Any]:
    """
    Recursively flatten a list.
    """
    for item in list_:
        if isinstance(item, list):
            yield from flatten_list(item)
        else:
            yield item


def make_symlink(src_path: pathlib.Path, dest_path: pathlib.Path) -> SuccessTuple:
    """
    Wrap around `pathlib.Path.symlink_to`, but add support for Windows.

    Parameters
    ----------
    src_path: pathlib.Path
        The source path.

    dest_path: pathlib.Path
        The destination path.

    Returns
    -------
    A SuccessTuple indicating success.
    """
    try:
        dest_path.symlink_to(src_path)
        success = True
    except Exception as e:
        success = False
        msg = str(e)
    if success:
        return success, "Success"
    
    ### Failed to create a symlink.
    ### If we're not on Windows, return an error.
    import platform
    if platform.system() != 'Windows':
        return success, msg

    try:
        import _winapi
    except ImportError:
        return False, "Unable to import _winapi."

    if src_path.is_dir():
        try:
            _winapi.CreateJunction(str(src_path), str(dest_path))
        except Exception as e:
            return False, str(e)
        return True, "Success"

    ### Last resort: copy the file on Windows.
    import shutil
    try:
        shutil.copy(src_path, dest_path)
    except Exception as e:
        return False, str(e)
    
    return True, "Success"


def is_symlink(path: pathlib.Path) -> bool:
    """
    Wrap `path.is_symlink()` but add support for Windows junctions.
    """
    if path.is_symlink():
        return True
    import platform, os
    if platform.system() != 'Windows':
        return False
    try:
        return bool(os.readlink(path))
    except OSError:
        return False


def parametrized(dec):
    """
    A meta-decorator for allowing other decorator functions to have parameters.

    https://stackoverflow.com/a/26151604/9699829
    """
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)
        return repl
    return layer


def safely_extract_tar(tarf: 'file', output_dir: Union[str, 'pathlib.Path']) -> None:
    """
    Safely extract a TAR file to a give directory.
    This defends against CVE-2007-4559.

    Parameters
    ----------
    tarf: file
        The TAR file opened with `tarfile.open(path, 'r:gz')`.

    output_dir: Union[str, pathlib.Path]
        The output directory.
    """
    import os

    def is_within_directory(directory, target):
        abs_directory = os.path.abspath(directory)
        abs_target = os.path.abspath(target)
        prefix = os.path.commonprefix([abs_directory, abs_target])
        return prefix == abs_directory 

    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        for member in tar.getmembers():
            member_path = os.path.join(path, member.name)
            if not is_within_directory(path, member_path):
                raise Exception("Attempted Path Traversal in Tar File")

        tar.extractall(path=path, members=members, numeric_owner=numeric_owner)

    return safe_extract(tarf, output_dir)


def to_pandas_dtype(dtype: str) -> str:
    """
    Cast a supported Meerschaum dtype to a Pandas dtype.
    """
    if dtype in ('json', 'str', 'object'):
        return 'object'
    if dtype.lower().startswith('int'):
        return 'Int64'
    if dtype.lower().startswith('float'):
        return 'float64'
    if dtype == 'datetime':
        return 'datetime64[ns]'
    if dtype.startswith('datetime'):
        return dtype
    if dtype == 'bool':
        return dtype
    return 'object'


def get_unhashable_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain unhashable objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain unhashable objects.

    Returns
    -------
    A list of columns.
    """
    if len(df) == 0:
        return []
    return [
        col for col, val in df.iloc[0].items()
        if not isinstance(val, Hashable)
    ]


def get_json_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain unhashable objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain unhashable objects.

    Returns
    -------
    A list of columns to be encoded as JSON.
    """
    if len(df) == 0:
        return []
    return [
        col for col, typ in df.dtypes.items()
        if isinstance(df.iloc[0][col], (dict, list))
    ]
