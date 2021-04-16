#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle all things warnings and errors here
"""

from __future__ import annotations
from meerschaum.utils.typing import Any

import warnings

warnings.filterwarnings(
    "always",
    category = UserWarning
)
warnings.filterwarnings(
    "ignore",
    category = DeprecationWarning
)
warnings.filterwarnings(
    "always",
    category = ImportWarning
)
warnings.filterwarnings(
    "ignore",
    category = RuntimeWarning
)

#  class SilentException(Exception):
    #  """
    #  Raise a silent Exception.
    #  """
    #  import inspect
    #  def __init__(self, msg : str = ''):
        #  try:
            #  ln = sys.exc_info()[-1].tb_lineno
        #  except AttributeError:
            #  ln = inspect.currentframe().f_back.f_lineno
            #  #  ln = inspect.currentframe().f_lineno
        #  self.args = "{0.__name__} (line {1}): {2}".format(type(self), ln, msg),
        #  #  self.args = msg

def enable_depreciation_warnings(name) -> None:
    """
    Enable depreciation warnings in the warnings module.
    """
    import meerschaum.actions
    warnings.filterwarnings(
        "always",
        category = DeprecationWarning,
        module = name
    )

def warn(*args, stacklevel=2, stack=True, color : bool = True, **kw) -> None:
    """
    Raise a warning with custom Meerschaum formatting
    """
    get_config = None
    if color:
        try:
            from meerschaum.utils.formatting import CHARSET, ANSI, colored
        except ImportError:
            CHARSET = 'ascii'
            ANSI = False
        try:
            from meerschaum.config import get_config as _get_config
            from meerschaum.config import _config
            cf = _config()
            get_config = _get_config
        except ImportError:
            get_config = None
    import sys

    if get_config is None and color:
        try:
            warn_config = cf['formatting']['warnings']
        except KeyError:
            warn_config = {
                'ansi' : {'color' : []},
                'unicode' : {'icon' : ''},
                'ascii' : {'icon' : ''},
            }
    elif color:
        warn_config = get_config('formatting', 'warnings', patch=True)
    a = list(args)
    a[0] = ' ' + (warn_config[CHARSET]['icon'] if color else '') + ' ' + str(a[0])
    if color:
        if ANSI:
            a[0] = colored(a[0], *warn_config['ansi']['color'])
    if stacklevel is None or not stack:
        print(a[0], file=sys.stderr)
    else:
        warnings.warn(*a, stacklevel=stacklevel, **kw)

def exception_with_traceback(
        message : str,
        exception_class = Exception, 
        stacklevel = 1,
        tb_type = 'single'
    ):
    """
    Traceback construction help found here:
    https://stackoverflow.com/questions/27138440/how-to-create-a-traceback-object
    """
    import sys, types
    tb, depth = None, 0
    while True:
        try:
            frame = sys._getframe(depth)
            depth += 1
        except ValueError as e:
            break

        tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)

    tbs, _tb = [], tb
    while True:
        if _tb is None:
            break
        tbs.append(_tb)
        _tb = _tb.tb_next

    found_main, main_i = False, 0
    first_mrsm_after_main = None
    last_mrsm_i = None
    tbs[(-1 * stacklevel)].tb_next = None
    for i, _tb in enumerate([_tb for _tb in tbs]):
        if 'meerschaum' in str(_tb.tb_frame) and '__main__.py' in str(_tb.tb_frame):
            found_main = True
            main_i = i
            continue
        if i >= (len(tbs) - (stacklevel - 1)):
            tbs[i] = None
        elif (
                found_main and 'meerschaum' in str(_tb.tb_frame)
                and first_mrsm_after_main is None
                and 'Shell' not in str(_tb.tb_frame)
            ):
            first_mrsm_after_main = i
        elif 'meerschaum' in str(_tb.tb_frame):
            last_mrsm_i = i

    tbs = [_tb for _tb in tbs if tb is not None]

    if tb_type == 'single':
        return exception_class(message).with_traceback(tbs[-3])
    return exception_class(message).with_traceback(tbs[first_mrsm_after_main])

def error(
        message : str,
        exception_class = Exception,
        nopretty : bool = False,
        silent : bool = True,
        stack : bool = True,
    ):
    """
    Raise an error with custom Meerschaum formatting
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored, pprint, get_console
    from meerschaum.utils.packages import import_rich
    from meerschaum.config import get_config
    import types, sys, inspect
    rich = import_rich()
    error_config = get_config('formatting', 'errors', patch=True)
    message = ' ' + error_config[CHARSET]['icon'] + ' ' + str(message)
    exception = exception_with_traceback(message, exception_class, stacklevel=3)
    color_message = str(message)
    color_exception = exception_with_traceback(color_message, exception_class, stacklevel=3)
    if ANSI and not nopretty and not stack:
        color_message = '\n' + colored(message, *error_config['ansi']['color'])
        color_exception = exception_with_traceback(color_message, exception_class, stacklevel=3)
    try:
        trace = rich.traceback.Traceback.extract(
            exception_class, exception, exception.__traceback__
        )
        rtb = rich.traceback.Traceback(trace)
    except Exception as e:
        trace, rtb = None, None
    if trace is None or rtb is None:
        nopretty = True
    if not nopretty and stack:
        if get_console() is not None:
            get_console().print(rtb)
    frame = sys._getframe(len(inspect.stack()) - 1)
    #  sys.tracebacklimit = 0
    #  help(sys.excepthook)
    #  if silent: raise SilentException(message)
    #  if silent: sys.tracebacklimit = 0
    #  else: sys.tracebacklimit = None
    raise color_exception

def info(message : str, icon : bool = True, **kw):
    """
    Print an informative message
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import get_config
    import sys
    info_config = get_config('formatting', 'info', patch=True)
    if icon:
        message = ' ' + info_config[CHARSET]['icon'] + ' ' + message
    if ANSI:
        message = colored(message, *info_config['ansi']['color'])
    ### NOTE: There's a bug somewhere because I have to flush stdout every time.
    print(message, flush=True)
