#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle all things warnings and errors here
"""

import warnings

warnings.filterwarnings(
    "default",
    category = ImportWarning
)
warnings.filterwarnings(
    "ignore",
    category = RuntimeWarning
)

def enable_depreciation_warnings(name):
    import meerschaum.actions
    warnings.filterwarnings(
        "default",
        category = DeprecationWarning,
        module = name
    )

def warn(*args, stacklevel=2, **kw):
    """
    Raise a warning with custom Meerschaum formatting
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf, get_config
    warn_config = get_config('system', 'warnings', patch=True)
    a = list(args)
    a[0] = ' ' + warn_config[CHARSET]['icon'] + ' ' + a[0]
    if ANSI:
        a[0] = colored(a[0], *warn_config['ansi']['color'])
    return warnings.warn(*a, stacklevel=stacklevel, **kw)

def error(message, exception_class = Exception):
    """
    Raise an error with custom Meerschaum formatting
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf, get_config
    error_config = get_config('system', 'errors', patch=True)
    message = error_config[CHARSET]['icon'] + ' ' + message
    if ANSI:
        message = colored(message, *error_config['ansi']['color'])
    raise exception_class(message)
