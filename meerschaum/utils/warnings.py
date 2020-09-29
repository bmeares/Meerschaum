#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle all things warnings here
"""

import warnings
from meerschaum.utils.formatting import UNICODE, ANSI

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

#  warn = warnings.warn
def warn(*args, stacklevel=2, **kw):
    a = list(args)
    if UNICODE: a[0] = ' ⚠ ' + a[0]
    if ANSI:
        from more_termcolor import colored
        a[0] = colored(a[0], 'yellow')
    return warnings.warn(*a, stacklevel=stacklevel, **kw)
