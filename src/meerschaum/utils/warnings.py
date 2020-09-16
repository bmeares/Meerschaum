#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle all things warnings here
"""

import warnings

warnings.filterwarnings(
    "default",
    category = ImportWarning
)

def enable_depreciation_warnings(name):
    import meerschaum.actions
    warnings.filterwarnings(
        "default",
        category = DeprecationWarning,
        module = name
    )

warn = warnings.warn
#  def warn(*args, **kw):
    #  return warnings.warn(*args, **kw)
