#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
External API for importing Meerschaum paths.
"""

import pathlib
import inspect
import meerschaum.config._paths as _paths


def __getattr__(*args, **kwargs):
    return _paths.__getattr__(*args, **kwargs)


_globals_dict = inspect.getmembers(
    _paths,
    lambda member: not inspect.isroutine(member)
)
_all_caps_globals = [
    name
    for name, value in _globals_dict
    if ('PATH' in name or 'FILE' in name) and not name.startswith('_')
    and isinstance(value, pathlib.Path)
]

__all__ = tuple(_all_caps_globals + list(_paths.paths.keys()))
