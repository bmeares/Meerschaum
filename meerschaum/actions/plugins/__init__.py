#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle plugins imports here.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Union, Optional

def make_action(
        function : Callable[[Any], Any],
        shell : bool = False,
        debug : bool = False
    ) -> Callable[[Any], Any]:
    """
    Make a function a Meerschaum action. Useful for plugins that are adding multiple actions.

    Usage:
    ```
    >>> from meerschaum.actions.plugins import make_action
    >>> 
    >>> @make_action
    ... def my_action(**kw):
    ...     print('foo')
    ...     return True, "Success"
    >>> 
    ```
    """
    from meerschaum.actions import __all__ as _all, actions
    global __all__, actions
    #  import meerschaum.actions.shell as shell_pkg
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import add_method_to_class
    if function.__name__ not in _all:
        _all.append(function.__name__)
    actions[function.__name__] = function
    #  add_method_to_class(function, shell_pkg.Shell)
    #  if shell and meerschaum.actions._shell is not None:
        #  add_method_to_class(function, meerschaum.actions.get_shell(debug=debug), 'do_' + function.__name__)
    return function

#  __path__ = None
def import_plugins() -> Optional['ModuleType']:
    """
    Import the Meerschaum plugins directory.
    """
    global __path__
    import sys
    from meerschaum.config._paths import (
        PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH, PLUGINS_INIT_PATH
    )
    PLUGINS_RESOURCES_PATH.mkdir(parents=True, exist_ok=True)
    PLUGINS_INIT_PATH.touch()

    from meerschaum.utils.warnings import error, warn
    if str(PLUGINS_RESOURCES_PATH.parent) not in sys.path:
        sys.path.insert(0, str(PLUGINS_RESOURCES_PATH.parent))
    if str(PLUGINS_RESOURCES_PATH.parent) not in __path__:
        __path__.append(str(PLUGINS_RESOURCES_PATH.parent))
    try:
        import plugins
    except ImportError:
        plugins = None

    if plugins is None:
        warn(f"Failed to import plugins.", stacklevel=3)

    sys.path.remove(str(PLUGINS_RESOURCES_PATH.parent))

    return plugins

def load_plugins(debug : bool = False, shell : bool = False):
    ### append the plugins modules
    from inspect import isfunction, getmembers
    from meerschaum.actions import __all__ as _all, modules
    from meerschaum.utils.packages import get_modules_from_package
    _plugins_names, plugins_modules = get_modules_from_package(
        import_plugins(),
        names = True,
        recursive = True,
        modules_venvs = True
    )
    _all += _plugins_names
    #  actions_mod.__all__ += _plugins_names
    #  actions_mod.modules += plugins_modules
    modules += plugins_modules
    for module in plugins_modules:
        for name, func in getmembers(module):
            if not isfunction(func): continue
            if name == module.__name__.split('.')[-1]:
                make_action(func, shell=shell, debug=debug)

def get_plugins_names() -> Optional[List[str]]:
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.utils.warnings import warn, error
    try:
        names = get_modules_from_package(import_plugins(), names=True, recursive=True)[0]
    except Exception as e:
        names = None
        warn(e, stacklevel=3)
    return names

def get_plugins_modules() -> Optional[List['ModuleType']]:
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.utils.warnings import warn, error
    try:
        modules = get_modules_from_package(import_plugins(), recursive=True)
    except Exception as e:
        modules = None
        warn(e, stacklevel=3)
    return modules

def get_data_plugins() -> List['ModuleType']:
    """
    Return plugins which contain `fetch()` or `sync()` functions.
    """
    import inspect
    mods = get_plugins_modules()
    data_names = {'sync', 'fetch'}
    data_plugins = []
    for m in mods:
        for name, ob in inspect.getmembers(m):
            if not inspect.isfunction(ob):
                continue
            if name not in data_names:
                continue
            data_plugins.append(m)
    return data_plugins
