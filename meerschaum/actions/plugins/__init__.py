#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle plugins imports here.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Union, Optional

def make_action(function : Callable[[Any], Any], debug : bool = False):
    """
    Make a function a Meerschaum action. Useful for plugins that are adding multiple actions.

    Usage:
    ```
    >>> import meerschaum as mrsm
    >>> 
    >>> @mrsm.action
    ... def my_action(**kw):
    ...     print('foo')
    ...     return True, "Success"
    >>> 
    ```
    """
    import meerschaum.actions
    global __all__, actions
    import meerschaum.actions.shell as shell_pkg
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import add_method_to_class
    if function.__name__ not in meerschaum.actions.__all__:
        meerschaum.actions.__all__.append(function.__name__)
    meerschaum.actions.actions[function.__name__] = function
    add_method_to_class(function, shell_pkg.Shell)
    add_method_to_class(function, meerschaum.actions.get_shell(debug=debug), 'do_' + function.__name__)
    return function

def import_plugins() -> Optional['ModuleType']:
    """
    Import the Meerschaum plugins directory.
    """
    import sys
    from meerschaum.config._paths import (
        RESOURCES_PATH, PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
    )
    from meerschaum.utils.warnings import error, warn
    if str(RESOURCES_PATH) not in sys.path: sys.path.append(str(RESOURCES_PATH))
    if str(RESOURCES_PATH) not in __path__: __path__.append(str(RESOURCES_PATH))
    try:
        import plugins
    except ImportError:
        plugins = None

    if plugins is None:
        warn(f"Failed to import plugins.", stacklevel=3)

    return plugins

def load_plugins(debug : bool = False):
    ### append the plugins modules
    from inspect import isfunction, getmembers
    import meerschaum.actions
    from meerschaum.utils.packages import get_modules_from_package
    _plugins_names, plugins_modules = get_modules_from_package(
        import_plugins(),
        names = True,
        recursive = True,
        modules_venvs = True
    )
    meerschaum.actions.__all__ += _plugins_names
    meerschaum.actions.modules += plugins_modules
    for module in plugins_modules:
        for name, func in getmembers(module):
            if not isfunction(func): continue
            if name == module.__name__.split('.')[-1]:
                make_action(func, debug=debug)

def get_plugins_names() -> Optional[List[str]]:
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.utils.warnings import warn, error
    try:
        names = get_modules_from_package(import_plugins(), names=True, recursive=True)[0]
    except Exception as e:
        names = None
        warn(e, stacklevel=3)
    return names

def get_plugins_modules() -> Optional[List[str]]:
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.utils.warnings import warn, error
    try:
        modules = get_modules_from_package(import_plugins(), recursive=True)
    except Exception as e:
        modules = None
        warn(e, stacklevel=3)
    return modules

