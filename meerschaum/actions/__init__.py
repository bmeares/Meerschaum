#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default actions available to the mrsm CLI.
"""

from meerschaum.utils.packages import get_modules_from_package
from meerschaum.utils.misc import add_method_to_class
from meerschaum.utils.warnings import enable_depreciation_warnings
enable_depreciation_warnings(__name__)

### build __all__ from other .py files in this package
import sys
__all__, modules = get_modules_from_package(sys.modules[__name__], names=True)

### append the plugins modules
from meerschaum.config._paths import (
    RESOURCES_PATH, PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
)
if str(RESOURCES_PATH) not in sys.path: sys.path.append(str(RESOURCES_PATH))
if str(RESOURCES_PATH) not in __path__: __path__.append(str(RESOURCES_PATH))
import plugins
_plugins_names, plugins_modules = get_modules_from_package(
    plugins,
    names = True,
    recursive = True,
    modules_venvs = True
)
__all__ += _plugins_names
modules += plugins_modules


### build the actions dictionary by importing all
### functions that do not begin with '_' from all submodules
from inspect import getmembers, isfunction
import importlib
actions = dict()
for module in modules:

    """
    A couple important things happening here:
    1. Find all functions in all modules in `actions` package
        (skip functions that begin with '_')
    2. Add them as members to the Shell class
        - Original definition : meerschaum.actions.shell.Shell
        - New definition      : meerschaum.actions.Shell
    3. Populate the actions dictionary with function names and functions

    UPDATE:
    Shell modifications have been deferred to get_shell in order to improve lazy loading.

    """

    actions.update(
        dict(
            [
                ### __name__ and new function pointer
                (ob[0], ob[1])
                    for ob in getmembers(module)
                        if isfunction(ob[1])
                            ### check that the function belongs to the module
                            and ob[0] == module.__name__.replace('_', '').split('.')[-1]
                            ### skip functions that start with '_'
                            and ob[0][0] != '_'
            ]
        )
    )


from meerschaum.actions._entry import _entry as entry
shell = None
def get_shell(sysargs : list = []):
    """
    Lazy load the Shell
    """
    global shell

    if shell is None:
        from meerschaum.actions.shell import Shell
        for a, f in actions.items():
            add_method_to_class(func=f, class_def=Shell, method_name='do_' + a)

        shell = Shell(actions, sysargs=sysargs)

    return shell
