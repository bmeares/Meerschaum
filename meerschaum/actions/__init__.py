#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default actions available to the mrsm CLI.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Optional, Union, List
from meerschaum.utils.packages import get_modules_from_package
from meerschaum.utils.warnings import enable_depreciation_warnings
enable_depreciation_warnings(__name__)
_shell = None
_custom_actions = []

### build __all__ from other .py files in this package
import sys
__all__, modules = get_modules_from_package(sys.modules[__name__], names=True)

### append the plugins modules
#  from meerschaum.config._paths import (
    #  RESOURCES_PATH, PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
#  )
#  if str(RESOURCES_PATH) not in sys.path: sys.path.append(str(RESOURCES_PATH))
#  if str(RESOURCES_PATH) not in __path__: __path__.append(str(RESOURCES_PATH))
#  import plugins
#  from plugins import testing
#  help(testing)
#  _plugins_names, plugins_modules = get_modules_from_package(
    #  plugins,
    #  names = True,
    #  recursive = True,
    #  modules_venvs = True
#  )
#  __all__ += _plugins_names
#  modules += plugins_modules


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
def get_shell(sysargs : List[str] = [], debug : bool = False):
    """
    Lazy load the Shell
    """
    global _shell
    from meerschaum.utils.debug import dprint

    if _shell is None:
        if debug: dprint("Loading the shell...")
        from meerschaum.utils.misc import add_method_to_class
        import meerschaum.actions.shell as shell_pkg
        for a, f in actions.items():
            add_method_to_class(func=f, class_def=shell_pkg.Shell, method_name='do_' + a)

        _shell = shell_pkg.Shell(actions, sysargs=sysargs)

    return _shell

from meerschaum.actions.plugins import make_action, load_plugins, import_plugins
plugins = import_plugins()
__pdoc__ = {'plugins' : False}
load_plugins(debug=True)
