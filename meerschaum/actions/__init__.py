#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Actions available to the mrsm CLI
"""

from meerschaum.utils.misc import add_method_to_class, get_modules_from_package
from meerschaum.actions.shell import Shell
from meerschaum.utils.warnings import enable_depreciation_warnings
enable_depreciation_warnings(__name__)

### build __all__ from other .py files in this package
import sys
__all__, modules = get_modules_from_package(sys.modules[__name__], names=True)

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
    """

    actions.update(
        dict(
            [
                ### __name__ and new function pointer
                (ob[0], add_method_to_class(func=ob[1], class_def=Shell, method_name='do_' + ob[0]))
                    for ob in getmembers(module)
                        if isfunction(ob[1])
                            and ob[0][0] != '_'            
            ]
        )
    )

from meerschaum.actions._entry import _entry as entry

shell = Shell()
