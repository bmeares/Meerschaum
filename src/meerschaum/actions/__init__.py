#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Actions available to the mrsm CLI
"""

### build __all__ from other .py files in this package
from os.path import dirname, basename, isfile, join
import glob
modules = glob.glob(join(dirname(__file__), "*.py"))
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]

### build the actions dictionary by importing all
### functions that do not begin with '_' from all submodules
from inspect import getmembers, isfunction
import importlib
actions = dict()
for module_name in ["meerschaum.actions." + mod_name for mod_name in __all__]:
    module = importlib.import_module(module_name)
    actions.update(
        dict(
            [
                ob for ob in getmembers(module)
                    if isfunction(ob[1])
                        and ob[0][0] != '_'            
            ]
        )
    )

