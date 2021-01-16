#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Hook attempt_import into the built-in import system
"""

#  from meerschaum.utils.packaging import attempt_import

#  from importlib.machinery import PathFinder
#  class ImportHook(PathFinder):
    #  def find_spec(self, fullname, path=None, target=None):
        #  result = super(ImportHook, self).find_spec(fullname, path, target)
        #  if result is None and path is None: print(fullname)
        #  attempt_import(fullname, debug=True)
        #  return result


#  import importlib.abc
#  import importlib.machinery
#  import sys
#  import types


#  class DependencyInjectorFinder(importlib.abc.MetaPathFinder):
    #  def __init__(self, loader):
        #  self._loader = loader

    #  def find_spec(self, fullname, path, target=None):
        #  """Attempt to locate the requested module
        #  fullname is the fully-qualified name of the module,
        #  path is set to __path__ for sub-modules/packages, or None otherwise.
        #  target can be a module object, but is unused in this example.
        #  """
        #  print(fullname)
        #  return None
        #  #  return importlib.machinery.ModuleSpec(fullname, self._loader)
    
#  class DependencyInjectorLoader(importlib.abc.Loader):
    #  def __init__(self):
        #  pass
    #  def create_module(self, spec):
        #  """Create the given module from the supplied module spec
        #  Under the hood, this module returns a service or a dummy module,
        #  depending on whether Python is still importing one of the names listed
        #  in _COMMON_PREFIX.
        #  """
        #  #  dummy = types.ModuleType("")
        #  #  dummy.__path__ = []
        #  #  return dummy
        #  return None
        #  #  return importlib.import_module(spec.name)

    #  def exec_module(self, module):
        #  """Execute the given module in its own namespace
        #  This method is required to be present by importlib.abc.Loader,
        #  but since we know our module object is already fully-formed,
        #  this method merely no-ops.
        #  """
        #  pass

#  class DependencyInjector:
    #  """
    #  Convenience wrapper for DependencyInjectorLoader and DependencyInjectorFinder.
    #  """
    #  def __init__(self):
        #  self._loader = DependencyInjectorLoader()
        #  self._finder = DependencyInjectorFinder(self._loader)
    #  def install(self):
        #  sys.meta_path.append(self._finder)
