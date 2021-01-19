#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing packages and virtual environments reside here.
"""

import importlib, importlib.util
_import_module = importlib.import_module
from meerschaum.utils.packages._packages import packages, all_packages
import os, pathlib, sys
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.debug import dprint

_import_hook_venv = None
active_venvs = set()

def is_venv_active(
        venv : str = 'mrsm',
        debug : bool = False
    ) -> bool:
    """
    Check if a virtual environment is active
    """
    if venv is None: return False
    if debug: dprint(f"Checking if virtual environment '{venv}' is active.")
    return venv in active_venvs

def deactivate_venv(
        venv : str = 'mrsm',
        debug : bool = False
    ) -> bool:
    """
    Remove a virtual environment from sys.path (if it's been activated)
    """
    global active_venvs
    if debug: dprint(f"Deactivating virtual environment '{venv}'...")
    if venv in active_venvs: active_venvs.remove(venv)
    indices, new_path = [], []
    if sys.path is None: return False
    for i, p in enumerate(sys.path):
        if f'venvs/{venv}/lib' not in str(p):
            new_path.append(p)
    sys.path = new_path

    ### clear the import virtual environment override
    #  uninstall_import_hook(venv, debug=debug)

    if debug: dprint(f'sys.path: {sys.path}')
    return True

def activate_venv(
        venv : str = 'mrsm',
        debug : bool = False
    ) -> bool:
    """
    Create a virtual environment (if it doesn't exist) and add it to sys.path if necessary
    """
    global active_venvs
    if venv in active_venvs: return True
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    virtualenv = attempt_import('virtualenv', install=True, venv=None, debug=debug)
    venv_path = pathlib.Path(os.path.join(VIRTENV_RESOURCES_PATH, venv))
    bin_path = pathlib.Path(os.path.join(venv_path), 'bin')
    activate_this_path = pathlib.Path(os.path.join(bin_path, 'activate_this.py'))
    old_cwd = pathlib.Path(os.getcwd())
    os.chdir(VIRTENV_RESOURCES_PATH)
    if not activate_this_path.exists():
        if debug: dprint(f"Creating virtual environment '{venv}'...")
        virtualenv.cli_run([venv, '--download', '--system-site-packages'])
    if debug: dprint(f"Activating virtual environment '{venv}'...")
    try:
        exec(open(activate_this_path).read(), {'__file__' : activate_this_path})
    except Exception as e:
        warn(str(e))
        return False
    active_venvs.add(venv)
    os.chdir(old_cwd)

    ### override built-in import with attempt_import
    #  install_import_hook(venv, debug=debug)

    if debug: dprint(f'sys.path: {sys.path}')
    return True

def venv_exec(code : str, venv : str = 'mrsm', debug : bool = False) -> bool:
    """
    Execute Python code in a subprocess via a virtual environment's interpeter.

    Return True if the code successfully executes, False on failure.
    """
    import subprocess, sys
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    executable = (
        sys.executable if venv is None
        else os.path.join(VIRTENV_RESOURCES_PATH, venv, 'bin', 'python')
    )
    return subprocess.call([executable, '-c', code]) == 0

def pip_install(
        *packages : list,
        args : list = ['--upgrade'],
        venv : str = 'mrsm',
        deactivate : bool = True,
        debug : bool = False
    ) -> bool:
    """
    Install pip packages
    """
    try:
        import pip
    except ImportError:
        import ensurepip
        ensurepip.bootstrap(upgrade=True,)
    if venv is not None:
        activate_venv(venv=venv, debug=debug)
        if '--ignore-installed' not in args: args += ['--ignore-installed']
    if 'install' not in args: args = ['install'] + args
    success = run_python_package('pip', args + list(packages), venv=venv, debug=debug) == 0
    if venv is not None and deactivate:
        deactivate_venv(venv=venv, debug=debug)
    return success

def run_python_package(
        package_name : str,
        args : list = [],
        venv : str = None,
        debug : bool = False
    ) -> int:
    """
    Runs an installed python package.
    E.g. Translates to `/usr/bin/python -m [package]`
    """
    import sys, os
    from subprocess import call
    from meerschaum.utils.debug import dprint
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    executable = (
        sys.executable if venv is None
        else os.path.join(VIRTENV_RESOURCES_PATH, venv, 'bin', 'python')
    )
    command = [executable, '-m', package_name] + args
    if debug: print(command)
    return call(command)

def attempt_import(
        *names : list,
        lazy : bool = True,
        warn : bool = True,
        install : bool = True,
        venv : str = 'mrsm',
        precheck : bool = True,
        debug : bool = False,
    ) -> 'module or tuple of modules':
    """
    Raise a warning if packages are not installed; otherwise import and return modules.
    If lazy = True, return lazy-imported modules.

    Returns tuple of modules if multiple names are provided, else returns one module.

    Examples:
        pandas, sqlalchemy = attempt_import('pandas', 'sqlalchemy')
        pandas = attempt_import('pandas')
    """
    ### to prevent recursion, check if parent Meerschaum package is being imported
    if names == ('meerschaum',): return _import_module('meerschaum')

    if venv == 'mrsm' and _import_hook_venv is not None:
        if debug: f"Import hook for virtual environmnt '{_import_hook_venv}' is active."
        venv = _import_hook_venv

    if venv is not None: activate_venv(venv=venv, debug=debug)
    _warnings = _import_module('meerschaum.utils.warnings')
    warn_function = _warnings.warn

    def do_import(_name : str):
        #  is_venv_active(venv, debug=debug)
        if venv is not None: activate_venv(venv=venv, debug=debug)
        ### determine the import method (lazy vs normal)
        if not lazy: import_method = _import_module if not lazy else lazy_import
        try:
            mod = _import_module(_name)
        except Exception as e:
            if warn: warn_function(f"Failed to import module '{_name}'.", ImportWarning, stacklevel=3)
            mod = None
        if venv is not None: deactivate_venv(venv=venv, debug=debug)
        return mod

    modules = []
    for name in names:
        root_name = name.split('.')[0]
        if venv is not None: activate_venv(debug=debug)
        if precheck is False:
            found_modules = do_import(name) is not None
        else:
            try:
                found_module = (importlib.util.find_spec(name) is not None)
            except ModuleNotFoundError as e:
                found_module = False
        if not found_module:
            install_name = root_name
            if root_name in all_packages:
                install_name = all_packages[root_name]
            elif warn:
                warn_function(
                    f"Package '{install_name}' is not declared in meerschaum.utils.packages.",
                    ImportWarning,
                    stacklevel = 3
                )
            if install:
                ### NOTE: pip_install deactivates venv, so deactivate must be False.
                install_success = pip_install(install_name, venv=venv, deactivate=False, debug=debug)
                if not install_success and warn:
                    warn_function(
                        f"Failed to install '{install_name}'.",
                        ImportWarning,
                        stacklevel = 3
                    )
            elif warn:
                warn_function(
                    (f"\n\nMissing package '{name}'; features will not work correctly. "
                    f"\n\nRun `pip install {install_name}` set install=True when calling attempt_import.\n"),
                    ImportWarning,
                    stacklevel = 3
                )
        modules.append(do_import(name))

    if venv is not None: deactivate_venv(venv=venv, debug=debug)

    modules = tuple(modules)
    if len(modules) == 1: return modules[0]
    return modules

def lazy_import(
        name : str,
        local_name : str = None
    ):
    """
    Lazily import a package
    Uses the tensorflow LazyLoader implementation (Apache 2.0 License)
    """
    from meerschaum.utils.lazy_loader import LazyLoader
    if local_name is None:
        local_name = name
    return LazyLoader(local_name, globals(), name)

def import_pandas() -> 'module':
    """
    Quality-of-life function to attempt to import the configured version of pandas
    """
    from meerschaum.config import get_config
    pandas_module_name = get_config('system', 'connectors', 'all', 'pandas', patch=True)
    ### NOTE: modin does NOT currently work!
    if pandas_module_name == 'modin':
        pandas_module_name = 'modin.pandas'
    return attempt_import(pandas_module_name)

def get_modules_from_package(
        package : 'package',
        names : bool = False,
        recursive : bool = False,
        lazy : bool = False,
        modules_venvs : bool = False,
        debug : bool = False
    ):
    """
    Find and import all modules in a package.

    Returns: either list of modules or tuple of lists
    
    names = False (default) : modules
    names = True            : (__all__, modules)
    """
    from meerschaum.utils.debug import dprint
    from os.path import dirname, join, isfile, isdir, basename
    import glob, importlib

    if recursive: pattern = '*'
    else: pattern = '*.py'
    module_names = glob.glob(join(dirname(package.__file__), pattern), recursive=recursive)
    _all = [
        basename(f)[:-3] if isfile(f) else basename(f)
            for f in module_names
                if (isfile(f) or isdir(f))
                    and not f.endswith('__init__.py')
                    and not f.endswith('__pycache__')
    ]

    if debug: dprint(_all)
    modules = []
    for module_name in [package.__name__ + "." + mod_name for mod_name in _all]:
        ### there's probably a better way than a try: catch but it'll do for now
        try:
            if modules_venvs: activate_venv(module_name.split('.')[-1], debug=debug)
            if lazy:
                modules.append(lazy_import(module_name))
            else:
                modules.append(_import_module(module_name))
        except Exception as e:
            if debug: dprint(e)
            pass
        finally:
            if modules_venvs: deactivate_venv(module_name.split('.')[-1], debug=debug)
    if names:
        return _all, modules

    return modules

def import_children(
        package : 'package' = None,
        package_name : str = None,
        types : list = ['method', 'builtin', 'function', 'class', 'module'],
        lazy : bool = True,
        recursive : bool = False,
        debug : bool = False
    ) -> list:
    """
    Import all functions in a package to its __init__.
    package : package (default None)
        Package to import its functions into.
        If None (default), use parent
    
    package_name : str (default None)
        Name of package to import its functions into
        If None (default), use parent

    types : list
        types of members to return.
        Default : ['method', 'builtin', 'class', 'function', 'package', 'module']

    Returns: list of members
    """
    import sys, inspect
    from meerschaum.utils.debug import dprint
    
    ### if package_name and package are None, use parent
    if package is None and package_name is None:
        package_name = inspect.stack()[1][0].f_globals['__name__']

    ### populate package or package_name from other other
    if package is None:
        package = sys.modules[package_name]
    elif package_name is None:
        package_name = package.__name__

    ### Set attributes in sys module version of package.
    ### Kinda like setting a dictionary
    ###   functions[name] = func
    modules = get_modules_from_package(package, recursive=recursive, lazy=lazy, debug=debug)
    _all, members = [], []
    objects = []
    for module in modules:
        _objects = []
        for ob in inspect.getmembers(module):
            for t in types:
                ### ob is a tuple of (name, object)
                if getattr(inspect, 'is' + t)(ob[1]):
                    _objects.append(ob)

        if 'module' in types:
            _objects.append((module.__name__.split('.')[0], module))
        objects += _objects
    for ob in objects:
        setattr(sys.modules[package_name], ob[0], ob[1])
        _all.append(ob[0])
        members.append(ob[1])

    if debug: dprint(_all)
    ### set __all__ for import *
    setattr(sys.modules[package_name], '__all__', _all)
    return members

def reload_package(
        package : 'package',
        lazy : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Recursively load a package's subpackages, even if they were not previously loaded
    """
    import os, types, importlib, sys
    from meerschaum.utils.debug import dprint
    assert(hasattr(package, "__package__"))
    fn = package.__file__
    fn_dir = os.path.dirname(fn) + os.sep
    module_visit = {fn}
    del fn

    def reload_recursive_ex(module):
        import os, types, importlib
        from meerschaum.utils.debug import dprint
        ### forces import of lazily-imported modules
        del sys.modules[module.__name__]
        module = __import__(module.__name__)
        module = _import_module(module.__name__)
        _module = importlib.reload(module)
        sys.modules[module.__name__] = _module

        for module_child in get_modules_from_package(module, recursive=True, lazy=lazy):
            if isinstance(module_child, types.ModuleType) and hasattr(module_child, '__name__'):
                fn_child = getattr(module_child, "__file__", None)
                if (fn_child is not None) and fn_child.startswith(fn_dir):
                    if fn_child not in module_visit:
                        if debug: dprint(f"reloading: {fn_child} from {module}")
                        module_visit.add(fn_child)
                        reload_recursive_ex(module_child)

    return reload_recursive_ex(package)

def is_installed(
        name : str
    ) -> bool:
    """
    Check whether a package is installed.
    name : str
        Name of the package in question
    """
    import importlib.util
    return importlib.util.find_spec(name) is None

### NOTE: this is at the bottom to avoid import problems
#  from meerschaum.utils.packages._ImportHook import install

from meerschaum.utils.warnings import warn
from importlib.machinery import PathFinder
class ImportHook(PathFinder):
    def __init__(self, venv : str = None, debug : bool = False):
        self.venv = venv
        self.debug = debug

    #  def __del__(self):
        #  deactivate_venv(self.venv, debug=self.debug)

    def create_module(self, spec):
        print(spec)

    def exec_module(self, module):
        print(module)

    def find_spec(self, fullname, path=None, target=None):
        if self.venv is not None and self.venv not in active_venvs:
            activate_venv(self.venv, debug=False)
        result = super(ImportHook, self).find_spec(fullname, path, target)
        #  try:
            #  __import__(fullname)
        #  except Exception as e:
            #  print(str(e))
        if result is None and path is None and target is None:
            if not fullname in sys.builtin_module_names:
                warn(fullname, stacklevel=3)
                pip_install(fullname, venv=self.venv, debug=self.debug)
            #  pass
            #  #  attempt_import(fullname, debug=True, venv=self.venv, precheck=False)
        #  if self.venv is not None:
            #  deactivate_venv(self.venv, debug=False)
        return result

def install_import_hook(venv : str = 'mrsm', debug : bool = False) -> bool:
    global _import_hook_venv
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    if _import_hook_venv is not None and _import_hook_venv != venv:
        if venv != 'mrsm':
            warn(
                f"Virtual environment '{_import_hook_venv}' was set as the import hook " +
                f"but is being overwritten by '{venv}'.", stacklevel=4
            )
        else:
            if debug:
                dprint(f"_import_hook_venv is '{_import_hook_venv}', " +
                    "attempted to install venv '{venv}'. " +
                    "Uninstall import hook before installing '{venv}'."
                )
            return False
    if debug: dprint(f"Installing import hook for virtual environment '{venv}'...")
    importlib.import_module = attempt_import
    _import_hook_venv = venv
    found_hook = False
    for finder in sys.meta_path:
        if isinstance(finder, ImportHook) and finder.venv == venv:
            found_hook = True
            break
    if not found_hook:
        sys.meta_path.insert(0, ImportHook(venv, debug=True))

    return True

def uninstall_import_hook(venv : str = 'mrsm', all_hooks : bool = False, debug : bool = False) -> bool:
    global _import_hook_venv
    if debug: dprint(f"Uninstalling import hook (was set to {_import_hook_venv})...")
    importlib.import_module = _import_module
    _import_hook_venv = None
    return True
    new_meta_path, to_delete = [], []
    for finder in sys.meta_path:
        if (
            not isinstance(finder, ImportHook) or (
                isinstance(finder, ImportHook) and not all_hooks and finder.venv != venv
            )
        ):
            new_meta_path.append(finder)
        else:
            to_delete.append(finder)
    sys.meta_path = new_meta_path
    del to_delete[:]

    return True

