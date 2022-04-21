#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Expose plugin management APIs from the `meerschaum.plugins` module.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Union, Optional, Dict, List
from meerschaum.utils.threading import Lock, RLock
from meerschaum._internal.Plugin._Plugin import Plugin

_api_plugins : dict = {}
_locks = {
    '_api_plugins': RLock(),
    '__path__': RLock(),
    'sys.path': RLock(),
}
__pdoc__ = {'venvs': False, 'data': False, 'stack': False, 'plugins': False}

def make_action(
        function: Callable[[Any], Any],
        shell: bool = False,
        activate: bool = True,
        deactivate: bool = True,
        debug: bool = False
    ) -> Callable[[Any], Any]:
    """
    Make a function a Meerschaum action. Useful for plugins that are adding multiple actions.
    
    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to become a Meerschaum action. Must accept all keyword arguments.
        
    shell: bool, default False
        Not used.
        
    Returns
    -------
    Another function (this is a decorator function).

    Examples
    --------
    >>> from meerschaum.plugins import make_action
    >>>
    >>> @make_action
    ... def my_action(**kw):
    ...     print('foo')
    ...     return True, "Success"
    >>>
    """

    from meerschaum.actions import __all__ as _all, actions
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import add_method_to_class
    package_name = function.__globals__['__name__']
    plugin_name = (
        package_name.split('plugins.')[-1]
        if package_name.startswith('plugins.') else None
    )

    def _new_action_venv_wrapper(*args, debug: bool = False, **kw):
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        if plugin_name and activate:
            activate_venv(plugin_name, debug=debug)
        result = function(*args, debug=debug, **kw)
        if plugin_name and deactivate:
            deactivate_venv(plugin_name, debug=debug)
        return result

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(
            f"Adding action '{function.__name__}' from plugin " +
            f"'{function.__module__.split('.')[-1]}'..."
        )

    if function.__name__ not in _all:
        _all.append(function.__name__)
    actions[function.__name__] = _new_action_venv_wrapper
    return function


def api_plugin(function: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Execute the function when initializing the Meerschaum API module.
    Useful for lazy-loading heavy plugins only when the API is started,
    such as when editing the `meerschaum.api.app` FastAPI app.
    
    The FastAPI app will be passed as the only parameter.
    
    Parameters
    ----------
    function: Callable[[Any, Any]]
        The function to be called before starting the Meerschaum API.
        
    Returns
    -------
    Another function (decorator function).

    Examples
    --------
    >>> from meerschaum.plugins import api_plugin
    >>>
    >>> @api_plugin
    >>> def initialize_plugin(app):
    ...     @app.get('/my/new/path')
    ...     def new_path():
    ...         return {'message' : 'It works!'}
    >>>
    """

    global _api_plugins
    _locks['_api_plugins'].acquire()
    try:
        if function.__module__ not in _api_plugins:
            _api_plugins[function.__module__] = []
        _api_plugins[function.__module__].append(function)
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(e)
    finally:
        _locks['_api_plugins'].release()
    return function


def import_plugins(
        plugins_to_import: Union[str, List[str], None] = None,
        warn: bool = True,
    ) -> Union[
        'ModuleType', Tuple['ModuleType', None]
    ]:
    """
    Import the Meerschaum plugins directory.

    Parameters
    ----------
    plugins_to_import: Union[str, List[str], None]
        If provided, only import the specified plugins.
        Otherwise import the entire plugins module. May be a string, list, or `None`.
        Defaults to `None`.

    Returns
    -------
    A module of list of modules, depening on the number of plugins provided.

    """
    global __path__
    import sys
    import importlib
    from meerschaum.config._paths import (
        PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH, PLUGINS_INIT_PATH
    )
    PLUGINS_RESOURCES_PATH.mkdir(parents=True, exist_ok=True)
    PLUGINS_INIT_PATH.touch()

    _locks['__path__'].acquire()
    _locks['sys.path'].acquire()

    if isinstance(plugins_to_import, str):
        plugins_to_import = [plugins_to_import]

    from meerschaum.utils.warnings import error, warn as _warn
    if str(PLUGINS_RESOURCES_PATH.parent) not in sys.path:
        sys.path.insert(0, str(PLUGINS_RESOURCES_PATH.parent))
    if str(PLUGINS_RESOURCES_PATH.parent) not in __path__:
        __path__.append(str(PLUGINS_RESOURCES_PATH.parent))

    if not plugins_to_import:
        try:
            plugins = importlib.import_module('plugins')
        except ImportError as e:
            warn(e)
            plugins = None
    else:
        from meerschaum.utils.packages import attempt_import
        plugins = [importlib.import_module(f'plugins.{p}') for p in plugins_to_import]

    if plugins is None and warn:
        _warn(f"Failed to import plugins.", stacklevel=3)

    if str(PLUGINS_RESOURCES_PATH.parent) in sys.path:
        sys.path.remove(str(PLUGINS_RESOURCES_PATH.parent))

    _locks['__path__'].release()
    _locks['sys.path'].release()

    if isinstance(plugins, list):
        return (plugins[0] if len(plugins) == 1 else tuple(plugins))
    return plugins

def load_plugins(debug: bool = False, shell: bool = False) -> None:
    """
    Import Meerschaum plugins and update the actions dictionary.

    """
    ### append the plugins modules
    from inspect import isfunction, getmembers
    from meerschaum.actions import __all__ as _all, modules, make_action
    from meerschaum.utils.packages import get_modules_from_package
    if debug:
        from meerschaum.utils.debug import dprint

    _plugins_names, plugins_modules = get_modules_from_package(
        import_plugins(),
        names = True,
        recursive = True,
        modules_venvs = True
    )
    _all += _plugins_names
    ### I'm appending here to keep from redefining the modules list.
    new_modules = [mod for mod in modules if not mod.__name__.startswith('plugins.')] + plugins_modules
    n_mods = len(modules)
    for mod in new_modules:
        modules.append(mod)
    for i in range(n_mods):
        modules.pop(0)

    for module in plugins_modules:
        for name, func in getmembers(module):
            if not isfunction(func):
                continue
            if name == module.__name__.split('.')[-1]:
                make_action(func, **{'shell': shell, 'debug': debug})

def reload_plugins(plugins: Optional[List[str]] = None, debug: bool = False) -> None:
    """
    Reload plugins back into memory.

    Parameters
    ----------
    plugins: Optional[List[str]], default None
        The plugins to reload. `None` will reload all plugins.

    """
    import sys
    if debug:
        from meerschaum.utils.debug import dprint

    if not plugins:
        plugins = get_plugins_names()
    for plugin_name in plugins:
        if debug:
            dprint(f"Reloading plugin '{plugin_name}'...")
        mod_name = 'plugins.' + str(plugin_name)
        if mod_name in sys.modules:
            del sys.modules[mod_name]
    load_plugins(debug=debug)


def get_plugins_names() -> Union[List[str], None]:
    """
    Return a list of installed plugins, or `None` if things break.
    """
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.utils.warnings import warn, error
    try:
        names = get_modules_from_package(import_plugins(), names=True, recursive=True)[0]
    except Exception as e:
        names = None
        warn(e, stacklevel=3)
    return names

def get_plugins_modules() -> Union[List['ModuleType'], None]:
    """
    Return a list of modules for the installed plugins, or `None` if things break.
    """
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
    Only return the modules of plugins with either `fetch()` or `sync()` functions.
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

def add_plugin_argument(*args, **kwargs) -> None:
    """
    Add argparse arguments under the 'Plugins options' group.
    Takes the same parameters as the regular argparse `add_argument()` function.

    Examples
    --------
    >>> add_plugin_argument('--foo', type=int, help="This is my help text!")
    >>> 
    """
    from meerschaum.actions._parser import groups, _seen_plugin_args
    from meerschaum.actions import _get_parent_plugin
    from meerschaum.utils.warnings import warn, error
    _parent_plugin_name = _get_parent_plugin(2)
    if _parent_plugin_name is None:
        error(f"You may only call `add_plugin_argument()` from within a Meerschaum plugin.")
    group_key = 'plugin_' + _parent_plugin_name
    if group_key not in groups:
        groups[group_key] = parser.add_argument_group(
            title=f"Plugin '{_parent_plugin_name}' options"
        )
        _seen_plugin_args[group_key] = set()
    try:
        if str(args) not in _seen_plugin_args[group_key]:
            groups[group_key].add_argument(*args, **kwargs)
            _seen_plugin_args[group_key].add(str(args))
    except Exception as e:
        warn(e)

