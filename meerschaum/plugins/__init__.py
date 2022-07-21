#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Expose plugin management APIs from the `meerschaum.plugins` module.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Union, Optional, Dict, List, Tuple
from meerschaum.utils.threading import Lock, RLock
from meerschaum.plugins._Plugin import Plugin

_api_plugins: Dict[str, List[Callable[['fastapi.App'], Any]]] = {}
_locks = {
    '_api_plugins': RLock(),
    '__path__': RLock(),
    'sys.path': RLock(),
}
__all__ = (
    "Plugin", "make_action", "api_plugin", "import_plugins",
    "reload_plugins", "get_plugins", "get_data_plugins", "add_plugin_argument",
)
__pdoc__ = {
    'venvs': False, 'data': False, 'stack': False, 'plugins': False,
}

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

    from meerschaum.actions import actions
    from meerschaum.utils.formatting import pprint
    package_name = function.__globals__['__name__']
    plugin_name = (
        package_name.split('.')[1]
        if package_name.startswith('plugins.') else None
    )
    plugin = Plugin(plugin_name) if plugin_name else None

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(
            f"Adding action '{function.__name__}' from plugin " +
            f"'{function.__module__.split('.')[-1]}'..."
        )

    actions[function.__name__] = function
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
    with _locks['_api_plugins']:
        try:
            if function.__module__ not in _api_plugins:
                _api_plugins[function.__module__] = []
            _api_plugins[function.__module__].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


def import_plugins(
        *plugins_to_import: Union[str, List[str], None],
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
    import sys
    import importlib
    from meerschaum.utils.misc import flatten_list
    from meerschaum.utils.warnings import error, warn as _warn
    from meerschaum.config._paths import (
        PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH, PLUGINS_INIT_PATH
    )
    PLUGINS_RESOURCES_PATH.mkdir(parents=True, exist_ok=True)
    PLUGINS_INIT_PATH.touch()
    plugins_to_import = list(plugins_to_import)

    if str(PLUGINS_RESOURCES_PATH.parent) not in sys.path:
        with _locks['sys.path']:
            sys.path.insert(0, str(PLUGINS_RESOURCES_PATH.parent))
    if str(PLUGINS_RESOURCES_PATH.parent) not in __path__:
        with _locks['__path__']:
            __path__.append(str(PLUGINS_RESOURCES_PATH.parent))

    if not plugins_to_import:
        try:
            plugins = importlib.import_module('plugins')
        except ImportError as e:
            warn(e)
            plugins = None
    else:
        from meerschaum.utils.packages import attempt_import
        plugins = [importlib.import_module(f'plugins.{p}') for p in flatten_list(plugins_to_import)]

    if plugins is None and warn:
        _warn(f"Failed to import plugins.", stacklevel=3)

    if str(PLUGINS_RESOURCES_PATH.parent) in sys.path:
        with _locks['sys.path']:
            sys.path.remove(str(PLUGINS_RESOURCES_PATH.parent))

    if isinstance(plugins, list):
        return (plugins[0] if len(plugins) == 1 else tuple(plugins))
    return plugins


def load_plugins(debug: bool = False, shell: bool = False) -> None:
    """
    Import Meerschaum plugins and update the actions dictionary.
    """
    from inspect import isfunction, getmembers
    from meerschaum.actions import __all__ as _all, modules
    from meerschaum.utils.packages import get_modules_from_package
    if debug:
        from meerschaum.utils.debug import dprint

    _plugins_names, plugins_modules = get_modules_from_package(
        import_plugins(),
        names = True,
        recursive = True,
        modules_venvs = True
    )
    ### I'm appending here to keep from redefining the modules list.
    new_modules = (
        [mod for mod in modules if not mod.__name__.startswith('plugins.')]
        + plugins_modules
    )
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


def get_plugins(*to_load) -> Union[Tuple[Plugin], Plugin]:
    """
    Return a list of `Plugin` objects.
    """
    from meerschaum.utils.packages import get_modules_from_package
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    import os
    _plugins = [
        Plugin(name) for name in (
            to_load or [
                (
                    name if (PLUGINS_RESOURCES_PATH / name).is_dir()
                    else name[:-3]
                ) for name in os.listdir(PLUGINS_RESOURCES_PATH) if name != '__init__.py'
            ]
        )
    ]
    plugins = tuple(plugin for plugin in _plugins if plugin.is_installed())
    if len(to_load) == 1:
        return plugins[0]
    return plugins


def get_plugins_names(*to_load) -> List[str]:
    """
    Return a list of installed plugins.
    """
    return [plugin.name for plugin in get_plugins(*to_load)]

def get_plugins_modules(*to_load) -> List['ModuleType']:
    """
    Return a list of modules for the installed plugins, or `None` if things break.
    """
    return [plugin.module for plugin in get_plugins(*to_load)]

def get_data_plugins() -> List[Plugin]:
    """
    Only return the modules of plugins with either `fetch()` or `sync()` functions.
    """
    import inspect
    plugins = get_plugins()
    mods = get_plugins_modules()
    data_names = {'sync', 'fetch'}
    data_plugins = []
    for plugin in plugins:
        for name, ob in inspect.getmembers(plugin.module):
            if not inspect.isfunction(ob):
                continue
            if name not in data_names:
                continue
            data_plugins.append(plugin)
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
    from meerschaum.actions.arguments._parser import groups, _seen_plugin_args, parser
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

