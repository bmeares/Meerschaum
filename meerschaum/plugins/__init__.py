#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Expose plugin management APIs from the `meerschaum.plugins` module.
"""

from __future__ import annotations
import functools
import meerschaum as mrsm
from meerschaum.utils.typing import Callable, Any, Union, Optional, Dict, List, Tuple
from meerschaum.utils.threading import Lock, RLock
from meerschaum.plugins._Plugin import Plugin

_api_plugins: Dict[str, List[Callable[['fastapi.App'], Any]]] = {}
_pre_sync_hooks: Dict[str, List[Callable[[Any], Any]]] = {}
_post_sync_hooks: Dict[str, List[Callable[[Any], Any]]] = {}
_locks = {
    '_api_plugins': RLock(),
    '_dash_plugins': RLock(),
    '_pre_sync_hooks': RLock(),
    '_post_sync_hooks': RLock(),
    '__path__': RLock(),
    'sys.path': RLock(),
    'internal_plugins': RLock(),
    '_synced_symlinks': RLock(),
    'PLUGINS_INTERNAL_LOCK_PATH': RLock(),
}
__all__ = (
    "Plugin", "make_action", "api_plugin", "dash_plugin", "web_page",
    "import_plugins", "from_plugin_import",
    "reload_plugins", "get_plugins", "get_data_plugins", "add_plugin_argument",
    "pre_sync_hook", "post_sync_hook",
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
            f"'{plugin}'..."
        )

    actions[function.__name__] = function
    return function


def pre_sync_hook(
        function: Callable[[Any], Any],
    ) -> Callable[[Any], Any]:
    """
    Register a function as a sync hook to be executed right before sync.
    
    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to execute right before a sync.
        
    Returns
    -------
    Another function (this is a decorator function).

    Examples
    --------
    >>> from meerschaum.plugins import pre_sync_hook
    >>>
    >>> @pre_sync_hook
    ... def log_sync(pipe, **kwargs):
    ...     print(f"About to sync {pipe} with kwargs:\n{kwargs}.")
    >>>
    """
    with _locks['_pre_sync_hooks']:
        try:
            if function.__module__ not in _pre_sync_hooks:
                _pre_sync_hooks[function.__module__] = []
            _pre_sync_hooks[function.__module__].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


def post_sync_hook(
        function: Callable[[Any], Any],
    ) -> Callable[[Any], Any]:
    """
    Register a function as a sync hook to be executed upon completion of a sync.
    
    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to execute upon completion of a sync.
        
    Returns
    -------
    Another function (this is a decorator function).

    Examples
    --------
    >>> from meerschaum.plugins import post_sync_hook
    >>>
    >>> @post_sync_hook
    ... def log_sync(pipe, success_tuple, duration=None, **kwargs):
    ...     print(f"It took {round(duration, 2)} seconds to sync {pipe}.")
    >>>
    """
    with _locks['_post_sync_hooks']:
        try:
            if function.__module__ not in _post_sync_hooks:
                _post_sync_hooks[function.__module__] = []
            _post_sync_hooks[function.__module__].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


_plugin_endpoints_to_pages = {}
def web_page(
        page: Union[str, None, Callable[[Any], Any]] = None,
        login_required: bool = True,
        **kwargs
    ) -> Any:
    """
    Quickly add pages to the dash application.

    Examples
    --------
    >>> import meerschaum as mrsm
    >>> from meerschaum.plugins import web_page
    >>> html = mrsm.attempt_import('dash.html')
    >>> 
    >>> @web_page('foo/bar', login_required=False)
    >>> def foo_bar():
    ...     return html.Div([html.H1("Hello, World!")])
    >>> 
    """
    page_str = None

    def _decorator(_func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        nonlocal page_str

        @functools.wraps(_func)
        def wrapper(*_args, **_kwargs):
            return _func(*_args, **_kwargs)

        if page_str is None:
            page_str = _func.__name__

        page_str = page_str.lstrip('/').rstrip('/').strip()
        _plugin_endpoints_to_pages[page_str] = {
            'function': _func,
            'login_required': login_required,
        }
        return wrapper

    if callable(page):
        decorator_to_return = _decorator(page)
        page_str = page.__name__
    else:
        decorator_to_return = _decorator
        page_str = page

    return decorator_to_return


_dash_plugins = {}
def dash_plugin(function: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Execute the function when starting the Dash application.
    """
    with _locks['_dash_plugins']:
        try:
            if function.__module__ not in _dash_plugins:
                _dash_plugins[function.__module__] = []
            _dash_plugins[function.__module__].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


def api_plugin(function: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Execute the function when initializing the Meerschaum API module.
    Useful for lazy-loading heavy plugins only when the API is started,
    such as when editing the `meerschaum.api.app` FastAPI app.
    
    The FastAPI app will be passed as the only parameter.
    
    Examples
    --------
    >>> from meerschaum.plugins import api_plugin
    >>>
    >>> @api_plugin
    >>> def initialize_plugin(app):
    ...     @app.get('/my/new/path')
    ...     def new_path():
    ...         return {'message': 'It works!'}
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


_synced_symlinks: bool = False
def sync_plugins_symlinks(debug: bool = False, warn: bool = True) -> None:
    """
    Update the plugins 
    """
    global _synced_symlinks
    with _locks['_synced_symlinks']:
        if _synced_symlinks:
            return

    import sys, os, pathlib, time
    from collections import defaultdict
    import importlib.util
    from meerschaum.utils.misc import flatten_list, make_symlink, is_symlink
    from meerschaum.utils.warnings import error, warn as _warn
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.venv import Venv, activate_venv, deactivate_venv, is_venv_active
    from meerschaum.config._paths import (
        PLUGINS_RESOURCES_PATH,
        PLUGINS_ARCHIVES_RESOURCES_PATH,
        PLUGINS_INIT_PATH,
        PLUGINS_DIR_PATHS,
        PLUGINS_INTERNAL_LOCK_PATH,
    )

    ### If the lock file exists, sleep for up to a second or until it's removed before continuing.
    with _locks['PLUGINS_INTERNAL_LOCK_PATH']:
        if PLUGINS_INTERNAL_LOCK_PATH.exists():
            lock_sleep_total     = STATIC_CONFIG['plugins']['lock_sleep_total']
            lock_sleep_increment = STATIC_CONFIG['plugins']['lock_sleep_increment']
            lock_start = time.perf_counter()
            while (
                (time.perf_counter() - lock_start) < lock_sleep_total
            ):
                time.sleep(lock_sleep_increment)
                if not PLUGINS_INTERNAL_LOCK_PATH.exists():
                    break
                try:
                    PLUGINS_INTERNAL_LOCK_PATH.unlink()
                except Exception as e:
                    if warn:
                        _warn(f"Error while removing lockfile {PLUGINS_INTERNAL_LOCK_PATH}:\n{e}")
                    break

        ### Begin locking from other processes.
        try:
            PLUGINS_INTERNAL_LOCK_PATH.touch()
        except Exception as e:
            if warn:
                _warn(f"Unable to create lockfile {PLUGINS_INTERNAL_LOCK_PATH}:\n{e}")

    with _locks['internal_plugins']:

        try:
            from importlib.metadata import entry_points
        except ImportError:
            importlib_metadata = attempt_import('importlib_metadata', lazy=False)
            entry_points = importlib_metadata.entry_points

        ### NOTE: Allow plugins to be installed via `pip`.
        packaged_plugin_paths = []
        discovered_packaged_plugins_eps = entry_points(group='meerschaum.plugins')
        for ep in discovered_packaged_plugins_eps:
            module_name = ep.name
            for package_file_path in ep.dist.files:
                if package_file_path.suffix != '.py':
                    continue
                if str(package_file_path) == f'{module_name}.py':
                    packaged_plugin_paths.append(package_file_path.locate())
                elif str(package_file_path) == f'{module_name}/__init__.py':
                    packaged_plugin_paths.append(package_file_path.locate().parent)

        if is_symlink(PLUGINS_RESOURCES_PATH) or not PLUGINS_RESOURCES_PATH.exists():
            try:
                PLUGINS_RESOURCES_PATH.unlink()
            except Exception as e:
                pass

        PLUGINS_RESOURCES_PATH.mkdir(exist_ok=True)

        existing_symlinked_paths = [
            (PLUGINS_RESOURCES_PATH / item) 
            for item in os.listdir(PLUGINS_RESOURCES_PATH)
        ]
        for plugins_path in PLUGINS_DIR_PATHS:
            if not plugins_path.exists():
                plugins_path.mkdir(exist_ok=True, parents=True)
        plugins_to_be_symlinked = list(flatten_list(
            [
                [
                    (plugins_path / item)
                    for item in os.listdir(plugins_path)
                    if (
                        not item.startswith('.')
                    ) and (item not in ('__pycache__', '__init__.py'))
                ]
                for plugins_path in PLUGINS_DIR_PATHS
            ]
        ))
        plugins_to_be_symlinked.extend(packaged_plugin_paths)

        ### Check for duplicates.
        seen_plugins = defaultdict(lambda: 0)
        for plugin_path in plugins_to_be_symlinked:
            plugin_name = plugin_path.stem
            seen_plugins[plugin_name] += 1
        for plugin_name, plugin_count in seen_plugins.items():
            if plugin_count > 1:
                if warn:
                    _warn(f"Found duplicate plugins named '{plugin_name}'.")

        for plugin_symlink_path in existing_symlinked_paths:
            real_path = pathlib.Path(os.path.realpath(plugin_symlink_path))

            ### Remove invalid symlinks.
            if real_path not in plugins_to_be_symlinked:
                if not is_symlink(plugin_symlink_path):
                    continue
                try:
                    plugin_symlink_path.unlink()
                except Exception as e:
                    pass

            ### Remove valid plugins from the to-be-symlinked list.
            else:
                plugins_to_be_symlinked.remove(real_path)

        for plugin_path in plugins_to_be_symlinked:
            plugin_symlink_path = PLUGINS_RESOURCES_PATH / plugin_path.name
            try:
                ### There might be duplicate folders (e.g. __pycache__).
                if (
                    plugin_symlink_path.exists()
                    and
                    plugin_symlink_path.is_dir()
                    and
                    not is_symlink(plugin_symlink_path)
                ):
                    continue
                success, msg = make_symlink(plugin_path, plugin_symlink_path)
            except Exception as e:
                success, msg = False, str(e)
            if not success:
                if warn:
                    _warn(
                        f"Failed to create symlink {plugin_symlink_path} "
                        + f"to {plugin_path}:\n    {msg}"
                    )

    ### Release symlink lock file in case other processes need it.
    with _locks['PLUGINS_INTERNAL_LOCK_PATH']:
        try:
            if PLUGINS_INTERNAL_LOCK_PATH.exists():
                PLUGINS_INTERNAL_LOCK_PATH.unlink()
        ### Sometimes competing threads will delete the lock file at the same time.
        except FileNotFoundError:
            pass
        except Exception as e:
            if warn:
                _warn(f"Error cleaning up lockfile {PLUGINS_INTERNAL_LOCK_PATH}:\n{e}")

        try:
            if not PLUGINS_INIT_PATH.exists():
                PLUGINS_INIT_PATH.touch()
        except Exception as e:
            error(f"Failed to create the file '{PLUGINS_INIT_PATH}':\n{e}")

    with _locks['__path__']:
        if str(PLUGINS_RESOURCES_PATH.parent) not in __path__:
            __path__.append(str(PLUGINS_RESOURCES_PATH.parent))

    with _locks['_synced_symlinks']:
        _synced_symlinks = True


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
    import os
    import importlib
    from meerschaum.utils.misc import flatten_list
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    from meerschaum.utils.venv import is_venv_active, activate_venv, deactivate_venv, Venv
    from meerschaum.utils.warnings import warn as _warn
    plugins_to_import = list(plugins_to_import)
    with _locks['sys.path']:

        ### Since plugins may depend on other plugins,
        ### we need to activate the virtual environments for library plugins.
        ### This logic exists in `Plugin.activate_venv()`,
        ### but that code requires the plugin's module to already be imported.
        ### It's not a guarantee of correct activation order,
        ### e.g. if a library plugin pins a specific package and another 
        plugins_names = get_plugins_names()
        already_active_venvs = [is_venv_active(plugin_name) for plugin_name in plugins_names]

        if not sys.path or sys.path[0] != str(PLUGINS_RESOURCES_PATH.parent):
            sys.path.insert(0, str(PLUGINS_RESOURCES_PATH.parent))

        if not plugins_to_import:
            for plugin_name in plugins_names:
                activate_venv(plugin_name)
            try:
                imported_plugins = importlib.import_module(PLUGINS_RESOURCES_PATH.stem)
            except ImportError as e:
                _warn(f"Failed to import the plugins module:\n    {e}")
                import traceback
                traceback.print_exc()
                imported_plugins = None
            for plugin_name in plugins_names:
                if plugin_name in already_active_venvs:
                    continue
                deactivate_venv(plugin_name)

        else:
            imported_plugins = []
            for plugin_name in flatten_list(plugins_to_import):
                plugin = Plugin(plugin_name)
                try:
                    with Venv(plugin):
                        imported_plugins.append(
                            importlib.import_module(
                                f'{PLUGINS_RESOURCES_PATH.stem}.{plugin_name}'
                            )
                        )
                except Exception as e:
                    _warn(
                        f"Failed to import plugin '{plugin_name}':\n    "
                        + f"{e}\n\nHere's a stacktrace:",
                        stack = False,
                    )
                    from meerschaum.utils.formatting import get_console
                    get_console().print_exception(
                        suppress = [
                            'meerschaum/plugins/__init__.py',
                            importlib,
                            importlib._bootstrap,
                        ]
                    )
                    imported_plugins.append(None)

        if imported_plugins is None and warn:
            _warn(f"Failed to import plugins.", stacklevel=3)

        if str(PLUGINS_RESOURCES_PATH.parent) in sys.path:
            sys.path.remove(str(PLUGINS_RESOURCES_PATH.parent))

    if isinstance(imported_plugins, list):
        return (imported_plugins[0] if len(imported_plugins) == 1 else tuple(imported_plugins))
    return imported_plugins


def from_plugin_import(plugin_import_name: str, *attrs: str) -> Any:
    """
    Emulate the `from module import x` behavior.

    Parameters
    ----------
    plugin_import_name: str
        The import name of the plugin's module.
        Separate submodules with '.' (e.g. 'compose.utils.pipes')

    attrs: str
        Names of the attributes to return.

    Returns
    -------
    Objects from a plugin's submodule.
    If multiple objects are provided, return a tuple.

    Examples
    --------
    >>> init = from_plugin_import('compose.utils', 'init')
    >>> with mrsm.Venv('compose'):
    ...     cf = init()
    >>> build_parent_pipe, get_defined_pipes = from_plugin_import(
    ...     'compose.utils.pipes',
    ...     'build_parent_pipe',
    ...     'get_defined_pipes',
    ... )
    >>> parent_pipe = build_parent_pipe(cf)
    >>> defined_pipes = get_defined_pipes(cf)
    """
    import importlib
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    from meerschaum.utils.warnings import warn as _warn
    if plugin_import_name.startswith('plugins.'):
        plugin_import_name = plugin_import_name[len('plugins.'):]
    plugin_import_parts = plugin_import_name.split('.')
    plugin_root_name = plugin_import_parts[0]
    plugin = mrsm.Plugin(plugin_root_name)

    submodule_import_name = '.'.join(
        [PLUGINS_RESOURCES_PATH.stem]
        + plugin_import_parts
    )
    if len(attrs) == 0:
        raise ValueError(f"Provide which attributes to return from '{submodule_import_name}'.")

    attrs_to_return = []
    with mrsm.Venv(plugin):
        if plugin.module is None:
            return None

        try:
            submodule = importlib.import_module(submodule_import_name)
        except ImportError as e:
            _warn(
                f"Failed to import plugin '{submodule_import_name}':\n    "
                + f"{e}\n\nHere's a stacktrace:",
                stack=False,
            )
            from meerschaum.utils.formatting import get_console
            get_console().print_exception(
                suppress=[
                    'meerschaum/plugins/__init__.py',
                    importlib,
                    importlib._bootstrap,
                ]
            )
            return None

        for attr in attrs:
            try:
                attrs_to_return.append(getattr(submodule, attr))
            except Exception:
                _warn(f"Failed to access '{attr}' from '{submodule_import_name}'.")
                attrs_to_return.append(None)
        
        if len(attrs) == 1:
            return attrs_to_return[0]

        return tuple(attrs_to_return)


def load_plugins(debug: bool = False, shell: bool = False) -> None:
    """
    Import Meerschaum plugins and update the actions dictionary.
    """
    from inspect import isfunction, getmembers
    from meerschaum.actions import __all__ as _all, modules
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
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
        [
            mod for mod in modules
            if not mod.__name__.startswith(PLUGINS_RESOURCES_PATH.stem + '.')
        ]
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


def get_plugins(*to_load, try_import: bool = True) -> Union[Tuple[Plugin], Plugin]:
    """
    Return a list of `Plugin` objects.

    Parameters
    ----------
    to_load:
        If specified, only load specific plugins.
        Otherwise return all plugins.

    try_import: bool, default True
        If `True`, allow for plugins to be imported.
    """
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    import os
    sync_plugins_symlinks()
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
    plugins = tuple(plugin for plugin in _plugins if plugin.is_installed(try_import=try_import))
    if len(to_load) == 1:
        if len(plugins) == 0:
            raise ValueError(f"Plugin '{to_load[0]}' is not installed.")
        return plugins[0]
    return plugins


def get_plugins_names(*to_load, **kw) -> List[str]:
    """
    Return a list of installed plugins.
    """
    return [plugin.name for plugin in get_plugins(*to_load, **kw)]


def get_plugins_modules(*to_load, **kw) -> List['ModuleType']:
    """
    Return a list of modules for the installed plugins, or `None` if things break.
    """
    return [plugin.module for plugin in get_plugins(*to_load, **kw)]


def get_data_plugins() -> List[Plugin]:
    """
    Only return the modules of plugins with either `fetch()` or `sync()` functions.
    """
    import inspect
    plugins = get_plugins()
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
    from meerschaum._internal.arguments._parser import groups, _seen_plugin_args, parser
    from meerschaum.utils.warnings import warn, error
    _parent_plugin_name = _get_parent_plugin(2)
    title = f"Plugin '{_parent_plugin_name}' options" if _parent_plugin_name else 'Custom options'
    group_key = 'plugin_' + (_parent_plugin_name or '')
    if group_key not in groups:
        groups[group_key] = parser.add_argument_group(
            title = title,
        )
        _seen_plugin_args[group_key] = set()
    try:
        if str(args) not in _seen_plugin_args[group_key]:
            groups[group_key].add_argument(*args, **kwargs)
            _seen_plugin_args[group_key].add(str(args))
    except Exception as e:
        warn(e)


def _get_parent_plugin(stacklevel: int = 1) -> Union[str, None]:
    """If this function is called from outside a Meerschaum plugin, it will return None."""
    import inspect, re
    try:
        parent_globals = inspect.stack()[stacklevel][0].f_globals
        parent_file = parent_globals.get('__file__', '')
        return parent_globals['__name__'].replace('plugins.', '').split('.')[0]
    except Exception as e:
        return None
