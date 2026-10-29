#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Expose plugin management APIs from the `meerschaum.plugins` module.
"""

from __future__ import annotations

import contextlib
import pathlib
import functools
from collections import defaultdict

import meerschaum as mrsm
from meerschaum.utils.typing import Callable, Any, Union, Optional, Dict, List, Tuple
from meerschaum.utils.threading import RLock
from meerschaum.core.Plugin import Plugin
from meerschaum.plugins._page import figure, figure_json

_api_plugins: Dict[str, List[Callable[['fastapi.App'], Any]]] = {}
_plugins_import_errors: Dict[str, Exception] = {}
_pre_sync_hooks: Dict[Union[str, None], List[Callable[[Any], Any]]] = {}
_post_sync_hooks: Dict[Union[str, None], List[Callable[[Any], Any]]] = {}
_actions_daemon_enabled: Dict[str, bool] = {}
### The words of each plugin action (`{'weather_wait': ['weather', 'wait']}`):
### `@make_action(name=...)` sets them, an underscore name (`def weather_wait`) splits on `_`.
### The plugin manifest carries them so the CLI lists `wait` under `mrsm weather` in help and
### completion.
_action_words: Dict[str, List[str]] = {}
### Tags added to the head of every console page with `add_head()`, by plugin.
_console_head: Dict[Union[str, None], List[str]] = {}
_locks = {
    '_api_plugins': RLock(),
    '_dash_plugins': RLock(),
    '_pre_sync_hooks': RLock(),
    '_post_sync_hooks': RLock(),
    '_actions_daemon_enabled': RLock(),
    '_console_head': RLock(),
    '__path__': RLock(),
    'sys.path': RLock(),
    'internal_plugins': RLock(),
    '_synced_symlinks': RLock(),
    'PLUGINS_INTERNAL_LOCK_PATH': RLock(),
}
__all__ = (
    "Plugin",
    "make_action",
    "api_plugin",
    "dash_plugin",
    "web_page",
    "web_callback",
    "add_head",
    "import_plugins",
    "from_plugin_import",
    "invalidate_plugins_cache",
    "reload_plugins",
    "get_plugins",
    "get_data_plugins",
    "add_plugin_argument",
    "pre_sync_hook",
    "post_sync_hook",
    "figure",
    "figure_json",
)
__pdoc__ = {
    'venvs': False,
    'data': False,
    'stack': False,
    'plugins': False,
}

_FIRST_PARTY_PLUGIN_MODULES = {
    'compose': 'meerschaum.compose',
}


def _import_first_party_plugin(plugin_import_name: str):
    """Import a compatibility alias for a plugin which moved into Meerschaum core."""
    import importlib

    plugin_import_parts = plugin_import_name.split('.')
    module_root = _FIRST_PARTY_PLUGIN_MODULES.get(plugin_import_parts[0], None)
    if module_root is None:
        return None
    return importlib.import_module('.'.join([module_root] + plugin_import_parts[1:]))


def _get_plugins_package_name() -> str:
    """
    Return the import name of the plugins package (`plugins`)
    without resolving (and creating) the plugins resources directory.
    """
    from meerschaum.config._paths import paths
    path_parts = paths['PLUGINS_RESOURCES_PATH']
    last_part = path_parts[-1] if isinstance(path_parts, (list, tuple)) else path_parts
    return pathlib.Path(last_part).stem


### Set by the worker program (`crates/meerschaum-worker/python/host.py`).
_IN_WORKER: bool = False


@contextlib.contextmanager
def plugin_import_guard():
    """
    Hold while plugin modules are imported. In a worker's interpreter no worker is started
    meanwhile (`_core.plugin_import_begin()`): a plugin whose import syncs a `plugin:` pipe or
    calls `mrsm.entry()` would otherwise start workers without end, each importing it again.
    Anywhere else this does nothing.
    """
    if not _IN_WORKER:
        yield
        return
    from meerschaum import _core
    _core.plugin_import_begin()
    try:
        yield
    finally:
        _core.plugin_import_end()


def _get_loaded_module(module_name: str) -> Union['ModuleType', None]:
    """
    Return a fully-initialized module from `sys.modules`, else `None`.
    A module still being imported (e.g. by another thread) is not returned.
    """
    import sys
    module = sys.modules.get(module_name, None)
    if module is None:
        return None
    if getattr(getattr(module, '__spec__', None), '_initializing', False):
        return None
    return module


def _action_name_words(name: Any) -> List[str]:
    """
    Return the words of an explicit action name (`'weather wait'` -> `['weather', 'wait']`),
    or raise a `ValueError` for a name that cannot be typed as an action.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"An action name must be a non-empty string, not {name!r}.")
    words = name.split()
    for word in words:
        if word.startswith('-'):
            raise ValueError(
                f"Invalid action name {name!r}: the word '{word}' would be read as a flag."
            )
        ### The action is registered as the words joined by `_`, which is how the dispatcher
        ### finds and strips them: a `_` inside a word would split it there.
        if '_' in word:
            raise ValueError(
                f"Invalid action name {name!r}: write '{word}' as separate words "
                f"('{word.replace('_', ' ')}') or with a hyphen ('{word.replace('_', '-')}')."
            )
    return words


def _named_action(func: Callable[[Any], Any], action_key: str) -> Callable[[Any], Any]:
    """
    Return `func` registered under `action_key` (its `__name__`), as an underscore action is:
    the dispatcher strips the action's words from `action` by the function's name.
    The signature and docstring are `func`'s (`functools.wraps`).
    """
    @functools.wraps(func)
    def _action(*args, **kwargs):
        return func(*args, **kwargs)

    _action.__name__ = action_key
    _action.__qualname__ = action_key
    return _action


def make_action(
    function: Union[Callable[[Any], Any], str, None] = None,
    shell: bool = False,
    activate: bool = True,
    deactivate: bool = True,
    debug: bool = False,
    daemon: bool = True,
    skip_if_loaded: bool = True,
    name: Optional[str] = None,
    _plugin_name: Optional[str] = None,
) -> Callable[[Any], Any]:
    """
    Make a function a Meerschaum action. Useful for plugins that are adding multiple actions.

    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to become a Meerschaum action. Must accept all keyword arguments.

    shell: bool, default False
        Not used.

    daemon: bool, default True
        If `False`, the action may not be detached with `-d`.

    name: Optional[str], default None
        The words that run the action, e.g. `'weather wait'` for `mrsm weather wait` (and
        `mrsm weather wait other words`, which arrive as `action=['other', 'words']`).
        Defaults to the function's name, where an underscore separates words: `def weather_wait`
        also runs as `mrsm weather wait`. `@make_action('weather wait')` is the same as
        `@make_action(name='weather wait')`.

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
    >>> @make_action(name='weather wait')
    ... def wait_for_counts(action=None, **kw):
    ...     return True, f"Waited for {action}."
    >>>
    """
    if isinstance(function, str):
        name, function = function, None
    words = _action_name_words(name) if name is not None else None

    def _decorator(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        from meerschaum.actions import actions, _custom_actions_plugins, _plugins_actions
        action_key = '_'.join(words) if words else func.__name__
        plugin_name = _plugin_name or _get_parent_plugin(function=func)

        if skip_if_loaded and action_key in actions:
            existing = actions[action_key]
            same_function = existing is func or getattr(existing, '__wrapped__', None) is func
            if words and not same_function and _custom_actions_plugins.get(action_key) != plugin_name:
                from meerschaum.utils.warnings import warn
                warn(
                    f"The action '{' '.join(words)}' already exists; "
                    f"plugin '{plugin_name}' does not replace it.",
                    stack=False,
                )
            return func

        plugin = Plugin(plugin_name) if plugin_name else None

        if debug:
            from meerschaum.utils.debug import dprint
            dprint(
                f"Adding action '{action_key}' from plugin "
                f"'{plugin}'..."
            )

        registered = func if action_key == func.__name__ else _named_action(func, action_key)
        actions[action_key] = registered
        _custom_actions_plugins[action_key] = plugin_name
        if plugin_name not in _plugins_actions:
            _plugins_actions[plugin_name] = []
        _plugins_actions[plugin_name].append(action_key)
        _action_words[action_key] = list(words) if words else [
            word for word in action_key.split('_') if word
        ] or [action_key]
        if not daemon:
            _actions_daemon_enabled[action_key] = False
        return func

    if function is None:
        return _decorator
    return _decorator(function)


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
        plugin_name = _get_parent_plugin(function=function)
        try:
            if plugin_name not in _pre_sync_hooks:
                _pre_sync_hooks[plugin_name] = []
            _pre_sync_hooks[plugin_name].append(function)
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
    >>> from meerschaum.utils.misc import interval_str
    >>> from datetime import timedelta
    >>>
    >>> @post_sync_hook
    ... def log_sync(pipe, success_tuple, duration=None, **kwargs):
    ...     duration_delta = timedelta(seconds=duration)
    ...     duration_text = interval_str(duration_delta)
    ...     print(f"It took {duration_text} to sync {pipe}.")
    >>>
    """
    with _locks['_post_sync_hooks']:
        try:
            plugin_name = _get_parent_plugin(function=function)
            if plugin_name not in _post_sync_hooks:
                _post_sync_hooks[plugin_name] = []
            _post_sync_hooks[plugin_name].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


### The Bootstrap themes the console ships: `@web_page(theme=...)` names them, or gives the URL of
### another theme's stylesheet.
_CONSOLE_THEMES: Dict[str, str] = {
    'dark': 'dark',
    'darkly': 'dark',
    'light': 'light',
    'flatly': 'light',
}
### What `@web_page(head=[...])` and `add_head(...)` may add as a dictionary or a Dash component.
_HEAD_TAGS = ('meta', 'link', 'script', 'style', 'noscript')
_VOID_HEAD_TAGS = ('meta', 'link')


def _normalize_theme(theme: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return `(kind, url)` for a `@web_page(theme=...)` value: `('dark', None)`, `('light', None)`,
    `('url', <stylesheet>)`, or `(None, None)` for the default (a warning names an unknown value).
    """
    if theme is None:
        return None, None
    text = str(theme).strip()
    if text.lower() in _CONSOLE_THEMES:
        return _CONSOLE_THEMES[text.lower()], None
    if text.startswith(('https://', 'http://', '//', '/')):
        return 'url', text
    from meerschaum.utils.warnings import warn
    warn(
        f"Unknown web page theme {theme!r}: use 'dark', 'light' or the URL of a Bootstrap "
        "stylesheet (for example `dash_bootstrap_components.themes.LUX`). Using the default theme.",
        stack=False,
    )
    return None, None


def _head_tag_html(tag: Any) -> Union[str, None]:
    """
    Return the HTML of one head tag: a string is used as is (one or more tags), a dictionary is
    `{'tag': 'meta', 'name': ..., 'content': ...}` (text of a `script` or `style` under
    `children`), and a Dash component (`html.Link(...)`, `html.Meta(...)`, `html.Script(...)`) is
    rendered from its properties.
    """
    import html as html_lib
    if tag is None:
        return None
    if isinstance(tag, str):
        return tag.strip() or None
    if not isinstance(tag, dict) and hasattr(tag, 'to_plotly_json'):
        component = tag.to_plotly_json()
        props = {
            key: value
            for key, value in (component.get('props', {}) or {}).items()
            if value is not None
        }
        tag = {'tag': str(component.get('type', '')).lower(), **props}
    if not isinstance(tag, dict):
        raise TypeError(
            f"A head tag must be an HTML string, a dictionary or a Dash component, not {tag!r}."
        )
    attributes = dict(tag)
    name = str(attributes.pop('tag', '') or '').lower()
    if name not in _HEAD_TAGS:
        raise ValueError(
            f"Unsupported head tag {name!r}: use one of {', '.join(_HEAD_TAGS)}."
        )
    text = None
    for key in ('children', 'content_text', 'text'):
        if key in attributes:
            text = attributes.pop(key)
    renamed = {'className': 'class', 'htmlFor': 'for', 'httpEquiv': 'http-equiv'}
    parts = [name]
    for key, value in attributes.items():
        key = renamed.get(key, key)
        if value is None or value is False:
            continue
        if value is True:
            parts.append(html_lib.escape(str(key), quote=True))
            continue
        parts.append(
            f'{html_lib.escape(str(key), quote=True)}="{html_lib.escape(str(value), quote=True)}"'
        )
    opening = '<' + ' '.join(parts) + '>'
    if name in _VOID_HEAD_TAGS:
        return opening
    body = '' if text is None else str(text)
    if name in ('script', 'style'):
        body = body.replace('</', '<\\/')
    else:
        body = html_lib.escape(body)
    return f'{opening}{body}</{name}>'


def _head_tags_html(tags: Any) -> List[str]:
    """
    Return the HTML of each of `tags` (one tag or a list of them), see `_head_tag_html()`.
    """
    if tags is None:
        return []
    if isinstance(tags, (str, dict)) or hasattr(tags, 'to_plotly_json'):
        tags = [tags]
    out = []
    for tag in tags:
        rendered = _head_tag_html(tag)
        if rendered:
            out.append(rendered)
    return out


def add_head(*tags: Any) -> None:
    """
    Add tags to the `<head>` of every page of the web console (and of the documents of
    page-contract pages): meta tags, stylesheets, scripts, fonts, a web app manifest...

    Each tag is an HTML string, a dictionary (`{'tag': 'meta', 'name': 'theme-color',
    'content': '#222'}`; the text of a `script` or `style` goes under `children`) or a Dash
    component (`html.Link(rel='stylesheet', href='...')`). Call it at the top level of your plugin
    (it applies when the console starts); `@web_page(head=[...])` adds tags to one page only.

    Examples
    --------
    >>> from meerschaum.plugins import add_head
    >>> add_head(
    ...     '<meta name="theme-color" content="#222222">',
    ...     {'tag': 'link', 'rel': 'preconnect', 'href': 'https://fonts.gstatic.com'},
    ... )
    """
    rendered = _head_tags_html(list(tags))
    if not rendered:
        return
    plugin_name = _get_parent_plugin()
    with _locks['_console_head']:
        existing = _console_head.setdefault(plugin_name, [])
        for tag in rendered:
            if tag not in existing:
                existing.append(tag)


def get_console_head() -> List[str]:
    """
    Return the HTML of every tag added with `add_head()`, in the order the plugins added them.
    """
    with _locks['_console_head']:
        return [tag for tags in _console_head.values() for tag in tags]


_plugin_endpoints_to_pages = {}
_plugins_web_pages = {}
def web_page(
    page: Union[str, None, Callable[[Any], Any]] = None,
    login_required: bool = True,
    skip_navbar: bool = False,
    page_group: Optional[str] = None,
    dark_theme: Optional[bool] = None,
    theme: Optional[str] = None,
    head: Optional[List[Any]] = None,
    **kwargs
) -> Any:
    """
    Quickly add pages to the dash application.

    Parameters
    ----------
    page: Union[str, None, Callable[[Any], Any]], default None
        The page's path in the console (`'foo/bar'` is served at `/dash/foo/bar`; sub-paths
        such as `/dash/foo/bar/1` route to it too). Defaults to the function's name.

    login_required: bool, default True
        If `True`, visitors without a console session see the login page instead.
        A public page (`login_required=False`) runs its callbacks for anonymous visitors only if
        they are public too: register them with `@web_callback(..., public=True)` or inside a
        `@dash_plugin(public=True)` function.

    skip_navbar: bool, default False
        If `True`, the console's navigation bar is not added above the page.

    navbar: bool, default False
        If `True`, the page gets the console's top navigation (its pages, the instance selector
        and the user menu). By default a plugin's page keeps its own layout under a plain bar
        (the logo, a link to the Overview, and Sign out).

    page_group: Optional[str], default None
        The group the page is listed under (defaults to the plugin's name).

    dark_theme: Optional[bool], default None
        Whether the `dbc_dark` component overrides apply to this page. By default they do for the
        console's dark theme and do not for `theme='light'` or a custom stylesheet.
        `dark_theme=False` without a `theme` gives the light theme, as before.

    theme: Optional[str], default None
        The Bootstrap theme of this page: `'dark'` (the console's theme, the default), `'light'`,
        or the URL of a Bootstrap stylesheet (for example `dash_bootstrap_components.themes.LUX`,
        or a file you serve with `meerschaum.api.mount_static`). Exactly one theme stylesheet
        applies while the page is open, and the console's theme returns on other pages.

    head: Optional[List[Any]], default None
        Tags to add to the page's `<head>` while it is open: HTML strings, dictionaries
        (`{'tag': 'link', 'rel': 'stylesheet', 'href': '...'}`) or Dash components
        (`html.Script(src='...')`). Stylesheets and meta tags apply only while the page is open;
        a script runs once, the first time the page opens. `add_head()` adds tags to every page.

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
    >>> @web_page('reports', theme='light', head=['<meta name="robots" content="noindex">'])
    >>> def reports():
    ...     return html.Div([html.H1("Reports")])
    >>>
    """
    page_str = None
    theme_kind, theme_url = _normalize_theme(theme)
    if theme_kind in ('dark', 'light'):
        if dark_theme is not None and dark_theme != (theme_kind == 'dark'):
            from meerschaum.utils.warnings import warn
            warn(
                f"`@web_page(theme={theme!r}, dark_theme={dark_theme})`: "
                f"the {theme_kind} theme applies.",
                stack=False,
            )
        resolved_dark = theme_kind == 'dark'
    elif theme_kind == 'url':
        resolved_dark = bool(dark_theme) if dark_theme is not None else False
    else:
        resolved_dark = True if dark_theme is None else bool(dark_theme)
    resolved_theme = theme_url if theme_kind == 'url' else ('dark' if resolved_dark else 'light')
    head_tags = _head_tags_html(head)

    def _decorator(_func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        nonlocal page_str, page_group

        @functools.wraps(_func)
        def wrapper(*_args, **_kwargs):
            return _func(*_args, **_kwargs)

        if page_str is None:
            page_str = _func.__name__

        page_str = page_str.lstrip('/').rstrip('/').strip()
        if not page_str.startswith('dash'):
            page_str = f'/dash/{page_str}'
        page_key = (
            ' '.join(
                [
                    word.capitalize()
                    for word in (
                        page_str.replace('/dash', '').lstrip('/').rstrip('/').strip()
                        .replace('-', ' ').replace('_', ' ').split(' ')
                    )
                ]
            )
        )

        plugin_name = _get_parent_plugin(function=_func)
        page_group = page_group or plugin_name
        if page_group not in _plugin_endpoints_to_pages:
            _plugin_endpoints_to_pages[page_group] = {}
        _plugin_endpoints_to_pages[page_group][page_str] = {
            'function': _func,
            'login_required': login_required,
            'skip_navbar': skip_navbar,
            ### The console's top navigation instead of the plain bar (`build_plugin_navbar`).
            'navbar': bool(kwargs.get('navbar', False)),
            'page_key': page_key,
            'dark_theme': resolved_dark,
            'theme': resolved_theme,
            'head': list(head_tags),
        }
        if plugin_name not in _plugins_web_pages:
            _plugins_web_pages[plugin_name] = []
        _plugins_web_pages[plugin_name].append(_func)
        return wrapper

    if callable(page):
        decorator_to_return = _decorator(page)
        page_str = page.__name__
    else:
        decorator_to_return = _decorator
        page_str = page

    return decorator_to_return


### Dash callbacks are public (served without a console session) when their function carries this
### attribute: `@web_callback(..., public=True)`, the callbacks a `@dash_plugin(public=True)` function
### registers, and the console's login and registration callbacks. The API host's guard
### (`meerschaum.api.host._callbacks`) follows `__wrapped__` from the function Dash calls.
_PUBLIC_CALLBACK_ATTR: str = '_mrsm_public_callback'


def _mark_public_callback(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Mark a Dash callback function as public and return it.
    """
    for target in (func, getattr(func, '__func__', None)):
        if target is None:
            continue
        try:
            setattr(target, _PUBLIC_CALLBACK_ATTR, True)
            return func
        except (AttributeError, TypeError):
            continue
    from meerschaum.utils.warnings import warn
    warn(f"Could not mark the callback {func!r} as public.", stack=False)
    return func


def _is_public_callback(func: Any) -> bool:
    """
    Return whether the function Dash calls for a callback (or one it wraps) is marked public.
    """
    for _ in range(32):
        if func is None:
            return False
        if getattr(func, _PUBLIC_CALLBACK_ATTR, False) is True:
            return True
        func = getattr(func, '__wrapped__', None) or getattr(func, '__func__', None)
    return False


def _built_dash_app() -> Any:
    """
    Return the console's Dash app if this process built it (never builds it).
    """
    import sys
    module = sys.modules.get('meerschaum.api.dash', None)
    if module is None:
        return None
    return module.__dict__.get('dash_app', None)


def _callback_registrations(dash_app: Any) -> Dict[str, Any]:
    """
    Snapshot the places a Dash callback can be registered in: the app's callback map, the
    `dash_extensions` blueprint of a `DashProxy` app, and the global `dash.callback` and
    `dash_extensions.enrich.callback` registries.
    """
    import sys
    dash_callback = sys.modules.get('dash._callback', None)
    enrich = sys.modules.get('dash_extensions.enrich', None)
    blueprint = getattr(dash_app, 'blueprint', None)
    global_blueprint = getattr(enrich, 'GLOBAL_BLUEPRINT', None)
    return {
        'map': dict(getattr(dash_app, 'callback_map', None) or {}),
        'global_map': dict(getattr(dash_callback, 'GLOBAL_CALLBACK_MAP', None) or {}),
        'blueprint': list(getattr(blueprint, 'callbacks', None) or []),
        'global_blueprint': list(getattr(global_blueprint, 'callbacks', None) or []),
    }


@contextlib.contextmanager
def _registering_public_callbacks(dash_app: Any):
    """
    Mark every Dash callback registered inside this block public (whichever way it was
    registered: `dash_app.callback`, `dash.callback` or `dash_extensions.enrich.callback`).
    """
    before = _callback_registrations(dash_app)
    try:
        yield
    finally:
        after = _callback_registrations(dash_app)
        functions = []
        for key in ('blueprint', 'global_blueprint'):
            known = {id(item) for item in before[key]}
            functions.extend(
                getattr(item, 'f', None) for item in after[key] if id(item) not in known
            )
        for key in ('map', 'global_map'):
            functions.extend(
                (spec or {}).get('callback', None)
                for callback_id, spec in after[key].items()
                if callback_id not in before[key]
            )
        for func in functions:
            if callable(func):
                _mark_public_callback(func)


_dash_plugins = {}
def dash_plugin(
    function: Optional[Callable[[Any], Any]] = None,
    *,
    public: bool = False,
) -> Callable[[Any], Any]:
    """
    Execute the function when starting the Dash application (the web console), with the console's
    `dash_app` as its argument: register your callbacks in it with `@dash_app.callback(...)`.

    Parameters
    ----------
    public: bool, default False
        Every callback the function registers runs without a console session. By default a
        callback answers only signed-in visitors (`api:dash:callbacks_require_session`); make
        callbacks public only for pages anyone may use (`@web_page(login_required=False)`), and
        use `@web_callback(..., public=True)` to choose callbacks one at a time.

    Examples
    --------
    >>> from meerschaum.plugins import dash_plugin
    >>>
    >>> @dash_plugin(public=True)
    ... def init_dash(dash_app):
    ...     from dash import Input, Output
    ...     @dash_app.callback(Output('greeting', 'children'), Input('name', 'value'))
    ...     def greet(name):
    ...         return f"Hello, {name}!"
    """
    def _decorator(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        registered = func
        if public:
            @functools.wraps(func)
            def _public_dash_plugin(dash_app, *args, **kwargs):
                with _registering_public_callbacks(dash_app):
                    return func(dash_app, *args, **kwargs)
            registered = _public_dash_plugin

        with _locks['_dash_plugins']:
            plugin_name = _get_parent_plugin(function=func)
            try:
                if plugin_name not in _dash_plugins:
                    _dash_plugins[plugin_name] = []
                _dash_plugins[plugin_name].append(registered)
            except Exception as e:
                from meerschaum.utils.warnings import warn
                warn(e)
        return func

    if function is not None:
        if not callable(function):
            raise TypeError("`@dash_plugin` decorates a function.")
        return _decorator(function)
    return _decorator


def _dash_dependencies(kind: str, specs: Any) -> List[Any]:
    """
    Return Dash dependencies for `web_callback(output=..., inputs=..., state=...)`: each spec is an
    `'id.property'` string, an `(id, property)` tuple or a Dash dependency (kept as is).
    """
    from dash.dependencies import Input, Output, State
    dependency_class = {'output': Output, 'inputs': Input, 'state': State}[kind]
    if isinstance(specs, (str, tuple)):
        specs = [specs]
    out = []
    for spec in specs:
        if isinstance(spec, str):
            component_id, dot, prop = spec.strip().rpartition('.')
            if not dot or not component_id or not prop:
                raise ValueError(
                    f"`@web_callback({kind}=...)` takes 'component-id.property' strings, not {spec!r}."
                )
            out.append(dependency_class(component_id, prop))
        elif isinstance(spec, tuple) and len(spec) == 2:
            out.append(dependency_class(*spec))
        else:
            out.append(spec)
    return out


def web_callback(
    *args: Any,
    public: bool = False,
    output: Any = None,
    inputs: Any = None,
    state: Any = None,
    **kwargs: Any
) -> Callable[[Any], Any]:
    """
    Register a Dash callback on the web console: `@web_callback(...)` takes what
    `@dash_app.callback(...)` takes (outputs, inputs, states, `prevent_initial_call`, ...),
    at the top level of your plugin, without a `@dash_plugin` function.

    Parameters
    ----------
    public: bool, default False
        If `True`, the callback runs for visitors without a console session (for a page with
        `@web_page(login_required=False)`). Otherwise the console answers it only for signed-in
        visitors and sends the others to the login page.

    output: Union[str, List[str], None], default None
        The outputs as `'component-id.property'` strings (instead of `Output(...)` arguments), so
        your plugin need not import Dash: it is imported when the console starts.

    inputs: Union[str, List[str], None], default None
        The inputs, likewise (`'my-button.n_clicks'`).

    state: Union[str, List[str], None], default None
        The states, likewise (`'session-store.data'`).

    Examples
    --------
    >>> from meerschaum.plugins import web_page, web_callback
    >>>
    >>> @web_callback(output='visits.children', inputs='visit-button.n_clicks', public=True)
    ... def count_visits(n_clicks):
    ...     return f"{n_clicks or 0} visits"
    >>>
    >>> from dash import Input, Output
    >>> @web_callback(Output('clock', 'children'), Input('tick', 'n_intervals'))
    ... def clock(n_intervals):
    ...     return str(n_intervals)
    """
    if (
        len(args) == 1 and not kwargs and output is None and inputs is None and state is None
        and callable(args[0]) and not hasattr(args[0], 'component_id')
    ):
        raise TypeError(
            "`@web_callback` takes the callback's outputs and inputs, e.g. "
            "`@web_callback(output='out.children', inputs='button.n_clicks')`."
        )

    def _arguments() -> Tuple[List[Any], Dict[str, Any]]:
        """The positional and keyword arguments for `dash_app.callback` (Dash imported now)."""
        dependencies = list(args)
        flexible = dict(kwargs)
        for kind, specs in (('output', output), ('inputs', inputs), ('state', state)):
            if specs is None:
                continue
            if isinstance(specs, dict):
                flexible[kind] = specs
                continue
            dependencies.extend(_dash_dependencies(kind, specs))
        return dependencies, flexible

    def _decorator(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        if public:
            _mark_public_callback(func)
        dash_app = _built_dash_app()
        if dash_app is not None:
            dependencies, flexible = _arguments()
            dash_app.callback(*dependencies, **flexible)(func)
            return func

        ### The console is built later (by the API host): register the callback then, as the
        ### `@dash_plugin` functions are.
        def _register(app):
            dependencies, flexible = _arguments()
            app.callback(*dependencies, **flexible)(func)

        for attr in ('__module__', '__name__', '__qualname__', '__doc__'):
            try:
                setattr(_register, attr, getattr(func, attr))
            except (AttributeError, TypeError):
                pass
        with _locks['_dash_plugins']:
            plugin_name = _get_parent_plugin(function=func)
            if plugin_name not in _dash_plugins:
                _dash_plugins[plugin_name] = []
            _dash_plugins[plugin_name].append(_register)
        return func

    return _decorator


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
            plugin_name = _get_parent_plugin(function=function)
            if plugin_name not in _api_plugins:
                _api_plugins[plugin_name] = []
            _api_plugins[plugin_name].append(function)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)
    return function


_synced_symlinks: int = 0
_injected_plugin_symlinks = defaultdict(lambda: 0)
def sync_plugins_symlinks(debug: bool = False, warn: bool = True) -> None:
    """
    Update the plugins' internal symlinks. 
    """
    from meerschaum.utils.warnings import error, warn as _warn, dprint
    global _synced_symlinks
    with _locks['_synced_symlinks']:
        if _synced_symlinks > 1:
            if debug:
                dprint("Skip syncing symlinks...")
            return

    import os
    import pathlib
    from collections import defaultdict
    from meerschaum.utils.misc import flatten_list, make_symlink, is_symlink
    import meerschaum.config.paths as paths
    from meerschaum.utils.locks import InterProcessLock

    with InterProcessLock(paths.PLUGINS_INTERNAL_LOCK_PATH), _locks['internal_plugins']:

        try:
            from importlib.metadata import entry_points
        except ImportError:
            importlib_metadata = mrsm.attempt_import('importlib_metadata', lazy=False)
            entry_points = importlib_metadata.entry_points

        ### NOTE: Allow plugins to be installed via `pip`.
        packaged_plugin_paths = []
        try:
            discovered_packaged_plugins_eps = entry_points(group='meerschaum.plugins')
        except TypeError:
            discovered_packaged_plugins_eps = []

        for ep in discovered_packaged_plugins_eps:
            module_name = ep.name
            for package_file_path in ep.dist.files:
                if package_file_path.suffix != '.py':
                    continue
                if str(package_file_path) == f'{module_name}.py':
                    packaged_plugin_paths.append(package_file_path.locate())
                elif str(package_file_path) == f'{module_name}/__init__.py':
                    packaged_plugin_paths.append(package_file_path.locate().parent)

        if is_symlink(paths.PLUGINS_RESOURCES_PATH) or not paths.PLUGINS_RESOURCES_PATH.exists():
            try:
                paths.PLUGINS_RESOURCES_PATH.unlink()
            except Exception:
                pass

        paths.PLUGINS_RESOURCES_PATH.mkdir(exist_ok=True)

        existing_symlinked_paths = {
            _existing_symlink: pathlib.Path(os.path.realpath(_existing_symlink))
            for item in os.listdir(paths.PLUGINS_RESOURCES_PATH)
            if is_symlink(_existing_symlink := (paths.PLUGINS_RESOURCES_PATH / item))
        }
        injected_symlinked_paths = {
            _injected_symlink: pathlib.Path(os.path.realpath(_injected_symlink))
            for item in os.listdir(paths.PLUGINS_INJECTED_RESOURCES_PATH)
            if is_symlink(_injected_symlink := (paths.PLUGINS_INJECTED_RESOURCES_PATH / item))
        }
        plugins_to_be_symlinked = list(flatten_list(
            [
                [
                    pathlib.Path(os.path.realpath(plugins_path / item))
                    for item in os.listdir(plugins_path)
                    if (
                        not item.startswith('.')
                    ) and (item not in ('__pycache__', '__init__.py'))
                ]
                for plugins_path in paths.PLUGINS_DIR_PATHS
                if plugins_path.exists()
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

        for plugin_symlink_path, real_path in existing_symlinked_paths.items():

            ### Remove invalid symlinks.
            if real_path not in plugins_to_be_symlinked:
                if _injected_plugin_symlinks[plugin_symlink_path] > 1:
                    continue
                if plugin_symlink_path in injected_symlinked_paths:
                    continue
                if real_path in injected_symlinked_paths.values():
                    continue
                ### Only reap a symlink whose target NO LONGER EXISTS (a genuinely stale
                ### plugin). A symlink whose target still exists is a valid plugin from
                ### another configured plugins directory and must not be removed here:
                ### `PLUGINS_DIR_PATHS` is process-global and can transiently fall back to
                ### the host default when `MRSM_PLUGINS_DIR` is momentarily absent from the
                ### environment (e.g. a daemon thread or a `replace_env` context). Removing
                ### valid cross-dir symlinks in that window deletes a project's plugin
                ### links from the shared per-root `.internal/plugins`, so a
                ### `plugin:<name>`-backed background job then fails to import its plugin.
                try:
                    if real_path.exists():
                        continue
                except Exception:
                    pass
                try:
                    plugin_symlink_path.unlink()
                except Exception:
                    pass

            ### Remove valid plugins from the to-be-symlinked list.
            else:
                plugins_to_be_symlinked.remove(real_path)

        for plugin_path in plugins_to_be_symlinked:
            plugin_symlink_path = paths.PLUGINS_RESOURCES_PATH / plugin_path.name
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

    with _locks['PLUGINS_INTERNAL_LOCK_PATH']:
        try:
            if not paths.PLUGINS_INIT_PATH.exists():
                paths.PLUGINS_INIT_PATH.touch()
        except Exception as e:
            error(f"Failed to create the file '{paths.PLUGINS_INIT_PATH}':\n{e}")

    with _locks['__path__']:
        if str(paths.PLUGINS_RESOURCES_PATH.parent) not in __path__:
            __path__.append(str(paths.PLUGINS_RESOURCES_PATH.parent))

    with _locks['_synced_symlinks']:
        _synced_symlinks += 1


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
    import meerschaum.config.paths as paths
    from meerschaum.utils.misc import flatten_list
    from meerschaum.utils.venv import (
        is_venv_active, activate_venv, deactivate_venv, Venv, ensure_default_venvs,
    )
    from meerschaum.utils.warnings import warn as _warn
    plugins_to_import = list(plugins_to_import)
    ### PARITY: v4 had already activated the shared 'mrsm' venv (`attempt_import`) before the first
    ### plugin was imported, so plugin code with its own `required` venv still saw it.
    ensure_default_venvs()
    prepended_sys_path = False
    with _locks['sys.path']:

        ### Since plugins may depend on other plugins,
        ### we need to activate the virtual environments for library plugins.
        ### This logic exists in `Plugin.activate_venv()`,
        ### but that code requires the plugin's module to already be imported.
        ### It's not a guarantee of correct activation order,
        ### e.g. if a library plugin pins a specific package and another 
        plugins_names = get_plugins_names()
        already_active_venvs = {
            plugin_name
            for plugin_name in plugins_names
            if is_venv_active(plugin_name)
        }

        if not sys.path or sys.path[0] != str(paths.PLUGINS_RESOURCES_PATH.parent):
            prepended_sys_path = True
            sys.path.insert(0, str(paths.PLUGINS_RESOURCES_PATH.parent))

        if not plugins_to_import:
            for plugin_name in plugins_names:
                activate_venv(plugin_name)
            try:
                with plugin_import_guard():
                    imported_plugins = importlib.import_module(paths.PLUGINS_RESOURCES_PATH.stem)
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
                    with Venv(plugin, init_if_not_exists=False), plugin_import_guard():
                        imported_plugins.append(
                            importlib.import_module(
                                f'{paths.PLUGINS_RESOURCES_PATH.stem}.{plugin_name}'
                            )
                        )
                    _ = _plugins_import_errors.pop(plugin_name, None)
                except Exception as e:
                    _plugins_import_errors[plugin_name] = e
                    _warn(
                        f"Failed to import plugin '{plugin_name}':\n    "
                        + f"{e}\n\nHere's a stacktrace:",
                        stack = False,
                    )
                    from meerschaum.utils.formatting import print_exception
                    print_exception(
                        suppress = [
                            'meerschaum/plugins/__init__.py',
                            importlib,
                            importlib._bootstrap,
                        ]
                    )
                    imported_plugins.append(None)

        if imported_plugins is None and warn:
            _warn("Failed to import plugins.", stacklevel=3)

        if prepended_sys_path and str(paths.PLUGINS_RESOURCES_PATH.parent) in sys.path:
            sys.path.remove(str(paths.PLUGINS_RESOURCES_PATH.parent))

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
    from meerschaum.utils.warnings import warn as _warn
    if plugin_import_name.startswith('plugins.'):
        plugin_import_name = plugin_import_name[len('plugins.'):]
    plugin_import_parts = plugin_import_name.split('.')
    plugin_root_name = plugin_import_parts[0]

    plugins_package_name = _get_plugins_package_name()
    submodule_import_name = '.'.join([plugins_package_name] + plugin_import_parts)
    if len(attrs) == 0:
        raise ValueError(f"Provide which attributes to return from '{submodule_import_name}'.")

    def _get_attrs(module) -> Any:
        attrs_to_return = []
        for attr in attrs:
            try:
                attrs_to_return.append(getattr(module, attr))
            except Exception:
                _warn(f"Failed to access '{attr}' from '{submodule_import_name}'.")
                attrs_to_return.append(None)
        return attrs_to_return[0] if len(attrs_to_return) == 1 else tuple(attrs_to_return)

    first_party_submodule = _import_first_party_plugin(plugin_import_name)
    if first_party_submodule is not None:
        attrs_to_return = [getattr(first_party_submodule, attr) for attr in attrs]
        return attrs_to_return[0] if len(attrs_to_return) == 1 else tuple(attrs_to_return)

    ### Skip building the plugin and activating its venv when the submodule is already imported.
    loaded_submodule = _get_loaded_module(submodule_import_name)
    if (
        loaded_submodule is not None
        and _get_loaded_module(f'{plugins_package_name}.{plugin_root_name}') is not None
    ):
        return _get_attrs(loaded_submodule)

    plugin = mrsm.Plugin(plugin_root_name)
    with mrsm.Venv(plugin):
        if plugin.module is None:
            raise ImportError(f"Unable to import plugin '{plugin}'.")

        try:
            with plugin_import_guard():
                submodule = importlib.import_module(submodule_import_name)
        except ImportError as e:
            _warn(
                f"Failed to import plugin '{submodule_import_name}':\n    "
                + f"{e}\n\nHere's a stacktrace:",
                stack=False,
            )
            from meerschaum.utils.formatting import print_exception
            print_exception(
                suppress=[
                    'meerschaum/plugins/__init__.py',
                    importlib,
                    importlib._bootstrap,
                ]
            )
            ### Return one `None` per attribute so tuple unpacking still works.
            return None if len(attrs) == 1 else (None,) * len(attrs)

        return _get_attrs(submodule)


_loaded_plugins: bool = False
def load_plugins(
    skip_if_loaded: bool = True,
    shell: bool = False,
    debug: bool = False,
) -> None:
    """
    Import Meerschaum plugins and update the actions dictionary.
    """
    global _loaded_plugins
    from meerschaum.utils.warnings import dprint

    if skip_if_loaded and _loaded_plugins:
        if debug:
            dprint("Skip loading plugins...")
        return

    from inspect import isfunction, getmembers
    import meerschaum.config.paths as paths
    from meerschaum.actions import __all__ as _all, modules
    from meerschaum.utils.packages import get_modules_from_package

    with plugin_import_guard():
        _plugins_names, plugins_modules = get_modules_from_package(
            import_plugins(),
            names = True,
            recursive = True,
            modules_venvs = True
        )

    ### I'm appending here to keep from redefining the modules list.
    new_modules = (
        [
            mod
            for mod in modules
            if not mod.__name__.startswith(paths.PLUGINS_RESOURCES_PATH.stem + '.')
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
                make_action(
                    func,
                    **{'shell': shell, 'debug': debug},
                    _plugin_name=_get_parent_plugin(function=func),
                    skip_if_loaded=True,
                )

    _loaded_plugins = True


def unload_custom_actions(plugins: Optional[List[str]] = None, debug: bool = False) -> None:
    """
    Unload the custom actions added by plugins.
    """
    from meerschaum.actions import (
        actions,
        _custom_actions_plugins,
        _plugins_actions,
    )

    plugins = plugins if plugins is not None else list(_plugins_actions)

    for plugin in plugins:
        action_names = _plugins_actions.get(plugin, [])
        for action_name in action_names:
            _ = actions.pop(action_name, None)
            _ = _custom_actions_plugins.pop(action_name, None)
            _ = _actions_daemon_enabled.pop(action_name, None)
            _ = _action_words.pop(action_name, None)

        _ = _plugins_actions.pop(plugin, None)
    

def unload_plugins(
    plugins: Optional[List[str]] = None,
    remove_symlinks: bool = True,
    debug: bool = False,
) -> None:
    """
    Unload the specified plugins from memory.
    """
    global _loaded_plugins, _synced_symlinks
    import os
    import sys
    import pathlib
    import meerschaum.config.paths as paths
    from meerschaum.connectors import unload_plugin_connectors
    if debug:
        from meerschaum.utils.warnings import dprint

    _loaded_plugins = False
    _synced_symlinks = 0

    all_plugins = get_plugins_names()
    plugins = plugins if plugins is not None else all_plugins
    if debug:
        dprint(f"Unloading plugins: {plugins}")

    unload_custom_actions(plugins, debug=debug)
    unload_plugin_connectors(plugins, debug=debug)

    module_prefix = f"{paths.PLUGINS_RESOURCES_PATH.stem}."
    loaded_modules = [mod_name for mod_name in sys.modules if mod_name.startswith(module_prefix)]

    root_plugins_mod = (
        sys.modules.get(paths.PLUGINS_RESOURCES_PATH.stem, None)
        if sorted(plugins) != sorted(all_plugins)
        else sys.modules.pop(paths.PLUGINS_RESOURCES_PATH.stem, None)
    )

    for plugin_name in plugins:
        for mod_name in loaded_modules:
            if (
                mod_name[len(module_prefix):].startswith(plugin_name + '.')
                or mod_name[len(module_prefix):] == plugin_name
            ):
                _ = sys.modules.pop(mod_name, None)

        if root_plugins_mod is not None and plugin_name in root_plugins_mod.__dict__:
            try:
                delattr(root_plugins_mod, plugin_name)
            except Exception:
                pass

        ### Unload sync hooks.
        _ = _pre_sync_hooks.pop(plugin_name, None)
        _ = _post_sync_hooks.pop(plugin_name, None)
        _ = _plugins_import_errors.pop(plugin_name, None)

        ### Unload API endpoints and pages.
        _ = _dash_plugins.pop(plugin_name, None)
        _ = _api_plugins.pop(plugin_name, None)
        with _locks['_console_head']:
            _ = _console_head.pop(plugin_name, None)
        web_page_funcs = _plugins_web_pages.pop(plugin_name, None) or []
        page_groups_to_pop = []
        for page_group, page_functions in _plugin_endpoints_to_pages.items():
            page_functions_to_pop = [
                page_str
                for page_str, page_payload in page_functions.items()
                if page_payload.get('function', None) in web_page_funcs
            ]
            for page_str in page_functions_to_pop:
                page_functions.pop(page_str, None)
            if not page_functions:
                page_groups_to_pop.append(page_group)
        
        for page_group in page_groups_to_pop:
            _plugin_endpoints_to_pages.pop(page_group, None)

        ### Remove all but injected symlinks.
        if remove_symlinks:
            dir_symlink_path = paths.PLUGINS_RESOURCES_PATH / plugin_name
            dir_symlink_injected_path = paths.PLUGINS_INJECTED_RESOURCES_PATH / plugin_name
            file_symlink_path = paths.PLUGINS_RESOURCES_PATH / f"{plugin_name}.py"
            file_symlink_injected_path = paths.PLUGINS_INJECTED_RESOURCES_PATH / f"{plugin_name}.py"

            ### Only remove a symlink whose target NO LONGER EXISTS (a genuinely stale
            ### plugin). The per-root `.internal/plugins` directory is SHARED by every
            ### process operating on that root — including a `plugin:<name>`-backed
            ### background job that may have been started moments earlier (e.g. by
            ### `mrsm compose start jobs`, which loads project plugins, starts the job
            ### daemon, then unloads). Removing a still-valid plugin symlink here deletes
            ### it out from under that daemon, so its plugin import fails with
            ### "Plugin '<name>' cannot be found". In-memory unloading (popping
            ### `sys.modules` above) is process-local and is all that unloading needs;
            ### the symlink lifecycle is owned by `sync_plugins_symlinks`.
            def _target_exists(symlink_path):
                try:
                    return pathlib.Path(os.path.realpath(symlink_path)).exists()
                except Exception:
                    return False

            try:
                if (
                    dir_symlink_path.exists()
                    and not dir_symlink_injected_path.exists()
                    and not _target_exists(dir_symlink_path)
                ):
                    dir_symlink_path.unlink()
            except Exception:
                pass

            try:
                if (
                    file_symlink_path.exists()
                    and not file_symlink_injected_path.exists()
                    and not _target_exists(file_symlink_path)
                ):
                    file_symlink_path.unlink()
            except Exception:
                pass


def invalidate_plugins_cache() -> None:
    """
    Pop the cached `plugins` package and its submodules from `sys.modules`
    and reset the loaded-plugins state.

    The `plugins` package caches its `__path__` (the resolved
    `PLUGINS_RESOURCES_PATH`) at import time. When the active root or
    plugins-dir scope changes in-process (e.g. via
    `meerschaum.config.environment.replace_env`), that stale `__path__`
    makes subsequent plugin imports re-discover plugins under the previous
    scope's `.internal/plugins` directory. Call this whenever the plugins
    scope changes so the next import rebuilds against the current
    `PLUGINS_RESOURCES_PATH`.
    """
    global _loaded_plugins, _synced_symlinks
    import sys
    import meerschaum.config.paths as paths

    plugins_stem = paths.PLUGINS_RESOURCES_PATH.stem
    module_prefix = plugins_stem + '.'
    for mod_name in [
        mod_name
        for mod_name in sys.modules
        if mod_name == plugins_stem or mod_name.startswith(module_prefix)
    ]:
        _ = sys.modules.pop(mod_name, None)

    _loaded_plugins = False
    ### Also reset the symlinks counter — it's per-scope state, and leaving it
    ### >1 makes `sync_plugins_symlinks` skip creating the new scope's
    ### `.internal/plugins` (so `plugins` imports as an empty namespace package).
    _synced_symlinks = 0


def reload_plugins(plugins: Optional[List[str]] = None, debug: bool = False) -> None:
    """
    Reload plugins back into memory.

    Parameters
    ----------
    plugins: Optional[List[str]], default None
        The plugins to reload. `None` will reload all plugins.

    """
    global _synced_symlinks
    unload_plugins(plugins, debug=debug)
    _synced_symlinks = 0
    sync_plugins_symlinks(debug=debug)
    load_plugins(skip_if_loaded=False, debug=debug)


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
    import meerschaum.config.paths as paths
    import os
    sync_plugins_symlinks()
    _plugins = [
        Plugin(name)
        for name in (
            to_load or [
                (
                    name if (paths.PLUGINS_RESOURCES_PATH / name).is_dir()
                    else name[:-3]
                )
                for name in os.listdir(paths.PLUGINS_RESOURCES_PATH)
                if name != '__init__.py'
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


_data_plugins_cache: Dict[Tuple[str, ...], List[Plugin]] = {}
def get_data_plugins() -> List[Plugin]:
    """
    Only return the modules of plugins with either `fetch()` or `sync()` functions.
    """
    import inspect
    plugins = get_plugins()

    ### ponytail: cache on the installed plugins' names.
    ### Accessing `plugin.module` re-enters `import_plugins()` (venv activation +
    ### a nested `get_plugins()`) for every plugin, so an uncached call costs
    ### hundreds of milliseconds. The shell completer calls this on every keystroke
    ### (via `get_connector_labels()`), which is what made typing `-c` laggy.
    ### Installing or removing a plugin changes the key, so the cache self-invalidates;
    ### editing a plugin in place requires a restart to pick up a new `fetch()`/`sync()`.
    cache_key = tuple(plugin.name for plugin in plugins)
    if cache_key in _data_plugins_cache:
        return _data_plugins_cache[cache_key]

    data_names = {'sync', 'fetch'}
    data_plugins = []
    for plugin in plugins:
        for name, ob in inspect.getmembers(plugin.module):
            if not inspect.isfunction(ob):
                continue
            if name not in data_names:
                continue
            data_plugins.append(plugin)

    _data_plugins_cache[cache_key] = data_plugins
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
    from meerschaum.utils.warnings import warn
    _parent_plugin_name = _get_parent_plugin()
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


def inject_plugin_path(
    plugin_path: pathlib.Path,
    plugins_resources_path: Optional[pathlib.Path] = None) -> None:
    """
    Inject a plugin as a symlink into the internal `plugins` directory.

    Parameters
    ----------
    plugin_path: pathlib.Path
        The path to the plugin's source module.
    """
    import meerschaum.config.paths as paths
    from meerschaum.utils.misc import make_symlink
    if plugins_resources_path is None:
        plugins_resources_path = paths.PLUGINS_RESOURCES_PATH
        plugins_injected_resources_path = paths.PLUGINS_INJECTED_RESOURCES_PATH
    else:
        plugins_injected_resources_path = plugins_resources_path / '.injected'

    if plugin_path.is_dir():
        plugin_name = plugin_path.name
        dest_path = plugins_resources_path / plugin_name
        injected_path = plugins_injected_resources_path / plugin_name
    elif plugin_path.name == '__init__.py':
        plugin_name = plugin_path.parent.name
        dest_path = plugins_resources_path / plugin_name
        injected_path = plugins_injected_resources_path / plugin_name
    elif plugin_path.name.endswith('.py'):
        plugin_name = plugin_path.name[:(-1 * len('.py'))]
        dest_path = plugins_resources_path / plugin_path.name
        injected_path = plugins_injected_resources_path / plugin_path.name
    else:
        raise ValueError(f"Cannot deduce plugin name from path '{plugin_path}'.")

    _injected_plugin_symlinks[dest_path] += 1
    make_symlink(plugin_path, dest_path)
    make_symlink(plugin_path, injected_path)


def _get_parent_plugin(
    stacklevel: Union[int, Tuple[int, ...]] = (1, 2, 3, 4),
    function: Optional[Callable[[Any], Any]] = None,
) -> Union[str, None]:
    """If this function is called from outside a Meerschaum plugin, it will return None."""
    import inspect
    module_name = getattr(function, '__module__', '')
    if module_name.startswith('plugins.'):
        return module_name.split('.')[1]
    if not isinstance(stacklevel, tuple):
        stacklevel = (stacklevel,)

    for _level in stacklevel:
        try:
            parent_globals = inspect.stack()[_level][0].f_globals
            global_name = parent_globals.get('__name__', '')
            if global_name.startswith('meerschaum.'):
                continue
            plugin_name = global_name.replace('plugins.', '').split('.')[0]
            if plugin_name.startswith('_') or plugin_name == 'importlib':
                continue
            return plugin_name
        except Exception:
            continue

    return None
