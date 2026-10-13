#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the plugin manifest (design §4.3).

The manifest records everything the Rust host needs to know about installed plugins without
starting Python: `@make_action` actions (with docstrings and subactions), `add_plugin_argument`
arguments, sync hooks, `@api_plugin`, `@dash_plugin`, `@web_page`, MCP tools / resources /
prompts, custom connector types, and which data functions (`fetch`, `sync`, `register`, `setup`,
`subscribe`) each plugin module defines.

The schema is defined by `crates/meerschaum-worker/src/manifest.rs` (and
`crates/meerschaum-worker/schema/manifest.schema.json`); the Rust host adds the source
fingerprints and caches the result under the root directory.
"""

from __future__ import annotations

import inspect
import traceback
from typing import Any, Dict, List, Optional

MANIFEST_SCHEMA_VERSION = 1
DATA_FUNCTIONS = ('fetch', 'sync', 'register', 'setup', 'subscribe')


def _doc(func: Any) -> Optional[str]:
    try:
        return inspect.getdoc(func)
    except Exception:
        return None


def _plugin_of(func: Any) -> Optional[str]:
    from meerschaum.plugins import _get_parent_plugin
    module_name = getattr(func, '__module__', '') or ''
    if module_name.startswith('plugins.'):
        return module_name.split('.')[1]
    try:
        return _get_parent_plugin(function=func)
    except Exception:
        return None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if callable(value):
        return getattr(value, '__name__', str(value))
    return str(value)


def _argument_manifest(action: Any) -> Dict[str, Any]:
    """Describe one `argparse.Action` as data."""
    action_type = getattr(action, 'type', None)
    return {
        'option_strings': list(getattr(action, 'option_strings', []) or []),
        'dest': getattr(action, 'dest', None),
        'action': type(action).__name__,
        'nargs': _jsonable(getattr(action, 'nargs', None)),
        'const': _jsonable(getattr(action, 'const', None)),
        'default': _jsonable(getattr(action, 'default', None)),
        'type': (getattr(action_type, '__name__', str(action_type)) if action_type else None),
        'choices': _jsonable(getattr(action, 'choices', None)),
        'required': bool(getattr(action, 'required', False)),
        'help': getattr(action, 'help', None),
        'metavar': _jsonable(getattr(action, 'metavar', None)),
    }


def _empty_plugin(name: str) -> Dict[str, Any]:
    return {
        'name': name,
        'version': None,
        'path': None,
        'required': [],
        'data_functions': [],
        'actions': [],
        'arguments': [],
        'pre_sync_hooks': [],
        'post_sync_hooks': [],
        'api_plugins': [],
        'dash_plugins': [],
        'web_pages': [],
        'mcp_tools': [],
        'mcp_resources': [],
        'mcp_prompts': [],
        'connectors': [],
        'import_error': None,
    }


def build_manifest(debug: bool = False) -> Dict[str, Any]:
    """
    Import every installed plugin and return the manifest dictionary.
    Fingerprints are left empty for the host to fill in.
    """
    import sys
    import meerschaum as mrsm
    import meerschaum.plugins as plugins_module
    from meerschaum.plugins import get_plugins_names, load_plugins, Plugin
    from meerschaum._internal.arguments._parser import groups
    import meerschaum.connectors as connectors_module

    ### Snapshot the built-in registries before any plugin is imported.
    builtin_group_keys = set(groups.keys())
    mcp_before_tools, mcp_before_resources, mcp_before_prompts = set(), set(), set()
    mcp_registry = None
    try:
        from meerschaum.mcp import _registry as mcp_registry
        mcp_registry._ensure_loaded()
        mcp_before_tools = set(mcp_registry._TOOLS.keys())
        mcp_before_resources = {id(r) for r in mcp_registry._RESOURCES}
        mcp_before_prompts = set(mcp_registry._PROMPTS.keys())
    except Exception:
        mcp_registry = None

    try:
        connectors_module._load_builtin_custom_connectors()
    except Exception:
        pass
    builtin_types = set(connectors_module.custom_types)

    plugin_names = list(get_plugins_names())
    manifest_plugins: Dict[str, Dict[str, Any]] = {name: _empty_plugin(name) for name in plugin_names}

    load_errors: Dict[str, str] = {}
    try:
        load_plugins(skip_if_loaded=True, debug=debug)
    except Exception:
        load_errors['__all__'] = traceback.format_exc()

    from meerschaum.actions import actions, _plugins_actions, get_subactions

    for name in plugin_names:
        info = manifest_plugins[name]
        plugin = Plugin(name)
        try:
            module = plugin.module
        except Exception:
            module = None
        error = plugins_module._plugins_import_errors.get(name, None) or getattr(
            plugin, 'import_error', None
        )
        if error is not None:
            info['import_error'] = f"{type(error).__name__}: {error}"
        if module is None:
            continue
        info['version'] = getattr(module, '__version__', None)
        info['path'] = getattr(module, '__file__', None)
        required = getattr(module, 'required', [])
        info['required'] = [str(r) for r in required] if isinstance(required, (list, tuple)) else []
        info['data_functions'] = [
            fn_name
            for fn_name in DATA_FUNCTIONS
            if inspect.isfunction(getattr(module, fn_name, None))
        ]

    ### Actions
    for plugin_name, action_names in _plugins_actions.items():
        if plugin_name not in manifest_plugins:
            continue
        for action_name in action_names:
            func = actions.get(action_name, None)
            if func is None:
                continue
            subactions = []
            try:
                for sub_name, sub_func in get_subactions([action_name]).items():
                    subactions.append({'name': sub_name, 'doc': _doc(sub_func)})
            except Exception:
                pass
            manifest_plugins[plugin_name]['actions'].append({
                'name': action_name,
                'doc': _doc(func),
                'daemon': plugins_module._actions_daemon_enabled.get(action_name, True),
                'subactions': subactions,
            })

    ### Plugin arguments
    for group_key, group in groups.items():
        if group_key in builtin_group_keys or not group_key.startswith('plugin_'):
            continue
        plugin_name = group_key[len('plugin_'):]
        if plugin_name not in manifest_plugins:
            continue
        manifest_plugins[plugin_name]['arguments'].extend(
            _argument_manifest(action) for action in getattr(group, '_group_actions', [])
        )

    ### Hooks and host extensions
    registries = (
        ('pre_sync_hooks', plugins_module._pre_sync_hooks),
        ('post_sync_hooks', plugins_module._post_sync_hooks),
        ('api_plugins', plugins_module._api_plugins),
        ('dash_plugins', plugins_module._dash_plugins),
    )
    for key, registry in registries:
        for registered_name, functions in registry.items():
            for func in functions:
                plugin_name = _plugin_of(func) or registered_name
                if plugin_name and plugin_name.startswith('plugins.'):
                    plugin_name = plugin_name.split('.')[1]
                if plugin_name not in manifest_plugins:
                    continue
                manifest_plugins[plugin_name][key].append(getattr(func, '__name__', str(func)))

    for page_group, pages in plugins_module._plugin_endpoints_to_pages.items():
        for page_path, page_info in pages.items():
            func = page_info.get('function', None)
            plugin_name = _plugin_of(func)
            if plugin_name not in manifest_plugins:
                continue
            ### The mode is only known by calling the function once (as v4 and the API host do):
            ### a `str` / `(html, status)` result is the page contract, anything else a Dash layout.
            mode = 'dash'
            try:
                if isinstance(func(), (str, tuple)):
                    mode = 'html'
            except Exception:
                pass
            if mode == 'html':
                route_rest = page_path.lstrip('/')
                if route_rest == 'dash' or route_rest.startswith('dash/'):
                    route_rest = route_rest[len('dash'):].lstrip('/')
                route = ('/_pages/' + route_rest).rstrip('/') or '/_pages'
            else:
                route_rest = page_path.lstrip('/')
                if route_rest == 'dash' or route_rest.startswith('dash/'):
                    route_rest = route_rest[len('dash'):].lstrip('/')
                if route_rest == plugin_name or route_rest.startswith(plugin_name + '/'):
                    route_rest = route_rest[len(plugin_name):].lstrip('/')
                route = f"/dash/{plugin_name}/{route_rest}".rstrip('/')
            manifest_plugins[plugin_name]['web_pages'].append({
                'mode': mode,
                'route': route,
                'path': page_path,
                'page_group': page_group,
                'page_key': page_info.get('page_key', None),
                'login_required': bool(page_info.get('login_required', True)),
                'skip_navbar': bool(page_info.get('skip_navbar', False)),
                'dark_theme': bool(page_info.get('dark_theme', True)),
                'function': getattr(func, '__name__', None),
            })

    ### MCP
    if mcp_registry is not None:
        for tool_name, tool in mcp_registry._TOOLS.items():
            if tool_name in mcp_before_tools:
                continue
            plugin_name = _plugin_of(tool.handler)
            if plugin_name not in manifest_plugins:
                continue
            manifest_plugins[plugin_name]['mcp_tools'].append({
                'name': tool.name,
                'description': tool.description,
                'input_schema': _jsonable(tool.input_schema),
                'output_schema': _jsonable(tool.output_schema),
                'scopes': list(tool.scopes),
                'title': tool.title,
                'annotations': _jsonable(tool.annotations),
            })
        for resource in mcp_registry._RESOURCES:
            if id(resource) in mcp_before_resources:
                continue
            plugin_name = _plugin_of(resource.handler)
            if plugin_name not in manifest_plugins:
                continue
            manifest_plugins[plugin_name]['mcp_resources'].append({
                'name': resource.name,
                'description': resource.description,
                'uri': resource.uri,
                'uri_template': resource.uri_template,
                'mime_type': resource.mime_type,
                'scopes': list(resource.scopes),
                'title': resource.title,
            })
        for prompt_name, prompt in mcp_registry._PROMPTS.items():
            if prompt_name in mcp_before_prompts:
                continue
            plugin_name = _plugin_of(prompt.handler)
            if plugin_name not in manifest_plugins:
                continue
            manifest_plugins[plugin_name]['mcp_prompts'].append({
                'name': prompt_name,
                'description': getattr(prompt, 'description', None),
            })

    ### Custom connectors
    connector_types: Dict[str, Dict[str, Any]] = {}
    for plugin_name, types in connectors_module.plugins_types.items():
        if plugin_name not in manifest_plugins:
            continue
        for typ in types:
            if typ in builtin_types:
                continue
            cls = connectors_module.types.get(typ, None)
            if cls is None:
                continue
            methods = sorted(
                name
                for name, member in inspect.getmembers(cls)
                if not name.startswith('_') and callable(member)
                and getattr(member, '__qualname__', '').split('.')[0] == cls.__name__
            )
            connector_types[typ] = {
                'type': typ,
                'plugin': plugin_name,
                'class_name': cls.__name__,
                'is_instance': bool(getattr(cls, 'IS_INSTANCE', False)),
                'is_thread_safe': bool(getattr(cls, 'IS_THREAD_SAFE', False)),
                'is_streaming': bool(getattr(cls, 'IS_STREAMING', False)),
                'required_attributes': [str(a) for a in getattr(cls, 'REQUIRED_ATTRIBUTES', []) or []],
                'optional_attributes': [str(a) for a in getattr(cls, 'OPTIONAL_ATTRIBUTES', []) or []],
                'methods': methods,
                'doc': _doc(cls),
            }
            manifest_plugins[plugin_name]['connectors'].append(typ)

    def _with(key: str) -> List[str]:
        return [name for name, info in manifest_plugins.items() if info[key]]

    import platform
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'meerschaum_version': mrsm.__version__,
        'python_version': platform.python_version(),
        'python_executable': sys.executable,
        'fingerprint': None,
        'plugins': manifest_plugins,
        'connector_types': connector_types,
        'api_plugins': _with('api_plugins'),
        'dash_plugins': _with('dash_plugins'),
        'web_pages': _with('web_pages'),
        'mcp_tools': sorted(set(_with('mcp_tools') + _with('mcp_resources') + _with('mcp_prompts'))),
        'pre_sync_hooks': _with('pre_sync_hooks'),
        'post_sync_hooks': _with('post_sync_hooks'),
        'load_errors': load_errors,
    }
