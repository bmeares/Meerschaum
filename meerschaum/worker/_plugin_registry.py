"""Confined extension-registry inspection and source-bound handler selection.

The native Metadata launch must remove credentials and deny network before importing this
module or any source. Python checks are diagnostics, never Main's proof of that boundary.
Only Main's stored canonical Metadata operation can admit the returned untrusted data.
"""

from __future__ import annotations


def _complete(entries):
    return {'state': 'complete', 'entries': entries}


def _unsupported(feature):
    return {'state': 'unsupported', 'feature': feature}


def _observation_failure(feature, error):
    """Return only bounded code identifiers; exception messages may contain credentials."""
    import re
    error_type = type(error).__name__
    if not isinstance(error_type, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', error_type):
        error_type = 'Exception'
    marker = feature + ':' + error_type
    # These attributes identify missing code or a schema error without echoing the message,
    # traceback, filesystem path, user input, or private observer state.
    identifier = None
    if isinstance(error, (NameError, ImportError)):
        identifier = getattr(error, 'name', None)
    elif type(error).__module__.startswith('pydantic'):
        identifier = getattr(error, 'code', None)
    if isinstance(identifier, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.-]{0,95}', identifier):
        marker += ':' + identifier
    if isinstance(error, PermissionError):
        # Fixed SDK stage names are useful even when worker stderr is intentionally quiet.
        # Never publish a traceback filename, line, local value, or plugin-defined name.
        stages = {
            ('meerschaum.api', '<module>'): 'api_helper_import',
            ('meerschaum.api._oauth2', '<module>'): 'api_auth_helper_import',
            ('meerschaum.api._oauth2', 'generate_secret_key'): 'private_secret_initialization',
            ('meerschaum.config._paths', '__getattr__'): 'private_path_lookup',
            ('starlette.staticfiles', '__init__'): 'static_asset_lookup',
            ('meerschaum.worker._plugin_registry', '_require_metadata'): 'native_boundary_check',
            ('meerschaum.worker._plugin_invoke', '_require_execution'): 'native_boundary_check',
            ('meerschaum.worker._plugin_declarations', 'api_declarations'): 'api_schema',
            ('meerschaum.worker._plugin_declarations', 'dependency_scopes'): 'api_dependency_schema',
        }
        stage = None
        trace = error.__traceback__
        for _ in range(64):
            if trace is None:
                break
            key = (trace.tb_frame.f_globals.get('__name__'), trace.tb_frame.f_code.co_name)
            stage = stages.get(key, stage)
            if key == ('meerschaum.api', '<module>') and 1 <= trace.tb_lineno <= 4096:
                stage = 'api_helper_import_line_' + str(trace.tb_lineno)
            trace = trace.tb_next
        if stage:
            marker += ':' + stage
        if error.errno in (1, 13):
            marker += ':access_denied'
    return _unsupported(marker)


def _require_metadata():
    import errno
    import os
    import socket
    from meerschaum._internal import team_sandbox
    if (
        os.environ.get('MRSM_METADATA_EXECUTION') != '1'
        or os.environ.get('MRSM_API_EXECUTION') != '1'
        or not team_sandbox.has_required_socket_boundary()
    ):
        raise PermissionError('Extension discovery requires the native credential-free profile.')
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except OSError as error:
        if error.errno not in (errno.EPERM, errno.EACCES, errno.EAFNOSUPPORT):
            raise PermissionError('The discovery network filter could not be verified.') from error
    else:
        connection.close()
        raise PermissionError('Network remains available to metadata discovery.')


def _handler(catalog_sha256, plugin, source_sha256, selector, scopes=(), dash=None):
    import hashlib
    import json
    encoded = json.dumps(
        [catalog_sha256, plugin, source_sha256, selector],
        ensure_ascii=False, separators=(',', ':'),
    ).encode('utf-8')
    result = {
        'id': hashlib.sha256(b'mrsm-extension-handler-v1\0' + encoded).hexdigest(),
        'selector': selector,
        'scopes': sorted(set(scopes)),
    }
    if dash is not None:
        result['dash'] = dash
    return result


def _plugin_name(module_name):
    if not isinstance(module_name, str):
        return None
    return module_name.split('.')[1] if module_name.startswith('plugins.') else module_name


def _hooks(registry, plugin):
    functions = []
    for name, hooks in list(registry.items()):
        if _plugin_name(name) == plugin:
            functions.extend(list(hooks))
    return functions


def _api_registry(plugin, source_sha256, catalog_sha256, hooks):
    if not hooks:
        return _complete([])
    try:
        from fastapi import FastAPI
        from fastapi.routing import APIRoute
        from starlette.routing import Route
    except ImportError:
        return _unsupported('fastapi_unavailable')
    # No Main API app, shared host, auth manager, cookies, signers or console is imported.
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    try:
        for function in hooks:
            function(app)
        from meerschaum.worker._plugin_declarations import api_declarations
        return api_declarations(app, plugin, source_sha256, catalog_sha256)
    except Exception as error:
        # The producer fails explicitly; an import needing a source API is not empty metadata.
        return _observation_failure('api_hook_or_schema_failed', error)


def _page_registry(plugin, source_sha256, catalog_sha256, registry):
    from meerschaum.plugins import _get_parent_plugin
    entries, seen = [], set()
    try:
        for group, pages in list(registry.items()):
            for raw_path, page in list(pages.items()):
                function = page.get('function')
                owner = _get_parent_plugin(function=function) if callable(function) else None
                if owner != plugin:
                    continue
                path = str(raw_path).strip('/')
                if path.startswith('dash/'):
                    path = path[5:]
                if path in seen:
                    return _unsupported('page_duplicate_path')
                seen.add(path)
                # Legacy build_manifest catches this exception and guesses Dash. Here a failed
                # credential-free render remains unsupported, never a complete guessed page.
                result = function()
                if isinstance(result, str):
                    renderer = 'html'
                elif isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], str) and isinstance(result[1], int):
                    renderer = 'html'
                elif hasattr(result, 'to_plotly_json') or isinstance(result, (list, tuple)):
                    renderer = 'dash'
                else:
                    return _unsupported('page_unknown_render_result')
                entries.append(_handler(catalog_sha256, plugin, source_sha256, {
                    'kind': 'page', 'path': path, 'renderer': renderer,
                }))
        return _complete(entries)
    except Exception:
        return _unsupported('page_credential_free_render_failed')


def _dash_registry(plugin, source_sha256, catalog_sha256, hooks):
    if not hooks:
        return _complete([])
    try:
        from dash import Dash, html
        from dash import _callback as callback_registry
    except ImportError:
        return _unsupported('dash_unavailable')
    # This is a new private application, not meerschaum.api.dash's shared console.
    try:
        app = Dash(
            __name__, requests_pathname_prefix='/_isolated/',
            routes_pathname_prefix='/_isolated/', use_pages=False,
            suppress_callback_exceptions=True, serve_locally=True,
            include_assets_files=False,
        )
        app.layout = html.Div()
        for function in hooks:
            function(app)
        # Flush module-level @callback registrations into this app. Unsupported custom Dash
        # subclasses/transforms remain explicit until their private setup is implemented.
        before_map = dict(callback_registry.GLOBAL_CALLBACK_MAP)
        before_list = list(callback_registry.GLOBAL_CALLBACK_LIST)
        try:
            app._setup_server()
        finally:
            callback_registry.GLOBAL_CALLBACK_MAP.clear()
            callback_registry.GLOBAL_CALLBACK_MAP.update(before_map)
            callback_registry.GLOBAL_CALLBACK_LIST[:] = before_list
        entries = []
        for output, spec in list(app.callback_map.items()):
            if not isinstance(output, str) or not isinstance(spec, dict):
                return _unsupported('dash_unknown_callback')
            callback = spec.get('callback')
            if callable(callback):
                from meerschaum.plugins import _get_parent_plugin
                if _get_parent_plugin(function=callback) != plugin:
                    continue
            if not callable(callback):
                return _unsupported('dash_clientside_callback_requires_browser_manifest')
            declared_outputs=spec.get('output')
            if not isinstance(declared_outputs,list):
                declared_outputs=[declared_outputs]
            outputs=[]
            for declared in declared_outputs:
                if not isinstance(getattr(declared,'component_id',None),str) or not isinstance(getattr(declared,'component_property',None),str):
                    return _unsupported('dash_pattern_output_requires_typed_binding')
                outputs.append({'id':declared.component_id,'property':declared.component_property})
            inputs,state=spec.get('inputs',[]),spec.get('state',[])
            if any(not isinstance(binding,dict) or set(binding)!={'id','property'} or not all(isinstance(value,str) for value in binding.values()) for group in (inputs,state) for binding in group):
                return _unsupported('dash_pattern_input_requires_typed_binding')
            entries.append(_handler(catalog_sha256, plugin, source_sha256, {
                'kind': 'dash_callback', 'output': output,
            },dash={'inputs':inputs,'state':state,'outputs':outputs}))
        return _complete(entries)
    except Exception:
        return _unsupported('dash_private_registry_failed')


def _source_fingerprints(catalog, fingerprints):
    """Check the fixed native-computed map without deriving Host hashes from Python state."""
    import re
    selected = catalog.get('plugins')
    if (not isinstance(selected, dict) or len(selected) > 256
        or not isinstance(fingerprints, dict) or set(fingerprints) != set(selected)):
        raise PermissionError('The native fingerprint map has another selected source set.')
    placement = catalog.get('placement', {'kind': 'remote'})
    if not isinstance(placement, dict) or placement.get('kind') not in ('remote', 'host'):
        raise PermissionError('Unknown explicit source placement.')
    host = placement['kind'] == 'host'
    for plugin, source in selected.items():
        fingerprint = fingerprints[plugin]
        if not isinstance(source, dict) or not isinstance(fingerprint, dict):
            raise PermissionError('Invalid exact source fingerprint.')
        if host:
            if set(fingerprint) != {'kind', 'tree_sha256'} or fingerprint['kind'] != 'files' or source.get('archive_sha256'):
                raise PermissionError('A Host source cannot claim archive authority.')
            digest = fingerprint['tree_sha256']
        else:
            if (set(fingerprint) != {'kind', 'source_kind', 'archive_sha256'}
                or fingerprint['kind'] != 'archive'
                or fingerprint['source_kind'] != source.get('kind')
                or fingerprint['archive_sha256'] != source.get('archive_sha256')):
                raise PermissionError('An executor source has another archive identity.')
            digest = fingerprint['archive_sha256']
        if not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest):
            raise PermissionError('Invalid exact source content digest.')
    return fingerprints


def inspect_plugin_registries(catalog_sha256, catalog, fingerprints):
    """Inspect only already imported exact granted sources in a proven Metadata worker.

    The returned handlers still need Rust bounded/schema/path/scope/source validation. No
    handler is installed in Main, and this function never starts a listener or a shared host.
    """
    import json
    import os
    _require_metadata()
    import meerschaum.plugins as plugins
    selected = catalog.get('plugins')
    if not isinstance(selected, dict) or sorted(selected) != sorted(json.loads(os.environ['MRSM_EXECUTOR_PLUGINS'])):
        raise PermissionError('The extension registry has another selected source set.')
    fingerprints = _source_fingerprints(catalog, fingerprints)
    out = {}
    for plugin, source in selected.items():
        fingerprint = fingerprints[plugin]
        if fingerprint['kind'] == 'archive':
            source_sha256 = fingerprint['archive_sha256']
            identity = {'archive_sha256': source_sha256}
        else:
            source_sha256 = fingerprint['tree_sha256']
            identity = {'files_sha256': source_sha256}
        out[plugin] = {
            'source_catalog_sha256': catalog_sha256,
            'plugin': plugin,
            **identity,
            'api': _api_registry(plugin, source_sha256, catalog_sha256, _hooks(plugins._api_plugins, plugin)),
            'mcp': _mcp_registry(plugin, source_sha256, catalog_sha256),
            'pages': _page_registry(plugin, source_sha256, catalog_sha256, plugins._plugin_endpoints_to_pages),
            'dash_callbacks': _dash_registry(plugin, source_sha256, catalog_sha256, _hooks(plugins._dash_plugins, plugin)),
        }
    # A simple early bound; Rust remains authoritative for depth, count, paths and known scopes.
    if len(json.dumps(out, ensure_ascii=False).encode('utf-8')) > 2 * 1024 * 1024:
        raise ValueError('The confined extension registry exceeds its limit.')
    return out


def _mcp_registry(plugin, source_sha256, catalog_sha256):
    import sys
    registry = sys.modules.get('meerschaum.mcp._registry')
    if registry is None:
        return _complete([])
    from meerschaum.plugins import _get_parent_plugin
    entries = []
    for kind, registered in (
        ('mcp_tool', registry._TOOLS.values()),
        ('mcp_resource', registry._RESOURCES),
        ('mcp_prompt', registry._PROMPTS.values()),
    ):
        for entry in registered:
            if _get_parent_plugin(function=entry.handler) != plugin:
                continue
            entries.append(_handler(
                catalog_sha256, plugin, source_sha256,
                {'kind': kind, 'name': entry.name}, getattr(entry, 'scopes', ()),
            ))
    return _complete(entries)


def inspect_selected_sources(catalog_sha256, catalog, fingerprints):
    """One fixed operation under native Metadata; no module name or callable is accepted."""
    import json
    _require_metadata()
    if any(source.get('metadata') is not None for source in catalog.get('plugins', {}).values()):
        raise PermissionError('Discovery requires a source-only catalog.')
    fingerprints = _source_fingerprints(catalog, fingerprints)
    # This pool may differ from the manifest builder's short-lived process. The native
    # sandbox and deny_network were established before either process imported plugins.
    from meerschaum.plugins import load_plugins
    load_plugins(skip_if_loaded=True)
    result = {
        'registries': inspect_plugin_registries(catalog_sha256, catalog, fingerprints),
        'cli_arguments': inspect_cli_arguments(catalog),
        'capabilities': inspect_connector_capabilities(catalog_sha256, catalog),
    }
    if len(json.dumps(result, ensure_ascii=False).encode('utf-8')) > 2 * 1024 * 1024:
        raise ValueError('Observed source metadata exceeds its limit.')
    return result


def inspect_cli_arguments(catalog):
    """Report whether each loaded selected Python source registered plugin CLI arguments."""
    _require_metadata()
    from meerschaum import Plugin
    from meerschaum.plugins import _plugins_import_errors
    from meerschaum._internal.arguments._parser import groups

    result = {}
    for name, source in catalog['plugins'].items():
        observed = 'unsupported'
        if source.get('kind') in ('python_module', 'python_package'):
            try:
                plugin = Plugin(name)
                if (plugin.module is not None and name not in _plugins_import_errors
                        and not getattr(plugin, 'import_error', None)):
                    group = groups.get(f'plugin_{name}')
                    if group is None:
                        observed = 'absent'
                    elif isinstance(group._group_actions, list):
                        observed = 'present' if group._group_actions else 'absent'
            except Exception:
                pass
        result[name] = observed
    return result


def inspect_connector_capabilities(catalog_sha256, catalog):
    """Observe actual classes/module callables; unknown is never a default false value."""
    import json
    import os
    _require_metadata()
    import meerschaum as mrsm
    import meerschaum.connectors as connectors
    selected = catalog.get('plugins')
    if not isinstance(selected, dict) or sorted(selected) != sorted(json.loads(os.environ['MRSM_EXECUTOR_PLUGINS'])):
        raise PermissionError('Connector classification has another selected source set.')
    classified = {}
    for plugin, source in selected.items():
        if source.get('kind') not in ('python_module', 'python_package'):
            classified[plugin] = {'state': 'unsupported', 'feature': 'native_capability_projection_required'}
            continue
        try:
            module = mrsm.Plugin(plugin).module
            functions = sorted(name for name in ('fetch', 'sync', 'register', 'setup', 'subscribe') if callable(getattr(module, name, None)))
            classes = {}
            for typ in connectors.plugins_types.get(plugin, ()):
                cls = connectors.types.get(typ)
                if cls is None:
                    raise LookupError('A declared connector class is unavailable.')
                classes[typ] = {
                    'python': True,
                    'streaming': bool(getattr(cls, 'IS_STREAMING', False)),
                    'instance': bool(getattr(cls, 'IS_INSTANCE', False)),
                    'thread_safe': bool(getattr(cls, 'IS_THREAD_SAFE', False)),
                    'methods': sorted(name for name in dir(cls) if not name.startswith('_') and callable(getattr(cls, name, None))),
                    'required_attributes': sorted(str(name) for name in (getattr(cls, 'REQUIRED_ATTRIBUTES', ()) or ())),
                    'optional_attributes': sorted(str(name) for name in (getattr(cls, 'OPTIONAL_ATTRIBUTES', ()) or ())),
                }
            classified[plugin] = {
                'state': 'known', 'python': True, 'module_functions': functions,
                'required': sorted(str(name) for name in (getattr(module, 'required', ()) or ())),
                'connector_types': classes,
            }
        except Exception:
            classified[plugin] = {'state': 'unsupported', 'feature': 'observed_connector_classification_failed'}
    out = {'source_catalog_sha256': catalog_sha256, 'plugins': classified}
    if len(json.dumps(out, ensure_ascii=False).encode('utf-8')) > 2 * 1024 * 1024:
        raise ValueError('Connector classifications exceed their bound.')
    return out
