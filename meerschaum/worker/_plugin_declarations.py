"""Data-only declaration checks shared by confined discovery and invocation.

It never launches a producer, imports a plugin, authenticates a request, or installs
anything on Main. Both callers must already be in their native private boundary.
"""

from __future__ import annotations


def dependency_scopes(route):
    """Observe explicitly marked core SDK dependencies in a finite private FastAPI graph.

    The SDK factory must mark its own returned ScopedAuth closure. This is declaration data,
    never authentication: Main and the exact private runtime still validate every name.
    """
    root = getattr(route, 'dependant', None)
    if root is None:
        return set()
    pending, seen, scopes = [(root, 0)], set(), set()
    while pending:
        dependant, depth = pending.pop()
        if id(dependant) in seen:
            continue
        seen.add(id(dependant))
        if depth > 32 or len(seen) > 512:
            raise ValueError('The private API dependency graph exceeds its limits.')
        declared = getattr(getattr(dependant, 'call', None), '_mrsm_extension_scopes', ())
        if not isinstance(declared, (list, tuple)) or len(declared) > 64 or not all(isinstance(scope, str) for scope in declared):
            raise ValueError('Unsupported private SDK scope declaration.')
        scopes.update(declared)
        dependencies = getattr(dependant, 'dependencies', ())
        if not isinstance(dependencies, (list, tuple)) or len(dependencies) > 512:
            raise ValueError('Unsupported private API dependency graph.')
        pending.extend((dependency, depth + 1) for dependency in dependencies)
    return scopes


def api_declarations(app, plugin, source_sha256, catalog_sha256):
    """Observe the exact private ASGI application which will dispatch this invocation."""
    from fastapi.routing import APIRoute
    from starlette.routing import Route
    from meerschaum.worker._plugin_registry import _complete, _handler, _unsupported, _observation_failure
    try:
        schema = app.openapi()
        entries, seen = [], set()
        for route in app.routes:
            if type(route) not in (APIRoute, Route):
                return _unsupported('api_mount_or_websocket')
            path, methods = getattr(route, 'path', None), getattr(route, 'methods', None)
            if not isinstance(path, str) or not methods:
                return _unsupported('api_unknown_route')
            for method in sorted(methods):
                if (method, path) in seen:
                    return _unsupported('api_duplicate_route')
                seen.add((method, path))
                operation = schema.get('paths', {}).get(path, {}).get(method.lower(), {})
                scopes = dependency_scopes(route)
                for requirement in operation.get('security', []):
                    if not isinstance(requirement, dict):
                        return _unsupported('api_unknown_security_requirement')
                    for required in requirement.values():
                        if not isinstance(required, list) or not all(isinstance(scope, str) for scope in required):
                            return _unsupported('api_unknown_security_scopes')
                        scopes.update(required)
                extra = operation.get('x-mrsm-scopes', [])
                if not isinstance(extra, list) or not all(isinstance(scope, str) for scope in extra):
                    return _unsupported('api_unknown_declared_scopes')
                scopes.update(extra)
                entries.append(_handler(catalog_sha256, plugin, source_sha256, {
                    'kind': 'api', 'method': method, 'path': path,
                }, scopes))
        return _complete(entries)
    except Exception as error:
        return _observation_failure('api_hook_or_schema_failed', error)


def require_exact_handler(admitted, observed):
    """IDs alone do not bind config-dependent declared scopes or callback bindings."""
    if not isinstance(admitted, dict) or not isinstance(observed, dict) or admitted != observed:
        raise PermissionError('The handler declaration changed after credential-free discovery.')


def require_api_declaration(app, admitted, registry):
    """Reject a changed scope declaration before dispatching any private ASGI handler."""
    observed = api_declarations(
        app, registry['plugin'], registry.get('archive_sha256') or registry.get('files_sha256'),
        registry['source_catalog_sha256'],
    )
    if observed.get('state') != 'complete':
        raise PermissionError('The private API declaration is unsupported.')
    matches = [entry for entry in observed['entries'] if entry['selector'] == admitted['selector']]
    if len(matches) != 1:
        raise PermissionError('The exact private API declaration is unavailable.')
    require_exact_handler(admitted, matches[0])


def require_mcp_declaration(entry, admitted, metadata):
    """Compare current runtime scopes and input selectors, not a weaker cached contract."""
    scopes = getattr(entry, 'scopes', ())
    if not isinstance(scopes, (list, tuple)) or not all(isinstance(scope, str) for scope in scopes):
        raise PermissionError('The private MCP scope declaration is unsupported.')
    if sorted(set(scopes)) != admitted['scopes']:
        raise PermissionError('The private MCP scope declaration changed.')
    kind = admitted['selector']['kind']
    if kind == 'mcp_tool':
        fields = {'input_schema': getattr(entry, 'input_schema', None),
                  'output_schema': getattr(entry, 'output_schema', None)}
    elif kind == 'mcp_resource':
        fields = {
            'uri': getattr(entry, 'uri', None),
            'uri_template': getattr(entry, 'uri_template', None),
            'mime_type': getattr(entry, 'mime_type', None),
        }
    elif kind == 'mcp_prompt':
        fields = {'arguments': getattr(entry, 'arguments', ()) or []}
    else:
        raise PermissionError('Invalid exact MCP handler kind.')
    if any(metadata.get(field) != value for field, value in fields.items()):
        raise PermissionError('The private MCP input declaration changed.')


def dash_bindings(spec):
    """Read only finite string bindings; wildcard/component-code callbacks stay unsupported."""
    declared = spec.get('output')
    declared = declared if isinstance(declared, list) else [declared]
    outputs = []
    for output in declared:
        component_id = getattr(output, 'component_id', None)
        component_property = getattr(output, 'component_property', None)
        if not isinstance(component_id, str) or not isinstance(component_property, str):
            raise PermissionError('Pattern callback outputs require another typed protocol.')
        outputs.append({'id': component_id, 'property': component_property})
    inputs, state = spec.get('inputs', []), spec.get('state', [])
    if any(not isinstance(group, list) for group in (inputs, state)) or any(
        not isinstance(binding, dict) or set(binding) != {'id', 'property'}
        or not all(isinstance(value, str) for value in binding.values())
        for group in (inputs, state) for binding in group
    ):
        raise PermissionError('Pattern callback inputs require another typed protocol.')
    return {'inputs': inputs, 'state': state, 'outputs': outputs}


def require_dash_declaration(spec, admitted):
    if admitted.get('scopes') != [] or dash_bindings(spec) != admitted.get('dash'):
        raise PermissionError('The private callback declaration changed.')
