#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""The console's SQL access guard, delegated to the API server's Rust implementation.

Python only collects the hidden pipes and their connector-provided physical table aliases,
then resolves templates before repeating the same Rust check. Identifier parsing, database
clipping, internal-table/config restrictions and catalog checks have one implementation.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple

DEFINITION_KEYS: Tuple[Tuple[str, ...], ...] = (
    ('fetch', 'definition'), ('definition',), ('query',), ('sql',),
)
TARGET_KEYS: Tuple[str, ...] = ('target', 'target_name', 'target_table', 'target_table_name')


class HiddenTables(list):
    """Hidden target aliases together with the connector's identifier clipping rule."""

    def __init__(self, names=(), identifier_byte_limit=None):
        super().__init__(names)
        self.identifier_byte_limit = identifier_byte_limit


def pipe_metadata(pipe: Any) -> Dict[str, Any]:
    """The core's configured, default and physical targets, without database discovery."""
    from meerschaum import _core
    instance = pipe.instance_connector
    if getattr(instance, 'type', None) == 'sql':
        context = {'connector': instance._core_connector(discover_schema=False)}
    else:
        from meerschaum.core.Pipe._status import _status_context
        context = _status_context(pipe)
    return dict(_core.access_pipe_targets(pipe, context))


def check_definition(sql: str, hidden: Iterable[str]) -> Optional[str]:
    """The server's refusal for one raw or resolved SQL definition, or `None`."""
    from meerschaum import _core
    return _core.access_sql_guard(
        str(sql or ''), list(hidden), getattr(hidden, 'identifier_byte_limit', None),
    )


def check_target(target: str, hidden: Iterable[str]) -> Optional[str]:
    """The server's refusal for a target, including internal tables and config references."""
    from meerschaum import _core
    return _core.access_sql_guard(
        str(target or ''), list(hidden), getattr(hidden, 'identifier_byte_limit', None), target=True,
    )


def definitions(parameters: Dict[str, Any]) -> List[str]:
    """Nonempty SQL definition strings, in their normal precedence order."""
    out = []
    for path in DEFINITION_KEYS:
        value: Any = parameters
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if value is not None and str(value).strip():
            out.append(str(value))
    return out


def hidden_tables(viewer: Any, instance: Any) -> HiddenTables:
    """Hidden configured/default/physical targets and the backend's clipping rule."""
    import meerschaum as mrsm
    from meerschaum.api.dash import access
    if viewer.no_auth or viewer.is_admin:
        return HiddenTables()
    conn = instance if not isinstance(instance, str) else mrsm.get_connector(instance)
    carrier = mrsm.Pipe('mrsm', 'access', instance=conn, temporary=True)
    limit = pipe_metadata(carrier).get('identifier_byte_limit')
    names = []
    for ck, mk, lk, parameters in access.registered_pipes(conn):
        if access.can_see_pipe(viewer, parameters):
            continue
        pipe = mrsm.Pipe(ck, mk, lk, instance=conn, parameters=parameters, temporary=True)
        names.extend(pipe_metadata(pipe)['targets'])
    return HiddenTables(list(dict.fromkeys(names)), limit)


def check_parameters(
    viewer: Any, pipe: Any, parameters: Dict[str, Any], *, check_definitions: bool = True,
) -> Optional[str]:
    """Check targets plus raw and resolved definitions with the shared Rust guard."""
    texts = definitions(parameters) if check_definitions else []
    if texts and not viewer.can('pipes:define_sql'):
        return 'Your account may not set SQL definitions.'
    import meerschaum as mrsm
    admin = viewer.no_auth or viewer.is_admin
    hidden = hidden_tables(viewer, pipe.instance_connector)
    for key in (() if admin else TARGET_KEYS):
        if parameters.get(key):
            refusal = check_target(str(parameters[key]), hidden)
            if refusal:
                return refusal
    # A default/physical name may collide even when the raw parameters name no target.
    candidate = mrsm.Pipe(
        pipe.connector_keys, pipe.metric_key, pipe.location_key,
        instance=pipe.instance_connector, parameters=parameters, temporary=True,
    )
    for target in (() if admin else pipe_metadata(candidate)['targets']):
        refusal = check_target(target, hidden)
        if refusal:
            return refusal
    source_hidden = hidden
    if texts:
        from meerschaum.api.dash import access, engine, pipe_writes
        source_keys = pipe_metadata(candidate)['definition_instance_keys']
        try:
            source = engine.get_instance(source_keys, strict=True)
            if not admin and not any(
                pipe_writes.connector_allows(viewer, str(source), team)[0]
                for team in access.assignable_teams(viewer)
            ):
                return f"The connector '{source_keys}' is not available to your teams."
            source_hidden = hidden_tables(viewer, source)
        except Exception as e:
            return str(e)
    if admin:
        return None
    for sql in texts:
        refusal = check_definition(sql, source_hidden)
        if refusal:
            return refusal
        resolved = resolve_templates(sql, candidate)
        if resolved != sql:
            refusal = check_definition(resolved, source_hidden)
            if refusal:
                return refusal
    return None


def check_query(viewer: Any, query: str, pipe: Any) -> Optional[str]:
    """New pipe Preview uses the same checks as saving a definition."""
    return check_parameters(viewer, pipe, {'fetch': {'definition': str(query or '')}})


def resolve_templates(sql: str, pipe: Any) -> str:
    """Resolve pipe templates; the guard has already checked the raw string."""
    if '{{' not in str(sql):
        return str(sql)
    from meerschaum.utils.pipes import replace_pipes_syntax
    return str(replace_pipes_syntax(str(sql), _pipe=pipe))
