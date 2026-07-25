#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the MCP resources.

Resources are documents, not operations: things a client can fetch once, cache,
and cite. Anything that reads like a lookup table (the action list, the connector
list) belongs here rather than as a tool, so it does not consume a tool slot.
"""

from __future__ import annotations

import json

import meerschaum as mrsm
from meerschaum.mcp._registry import resource
from meerschaum.utils.typing import List


@resource(
    'meerschaum_pipes',
    (
        "Every registered pipe on the default instance, with its keys, tags, index columns, and "
        "dtypes. Use the `list_pipes` tool instead when you need filtering or paging."
    ),
    title='Registered pipes',
    uri='mrsm://pipes',
    scopes=['pipes:read'],
)
def _pipes_resource(**kwargs) -> str:
    from meerschaum.mcp._tools import _pipe_summary
    pipes = mrsm.get_pipes(as_list=True)
    return json.dumps([_pipe_summary(pipe) for pipe in pipes], indent=2, default=str)


@resource(
    'meerschaum_pipe',
    (
        "A single pipe's attributes. Address a null location key as 'None', e.g. "
        "mrsm://pipes/sql:main/weather/None."
    ),
    title='Pipe attributes',
    uri_template='mrsm://pipes/{connector_keys}/{metric_key}/{location_key}',
    scopes=['pipes:read'],
)
def _pipe_resource(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    **kwargs
) -> str:
    from meerschaum.mcp._tools import _resolve_pipe
    pipe = _resolve_pipe({
        'connector_keys': connector_keys,
        'metric_key': metric_key,
        'location_key': location_key,
    })
    return json.dumps(pipe.attributes, indent=2, default=str)


@resource(
    'meerschaum_actions',
    (
        "The names of every available Meerschaum action, for use with the `execute_action` tool. "
        "Actions blocked by this server's denylist are marked."
    ),
    title='Available actions',
    uri='mrsm://actions',
    scopes=['actions:execute'],
)
def _actions_resource(**kwargs) -> str:
    from meerschaum.actions import actions as mrsm_actions
    from meerschaum.mcp._security import is_action_permitted

    action_names = sorted(mrsm_actions.keys())
    return json.dumps(
        {
            'actions': [
                {
                    'name': name,
                    'permitted': is_action_permitted(name)[0],
                }
                for name in action_names
            ],
        },
        indent=2,
    )


@resource(
    'meerschaum_connectors',
    (
        "The keys (type:label) of the connectors configured on this instance, e.g. 'sql:main'. "
        "Credentials are never included."
    ),
    title='Configured connectors',
    uri='mrsm://connectors',
    scopes=['connectors:read'],
)
def _connectors_resource(**kwargs) -> str:
    from meerschaum.config import get_config

    connectors = get_config('meerschaum', 'connectors') or {}
    keys: List[str] = []
    for connector_type, labels in connectors.items():
        if not isinstance(labels, dict):
            continue
        for label in labels:
            if label == 'default':
                continue
            keys.append(f"{connector_type}:{label}")

    return json.dumps({'connectors': sorted(keys)}, indent=2)


@resource(
    'meerschaum_docs',
    (
        "How to use this MCP server: Meerschaum's data model, the tools available here, and the "
        "OAuth2 scope each one requires. Read this first if a tool's own description is not "
        "enough. Served locally — no network access required."
    ),
    title='Meerschaum MCP reference',
    uri='mrsm://docs',
    mime_type='text/markdown',
    scopes=[],
)
def _docs_resource(**kwargs) -> str:
    from meerschaum.mcp._docs import render_reference
    return render_reference()
