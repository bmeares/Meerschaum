#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the MCP tools.

Handlers return plain dicts. The dispatcher serializes them into both the
`content` text block and `structuredContent`, so handlers never format output
themselves.
"""

from __future__ import annotations

import json

import meerschaum as mrsm
from meerschaum.mcp._registry import tool, paginate, encode_cursor, decode_cursor
from meerschaum.mcp._security import (
    is_read_only_query,
    is_action_permitted,
    check_action_chain,
    check_action_execution_allowed,
    check_instance_keys,
    get_mcp_context,
)
from meerschaum.utils.typing import Any, Dict, List, Optional

DEFAULT_ROW_LIMIT: int = 200
MAX_ROW_LIMIT: int = 10_000
DEFAULT_PAGE_SIZE: int = 100

### Instance tables which hold credentials and metadata. The REST routes refuse
### to read or write a pipe pointing at one of these, and so do the MCP tools:
### otherwise `pipes:read` on a pipe registered with `target: mrsm_users` would
### return password hashes.
PROTECTED_TABLES: frozenset = frozenset({
    'mrsm_users', 'mrsm_plugins', 'mrsm_pipes', 'mrsm_tokens',
})

_CONNECTOR_KEYS_SCHEMA = {
    'type': 'string',
    'description': "The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'.",
}
_METRIC_KEY_SCHEMA = {
    'type': 'string',
    'description': "The pipe's metric key, e.g. 'weather'.",
}
_LOCATION_KEY_SCHEMA = {
    'type': 'string',
    'description': "The pipe's location key. Omit (or pass 'None') for a null location.",
}
_INSTANCE_KEYS_SCHEMA = {
    'type': 'string',
    'description': (
        "Where the pipe's metadata and data are stored, e.g. 'sql:main'. "
        "Omit for the server's default instance."
    ),
}
_PARAMS_SCHEMA = {
    'type': 'object',
    'description': (
        "Additional WHERE filters, e.g. {\"station\": \"KGMU\"}. "
        "A list means IN, a leading '_' negates, and null means IS NULL."
    ),
}
_BEGIN_SCHEMA = {
    'type': 'string',
    'description': (
        "Inclusive lower bound on the datetime axis (ISO 8601), "
        "or an integer as a string for integer-axis pipes. Omit for no lower bound."
    ),
}
_END_SCHEMA = {
    'type': 'string',
    'description': (
        "Exclusive upper bound on the datetime axis (ISO 8601), "
        "or an integer as a string for integer-axis pipes. Omit for no upper bound."
    ),
}
_CURSOR_SCHEMA = {
    'type': 'string',
    'description': (
        "Opaque cursor from a previous call's `next_cursor`. "
        "Omit to start from the beginning."
    ),
}


def _pipe_schema(
    extra_properties: Optional[Dict[str, Any]] = None,
    extra_required: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Return an input schema keyed by the three pipe keys plus `instance_keys`.
    """
    properties: Dict[str, Any] = {
        'connector_keys': _CONNECTOR_KEYS_SCHEMA,
        'metric_key': _METRIC_KEY_SCHEMA,
        'location_key': _LOCATION_KEY_SCHEMA,
        'instance_keys': _INSTANCE_KEYS_SCHEMA,
    }
    properties.update(extra_properties or {})
    return {
        'type': 'object',
        'required': ['connector_keys', 'metric_key'] + (extra_required or []),
        'properties': properties,
    }


_SUCCESS_TUPLE_OUTPUT_SCHEMA = {
    'type': 'object',
    'required': ['success', 'message'],
    'properties': {
        'success': {'type': 'boolean'},
        'message': {'type': 'string'},
    },
}


def _check_instance_keys(args: Dict[str, Any]) -> None:
    """
    Raise a `PermissionError` when the caller may not reach `instance_keys`.
    """
    permitted, reason = check_instance_keys(args.get('instance_keys') or None)
    if not permitted:
        raise PermissionError(reason)


def _check_protected_target(pipe: mrsm.Pipe) -> None:
    """
    Raise a `PermissionError` when a pipe points at a protected instance table.
    """
    if pipe.target in PROTECTED_TABLES:
        raise PermissionError(
            f"Refusing to access protected table '{pipe.target}'."
        )


def _resolve_pipe(args: Dict[str, Any]) -> mrsm.Pipe:
    """
    Return the `Pipe` addressed by a tool call's arguments.

    Raises a `PermissionError` when the caller's instance permissions do not
    cover `instance_keys`, or when the pipe targets a protected instance table.
    """
    location_key = args.get('location_key', None)
    if location_key in ('None', '', 'null', None):
        location_key = None

    _check_instance_keys(args)

    pipe = mrsm.Pipe(
        args['connector_keys'],
        args['metric_key'],
        location_key,
        instance=(args.get('instance_keys') or None),
    )
    _check_protected_target(pipe)
    return pipe


def _pipe_summary(pipe: mrsm.Pipe) -> Dict[str, Any]:
    """
    Return a compact, JSON-safe description of a pipe.
    """
    return {
        'connector_keys': pipe.connector_keys,
        'metric_key': pipe.metric_key,
        'location_key': pipe.location_key,
        'instance_keys': pipe.instance_keys,
        'tags': pipe.tags,
        'columns': pipe.columns,
        'dtypes': {col: str(typ) for col, typ in (pipe.dtypes or {}).items()},
    }


def _df_to_records(df) -> List[Dict[str, Any]]:
    """
    Return a DataFrame as a list of JSON-safe records.

    Uses Meerschaum's own serializer so that `numeric`, `bytes`, `geometry`, and
    timezone-aware datetime columns round-trip correctly.
    """
    if df is None:
        return []

    from meerschaum.utils.dataframe import to_json
    if hasattr(df, 'empty') and df.empty:
        return []

    return json.loads(to_json(df, orient='records'))


def _clamp_limit(value: Any) -> int:
    """
    Return a row limit within `[1, MAX_ROW_LIMIT]`.
    """
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_ROW_LIMIT

    return max(1, min(limit, MAX_ROW_LIMIT))


@tool(
    'list_pipes',
    (
        "List registered Meerschaum pipes and their keys, tags, index columns, and dtypes. "
        "Start here to discover what data is available. Results are paginated — pass the "
        "returned `next_cursor` back as `cursor` to continue."
    ),
    title='List pipes',
    scopes=['pipes:read'],
    read_only=True,
    idempotent=True,
    input_schema={
        'type': 'object',
        'properties': {
            'connector_keys': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': "Only include pipes with these connector keys. Omit for all.",
            },
            'metric_keys': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': "Only include pipes with these metric keys. Omit for all.",
            },
            'location_keys': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': (
                    "Only include pipes with these location keys. "
                    "Use 'None' to match a null location."
                ),
            },
            'tags': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': "Only include pipes carrying at least one of these tags.",
            },
            'instance_keys': _INSTANCE_KEYS_SCHEMA,
            'cursor': _CURSOR_SCHEMA,
        },
    },
    output_schema={
        'type': 'object',
        'required': ['pipes'],
        'properties': {
            'pipes': {'type': 'array', 'items': {'type': 'object'}},
            'count': {'type': 'integer', 'description': "Pipes in this page."},
            'total': {'type': 'integer', 'description': "Pipes matching the filters."},
            'next_cursor': {'type': ['string', 'null']},
        },
    },
)
def _list_pipes(**args) -> Dict[str, Any]:
    _check_instance_keys(args)
    kwargs: Dict[str, Any] = {'as_list': True}
    for key in ('connector_keys', 'metric_keys', 'location_keys', 'tags'):
        if args.get(key):
            kwargs[key] = args[key]
    if args.get('instance_keys'):
        kwargs['instance'] = args['instance_keys']

    pipes = [
        pipe for pipe in mrsm.get_pipes(**kwargs)
        if pipe.target not in PROTECTED_TABLES
    ]
    page, next_cursor = paginate(pipes, args.get('cursor'), page_size=DEFAULT_PAGE_SIZE)
    return {
        'pipes': [_pipe_summary(pipe) for pipe in page],
        'count': len(page),
        'total': len(pipes),
        'next_cursor': next_cursor,
    }


@tool(
    'get_pipe_attributes',
    (
        "Return a pipe's full attributes: its parameters dict (columns, dtypes, tags, fetch "
        "definition, verify settings) as registered on the instance."
    ),
    title='Get pipe attributes',
    scopes=['pipes:read'],
    read_only=True,
    idempotent=True,
    input_schema=_pipe_schema(),
)
def _get_pipe_attributes(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    return {'attributes': pipe.attributes}


@tool(
    'get_pipe_stats',
    (
        "Return a pipe's shape in one call: whether its table exists, its rowcount, its newest "
        "and oldest datetime values (sync time), its verification bound time, and the column "
        "types as they actually exist in the database. Prefer this over several separate "
        "lookups."
    ),
    title='Get pipe stats',
    scopes=['pipes:read'],
    read_only=True,
    idempotent=True,
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
        'params': _PARAMS_SCHEMA,
    }),
    output_schema={
        'type': 'object',
        'properties': {
            'exists': {'type': 'boolean'},
            'rowcount': {'type': ['integer', 'null']},
            'newest_sync_time': {'type': ['string', 'integer', 'null']},
            'oldest_sync_time': {'type': ['string', 'integer', 'null']},
            'bound_time': {'type': ['string', 'integer', 'null']},
            'columns_types': {'type': ['object', 'null']},
            'target': {'type': 'string'},
        },
    },
)
def _get_pipe_stats(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    begin = args.get('begin') or None
    end = args.get('end') or None
    params = args.get('params') or None

    exists = pipe.exists()
    stats: Dict[str, Any] = {
        'exists': exists,
        'target': pipe.target,
        'rowcount': None,
        'newest_sync_time': None,
        'oldest_sync_time': None,
        'bound_time': None,
        'columns_types': None,
    }
    if not exists:
        return stats

    stats['rowcount'] = pipe.get_rowcount(begin=begin, end=end, params=params)
    stats['newest_sync_time'] = pipe.get_sync_time(params=params, newest=True)
    stats['oldest_sync_time'] = pipe.get_sync_time(params=params, newest=False)
    stats['bound_time'] = pipe.get_bound_time()
    stats['columns_types'] = pipe.get_columns_types()
    return stats


@tool(
    'get_pipe_data',
    (
        "Read rows from a pipe. Narrow the result with `begin`/`end` (the datetime axis) and "
        "`params` (a WHERE filter) — on a datetime-axis pipe a time window is much cheaper than "
        "paging. Set `backtrack_minutes` to read the most recent rows relative to the pipe's "
        "sync time without knowing its bounds. Pass the returned `next_cursor` back as `cursor` "
        "for the next page."
    ),
    title='Get pipe data',
    scopes=['pipes:read'],
    read_only=True,
    idempotent=True,
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
        'params': _PARAMS_SCHEMA,
        'select_columns': {
            'type': 'array',
            'items': {'type': 'string'},
            'description': "Only return these columns. Omit for all.",
        },
        'omit_columns': {
            'type': 'array',
            'items': {'type': 'string'},
            'description': "Return every column except these.",
        },
        'backtrack_minutes': {
            'type': 'integer',
            'description': (
                "Read the rows within this many minutes before the pipe's newest sync time. "
                "Mutually exclusive with `begin`/`end`."
            ),
        },
        'order': {
            'type': 'string',
            'enum': ['asc', 'desc'],
            'description': "Sort direction on the datetime axis. Defaults to 'asc'.",
        },
        'limit': {
            'type': 'integer',
            'description': f"Maximum rows per page (default {DEFAULT_ROW_LIMIT}).",
        },
        'cursor': _CURSOR_SCHEMA,
    }),
    output_schema={
        'type': 'object',
        'required': ['rows'],
        'properties': {
            'rows': {'type': 'array', 'items': {'type': 'object'}},
            'count': {'type': 'integer'},
            'next_cursor': {'type': ['string', 'null']},
        },
    },
)
def _get_pipe_data(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    limit = _clamp_limit(args.get('limit', DEFAULT_ROW_LIMIT))
    offset = decode_cursor(args.get('cursor'))
    params = args.get('params') or None
    backtrack_minutes = args.get('backtrack_minutes', None)

    ### ponytail: offset paging re-reads the first `offset` rows on every page,
    ### because `Pipe.get_data()` has no offset parameter. Fine for the tens-of-
    ### pages case this is meant for; the tool description steers callers to
    ### `begin`/`end` windowing instead, which is what actually scales. Add a
    ### real server-side cursor (see `meerschaum.api._chunks`) if deep paging
    ### shows up in practice.
    fetch_limit = offset + limit + 1

    if backtrack_minutes is not None:
        df = pipe.get_backtrack_data(
            backtrack_minutes=int(backtrack_minutes),
            params=params,
            limit=fetch_limit,
        )
    else:
        df = pipe.get_data(
            select_columns=(args.get('select_columns') or None),
            omit_columns=(args.get('omit_columns') or None),
            begin=(args.get('begin') or None),
            end=(args.get('end') or None),
            params=params,
            order=args.get('order', 'asc'),
            limit=fetch_limit,
        )

    records = _df_to_records(df)
    window = records[offset:(offset + limit)]
    has_more = len(records) > (offset + limit)
    return {
        'rows': window,
        'count': len(window),
        'next_cursor': (encode_cursor(offset + len(window)) if has_more else None),
    }


@tool(
    'register_pipe',
    (
        "Create and register a new pipe (the programmatic equivalent of 'bootstrap pipe'). "
        "Declare its index columns (`datetime` drives incremental sync, `id`/`primary` give "
        "uniqueness) and optional dtypes and tags. For a SQL-fetch pipe, set "
        "`parameters.fetch.definition` to the source query."
    ),
    title='Register pipe',
    scopes=['pipes:write'],
    idempotent=True,
    input_schema=_pipe_schema({
        'columns': {
            'type': 'object',
            'description': (
                "Index columns by role, e.g. "
                "{\"datetime\": \"ts\", \"id\": \"station\", \"primary\": \"id\"}."
            ),
        },
        'dtypes': {
            'type': 'object',
            'description': (
                "Explicit column dtypes, e.g. {\"val\": \"float64\", \"attrs\": \"json\"}."
            ),
        },
        'tags': {
            'type': 'array',
            'items': {'type': 'string'},
            'description': "Tags for grouping and selecting this pipe.",
        },
        'parameters': {
            'type': 'object',
            'description': (
                "The full parameters dict, merged with `columns`/`dtypes`/`tags`. Common keys: "
                "`fetch.definition` (source SQL), `fetch.backtrack_minutes`, `upsert`, "
                "`autotime`, `autoincrement`, `verify.chunk_minutes`."
            ),
        },
    }),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _register_pipe(**args) -> Dict[str, Any]:
    location_key = args.get('location_key', None)
    if location_key in ('None', '', 'null', None):
        location_key = None

    _check_instance_keys(args)

    pipe = mrsm.Pipe(
        args['connector_keys'],
        args['metric_key'],
        location_key,
        instance=(args.get('instance_keys') or None),
        columns=(args.get('columns') or None),
        dtypes=(args.get('dtypes') or None),
        tags=(args.get('tags') or None),
        parameters=(args.get('parameters') or None),
    )
    ### Checked after construction because `target` may be set in `parameters`.
    _check_protected_target(pipe)
    success, message = pipe.register()
    return {'success': success, 'message': message}


@tool(
    'edit_pipe',
    (
        "Edit a registered pipe's parameters (columns, dtypes, tags, fetch definition, verify "
        "settings). By default the supplied parameters are deep-merged into the existing ones; "
        "set `replace` to true to overwrite the whole parameters dict."
    ),
    title='Edit pipe',
    scopes=['pipes:write'],
    idempotent=True,
    input_schema=_pipe_schema(
        {
            'parameters': {
                'type': 'object',
                'description': "Parameters to merge into (or replace) the pipe's parameters.",
            },
            'replace': {
                'type': 'boolean',
                'description': (
                    "Overwrite every parameter instead of deep-merging. Defaults to false."
                ),
                'default': False,
            },
        },
        extra_required=['parameters'],
    ),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _edit_pipe(**args) -> Dict[str, Any]:
    parameters = dict(args.get('parameters') or {})

    ### Don't let an edit re-point a pipe at a protected table.
    for target_key in ('target', 'target_name', 'target_table'):
        if str(parameters.get(target_key, '')) in PROTECTED_TABLES:
            raise PermissionError(
                f"Refusing to target protected table '{parameters[target_key]}'."
            )

    pipe = _resolve_pipe(args)
    if args.get('replace'):
        pipe.parameters = parameters
        success, message = pipe.edit(patch=False)
    else:
        success, message = pipe.update_parameters(parameters, persist=True)

    return {'success': success, 'message': message}


@tool(
    'sync_pipe',
    (
        "Sync a pipe: fetch new data from its source connector and store it on its instance. "
        "This is the normal incremental ingest path. Use `sync_documents` instead to insert rows "
        "you already have."
    ),
    title='Sync pipe',
    scopes=['pipes:write'],
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
    }),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _sync_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.sync(
        begin=(args.get('begin') or None),
        end=(args.get('end') or None),
    )
    return {'success': success, 'message': message}


@tool(
    'sync_documents',
    (
        "Insert or update rows in a pipe from a list of records you supply. No source connector "
        "is consulted — Meerschaum filters duplicates and upserts according to the pipe's "
        "configuration."
    ),
    title='Sync documents',
    scopes=['pipes:write'],
    input_schema=_pipe_schema(
        {
            'documents': {
                'type': 'array',
                'items': {'type': 'object'},
                'description': (
                    "Rows as dicts, e.g. "
                    "[{\"ts\": \"2024-01-01\", \"station\": \"KGMU\", \"val\": 1.1}]."
                ),
            },
        },
        extra_required=['documents'],
    ),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _sync_documents(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.sync(args.get('documents') or [])
    return {'success': success, 'message': message}


@tool(
    'verify_pipe',
    (
        "Re-sync a pipe's historical range in chunks, refetching any chunk whose local rowcount "
        "disagrees with its source. Use this to fill gaps after an outage."
    ),
    title='Verify pipe',
    scopes=['pipes:write'],
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
    }),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _verify_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.verify(
        begin=(args.get('begin') or None),
        end=(args.get('end') or None),
    )
    return {'success': success, 'message': message}


@tool(
    'deduplicate_pipe',
    (
        "Find and remove duplicate rows in a pipe by re-syncing the affected chunks. "
        "Rows are deleted and rewritten, so scope it with `begin`/`end`/`params` when you can."
    ),
    title='Deduplicate pipe',
    ### Rows are deleted and rewritten, so this needs the same scope as `clear_pipe`.
    scopes=['pipes:write', 'pipes:delete'],
    destructive=True,
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
        'params': _PARAMS_SCHEMA,
    }),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _deduplicate_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.deduplicate(
        begin=(args.get('begin') or None),
        end=(args.get('end') or None),
        params=(args.get('params') or None),
    )
    return {'success': success, 'message': message}


@tool(
    'clear_pipe',
    (
        "Delete rows from a pipe's table within an optional datetime range and filter, keeping "
        "the table and the pipe's registration. Deleted rows are not recoverable — pass "
        "`begin`/`end`/`params` to scope it, and note that omitting all three deletes every row."
    ),
    title='Clear pipe rows',
    ### Matches the REST route `DELETE /pipes/.../clear`, which requires `pipes:delete`.
    scopes=['pipes:delete'],
    destructive=True,
    input_schema=_pipe_schema({
        'begin': _BEGIN_SCHEMA,
        'end': _END_SCHEMA,
        'params': _PARAMS_SCHEMA,
    }),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _clear_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.clear(
        begin=(args.get('begin') or None),
        end=(args.get('end') or None),
        params=(args.get('params') or None),
    )
    return {'success': success, 'message': message}


@tool(
    'drop_pipe',
    (
        "Drop a pipe's target table while keeping its registration, so it can be re-synced from "
        "its source. All stored rows are lost."
    ),
    title='Drop pipe table',
    scopes=['pipes:drop'],
    destructive=True,
    idempotent=True,
    input_schema=_pipe_schema(),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _drop_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.drop()
    return {'success': success, 'message': message}


@tool(
    'delete_pipe',
    (
        "Delete a pipe entirely: drop its target table and remove its registration and "
        "parameters. This is irreversible and cannot be re-synced afterwards."
    ),
    title='Delete pipe',
    scopes=['pipes:delete'],
    destructive=True,
    idempotent=True,
    input_schema=_pipe_schema(),
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _delete_pipe(**args) -> Dict[str, Any]:
    pipe = _resolve_pipe(args)
    success, message = pipe.delete()
    return {'success': success, 'message': message}


@tool(
    'read_sql',
    (
        "Run a read-only SQL query (or read a whole table by name) through a SQL connector and "
        "return the rows. Only single read-only statements are accepted: anything that writes, "
        "or more than one statement, is refused. Read the `mrsm://connectors` resource for the "
        "available connector keys."
    ),
    title='Read SQL',
    ### `sql:read` and not `connectors:read`: listing connector labels (what the
    ### REST `/connectors` route grants) is a much smaller privilege than running
    ### `SELECT` against every database configured on the host.
    scopes=['sql:read'],
    read_only=True,
    idempotent=True,
    input_schema={
        'type': 'object',
        'required': ['connector_keys', 'query'],
        'properties': {
            'connector_keys': {
                'type': 'string',
                'description': "SQL connector keys, e.g. 'sql:main'.",
            },
            'query': {
                'type': 'string',
                'description': "A single read-only SQL statement, or a table name.",
            },
            'limit': {
                'type': 'integer',
                'description': f"Maximum rows per page (default {DEFAULT_ROW_LIMIT}).",
            },
            'cursor': _CURSOR_SCHEMA,
        },
    },
    output_schema={
        'type': 'object',
        'required': ['rows'],
        'properties': {
            'rows': {'type': 'array', 'items': {'type': 'object'}},
            'count': {'type': 'integer'},
            'next_cursor': {'type': ['string', 'null']},
        },
    },
)
def _read_sql(**args) -> Dict[str, Any]:
    import re
    from meerschaum.mcp._security import strip_sql_noise

    query = args['query']
    permitted, reason = is_read_only_query(query)
    if not permitted:
        raise ValueError(reason)

    ### Protected instance tables are off-limits here too, not just to the pipe tools.
    stripped, _ = strip_sql_noise(query)
    for word in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", (stripped or query)):
        if word.lower() in PROTECTED_TABLES:
            raise PermissionError(
                f"Refusing to query protected table '{word}'."
            )

    connector = mrsm.get_connector(args['connector_keys'])
    if not hasattr(connector, 'read'):
        raise ValueError(
            f"Connector '{args['connector_keys']}' does not support SQL reads."
        )

    limit = _clamp_limit(args.get('limit', DEFAULT_ROW_LIMIT))
    offset = decode_cursor(args.get('cursor'))

    ### Read one bounded chunk instead of the whole result set: an unqualified
    ### `SELECT * FROM big_table` must not materialize in the API worker.
    ### `silent=True` keeps a failed query from printing a traceback to stdout,
    ### which is the protocol channel under the stdio transport.
    fetch_limit = offset + limit + 1
    df = connector.read(query, chunksize=fetch_limit, chunks=1, silent=True)
    if df is None:
        ### `read()` warns and returns `None` on a database error. Without this the
        ### model would be told there were no rows.
        raise ValueError(
            "The query could not be executed. Check it for syntax errors, a missing "
            "table, or insufficient database privileges."
        )

    records = _df_to_records(df)
    window = records[offset:(offset + limit)]
    has_more = len(records) > (offset + limit)
    return {
        'rows': window,
        'count': len(window),
        'next_cursor': (encode_cursor(offset + len(window)) if has_more else None),
    }


@tool(
    'execute_action',
    (
        "Run any Meerschaum CLI action, for anything without a dedicated tool. The "
        "`mrsm://actions` resource lists the available action names. This grants broad control "
        "over the Meerschaum instance and its host, so prefer a dedicated tool when one exists. "
        "Actions which execute arbitrary code are blocked by the server's denylist."
    ),
    title='Execute action',
    scopes=['actions:execute'],
    destructive=True,
    open_world=True,
    input_schema={
        'type': 'object',
        'required': ['action'],
        'properties': {
            'action': {
                'type': 'string',
                'description': (
                    "The action to run, e.g. 'sync pipes' or 'show pipes'. "
                    "The first word is the action; the rest become subactions."
                ),
            },
            'kwargs': {
                'type': 'object',
                'description': (
                    "Keyword arguments for the action, e.g. "
                    "{\"connector_keys\": [\"sql:main\"], \"yes\": true}."
                ),
            },
        },
    },
    output_schema=_SUCCESS_TUPLE_OUTPUT_SCHEMA,
)
def _execute_action(**args) -> Dict[str, Any]:
    from meerschaum.actions import actions as mrsm_actions

    parts = str(args.get('action', '')).strip().split()
    if not parts:
        return {'success': False, 'message': "No action was given."}

    action_name = parts[0]
    if action_name not in mrsm_actions:
        return {
            'success': False,
            'message': (
                f"Unknown action '{action_name}'. "
                "Read the `mrsm://actions` resource for the available actions."
            ),
        }

    permitted, reason = is_action_permitted(action_name)
    if not permitted:
        return {'success': False, 'message': reason}

    ### Same check the REST `/actions` route makes: a non-admin may be barred
    ### from running actions entirely (`system:api:permissions:actions:non_admin`).
    allowed, allowed_message = check_action_execution_allowed()
    if not allowed:
        return {'success': False, 'message': allowed_message}

    kwargs = dict(args.get('kwargs') or {})
    if len(parts) > 1:
        kwargs.setdefault('action', parts[1:])

    ### A denylisted action must not be reachable as another action's subaction,
    ### e.g. `start job` with `{"action": ["sh", "..."]}`.
    chain = list(parts[1:]) + [
        str(word) for word in (
            kwargs.get('action')
            if isinstance(kwargs.get('action'), (list, tuple))
            else [kwargs['action']] if kwargs.get('action') else []
        )
    ]
    chain_permitted, chain_reason = check_action_chain(chain)
    if not chain_permitted:
        return {'success': False, 'message': chain_reason}

    ### Pin the action to the API's own instance, as the REST route does, so an
    ### action cannot reach an instance the caller isn't allowed to address.
    context = get_mcp_context()
    if context and context.get('api'):
        from meerschaum.api import get_api_connector
        instance_keys = kwargs.get('mrsm_instance', None)
        permitted, reason = check_instance_keys(instance_keys)
        if not permitted:
            return {'success': False, 'message': reason}
        kwargs['mrsm_instance'] = instance_keys or str(get_api_connector())

    success, message = mrsm_actions[action_name](**kwargs)
    return {'success': success, 'message': message}
