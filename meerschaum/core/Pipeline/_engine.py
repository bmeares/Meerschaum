#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The bridge to the Rust Syncing Engine (`meerschaum._core`).

The engine is Rust-only (design §5): a bare `pipe.sync()` never reaches it (R1), but
`parents=` / `children=` and `mrsm.Pipeline` do.
Requests and results cross as JSON text.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Union

#: Keyword arguments of `Pipe.sync()` the engine consumes itself.
ENGINE_KEYWORDS = frozenset({
    'begin', 'end', 'force', 'retries', 'min_seconds', 'check_existing', 'enforce_dtypes',
    'chunksize', 'workers', 'params', 'yes', 'pipeline_definitions', 'run_id', 'run_name', 'graph', 'resume', 'on_event',
})

#: Keyword arguments of `Pipe.sync()` with no meaning for an engine run.
IGNORED_KEYWORDS = frozenset({
    'blocking', 'callback', 'error_callback', 'sync_chunks', '_inplace', 'df',
})

_SQL_ATTRIBUTES = ('flavor', 'uri', 'host', 'port', 'username', 'password', 'database')


def get_core():
    """
    Return the `meerschaum._core` extension module, or raise `CoreNotBuiltError`
    (an `ImportError` naming the build script) when it is not built.
    """
    from meerschaum import _core
    if _core.load() is None:
        raise _core.CoreNotBuiltError(
            "The Syncing Engine (`parents=`, `children=`, `mrsm.Pipeline`) is part of the Rust "
            f"extension, which is not built ({_core.LOAD_ERROR}).\n"
            f"Build it from the repository root with `{_core.BUILD_SCRIPT}`."
        )
    return _core


def get_depth(value: Union[bool, int, None]) -> Union[bool, int, None]:
    """
    Normalize a `parents` / `children` argument: `True` is every generation, a positive `int` is
    that many generations, and anything falsy (`None`, `False`, `0`) is no traversal.
    """
    if value is None or value is False:
        return None
    if value is True:
        return True
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"A traversal depth must not be negative (got {value}).")
        return value or None
    raise ValueError(
        f"A traversal depth must be True or an integer number of generations (got {value!r})."
    )


def _bound(value: Union[datetime, int, str, None]) -> Union[str, int, None]:
    """
    Encode a sync bound for the engine. `''` and `None` mean the engine's default
    (a root syncs from its sync time minus the backtrack).
    """
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        raise ValueError("A sync bound must be a datetime or an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        dateutil_parser = mrsm.attempt_import('dateutil.parser')
        value = dateutil_parser.parse(value)
    if hasattr(value, 'to_pydatetime'):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        raise ValueError(f"A sync bound must be a datetime or an integer (got {value!r}).")
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat()


def connector_attributes(connector: Any) -> Optional[Dict[str, Any]]:
    """
    The attributes of an ad hoc `sql` connector, so the engine can rebuild it
    (connectors in a config registry are resolved by keys instead).
    """
    if getattr(connector, 'type', None) != 'sql':
        return None
    attributes = {key: connector.__dict__.get(key, None) for key in _SQL_ATTRIBUTES}
    attributes['schema'] = getattr(connector, 'schema', None)
    database = str(attributes.get('database') or '')
    uri = str(attributes.get('uri') or '')
    if ':memory:' in database or ':memory:' in uri:
        raise ValueError(
            f"'{connector}' is an in-memory database, which the Syncing Engine cannot share "
            "with this process."
        )
    return attributes


def build_request(
    instance: Any,
    targets: Optional[List[Any]] = None,
    filters: Optional[Dict[str, Any]] = None,
    parents: Union[bool, int, None] = None,
    children: Union[bool, int, None] = None,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Build the JSON request of an engine call: the instance, the selection and the sync options.

    `targets` are `Pipe` objects (or `(connector_keys, metric_key, location_key[, instance])`
    tuples); `filters` selects by `connector_keys`, `metric_keys`, `location_keys` and `tags`.
    """
    request: Dict[str, Any] = {
        'instance': str(instance),
        'parents': get_depth(parents),
        'children': get_depth(children),
        'connectors': {},
    }
    connectors = request['connectors']
    attributes = connector_attributes(instance)
    if attributes is not None:
        connectors[str(instance)] = attributes

    request_targets = []
    for target in targets or []:
        if isinstance(target, mrsm.Pipe):
            keys = [
                target.connector_keys,
                target.metric_key,
                target.location_key,
                target.instance_keys,
            ]
            try:
                source_attributes = (
                    connector_attributes(target.connector)
                    if target.connector_keys.startswith('sql:')
                    else None
                )
            except Exception:
                source_attributes = None
            if source_attributes is not None:
                connectors.setdefault(target.connector_keys, source_attributes)
            instance_attributes = connector_attributes(target.instance_connector)
            if instance_attributes is not None:
                connectors.setdefault(target.instance_keys, instance_attributes)
        else:
            keys = list(target)
            if len(keys) == 2:
                keys.append(None)
            if len(keys) == 3:
                keys.append(str(instance))
        request_targets.append(keys)
    if request_targets:
        request['targets'] = request_targets
    elif filters:
        request['filter'] = {
            key: (list(value) if isinstance(value, (list, tuple)) else [value])
            for key, value in filters.items()
            if value
        }
    elif targets is not None:
        request['targets'] = []

    sync = {}
    for key in (
        'check_existing', 'enforce_dtypes', 'force', 'retries', 'min_seconds', 'chunksize',
        'params',
    ):
        if key in kwargs:
            sync[key] = kwargs[key]
    extra = {
        key: value
        for key, value in kwargs.items()
        if key not in ENGINE_KEYWORDS and key not in IGNORED_KEYWORDS
    }
    if extra:
        sync['extra'] = extra
    request['sync'] = sync
    request['begin'] = _bound(kwargs.get('begin', None))
    request['end'] = _bound(kwargs.get('end', None))
    for key in ('workers', 'yes', 'pipeline_definitions', 'run_id', 'run_name', 'graph', 'resume'):
        if kwargs.get(key, None) is not None:
            request[key] = kwargs[key]
    if kwargs.get('force', False) and 'yes' not in request:
        request['yes'] = True
    return request


def _default(value: Any) -> Any:
    """Serialize the odd value (a `datetime` in `params`) a sync keyword may carry."""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def call(name: str, request: Dict[str, Any], **kwargs: Any) -> Any:
    """
    Call the engine function `name` with `request` (and keyword arguments such as
    `engine_run`'s `on_event`) and return the decoded JSON result.
    """
    core = get_core()
    response = getattr(core, name)(json.dumps(request, default=_default), **kwargs)
    return json.loads(response)


#: The engine state rows that belong to one pipe: `(metric, column)` of `mrsm:<metric>`.
_PIPE_STATE = (
    ('engine_ledger', 'pipe_ref'),
    ('engine_cursors', 'parent_ref'),
    ('engine_cursors', 'child_ref'),
    ('engine_state', 'pipe_ref'),
)


def forget_engine_state(pipe: Any, operation: str) -> None:
    """
    After a successful `pipe.drop()`, `pipe.delete()` or full `pipe.clear()`, forget the
    Syncing Engine's state for the pipe (its ledger entries, the edge cursors it is part of and
    its definition hash), so a pipe recreated under the same keys starts fresh, as in v4.

    The Rust instance connectors do this in their own drop, delete and clear paths
    (`pipe_ops::forget_engine_state`, used by the CLI and the server); this covers every pipe
    method call from Python, whichever implementation serves the instance (`operation` names the
    method that succeeded). It is best effort and never raises, and does nothing on an instance
    where the engine never ran (no `mrsm_engine_runs` table). Run history is kept.

    On SQL instances the Rust connector forgets it where it keeps it (`_mrsm_internal` on
    schema flavors, namespaced by the instance schema), so the placement rule lives in one place.
    """
    try:
        if str(pipe.connector_keys) == 'mrsm':
            return
        connector = pipe.instance_connector
        if connector is None:
            return
        ### On an API instance the rows are keyed on this client's `api:<label>` pipe refs and
        ### live on the server's instance; the client clears them over `/engine/pipes/*`
        ### (the generic path below: the API connector routes `mrsm:engine_*` pipes there).
        if getattr(connector, 'type', None) == 'sql' and hasattr(connector, '_rust_method'):
            connector._rust_method('forget_engine_state')(pipe)
            return

        def state_pipe(metric: str):
            return mrsm.Pipe(
                'mrsm', metric,
                instance=connector,
                temporary=True,
                static=True,
                cache=False,
                target=f'mrsm_{metric}',
            )

        if not state_pipe('engine_runs').exists():
            return
        pipe_ref = '|'.join((
            str(pipe.instance_keys),
            str(pipe.connector_keys),
            str(pipe.metric_key),
            str(pipe.location_key),
        ))
        for metric, column in _PIPE_STATE:
            state = state_pipe(metric)
            if state.exists():
                connector.clear_pipe(state, params={column: pipe_ref})
    except Exception:
        pass
