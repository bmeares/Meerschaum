#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A `Pipeline` is a **selection** over the pipe graph (design §5.2, D4, D5): a set of member
pipes plus how many generations of parents and children to pull in. Save it with a name to
keep it on its instance (`Pipeline.save()`), load it again with `mrsm.Pipeline('name')` and
list the saved ones with `mrsm.get_pipelines()`. The Rust Syncing Engine plans it
(`Pipeline.plan()`), runs it (`Pipeline.sync()`) and records every run
(`Pipeline.get_runs()`, `mrsm.get_runs()`).

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('sql:main', 'daily_rollup', instance='sql:main')

pipe.sync()                       # a plain sync of this pipe (no graph)
pipe.sync(parents=True)           # every ancestor in dependency order, then this pipe
pipe.sync(children=1)             # this pipe, then its direct children

pipeline = mrsm.Pipeline(pipe, parents=True)
print(pipeline.plan())            # a dry run: node | action | bounds | chunks | reason
run = pipeline.sync(workers=4)    # a SuccessTuple with `run.run_id`, `run.inserted`, ...
run = pipeline.sync(on_event=print)   # every event live, as a dict (see `_events.py`)
success, msg = pipeline.save('daily')  # keep the selection on the instance

daily = mrsm.Pipeline('daily')    # load the saved Pipeline
daily.sync()
```

The engine is Rust-only: `parents=`, `children=` and `Pipeline` run in the Rust core. A bare
`pipe.sync()` never uses the engine.

The engine reads the graph from the instance, so every pipe in the selection (and the pipes it
reads) must be registered. `Pipeline.sync()` registers member pipes that are not registered yet
(not for a saved Pipeline, whose members must already exist); `Pipeline.plan()` and
`Pipeline.pipes` never register anything.
"""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.connectors.parse import parse_instance_keys
from meerschaum.utils.typing import (
    Any, Callable, Dict, Iterable, List, Optional, SuccessTuple, Tuple, Union,
)
from meerschaum.core.Pipeline._engine import (
    build_request, call, get_core, get_depth, since_text,
)
from meerschaum.core.Pipeline._events import (
    EVENT_KEYS, EVENT_NAMES, EventCallback, decode_event, with_seconds,
)

_UNSET_BOUND = object()

#: Outcome counts `RunResult` totals over the nodes of a run.
_RUN_TOTALS = ('inserted', 'updated', 'upserted', 'deleted')

__all__ = (
    'Pipeline', 'Plan', 'PlannedNode', 'RunResult', 'get_pipelines', 'get_runs',
    'sync_with_graph', 'EVENT_KEYS', 'EVENT_NAMES', 'decode_event',
)


def _bounds_text(bounds: Optional[Dict[str, Any]]) -> str:
    """The `[begin, end)` text of a planned node's bounds."""
    if not bounds:
        return ''
    begin = bounds.get('begin', None)
    end = bounds.get('end', None)
    return f"[{_bound_text(begin)}, {_bound_text(end)})"


def _bound_text(bound: Any) -> str:
    if isinstance(bound, dict):
        if 'Default' in bound or bound.get('kind', None) == 'default':
            return 'default'
        values = list(bound.values())
        return str(values[0]) if len(values) == 1 else str(bound)
    if bound is None:
        return 'None'
    return str(bound)


class PlannedNode:
    """
    One node of a `Plan`: a pipe, the action the planner chose for it and why.

    Attributes
    ----------
    pipe_ref: str
        `instance|connector_keys|metric_key|location_key`.

    pipe: mrsm.Pipe
        The pipe (built on first access).

    action: str
        `root`, `skip`, `incremental`, `backfill`, `rebuild` or `observe`.

    bounds: Optional[Dict[str, Any]]
        The overall sync window (`None` when nothing syncs).

    chunks: int
        How many chunks the window is split into (0 when the sync chooses its own).

    reason: str
        The planner's human-readable reason.

    provisional: bool
        `True` for a child planned against current state, which is re-planned when its parents
        finish.

    raw: Dict[str, Any]
        The engine's full record of the node (ranges, cursors, definition hashes, warnings).
    """

    def __init__(self, raw: Dict[str, Any]):
        self.raw = raw
        self.pipe_ref: str = raw['pipe_ref']
        self.action: str = raw['action']
        self.bounds: Optional[Dict[str, Any]] = raw.get('bounds', None)
        self.chunks: int = len(raw.get('chunks', None) or [])
        self.reason: str = raw.get('reason', '')
        self.provisional: bool = bool(raw.get('provisional', False))
        self.requires_confirmation: bool = bool(raw.get('requires_confirmation', False))
        self._pipe: Optional[mrsm.Pipe] = None

    @property
    def pipe(self) -> mrsm.Pipe:
        if self._pipe is None:
            self._pipe = _pipe_from_ref(self.pipe_ref)
        return self._pipe

    def to_dict(self) -> Dict[str, Any]:
        """The plan table row of this node."""
        return {
            'node': str(self.pipe),
            'pipe_ref': self.pipe_ref,
            'action': self.action + ('*' if self.provisional else ''),
            'bounds': _bounds_text(self.bounds),
            'chunks': self.chunks,
            'reason': self.reason,
        }

    def __repr__(self) -> str:
        return f"PlannedNode({self.pipe_ref!r}, action={self.action!r})"


def _pipe_from_ref(pipe_ref: str) -> mrsm.Pipe:
    """The `Pipe` a `pipe_ref` (`instance|connector|metric|location`) names."""
    instance_keys, connector_keys, metric_key, location_key = pipe_ref.split('|', 3)
    return mrsm.Pipe(
        connector_keys,
        metric_key,
        None if location_key == 'None' else location_key,
        instance=instance_keys,
    )


def _check_ref(pipe_ref: str) -> str:
    """Return `pipe_ref` if it has the four `|`-separated keys, else raise `ValueError`."""
    parts = pipe_ref.split('|', 3)
    if len(parts) != 4 or not all(parts[:3]):
        raise ValueError(
            f"Invalid pipe reference {pipe_ref!r}: expected "
            "'instance|connector_keys|metric_key|location_key' "
            "(e.g. 'sql:main|sql:main|weather|None')."
        )
    return pipe_ref


class Plan:
    """
    A dry run: what the engine would do, with no data movement.

    `str(plan)` is the plan table (`node | action | bounds | chunks | reason`, `*` marks a
    provisional plan).

    Attributes
    ----------
    nodes: List[PlannedNode]
        The nodes in dependency order.

    edges: List[Tuple[str, str]]
        The `(parent, child)` pairs inside the selection.

    flat: bool
        `True` for a selection with no traversal and no dependencies between its members:
        every node syncs on its own, like `mrsm sync pipes`.

    warnings: List[str]
        Graph and selection warnings (including one per member pipe that is not registered).

    unregistered: List[mrsm.Pipe]
        Member pipes that are not registered on their instance, so the engine cannot plan
        them yet. `Pipeline.sync()` registers them first (except for a saved Pipeline, whose
        members must already be registered); `pipe.register()` registers one now.
    """

    def __init__(
        self,
        data: Dict[str, Any],
        pipeline: Optional['Pipeline'] = None,
        options: Optional[Dict[str, Any]] = None,
        unregistered: Optional[List[mrsm.Pipe]] = None,
    ):
        self.raw = data
        plan = data['plan']
        self.text: str = data.get('text', '')
        self.nodes: List[PlannedNode] = [PlannedNode(node) for node in plan['nodes']]
        self.edges: List[Tuple[str, str]] = [tuple(edge) for edge in plan.get('edges', [])]
        self.flat: bool = bool(plan.get('flat', False))
        self.warnings: List[str] = list(data.get('warnings', None) or [])
        self.unregistered: List[mrsm.Pipe] = list(unregistered or [])
        self._pipeline = pipeline
        self._options = options or {}
        self._unregistered_warnings: List[str] = [
            _unregistered_warning(pipe, pipeline)
            for pipe in self.unregistered
        ]
        self.warnings.extend(self._unregistered_warnings)

    @property
    def pipes(self) -> List[mrsm.Pipe]:
        """The pipes of the plan, in dependency order (`unregistered` pipes are not planned)."""
        return [node.pipe for node in self.nodes]

    def node(self, pipe: Union[mrsm.Pipe, str]) -> Optional[PlannedNode]:
        """The planned node of a pipe (or `pipe_ref`)."""
        pipe_ref = pipe if isinstance(pipe, str) else _pipe_ref(pipe)
        for node in self.nodes:
            if node.pipe_ref == pipe_ref:
                return node
        return None

    def to_dicts(self) -> List[Dict[str, Any]]:
        """The plan table as a list of rows."""
        return [node.to_dict() for node in self.nodes]

    def to_dataframe(
        self,
        as_polars: Optional[bool] = None,
        as_pandas: Optional[bool] = None,
    ) -> Any:
        """
        The plan table (`node`, `action`, `bounds`, `chunks`, `reason`) as a DataFrame of the
        `meerschaum:dataframe` library (Polars for a new root); `as_polars=True` or
        `as_pandas=True` asks for one.
        """
        from meerschaum.utils.dataframe import resolve_dataframe_library, ensure_dataframe_library
        columns = ['node', 'action', 'bounds', 'chunks', 'reason']
        library = resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas)
        module = ensure_dataframe_library(library)
        if library == 'polars':
            schema = {
                'node': module.String,
                'action': module.String,
                'bounds': module.String,
                'chunks': module.Int64,
                'reason': module.String,
            }
            rows = [{col: row.get(col) for col in columns} for row in self.to_dicts()]
            return module.DataFrame(rows, schema=schema, strict=False)
        return module.DataFrame(self.to_dicts(), columns=columns)

    def execute(self, **kwargs: Any) -> 'RunResult':
        """
        Run the pipeline this plan came from with the options it was planned with
        (the engine re-plans against current state as the run proceeds).
        """
        if self._pipeline is None:
            raise ValueError("This plan does not belong to a pipeline.")
        options = {**self._options, **kwargs}
        return self._pipeline.sync(**options)

    def __len__(self) -> int:
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)

    def __str__(self) -> str:
        if not self._unregistered_warnings:
            return self.text
        text = self.text if (not self.text or self.text.endswith('\n')) else self.text + '\n'
        return text + ''.join(f"warning: {warning}\n" for warning in self._unregistered_warnings)

    def __repr__(self) -> str:
        return f"Plan({len(self.nodes)} nodes)"


def _unregistered_warning(pipe: mrsm.Pipe, pipeline: Optional['Pipeline']) -> str:
    """The plan warning of a member pipe that is not registered."""
    owner = pipeline._owner_of(pipe) if pipeline is not None else None
    if owner is not None:
        return (
            f"{pipe} is not registered; register it before syncing "
            f"Pipeline '{owner}' (or remove it from the Pipeline)."
        )
    return (
        f"{pipe} is not registered; `Pipeline.sync()` registers it first "
        "(or call `pipe.register()` to plan it now)."
    )


def _pipe_ref(pipe: mrsm.Pipe) -> str:
    return '|'.join((
        str(pipe.instance_keys),
        str(pipe.connector_keys),
        str(pipe.metric_key),
        str(pipe.location_key),
    ))


class RunResult(tuple):
    """
    The result of an engine run: a `SuccessTuple` (`success, message = run` still unpacks)
    with the run's details. `pipe.sync(parents=..., children=...)`, `Pipeline.sync()` and
    `Plan.execute()` return one.

    Attributes
    ----------
    success: bool
        Whether the run succeeded (a failed node fails the run).

    message: str
        The run summary.

    run_id: Optional[str]
        The ID in the run history (`Pipeline.get_run(run_id)`); `None` if the run never started.

    plan: Optional[Plan]
        The plan that was executed.

    inserted, updated, upserted, deleted: Optional[int]
        Rows inserted (updated, ...) by every node of the run together, like the same fields of
        the `SyncResult` a plain `pipe.sync()` returns. `None` if the run never started.

    duration: float
        How long the run took, in wall-clock seconds (nodes that run in parallel overlap, so
        this is not the sum of the nodes' durations).

    outcomes: List[Dict[str, Any]]
        One entry per node that synced, in completion order: `pipe_ref`, `pipe` (`str(pipe)`)
        and `result` (`success`, `message`, `inserted`, `updated`, `upserted`, `deleted`,
        `changed_intervals`, `duration` (float seconds, or `None`), `begin`, `end`).

    critical_path: List[str]
        The `pipe_ref`s of the plan's critical path.

    warnings: List[str]
        Graph, selection and state warnings.

    record: Optional[Dict[str, Any]]
        The persisted run row and node rows.

    callback_error: Optional[Exception]
        The exception an `on_event` callback raised (the run continued without it), else
        `None`.
    """

    success: bool
    message: str

    def __new__(cls, success: bool, message: str, data: Optional[Dict[str, Any]] = None, **kwargs):
        self = super().__new__(cls, (bool(success), message))
        data = data or {}
        self.success = bool(success)
        self.message = message
        self.run_id = data.get('run_id', None)
        self.outcomes = [with_seconds(outcome) for outcome in data.get('outcomes', None) or []]
        self.critical_path = list(data.get('critical_path', None) or [])
        self.warnings = list(data.get('warnings', None) or [])
        self.duration = float(data.get('duration_seconds', 0.0) or 0.0)
        self.record = data.get('record', None)
        self.plan = Plan(data, **kwargs) if data.get('plan', None) else None
        ran = 'outcomes' in data
        for field in _RUN_TOTALS:
            setattr(self, field, _total(self.outcomes, field) if ran else None)
        self._data = data
        self._instance_request: Optional[Dict[str, Any]] = None
        self.callback_error: Optional[BaseException] = None
        return self

    def __reduce__(self):
        ### The details are JSON data; the pipeline back-reference is dropped.
        return (_rebuild_run_result, (self.success, self.message, self._data))

    @property
    def result(self) -> 'RunResult':
        """The run result (this object: it is already `SuccessTuple`-compatible)."""
        return self

    def get_outcome(self, pipe: Union[mrsm.Pipe, str]) -> Optional[Dict[str, Any]]:
        """The outcome of a node of the run."""
        pipe_ref = pipe if isinstance(pipe, str) else _pipe_ref(pipe)
        for outcome in self.outcomes:
            if outcome['pipe_ref'] == pipe_ref:
                return outcome
        return None

    def to_dict(self) -> Dict[str, Any]:
        """The run's summary fields as a dictionary."""
        return {
            'success': self.success,
            'message': self.message,
            'run_id': self.run_id,
            **{field: getattr(self, field) for field in _RUN_TOTALS},
            'duration': self.duration,
        }

    def events(self, after_seq: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        The run's persisted events (`run_started`, `node_started`, `node_finished`,
        `run_finished`, ...) in order, read from the instance after the run
        (`system.engine.events` sets how fine-grained they are). Each row has the keys of a live
        `on_event` event (`event`, `run_id`, `seq`, `ts`, `pipe_ref` and the event's own
        fields) plus `payload`, the event's own fields as one dict. `ts` is the ISO 8601 text
        the instance stored.

        To receive every event live while the run is going, pass `on_event=` to
        `Pipeline.sync()` (or `pipe.sync(parents=..., children=...)`).
        """
        if self.run_id is None or self._instance_request is None:
            return []
        request = {**self._instance_request, 'run_id': self.run_id}
        if after_seq is not None:
            request['after_seq'] = after_seq
        return _decode_events(call('engine_run_events', request))

    def __repr__(self) -> str:
        return f"RunResult(success={self.success}, run_id={self.run_id!r}, message={self.message!r})"


def _total(outcomes: List[Dict[str, Any]], field: str) -> int:
    """The sum of one count over the nodes of a run."""
    total = 0
    for outcome in outcomes:
        value = (outcome.get('result', None) or {}).get(field, None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total += int(value)
    return total


def _rebuild_run_result(success: bool, message: str, data: Dict[str, Any]) -> RunResult:
    return RunResult(success, message, data)


def _decode_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Decode persisted event rows (`{ts, run_id, seq, pipe_ref, event, payload}`): `payload` is
    parsed and its fields are also set at the top level, as in the live `on_event` events (the
    row's own keys win), with a node result's `duration` in float seconds.
    """
    events = []
    for row in rows:
        payload = row.get('payload', None)
        try:
            decoded = json.loads(payload) if isinstance(payload, str) and payload else payload
        except ValueError:
            decoded = payload
        decoded = with_seconds(decoded)
        fields = (
            {key: value for key, value in decoded.items() if key not in row}
            if isinstance(decoded, dict)
            else {}
        )
        events.append({**row, **fields, 'payload': decoded})
    return events


def _instance_of(pipes: List[mrsm.Pipe], instance: Optional[Any]) -> Any:
    if instance is not None:
        return parse_instance_keys(instance) if isinstance(instance, str) else instance
    if pipes:
        instances = {pipe.instance_keys for pipe in pipes}
        if len(instances) > 1:
            raise ValueError(
                "A Pipeline's member pipes must share one instance (the engine's home instance); "
                f"got {sorted(instances)}. Pass `instance=` to choose one."
            )
        return pipes[0].instance_connector
    return parse_instance_keys(None)


def _registry_of(instance: Optional[Any]) -> Any:
    """The instance connector that keeps saved Pipelines (default: the configured instance)."""
    if instance is None or isinstance(instance, str):
        return parse_instance_keys(instance)
    return instance


def _split_selection(pipes: Any) -> Tuple[List[str], List[Any]]:
    """
    Split the `pipes` argument of `Pipeline()` into saved Pipeline names and member pipes.

    A `str` without `|` is the name of a saved Pipeline and a `str` with `|` is a `pipe_ref`
    (`instance|connector|metric|location`); a `Pipe` or a `(connector_keys, metric_key[,
    location_key[, instance_keys]])` tuple is a member pipe; a list may mix them all; the nested
    dictionary `mrsm.get_pipes()` returns is its pipes.
    """
    if pipes is None:
        return [], []
    if isinstance(pipes, mrsm.Pipe):
        return [], [pipes]
    if isinstance(pipes, str):
        pipes = [pipes]
    elif (
        isinstance(pipes, tuple)
        and 2 <= len(pipes) <= 4
        and isinstance(pipes[0], str)
        and not any(isinstance(key, str) and '|' in key for key in pipes)
    ):
        ### A tuple of two to four keys is one pipe: `('sql:main', 'weather')`.
        return [], [pipes]
    elif isinstance(pipes, dict):
        from meerschaum.utils.pipes import flatten_pipes_dict
        pipes = flatten_pipes_dict(pipes)
    names, targets = [], []
    for item in pipes:
        if isinstance(item, mrsm.Pipe):
            targets.append(item)
        elif isinstance(item, str):
            if '|' in item:
                targets.append(_pipe_from_ref(_check_ref(item)))
            else:
                names.append(item)
        elif isinstance(item, (tuple, list)) and 2 <= len(item) <= 4:
            targets.append(tuple(item))
        else:
            raise TypeError(
                f"Invalid Pipeline member {item!r}: pass a Pipe, a "
                "(connector_keys, metric_key[, location_key]) tuple, a pipe reference "
                "('instance|connector|metric|location') or the name of a saved Pipeline."
            )
    return names, targets


def _read_definition(name: str, registry: Any) -> Dict[str, Any]:
    """Read the saved definition `name` from `registry`, or raise a `ValueError` that helps."""
    from meerschaum.core.Pipeline._saved import validate_name, _read, list_pipelines
    try:
        validate_name(name)
    except ValueError:
        raise ValueError(
            f"{name!r} is not the name of a saved Pipeline (names use letters, numbers, dots, "
            "underscores and hyphens). Pass member pipes as Pipe objects, "
            "(connector_keys, metric_key[, location_key]) tuples or "
            "'instance|connector|metric|location' references, "
            "or select them with tags= / connector_keys= / metric_keys= / location_keys=."
        ) from None
    try:
        return _read(name, registry)
    except ValueError:
        try:
            available = sorted(list_pipelines(registry))
        except Exception:
            available = []
        hint = (
            ' Saved Pipelines: ' + ', '.join(repr(other) for other in available[:20])
            + (', ...' if len(available) > 20 else '') + '.'
            if available
            else ' No Pipelines are saved there yet; save one with `Pipeline(...).save(name)`.'
        )
        raise ValueError(f"Pipeline {name!r} does not exist on '{registry}'.{hint}") from None


def _common(values: List[Any], what: str, names: List[str]) -> Any:
    """The one value every saved Pipeline of a combined selection shares, else raise."""
    distinct = []
    for value in values:
        if value not in distinct:
            distinct.append(value)
    if len(distinct) > 1:
        raise ValueError(
            f"The saved Pipelines {', '.join(repr(name) for name in names)} have different "
            f"`{what}` settings; pass `{what}=` to choose one for the combined selection."
        )
    return distinct[0] if distinct else None


class Pipeline:
    """
    A selection over the pipe graph: member pipes plus traversal depths (design §5.2, D4).

    Parameters
    ----------
    pipes: Union[str, mrsm.Pipe, Iterable[...], None], default None
        What to select:

        - a `str` without `|` is the **name of a saved Pipeline**, loaded from `instance`
          (`mrsm.Pipeline('daily')`; `ValueError` if it does not exist);
        - a `str` with `|` is a pipe reference, `'instance|connector|metric|location'`;
        - a `Pipe`, or a `(connector_keys, metric_key[, location_key])` tuple, is one member;
        - a list may mix all of these: the members of every named Pipeline plus the given
          pipes (the dictionary `mrsm.get_pipes()` returns works too).

    parents: Union[bool, int, None], default None
        `True` includes every ancestor of the members; an `int` includes that many generations.
        For a saved Pipeline, `None` keeps its saved setting and `False` turns traversal off.

    children: Union[bool, int, None], default None
        `True` includes every descendant of the members; an `int` that many generations.
        For a saved Pipeline, `None` keeps its saved setting and `False` turns traversal off.

    tags, connector_keys, metric_keys, location_keys: Union[str, List[str], None], default None
        Select the members by the same filters as `-t`, `-c`, `-m` and `-l`
        (negate a value with a leading `_`). Used when `pipes` is not provided.

    instance: Union[str, InstanceConnector, None], default None
        The instance that keeps the engine state and the saved Pipelines (default: the member
        pipes' instance, else the configured one). Saved Pipelines are loaded from here.

    name: Optional[str], default None
        The name to save this selection under (`save()`), also used in its run IDs.
        Without `pipes` or filters, `Pipeline(name='daily')` loads the saved Pipeline.

    graph: Optional[bool], default None
        Whether the run respects the dependencies between the members even without traversal
        (a child member waits for its parent member and is capped at its watermark).
        A saved Pipeline keeps its `graph` setting (default `True`), like
        `mrsm sync pipelines NAME`; an unsaved selection without `parents` / `children` syncs
        each member on its own, like `mrsm sync pipes`.

    Examples
    --------
    >>> pipeline = mrsm.Pipeline(pipe, parents=True)
    >>> plan = pipeline.plan()
    >>> run = pipeline.sync(workers=4)
    >>> run.success, run.run_id, run.inserted
    >>> pipeline.save('daily')
    >>> mrsm.Pipeline('daily').sync()
    """

    def __init__(
        self,
        pipes: Union[str, mrsm.Pipe, Iterable[Any], None] = None,
        parents: Union[bool, int, None] = None,
        children: Union[bool, int, None] = None,
        tags: Union[str, List[str], None] = None,
        connector_keys: Union[str, List[str], None] = None,
        metric_keys: Union[str, List[str], None] = None,
        location_keys: Union[str, List[str], None] = None,
        instance: Union[str, Any, None] = None,
        name: Optional[str] = None,
        graph: Optional[bool] = None,
    ):
        if name is not None and not isinstance(name, str):
            raise TypeError(f"A Pipeline name must be a string (got {name!r}).")
        names, targets = _split_selection(pipes)
        filters = {
            'tags': tags,
            'connector_keys': connector_keys,
            'metric_keys': metric_keys,
            'location_keys': location_keys,
        }
        filters = {k: ([v] if isinstance(v, str) else v) for k, v in filters.items() if v}
        if not names and not targets and not filters:
            if not name:
                raise ValueError(
                    "A Pipeline needs member pipes, a tags / keys filter or the name of a saved "
                    "Pipeline, e.g. `mrsm.Pipeline(pipe)`, `mrsm.Pipeline(tags=['daily'])` or "
                    "`mrsm.Pipeline('daily')`."
                )
            names = [name]
        if names and filters:
            raise ValueError(
                "A saved Pipeline cannot be combined with a tags / keys filter: "
                "pass the extra pipes themselves."
            )
        definitions = []
        registry = None
        if names:
            registry = _registry_of(instance)
            seen = set()
            for saved_name in names:
                if saved_name in seen:
                    continue
                seen.add(saved_name)
                definitions.append(_read_definition(saved_name, registry))
        self._configure(
            definitions,
            targets,
            filters,
            parents=parents,
            children=children,
            instance=(registry if registry is not None else instance),
            name=name,
            graph=graph,
        )

    def _configure(
        self,
        definitions: List[Dict[str, Any]],
        targets: List[Any],
        filters: Dict[str, Any],
        parents: Union[bool, int, None] = None,
        children: Union[bool, int, None] = None,
        instance: Union[str, Any, None] = None,
        name: Optional[str] = None,
        graph: Optional[bool] = None,
    ) -> None:
        """Set the selection: saved definitions (in order), extra members and filters."""
        names = [definition['name'] for definition in definitions]
        ### A member given as a `Pipe` object keeps that object (and its in-memory parameters);
        ### the other members of a saved Pipeline are built from their references.
        given = {
            _pipe_ref(target): target
            for target in targets
            if isinstance(target, mrsm.Pipe)
        }
        members: List[Any] = []
        seen = set()
        loaded: Dict[str, mrsm.Pipe] = {}
        for definition in definitions:
            for ref in definition.get('targets', None) or []:
                if ref in seen:
                    continue
                seen.add(ref)
                if ref in given:
                    members.append(given[ref])
                    continue
                member = _pipe_from_ref(ref)
                members.append(member)
                loaded[ref] = member
        for target in targets:
            if isinstance(target, mrsm.Pipe):
                ref = _pipe_ref(target)
                if ref in seen:
                    continue
                seen.add(ref)
            members.append(target)
        self._definitions: List[Dict[str, Any]] = [deepcopy(d) for d in definitions]
        self._loaded = loaded
        self._sources: List[Any] = list(names) + list(targets)
        self._targets: List[Any] = members
        self._filters: Dict[str, Any] = filters
        self.parents = (
            get_depth(parents)
            if parents is not None or not definitions
            else _common([get_depth(d.get('parents')) for d in definitions], 'parents', names)
        )
        self.children = (
            get_depth(children)
            if children is not None or not definitions
            else _common([get_depth(d.get('children')) for d in definitions], 'children', names)
        )
        self._graph: Optional[bool] = None if graph is None else bool(graph)
        if name is None and len(definitions) == 1 and not targets:
            name = definitions[0]['name']
        self.name: Optional[str] = name
        if definitions:
            self.instance = _registry_of(instance)
        else:
            pipe_targets = [target for target in members if isinstance(target, mrsm.Pipe)]
            self.instance = _instance_of(pipe_targets, instance)

    @classmethod
    def _from_definitions(
        cls,
        definitions: List[Dict[str, Any]],
        instance: Any,
        name: Optional[str] = None,
    ) -> 'Pipeline':
        """A Pipeline bound to saved definitions which were already read."""
        pipeline = cls.__new__(cls)
        pipeline._configure(definitions, [], {}, instance=instance, name=name)
        return pipeline

    @classmethod
    def load(cls, name: str, instance=None) -> 'Pipeline':
        """
        Load the saved Pipeline `name` from `instance` (default: the configured instance).
        The same as `mrsm.Pipeline(name, instance=instance)`.
        """
        return cls(name, instance=instance)

    @property
    def definition(self) -> Optional[Dict[str, Any]]:
        """
        The saved definition this Pipeline was loaded from or saved as (`name`, `registry`,
        `id`, `revision`, `targets`, `parents`, `children`, `description`, `defaults`, ...),
        or `None` for an unsaved selection (or one combined from several saved Pipelines).
        """
        return self._definitions[0] if len(self._definitions) == 1 else None

    @definition.setter
    def definition(self, value: Optional[Dict[str, Any]]) -> None:
        self._bind([value] if value else [])

    def _bind(self, definitions: List[Dict[str, Any]]) -> None:
        """Bind this object to saved definitions (after a save, a rename, a refresh, ...)."""
        self._definitions = list(definitions)

    def _owner_of(self, pipe: Any) -> Optional[str]:
        """
        The name of the saved Pipeline that `pipe` was loaded from as a member reference
        (`None` for a pipe given as a `Pipe` object, or one which is not a member).
        A member loaded from a reference must be registered already: registering it would
        create an empty pipe, so `sync()` refuses instead.
        """
        if not isinstance(pipe, mrsm.Pipe):
            return None
        ref = _pipe_ref(pipe)
        if self._loaded.get(ref, None) is not pipe:
            return None
        for definition in self._definitions:
            if ref in (definition.get('targets', None) or []):
                return definition.get('name', None)
        return self.name or ''

    @property
    def graph(self) -> bool:
        """
        Whether runs respect the dependencies between members even without traversal
        (see the `graph` argument).
        """
        if self._graph is not None:
            return self._graph
        if not self._definitions:
            return False
        return _common([d.get('graph', True) for d in self._definitions],
                       'graph', [d['name'] for d in self._definitions])

    @graph.setter
    def graph(self, value: Optional[bool]) -> None:
        self._graph = None if value is None else bool(value)

    @property
    def members(self) -> List[Any]:
        """
        The member pipes (or keys) the selection starts from, before traversal
        (empty for a tags / keys filter). Also available as `targets`.

        Assign a new list (`Pipe` objects, key tuples or `pipe_ref` strings) to change the
        selection, then `save()` to keep the change:

        ```python
        daily = mrsm.Pipeline('daily')
        daily.members = daily.members + [mrsm.Pipe('sql:main', 'weekly')]
        daily.save()
        ```
        """
        return list(self._targets)

    @members.setter
    def members(self, pipes: Any) -> None:
        names, targets = _split_selection(pipes)
        if names:
            raise ValueError(
                "Members are pipes (Pipe objects, key tuples or pipe references), not the names "
                "of saved Pipelines: combine saved Pipelines with `mrsm.Pipeline([...])`."
            )
        members: List[Any] = []
        seen = set()
        for target in targets:
            if isinstance(target, mrsm.Pipe):
                ref = _pipe_ref(target)
                if ref in seen:
                    continue
                seen.add(ref)
            members.append(target)
        self._loaded = {
            ref: member
            for ref, member in self._loaded.items()
            if any(target is member for target in members)
        }
        self._targets = members
        self._filters = {}
        self._sources = list(members)

    @property
    def targets(self) -> List[Any]:
        """The member pipes (or keys) the selection starts from (the same as `members`)."""
        return list(self._targets)

    @targets.setter
    def targets(self, pipes: Any) -> None:
        self.members = pipes

    @property
    def pipes(self) -> List[mrsm.Pipe]:
        """
        Every pipe the selection resolves to (members plus the traversed parents and children),
        in dependency order. This plans the selection, registering nothing: member pipes that
        are not registered yet come last (their dependencies are unknown until they are).
        """
        plan = self.plan()
        planned = plan.pipes
        return planned + [pipe for pipe in plan.unregistered if pipe not in planned]

    def _unregistered(self) -> List[mrsm.Pipe]:
        """Member pipes that are not registered on their instance (temporary pipes excluded)."""
        return [
            target
            for target in self._targets
            if isinstance(target, mrsm.Pipe)
            and not target.temporary
            and target.get_id() is None
        ]

    def _execution_defaults(self, explicit: Dict[str, Any]) -> Dict[str, Any]:
        """The saved execution defaults of the bound definitions (explicit options win)."""
        from meerschaum.core.Pipeline._saved import validate_execution_defaults
        merged: Dict[str, Any] = {}
        owners: Dict[str, str] = {}
        for definition in self._definitions:
            defaults = validate_execution_defaults(definition.get('defaults', None) or {})
            for key, value in defaults.items():
                if key in merged and merged[key] != value and key not in explicit:
                    raise ValueError(
                        f"The saved Pipelines '{owners[key]}' and '{definition['name']}' have "
                        f"different defaults for `{key}`; pass `{key}=` to choose one."
                    )
                if key not in merged:
                    merged[key] = value
                    owners[key] = definition['name']
        return merged

    def _request(self, targets: Optional[List[Any]] = None, **kwargs: Any) -> Dict[str, Any]:
        if self._definitions:
            kwargs = {**self._execution_defaults(kwargs), **kwargs}
            kwargs["pipeline_definitions"] = deepcopy(self._definitions)
        run_name = self.name or ('combined' if len(self._definitions) > 1 else None)
        if run_name:
            kwargs.setdefault("run_name", run_name)
        if self.graph:
            kwargs.setdefault("graph", True)
        parents = kwargs.pop('parents', self.parents)
        children = kwargs.pop('children', self.children)
        for reserved in ('instance', 'targets', 'filters'):
            if reserved in kwargs:
                raise ValueError(f"Set Pipeline {reserved} on the definition, not as an execution option.")
        return build_request(
            self.instance,
            targets=(self._targets if targets is None else targets),
            filters=self._filters,
            parents=parents,
            children=children,
            **kwargs
        )

    def _register_targets(self) -> Optional[SuccessTuple]:
        """The engine reads the graph from the instance, so register the members first."""
        for target in self._targets:
            if not isinstance(target, mrsm.Pipe) or target.temporary:
                continue
            if target.get_id() is None:
                owner = self._owner_of(target)
                if owner is not None:
                    return False, (
                        f"{target} is a member of Pipeline '{owner}' but is not registered "
                        f"on '{target.instance_keys}'. Register it, or remove it from the "
                        "Pipeline."
                    )
                success, message = target.register()
                if not success:
                    return False, message
        return None

    def plan(
        self,
        begin: Any = _UNSET_BOUND,
        end: Any = _UNSET_BOUND,
        expected: bool = False,
        **kwargs: Any
    ) -> Plan:
        """
        Plan the run without moving any data (and without registering anything).

        Parameters
        ----------
        begin, end: Union[datetime, int, str, None], default None
            Optionally bound the sync window. `None` and `''` leave it to the planner
            (a root syncs from its sync time minus the backtrack).

        expected: bool, default False
            If `False`, every node is planned against the state as it is now: a node whose
            parents run first is provisional (`*`) and is planned again by the executor once
            they finish. If `True`, return what `mrsm plan pipes` prints: each provisional node
            re-planned as if every node that runs before it synced new rows after its
            watermark, so its window starts where the executor will start it (often the start
            of a parent's chunk-grid cell). Only the start is predicted; the executor caps the
            end at the parents' new watermarks.

        Returns
        -------
        A `Plan` (`str(plan)` is the table; `plan.nodes` is the data). Member pipes that are
        not registered yet are in `plan.unregistered` and named in `plan.warnings`.
        """
        options = {key: value for key, value in {'begin': begin, 'end': end, **kwargs}.items()
                   if value is not _UNSET_BOUND}
        unregistered = self._unregistered()
        request = self._request(
            targets=(
                [target for target in self._targets if target not in unregistered]
                if unregistered
                else None
            ),
            **options
        )
        if expected:
            request['expected'] = True
        data = call('engine_plan', request)
        return Plan(
            data,
            pipeline=self,
            options={
                key: value
                for key, value in options.items()
                if value not in (None, '')
            },
            unregistered=unregistered,
        )

    def sync(self, begin: Any = _UNSET_BOUND, end: Any = _UNSET_BOUND, **kwargs: Any) -> RunResult:
        """
        Plan and run the selection, registering member pipes that are not registered yet
        (a saved Pipeline's members must already be registered).

        Parameters
        ----------
        begin, end: Union[datetime, int, str, None]
            Bound the sync window of the root nodes (other nodes are clamped to it).
            `None` and `''` leave the window to the planner: a root syncs from its sync time
            minus the backtrack, as a bare `pipe.sync()` does by default. (A bare
            `pipe.sync(begin=None)` means "no lower bound"; in an engine run it does not.)

        workers: Optional[int], default None
            Run up to this many nodes at once (default `system.engine.workers`).

        force, retries, min_seconds, check_existing, enforce_dtypes, chunksize, params, yes:
            The `Pipe.sync()` options, applied to every node. `yes` (or `force`) confirms a
            `rebuild`; without it a node whose definition changed is left `rebuild_pending`.

        resume: Optional[str], default None
            Re-plan only the nodes that failed or never ran in this earlier run ID.

        on_event: Optional[Callable[[Dict[str, Any]], Any]], default None
            Called with every event of the run as it happens (`run_started`, `node_planned`,
            `node_started`, `chunk_started`, `chunk_finished`, `node_finished`, `node_failed`,
            `node_skipped`, `watermark_advanced`, ..., `run_finished`), each a `dict` with
            `event`, `run_id`, `seq`, `ts` (an aware UTC `datetime`), `pipe_ref` and the
            event's own fields. It runs on this thread; the sync work runs on the engine's
            threads without the GIL. If it raises an `Exception`, the run continues, the error
            is warned about once and kept in `RunResult.callback_error`, and no further events
            are delivered. `KeyboardInterrupt` cancels the run.

        Returns
        -------
        A `RunResult`: a `SuccessTuple` with `run_id`, `plan`, `outcomes`, `events()`, the
        totals `inserted`, `updated`, `upserted` and `deleted` over every node, and `duration`
        (the run's wall-clock seconds).
        """
        on_event = kwargs.pop('on_event', None)
        failure = self._register_targets()
        if failure is not None:
            return RunResult(*failure)
        options = {key: value for key, value in {'begin': begin, 'end': end, **kwargs}.items()
                   if value is not _UNSET_BOUND}
        try:
            request = self._request(**options)
        except ValueError as e:
            return RunResult(False, str(e))
        callback = EventCallback(on_event) if on_event is not None else None
        if callback is None:
            data = call('engine_run', request)
        else:
            data = call('engine_run', request, on_event=callback)
            if callback.warning is not None:
                data['warnings'] = list(data.get('warnings', None) or []) + [callback.warning]
        run = RunResult(data['success'], data['message'], data, pipeline=self)
        run._instance_request = self._instance_request()
        if callback is not None:
            run.callback_error = callback.error
        return run

    def run(self, begin: Any = _UNSET_BOUND, end: Any = _UNSET_BOUND, **kwargs: Any) -> RunResult:
        """An alias of `Pipeline.sync()`."""
        return self.sync(begin=begin, end=end, **kwargs)

    def _instance_request(self) -> Dict[str, Any]:
        return build_request(self.instance)

    def _members_to_save(self) -> List[Any]:
        """The members a saved definition stores: the members, or the pipes the filters match."""
        if self._targets:
            return list(self._targets)
        if not self._filters:
            return []
        return [
            pipe
            for pipe in mrsm.get_pipes(
                connector_keys=self._filters.get('connector_keys', None),
                metric_keys=self._filters.get('metric_keys', None),
                location_keys=self._filters.get('location_keys', None),
                tags=self._filters.get('tags', None),
                instance=self.instance,
                as_list=True,
            )
            if str(pipe.connector_keys) != 'mrsm'
        ]

    def _settings_to_carry(self, with_defaults: bool = True) -> Dict[str, Any]:
        """The description, defaults and layout a new saved definition starts from."""
        definition = self.definition
        if definition is not None:
            return {
                'description': definition.get('description', None),
                'defaults': definition.get('defaults', None) if with_defaults else None,
                'layout': definition.get('layout', None),
            }
        if self._definitions and with_defaults:
            try:
                return {'defaults': self._execution_defaults({})}
            except ValueError as e:
                raise ValueError(f"{e} (pass `defaults={{...}}` to `save()`)") from None
        return {}

    def save(
        self,
        name: Optional[str] = None,
        overwrite: bool = False,
        description: Optional[str] = None,
        defaults: Optional[Dict[str, Any]] = None,
    ) -> SuccessTuple:
        """
        Save this selection's members, traversal and execution mode under `name`
        (default: `self.name`). Saving never changes the members' tags or registrations.

        Parameters
        ----------
        name: Optional[str], default None
            The name to save under (letters, numbers, dots, underscores and hyphens).

        overwrite: bool, default False
            A different Pipeline already saved under `name` is only replaced with
            `overwrite=True` (its description, defaults and layout are kept unless passed).
            A Pipeline loaded from `name` saves its changes without it, unless someone else
            saved a newer revision since it was loaded (load it again). Saving the same
            members, traversal and execution mode again is a no-op success.

        description: Optional[str], default None
            Set the saved description.

        defaults: Optional[Dict[str, Any]], default None
            Set the saved execution defaults (`begin`, `end`, `workers`, `retries`,
            `min_seconds`, `chunksize`, `check_existing`, `enforce_dtypes`, `force`, `params`,
            `yes`), used by every run of the saved Pipeline unless a run passes its own.

        Returns
        -------
        A `SuccessTuple`. On success, `self.name` and `self.definition` are the saved ones.
        """
        from meerschaum.core.Pipeline._saved import (
            _read, _refs, registration, save_pipeline, validate_name,
        )
        name = name or self.name
        if not name:
            return False, "Name the Pipeline to save it, e.g. `pipeline.save('daily')`."
        try:
            validate_name(name)
        except ValueError as e:
            return False, f"Cannot save Pipeline {name!r}: {e}"

        registry = self.instance
        try:
            members = self._members_to_save()
            graph = True if self._graph is None and not self._definitions else self.graph
            registration_pipe = registration(name, registry)
            registry_keys = registration_pipe.instance_keys
            exists = registration_pipe.get_id() is not None
            source = self.definition
            editing = (
                exists
                and source is not None
                and source.get('name', None) == name
                and str(source.get('registry', None)) == str(registry_keys)
            )
            expected_revision = None
            if exists and not overwrite:
                ### Saving what is already saved is a no-op (no new revision for other editors
                ### to trip over). Otherwise a Pipeline loaded from `name` saves its changes
                ### against the revision it loaded, and replacing a different one needs
                ### `overwrite`.
                current = _read(name, registry)
                same_selection = (
                    sorted(current.get('targets', None) or []) == _refs(members, registry_keys)
                    and get_depth(current.get('parents', None)) == self.parents
                    and get_depth(current.get('children', None)) == self.children
                    and current.get('graph', True) == graph
                )
                unchanged = (
                    (description is None or description == current.get('description', ''))
                    and (defaults is None or defaults == current.get('defaults', {}))
                )
                if same_selection and unchanged:
                    self._bind([current])
                    self.name = name
                    return True, (
                        f"Pipeline '{name}' is already saved on '{registry_keys}' "
                        "with these members."
                    )
                if not editing and not same_selection:
                    return False, (
                        f"A different Pipeline '{name}' is already saved on '{registry_keys}'. "
                        f"Load it with `mrsm.Pipeline({name!r})` to change it, or pass "
                        "`overwrite=True` to replace it."
                    )
                expected_revision = source['revision'] if editing else current['revision']

            if exists:
                definition = save_pipeline(
                    name, members, registry, self.parents, self.children,
                    expected_revision=expected_revision,
                    description=description,
                    defaults=defaults,
                    graph=graph,
                )
            else:
                carried = self._settings_to_carry(with_defaults=(defaults is None))
                definition = save_pipeline(
                    name, members, registry, self.parents, self.children,
                    description=(description if description is not None else carried.get('description', None)),
                    defaults=(defaults if defaults is not None else carried.get('defaults', None)),
                    layout=carried.get('layout', None),
                    create=True,
                    graph=graph,
                )
        except Exception as e:
            return False, f"Failed to save Pipeline '{name}' on '{registry}': {e}"

        self._bind([definition])
        self.name = name
        num_members = len(definition.get('targets', None) or [])
        try:
            num_unregistered = len(self._unregistered())
        except Exception:
            num_unregistered = 0
        note = (
            f" {num_unregistered} of them {'is' if num_unregistered == 1 else 'are'} not "
            "registered yet: `sync()` registers them."
            if num_unregistered
            else ''
        )
        return True, (
            f"Saved Pipeline '{name}' on '{definition['registry']}' "
            f"({num_members} member{'s' if num_members != 1 else ''}, "
            f"revision {definition['revision']}).{note}"
        )

    def exists(self) -> bool:
        """Whether a Pipeline is saved under this Pipeline's `name` on its instance."""
        if not self.name:
            return False
        from meerschaum.core.Pipeline._saved import registration
        try:
            return registration(self.name, self.instance).get_id() is not None
        except ValueError:
            return False

    def delete(self) -> SuccessTuple:
        """
        Delete the saved Pipeline (its definition and the membership tags it owns).
        The member pipes and their data are kept, and this object stays usable as an unsaved
        selection. Scheduled jobs that sync the Pipeline must be removed first.
        """
        from meerschaum.core.Pipeline._saved import delete_pipeline
        if not self.name:
            return False, "This Pipeline has no name, so it is not saved."
        try:
            success, message = delete_pipeline(self.name, self.instance)
        except Exception as e:
            return False, f"Failed to delete Pipeline '{self.name}': {e}"
        if not success:
            return success, message
        self._bind([])
        return True, (
            f"Deleted Pipeline '{self.name}' from '{self.instance}' "
            "(its member pipes and their data are kept)."
        )

    def rename(self, new_name: str) -> SuccessTuple:
        """
        Rename the saved Pipeline (its identity and run history are kept).
        Scheduled jobs that sync the Pipeline must be removed first.
        """
        from meerschaum.core.Pipeline._saved import rename_pipeline
        if not self.exists():
            return False, f"Pipeline '{self.name}' is not saved on '{self.instance}'."
        definition = self.definition
        expected_revision = (
            definition['revision']
            if definition is not None and definition.get('name', None) == self.name
            else None
        )
        old_name = self.name
        try:
            renamed = rename_pipeline(
                old_name, new_name, self.instance, expected_revision=expected_revision,
            )
        except Exception as e:
            return False, f"Failed to rename Pipeline '{old_name}': {e}"
        self._bind([renamed])
        self.name = new_name
        if old_name == new_name:
            return True, f"Pipeline '{old_name}' already has that name."
        return True, f"Renamed Pipeline '{old_name}' to '{new_name}' on '{self.instance}'."

    def copy(self, new_name: str, instance: Union[str, Any, None] = None) -> SuccessTuple:
        """
        Save a copy of this Pipeline as `new_name`, on `instance` (default: this Pipeline's
        instance). The copy has the same members (which keep their own instances), traversal,
        description and defaults, and a new identity; this object is unchanged.
        """
        from meerschaum.core.Pipeline._saved import save_pipeline, validate_name
        try:
            validate_name(new_name)
        except ValueError as e:
            return False, f"Cannot copy to {new_name!r}: {e}"
        registry = _registry_of(instance) if instance is not None else self.instance
        try:
            carried = self._settings_to_carry()
            definition = save_pipeline(
                new_name, self._members_to_save(), registry, self.parents, self.children,
                description=carried.get('description', None),
                defaults=carried.get('defaults', None),
                layout=carried.get('layout', None),
                create=True,
            )
        except Exception as e:
            return False, f"Failed to copy to Pipeline '{new_name}' on '{registry}': {e}"
        source = f"Pipeline '{self.name}'" if self.name else 'this selection'
        return True, f"Copied {source} to Pipeline '{new_name}' on '{definition['registry']}'."

    def analyze(self) -> Dict[str, Any]:
        """
        Resolve the saved Pipeline's members and traversal and preview the membership tags
        (`pipeline-NAME`) it would add or remove, without changing anything (like
        `mrsm analyze pipelines NAME`). Apply the proposal with `apply_analysis()`.

        Returns
        -------
        The proposal: `resolved` (each pipe and why it is included), `changes` (tag edits),
        `shared` (pipes other Pipelines also use), `errors` and a `fingerprint`.
        """
        from meerschaum.core.Pipeline._saved import analyze_pipeline
        if not self.name:
            raise ValueError("Save the Pipeline before analyzing it, e.g. `pipeline.save('daily')`.")
        return analyze_pipeline(self.name, self.instance)

    def apply_analysis(self, proposal: Optional[Dict[str, Any]] = None) -> SuccessTuple:
        """
        Apply the membership tag changes of a proposal from `analyze()` (without one, analyze
        now and apply that, like `mrsm analyze pipelines NAME --yes`). A proposal whose inputs
        changed since it was made is refused: analyze again and review the new one.
        """
        from meerschaum.core.Pipeline._saved import _read, apply_pipeline_analysis
        if not self.name:
            return False, "This Pipeline has no name, so it is not saved."
        try:
            proposal = proposal if proposal is not None else self.analyze()
            result = apply_pipeline_analysis(proposal, self.instance)
        except Exception as e:
            return False, f"Failed to apply the analysis of Pipeline '{self.name}': {e}"
        message = result.get('message', '')
        failures = [row for row in result.get('results', None) or [] if not row.get('success', True)]
        if failures:
            message += ''.join(f"\n{row['pipe_ref']}: {row['message']}" for row in failures)
        try:
            self._bind([_read(self.name, self.instance)])
        except Exception:
            pass
        return bool(result.get('success', False)), message

    def schedule(
        self,
        schedule: str,
        name: Optional[str] = None,
        workers: Optional[int] = None,
        retries: int = 0,
        force: bool = False,
        executor_keys: Optional[str] = None,
        start: bool = True,
    ) -> 'mrsm.Job':
        """
        Create (and start) a background job that syncs the saved Pipeline on a schedule,
        like Manage → Schedule on the console's Pipelines page.

        Parameters
        ----------
        schedule: str
            When to run, e.g. `'every 10 minutes'` or `'daily starting 00:30'`.

        name: Optional[str], default None
            The job's name (default `pipeline-NAME`).

        workers: Optional[int], default None
            Run up to this many pipes at once (`--workers`).

        retries: int, default 0
            Retry a failed pipe this many times (`--retries`).

        force: bool, default False
            Confirm rebuilds without asking (`--force`).

        executor_keys: Optional[str], default None
            Where the job runs (`'local'`, `'systemd'` or an `api:` instance; default: as `Job`).

        start: bool, default True
            Start the job now (a job that is already running is left running).

        Returns
        -------
        The `mrsm.Job`. Raises `ValueError` for a Pipeline that is not saved, an invalid
        schedule, or a job of that name that runs a different command, and `RuntimeError` if
        the job does not start.
        """
        from meerschaum.utils.schedule import parse_schedule
        from meerschaum.core.Pipeline._saved import job_arguments
        if not self.exists():
            raise ValueError(
                "Save the Pipeline before scheduling it, e.g. `pipeline.save('daily')`."
            )
        sysargs = job_arguments(
            self.name, self.instance, schedule, workers=workers, retries=retries, force=force,
        )
        parse_schedule(str(schedule))
        job_name = name or f'pipeline-{self.name}'
        job = mrsm.Job(job_name, executor_keys=executor_keys)
        if job.exists():
            if list(job.sysargs) != sysargs:
                raise ValueError(
                    f"A job named '{job_name}' already exists with a different command "
                    f"({' '.join(job.sysargs)}). Pass `name=` for a new job, or delete that "
                    "one first."
                )
        else:
            job = mrsm.Job(job_name, sysargs, executor_keys=executor_keys)
        if start and not job.is_running():
            success, message = job.start()
            if not success:
                raise RuntimeError(f"Job '{job_name}' did not start: {message}")
        return job

    def get_runs(
        self,
        limit: int = 20,
        status: Optional[str] = None,
        all_runs: bool = False,
        since: Union[datetime, timedelta, str, None] = None,
    ) -> List[Dict[str, Any]]:
        """
        Recent runs on the instance, newest first.

        Parameters
        ----------
        limit: int, default 20
            The most runs to return.

        status: Optional[str], default None
            Only `'running'`, `'succeeded'` or `'failed'` runs.

        all_runs: bool, default False
            By default a named Pipeline returns its own runs (those of the saved Pipeline, or
            whose run IDs carry its name), and an unnamed one the runs which targeted or
            contained one of its member pipes. If `True` (or the selection is a tags / keys
            filter), return every run on the instance.

        since: Union[datetime, timedelta, str, None], default None
            Only runs started at or after this time: a `datetime` (naive means UTC), a
            `timedelta` (that long ago, e.g. `timedelta(hours=1)`) or an ISO 8601 string.

        Returns
        -------
        Run summaries: `run` (the stored row), `status`, `duration_seconds` and `counts`
        (nodes by status).
        """
        request = self._instance_request()
        request['limit'] = limit
        if status is not None:
            request['status'] = status
        since = since_text(since)
        if since is not None:
            request['since'] = since
        if all_runs:
            return call('engine_runs', request)
        if self.name:
            ### The runs of the saved Pipeline (and of combined selections that included it), or
            ### those whose run IDs carry the name.
            request['run_name'] = self.name
            return call('engine_runs', request)
        pipe_targets = [t for t in self._targets if isinstance(t, mrsm.Pipe)]
        if len(pipe_targets) != 1 or len(self._targets) != 1:
            runs = call('engine_runs', request)
            if pipe_targets:
                refs = {_pipe_ref(pipe) for pipe in pipe_targets}
                runs = [
                    run for run in runs
                    if refs.intersection(run['run'].get('targets', []) or [])
                ]
            return runs
        request['pipe'] = _pipe_ref(pipe_targets[0])
        return call('engine_runs', request)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        One run with its node rows and the planned-vs-actual Gantt bars
        (`None` if the run is unknown).
        """
        request = self._instance_request()
        request['run_id'] = run_id
        return call('engine_run_detail', request)

    def get_events(self, run_id: str, after_seq: Optional[int] = None) -> List[Dict[str, Any]]:
        """The persisted events of a run, in order (see `RunResult.events()`)."""
        request = self._instance_request()
        request['run_id'] = run_id
        if after_seq is not None:
            request['after_seq'] = after_seq
        return _decode_events(call('engine_run_events', request))

    def __repr__(self) -> str:
        if self.name:
            import sys
            from meerschaum.config import get_config
            if hasattr(sys, 'ps1'):
                from meerschaum.utils.formatting._pipes import pipeline_repr
                return pipeline_repr(self.name, str(self.instance))
            suffix = f', instance={str(self.instance)!r}' if str(self.instance) != get_config('meerschaum', 'instance') else ''
            return f'Pipeline({self.name!r}{suffix})'
        if self._definitions:
            parts = [repr(self._sources)]
        else:
            parts = [repr(self._targets)] if self._targets else [repr(self._filters)]
        if self.parents:
            parts.append(f"parents={self.parents!r}")
        if self.children:
            parts.append(f"children={self.children!r}")
        return f"Pipeline({', '.join(parts)})"


def get_pipelines(instance: Union[str, Any, None] = None) -> List[Pipeline]:
    """
    Return the Pipelines saved on `instance` (default: the configured instance), sorted by name.

    Parameters
    ----------
    instance: Union[str, InstanceConnector, None], default None
        The instance whose saved Pipelines to list.

    Returns
    -------
    A list of `Pipeline` objects, each with its `name` and `definition`.

    Examples
    --------
    >>> for pipeline in mrsm.get_pipelines(instance='sql:main'):
    ...     print(pipeline.name, len(pipeline.members))
    """
    from meerschaum.core.Pipeline._saved import list_pipelines
    registry = _registry_of(instance)
    return [
        Pipeline._from_definitions([definition], registry)
        for _, definition in sorted(list_pipelines(registry).items())
    ]


def get_runs(
    instance: Union[str, Any, None] = None,
    limit: int = 20,
    status: Optional[str] = None,
    pipeline: Union[str, Pipeline, None] = None,
    pipe: Union[mrsm.Pipe, str, None] = None,
    since: Union[datetime, timedelta, str, None] = None,
) -> List[Dict[str, Any]]:
    """
    Recent engine runs on `instance` (default: the configured instance), newest first.

    Parameters
    ----------
    instance: Union[str, InstanceConnector, None], default None
        The instance that keeps the run history.

    limit: int, default 20
        The most runs to return.

    status: Optional[str], default None
        Only `'running'`, `'succeeded'` or `'failed'` runs.

    pipeline: Union[str, Pipeline, None], default None
        Only the runs of this saved Pipeline (a name or a `Pipeline`).

    pipe: Union[mrsm.Pipe, str, None], default None
        Only the runs which targeted or contained this pipe (a `Pipe` or a `pipe_ref`).

    since: Union[datetime, timedelta, str, None], default None
        Only runs started at or after this time: a `datetime` (naive means UTC), a
        `timedelta` (that long ago, e.g. `timedelta(hours=1)`) or an ISO 8601 string.

    Returns
    -------
    Run summaries: `run` (the stored row), `status`, `duration_seconds` and `counts`
    (nodes by status). See `Pipeline.get_runs()`.

    Examples
    --------
    >>> from datetime import timedelta
    >>> failed = mrsm.get_runs(status='failed', since=timedelta(days=1))
    """
    connector = (
        parse_instance_keys(instance)
        if isinstance(instance, (str, type(None)))
        else instance
    )
    request = build_request(connector)
    request['limit'] = limit
    if status is not None:
        request['status'] = status
    if pipeline is not None:
        request['run_name'] = pipeline.name if isinstance(pipeline, Pipeline) else str(pipeline)
    if pipe is not None:
        request['pipe'] = pipe if isinstance(pipe, str) else _pipe_ref(pipe)
    since = since_text(since)
    if since is not None:
        request['since'] = since
    return call('engine_runs', request)


def sync_with_graph(
    pipe: mrsm.Pipe,
    df: Any,
    parents: Union[bool, int, None] = None,
    children: Union[bool, int, None] = None,
    **kwargs: Any
) -> RunResult:
    """
    `Pipe.sync(parents=..., children=...)`: run `pipe` and its ancestors and / or descendants
    through the Syncing Engine. A `df` is not supported (the engine fetches each node itself):
    sync the data with a bare `pipe.sync(df)` first.
    """
    from meerschaum.core.Pipe._sync import InferFetch
    if df is not InferFetch and df is not None:
        raise ValueError(
            "`pipe.sync(df, parents=..., children=...)` is not supported: the Syncing Engine "
            "fetches each node itself. Sync the DataFrame with a bare `pipe.sync(df)`."
        )
    kwargs.pop('debug', None)
    return Pipeline(pipe, parents=parents, children=children).sync(**kwargs)
