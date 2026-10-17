#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A `Pipeline` is a **selection** over the implicit pipe graph (design §5.2, D4, D5): a set of
target pipes plus how many generations of parents and children to pull in. It is not stored
anywhere. The Rust Syncing Engine plans it (`Pipeline.plan()`), runs it (`Pipeline.sync()`)
and records every run (`Pipeline.get_runs()`).

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('sql:main', 'daily_rollup', instance='sql:main')

pipe.sync()                       # v4 behavior, unchanged
pipe.sync(parents=True)           # every ancestor in dependency order, then this pipe
pipe.sync(children=1)             # this pipe, then its direct children
pipeline = mrsm.Pipeline(pipe, parents=True)
print(pipeline.plan())            # node | action | bounds | chunks | reason
run = pipeline.sync(workers=4)    # a SuccessTuple with `run.run_id`, `run.outcomes`, ...
run = pipeline.sync(on_event=print)   # every event live, as a dict (see `_events.py`)
```

The engine is Rust-only (D14): `parents=`, `children=` and `Pipeline` work in every `MRSM_CORE`
mode, but need the extension built (`scripts/rust/build_py.sh`). A bare `pipe.sync()` never uses
the engine.

The engine reads the graph from the instance, so every pipe in the selection (and the pipes it
reads) must be registered; `Pipeline` registers its target pipes if they are not.
"""

from __future__ import annotations

from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Any, Callable, Dict, Iterable, List, Optional, SuccessTuple, Tuple, Union,
)
from meerschaum.core.Pipeline._engine import (
    build_request, call, get_core, get_depth,
)
from meerschaum.core.Pipeline._events import EVENT_KEYS, EVENT_NAMES, EventCallback, decode_event

__all__ = (
    'Pipeline', 'Plan', 'PlannedNode', 'RunResult', 'get_runs', 'sync_with_graph',
    'EVENT_KEYS', 'EVENT_NAMES', 'decode_event',
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
        instance_keys, connector_keys, metric_key, location_key = self.pipe_ref.split('|', 3)
        label = f"{connector_keys}/{metric_key}"
        if location_key != 'None':
            label += f"/{location_key}"
        return {
            'node': label,
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
        `True` for a selection with no traversal (v4 semantics: every node is a root).

    warnings: List[str]
        Graph and selection warnings.
    """

    def __init__(
        self,
        data: Dict[str, Any],
        pipeline: Optional['Pipeline'] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        self.raw = data
        plan = data['plan']
        self.text: str = data.get('text', '')
        self.nodes: List[PlannedNode] = [PlannedNode(node) for node in plan['nodes']]
        self.edges: List[Tuple[str, str]] = [tuple(edge) for edge in plan.get('edges', [])]
        self.flat: bool = bool(plan.get('flat', False))
        self.warnings: List[str] = list(data.get('warnings', None) or [])
        self._pipeline = pipeline
        self._options = options or {}

    @property
    def pipes(self) -> List[mrsm.Pipe]:
        """The pipes of the plan, in dependency order."""
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

    def to_dataframe(self) -> 'pd.DataFrame':
        """The plan table as a DataFrame."""
        pd = mrsm.attempt_import('pandas')
        return pd.DataFrame(
            self.to_dicts(),
            columns=['node', 'action', 'bounds', 'chunks', 'reason'],
        )

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
        return self.text

    def __repr__(self) -> str:
        return f"Plan({len(self.nodes)} nodes)"


def _pipe_ref(pipe: mrsm.Pipe) -> str:
    return '|'.join((
        str(pipe.instance_keys),
        str(pipe.connector_keys),
        str(pipe.metric_key),
        str(pipe.location_key),
    ))


class RunResult(tuple):
    """
    The result of an engine run: a `SuccessTuple` (`(success, message)`) with the run's details.

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

    outcomes: List[Dict[str, Any]]
        One entry per node in completion order: `pipe_ref`, `pipe` (the v4 `str(pipe)`) and
        `result` (`success`, `message`, `inserted`, `updated`, `upserted`, `deleted`,
        `changed_intervals`, `duration`, `begin`, `end`).

    critical_path: List[str]
        The `pipe_ref`s of the plan's critical path.

    warnings: List[str]
        Graph, selection and state warnings.

    duration: float
        Wall-clock seconds.

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
        self.outcomes = list(data.get('outcomes', None) or [])
        self.critical_path = list(data.get('critical_path', None) or [])
        self.warnings = list(data.get('warnings', None) or [])
        self.duration = float(data.get('duration_seconds', 0.0) or 0.0)
        self.record = data.get('record', None)
        self.plan = Plan(data, **kwargs) if data.get('plan', None) else None
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

    def events(self, after_seq: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        The run's persisted events (`run_started`, `node_started`, `node_finished`,
        `run_finished`, ...) in order, read from the instance after the run
        (`system.engine.events` sets how fine-grained they are). Each row is
        `{'ts', 'run_id', 'seq', 'pipe_ref', 'event', 'payload'}`.

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


def _rebuild_run_result(success: bool, message: str, data: Dict[str, Any]) -> RunResult:
    return RunResult(success, message, data)


def _decode_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import json
    events = []
    for row in rows:
        payload = row.get('payload', None)
        try:
            decoded = json.loads(payload) if isinstance(payload, str) and payload else payload
        except ValueError:
            decoded = payload
        events.append({**row, 'payload': decoded})
    return events


def _instance_of(pipes: List[mrsm.Pipe], instance: Optional[Any]) -> Any:
    if instance is not None:
        return mrsm.connectors.parse_instance_keys(instance) if isinstance(instance, str) else instance
    if pipes:
        instances = {pipe.instance_keys for pipe in pipes}
        if len(instances) > 1:
            raise ValueError(
                "A Pipeline's target pipes must share one instance (the engine's home instance); "
                f"got {sorted(instances)}. Pass `instance=` to choose one."
            )
        return pipes[0].instance_connector
    return mrsm.connectors.parse_instance_keys(None)


class Pipeline:
    """
    A selection over the pipe graph: target pipes plus traversal depths (design §5.2, D4).

    Parameters
    ----------
    pipes: Union[mrsm.Pipe, Iterable[...], None], default None
        The target pipes: a `Pipe`, or an iterable of `Pipe` objects and
        `(connector_keys, metric_key[, location_key])` tuples.

    parents: Union[bool, int, None], default None
        `True` includes every ancestor of the targets; an `int` includes that many generations.

    children: Union[bool, int, None], default None
        `True` includes every descendant of the targets; an `int` that many generations.

    tags, connector_keys, metric_keys, location_keys: Union[str, List[str], None], default None
        Select the targets by the same filters as `-t`, `-c`, `-m` and `-l`
        (negate a value with a leading `_`). Used when `pipes` is not provided.

    instance: Union[str, InstanceConnector, None], default None
        The instance holding the pipes and the engine state (default: the target pipes'
        instance, else the configured one).

    Examples
    --------
    >>> pipeline = mrsm.Pipeline(pipe, parents=True)
    >>> plan = pipeline.plan()
    >>> run = pipeline.sync(workers=4)
    >>> run.success, run.run_id
    """

    def __init__(
        self,
        pipes: Union[mrsm.Pipe, Iterable[Any], None] = None,
        parents: Union[bool, int, None] = None,
        children: Union[bool, int, None] = None,
        tags: Union[str, List[str], None] = None,
        connector_keys: Union[str, List[str], None] = None,
        metric_keys: Union[str, List[str], None] = None,
        location_keys: Union[str, List[str], None] = None,
        instance: Union[str, Any, None] = None,
    ):
        if isinstance(pipes, mrsm.Pipe):
            pipes = [pipes]
        elif pipes is not None and isinstance(pipes, tuple) and pipes and isinstance(pipes[0], str):
            pipes = [pipes]
        self._targets: List[Any] = list(pipes or [])
        self.parents = get_depth(parents)
        self.children = get_depth(children)
        self._filters = {
            'tags': tags,
            'connector_keys': connector_keys,
            'metric_keys': metric_keys,
            'location_keys': location_keys,
        }
        self._filters = {k: ([v] if isinstance(v, str) else v) for k, v in self._filters.items() if v}
        if not self._targets and not self._filters:
            raise ValueError("A Pipeline needs target pipes or a tags / keys filter.")
        pipe_targets = [target for target in self._targets if isinstance(target, mrsm.Pipe)]
        self.instance = _instance_of(pipe_targets, instance)

    @property
    def targets(self) -> List[Any]:
        """The pipes (or keys) the selection starts from."""
        return list(self._targets)

    @property
    def pipes(self) -> List[mrsm.Pipe]:
        """
        Every pipe the selection resolves to (targets plus the traversed parents and children),
        in dependency order.
        """
        return self.plan().pipes

    def _request(self, **kwargs: Any) -> Dict[str, Any]:
        return build_request(
            self.instance,
            targets=self._targets,
            filters=self._filters,
            parents=self.parents,
            children=self.children,
            **kwargs
        )

    def _register_targets(self) -> Optional[SuccessTuple]:
        """The engine reads the graph from the instance, so register the targets first."""
        for target in self._targets:
            if not isinstance(target, mrsm.Pipe) or target.temporary:
                continue
            if target.get_id() is None:
                success, message = target.register()
                if not success:
                    return False, message
        return None

    def plan(self, begin: Any = None, end: Any = None, **kwargs: Any) -> Plan:
        """
        Plan the run without moving any data.

        Parameters
        ----------
        begin, end: Union[datetime, int, str, None], default None
            Optionally bound the sync window. `None` and `''` leave it to the planner.

        Returns
        -------
        A `Plan` (`str(plan)` is the table; `plan.nodes` is the data).
        """
        failure = self._register_targets()
        if failure is not None:
            raise ValueError(failure[1])
        options = {'begin': begin, 'end': end, **kwargs}
        request = self._request(**options)
        data = call('engine_plan', request)
        return Plan(data, pipeline=self, options={
            key: value
            for key, value in options.items()
            if value not in (None, '')
        })

    def sync(self, begin: Any = '', end: Any = None, **kwargs: Any) -> RunResult:
        """
        Plan and run the selection.

        Parameters
        ----------
        begin, end: Union[datetime, int, str, None]
            Bound the sync window of the root nodes (other nodes are clamped to it).

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
        A `RunResult`: a `SuccessTuple` with `run_id`, `plan`, `outcomes` and `events()`.
        """
        on_event = kwargs.pop('on_event', None)
        failure = self._register_targets()
        if failure is not None:
            return RunResult(*failure)
        request = self._request(begin=begin, end=end, **kwargs)
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

    def run(self, begin: Any = '', end: Any = None, **kwargs: Any) -> RunResult:
        """An alias of `Pipeline.sync()`."""
        return self.sync(begin=begin, end=end, **kwargs)

    def _instance_request(self) -> Dict[str, Any]:
        return build_request(self.instance)

    def get_runs(
        self,
        limit: int = 20,
        status: Optional[str] = None,
        all_runs: bool = False,
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
            By default only runs which targeted or contained one of this pipeline's target pipes
            are returned. If `True` (or the pipeline selects by filters), return every run.

        Returns
        -------
        Run summaries: `run` (the stored row), `status`, `duration_seconds` and `counts`
        (nodes by status).
        """
        request = self._instance_request()
        request['limit'] = limit
        if status is not None:
            request['status'] = status
        pipe_targets = [t for t in self._targets if isinstance(t, mrsm.Pipe)]
        if all_runs or len(pipe_targets) != 1 or len(self._targets) != 1:
            runs = call('engine_runs', request)
            if not all_runs and pipe_targets:
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
        """The persisted events of a run, in order."""
        request = self._instance_request()
        request['run_id'] = run_id
        if after_seq is not None:
            request['after_seq'] = after_seq
        return _decode_events(call('engine_run_events', request))

    def __repr__(self) -> str:
        parts = [repr(self._targets)] if self._targets else [repr(self._filters)]
        if self.parents:
            parts.append(f"parents={self.parents!r}")
        if self.children:
            parts.append(f"children={self.children!r}")
        return f"Pipeline({', '.join(parts)})"


def get_runs(
    instance: Union[str, Any, None] = None,
    limit: int = 20,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Recent engine runs on `instance` (default: the configured instance), newest first.
    See `Pipeline.get_runs()`.
    """
    connector = (
        mrsm.connectors.parse_instance_keys(instance)
        if isinstance(instance, (str, type(None)))
        else instance
    )
    request = build_request(connector)
    request['limit'] = limit
    if status is not None:
        request['status'] = status
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
