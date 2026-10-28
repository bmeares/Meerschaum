#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
What the console's pipelines page and the pipe cards' Parents / Children tabs read from the
Syncing Engine: the dependency graph with each pipe's planned action, the runs, one run's
planned-versus-actual bars and its events.

These are the engine's own bindings (`meerschaum.core.Pipeline`), the same functions behind the
`/engine/*` state routes and `mrsm show runs`, `show run <id>` and `plan pipes`; nothing here
writes. Every function takes the instance (the API instance by default) and returns plain
`dict`s and `list`s.
"""

from __future__ import annotations

from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

#: Planned actions that mean the pipe is behind its parents (or needs a rebuild).
PENDING_ACTIONS = frozenset({'incremental', 'backfill'})
REBUILD_ACTIONS = frozenset({'rebuild', 'rebuild_pending'})

#: The colors of a node, by status (the Rust console's palette).
STATUS_COLORS: Dict[str, str] = {
    'caught_up': '#2e9e5b',
    'pending': '#e0a526',
    'rebuild': '#d1495b',
    'failed': '#d1495b',
    'root': '#3486bd',
    'observe': '#9aa5b1',
    'never_run': '#9aa5b1',
}


def split_ref(pipe_ref: str) -> Tuple[str, str, str, Optional[str]]:
    """
    Split a `pipe_ref` (`instance|connector_keys|metric_key|location_key`).
    """
    instance_keys, connector_keys, metric_key, location_key = pipe_ref.split('|', 3)
    return instance_keys, connector_keys, metric_key, (None if location_key == 'None' else location_key)


#: The prefix of a pipe a viewer may not see, as runs, events and graphs name it
#: (`__hidden__:1`, `__hidden__:2`, ...): its keys never reach the page.
HIDDEN_PREFIX: str = '__hidden__'


def is_hidden_ref(pipe_ref: Any) -> bool:
    """Whether a `pipe_ref` stands for a pipe the viewer may not see."""
    return str(pipe_ref or '').startswith(HIDDEN_PREFIX)


def ref_label(pipe_ref: str) -> str:
    """The official Pipe representation, including a non-default instance."""
    if is_hidden_ref(pipe_ref):
        from meerschaum.api.dash.access import HIDDEN_LABEL
        number = str(pipe_ref)[len(HIDDEN_PREFIX):].lstrip(':')
        return HIDDEN_LABEL + (f" {number}" if number else '')
    if pipe_ref.count('|') < 3:
        ### A canvas placeholder (`__upstream__`), not a Pipe.
        return pipe_ref
    instance, connector, metric, location = split_ref(pipe_ref)
    return str(mrsm.Pipe(connector, metric, location, instance=instance))


class _HistoryRun(dict):
    """A request-local result whose provenance is never part of its JSON representation."""


def _public_history_value(value: Any) -> Any:
    """Remove storage-only snapshot proofs after access decisions, retaining original refs."""
    if isinstance(value, dict):
        out = _HistoryRun() if isinstance(value, _HistoryRun) else {}
        for key, item in value.items():
            if key == 'selection' and isinstance(item, str) and 'registration_registry' in item:
                import json
                try:
                    item = json.dumps(_public_history_value(json.loads(item)))
                except ValueError:
                    pass
            elif key == 'pipeline_definitions' and isinstance(item, list):
                item = [
                    {name: field for name, field in snapshot.items() if name != 'registration_registry'}
                    if isinstance(snapshot, dict) else snapshot
                    for snapshot in item
                ]
            out[key] = _public_history_value(item)
        if isinstance(out, _HistoryRun):
            out._snapshot_registries = getattr(value, '_snapshot_registries', frozenset())
        return out
    if isinstance(value, list):
        return [_public_history_value(item) for item in value]
    return value


def _public_history_run(run: Dict[str, Any]) -> Dict[str, Any]:
    """Keep already-validated registry provenance only for immediate host-side attribution."""
    import json
    selection = (run.get('run') or {}).get('selection')
    if isinstance(selection, str):
        try:
            selection = json.loads(selection)
        except ValueError:
            selection = None
    registries = set()
    if isinstance(selection, dict):
        snapshots = selection.get('pipeline_definitions')
        for snapshot in snapshots if isinstance(snapshots, list) else []:
            if not isinstance(snapshot, dict):
                continue
            values = tuple(snapshot.get(key) for key in ('name', 'id', 'registration_registry'))
            if all(isinstance(value, str) and value for value in values):
                registries.add(values)
    public = _public_history_value(run)
    if not registries:
        return dict(public)
    out = _HistoryRun(public)
    out._snapshot_registries = frozenset(registries)
    return out


def _run_generations(instance: Any, run_ids: List[str]) -> Dict[str, bool]:
    """Fresh private native evidence, requested in bulk and never returned to the browser."""
    ids = sorted({str(run_id) for run_id in run_ids if run_id})
    if not ids:
        return {}
    request = _request(instance)
    request['run_ids'] = ids
    try:
        current = _call('engine_run_generations', request)
    except Exception:
        return {}
    return {run_id: current.get(run_id) is True for run_id in ids} if isinstance(current, dict) else {}


def _api_pipeline_registry(instance: Any) -> Dict[str, Dict[str, Any]]:
    """One fresh canonical metadata snapshot, before the API client's public rebinding."""
    import json
    conn = mrsm.get_connector(instance) if isinstance(instance, str) else instance
    query = {'as_dict': True}
    if conn.instance_keys:
        query['instance_keys'] = conn.instance_keys
    response = conn.get('/pipelines/keys', params=query)
    body = response.json()
    if not response.ok or not isinstance(body, dict) or body.get('success') is not True:
        return {}
    records = body.get('keys')
    records = records.values() if isinstance(records, dict) else records
    if not isinstance(records, (list, tuple)):
        records = list(records) if records is not None else []
    out = {}
    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) < 4 or list(record[:2]) != ['mrsm', 'pipeline']:
            continue
        parameters = record[3]
        if isinstance(parameters, str):
            try:
                parameters = json.loads(parameters)
            except ValueError:
                continue
        if (record[2] and isinstance(parameters, dict)
            and isinstance(parameters.get('pipeline'), dict)):
            out[str(record[2])] = parameters
    return out


class Anonymizer:
    """
    What a viewer sees of the engine's records on an instance: the `pipe_ref`s of pipes they may
    see stay; every other one becomes `__hidden__:<n>` (numbered per record), and a
    `Pipe('...')` naming it in a text (a plan's reason, a run's message, an error) becomes
    "a hidden pipe". Administrators (and `--no-auth`) see everything (`active` is `False`).
    """

    def __init__(self, viewer: Any = None, instance: Any = None, refs=None):
        from meerschaum.api.dash import access
        self.viewer = viewer if viewer is not None else access.current_viewer()
        self.instance = instance if instance is not None else get_instance()
        self.instance_keys = str(self.instance)
        self.pipeline_definitions = None
        self.pipeline_registries: Dict[str, str] = {}
        self.canonical_registries = set()
        self.run_generations: Dict[str, bool] = {}
        self.reference_visibility = None
        if refs is not None and not (self.viewer.no_auth or self.viewer.is_admin):
            if self.instance_keys.startswith('api:'):
                self.load_pipeline_definitions()
            requested = {self.visibility_ref(str(ref)) for ref in refs}
            allowed = access.visible_ref_subset(self.viewer, requested)
            self.reference_visibility = {ref: ref in allowed for ref in requested}
        try:
            self.visible = (
                {ref for ref, allowed in self.reference_visibility.items() if allowed}
                if self.reference_visibility is not None else
                access.visible_refs(self.viewer, self.instance)
            )
        except Exception:
            self.visible = set()
        self.active = self.visible is not None
        self.hidden: Dict[str, str] = {}

    def load_pipeline_definitions(self) -> None:
        """Current UUID, ACL and canonical registry, shared by every run in this request."""
        if self.pipeline_definitions is not None:
            return
        from meerschaum.api.dash import access
        api = self.instance_keys.startswith('api:')
        try:
            registry = (
                _api_pipeline_registry(self.instance) if api
                else dict(access._pipeline_registry(self.instance))
            )
            self.pipeline_definitions = access.visible_pipelines(
                self.instance, viewer=self.viewer, include_members=False,
                registry_parameters=registry,
            )
            identities = {}
            for parameters in registry.values():
                definition = parameters.get('pipeline') or {}
                identity = definition.get('id')
                if isinstance(identity, str) and identity:
                    identities[identity] = identities.get(identity, 0) + 1
                    canonical = definition.get('registry')
                    if api and isinstance(canonical, str) and canonical:
                        self.pipeline_registries[identity] = canonical
                        self.canonical_registries.add(canonical)
            self.pipeline_definitions = {
                name: definition for name, definition in self.pipeline_definitions.items()
                if isinstance((registry.get(name, {}).get('pipeline') or {}).get('id'), str)
                and registry[name]['pipeline']['id']
                and not registry[name]['pipeline']['id'].startswith('legacy:')
                and identities[registry[name]['pipeline']['id']] == 1
                and (not api or definition['id'] in self.pipeline_registries)
            }
        except Exception:
            self.pipeline_definitions = {}
            self.pipeline_registries = {}
            self.canonical_registries = set()

    def visibility_ref(self, ref: str) -> str:
        """Use only the selected server's declared canonical registry for local ACL reads."""
        parts = ref.split('|', 1)
        if (self.instance_keys.startswith('api:') and len(parts) == 2
            and len(self.canonical_registries) == 1 and parts[0] in self.canonical_registries):
            return self.instance_keys + '|' + parts[1]
        return ref

    def check_generations(self, run_ids: List[str]) -> None:
        """One bounded native lookup per request batch, including unavailable proofs."""
        if not self.active:
            return
        missing = [str(run_id) for run_id in run_ids if run_id and str(run_id) not in self.run_generations]
        if missing:
            self.run_generations.update({run_id: False for run_id in missing})
            self.run_generations.update(_run_generations(self.instance, missing))

    def is_visible(self, ref: Any) -> bool:
        ref = self.visibility_ref(str(ref or ''))
        if not self.active or ref.count('|') < 3 or is_hidden_ref(ref):
            return True
        if self.reference_visibility is not None:
            if ref not in self.reference_visibility:
                from meerschaum.api.dash import access
                self.reference_visibility[ref] = ref in access.visible_ref_subset(self.viewer, [ref])
            return self.reference_visibility[ref]
        if ref.split('|', 1)[0] == self.instance_keys:
            return ref in self.visible
        ### A member on another instance: that instance's pipes decide.
        from meerschaum.api.dash import access
        return access.ref_is_visible(self.viewer, ref)

    def ref(self, ref: Any) -> Any:
        """The ref as the viewer may see it."""
        if ref is None or self.is_visible(ref):
            return ref
        ref = str(ref)
        if ref not in self.hidden:
            self.hidden[ref] = f"{HIDDEN_PREFIX}:{len(self.hidden) + 1}"
        return self.hidden[ref]

    def refs(self, refs: Any) -> List[Any]:
        return [self.ref(ref) for ref in (refs or [])]

    def text(self, text: Any) -> Any:
        """`Pipe('...')` mentions of pipes the viewer may not see, as "a hidden pipe"."""
        if not self.active or not isinstance(text, str) or 'Pipe(' not in text:
            return text
        import re
        import ast

        def _replace(match):
            try:
                node = ast.parse('f(' + match.group(1) + ')', mode='eval').body
                args = [ast.literal_eval(arg) for arg in node.args]
                kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords}
            except Exception:
                return match.group(0)
            if len(args) < 2:
                return match.group(0)
            instance_keys = kwargs.get('instance', kwargs.get('mrsm_instance', self.instance_keys))
            location = args[2] if len(args) > 2 else None
            ref = '|'.join((str(instance_keys), str(args[0]), str(args[1]), str(location)))
            return match.group(0) if self.is_visible(ref) else 'a hidden pipe'

        return re.sub(r"Pipe\(((?:'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|[^()'\"])*)\)", _replace, text)

    def value(self, value: Any) -> Any:
        """A nested record with its refs and texts anonymized."""
        if not self.active:
            return value
        if isinstance(value, dict):
            if value.get('pipe_ref') and not self.is_visible(value['pipe_ref']):
                # Keep the position and status of a hidden member, never its payload.
                value = {
                    key: item for key, item in value.items()
                    if key in (
                        'pipe_ref', 'run_id', 'event', 'seq', 'ts', 'action', 'status',
                        'started', 'actual_started', 'finished', 'duration_seconds',
                        'planned_start', 'planned_end', 'actual_start', 'actual_end',
                    )
                }
                value['hidden'] = True
            out = {}
            for key, item in value.items():
                safe_key = self.ref(key) if isinstance(key, str) and key.count('|') >= 3 else key
                if key in ('pipe_ref', 'parent', 'child', 'ref') and isinstance(item, str):
                    out[safe_key] = self.ref(item)
                elif key in ('targets', 'refs', 'resolved_targets', 'nodes') and isinstance(item, list):
                    out[safe_key] = [
                        self.ref(entry) if isinstance(entry, str) and entry.count('|') >= 3 else self.value(entry)
                        for entry in item
                    ]
                else:
                    out[safe_key] = self.value(item)
            return out
        if isinstance(value, list):
            return [self.value(item) for item in value]
        if isinstance(value, str):
            if value.count('|') >= 3 and not self.is_visible(value):
                return self.ref(value)
            return self.text(value)
        return value

    def run(self, run: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """A run summary as the viewer sees it, or `None` when none of its pipes is visible."""
        if not self.active:
            return _public_history_run(run)
        import json
        record = dict(run.get('run') or {})
        refs = list(record.get('targets') or [])
        selection = record.get('selection', None)
        parsed = None
        attributions = set()
        if isinstance(selection, str):
            try:
                parsed = json.loads(selection)
            except ValueError:
                parsed = None
        elif isinstance(selection, dict):
            parsed = selection
        if isinstance(parsed, dict):
            snapshots = parsed.get('pipeline_definitions')
            if snapshots:
                if not isinstance(snapshots, list):
                    return None
                if self.pipeline_definitions is None:
                    self.load_pipeline_definitions()
                # A run ID may itself contain the saved name. Withhold the whole record if
                # any saved identity is hidden, missing or replaced; Pipe metrics are separate.
                for snapshot in snapshots:
                    if not isinstance(snapshot, dict):
                        return None
                    registry = snapshot.get('registration_registry', snapshot.get('registry'))
                    # A remote home is explicit, not an equivalence between API aliases.
                    # The mandatory fresh remote generation RPC below validates its stored
                    # canonical snapshot even when transport credentials are administrative.
                    matches = [
                        definition for definition in self.pipeline_definitions.values()
                        if snapshot.get('id') == definition.get('id')
                        and (
                            registry == self.pipeline_registries.get(definition['id'], definition.get('registry'))
                            or (
                                self.instance_keys.startswith('api:')
                                and 'registration_registry' not in snapshot
                                and snapshot.get('registry') == self.instance_keys
                                and definition.get('registry') == self.instance_keys
                            )
                        )
                    ]
                    if (len(matches) != 1
                        or not snapshot.get('name')
                        or str(snapshot.get('id') or '').startswith('legacy:')):
                        return None
                    if self.instance_keys.startswith('api:'):
                        attributions.add((snapshot['name'], snapshot['id'], self.instance_keys))
            refs += list(((parsed.get('targets') or {}) if isinstance(parsed.get('targets'), dict) else {}).get('refs') or [])
            refs += list(parsed.get('resolved_targets') or [])
        if not refs or not any(self.is_visible(ref) for ref in refs):
            return None
        run_id = record.get('run_id')
        self.check_generations([run_id])
        if self.run_generations.get(run_id) is not True:
            return None
        self.hidden = {}
        out = self.value(dict(run))
        if any(not self.is_visible(ref) for ref in refs):
            out.setdefault('run', {}).pop('message', None)
            out['run'].pop('error', None)
        if isinstance(parsed, dict):
            out.setdefault('run', {})['selection'] = json.dumps(self.value(parsed))
        public = _public_history_run(out)
        if attributions:
            registries = getattr(public, '_snapshot_registries', frozenset())
            public = _HistoryRun(public)
            public._snapshot_registries = frozenset(attributions) | registries
        return public

    def detail(self, detail: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """A run's detail (summary, nodes, bars) as the viewer sees it, or `None`."""
        if not detail:
            return detail
        if not self.active:
            out = _public_history_value(detail)
            if detail.get('summary'):
                out['summary'] = self.run(detail['summary'])
            return out
        summary = self.run(detail.get('summary') or {}) if detail.get('summary') else None
        refs = [bar.get('pipe_ref') for bar in (detail.get('bars') or [])]
        if detail.get('summary') and summary is None:
            return None
        if refs and not any(self.is_visible(ref) for ref in refs) and summary is None:
            return None
        out = self.value({key: value for key, value in detail.items() if key != 'summary'})
        if summary is not None:
            out['summary'] = summary
        return _public_history_value(out)

    def events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """A run's events as the viewer sees them."""
        if not self.active:
            return _public_history_value(events)
        out = []
        for event in events or []:
            event = self.value(event)
            if self.hidden and not event.get('pipe_ref'):
                for key in ('message', 'error', 'selection'):
                    event.pop(key, None)
                    if isinstance(event.get('payload'), dict):
                        event['payload'].pop(key, None)
            out.append(_public_history_value(event))
        return out


def action_label(action: str) -> str:
    """Describe the next sync without exposing engine bookkeeping."""
    return {
        'root': 'Fetch source data', 'incremental': 'New data to sync',
        'backfill': 'Reprocess changed data', 'skip': 'Up to date',
        'rebuild': 'Rebuild required', 'rebuild_pending': 'Rebuild needs approval',
        'observe': 'Listens for new data', 'upstream_failed': 'Blocked: a parent failed',
    }.get(action, action.replace('_', ' ').capitalize())


#: A run's node counts in plain words (`2 synced, 1 up to date`), by node status.
STATUS_WORDS: Dict[str, str] = {
    'succeeded': 'synced',
    'skipped': 'up to date',
    'observed': 'listening for new data',
    'failed': 'failed',
    'upstream_failed': 'blocked by a failed parent',
    'rebuild_pending': 'waiting for rebuild approval',
    'running': 'running',
    'pending': 'not started',
}


def counts_label(counts: Optional[Dict[str, int]]) -> str:
    """A run's node counts in plain words, without the zeros (`2 synced, 1 up to date`)."""
    parts = [
        f"{value} {STATUS_WORDS.get(key, key.replace('_', ' '))}"
        for key, value in (counts or {}).items() if value
    ]
    return ', '.join(parts) or 'Nothing ran'


def bounds_label(bounds: Optional[Dict[str, Any]], backtrack: Optional[str] = None) -> str:
    """
    A sync window in words (`last sync − 1 day → latest data`, `2026-10-12 09:00 → latest
    data`), from a planned node's `bounds` (`begin` is `''` for "from the last sync, minus the
    backtrack window" and `None` for no lower bound; `end` is `None` for no upper bound).
    """
    if not bounds:
        return '—'
    begin, end = bounds.get('begin', None), bounds.get('end', None)
    if begin is None and end is None:
        return 'all data'
    if begin == '':
        begin_text = f"last sync − {backtrack}" if backtrack else 'last sync'
    elif begin is None:
        begin_text = 'the beginning'
    else:
        begin_text = bound_label(begin)
    end_text = 'latest data' if end is None else bound_label(end)
    return f"{begin_text} → {end_text}"


def bound_label(bound: Any) -> str:
    """One bound for people: `2026-10-12 09:00` (seconds only when set), or an integer."""
    if isinstance(bound, dict):
        values = list(bound.values())
        bound = values[0] if len(values) == 1 else bound
    if isinstance(bound, (int, float)) and not isinstance(bound, bool):
        return str(bound)
    parsed = _parse_ts(str(bound))
    if parsed is None:
        return str(bound)
    text = parsed.strftime('%Y-%m-%d %H:%M:%S' if parsed.second or parsed.microsecond else '%Y-%m-%d %H:%M')
    return text


def action_indicator(action: str, failed: bool = False) -> Any:
    """An accessible icon whose sync status label appears on hover."""
    from meerschaum.utils.packages import import_html
    html = import_html()
    label = 'Last run failed' if failed else action_label(action)
    symbol = '!' if failed else {
        'root': '☁', 'skip': '✓', 'incremental': '↻', 'backfill': '↻',
        'rebuild': '⚠', 'rebuild_pending': '⚠', 'observe': '◌',
    }.get(action, '◌')
    return html.Span([
        html.Span(symbol, **{'aria-hidden': 'true'}),
    ], className='pipeline-stage-status', title=label, **{'aria-label': label})


def pipe_href(pipe_ref: str) -> str:
    """
    The console path of a pipe's card.
    """
    from urllib.parse import quote
    instance_keys, connector_keys, metric_key, location_key = split_ref(pipe_ref)
    parts = [connector_keys, metric_key] + ([location_key] if location_key is not None else [])
    return '/dash/pipes/' + '/'.join(quote(part, safe='') for part in parts) + (
        '?instance=' + quote(instance_keys, safe='')
    )


#: The cookie holding the console's global instance (the instance selector), shared by pages.
INSTANCE_COOKIE: str = 'mrsm-instance'


def global_instance_keys() -> Optional[str]:
    """
    The console's global instance, as the instance selector last set it (`INSTANCE_COOKIE`),
    or `None` outside a request or before it was ever chosen.
    """
    keys = None
    try:
        import dash
        keys = (dash.callback_context.cookies or {}).get(INSTANCE_COOKIE)
    except Exception:
        try:
            import flask
            keys = flask.request.cookies.get(INSTANCE_COOKIE) if flask.has_request_context() else None
        except Exception:
            return None
    if not keys:
        return None
    from urllib.parse import unquote
    keys = unquote(keys)
    try:
        from meerschaum.connectors.parse import parse_instance_keys
        parse_instance_keys(keys)
    except Exception:
        return None
    return keys


def resolve_instance(instance_keys: Optional[str] = None) -> Tuple[Any, Optional[str]]:
    """
    The instance connector to use and, when one was asked for but refused, its keys.

    `instance_keys` (a `?instance=` deep link), else the console's global instance, must pass the
    API's instance permissions (`allow_multiple_instances`, `allowed_instance_keys`) and be valid;
    otherwise the API's own instance is used and the refused keys are returned with it.
    """
    from meerschaum.api import get_api_connector
    requested = instance_keys or global_instance_keys()
    if requested:
        try:
            return get_api_connector(str(requested)), None
        except Exception:
            return get_api_connector(), str(requested)
    return get_api_connector(), None


def get_instance(instance_keys: Optional[str] = None, strict: bool = False):
    """
    The instance connector: `instance_keys` (a `?instance=` deep link), else the console's global
    instance, else the API's. A refused instance is never used: the API's instance is returned
    instead (`instance_problem` says so), or with `strict` (the instance of a member or a run,
    where another instance would be the wrong data) a `PermissionError` is raised.
    """
    instance, refused = resolve_instance(instance_keys)
    if refused and strict:
        raise PermissionError(f"Instance '{refused}' is not available to this API.")
    return instance


def instance_problem(instance_keys: Optional[str] = None) -> Optional[str]:
    """Why the requested (or global) instance is not the one shown, or `None`."""
    instance, refused = resolve_instance(instance_keys)
    if not refused:
        return None
    return f"Instance '{refused}' is not available; showing {instance} instead."


def _call(name: str, request: Dict[str, Any]) -> Any:
    from meerschaum.core.Pipeline._engine import call
    return call(name, request)


def _request(instance: Any, **kwargs: Any) -> Dict[str, Any]:
    from meerschaum.core.Pipeline._engine import build_request
    return build_request(instance, **kwargs)


def _instance_pipes(instance: Any, viewer: Any = None) -> List[mrsm.Pipe]:
    from meerschaum.utils.pipes import flatten_pipes_dict
    from meerschaum.api.dash import access
    ### `get_pipes()` leaves out Meerschaum's internal `mrsm` pipes (saved Pipelines' registry).
    pipes_dict = mrsm.get_pipes(instance=instance, as_list=False)
    viewer = viewer if viewer is not None else access.current_viewer()
    return access.visible_pipes(viewer, flatten_pipes_dict(pipes_dict))


def plan_graph(
    instance: Any = None,
    pipes: Optional[List[mrsm.Pipe]] = None,
    parents: Any = None,
    children: Any = None,
    viewer: Any = None,
) -> Dict[str, Any]:
    """
    Plan a selection and return its graph: `{nodes, edges, warnings, hidden}`.

    Each node is `{pipe_ref, label, action, reason, provisional, status}` and each edge
    `{parent, child, label, lagging}`. With no `pipes`, every registered pipe of the instance
    `viewer` may see is selected. `parents` / `children` extend a selection as `Pipeline(...)`
    does. The pipes `viewer` (by default the request's) may not see are left out of the nodes
    and edges and only counted (`hidden`); a reason naming one says "a hidden pipe".
    """
    from meerschaum.core.Pipeline import Plan
    instance = instance if instance is not None else get_instance()
    selected = pipes if pipes is not None else _instance_pipes(instance, viewer=viewer)
    if not selected:
        return {'nodes': [], 'edges': [], 'warnings': [], 'hidden': 0}
    request = _request(
        instance,
        targets=selected,
        parents=True if pipes is None and parents is None else parents,
        children=children,
    )
    # Preserve dependencies inside the selected membership without adding outside pipes.
    request['graph'] = True
    plan = Plan(_call('engine_plan', request))
    nodes = []
    by_ref = {}
    for planned in plan.nodes:
        node = {
            'pipe_ref': planned.pipe_ref,
            'label': ref_label(planned.pipe_ref),
            'action': planned.action,
            'reason': planned.reason,
            'bounds': planned.bounds,
            'bounds_text': (planned.raw.get('bounds_text') or bounds_label(
                planned.bounds, planned.raw.get('backtrack'))) if planned.bounds else '',
            'backtrack': planned.raw.get('backtrack'),
            'provisional': planned.provisional,
            'status': node_status(planned.action),
        }
        nodes.append(node)
        by_ref[planned.pipe_ref] = node
    edges = []
    for parent_ref, child_ref in plan.edges:
        child = by_ref.get(child_ref, {})
        lagging = child.get('action') in PENDING_ACTIONS | REBUILD_ACTIONS
        edges.append({
            'parent': parent_ref,
            'child': child_ref,
            'label': (
                action_label(child.get('action', ''))
            ),
            'lagging': lagging,
        })
    anonymizer = Anonymizer(viewer, instance)
    hidden = 0
    if anonymizer.active:
        hidden = sum(1 for node in nodes if not anonymizer.is_visible(node['pipe_ref']))
        nodes = [node for node in nodes if anonymizer.is_visible(node['pipe_ref'])]
        for node in nodes:
            node['reason'] = anonymizer.text(node.get('reason'))
            node['bounds_text'] = anonymizer.text(node.get('bounds_text'))
        edges = [
            edge for edge in edges
            if anonymizer.is_visible(edge['parent']) and anonymizer.is_visible(edge['child'])
        ]
    return {
        'nodes': nodes,
        'edges': edges,
        'warnings': [anonymizer.text(warning) for warning in plan.warnings],
        'hidden': hidden,
    }


def node_status(action: str) -> str:
    """
    The status class of a planned node (`caught_up`, `pending`, `rebuild`, `root`, `observe`).
    """
    if action in REBUILD_ACTIONS:
        return 'rebuild'
    if action in PENDING_ACTIONS:
        return 'pending'
    if action == 'root':
        return 'root'
    if action == 'observe':
        return 'observe'
    return 'caught_up'


def layered_positions(
    refs: List[str],
    edges: List[Tuple[str, str]],
) -> Dict[str, Tuple[float, float]]:
    """
    Place nodes in columns by dependency depth (a parent is left of its children), spread
    evenly in each column. Cycles (which the engine rejects) are broken arbitrarily.
    """
    parents_of: Dict[str, List[str]] = {ref: [] for ref in refs}
    for parent, child in edges:
        if parent in parents_of and child in parents_of and parent != child:
            parents_of[child].append(parent)

    depth: Dict[str, int] = {}

    def _depth(ref: str, trail: Tuple[str, ...] = ()) -> int:
        if ref in depth:
            return depth[ref]
        if ref in trail:
            return 0
        value = 1 + max((_depth(p, trail + (ref,)) for p in parents_of[ref]), default=-1)
        depth[ref] = value
        return value

    for ref in refs:
        _depth(ref)

    columns: Dict[int, List[str]] = {}
    for ref in refs:
        columns.setdefault(depth[ref], []).append(ref)

    positions: Dict[str, Tuple[float, float]] = {}
    for column, members in columns.items():
        count = len(members)
        for i, ref in enumerate(sorted(members, key=ref_label)):
            positions[ref] = (float(column), (count - 1) / 2.0 - i)
    return positions


def canvas_positions(graph: Dict[str, Any]) -> Dict[str, Tuple[float, float]]:
    """Lay out each connected pipeline separately, with sources on the left."""
    refs = {node['pipe_ref'] for node in graph['nodes']}
    neighbours = {ref: set() for ref in refs}
    edges = [(edge['parent'], edge['child']) for edge in graph['edges']]
    for parent, child in edges:
        if parent in refs and child in refs:
            neighbours[parent].add(child)
            neighbours[child].add(parent)
    positions = {}
    offset = 0.0
    while refs:
        first = min(refs, key=lambda ref: (not neighbours[ref], ref_label(ref)))
        component, pending = set(), {first}
        while pending:
            ref = pending.pop()
            component.add(ref)
            pending.update(neighbours[ref] - component)
        refs.difference_update(component)
        local = layered_positions(sorted(component), edges)
        top = max(y for _, y in local.values())
        bottom = min(y for _, y in local.values())
        positions.update({ref: (x, offset + top - y) for ref, (x, y) in local.items()})
        offset += top - bottom + 1.5
    return positions


def focus_graph(graph: Dict[str, Any], selected: str) -> Dict[str, Any]:
    """Show the selected pipe's ancestors and descendants."""
    visible = {selected}
    for forward in (True, False):
        found = {selected}
        while True:
            added = {
                edge['child' if forward else 'parent'] for edge in graph['edges']
                if edge['parent' if forward else 'child'] in found
            } - found
            if not added:
                break
            found.update(added)
        visible.update(found)
    return {
        **graph,
        'nodes': [node for node in graph['nodes'] if node['pipe_ref'] in visible],
        'edges': [edge for edge in graph['edges'] if edge['parent'] in visible and edge['child'] in visible],
    }


def run_belongs_to_pipeline(run: Dict[str, Any], name: str, definition=None) -> bool:
    """Match the complete name before the engine's timestamp and collision suffix."""
    import re
    record = run.get('run', {})
    selection = record.get('selection')
    if selection:
        import json
        try:
            selection = json.loads(selection) if isinstance(selection, str) else selection
            definitions = selection.get('pipeline_definitions') or []
            if definitions:
                registries = getattr(run, '_snapshot_registries', frozenset())
                return any((snapshot.get('id') == definition['id']
                            if definition and definition.get('id')
                            else snapshot.get('name') == name)
                           and (not definition or not definition.get('registry')
                                or snapshot.get('registry') == definition['registry']
                                or (snapshot.get('name'), snapshot.get('id'), definition['registry']) in registries)
                           for snapshot in definitions)
        except (ValueError, TypeError, AttributeError):
            pass
    run_id = run.get('run', {}).get('run_id', '')
    return bool(re.fullmatch(re.escape(name) + r'-\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d\.\d{6}Z-[a-f0-9]{8}', run_id))


def stopped_execution_times(instance):
    """Correlate unfinished engine records with stopped local execution workers.

    A killed worker cannot persist run_finished. Its retained Job identity provides the
    actual stop time, including after a page reload. Never infer completion from a missing Job.
    """
    from meerschaum.jobs import get_jobs
    ended = {}
    for job in get_jobs(executor_keys='local', include_hidden=True, combine_local_and_systemd=False).values():
        try:
            metadata = job.daemon.properties.get('mrsm_execution') or {}
            run_id = metadata.get('run_id')
            args = job.sysargs
            registry = args[args.index('-i') + 1] if '-i' in args else str(get_instance())
            if run_id and registry == str(instance) and job.status == 'stopped' and job.ended:
                finished = job.ended
                ended[run_id] = finished.replace(tzinfo=finished.tzinfo or timezone.utc).isoformat()
        except Exception:
            continue
    return ended


def reconcile_stopped_run(summary, ended):
    """Use a known worker termination time without changing successful or failed runs."""
    if summary.get('status') != 'running' or not ended:
        return False
    summary['status'] = 'stopped'
    summary['run']['finished'] = ended
    start, finish = _parse_ts(summary['run'].get('started')), _parse_ts(ended)
    if start and finish:
        start = start.replace(tzinfo=start.tzinfo or timezone.utc)
        finish = finish.replace(tzinfo=finish.tzinfo or timezone.utc)
        summary['duration_seconds'] = max(0, (finish-start).total_seconds())
    return True


def get_runs(
    instance: Any = None,
    run_id: Optional[str] = None,
    pipe: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    run_name: Optional[str] = None,
    viewer: Any = None,
) -> List[Dict[str, Any]]:
    """
    Recent runs, newest first (`{run, status, duration_seconds, counts}` each). `run_id` selects
    one run; `pipe` matches a Pipe label or reference in the targets or executed stages. Only the
    runs of a pipe `viewer` (by default the request's) may see are listed, with the other pipes
    anonymized (`Anonymizer`).
    """
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['limit'] = limit
    if run_name:
        request['run_name'] = run_name
    if status:
        request['status'] = status
    if run_id:
        request['run_id'] = run_id
    if pipe:
        request['pipe'] = pipe.strip()
    runs = _call('engine_runs', request)
    if any(run.get('status') == 'running' for run in runs):
        ended = stopped_execution_times(instance)
        for run in runs:
            reconcile_stopped_run(run, ended.get(run['run']['run_id']))
    refs = [ref for summary in runs for ref in (summary.get('run', {}).get('targets') or [])]
    anonymizer = Anonymizer(viewer, instance, refs=refs)
    if anonymizer.active:
        anonymizer.check_generations([run.get('run', {}).get('run_id') for run in runs])
        runs = [visible for visible in (anonymizer.run(run) for run in runs) if visible is not None]
    else:
        runs = [anonymizer.run(run) for run in runs]
    return runs


def get_run_detail(instance: Any, run_id: str, viewer: Any = None) -> Optional[Dict[str, Any]]:
    """
    One run with its nodes and the planned-versus-actual bars (`None` for an unknown run, or one
    without a pipe `viewer` may see; the pipes they may not see are anonymized).
    """
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['run_id'] = run_id
    detail = _call('engine_run_detail', request)
    if detail and (detail.get('summary') or {}).get('status') == 'running':
        ended = stopped_execution_times(instance).get(run_id)
        if reconcile_stopped_run(detail['summary'], ended):
            for bar in detail.get('bars', []):
                if bar.get('status') in ('running', 'pending'):
                    bar['status'] = 'stopped'
                    if bar.get('actual_start') and not bar.get('actual_end'):
                        bar['actual_end'] = ended
    return Anonymizer(viewer, instance).detail(detail)


def get_run_events(
    instance: Any,
    run_id: str,
    after_seq: Optional[int] = None,
    viewer: Any = None,
) -> List[Dict[str, Any]]:
    """
    The persisted events of a run, in order (none for a run without a pipe `viewer` may see;
    the pipes they may not see are anonymized).
    """
    from meerschaum.core.Pipeline import _decode_events
    instance = instance if instance is not None else get_instance()
    anonymizer = Anonymizer(viewer, instance)
    if anonymizer.active:
        ### The run's targets number its hidden pipes, as in its detail (`Anonymizer.run`).
        request = _request(instance)
        request['run_id'] = run_id
        request['limit'] = 1
        summaries = _call('engine_runs', request) or []
        if not summaries or anonymizer.run(summaries[0]) is None:
            return []
    request = _request(instance)
    request['run_id'] = run_id
    if after_seq is not None:
        request['after_seq'] = after_seq
    return anonymizer.events(_decode_events(_call('engine_run_events', request)))


def run_events_stream_path(
    run_id: str,
    instance: Any = None,
    wait: Optional[float] = None,
) -> str:
    """
    The same-origin path of a run's live event stream on the Rust server
    (`GET /engine/runs/{run_id}/events/stream`, Server-Sent Events of the typed engine events).
    `instance` names the instance that keeps the run's history; `wait` (seconds) lets the stream
    wait for a run that has not started yet (a job that was just launched).
    """
    from urllib.parse import quote, urlencode
    params = {}
    if instance is not None:
        params['instance_keys'] = str(instance)
    if wait:
        params['wait'] = str(wait)
    return (
        '/engine/runs/' + quote(str(run_id), safe='') + '/events/stream'
        + ('?' + urlencode(params) if params else '')
    )


def _parse_ts(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    except ValueError:
        return None


def gantt_figure(detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    A Plotly figure of observed execution intervals, including live running nodes.
    """
    traces = []
    for name, color, planned in (('Actual', '#3486bd', False),):
        ys, bases, xs, colors, statuses = [], [], [], [], []
        for bar in detail.get('bars', []):
            start = _parse_ts(bar.get('planned_start' if planned else 'actual_start'))
            end = _parse_ts(bar.get('planned_end' if planned else 'actual_end'))
            if start is None:
                continue
            status = bar.get('status', '')
            if not planned and end is None:
                end = datetime.now(timezone.utc) if start.tzinfo else datetime.now()
                status = 'running'
            if end is None:
                continue
            colors.append(color if planned else {
                'running': '#3486bd', 'succeeded': '#2e9e5b', 'failed': '#d1495b',
                'stopped': '#9aa5b1', 'cancelled': '#9aa5b1',
                'upstream_failed': '#d1495b', 'skipped': '#9aa5b1',
                'rebuild_pending': '#e0a526',
            }.get(status, color))
            statuses.append(status)
            ys.append(ref_label(bar['pipe_ref']))
            bases.append(start.isoformat())
            xs.append(max(int((end - start).total_seconds() * 1000), 1))
        traces.append({
            'type': 'bar',
            'orientation': 'h',
            'name': name,
            'y': ys,
            'base': bases,
            'x': xs,
            'marker': {'color': colors},
            'customdata': statuses,
            'hovertemplate': '%{y}<br>%{customdata}<extra>%{fullData.name}</extra>',
        })
    run_id = (detail.get('summary', {}).get('run', {}) or {}).get('run_id', '')
    return {
        'data': traces,
        'layout': {
            'title': {'text': f"Run {run_id}"},
            'dragmode': False,
            'xaxis': {'type': 'date', 'fixedrange': True},
            'margin': {'l': 30, 'r': 30, 't': 60, 'b': 60},
            'barmode': 'group',
            'uirevision': run_id,
            'yaxis': {'fixedrange': True, 'autorange': 'reversed', 'automargin': True,
                      'categoryorder': 'array',
                      'categoryarray': [ref_label(bar['pipe_ref']) for bar in detail.get('bars', [])]},
            'height': max(260, 120 + 60 * len({t for t in traces[0]['y']} if traces else [])),
            'paper_bgcolor': 'rgba(0,0,0,0)',
            'plot_bgcolor': 'rgba(0,0,0,0)',
            'font': {'color': '#d0d6dc'},
        },
    }


def describe_event(row: Dict[str, Any]) -> str:
    """
    One line of a run's output, in plain words: `<time> <what happened>`
    (`12:00:01.250 Synced Pipe('sql:main', 'daily'): 3 inserted`).
    """
    payload = row.get('payload') or {}
    event = row.get('event', '')
    ts = str(row.get('ts', ''))[11:23]
    pipe_ref = row.get('pipe_ref')
    pipe = ref_label(pipe_ref) if pipe_ref else ''

    def rows_text(source: Dict[str, Any]) -> str:
        parts = [f"{source[key]:,} {key}" for key in ('inserted', 'updated', 'upserted')
                 if source.get(key)]
        return ', '.join(parts) or 'no new rows'

    def chunk_text() -> str:
        text = f"chunk {payload.get('chunk', 0) + 1 if isinstance(payload.get('chunk'), int) else ''}"
        if payload.get('total') is not None:
            text += f" of {payload['total']}"
        return text

    if event == 'run_started':
        nodes = payload.get('nodes', 0)
        text = f"Run started: {nodes} pipe{'s' if nodes != 1 else ''}"
    elif event == 'run_finished':
        text = ('Run finished: ' if payload.get('success', True) else 'Run failed: ') + str(
            payload.get('message', '')).split('\n', 1)[0]
    elif event == 'node_planned':
        text = f"{pipe}: {action_label(payload.get('action', '')).lower()}"
    elif event == 'node_started':
        attempt = payload.get('attempt', 1) or 1
        text = f"Syncing {pipe}" + (f" (attempt {attempt})" if attempt > 1 else '')
    elif event == 'chunk_started':
        text = f"{pipe}: {chunk_text()} started"
        if payload.get('rows') is not None:
            text += f" ({payload['rows']:,} rows)"
    elif event == 'chunk_finished':
        done = 'failed' if payload.get('success') is False else 'done'
        text = f"{pipe}: {chunk_text()} {done}"
        if payload.get('success') is not False:
            text += f" ({rows_text(payload)})"
    elif event == 'node_finished':
        result = payload.get('result') or {}
        if result.get('success', True):
            text = f"Synced {pipe}: {rows_text(result)}"
        else:
            text = f"{pipe} failed: {str(result.get('message', '')).split(chr(10), 1)[0]}"
    elif event == 'node_failed':
        error = str(payload.get('error', '')).split('\n', 1)[0]
        text = f"{pipe} failed: {error}" + ('; trying again' if payload.get('will_retry') else '')
    elif event == 'node_skipped':
        text = f"{pipe}: {payload.get('reason', '')}"
    elif event == 'node_rebuild_pending':
        text = f"{pipe} waits for rebuild approval: {payload.get('reason', '')}"
    elif event == 'watermark_advanced':
        to = payload.get('to')
        text = f"{pipe}: latest data now {bound_label(to)}" if to is not None else f"{pipe}: new data"
    elif event == 'stream_flushed':
        rows = payload.get('rows', 0) or 0
        text = f"{pipe}: wrote {rows:,} new row{'s' if rows != 1 else ''}"
        if payload.get('success') is False:
            text = f"{pipe}: could not write {rows:,} new row{'s' if rows != 1 else ''}"
    elif event == 'stream_restarted':
        text = f"{pipe}: listening again after an error: {payload.get('error', '')}"
    elif event == 'children_triggered':
        nodes = payload.get('nodes') or []
        count = len(nodes) if isinstance(nodes, list) else nodes
        text = f"New data: syncing {count} downstream pipe{'s' if count != 1 else ''}"
    else:
        text = ' '.join(part for part in (event.replace('_', ' ').capitalize(), pipe) if part)
    return f"{ts} {text}".strip()


def relations_of(pipe: mrsm.Pipe) -> Dict[str, List[Dict[str, Any]]]:
    """
    The Parents and Children tabs of a pipe card: `{'parents': [...], 'children': [...]}`, each a
    list of `{pipe_ref, label, href, action, reason, lagging}` (the neighbour's planned action,
    with the planner's reason for the edge).
    """
    if pipe.get_id() is None:
        return {'parents': [], 'children': []}
    ### Every neighbour: the caller decides what the viewer sees (`pipes._relation_rows`).
    from meerschaum.api.dash.access import SYSTEM
    graph = plan_graph(pipe.instance_connector, [pipe], parents=1, children=1, viewer=SYSTEM)
    this_ref = '|'.join((
        str(pipe.instance_keys), str(pipe.connector_keys), str(pipe.metric_key),
        str(pipe.location_key),
    ))
    nodes = {node['pipe_ref']: node for node in graph['nodes']}
    out: Dict[str, List[Dict[str, Any]]] = {'parents': [], 'children': []}
    for edge in graph['edges']:
        if edge['child'] == this_ref:
            neighbour, side = edge['parent'], 'parents'
        elif edge['parent'] == this_ref:
            neighbour, side = edge['child'], 'children'
        else:
            continue
        node = nodes.get(neighbour, {})
        out[side].append({
            'pipe_ref': neighbour,
            'label': ref_label(neighbour),
            'href': pipe_href(neighbour),
            'action': node.get('action', ''),
            'reason': node.get('reason', ''),
            'lagging': edge['lagging'],
        })
    return out
