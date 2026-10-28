"""Numeric sync history read through the shared native instance connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone


def _instance(instance):
    from meerschaum.connectors.parse import parse_instance_keys
    return parse_instance_keys(instance) if isinstance(instance, (str, type(None))) else instance


def _context(instance):
    import meerschaum as mrsm
    from meerschaum.core.Pipe._status import _status_context
    carrier = mrsm.Pipe('mrsm', 'engine_metrics', instance=instance, temporary=True)
    return carrier, _status_context(carrier)


def _time(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime):
        raise ValueError('Metrics bounds must be datetimes or ISO-8601 timestamps.')
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def get_engine_metrics(instance=None, begin=None, end=None, pipe=None, pipe_ref=None,
                       pipeline=None, limit=1000, buckets=48, visible_refs=None,
                       visible_pipelines=None):
    """Read numeric sync and Pipeline history on an instance.

    The default window is the last 24 hours; retained history spans 30 days. Queries
    aggregate at most 5,000 sync rows and return ``truncated`` when more match. Durations
    are seconds, timestamps are UTC, and ``success_rate`` is a fraction from 0 to 1.
    ``rows`` counts changed rows (inserted + updated + upserted + deleted), so
    replacement syncs count both deletions and new rows; unchanged syncs count zero.
    Local callers are trusted. Hosts must supply fresh ``visible_refs`` and
    ``visible_pipelines`` before aggregation; empty lists see nothing. API instances
    additionally enforce their current principal's scopes, teams and object access.
    """
    from meerschaum import _core
    instance = _instance(instance)
    carrier, context = _context(instance)
    if pipe is not None and pipe_ref is not None:
        raise ValueError('Supply pipe or pipe_ref, not both.')
    reference = pipe_ref if pipe_ref is not None else pipe
    if reference is not None and not isinstance(reference, str):
        reference = '|'.join((str(reference.instance_keys), str(reference.connector_keys),
                              str(reference.metric_key), str(reference.location_key)))
    request = {
        'instance': str(instance), 'begin': _time(begin), 'end': _time(end),
        'pipe_ref': reference, 'pipeline': getattr(pipeline, 'name', pipeline),
        'limit': limit, 'buckets': buckets,
        'visible_refs': list(visible_refs) if visible_refs is not None else None,
        'visible_pipelines': list(visible_pipelines) if visible_pipelines is not None else None,
    }
    return json.loads(_core.engine_metrics(json.dumps(request), carrier, context))


def initialize_engine_metrics(instance=None):
    """Create empty native metrics/run/node storage and indexes for dashboards.

    API servers initialize their served instance during startup. A remote initialization
    requires a full administrator; local access uses the instance's own credentials.
    """
    from meerschaum import _core
    instance = _instance(instance)
    carrier, context = _context(instance)
    _core.initialize_engine_metrics(json.dumps({'instance': str(instance)}), carrier, context)

