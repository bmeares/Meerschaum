#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The per-pipe sync status: the latest outcome of a pipe's syncs, recorded by every sync path
(`mrsm sync pipes`, jobs, Pipeline runs, the API server and `pipe.sync()`) on the pipe's instance.

Each status is a `dict` with these keys:

- `pipe_ref`, `connector_keys`, `metric_key`, `location_key`, `instance_keys`: the pipe.
- `last_attempt`: when the latest recorded sync ended (ISO-8601 UTC text).
- `last_success`, `last_failure`: when the latest successful / failed sync ended, or `None`.
- `success`, `message`: the latest sync's result (long messages are shortened).
- `rows_inserted`, `rows_updated`, `rows_upserted`: the latest sync's row counts.
- `duration_seconds`: how long the latest sync took.
- `consecutive_failures`: failed syncs since the last success (`0` after a success).
- `source`: what ran the latest sync: `'cli'`, `'job'`, `'engine'` (a Pipeline run),
  `'api'` (the API server) or `'python'`.
- `job`: the name of the job the latest sync ran in, or `None`.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, InstanceConnector, Optional, Union


def _status_context(pipe: mrsm.Pipe, debug: bool = False) -> Dict[str, Any]:
    """The connectors the `_core` status bindings read the pipe's instance through."""
    from meerschaum.core.Pipe._sync import _native_context
    context, reason = _native_context(pipe, debug=debug)
    if context is None:
        raise RuntimeError(f"Cannot use the instance of {pipe}: {reason}.")
    return context


def get_sync_status(self, debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Return how this pipe's latest recorded sync went.

    Every sync records its outcome on the pipe's instance: `mrsm sync pipes`, background jobs,
    Pipeline runs, the API server and `pipe.sync()`. The outcome is written in the background
    (a sync never waits for it): successes of one pipe at most every few seconds, failures at
    once. A read in the same process sees the latest sync, and a script writes its last
    outcomes when it exits.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A dictionary (see `meerschaum.core.Pipe._status`), or `None` if no sync of this pipe has
    been recorded. Reading the status from an unreachable instance raises an exception.

    Examples
    --------
    >>> pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:local')
    >>> pipe.sync([{'ts': '2026-01-01', 'temp': 20.1}])
    >>> status = pipe.get_sync_status()
    >>> status['success'], status['consecutive_failures']
    (True, 0)
    """
    from meerschaum import _core
    return _core.pipe_get_sync_status(self, _status_context(self, debug=debug), debug=debug)


def get_sync_statuses(
    instance: Union[str, InstanceConnector, None] = None,
    debug: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Return the latest recorded sync of every pipe on an instance.

    Parameters
    ----------
    instance: Union[str, InstanceConnector, None], default None
        The instance connector or its keys. Defaults to the configured instance
        (`meerschaum:instance`).

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A dictionary of `pipe_ref` (`'instance|connector|metric|location'`) to the pipe's status
    dictionary (see `meerschaum.core.Pipe._status`). Pipes without a recorded sync are absent.
    Reading from an unreachable instance raises an exception.

    Examples
    --------
    >>> statuses = mrsm.get_sync_statuses('sql:local')
    >>> failing = [s for s in statuses.values() if s['consecutive_failures'] > 0]
    """
    from meerschaum import _core
    from meerschaum.config import get_config
    if instance is None:
        instance = get_config('meerschaum', 'instance')
    ### A pipe only carries the instance to the binding: it is never registered or read.
    carrier = mrsm.Pipe('mrsm', 'engine_sync_status', instance=instance, temporary=True)
    statuses = _core.get_sync_statuses(
        carrier,
        _status_context(carrier, debug=debug),
        debug=debug,
    )
    return {status['pipe_ref']: status for status in statuses}
