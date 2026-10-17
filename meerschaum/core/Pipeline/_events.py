#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The live event stream of an engine run (design §5.7): `Pipeline.sync(on_event=...)` and
`pipe.sync(parents=..., children=..., on_event=...)`.

Each event is a plain `dict` (see `EVENT_KEYS` and "Live events in Python" in
`docs/zensical/reference/pipelines/visualization.md`):

```python
{
    'event': 'node_finished',          # the event name
    'run_id': '01JA...',               # the run
    'seq': 7,                          # per-run emission order, from 0
    'ts': datetime(..., tzinfo=utc),   # when it happened (timezone-aware UTC)
    'pipe_ref': 'sql:main|sql:main|daily_rollup|None',   # None for run-level events
    'result': {...},                   # the event's own fields, at the top level
}
```

The callback runs on the thread that called `sync()`, between events; the engine syncs on its
own threads without the GIL, so a slow callback only delays later callbacks (events queue up,
nothing is dropped) and never the sync work.
"""

from __future__ import annotations

import json
import traceback
import warnings
from datetime import datetime, timezone

from meerschaum.utils.typing import Any, Callable, Dict, Optional

#: The envelope keys every event has. The other keys depend on `event`.
EVENT_KEYS = ('event', 'run_id', 'seq', 'ts', 'pipe_ref')

#: Every event name the engine emits.
EVENT_NAMES = (
    'run_started', 'node_planned', 'node_started', 'chunk_started', 'chunk_finished',
    'node_finished', 'node_failed', 'node_skipped', 'node_rebuild_pending',
    'watermark_advanced', 'stream_flushed', 'children_triggered', 'stream_restarted',
    'run_finished',
)


def _parse_ts(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.replace('Z', '+00:00')
    ### `fromisoformat` accepts at most six fractional digits before Python 3.11.
    if '.' in text:
        head, _, tail = text.partition('.')
        digits = ''.join(c for c in tail if c.isdigit())
        rest = tail[len(digits):]
        text = f"{head}.{digits[:6]}{rest}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def decode_event(text: str) -> Dict[str, Any]:
    """Decode one event from the engine's JSON text into the documented `dict`."""
    event = json.loads(text)
    event['ts'] = _parse_ts(event.get('ts', None))
    return event


class EventCallback:
    """
    Wraps a user's `on_event` callable for the engine.

    An `Exception` raised by the callable does **not** stop the run: it is reported once with
    `warnings.warn` (with its traceback), the callable receives no further events, and the
    exception is kept in `error` (the `RunResult` exposes it as `callback_error` and adds a
    warning). A `BaseException` that is not an `Exception` (`KeyboardInterrupt`, `SystemExit`)
    cancels the run and propagates.
    """

    def __init__(self, callback: Callable[[Dict[str, Any]], Any]):
        if not callable(callback):
            raise TypeError(f"`on_event` must be callable (got {callback!r}).")
        self.callback = callback
        self.error: Optional[BaseException] = None
        self.delivered = 0

    def __call__(self, text: str) -> None:
        if self.error is not None:
            return
        event = decode_event(text)
        try:
            self.callback(event)
        except Exception as e:
            self.error = e
            warnings.warn(
                f"The `on_event` callback raised on `{event.get('event', None)}` "
                f"(seq {event.get('seq', None)}); the run continues without it:\n"
                + ''.join(traceback.format_exception(type(e), e, e.__traceback__)),
                stacklevel=2,
            )
            return
        self.delivered += 1

    @property
    def warning(self) -> Optional[str]:
        """The run warning describing a callback failure, if any."""
        if self.error is None:
            return None
        return (
            f"The `on_event` callback raised {type(self.error).__name__}: {self.error} "
            f"(after {self.delivered} event{'s' if self.delivered != 1 else ''}); "
            "later events were not delivered."
        )
