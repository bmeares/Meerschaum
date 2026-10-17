#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Share process-table scans between daemons.

A daemon whose PID file was lost may still be running, and the only way to find it is to
search every process's command line for its `daemon_id`. Scanning the process table is
expensive, so one scan is shared by every daemon in a short window and negative results
are remembered per daemon.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

from meerschaum.utils.typing import Dict, List, Optional, Tuple
from meerschaum.utils.packages import attempt_import

INDEX_TTL_SECONDS: float = 2.0
"""How long a scan of the process table may be reused by other daemons."""

NEGATIVE_TTL_SECONDS: float = 60.0
"""How long to wait before looking again for a daemon which was not found."""

LAUNCH_GRACE_SECONDS: float = 30.0
"""
For this long after a launch, the daemon's PID file may not exist yet,
so negative results are not remembered.
"""

CREATE_TIME_SLACK_SECONDS: float = 5.0
"""How much later than `began` a process may have started and still be the daemon's."""

_MARKER_PREFIX = "daemon_id='"

_lock = threading.Lock()
_index: Optional[Tuple[float, datetime, List[Tuple[int, str]]]] = None
_negative: Dict[str, Tuple[float, Tuple[Optional[str], ...]]] = {}


def _scan() -> List[Tuple[int, str]]:
    """
    Walk the process table once and return `(pid, command line)` for each process
    which mentions a `daemon_id`.
    """
    psutil = attempt_import('psutil')
    my_pid = os.getpid()
    entries = []
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            if proc.info['pid'] == my_pid:
                continue
            cmdline = proc.info.get('cmdline') or []
            joined = '\0'.join(part or '' for part in cmdline)
            if _MARKER_PREFIX in joined:
                entries.append((int(proc.info['pid']), joined))
        except Exception:
            continue
    return entries


def find_pids(daemon_id: str, not_older_than: Optional[datetime] = None) -> List[int]:
    """
    Return the PIDs of the processes whose command line references the `daemon_id`.
    The process table is only scanned if the last scan is older than `INDEX_TTL_SECONDS`
    or began before `not_older_than` (a naive UTC datetime, e.g. when the job was launched).
    """
    global _index
    marker = f"daemon_id='{daemon_id}'"
    with _lock:
        if (
            _index is None
            or (time.monotonic() - _index[0]) > INDEX_TTL_SECONDS
            or (not_older_than is not None and _index[1] < not_older_than)
        ):
            started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            _index = (time.monotonic(), started_at, _scan())
        entries = _index[2]
    return [pid for pid, cmdline in entries if marker in cmdline]


def invalidate(daemon_id: Optional[str] = None) -> None:
    """
    Forget the shared scan and the remembered negative results for `daemon_id`
    (or for every daemon if it is `None`).
    """
    global _index
    with _lock:
        _index = None
        if daemon_id is None:
            _negative.clear()
        else:
            _negative.pop(daemon_id, None)


def is_known_missing(daemon_id: str, fingerprint: Tuple[Optional[str], ...]) -> bool:
    """
    Return whether a scan recently failed to find the daemon and nothing has changed since.
    `fingerprint` is the daemon's recorded timestamps, so starting the job (even from another
    process) invalidates the remembered result.
    """
    with _lock:
        entry = _negative.get(daemon_id)
        if entry is None:
            return False
        remembered_at, remembered_fingerprint = entry
        if remembered_fingerprint != fingerprint:
            _negative.pop(daemon_id, None)
            return False
        if (time.monotonic() - remembered_at) > NEGATIVE_TTL_SECONDS:
            _negative.pop(daemon_id, None)
            return False
        return True


def remember_missing(daemon_id: str, fingerprint: Tuple[Optional[str], ...]) -> None:
    """
    Remember that the daemon could not be found.
    """
    with _lock:
        _negative[daemon_id] = (time.monotonic(), fingerprint)


def parse_timestamp(text: Optional[str]) -> Optional[datetime]:
    """
    Parse a naive UTC timestamp from a daemon's properties.
    """
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text))
    except Exception:
        return None


def is_stale_process(pid: int, moment: datetime, slack_seconds: float) -> bool:
    """
    Return whether the process cannot be the one which wrote `moment` (a naive UTC datetime):
    it no longer exists, or it was created more than `slack_seconds` after `moment`
    (a reused PID). An unreadable creation time is not treated as stale.
    """
    psutil = attempt_import('psutil')
    try:
        created = datetime.fromtimestamp(
            psutil.Process(pid).create_time(),
            timezone.utc,
        ).replace(tzinfo=None)
    except psutil.NoSuchProcess:
        return True
    except Exception:
        return False
    return (created - moment).total_seconds() > slack_seconds
