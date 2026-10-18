#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Assertion messages for engine runs: the summary of a `RunResult` alone says only
`1 succeeded, 1 failed`, so a failure in CI would not show which node failed or why.
"""

from __future__ import annotations

from typing import Any


def why(run: Any) -> str:
    """
    The run message, every outcome that did not succeed (with the node's own message) and the
    engine's record of each node (action, status, reason, attempts, message): what
    `mrsm show run <run_id>` prints.
    """
    parts = [f"{getattr(run, 'message', run)}"]
    failed = [
        (o.get('pipe_ref'), o.get('status'), (o.get('result') or {}).get('message'))
        for o in (getattr(run, 'outcomes', None) or [])
        if not (o.get('result') or {}).get('success')
    ]
    parts.append(f"failed nodes: {failed}")
    record = getattr(run, 'record', None) or {}
    nodes = [
        {
            key: node.get(key)
            for key in ('pipe_ref', 'action', 'status', 'reason', 'attempts', 'message')
        }
        for node in (record.get('nodes', None) or [])
    ]
    parts.append(f"run {getattr(run, 'run_id', None)} nodes: {nodes}")
    return '; '.join(parts)
