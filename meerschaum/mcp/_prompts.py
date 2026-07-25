#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the MCP prompts.

Prompts are the workflows a client surfaces to the user (often as slash
commands). Each handler returns a list of MCP message dicts.
"""

from __future__ import annotations

from meerschaum.mcp._registry import prompt
from meerschaum.utils.typing import Any, Dict, List, Optional


def _user_message(text: str) -> List[Dict[str, Any]]:
    """
    Return a single-turn user message, the shape `prompts/get` expects.
    """
    return [{'role': 'user', 'content': {'type': 'text', 'text': text}}]


@prompt(
    'bootstrap_pipe',
    (
        "Walk through designing and registering a new pipe for a described data source, "
        "choosing index columns and dtypes before writing anything."
    ),
    title='Bootstrap a pipe',
    arguments=[
        {
            'name': 'source',
            'description': (
                "The data source to ingest, e.g. 'the orders table in sql:warehouse' or "
                "'the NOAA weather API'."
            ),
            'required': True,
        },
        {
            'name': 'instance_keys',
            'description': "Where to store the pipe, e.g. 'sql:main'. Optional.",
            'required': False,
        },
    ],
    scopes=['pipes:write'],
)
def _bootstrap_pipe_prompt(
    source: str = '',
    instance_keys: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    instance_line = (
        f"Register it on the instance `{instance_keys}`."
        if instance_keys
        else "Register it on the default instance."
    )
    return _user_message(
        f"""\
Help me register a new Meerschaum pipe for this data source:

{source}

{instance_line}

Work in this order and stop for my confirmation before writing anything:

1. Read the `mrsm://docs` resource if you have not already, then inspect what already exists —
   `list_pipes` to check for an overlapping pipe, and `read_sql` or `mrsm://connectors` to look at
   the source's shape if it is a SQL connector.
2. Propose the three pipe keys (`connector_keys`, `metric_key`, `location_key`), explaining what
   each one means for this source.
3. Propose the index columns: which column is the `datetime` axis that drives incremental sync,
   which columns form the `id`/`primary` uniqueness key, and why.
4. Propose the dtypes, calling out anything that needs an explicit Meerschaum dtype
   (`numeric`, `json`, `uuid`, `bytes`, `geometry[srid]`) rather than a Pandas default.
5. Show me the exact `register_pipe` arguments you intend to use, then wait for my go-ahead.
6. After I confirm, register it, run `sync_pipe`, and report the resulting `get_pipe_stats`."""
    )


@prompt(
    'diagnose_sync_failure',
    (
        "Systematically diagnose why a pipe is not syncing, or is syncing the wrong rows, "
        "working from its configuration and current state rather than guesses."
    ),
    title='Diagnose a sync failure',
    arguments=[
        {
            'name': 'connector_keys',
            'description': "The pipe's connector keys, e.g. 'sql:main'.",
            'required': True,
        },
        {
            'name': 'metric_key',
            'description': "The pipe's metric key, e.g. 'weather'.",
            'required': True,
        },
        {
            'name': 'location_key',
            'description': "The pipe's location key. Omit for a null location.",
            'required': False,
        },
        {
            'name': 'symptom',
            'description': (
                "What you are seeing, e.g. 'no new rows since Tuesday' or 'duplicate rows'."
            ),
            'required': False,
        },
    ],
    scopes=['pipes:read'],
)
def _diagnose_sync_failure_prompt(
    connector_keys: str = '',
    metric_key: str = '',
    location_key: Optional[str] = None,
    symptom: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    keys_line = f"`{connector_keys}` / `{metric_key}` / `{location_key or 'None'}`"
    symptom_line = f"\n\nThe symptom I am seeing: {symptom}" if symptom else ""
    return _user_message(
        f"""\
Diagnose the sync behavior of the Meerschaum pipe {keys_line}.{symptom_line}

Gather evidence before forming a hypothesis:

1. `get_pipe_attributes` — read its `columns` (especially the `datetime` axis), `dtypes`,
   `fetch` config (including `backtrack_minutes`), `verify` config, and whether `upsert`,
   `static`, or `enforce` are set.
2. `get_pipe_stats` — confirm the table exists, and compare its rowcount, newest and oldest sync
   times, and actual database column types against what the parameters claim.
3. `get_pipe_data` with `backtrack_minutes` — look at the most recent rows actually stored.
4. If it is a SQL-connector pipe, use `read_sql` against the source to compare what the source
   currently returns for the same window.

Then tell me:

- The most likely cause, and which specific piece of evidence supports it.
- Whether a plain `sync_pipe`, a `verify_pipe` over a range, or a `deduplicate_pipe` is the right
  remedy — and what each would do to the stored rows.
- Anything in the pipe's configuration that will cause this again.

Do not modify the pipe or its data. Recommend the fix and let me run it."""
    )


@prompt(
    'explain_pipe',
    (
        "Explain what a pipe contains and how it is configured, in terms someone unfamiliar "
        "with the instance can follow."
    ),
    title='Explain a pipe',
    arguments=[
        {
            'name': 'connector_keys',
            'description': "The pipe's connector keys, e.g. 'sql:main'.",
            'required': True,
        },
        {
            'name': 'metric_key',
            'description': "The pipe's metric key, e.g. 'weather'.",
            'required': True,
        },
        {
            'name': 'location_key',
            'description': "The pipe's location key. Omit for a null location.",
            'required': False,
        },
    ],
    scopes=['pipes:read'],
)
def _explain_pipe_prompt(
    connector_keys: str = '',
    metric_key: str = '',
    location_key: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    keys_line = f"`{connector_keys}` / `{metric_key}` / `{location_key or 'None'}`"
    return _user_message(
        f"""\
Explain the Meerschaum pipe {keys_line} to someone who has not seen this instance before.

Read `get_pipe_attributes`, `get_pipe_stats`, and a small sample via `get_pipe_data`
(`limit` 10, `backtrack_minutes` set) — then cover:

- What this data stream is, judging from its keys, tags, and columns.
- Where it comes from, and how new data arrives (its source connector and fetch config).
- Its time axis and grain: which column is the `datetime` index, what a single row represents,
  and which columns make a row unique.
- Its actual size and freshness — rowcount, and how far behind the newest row is.
- Any configuration worth knowing about: `upsert`, `static`, `autoincrement`, `autotime`,
  a non-default `precision`, or partitioning settings.
- Anything that looks inconsistent, such as declared dtypes disagreeing with the database's
  real column types, or a datetime axis with no recent rows.

Do not modify anything."""
    )
