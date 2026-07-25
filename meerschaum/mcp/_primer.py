#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the server instructions sent in the `initialize` response.

This teaches the client Meerschaum's data model. The tools' own descriptions
cover the individual operations, so keep this focused on concepts that are not
obvious from a tool signature.
"""

MRSM_PRIMER: str = """\
Meerschaum is an ETL framework built around **pipes** — named data streams synced into tables.

A pipe is identified by three string keys:
- `connector_keys`: the data source, e.g. `sql:main`, `plugin:noaa`.
- `metric_key`: a label for the stream, e.g. `weather`.
- `location_key`: an optional tag/shard (often null).

A fourth key, the **instance connector** (`instance_keys`, e.g. `sql:main`), is where a pipe's
metadata and data are stored. Tools default to the configured instance when it is omitted.

A pipe's metadata lives in `pipe.parameters` (a dict). Key fields:
- `columns`: maps roles to column names — `datetime` (drives incremental sync), `id`/`primary`
  (uniqueness), `value`.
- `dtypes`: explicit column types. Meerschaum dtype strings: `datetime`, `int`, `numeric`, `uuid`,
  `json`, `bytes`, `geometry[srid]`; any Pandas dtype (`Int64`, `float64`, `bool`) also works.
- `tags`: list of labels for grouping pipes.

The `params` filter (used by the data tools) builds a SQL `WHERE`:
- `{"color": "red"}` -> `= 'red'`; `{"color": ["red", "blue"]}` -> `IN (...)`.
- Prefix `_` negates: `{"color": "_red"}` -> `!= 'red'`.
- Null: `{"color": null}` or `{"color": "None"}` -> `IS NULL`; `"_None"` -> `IS NOT NULL`.

How to work here:
- Call `list_pipes` to discover pipes, then `get_pipe_attributes` for one pipe's parameters and
  `get_pipe_stats` for its rowcount, sync time, and actual database column types.
- Read data with `get_pipe_data` (+ `params`/`begin`/`end`), or `read_sql` for ad-hoc queries.
- `sync_pipe` ingests new data from the pipe's source; `sync_documents` ingests rows you supply;
  `verify_pipe` re-syncs a historical range.
- For anything without a dedicated tool, `execute_action` runs any Meerschaum CLI action. The
  `mrsm://actions` resource lists what is available.
- Times accept ISO datetimes or integers (for integer-axis pipes).
- Prefer narrowing with `begin`/`end`/`params` over paging through everything: on a datetime-axis
  pipe a time window is far cheaper than a deep cursor.

Read the `mrsm://docs` resource for the tool and scope reference. For anything deeper, fetch
https://meerschaum.io/llms.txt (an index of the docs) and follow the relevant link, or
https://meerschaum.io/llms-full.txt for the full text."""
