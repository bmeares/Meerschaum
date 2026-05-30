#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Native range partitioning for non-TimescaleDB SQL flavors.

TimescaleDB auto-creates chunks on insert; other flavors do not. These methods, mixed into
`SQLConnector`, declare a partitioned parent table (`PARTITION BY RANGE` on the datetime column)
and pre-create the interval-aligned child partitions a dataframe needs before it is inserted.

Partitioning is opt-in for non-TimescaleDB flavors: set `pipe.parameters['hypertable'] = True`
(the same flag TimescaleDB uses). The partition width reuses the pipe's chunk interval
(`pipe.parameters['verify']['chunk_minutes']`, default 1440).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Optional, List, Tuple, SuccessTuple
from meerschaum.utils.debug import dprint

### Non-TimescaleDB flavors supporting declarative `PARTITION BY RANGE` on a datetime column.
PARTITIONABLE_FLAVORS = {'postgresql', 'postgis'}

### Safety cap on the number of partitions created for a single dataframe.
_MAX_PARTITIONS_PER_SYNC = 10_000

### Anchor interval-aligned partition boundaries to the Unix epoch so the same value always maps
### to the same partition, regardless of which rows happen to arrive first.
_EPOCH_NAIVE = datetime(1970, 1, 1)


def _should_partition(self, pipe: mrsm.Pipe) -> bool:
    """
    Return whether a pipe's target table should use native range partitioning.

    TimescaleDB is excluded here — it manages its own chunks via the hypertable path.
    """
    if self.flavor not in PARTITIONABLE_FLAVORS:
        return False
    if pipe.columns.get('datetime', None) is None:
        return False
    return bool(pipe.parameters.get('hypertable', False))


def _get_partition_column(self, pipe: mrsm.Pipe) -> Optional[str]:
    """Return the column a pipe is partitioned by (its datetime column), or `None`."""
    if not self._should_partition(pipe):
        return None
    return pipe.columns.get('datetime', None)


def _partition_bounds(
    self,
    value: Union[datetime, int],
    interval: Union[timedelta, int],
) -> Tuple[Union[datetime, int], Union[datetime, int]]:
    """
    Return the `[lo, hi)` interval-aligned partition boundaries containing `value`.
    """
    if isinstance(interval, int):
        n = int(value) // interval
        lo = n * interval
        return lo, lo + interval

    epoch = (
        datetime(1970, 1, 1, tzinfo=value.tzinfo)
        if getattr(value, 'tzinfo', None) is not None
        else _EPOCH_NAIVE
    )
    n = (value - epoch) // interval
    lo = epoch + (n * interval)
    return lo, lo + interval


def _partition_literal(self, value: Union[datetime, int]) -> str:
    """Return the SQL literal for a partition boundary value."""
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    return str(value)


def _partition_name(self, pipe: mrsm.Pipe, lo: Union[datetime, int]) -> str:
    """Return the (truncated) child-partition table name for a lower boundary."""
    from meerschaum.utils.sql import truncate_item_name
    if isinstance(lo, datetime):
        suffix = lo.strftime('%Y%m%d%H%M%S')
    else:
        suffix = str(lo).replace('-', 'n')
    return truncate_item_name(f"{pipe.target}_p_{suffix}", self.flavor)


def _get_partition_ranges_for_df(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> List[Tuple[Union[datetime, int], Union[datetime, int]]]:
    """
    Return the list of `(lo, hi)` partition boundaries spanning a dataframe's datetime values.

    Walks the interval grid from the dataframe's minimum to maximum datetime so contiguous
    intermediate partitions are also created.
    """
    dt_col = pipe.columns.get('datetime', None)
    if dt_col is None or dt_col not in getattr(df, 'columns', []):
        return []

    series = df[dt_col].dropna()
    if len(series) == 0:
        return []

    min_val, max_val = series.min(), series.max()
    if hasattr(min_val, 'to_pydatetime'):
        min_val = min_val.to_pydatetime()
    if hasattr(max_val, 'to_pydatetime'):
        max_val = max_val.to_pydatetime()
    if not isinstance(min_val, datetime):
        min_val, max_val = int(min_val), int(max_val)

    interval = pipe.get_chunk_interval(debug=debug)

    ranges = []
    lo, _ = self._partition_bounds(min_val, interval)
    cursor = lo
    count = 0
    while cursor <= max_val:
        lo_cursor, hi_cursor = self._partition_bounds(cursor, interval)
        ranges.append((lo_cursor, hi_cursor))
        cursor = hi_cursor
        count += 1
        if count >= _MAX_PARTITIONS_PER_SYNC:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {_MAX_PARTITIONS_PER_SYNC}-partition limit for {pipe}; "
                "consider a larger `chunk_minutes`.",
                stack=False,
            )
            break
    return ranges


def _create_missing_partitions(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> SuccessTuple:
    """
    Create any range partitions a dataframe needs before its rows are inserted.

    A no-op (success) for non-partitioned pipes or empty dataframes.
    """
    from meerschaum.utils.sql import sql_item_name

    if not self._should_partition(pipe):
        return True, "Pipe is not partitioned."
    if df is None or not hasattr(df, 'columns') or len(df) == 0:
        return True, "No rows to partition."

    ranges = self._get_partition_ranges_for_df(pipe, df, debug=debug)
    if not ranges:
        return True, "No partitions to create."

    schema = self.get_pipe_schema(pipe)
    parent_name = sql_item_name(pipe.target, self.flavor, schema)

    queries = []
    for lo, hi in ranges:
        part_name = sql_item_name(self._partition_name(pipe, lo), self.flavor, schema)
        queries.append(
            f"CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF {parent_name}\n"
            f"FOR VALUES FROM ({self._partition_literal(lo)}) TO ({self._partition_literal(hi)})"
        )

    if debug:
        dprint(f"[{self}] Ensuring {len(queries)} partition(s) for {pipe}.")

    try:
        success = all(self.exec_queries(
            queries, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
        ))
    except Exception as e:
        return False, f"Failed to create partitions for {pipe}:\n{e}"

    if not success:
        return False, f"Failed to create partitions for {pipe}."
    return True, f"Ensured {len(queries)} partition(s) for {pipe}."
