#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Native range partitioning for non-TimescaleDB SQL flavors.

TimescaleDB auto-creates chunks on insert; other flavors do not. These methods, mixed into
`SQLConnector`, declare a partitioned parent table (`PARTITION BY RANGE` on the datetime column)
and pre-create the interval-aligned child partitions a dataframe needs before it is inserted.

Partitioning is on by default for datetime-axis pipes on these flavors (`hypertable` defaults to
`True`, the same flag TimescaleDB uses); set `pipe.parameters['hypertable'] = False` to opt out.
The partition width reuses the pipe's chunk interval (`pipe.parameters['verify']['chunk_minutes']`,
default 43200 — 30 days).
"""

from __future__ import annotations

from datetime import datetime, date, timedelta, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Optional, List, Tuple, SuccessTuple
from meerschaum.utils.debug import dprint

### PostgreSQL-style: empty `PARTITION BY RANGE` parent, children added via `CREATE TABLE ...
### PARTITION OF ... FOR VALUES FROM/TO`.
PG_PARTITION_FLAVORS = {'postgresql', 'postgis'}
### MySQL-style: initial partitions inline at `CREATE TABLE`, extended via `ALTER TABLE ADD
### PARTITION ... VALUES LESS THAN`.
MYSQL_PARTITION_FLAVORS = {'mysql', 'mariadb'}
### MSSQL-style: a partition function + scheme (database objects) created before the table;
### extended via `ALTER PARTITION FUNCTION ... SPLIT RANGE`.
MSSQL_PARTITION_FLAVORS = {'mssql'}
### Non-TimescaleDB flavors supporting declarative range partitioning on a datetime column.
PARTITIONABLE_FLAVORS = PG_PARTITION_FLAVORS | MYSQL_PARTITION_FLAVORS | MSSQL_PARTITION_FLAVORS

### Safety cap on the number of partitions created for a single dataframe. This is a guard against
### a pathological config (e.g. a tiny interval over a huge range) issuing a flood of DDL in one
### sync — not a per-table limit (the database enforces its own, e.g. MySQL's 8192). Override it
### under `system.connectors.sql.instance.max_partitions_per_sync`.
_MAX_PARTITIONS_PER_SYNC = 10_000


def _max_partitions_per_sync() -> int:
    """Return the configured per-sync partition-creation cap (falls back to the default)."""
    from meerschaum.config import get_config
    try:
        configured = get_config(
            'system', 'connectors', 'sql', 'instance', 'max_partitions_per_sync',
            warn=False,
        )
    except Exception:
        configured = None
    try:
        return int(configured) if configured is not None else _MAX_PARTITIONS_PER_SYNC
    except (TypeError, ValueError):
        return _MAX_PARTITIONS_PER_SYNC

### Anchor interval-aligned partition boundaries to the Unix epoch so the same value always maps
### to the same partition, regardless of which rows happen to arrive first.
_EPOCH_NAIVE = datetime(1970, 1, 1)


def _materialize_scalar(val: Any) -> Any:
    """
    Return a concrete scalar from a possibly-lazy reduction result.

    Dask reductions (`series.min()` / `series.max()`) return a lazy `Scalar` object rather than a
    real number or `Timestamp`; `.compute()` materializes it. Pandas, NumPy, and built-in scalars
    have no `.compute()` and are returned unchanged.
    """
    if isinstance(val, (datetime, int, float)):
        return val
    compute = getattr(val, 'compute', None)
    if callable(compute):
        try:
            return compute()
        except Exception:
            return val
    return val


def _normalize_boundary(val: Any) -> Any:
    """
    Coerce a dataframe reduction result into a partition-boundary value.

    Materializes lazy Dask scalars, unwraps pandas `Timestamp` to `datetime`, and promotes a plain
    `date` (a `date`-dtype axis) to a midnight `datetime` so it flows through the timedelta-grid
    path. Integers and other scalars pass through unchanged for the integer-axis branch.
    """
    val = _materialize_scalar(val)
    if hasattr(val, 'to_pydatetime'):
        val = val.to_pydatetime()
    ### `date` is not a subclass of `datetime`; promote it to midnight so interval math (and the
    ### `isinstance(..., datetime)` axis check) treat it like a timestamp boundary.
    if isinstance(val, date) and not isinstance(val, datetime):
        val = datetime(val.year, val.month, val.day)
    return val


def _should_partition(self, pipe: mrsm.Pipe) -> bool:
    """
    Return whether a pipe's target table should use native range partitioning.

    TimescaleDB is excluded here — it manages its own chunks via the hypertable path.
    """
    if self.flavor not in PARTITIONABLE_FLAVORS:
        return False
    if pipe.columns.get('datetime', None) is None:
        return False
    ### `hypertable` defaults to `True` (the same default the TimescaleDB create-table path uses),
    ### so datetime-axis pipes on these flavors are partitioned by default. Set it to `False` to
    ### opt out. Pre-existing non-partitioned tables are protected separately (see
    ### `_create_missing_partitions_pg`, which skips a parent that isn't actually partitioned).
    return bool(pipe.parameters.get('hypertable', True))


def _get_partition_column(self, pipe: mrsm.Pipe) -> Optional[str]:
    """Return the column a pipe is partitioned by (its datetime column), or `None`."""
    if not self._should_partition(pipe):
        return None
    return pipe.columns.get('datetime', None)


def _get_partition_count(self, pipe: mrsm.Pipe, debug: bool = False) -> Optional[int]:
    """Return the number of native range partitions for a pipe's target table, or `None`."""
    from meerschaum.utils.sql import sql_item_name
    flavor = self.flavor
    schema = self.get_pipe_schema(pipe)
    try:
        if flavor in PG_PARTITION_FLAVORS:
            full = sql_item_name(pipe.target, flavor, schema).replace("'", "''")
            val = self.value(
                f"SELECT COUNT(*) FROM pg_inherits WHERE inhparent = '{full}'::regclass",
                silent=True, debug=debug,
            )
            return int(val) if val is not None else None
        if flavor in MYSQL_PARTITION_FLAVORS:
            db_name = (
                schema or getattr(self, 'database', None) or self.parse_uri(self.URI).get('database', None)
            )
            if not db_name:
                return None
            clean_db = db_name.replace("'", "''")
            clean_target = pipe.target.replace("'", "''")
            val = self.value(
                "SELECT COUNT(*) FROM information_schema.PARTITIONS\n"
                f"WHERE TABLE_SCHEMA = '{clean_db}' AND TABLE_NAME = '{clean_target}'\n"
                "  AND PARTITION_NAME IS NOT NULL",
                silent=True, debug=debug,
            )
            return int(val) if val is not None else None
        if flavor in MSSQL_PARTITION_FLAVORS:
            ### A `RANGE RIGHT` function with N boundaries yields N + 1 partitions.
            func_name = self._partition_function_name(pipe).replace("'", "''")
            val = self.value(
                "SELECT COUNT(*) FROM sys.partition_functions pf\n"
                "JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id\n"
                f"WHERE pf.name = '{func_name}'",
                silent=True, debug=debug,
            )
            return (int(val) + 1) if val is not None else None
    except Exception as e:
        if debug:
            dprint(f"[{self}] Could not count partitions for {pipe}: {e}")
        return None
    return None


def _get_chunk_count_timescaledb(self, pipe: mrsm.Pipe, debug: bool = False) -> Optional[int]:
    """Return the number of chunks in a TimescaleDB hypertable, or `None`."""
    schema = self.get_pipe_schema(pipe)
    clean_target = pipe.target.replace("'", "''")
    schema_clause = (
        f" AND hypertable_schema = '{schema.replace(chr(39), chr(39) * 2)}'" if schema else ""
    )
    query = (
        "SELECT COUNT(*) FROM timescaledb_information.chunks\n"
        f"WHERE hypertable_name = '{clean_target}'{schema_clause}"
    )
    try:
        val = self.value(query, silent=True, debug=debug)
        return int(val) if val is not None else None
    except Exception as e:
        if debug:
            dprint(f"[{self}] Could not count chunks for {pipe}: {e}")
        return None


def get_partition_info(self, pipe: mrsm.Pipe, debug: bool = False) -> dict:
    """
    Return a summary of a pipe's target table partitioning for `show partitions`.

    Keys:
    - `flavor`: the connector flavor.
    - `partitioned`: whether the table is range-partitioned (native) or a TimescaleDB hypertable.
    - `count`: the number of partitions / chunks (`None` if unknown).
    - `interval`: the physical partition width (`timedelta`, epoch-`int`, or `None`).
    """
    info = {'flavor': self.flavor, 'partitioned': False, 'count': None, 'interval': None}
    try:
        if not pipe.exists(debug=debug):
            return info
    except Exception:
        return info

    flavor = self.flavor
    if flavor in _TIMESCALEDB_FLAVORS:
        if not self._is_hypertable(pipe, debug=debug):
            return info
        info['partitioned'] = True
        info['count'] = self._get_chunk_count_timescaledb(pipe, debug=debug)
        info['interval'] = pipe.get_chunk_interval(debug=debug)
        return info

    if not self._should_partition(pipe):
        return info
    ### Report based on the table's ACTUAL state, not just the `hypertable` flag — a pre-existing
    ### plain table (created before partitioning, or with `hypertable` only just enabled) has no
    ### partitions and should not be reported as partitioned.
    count = self._get_partition_count(pipe, debug=debug)
    if not count:
        return info
    info['partitioned'] = True
    info['count'] = count
    info['interval'] = pipe.get_chunk_interval(debug=debug)
    return info


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
        ### MySQL/MariaDB store datetimes timezone-naive (as UTC); their `RANGE COLUMNS` literals
        ### must therefore be naive. PostgreSQL keeps the offset so `TIMESTAMPTZ` boundaries are
        ### unambiguous regardless of session timezone.
        if self.flavor in MYSQL_PARTITION_FLAVORS:
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return "'" + value.strftime('%Y-%m-%d %H:%M:%S.%f') + "'"
        return f"'{value.isoformat()}'"
    return str(value)


def _get_initial_partition_bounds(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> List[Tuple[str, str]]:
    """
    Return `(partition_name, upper_bound_literal)` tuples (ascending) for the initial inline
    partition definitions of a MySQL/MariaDB `CREATE TABLE`.
    """
    ranges = self._get_partition_ranges_for_df(pipe, df, debug=debug)
    return [
        (self._partition_name(pipe, lo), self._partition_literal(hi))
        for lo, hi in ranges
    ]


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

    ### Normalize materializes lazy Dask scalars, unwraps Timestamps, and promotes `date` to a
    ### midnight `datetime` so date-axis pipes don't fall through to the integer branch.
    min_val, max_val = _normalize_boundary(series.min()), _normalize_boundary(series.max())
    if not isinstance(min_val, datetime):
        min_val, max_val = int(min_val), int(max_val)

    interval = pipe.get_chunk_interval(debug=debug)

    ranges = []
    lo, _ = self._partition_bounds(min_val, interval)
    cursor = lo
    count = 0
    max_parts = _max_partitions_per_sync()
    while cursor <= max_val:
        lo_cursor, hi_cursor = self._partition_bounds(cursor, interval)
        ranges.append((lo_cursor, hi_cursor))
        cursor = hi_cursor
        count += 1
        if count >= max_parts:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {max_parts}-partition limit for {pipe}; "
                "consider a larger `chunk_minutes`.",
                stack=False,
            )
            break
    return ranges


def _partition_function_name(self, pipe: mrsm.Pipe) -> str:
    """Return the (truncated) MSSQL partition-function name for a pipe."""
    from meerschaum.utils.sql import truncate_item_name
    return truncate_item_name(f"pf_{pipe.target}", self.flavor)


def _partition_scheme_name(self, pipe: mrsm.Pipe) -> str:
    """Return the (truncated) MSSQL partition-scheme name for a pipe."""
    from meerschaum.utils.sql import truncate_item_name
    return truncate_item_name(f"ps_{pipe.target}", self.flavor)


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
    if not self._should_partition(pipe):
        return True, "Pipe is not partitioned."
    if df is None or not hasattr(df, 'columns') or len(df) == 0:
        return True, "No rows to partition."

    if self.flavor in PG_PARTITION_FLAVORS:
        return self._create_missing_partitions_pg(pipe, df, debug=debug)
    if self.flavor in MYSQL_PARTITION_FLAVORS:
        return self._create_missing_partitions_mysql(pipe, df, debug=debug)
    if self.flavor in MSSQL_PARTITION_FLAVORS:
        return self._create_missing_partitions_mssql(pipe, df, debug=debug)
    return True, "Pipe is not partitioned."


def _create_missing_partitions_pg(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> SuccessTuple:
    """
    Create missing PostgreSQL child partitions via `CREATE TABLE IF NOT EXISTS ... PARTITION OF`.
    """
    from meerschaum.utils.sql import sql_item_name

    ranges = self._get_partition_ranges_for_df(pipe, df, debug=debug)
    if not ranges:
        return True, "No partitions to create."

    schema = self.get_pipe_schema(pipe)
    parent_name = sql_item_name(pipe.target, self.flavor, schema)

    ### Guard against a pre-existing PLAIN table: `CREATE TABLE ... PARTITION OF` errors if the
    ### parent isn't declaratively partitioned. With `hypertable` defaulting to `True`, an older
    ### non-partitioned table would otherwise break on its next sync. (MySQL/MSSQL no-op naturally
    ### when no partitions/function exist, so they need no equivalent guard.)
    parent_regclass = parent_name.replace("'", "''")
    try:
        is_partitioned = self.value(
            f"SELECT 1 FROM pg_partitioned_table WHERE partrelid = '{parent_regclass}'::regclass",
            silent=True, debug=debug,
        )
    except Exception:
        is_partitioned = None
    if not is_partitioned:
        return True, f"{pipe} target table is not partitioned; skipping partition creation."

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


def _get_mysql_max_partition_bound(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Optional[Union[datetime, int]]:
    """
    Return the highest existing `VALUES LESS THAN` boundary for a MySQL/MariaDB partitioned table.

    Parses `information_schema.PARTITIONS.PARTITION_DESCRIPTION` (the literal upper bound) rather
    than partition names, which may be truncated. Returns `None` if no partitions are found.
    """
    ### On MySQL/MariaDB a "schema" is a database; honor a pipe's configured schema so this lookup
    ### matches the database the partition DDL actually targets (`get_pipe_schema` in the DDL path).
    db_name = (
        self.get_pipe_schema(pipe)
        or self.database
        or self.parse_uri(self.URI).get('database', None)
    )
    if not db_name:
        return None
    clean_db = db_name.replace("'", "''")
    clean_target = pipe.target.replace("'", "''")
    query = (
        "SELECT PARTITION_DESCRIPTION\n"
        "FROM information_schema.PARTITIONS\n"
        f"WHERE TABLE_SCHEMA = '{clean_db}' AND TABLE_NAME = '{clean_target}'\n"
        "  AND PARTITION_DESCRIPTION IS NOT NULL AND PARTITION_DESCRIPTION != 'MAXVALUE'"
    )
    try:
        df = self.read(query, silent=True, debug=debug)
    except Exception:
        return None
    if df is None or len(df) == 0:
        return None

    dt_col = pipe.columns.get('datetime', None)
    dt_dtype = str(pipe.dtypes.get(dt_col, 'datetime')) if dt_col else 'datetime'
    is_int_axis = 'int' in dt_dtype.lower()

    max_bound = None
    for raw in df['PARTITION_DESCRIPTION'].tolist():
        ### `RANGE COLUMNS` datetime descriptions look like `'2024-01-08 00:00:00.000000'`
        ### (quoted); integer descriptions are bare numbers.
        text = str(raw).strip().strip("'").strip('"')
        try:
            bound = int(text) if is_int_axis else datetime.fromisoformat(text)
        except Exception:
            continue
        if max_bound is None or bound > max_bound:
            max_bound = bound
    return max_bound


def _create_missing_partitions_mysql(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> SuccessTuple:
    """
    Extend a MySQL/MariaDB partitioned table upward via `ALTER TABLE ... ADD PARTITION`.

    Values below the highest existing boundary already fall into an existing partition; only
    values at or beyond it need new partitions appended (in ascending order).
    """
    from meerschaum.utils.sql import sql_item_name

    dt_col = pipe.columns.get('datetime', None)
    if dt_col is None or dt_col not in getattr(df, 'columns', []):
        return True, "No partitions to create."

    series = df[dt_col].dropna()
    if len(series) == 0:
        return True, "No partitions to create."

    max_val = _normalize_boundary(series.max())

    max_bound = self._get_mysql_max_partition_bound(pipe, debug=debug)
    if max_bound is None:
        ### No partitions exist (table created without them); nothing we can extend.
        return True, "No partitions to create."

    interval = pipe.get_chunk_interval(debug=debug)

    ### Normalize the dataframe's max to the same type/timezone as the stored boundary.
    if isinstance(max_bound, datetime):
        if not isinstance(max_val, datetime):
            return True, "No partitions to create."
        if max_val.tzinfo is not None:
            max_val = max_val.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        max_val = int(max_val)

    parent_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))

    queries = []
    cursor = max_bound
    count = 0
    max_parts = _max_partitions_per_sync()
    while cursor <= max_val:
        lo = cursor
        hi = (lo + interval) if isinstance(lo, datetime) else (lo + interval)
        part_name = sql_item_name(self._partition_name(pipe, lo), self.flavor)
        queries.append(
            f"ALTER TABLE {parent_name} ADD PARTITION "
            f"(PARTITION {part_name} VALUES LESS THAN ({self._partition_literal(hi)}))"
        )
        cursor = hi
        count += 1
        if count >= max_parts:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {max_parts}-partition limit for {pipe}; "
                "consider a larger `chunk_minutes`.",
                stack=False,
            )
            break

    if not queries:
        return True, "No partitions to create."

    if debug:
        dprint(f"[{self}] Adding {len(queries)} partition(s) to {pipe}.")

    try:
        success = all(self.exec_queries(
            queries, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
        ))
    except Exception as e:
        return False, f"Failed to add partitions for {pipe}:\n{e}"

    if not success:
        return False, f"Failed to add partitions for {pipe}."
    return True, f"Added {len(queries)} partition(s) to {pipe}."


def _get_partition_boundary_values(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> List[Union[datetime, int]]:
    """
    Return the ascending interval-aligned boundary values (`lo` grid points) spanning a
    dataframe — the split points for an MSSQL `RANGE RIGHT` partition function.
    """
    ranges = self._get_partition_ranges_for_df(pipe, df, debug=debug)
    return [lo for lo, _ in ranges]


def _get_mssql_partition_creation_queries(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> List[str]:
    """
    Build the `CREATE PARTITION FUNCTION` / `CREATE PARTITION SCHEME` queries for a pipe.

    Idempotent (guarded by `sys.partition_functions` / `sys.partition_schemes`); intended to be
    run before the `CREATE TABLE` that places its clustered index on the scheme.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.dtypes.sql import get_db_type_from_pd_type

    dt_col = pipe.columns.get('datetime', None)
    if dt_col is None:
        return []

    func_name = self._partition_function_name(pipe)
    scheme_name = self._partition_scheme_name(pipe)
    func_item = sql_item_name(func_name, self.flavor, None)
    scheme_item = sql_item_name(scheme_name, self.flavor, None)
    clean_func = func_name.replace("'", "''")
    clean_scheme = scheme_name.replace("'", "''")

    dt_db_type = get_db_type_from_pd_type(pipe.dtypes.get(dt_col, 'datetime'), self.flavor)
    boundaries = self._get_partition_boundary_values(pipe, df, debug=debug)
    values_clause = ', '.join(self._partition_literal(b) for b in boundaries)

    return [
        (
            f"IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{clean_func}')\n"
            f"CREATE PARTITION FUNCTION {func_item} ({dt_db_type})\n"
            f"AS RANGE RIGHT FOR VALUES ({values_clause})"
        ),
        (
            f"IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{clean_scheme}')\n"
            f"CREATE PARTITION SCHEME {scheme_item}\n"
            f"AS PARTITION {func_item} ALL TO ([PRIMARY])"
        ),
    ]


def _get_mssql_max_partition_boundary(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Optional[Union[datetime, int]]:
    """
    Return the highest existing boundary of a pipe's MSSQL partition function, or `None`.
    """
    func_name = self._partition_function_name(pipe).replace("'", "''")
    dt_col = pipe.columns.get('datetime', None)
    dt_dtype = str(pipe.dtypes.get(dt_col, 'datetime')) if dt_col else 'datetime'
    is_int_axis = 'int' in dt_dtype.lower()

    cast_type = 'BIGINT' if is_int_axis else 'NVARCHAR(64)'
    query = (
        f"SELECT CONVERT(NVARCHAR(64), MAX(CAST(prv.value AS {('BIGINT' if is_int_axis else 'DATETIMEOFFSET')})), 127) AS b\n"
        "FROM sys.partition_functions pf\n"
        "JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id\n"
        f"WHERE pf.name = '{func_name}'"
    )
    try:
        result = self.value(query, silent=True, debug=debug)
    except Exception:
        return None
    if result is None:
        return None

    text = str(result).strip()
    if is_int_axis:
        try:
            return int(text)
        except Exception:
            return None
    ### Boundaries are interval-aligned (no sub-second component); parse the leading
    ### `YYYY-MM-DDTHH:MM:SS` and treat as UTC.
    try:
        return datetime.fromisoformat(text[:19]).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _create_missing_partitions_mssql(
    self,
    pipe: mrsm.Pipe,
    df: 'Any',
    debug: bool = False,
) -> SuccessTuple:
    """
    Extend a pipe's MSSQL partition function upward via `ALTER PARTITION FUNCTION ... SPLIT RANGE`.

    Each new boundary needs `ALTER PARTITION SCHEME ... NEXT USED` first so the new partition has
    a target filegroup. Values below the highest existing boundary already fall into a partition.
    """
    from meerschaum.utils.sql import sql_item_name

    dt_col = pipe.columns.get('datetime', None)
    if dt_col is None or dt_col not in getattr(df, 'columns', []):
        return True, "No partitions to create."

    series = df[dt_col].dropna()
    if len(series) == 0:
        return True, "No partitions to create."

    max_val = _normalize_boundary(series.max())

    max_bound = self._get_mssql_max_partition_boundary(pipe, debug=debug)
    if max_bound is None:
        return True, "No partitions to create."

    interval = pipe.get_chunk_interval(debug=debug)

    if isinstance(max_bound, datetime):
        if not isinstance(max_val, datetime):
            return True, "No partitions to create."
        if max_val.tzinfo is None:
            max_val = max_val.replace(tzinfo=timezone.utc)
        else:
            max_val = max_val.astimezone(timezone.utc)
    else:
        max_val = int(max_val)

    func_item = sql_item_name(self._partition_function_name(pipe), self.flavor, None)
    scheme_item = sql_item_name(self._partition_scheme_name(pipe), self.flavor, None)

    queries = []
    cursor = max_bound + interval
    count = 0
    max_parts = _max_partitions_per_sync()
    while cursor <= max_val:
        queries.append(f"ALTER PARTITION SCHEME {scheme_item} NEXT USED [PRIMARY]")
        queries.append(
            f"ALTER PARTITION FUNCTION {func_item}() SPLIT RANGE ({self._partition_literal(cursor)})"
        )
        cursor = cursor + interval
        count += 1
        if count >= max_parts:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {max_parts}-partition limit for {pipe}; "
                "consider a larger `chunk_minutes`.",
                stack=False,
            )
            break

    if not queries:
        return True, "No partitions to create."

    if debug:
        dprint(f"[{self}] Splitting {count} partition(s) for {pipe}.")

    try:
        success = all(self.exec_queries(
            queries, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
        ))
    except Exception as e:
        return False, f"Failed to split partitions for {pipe}:\n{e}"

    if not success:
        return False, f"Failed to split partitions for {pipe}."
    return True, f"Split {count} partition(s) for {pipe}."


def _get_partition_cleanup_queries(self, pipe: mrsm.Pipe) -> List[str]:
    """
    Return queries dropping a pipe's MSSQL partition scheme and function (in that order).

    Safe to call unconditionally — returns `[]` for non-MSSQL or non-partitioned pipes. The
    scheme must be dropped before the function it references.
    """
    if self.flavor not in MSSQL_PARTITION_FLAVORS or not self._should_partition(pipe):
        return []
    from meerschaum.utils.sql import sql_item_name
    func_name = self._partition_function_name(pipe)
    scheme_name = self._partition_scheme_name(pipe)
    func_item = sql_item_name(func_name, self.flavor, None)
    scheme_item = sql_item_name(scheme_name, self.flavor, None)
    clean_func = func_name.replace("'", "''")
    clean_scheme = scheme_name.replace("'", "''")
    return [
        (
            f"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{clean_scheme}')\n"
            f"DROP PARTITION SCHEME {scheme_item}"
        ),
        (
            f"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{clean_func}')\n"
            f"DROP PARTITION FUNCTION {func_item}"
        ),
    ]


### TimescaleDB sets a hypertable's chunk interval natively; the change applies to FUTURE chunks
### only (existing chunks keep their size), so no table rewrite is needed.
_TIMESCALEDB_FLAVORS = {'timescaledb', 'timescaledb-ha'}


def partition_pipe(
    self,
    pipe: mrsm.Pipe,
    chunk_minutes: Optional[int] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Rebuild a pipe's target table to a new partition (chunk) width.

    The width is taken from `chunk_minutes` if provided, else the pipe's configured
    `verify.chunk_minutes`. The new width is persisted to `verify.chunk_minutes`, which is the
    authoritative partition width (see `Pipe.get_chunk_interval`).

    Strategy by flavor:

    - **TimescaleDB**: call `set_chunk_time_interval()`. This changes the width of FUTURE chunks
      only; existing chunks are not rewritten.
    - **PostgreSQL / PostGIS, MySQL / MariaDB, MSSQL**: rebuild the table by reading its data,
      dropping it, and re-syncing at the new width. This reuses the tested `create_pipe_table_from_df`
      and `_create_missing_partitions` paths, and (for MSSQL) frees the partition function/scheme
      names so they can be recreated. The whole table is read into memory; for very large tables
      consider a manual chunked rebuild.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The partitioned pipe whose target table to repartition.

    chunk_minutes: Optional[int], default None
        The new partition width in minutes. Defaults to the pipe's `verify.chunk_minutes`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import warn

    flavor = self.flavor
    if flavor not in (PARTITIONABLE_FLAVORS | _TIMESCALEDB_FLAVORS):
        return False, f"Repartitioning is not supported for flavor '{flavor}'."

    is_timescaledb = flavor in _TIMESCALEDB_FLAVORS
    if not is_timescaledb and not self._should_partition(pipe):
        return False, (
            f"{pipe} is not partitioned. Set `hypertable` to `True` (and define a `datetime` "
            "column) to enable native range partitioning."
        )

    if pipe.columns.get('datetime', None) is None:
        return False, f"{pipe} has no `datetime` column to partition by."

    if not pipe.exists(debug=debug):
        return False, f"{pipe} does not exist; nothing to repartition."

    new_minutes = (
        chunk_minutes
        if chunk_minutes is not None
        else (
            pipe.parameters.get('verify', {}).get('chunk_minutes', None)
            or get_config('pipes', 'parameters', 'verify', 'chunk_minutes')
        )
    )
    if not isinstance(new_minutes, int) or new_minutes <= 0:
        return False, f"Invalid chunk interval '{new_minutes}'; must be a positive integer of minutes."

    ### TimescaleDB: native, no rewrite. Future chunks adopt the new interval.
    if is_timescaledb:
        from meerschaum.utils.sql import sql_item_name
        ### `set_chunk_time_interval` takes the hypertable as a `regclass`; pass the
        ### schema-qualified, quoted name as a string literal so it resolves unambiguously.
        pipe_name = sql_item_name(pipe.target, flavor, self.get_pipe_schema(pipe))
        regclass_literal = "'" + pipe_name.replace("'", "''") + "'"
        interval = pipe.get_chunk_interval(new_minutes, debug=debug)
        chunk_time_interval = (
            f"{interval}"
            if isinstance(interval, int)
            else f"INTERVAL '{int(interval.total_seconds() / 60)} MINUTES'"
        )
        query = f"SELECT set_chunk_time_interval({regclass_literal}, {chunk_time_interval})"
        try:
            success = self.exec(query, silent=(not debug), debug=debug) is not None
        except Exception as e:
            return False, f"Failed to set chunk interval for {pipe}:\n{e}"
        if not success:
            return False, f"Failed to set chunk interval for {pipe}."
        pipe.update_parameters(
            {'verify': {'chunk_minutes': new_minutes}}, persist=True, debug=debug
        )
        return True, (
            f"Set chunk interval for {pipe} to {new_minutes} minutes "
            "(applies to future chunks; existing chunks are unchanged)."
        )

    ### Non-TimescaleDB: rebuild via a drop + re-sync round-trip.
    current_interval = pipe.get_chunk_interval(debug=debug)
    new_interval = pipe.get_chunk_interval(new_minutes, debug=debug)
    if current_interval == new_interval:
        return True, f"{pipe} is already partitioned at {new_minutes} minutes."

    rowcount_before = pipe.get_rowcount(debug=debug)

    if debug:
        dprint(f"[{self}] Reading {pipe} data to rebuild partitions at {new_minutes} minutes.")
    df = pipe.get_data(debug=debug)
    if df is None:
        return False, f"Could not read data for {pipe}; aborting repartition."

    ### Persist the new width BEFORE recreating so the rebuild lays partitions at the new size.
    ### `verify.chunk_minutes` is the authoritative partition width.
    update_success, update_msg = pipe.update_parameters(
        {'verify': {'chunk_minutes': new_minutes}},
        persist=True,
        debug=debug,
    )
    if not update_success:
        return False, f"Failed to persist new partition width for {pipe}:\n{update_msg}"

    drop_success, drop_msg = pipe.drop(debug=debug)
    if not drop_success:
        return False, f"Failed to drop {pipe} during repartition:\n{drop_msg}"

    ### Re-sync the data we read; `create_pipe_table_from_df` recreates the table at the new
    ### width and `_create_missing_partitions` populates the partitions.
    sync_success, sync_msg = pipe.sync(df, debug=debug)
    if not sync_success:
        return False, (
            f"Repartition of {pipe} failed during re-sync; the table was dropped and must be "
            f"resynced from its source:\n{sync_msg}"
        )

    rowcount_after = pipe.get_rowcount(debug=debug)
    if (
        rowcount_before is not None
        and rowcount_after is not None
        and rowcount_after != rowcount_before
    ):
        warn(
            f"Row count changed during repartition of {pipe} "
            f"({rowcount_before} -> {rowcount_after}).",
            stack=False,
        )

    return True, f"Repartitioned {pipe} to {new_minutes} minutes."
