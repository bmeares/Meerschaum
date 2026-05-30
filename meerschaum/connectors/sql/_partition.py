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

from datetime import datetime, timedelta, timezone

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

    max_val = series.max()
    if hasattr(max_val, 'to_pydatetime'):
        max_val = max_val.to_pydatetime()

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
        if count >= _MAX_PARTITIONS_PER_SYNC:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {_MAX_PARTITIONS_PER_SYNC}-partition limit for {pipe}; "
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

    max_val = series.max()
    if hasattr(max_val, 'to_pydatetime'):
        max_val = max_val.to_pydatetime()

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
    while cursor <= max_val:
        queries.append(f"ALTER PARTITION SCHEME {scheme_item} NEXT USED [PRIMARY]")
        queries.append(
            f"ALTER PARTITION FUNCTION {func_item}() SPLIT RANGE ({self._partition_literal(cursor)})"
        )
        cursor = cursor + interval
        count += 1
        if count >= _MAX_PARTITIONS_PER_SYNC:
            from meerschaum.utils.warnings import warn
            warn(
                f"Reached the {_MAX_PARTITIONS_PER_SYNC}-partition limit for {pipe}; "
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
