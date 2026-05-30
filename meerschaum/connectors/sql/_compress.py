#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Measure and reduce the disk usage of pipes' target tables.

These methods are mixed into `SQLConnector` (see `meerschaum/connectors/sql/_SQLConnector.py`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Optional, List, Dict, SuccessTuple
from meerschaum.utils.debug import dprint

### Flavors whose tables support some form of compression.
COMPRESSIBLE_FLAVORS = {
    'timescaledb',
    'timescaledb-ha',
    'mysql',
    'mariadb',
    'mssql',
}


### Recognized trailing keywords in an `orderby` spec (e.g. `"ts DESC NULLS LAST"`).
_ORDERBY_DIRECTION_KEYWORDS = frozenset({'ASC', 'DESC', 'NULLS', 'FIRST', 'LAST'})


def _normalize_columnstore_cols(
    value: Union[str, List[str]],
    flavor: str,
    allow_direction: bool = False,
) -> List[str]:
    """
    Normalize a user-supplied `segmentby` / `orderby` spec into a list of quoted column refs.

    Accepts a string (possibly comma-separated, e.g. `"a, b DESC"`), a list of strings, or a
    list whose elements are themselves comma-separated. Each resulting entry is one column,
    optionally followed by a direction (`ASC` / `DESC`, plus `NULLS FIRST` / `NULLS LAST`) when
    `allow_direction` is `True`. Column identifiers are quoted via `sql_item_name`; recognized
    direction keywords are preserved unquoted and upper-cased.

    An entry whose trailing tokens are not all recognized direction keywords is treated as a
    single bare identifier and quoted whole, rather than guessing where the column name ends.
    """
    from meerschaum.utils.sql import sql_item_name
    raw = value if isinstance(value, (list, tuple)) else [value]
    entries = []
    for item in raw:
        entries.extend(part.strip() for part in str(item).split(','))

    normalized = []
    for entry in entries:
        if not entry:
            continue
        col, direction = entry, ''
        if allow_direction:
            tokens = entry.split()
            if len(tokens) > 1 and all(
                tok.upper() in _ORDERBY_DIRECTION_KEYWORDS for tok in tokens[1:]
            ):
                col = tokens[0]
                direction = ' ' + ' '.join(tok.upper() for tok in tokens[1:])
        normalized.append(f"{sql_item_name(col, flavor=flavor)}{direction}")
    return normalized


def get_pipe_size(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[int, None]:
    """
    Return the on-disk size of a pipe's target table in bytes.

    For TimescaleDB hypertables, the total hypertable size (including chunks and indexes)
    is returned. Other flavors use their native size functions where available.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table size to measure.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of bytes occupied by the target table,
    or `None` if the size could not be determined.
    """
    from meerschaum.utils.sql import sql_item_name, hypertable_queries

    if not pipe.exists(debug=debug):
        return None

    flavor = self.flavor
    schema = self.get_pipe_schema(pipe)
    pipe_name = sql_item_name(pipe.target, flavor, schema)

    def _value(query: str) -> Union[int, None]:
        try:
            result = self.value(query, silent=True, debug=debug)
            return int(result) if result is not None else None
        except Exception:
            return None

    ### TimescaleDB / Citus expose dedicated size functions for distributed tables.
    if flavor in hypertable_queries:
        size = _value(hypertable_queries[flavor].format(table_name=pipe_name))
        if size is not None:
            return size

    if flavor in ('timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus'):
        ### `pg_partition_tree` sums the parent plus every child partition (a partitioned parent
        ### holds no rows itself); it returns the single relation for non-partitioned tables.
        size = _value(
            "SELECT SUM(pg_total_relation_size(relid))\n"
            f"FROM pg_partition_tree('{pipe_name}')"
        )
        if size is not None:
            return size
        return _value(f"SELECT pg_total_relation_size('{pipe_name}')")

    if flavor == 'cockroachdb':
        return _value(f"SELECT pg_total_relation_size('{pipe_name}')")

    if flavor in ('mysql', 'mariadb'):
        ### A MySQL/MariaDB "schema" is a database; honor a pipe's configured schema so the size
        ### lookup matches the database the table actually lives in.
        db_name = (
            self.get_pipe_schema(pipe)
            or self.database
            or self.parse_uri(self.URI).get('database', None)
        )
        if not db_name:
            return None
        clean_db = db_name.replace("'", "''")
        clean_target = pipe.target.replace("'", "''")
        return _value(
            "SELECT data_length + index_length\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = '{clean_db}' AND table_name = '{clean_target}'"
        )

    if flavor == 'mssql':
        clean_name = pipe_name.replace("'", "''")
        return _value(
            "SELECT SUM(reserved_page_count) * 8192\n"
            "FROM sys.dm_db_partition_stats\n"
            f"WHERE object_id = OBJECT_ID('{clean_name}')"
        )

    if flavor in ('sqlite', 'geopackage'):
        clean_target = pipe.target.replace("'", "''")
        ### `dbstat` is only available when SQLite is compiled with SQLITE_ENABLE_DBSTAT_VTAB.
        return _value(f"SELECT SUM(pgsize) FROM dbstat WHERE name = '{clean_target}'")

    ### duckdb, oracle, and unknown flavors have no portable per-table size query.
    return None


def _get_compress_settings(
    self,
    pipe: mrsm.Pipe,
) -> Dict[str, Any]:
    """
    Resolve the compression settings (segmentby, orderby, after) for a pipe.

    Reads the `compress` parameter (which may be a `bool` or a `dict`) and falls back
    to the pipe's index columns for sensible TimescaleDB defaults.
    """
    from meerschaum.utils.sql import sql_item_name
    compress_param = pipe.parameters.get('compress', False)
    settings = compress_param if isinstance(compress_param, dict) else {}

    dt_col = pipe.columns.get('datetime', None)
    dt_col_name = sql_item_name(dt_col, flavor=self.flavor) if dt_col else None
    id_col = pipe.columns.get('id', None)
    id_col_name = sql_item_name(id_col, flavor=self.flavor) if id_col else None
    primary_col = pipe.columns.get('primary', None)

    ### A unique / primary `id` column is high-cardinality: segmenting by it yields ≈1 row per
    ### segment, which defeats columnstore batching (no compression, larger table). Such columns
    ### belong in `orderby` instead. See the Hypercore docs:
    ### https://www.tigerdata.com/docs/build/columnar-storage/setup-hypercore
    id_is_unique = bool(id_col) and id_col == primary_col

    segmentby = settings.get('segmentby', None)
    if segmentby is None:
        segmentby = [id_col_name] if (id_col and not id_is_unique) else []
    else:
        segmentby = _normalize_columnstore_cols(segmentby, self.flavor)

    orderby = settings.get('orderby', None)
    if orderby is None:
        orderby = []
        if dt_col:
            orderby.append(f"{dt_col_name} DESC")
        ### Keep the unique `id` in `orderby` (not `segmentby`) so chunks stay well-ordered.
        if id_is_unique and id_col_name not in orderby:
            orderby.append(id_col_name)
    else:
        orderby = _normalize_columnstore_cols(orderby, self.flavor, allow_direction=True)

    after = settings.get('after', None)

    return {
        'segmentby': [c for c in segmentby if c],
        'orderby': [c for c in orderby if c],
        'after': after,
    }


def _is_hypertable(self, pipe: mrsm.Pipe, debug: bool = False) -> bool:
    """Return whether a pipe's target table is a TimescaleDB hypertable."""
    from meerschaum.utils.sql import sql_item_name, hypertable_queries
    if self.flavor not in hypertable_queries:
        return False
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    query = hypertable_queries[self.flavor].format(table_name=pipe_name)
    return self.value(query, silent=True, debug=debug) is not None


def _get_columnstore_settings_query(
    self,
    pipe: mrsm.Pipe,
) -> str:
    """
    Build the `ALTER TABLE` query that enables the Hypercore columnstore and configures its
    `segmentby` / `orderby` for a TimescaleDB hypertable.

    NOTE: this must be committed in its own transaction *before* `add_columnstore_policy()`;
    enabling the columnstore and adding the policy in the same transaction raises
    "columnstore not enabled" (see timescale/timescaledb#8600).
    """
    from meerschaum.utils.sql import sql_item_name
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    settings = self._get_compress_settings(pipe)

    ### `enable_columnstore` is the Hypercore replacement for the legacy `timescaledb.compress`
    ### (which still works as an alias). `segmentby`/`orderby` likewise supersede
    ### `compress_segmentby`/`compress_orderby`.
    set_options = ['timescaledb.enable_columnstore']
    if settings['segmentby']:
        cols = ', '.join(settings['segmentby']).replace("'", "''")
        set_options.append(f"timescaledb.segmentby = '{cols}'")
    if settings['orderby']:
        cols = ', '.join(settings['orderby']).replace("'", "''")
        set_options.append(f"timescaledb.orderby = '{cols}'")

    return f"ALTER TABLE {pipe_name} SET ({', '.join(set_options)})"


def _get_columnstore_policy_query(
    self,
    pipe: mrsm.Pipe,
) -> str:
    """
    Build the `CALL add_columnstore_policy(...)` query that schedules automatic conversion of
    old chunks to the columnstore for a TimescaleDB hypertable.

    `add_columnstore_policy` is the Hypercore replacement for `add_compression_policy` (same
    underlying mechanism — the "columnstore policy" *is* the "compression policy"). Run this in a
    separate transaction from `_get_columnstore_settings_query` (see timescale/timescaledb#8600).
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.dtypes import are_dtypes_equal, MRSM_PRECISION_UNITS_SCALARS
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    settings = self._get_compress_settings(pipe)

    ### Hypertables with an integer time dimension (int epoch datetime axis) require an integer
    ### `after`; an `INTERVAL` raises `InvalidParameterValue`.
    dt_col = pipe.columns.get('datetime', None)
    dt_typ = str(pipe.dtypes.get(dt_col, 'datetime')) if dt_col else 'datetime'
    dt_is_integer = are_dtypes_equal(dt_typ, 'int')

    after = settings['after']
    if dt_is_integer:
        ### Honor an explicit integer, otherwise derive a 7-day-equivalent offset from the axis
        ### precision (e.g. 604800 for second precision, 604800000 for millisecond).
        if isinstance(after, int) and not isinstance(after, bool):
            after_clause = str(after)
        else:
            unit = pipe.precision.get('unit', 'second')
            scalar = MRSM_PRECISION_UNITS_SCALARS.get(unit, 1)
            ### 7 days = 604800 seconds; scalar is units-per-second.
            after_clause = str(int(604800 * scalar))
    else:
        if not after:
            ### Default to converting chunks older than 7 days.
            after = '7 days'
        after_clean = str(after).replace("'", "''")
        after_clause = f"INTERVAL '{after_clean}'"

    return (
        f"CALL add_columnstore_policy('{pipe_name}', after => {after_clause}, "
        "if_not_exists => true)"
    )


def _get_columnstore_remove_policy_query(
    self,
    pipe: mrsm.Pipe,
) -> str:
    """
    Build the `CALL remove_columnstore_policy(...)` query.

    Used so the explicit `compress pipes` action can re-create the policy with the configured
    `after` — `add_columnstore_policy(..., if_not_exists => true)` will NOT update an existing
    policy (e.g. the one TimescaleDB auto-creates for Hypercore tables at `CREATE TABLE`).
    """
    from meerschaum.utils.sql import sql_item_name
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    return f"CALL remove_columnstore_policy('{pipe_name}', if_exists => true)"


def apply_compression_policy(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Idempotently enable compression and install a compression policy for a pipe.

    Intended to be called automatically (e.g. after a sync) when `pipe.compress` is set.
    Only TimescaleDB hypertables are affected; all other flavors are a no-op success.
    Failures are non-fatal and never raise.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table should have a compression policy.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if self.flavor not in ('timescaledb', 'timescaledb-ha'):
        return True, "Compression policies are only supported for TimescaleDB."

    if not pipe.parameters.get('compress', False):
        return True, "Compression is not enabled for this pipe."

    try:
        if not pipe.exists(debug=debug) or not self._is_hypertable(pipe, debug=debug):
            return True, f"{pipe} is not a hypertable; skipping compression policy."

        ### Enable the columnstore and add the policy in SEPARATE transactions
        ### (see timescale/timescaledb#8600).
        settings_success = all(self.exec_queries(
            [self._get_columnstore_settings_query(pipe)],
            break_on_error=True, rollback=True, silent=True, debug=debug,
        ))
        policy_success = all(self.exec_queries(
            [self._get_columnstore_policy_query(pipe)],
            break_on_error=True, rollback=True, silent=True, debug=debug,
        ))
        if not (settings_success and policy_success):
            return False, f"Failed to apply a compression policy to {pipe}."
    except Exception as e:
        msg = f"Failed to apply a compression policy to {pipe}:\n{e}"
        if debug:
            dprint(msg)
        return False, msg

    return True, f"Applied a compression policy to {pipe}."


def compress_pipe(
    self,
    pipe: mrsm.Pipe,
    no_policy: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Compress a pipe's target table to reduce disk usage.

    For TimescaleDB, enables the Hypercore columnstore, installs a columnstore (compression)
    policy (so future synced chunks are converted automatically), and converts any existing
    uncompressed chunks now. For MySQL/MariaDB and MSSQL, applies the flavor's native table
    compression. Other flavors are unsupported.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to compress.

    no_policy: bool, default False
        If `True` (TimescaleDB only), compress existing chunks now without installing an ongoing
        columnstore (compression) policy. Any pre-existing policy is left untouched.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success, including the amount of disk reclaimed.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.formatting import format_bytes

    if not pipe.exists(debug=debug):
        return False, f"{pipe} does not exist; nothing to compress."

    flavor = self.flavor
    if flavor not in COMPRESSIBLE_FLAVORS:
        return False, f"Compression is not supported for flavor '{flavor}'."

    pipe_name = sql_item_name(pipe.target, flavor, self.get_pipe_schema(pipe))
    size_before = pipe.get_size(debug=debug)

    ### Each group is run in its own transaction. TimescaleDB requires enabling the columnstore
    ### and adding its policy in separate transactions (see timescale/timescaledb#8600).
    query_groups: List[List[str]] = []
    if flavor in ('timescaledb', 'timescaledb-ha'):
        if not self._is_hypertable(pipe, debug=debug):
            return False, (
                f"{pipe} is not a hypertable; only hypertables support TimescaleDB compression."
            )
        ### 1. Enable the columnstore (required before any chunk can be converted).
        query_groups.append([self._get_columnstore_settings_query(pipe)])
        ### 2. Install a policy for ongoing conversion — re-create it so the configured `after`
        ### wins over any existing (e.g. auto-created) policy. Skipped entirely with `no_policy`,
        ### which compresses existing chunks now but leaves any pre-existing policy untouched.
        if not no_policy:
            query_groups.append([self._get_columnstore_remove_policy_query(pipe)])
            query_groups.append([self._get_columnstore_policy_query(pipe)])
        ### 3. Convert any existing uncompressed chunks now. `compress_chunk` is the still-supported
        ### function form of `convert_to_columnstore` (transaction-safe, unlike the `CALL` form).
        query_groups.append([
            f"SELECT compress_chunk(c, if_not_compressed => true) "
            f"FROM show_chunks('{pipe_name}') c"
        ])
    elif flavor in ('mysql', 'mariadb'):
        query_groups.append([f"ALTER TABLE {pipe_name} ROW_FORMAT=COMPRESSED"])
    elif flavor == 'mssql':
        query_groups.append([
            f"ALTER TABLE {pipe_name} REBUILD PARTITION = ALL "
            "WITH (DATA_COMPRESSION = PAGE)"
        ])

    try:
        success = all(
            all(self.exec_queries(
                group, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
            ))
            for group in query_groups
        )
    except Exception as e:
        return False, f"Failed to compress {pipe}:\n{e}"

    if not success:
        return False, f"Failed to compress {pipe}."

    pipe._clear_cache_key('_exists', debug=debug)
    size_after = pipe.get_size(debug=debug)

    reclaimed_msg = ""
    if size_before is not None and size_after is not None:
        reclaimed = size_before - size_after
        change_str = f"{format_bytes(size_before)} to {format_bytes(size_after)}"
        if reclaimed > 0:
            reclaimed_msg = f"Reclaimed {format_bytes(reclaimed)} ({change_str})."
        elif reclaimed < 0:
            ### On small tables, compression overhead can exceed the savings.
            reclaimed_msg = f"Size grew by {format_bytes(-reclaimed)} ({change_str})."
        else:
            reclaimed_msg = f"Size unchanged ({format_bytes(size_before)})."

    return True, reclaimed_msg


def _get_columnstore_disable_query(
    self,
    pipe: mrsm.Pipe,
) -> str:
    """
    Build the `ALTER TABLE` query that disables the Hypercore columnstore for a TimescaleDB
    hypertable. Only valid once every chunk has been decompressed (no compressed chunks may
    remain) and the columnstore policy has been removed.
    """
    from meerschaum.utils.sql import sql_item_name
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    return f"ALTER TABLE {pipe_name} SET (timescaledb.enable_columnstore = false)"


def decompress_pipe(
    self,
    pipe: mrsm.Pipe,
    no_policy: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Decompress a pipe's target table, the inverse of `compress_pipe()`.

    For TimescaleDB, removes the columnstore (compression) policy, converts every compressed
    chunk back to row-store, and disables the columnstore so future synced chunks stay
    uncompressed. For MySQL/MariaDB and MSSQL, reverts the flavor's native table compression.
    Other flavors are unsupported.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to decompress.

    no_policy: bool, default False
        If `True` (TimescaleDB only), decompress existing chunks now but leave the columnstore
        (compression) policy in place — chunks will be recompressed on the policy's schedule.
        Useful to temporarily decompress for a bulk backfill without disabling compression.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success, including the change in disk size.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.formatting import format_bytes

    if not pipe.exists(debug=debug):
        return False, f"{pipe} does not exist; nothing to decompress."

    flavor = self.flavor
    if flavor not in COMPRESSIBLE_FLAVORS:
        return False, f"Decompression is not supported for flavor '{flavor}'."

    pipe_name = sql_item_name(pipe.target, flavor, self.get_pipe_schema(pipe))
    size_before = pipe.get_size(debug=debug)

    ### Each group is run in its own transaction.
    query_groups: List[List[str]] = []
    if flavor in ('timescaledb', 'timescaledb-ha'):
        if not self._is_hypertable(pipe, debug=debug):
            return False, (
                f"{pipe} is not a hypertable; only hypertables support TimescaleDB compression."
            )
        ### 1. Remove the ongoing policy so chunks aren't recompressed. Skipped with `no_policy`,
        ### which decompresses existing chunks now but leaves the policy (e.g. for a backfill).
        if not no_policy:
            query_groups.append([self._get_columnstore_remove_policy_query(pipe)])
        ### 2. Convert every compressed chunk back to row-store. `decompress_chunk` is the function
        ### form (transaction-safe, unlike the `CALL` form of `convert_to_rowstore`).
        query_groups.append([
            f"SELECT decompress_chunk(c, if_compressed => true) "
            f"FROM show_chunks('{pipe_name}') c"
        ])
        ### 3. Disable the columnstore so future synced chunks stay uncompressed. Only valid once
        ### no compressed chunks remain, and only sensible when the policy is also gone.
        if not no_policy:
            query_groups.append([self._get_columnstore_disable_query(pipe)])
    elif flavor in ('mysql', 'mariadb'):
        query_groups.append([f"ALTER TABLE {pipe_name} ROW_FORMAT=DYNAMIC"])
    elif flavor == 'mssql':
        query_groups.append([
            f"ALTER TABLE {pipe_name} REBUILD PARTITION = ALL "
            "WITH (DATA_COMPRESSION = NONE)"
        ])

    try:
        success = all(
            all(self.exec_queries(
                group, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
            ))
            for group in query_groups
        )
    except Exception as e:
        return False, f"Failed to decompress {pipe}:\n{e}"

    if not success:
        return False, f"Failed to decompress {pipe}."

    pipe._clear_cache_key('_exists', debug=debug)
    size_after = pipe.get_size(debug=debug)

    change_msg = ""
    if size_before is not None and size_after is not None:
        added = size_after - size_before
        change_str = f"{format_bytes(size_before)} to {format_bytes(size_after)}"
        if added > 0:
            change_msg = f"Expanded by {format_bytes(added)} ({change_str})."
        elif added < 0:
            change_msg = f"Shrank by {format_bytes(-added)} ({change_str})."
        else:
            change_msg = f"Size unchanged ({format_bytes(size_before)})."

    return True, change_msg
