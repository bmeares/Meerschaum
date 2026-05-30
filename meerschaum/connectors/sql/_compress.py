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

    if flavor in ('timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus', 'cockroachdb'):
        return _value(f"SELECT pg_total_relation_size('{pipe_name}')")

    if flavor in ('mysql', 'mariadb'):
        db_name = self.database or self.parse_uri(self.URI).get('database', None)
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
    compress_param = pipe.parameters.get('compress', False)
    settings = compress_param if isinstance(compress_param, dict) else {}

    dt_col = pipe.columns.get('datetime', None)
    id_col = pipe.columns.get('id', None)

    segmentby = settings.get('segmentby', None)
    if segmentby is None:
        segmentby = [id_col] if id_col else []
    elif isinstance(segmentby, str):
        segmentby = [segmentby]

    orderby = settings.get('orderby', None)
    if orderby is None:
        orderby = [f"{dt_col} DESC"] if dt_col else []
    elif isinstance(orderby, str):
        orderby = [orderby]

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


def _get_timescaledb_compress_queries(
    self,
    pipe: mrsm.Pipe,
    include_policy: bool = True,
) -> List[str]:
    """
    Build the queries to enable compression (and optionally add a compression policy)
    for a TimescaleDB hypertable.
    """
    from meerschaum.utils.sql import sql_item_name
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    settings = self._get_compress_settings(pipe)

    set_options = ['timescaledb.compress']
    if settings['segmentby']:
        cols = ', '.join(settings['segmentby'])
        set_options.append(f"timescaledb.compress_segmentby = '{cols}'")
    if settings['orderby']:
        cols = ', '.join(settings['orderby'])
        set_options.append(f"timescaledb.compress_orderby = '{cols}'")

    queries = [f"ALTER TABLE {pipe_name} SET ({', '.join(set_options)})"]

    if include_policy:
        after = settings['after']
        if not after:
            ### Default to compressing chunks older than 7 days.
            after = '7 days'
        after_clean = str(after).replace("'", "''")
        queries.append(
            f"SELECT add_compression_policy('{pipe_name}', INTERVAL '{after_clean}', "
            "if_not_exists => true)"
        )

    return queries


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

        queries = self._get_timescaledb_compress_queries(pipe, include_policy=True)
        success = all(self.exec_queries(queries, break_on_error=False, silent=True, debug=debug))
        if not success:
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
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Compress a pipe's target table to reduce disk usage.

    For TimescaleDB, enables native compression, installs a compression policy
    (so future synced chunks are compressed automatically), and compresses any
    existing uncompressed chunks. For MySQL/MariaDB and MSSQL, applies the flavor's
    native table compression. Other flavors are unsupported.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to compress.

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

    queries: List[str] = []
    if flavor in ('timescaledb', 'timescaledb-ha'):
        if not self._is_hypertable(pipe, debug=debug):
            return False, (
                f"{pipe} is not a hypertable; only hypertables support TimescaleDB compression."
            )
        ### Enable compression + install a policy for ongoing automatic compression.
        queries.extend(self._get_timescaledb_compress_queries(pipe, include_policy=True))
        ### Compress any existing uncompressed chunks now.
        queries.append(
            f"SELECT compress_chunk(c, if_not_compressed => true) "
            f"FROM show_chunks('{pipe_name}') c"
        )
    elif flavor in ('mysql', 'mariadb'):
        queries.append(f"ALTER TABLE {pipe_name} ROW_FORMAT=COMPRESSED")
    elif flavor == 'mssql':
        queries.append(
            f"ALTER TABLE {pipe_name} REBUILD PARTITION = ALL "
            "WITH (DATA_COMPRESSION = PAGE)"
        )

    try:
        success = all(self.exec_queries(queries, break_on_error=False, silent=(not debug), debug=debug))
    except Exception as e:
        return False, f"Failed to compress {pipe}:\n{e}"

    if not success:
        return False, f"Failed to compress {pipe}."

    pipe._clear_cache_key('_exists', debug=debug)
    size_after = pipe.get_size(debug=debug)

    reclaimed_msg = ""
    if size_before is not None and size_after is not None:
        reclaimed = size_before - size_after
        change_str = f"{format_bytes(size_before)} → {format_bytes(size_after)}"
        if reclaimed > 0:
            reclaimed_msg = f" Reclaimed {format_bytes(reclaimed)} ({change_str})."
        elif reclaimed < 0:
            ### On small tables, compression overhead can exceed the savings.
            reclaimed_msg = f" Size grew by {format_bytes(-reclaimed)} ({change_str})."
        else:
            reclaimed_msg = f" Size unchanged ({change_str})."

    return True, f"Compressed {pipe}.{reclaimed_msg}"
