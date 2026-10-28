#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Retrieve Pipes' data from instances.
"""

from __future__ import annotations
from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Optional, Dict, Any, Union, List, Tuple, Iterator, TYPE_CHECKING,
)

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')
    pl = mrsm.attempt_import('polars')


def _iterate_chunks(chunks: Iterator[Any]) -> Iterator[Any]:
    """
    The chunks `_core.pipe_get_data_chunks` reads one at a time, as a generator
    (what `Pipe.get_data(as_iterator=True)` returns).
    """
    yield from chunks


def _silent_chunks(chunks: Iterator[Any]) -> Iterator[Any]:
    """`chunks`, ending quietly at a read error (`get_data(as_iterator=True, silent=True)`)."""
    try:
        yield from chunks
    except Exception:
        return


def _wants_polars(
    as_docs: bool = False,
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
) -> bool:
    """
    Whether a read returns Polars frames (`meerschaum:dataframe`, or the call's `as_polars` /
    `as_pandas`), after checking the library is installed (`ImportError` naming the extra).
    Documents (`as_docs`) need neither library.
    """
    if as_docs:
        return False
    from meerschaum.utils.dataframe import resolve_dataframe_library, ensure_dataframe_library
    library = resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas)
    ensure_dataframe_library(library)
    return library == 'polars'


def _rust_context(
    self,
    debug: bool = False,
    refresh: bool = False,
) -> Dict[str, Any]:
    """
    Return the `context` the `_core.pipe_*` bindings take (the instance's Rust SQL connector, the
    other Rust connectors of the call, the attributes of the connectors this process defined ad
    hoc, and the SQL pipe's cached columns types).

    An instance that cannot be parsed raises as in v4 (`Unable to parse connector keys ...`); a
    SQL connector with no usable Rust driver (an unknown flavor, a missing Oracle client library)
    raises `RuntimeError`.
    """
    from meerschaum.core.Pipe._sync import _native_context
    context, reason = _native_context(self, debug=debug)
    if context is None:
        raise RuntimeError(f"Cannot use the instance of {self}: {reason}.")
    ### A SQL pipe's columns types are cached on the Python pipe (`get_columns_types()`), which
    ### saves the native read from asking the database's catalog on every call.
    context['columns_types'] = (
        (self.get_columns_types(refresh=refresh, debug=debug) or None)
        if context['connector'] is not None
        else None
    )
    return context


def get_data(
    self,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, str, None] = None,
    end: Union[datetime, int, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    as_docs: bool = False,
    as_iterator: bool = False,
    as_chunks: bool = False,
    as_dask: bool = False,
    as_polars: Optional[bool] = None,
    add_missing_columns: bool = False,
    chunk_interval: Union[timedelta, int, None] = None,
    order: Optional[str] = 'asc',
    limit: Optional[int] = None,
    fresh: bool = False,
    silent: bool = False,
    debug: bool = False,
    as_pandas: Optional[bool] = None,
    **kw: Any
) -> Union['pl.DataFrame', 'pd.DataFrame', Iterator[Any], List[Dict[str, Any]], None]:
    """
    Get a pipe's data from the instance connector.

    Parameters
    ----------
    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, str, None], default None
        Lower bound datetime to begin searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime >= begin`.
        Defaults to `None`.

    end: Union[datetime, int, str, None], default None
        Upper bound datetime to stop searching for data (exclusive).
        Translates to a `WHERE` clause like `WHERE datetime < end`.
        Defaults to `None`.

    params: Optional[Dict[str, Any]], default None
        Filter the rows by column values (a `WHERE` clause), e.g.
        `params={'station': 'KATL'}`:

        - a value is `=`: `{'station': 'KATL'}`;
        - a list is `IN`: `{'station': ['KATL', 'KGMU']}`;
        - a leading `_` negates: `{'station': '_KATL'}` is `!=`, `{'station': ['_KATL']}`
          is `NOT IN`;
        - `None` (or `'None'`) is `IS NULL` and `'_None'` is `IS NOT NULL`;
        - a dictionary value is compared as JSON text.

        See `meerschaum.utils.sql.build_where` for more details.

    as_docs: bool, default False
        If `True`, return a list of dictionaries rather than a DataFrame.
        Relies on `get_pipe_docs` from the instance connector if implemented.
        May be combined with `as_chunks` to return an `Iterator[List[Dict]]`
        chunked by time bounds (useful for large result sets without pandas overhead).

    as_iterator: bool, default False
        If `True`, return a generator of chunks of pipe data.
        When combined with `as_docs=True`, yields `List[Dict]` per chunk instead of DataFrames.

    as_chunks: bool, default False
        Alias for `as_iterator`.
        When combined with `as_docs=True`, yields `List[Dict]` per chunk instead of DataFrames.

    as_dask: bool, default False
        Not supported in Meerschaum 5: `True` raises a `TypeError`.
        Read the data in chunks with `as_iterator=True` (or as one Polars frame) instead.

    as_polars: Optional[bool], default None
        `True` returns a `polars.DataFrame` and `False` a pandas one, whatever the
        `meerschaum:dataframe` setting says (see `as_pandas`). Chunks (`as_iterator=True`)
        follow it too. This may not be combined with `as_docs`.

    add_missing_columns: bool, default False
        If `True`, add any missing columns from `Pipe.dtypes` to the dataframe.

    chunk_interval: Union[timedelta, int, None], default None
        If `as_iterator`, then return chunks with `begin` and `end` separated by this interval.
        This may be set under `pipe.parameters['chunk_minutes']`.
        By default, use a timedelta of 43200 minutes (30 days).
        If `chunk_interval` is an integer and the `datetime` axis a timestamp,
        then use a timedelta with the number of minutes configured to this value.
        If the `datetime` axis is an integer, an `int` is used verbatim as the number of
        epoch units, and a `timedelta` is scaled into epoch units by the pipe's `precision`.

    order: Optional[str], default 'asc'
        If `order` is not `None`, sort the resulting dataframe by indices.

    limit: Optional[int], default None
        If provided, cap the dataframe to this many rows.

    fresh: bool, default False
        If `True`, skip local cache and directly query the instance connector.

    silent: bool, default False
        If `True`, return `None` instead of raising when the instance cannot be read
        (Meerschaum 4's behavior). By default, database errors raise; a pipe whose table
        does not exist yet returns `None` either way.

    debug: bool, default False
        Verbosity toggle.
        Defaults to `False`.

    as_pandas: Optional[bool], default None
        `True` returns a pandas DataFrame (a `geopandas.GeoDataFrame` when the pipe has
        geometry columns) and `False` a Polars one. By default (`None`) the frame is the
        library of the `meerschaum:dataframe` setting (`MRSM_DATAFRAME` overrides it):
        Polars for a new root, pandas for a root created before Meerschaum 5.0.0rc15.
        Polars frames carry geometry as WKB (binary) with the CRS in the field metadata.

    kw: Any
        Other keyword arguments are passed on to the instance connector (a SQL instance
        reads `begin_add_minutes`, `end_add_minutes`, `replace_nulls` and
        `skip_existing_cols_check`; a custom instance connector may read its own). A keyword
        that nothing reads is ignored with a warning that suggests the parameter you probably
        meant (`start` → `begin`, `where` or `filters` → `params`).

    Returns
    -------
    A DataFrame of the pipe's data: Polars or pandas, per `meerschaum:dataframe`
    (or `as_polars` / `as_pandas`).
    A `List[Dict]` if `as_docs=True`.
    An iterator of DataFrames if `as_chunks=True` (or `as_iterator=True`).
    An `Iterator[List[Dict]]` if both `as_docs=True` and `as_chunks=True`.
    `None` if the pipe's table does not exist yet.

    """
    from meerschaum import _core
    from meerschaum.core.Pipe._keywords import warn_unknown_get_data_keywords
    warn_unknown_get_data_keywords(self, kw, as_docs=as_docs)
    if as_polars and (as_docs or as_dask):
        raise ValueError("`as_polars` may not be combined with `as_docs` or `as_dask`.")
    if as_pandas and (as_docs or as_dask):
        raise ValueError("`as_pandas` may not be combined with `as_docs` or `as_dask`.")
    if as_dask:
        raise TypeError(
            "Dask is not supported in Meerschaum 5. Use `as_iterator=True` for chunks "
            "or `as_polars=True`. See the migration guide: reference/migrating-to-v5."
        )
    polars = _wants_polars(as_docs, as_polars, as_pandas)
    try:
        context = _rust_context(self, debug=debug)
        begin, end = self.parse_date_bounds(begin, end, debug=debug)
        if as_iterator or as_chunks:
            chunks = _iterate_chunks(_core.pipe_get_data_chunks(
                self,
                context,
                select_columns=select_columns,
                omit_columns=omit_columns,
                begin=begin,
                end=end,
                params=params,
                as_docs=as_docs,
                as_polars=polars,
                chunk_interval=chunk_interval,
                order=order,
                limit=limit,
                kwargs=kw,
                debug=debug,
            ))
            return _silent_chunks(chunks) if silent else chunks
        return _core.pipe_get_data(
            self,
            context,
            select_columns=select_columns,
            omit_columns=omit_columns,
            begin=begin,
            end=end,
            params=params,
            as_docs=as_docs,
            as_polars=polars,
            add_missing_columns=add_missing_columns,
            order=order,
            limit=limit,
            kwargs=kw,
            debug=debug,
        )
    except Exception:
        ### Database errors raise; `silent=True` keeps Meerschaum 4's `None`.
        if silent:
            return None
        raise


def get_backtrack_data(
    self,
    backtrack_minutes: Optional[int] = None,
    begin: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    fresh: bool = False,
    debug: bool = False,
    **kw: Any
) -> Optional['pd.DataFrame']:
    """
    Get the most recent data from the instance connector as a Pandas DataFrame.

    Parameters
    ----------
    backtrack_minutes: Optional[int], default None
        How many minutes from `begin` to select from.
        If `None`, use `pipe.parameters['fetch']['backtrack_minutes']`.

    begin: Optional[datetime], default None
        The starting point to search for data.
        If begin is `None` (default), use the most recent observed datetime
        (AKA sync_time).

        ```
        E.g. begin = 02:00

        Search this region.           Ignore this, even if there's data.
        /  /  /  /  /  /  /  /  /  |
        -----|----------|----------|----------|----------|----------|
        00:00      01:00      02:00      03:00      04:00      05:00

        ```

    params: Optional[Dict[str, Any]], default None
        The standard Meerschaum `params` query dictionary.

    limit: Optional[int], default None
        If provided, cap the number of rows to be returned.

    fresh: bool, default False
        If `True`, Ignore local cache and pull directly from the instance connector.
        Only comes into effect if a pipe was created with `cache=True`.

    debug: bool default False
        Verbosity toggle.

    **kw: Any
        Also `end` (exclusive), `select_columns`, `omit_columns`, `order` (default `'desc'`),
        `as_docs`, `as_polars`, `as_pandas` and `add_missing_columns`, as in `get_data()`.
        Other keywords go to the instance connector; a keyword nothing reads is ignored with a
        warning that suggests the parameter you probably meant.

    Returns
    -------
    A DataFrame (Polars or pandas, per `meerschaum:dataframe`, as `get_data()`) of the pipe's
    data corresponding to the provided parameters. Backtrack data is a convenient way to get a
    pipe's data "backtracked" from the most recent datetime.
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._keywords import BACKTRACK_KEYWORDS, warn_unknown_backtrack_keywords
    warn_unknown_backtrack_keywords(self, kw)
    polars = _wants_polars(
        kw.get('as_docs', False),
        kw.get('as_polars', None),
        kw.get('as_pandas', None),
    )
    context = _rust_context(self, debug=debug)
    named = BACKTRACK_KEYWORDS
    end = kw.get('end', None)
    return _core.pipe_get_backtrack_data(
        self,
        context,
        backtrack_minutes=backtrack_minutes,
        begin=self.parse_date_bounds(begin, debug=debug),
        params=params,
        limit=limit,
        order=kw.get('order', 'desc') or 'desc',
        select_columns=kw.get('select_columns', None),
        omit_columns=kw.get('omit_columns', None),
        end=self.parse_date_bounds(end, debug=debug) if end is not None else None,
        as_docs=kw.get('as_docs', False),
        as_polars=polars,
        add_missing_columns=kw.get('add_missing_columns', False),
        kwargs={key: val for key, val in kw.items() if key not in named},
        debug=debug,
    )


def get_rowcount(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    estimate: bool = False,
    silent: bool = False,
    debug: bool = False
) -> int:
    """
    Get a Pipe's instance or remote rowcount.

    Parameters
    ----------
    begin: Optional[datetime], default None
        Count rows where datetime >= begin (inclusive).

    end: Optional[datetime], default None
        Count rows where datetime < end (exclusive).

    remote: bool, default False
        Count rows from a pipe's remote source.
        **NOTE**: This is experimental!

    estimate: bool, default False
        If `True`, return the database's estimate of the number of rows from its catalog
        statistics (PostgreSQL, TimescaleDB, MySQL / MariaDB, MSSQL, Oracle) instead of
        counting them: instant on large tables, and close (the statistics trail recent
        writes). The count is exact where the database has no estimate (SQLite, DuckDB), for
        small tables, and with `begin`, `end`, `params` or `remote`.

    silent: bool, default False
        If `True`, return 0 instead of raising when the instance cannot be read
        (Meerschaum 4's behavior).

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of rows in the pipe corresponding to the provided parameters.
    Returned 0 if the pipe does not exist. Raises the database error if the instance cannot
    be read (0 with `silent=True`).
    """
    from meerschaum import _core
    try:
        context = _rust_context(self, debug=debug)
        begin, end = self.parse_date_bounds(begin, end, debug=debug)
        return _core.pipe_get_rowcount(
            self,
            context,
            begin=begin,
            end=end,
            params=params,
            remote=remote,
            estimate=estimate,
            debug=debug,
        )
    except Exception:
        if silent:
            return 0
        raise


def get_size(
    self,
    debug: bool = False,
    **kw: Any
) -> Union[int, None]:
    """
    Return the on-disk size of the pipe's target table in bytes.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of bytes occupied by the pipe's target table,
    or `None` if the size could not be determined (e.g. the connector does
    not implement `get_pipe_size()` or the table does not exist).
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_keywords

    connector = self.instance_connector
    try:
        with Venv(get_connector_plugin(connector)):
            if not hasattr(connector, 'get_pipe_size'):
                return None
            kwargs = filter_keywords(
                connector.get_pipe_size,
                debug=debug,
                **kw
            )
            return connector.get_pipe_size(self, **kwargs)
    except NotImplementedError:
        return None
    except Exception as e:
        warn(f"Failed to get the size of {self}:\n{e}", stack=False)
    return None


def get_chunk_interval(
    self,
    chunk_interval: Union[timedelta, int, None] = None,
    debug: bool = False,
) -> Union[timedelta, int]:
    """
    Get the chunk interval to use for this pipe.

    The size is read from the `verify` parameters. Any one of these aliased keys may be used
    (the first present, in this priority order, wins):

        - `verify.chunk_minutes` (the default; 43200 — 30 days — if none is set)
        - `verify.chunk_hours`
        - `verify.chunk_days`
        - `verify.chunk_weeks`
        - `verify.chunk_years`
        - `verify.chunk_seconds`

    For an integer datetime axis, `verify.chunk_range` (if set) is used verbatim as the chunk size
    in epoch units. Otherwise the time-based size above is converted to epoch units via the pipe's
    `precision`, or — preserving legacy behavior when no `precision` is set — its minutes are used
    verbatim.

    Parameters
    ----------
    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this value instead of the `verify` keys.
        An `int` is already in the axis's own units — epoch units for an integer axis,
        minutes otherwise — and is used verbatim. A `timedelta` is a duration, so on an
        integer axis it is scaled into epoch units by the pipe's `precision`.

    Returns
    -------
    The chunk interval (`timedelta` or `int`) to use with this pipe's `datetime` axis.
    """
    from meerschaum import _core
    return _core.get_chunk_interval(self, chunk_interval, self.dtypes)


def get_chunk_bounds(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    bounded: bool = False,
    chunk_interval: Union[timedelta, int, None] = None,
    align: bool = False,
    debug: bool = False,
) -> List[
    Tuple[
        Union[datetime, int, None],
        Union[datetime, int, None],
    ]
]:
    """
    Return a list of datetime bounds for iterating over the pipe's `datetime` axis.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None
        If provided, do not select less than this value.
        Otherwise the first chunk will be unbounded.

    end: Union[datetime, int, None], default None
        If provided, do not select greater than or equal to this value.
        Otherwise the last chunk will be unbounded.

    bounded: bool, default False
        If `True`, do not include `None` in the first chunk.

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this interval for the size of chunk boundaries.
        The default value for this pipe may be set
        under `pipe.parameters['verify']['chunk_minutes']`.

    align: bool, default False
        If `True`, anchor the interior chunk boundaries to a fixed Unix-epoch grid (the same
        grid used for native range partitioning) rather than to `begin`. This makes the
        boundaries deterministic across re-syncs and aligned with the pipe's partitions
        (used by `Pipe.verify()`). The first chunk's lower bound and the last chunk's upper
        bound are still clamped to `begin` / `end`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of chunk bounds (datetimes or integers).
    If unbounded, the first and last chunks will include `None`.
    """
    from meerschaum import _core
    ### The sync times stand in for an unset `begin` / `end` (v4 queries them the same way).
    oldest = self.get_sync_time(newest=False, debug=debug) if begin is None else None
    newest = self.get_sync_time(newest=True, debug=debug) if end is None else None
    return _core.get_chunk_bounds(
        self.parse_date_bounds(begin, debug=debug),
        self.parse_date_bounds(end, debug=debug),
        bounded,
        self.get_chunk_interval(chunk_interval, debug=debug),
        align,
        self.parse_date_bounds(oldest, debug=debug),
        self.parse_date_bounds(newest, debug=debug),
    )


def get_chunk_bounds_batches(
    self,
    chunk_bounds: List[Tuple[Union[datetime, int, None], Union[datetime, int, None]]],
    batchsize: Optional[int] = None,
    workers: Optional[int] = None,
    debug: bool = False,
) -> List[
    Tuple[
        Tuple[
            Union[datetime, int, None],
            Union[datetime, int, None],
        ], ...
    ]
]:
    """
    Return a list of tuples of chunk bounds of size `batchsize`.

    Parameters
    ----------
    chunk_bounds: List[Tuple[Union[datetime, int, None], Union[datetime, int, None]]]
        A list of chunk_bounds (see `Pipe.get_chunk_bounds()`).

    batchsize: Optional[int], default None
        How many chunks to include in a batch. Defaults to `Pipe.get_num_workers()`.

    workers: Optional[int], default None
        If `batchsize` is `None`, use this as the desired number of workers.
        Passed to `Pipe.get_num_workers()`.

    Returns
    -------
    A list of tuples of chunk bound tuples.
    """
    from meerschaum.utils.misc import iterate_chunks
    
    if batchsize is None:
        batchsize = self.get_num_workers(workers=workers)

    return [
        tuple(
            _batch_chunk_bounds
            for _batch_chunk_bounds in batch
            if _batch_chunk_bounds is not None
        )
        for batch in iterate_chunks(chunk_bounds, batchsize)
        if batch
    ]


def parse_date_bounds(self, *dt_vals: Union[datetime, int, None], debug: bool = False) -> Union[
    datetime,
    int,
    str,
    None,
    Tuple[Union[datetime, int, str, None]]
]:
    """
    Given a date bound (begin, end), coerce a timezone if necessary.
    """
    from meerschaum.utils.misc import is_int
    from meerschaum.utils.dtypes import coerce_timezone, MRSM_PD_DTYPES, are_dtypes_equal
    from meerschaum.utils.warnings import warn

    _columns = None
    _dtypes = None

    def _get_coercion_info():
        nonlocal _columns, _dtypes
        if _columns is None:
            _columns = self.get_parameters(debug=debug).get('columns', {}) or {}
        if _dtypes is None:
            _dtypes = self.get_dtypes(debug=debug)

    def _parse_date_bound(dt_val):
        if dt_val is None:
            return None

        if isinstance(dt_val, int):
            return dt_val

        if dt_val == '':
            return ''

        if is_int(dt_val):
            return int(dt_val)

        if isinstance(dt_val, str):
            ### Only a text bound needs `dateutil` (a missing package is not installed
            ### for the integer, `None` and datetime bounds that never reach here).
            dateutil_parser = mrsm.attempt_import('dateutil.parser')
            try:
                dt_val = dateutil_parser.parse(dt_val)
            except Exception as e:
                warn(f"Could not parse '{dt_val}' as datetime:\n{e}")
                return None

        _get_coercion_info()
        dt_col = _columns.get('datetime', None)
        dt_typ = str(_dtypes.get(dt_col, 'datetime'))
        if are_dtypes_equal(dt_typ, 'int'):
            if self.get_parameters(debug=debug).get('precision'):
                from meerschaum.utils.dtypes import datetime_to_int
                return datetime_to_int(dt_val, self.precision['unit'])
            from meerschaum.utils.warnings import error
            error(
                f"Cannot use datetime bound '{dt_val}' on the non-epoch integer axis "
                f"of {self}.\n    Pass an integer instead, or set the `precision` parameter.",
                ValueError,
            )
        if dt_typ == 'datetime':
            dt_typ = MRSM_PD_DTYPES['datetime']
        return coerce_timezone(dt_val, strip_utc=('utc' not in dt_typ.lower()))

    bounds = tuple(_parse_date_bound(dt_val) for dt_val in dt_vals)
    if len(bounds) == 1:
        return bounds[0]
    return bounds


def get_doc(self, **kwargs) -> Union[Dict[str, Any], None]:
    """
    Convenience function to return a single row as a dictionary (or `None`) from `Pipe.get_data().
    Keywords arguments are passed to `Pipe.get_data()`.
    """
    from meerschaum.utils.warnings import warn
    kwargs['limit'] = 1
    kwargs['as_docs'] = True
    try:
        docs = self.get_data(**kwargs)
        if not docs:
            return None
        return docs[0]
    except Exception as e:
        warn(f"Failed to read value from {self}:\n{e}", stack=False)
        return None

def get_docs(self, **kwargs) -> list[dict[str, Any]]:
    """
    Convenience method to return a pipe's data as a list of dictionaries.
    Relies on `get_pipe_docs` from the instance connector if implemented.
    """
    kwargs['as_docs'] = True
    return self.get_data(**kwargs)

def get_value(
    self,
    column: str,
    params: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> Any:
    """
    Convenience function to return a single value (or `None`) from `Pipe.get_data()`.
    Keywords arguments are passed to `Pipe.get_data()`.
    """
    from meerschaum.utils.warnings import warn
    kwargs['select_columns'] = [column]
    kwargs['limit'] = 1
    kwargs['as_docs'] = True
    try:
        docs = self.get_data(params=params, **kwargs)
        if not docs:
            return None
        if column not in docs[0]:
            raise ValueError(f"Column '{column}' was not included in the result set.")
        return docs[0][column]
    except Exception as e:
        warn(f"Failed to read value from {self}:\n{e}", stack=False)
        return None
