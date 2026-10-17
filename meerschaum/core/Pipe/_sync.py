#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a pipe's data with its source via its connector.
"""

from __future__ import annotations

import json
import time
import threading
import multiprocessing
import functools
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Union,
    Optional,
    Callable,
    Any,
    Tuple,
    SuccessTuple,
    Dict,
    List,
)
from meerschaum.utils.warnings import warn, error
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum._core._gate import rust_impl as _rust_impl

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')

class InferFetch:
    MRSM_INFER_FETCH: bool = True


class SyncResult(tuple):
    """
    The result of `Pipe.sync()`: a `SuccessTuple` (`(success, message)`, so
    `success, msg = pipe.sync()` unpacks as ever) with the structured fields of the sync.

    The fields are `None` when the sync did not report them (a plugin's own `sync()`,
    the Python implementation, a failure before the data was written).

    Attributes
    ----------
    success: bool
        Whether the sync succeeded.

    message: str
        The message.

    inserted: Optional[int]
        Rows inserted.

    updated: Optional[int]
        Rows updated.

    upserted: Optional[int]
        Rows upserted.

    deleted: Optional[int]
        Rows deleted.

    changed_intervals: Optional[List[Tuple[Any, Any]]]
        The `(begin, end)` intervals of the datetime axis that changed: the epoch-aligned grid
        cells (one chunk interval wide) that hold the rows the sync handed to the instance,
        merged into runs. This is a superset of the rows that actually changed (rows that
        turned out to be unchanged duplicates are inside it too). An open end is `None`.

    duration: Optional[float]
        Seconds the sync took.

    begin: Union[datetime, int, None]
        The lower bound of the sync, if one was given.

    end: Union[datetime, int, None]
        The upper bound of the sync, if one was given.
    """

    _FIELDS = (
        'inserted', 'updated', 'upserted', 'deleted',
        'changed_intervals', 'duration', 'begin', 'end',
    )

    success: bool
    message: str

    def __new__(cls, success: bool, message: str = 'Success', **fields: Any):
        self = super().__new__(cls, (success, message))
        self.success = success
        self.message = message
        for name in cls._FIELDS:
            setattr(self, name, fields.pop(name, None))
        if fields:
            raise TypeError(f"Unexpected sync result fields: {', '.join(sorted(fields))}.")
        return self

    @classmethod
    def from_tuple(cls, result: Any) -> Any:
        """
        Return `result` as a `SyncResult` if it is a plain `(bool, str)` tuple,
        otherwise return it as it is (e.g. a `RunResult` or a plugin's own tuple).
        """
        if type(result) is not tuple or len(result) != 2:
            return result
        return cls(result[0], result[1])

    def to_dict(self) -> Dict[str, Any]:
        """
        Return the result's fields as a dictionary.
        """
        return {
            'success': self.success,
            'message': self.message,
            **{name: getattr(self, name) for name in self._FIELDS},
        }

    def __reduce__(self):
        return _rebuild_sync_result, (self.to_dict(),)


def _rebuild_sync_result(fields: Dict[str, Any]) -> SyncResult:
    return SyncResult(fields.pop('success'), fields.pop('message'), **fields)


### Caches of the table's layout the Python pipe drops after a sync (as `SQLConnector.sync_pipe`).
_NATIVE_SYNC_CACHE_KEYS = (
    '_exists',
    '_columns_types',
    '_columns_types_timestamp',
    '_columns_indices',
    '_columns_indices_timestamp',
)


def _plain_value(value: Any) -> bool:
    """
    Whether a connector attribute can cross to Rust and to a worker (data, not objects).
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_plain_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(k, str) and _plain_value(v) for k, v in value.items())
    return False


def _connector_attributes(connector: Any) -> Dict[str, Any]:
    """
    The attributes that rebuild `connector` elsewhere (`get_connector(type, label, **attributes)`).
    """
    meta = dict(getattr(connector, 'meta', None) or {})
    meta.pop('type', None)
    meta.pop('label', None)
    return {key: value for key, value in meta.items() if _plain_value(value)}


def _is_dask(df: Any) -> bool:
    return type(df).__module__.split('.', maxsplit=1)[0] == 'dask'


class NativeUnavailable(str):
    """
    Why a pipe's connectors cannot run natively: the message, plus a `kind` callers branch on
    (never on the text): `'oracle_client'` (Oracle without `libclntsh`, W15L-006: the one case
    that keeps the v4 body, with a warning once per process, until wave 16), `'no_driver'` (a SQL
    connector whose Rust build failed, or a flavor that is not routed: an error) or
    `'unbuildable'` (the instance connector cannot be built at all).
    """

    kind: str = 'unbuildable'

    def __new__(cls, message: str, kind: str = 'unbuildable'):
        obj = super().__new__(cls, message)
        obj.kind = kind
        return obj


_ORACLE_FALLBACK_WARNED = False


def _warn_oracle_fallback(self, reason: str, what: str) -> None:
    """Warn, once per process, that the Python Oracle driver is used (`NativeUnavailable.kind`)."""
    global _ORACLE_FALLBACK_WARNED
    if _ORACLE_FALLBACK_WARNED:
        return
    _ORACLE_FALLBACK_WARNED = True
    warn(
        f"{what} {self} with the Python Oracle driver: " + str(reason).split(':', 1)[-1].strip(),
        stack=False,
    )


def _native_unavailable(self, reason: str, debug: bool = False) -> Optional[SyncResult]:
    """
    The native sync cannot run for this pipe's connectors. There is no silent fallback: the only
    case that keeps the v4 body (with a warning, once per process) is an `oracle` instance whose
    client library (`libclntsh`) is missing, which `python-oracledb`'s thin mode does not need;
    wave 16 replaces it with an error. Anything else is a failed result.
    """
    if getattr(reason, 'kind', None) == 'oracle_client':
        _warn_oracle_fallback(self, reason, 'Syncing')
        return None
    self._invalidate_cache(debug=debug)
    return SyncResult(False, f"Cannot sync {self}: {reason}.")


def _no_rust_driver(role: str, connector: Any) -> 'NativeUnavailable':
    cause = (getattr(connector, '__dict__', {}) or {}).get('_rust_error', None)
    if getattr(connector, 'flavor', None) == 'oracle' and not cause:
        return NativeUnavailable(
            "oracle: the Oracle client library (libclntsh) is not installed, so the Rust Oracle "
            "driver is unavailable",
            'oracle_client',
        )
    flavor = getattr(connector, 'flavor', None)
    return NativeUnavailable(
        f"{role} is a SQL connector ('{flavor}') with no usable Rust driver"
        + (f": {cause}" if cause else " (its flavor is not routed to Rust)"),
        'no_driver',
    )


def _native_context(self, debug: bool = False) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Return `(context, '')` with the connectors `_core.sync_pipe` needs, or `(None, reason)` when
    the pipe's instance cannot run natively. The reason is logged under `debug`.

    The context holds the instance's `meerschaum._core.SqlConnector` (`connector`, `None` for a
    non-SQL instance), the other Rust SQL connectors of the call (`connectors`, a SQL source the
    Python process holds) and the attributes of the connectors this process defined ad hoc
    (`attributes`), which the Rust side rebuilds and the Python workers learn about.
    """
    from meerschaum.core.Pipe._verify import _rust_sql_connector
    try:
        instance_connector = self.instance_connector
    except Exception:
        return None, NativeUnavailable("the instance connector cannot be built")
    if isinstance(instance_connector, str) or instance_connector is None:
        return None, NativeUnavailable("the instance connector cannot be built")

    context: Dict[str, Any] = {'connector': None, 'connectors': {}, 'attributes': {}}
    instance_type = getattr(instance_connector, 'type', None)
    if instance_type == 'sql':
        rust_connector = _rust_sql_connector(instance_connector)
        if rust_connector is None:
            return None, _no_rust_driver('the instance', instance_connector)
        context['connector'] = rust_connector
    ### Any other instance (`api:`, `valkey:`, a `@make_connector` type) is built by the native
    ### flow from its keys: the Rust clients for `api:` and `valkey:`, the worker for the rest.
    context['attributes'][str(instance_connector)] = _connector_attributes(instance_connector)

    ### The source: connectors this process already holds are shared with the native flow
    ### (a SQL source keeps its pool); every other source is built by keys, from the
    ### config registry or a worker.
    connector_keys = str(self.connector_keys)
    if ':' in connector_keys and connector_keys != str(instance_connector):
        source_type, source_label = connector_keys.split(':', maxsplit=1)
        held = mrsm.connectors.connectors.get(source_type, {}).get(source_label, None)
        if held is not None and source_type != 'plugin':
            if source_type == 'sql':
                rust_source = _rust_sql_connector(held)
                if rust_source is None:
                    return None, _no_rust_driver('the source', held)
                context['connectors'][connector_keys] = rust_source
            context['attributes'][connector_keys] = _connector_attributes(held)
    return context, ''


def _native_python_interpreter() -> None:
    """
    A no-op kept for callers: the extension passes this process's `sys.executable` to the worker
    session itself (`MRSM_PYTHON` still wins), and the environment is never modified.
    """


def _native_pipe_result(
    self,
    out: Dict[str, Any],
    debug: bool = False,
) -> SyncResult:
    """
    Turn the dictionary `_core.sync_pipe` returned into the `SyncResult` of `Pipe.sync()`
    (and apply what the sync changed to this pipe).
    """
    for cache_key in _NATIVE_SYNC_CACHE_KEYS:
        self._clear_cache_key(cache_key, debug=debug)
    self._invalidate_cache(debug=debug)

    if out.get('success') is False and out['message'].startswith('Expecting columns for table'):
        ### PARITY: v4 raises (`utils/sql.py:2329`) when documents with no column at all
        ### (`[{}, {}]`) would create the table.
        raise ValueError(out['message'])

    if out.get('success') is False and out['message'].startswith('You are trying to merge on '):
        ### PARITY (H9): pandas' `merge` refuses a timezone-naive key column against a
        ### timezone-aware one in `filter_existing()` and v4 lets the `ValueError` escape.
        raise ValueError(out['message'])

    if 'invalid' in out:
        ### `Pipe.enforce_dtypes()` warns when the data cannot become a DataFrame
        ### and the instance then refuses the `None`.
        warn(f"Unable to cast incoming data as a DataFrame...:\n{out['invalid']}", stack=False)
        return SyncResult(False, f"DataFrame is None. Cannot sync {self}.")

    ### The sync changed some of the registered parameters (`autoincrement`, numeric and
    ### newly detected special `dtypes`): the engine persisted them, so patch the object.
    if out['parameters']:
        self.update_parameters(out['parameters'], persist=False, debug=debug)

    message = out['message']
    if (
        debug
        and out['success']
        and message.endswith('.')
        and message.startswith(('Inserted ', 'Upserted '))
        and getattr(self.instance_connector, 'type', None) == 'sql'
    ):
        ### `SQLConnector.sync_pipe` names the table and the time under `debug`.
        from meerschaum.utils.sql import sql_item_name
        instance_connector = self.instance_connector
        table_name = sql_item_name(
            self.target,
            instance_connector.flavor,
            instance_connector.get_pipe_schema(self),
        )
        message = (
            message[:-1]
            + f"\non table {table_name}\n"
            + f"in {round(out['duration'] or 0.0, 2)} seconds."
        )

    return SyncResult(
        out['success'],
        message,
        inserted=out['inserted'],
        updated=out['updated'],
        upserted=out['upserted'],
        deleted=out['deleted'],
        changed_intervals=out['changed_intervals'],
        duration=out['duration'],
        begin=out['begin'],
        end=out['end'],
    )


def _sync_native(
    self,
    rust_sync: Callable[..., Dict[str, Any]],
    df: Any,
    begin: Union[datetime, int, str, None],
    end: Union[datetime, int, None],
    check_existing: bool,
    enforce_dtypes: bool,
    chunksize: Optional[int],
    sync_chunks: bool,
    force: bool,
    retries: int,
    min_seconds: Union[int, float],
    blocking: bool,
    workers: Optional[int],
    callback: Optional[Callable[..., Any]],
    error_callback: Optional[Callable[..., Any]],
    debug: bool,
    inplace: bool,
    kw: Dict[str, Any],
) -> Optional[SyncResult]:
    """
    Run `Pipe.sync()` through `_core.sync_pipe`, or return `None` to keep the Python
    implementation (the instance has no native driver yet, see `PARITY.md`, Wave 15 task L).
    """
    import os
    if df is None:
        self._invalidate_cache(debug=debug)
        return SyncResult(
            False,
            f"You passed `None` instead of data into `sync()` for {self}.\n"
            + "Omit the DataFrame to infer fetching.",
        )
    if _is_dask(df):
        raise TypeError(
            "Dask DataFrames are not supported in Meerschaum 5. Convert the data to pandas or "
            "Polars (`ddf.compute()`) or pass a generator of chunks. "
            "See the migration guide: reference/migrating-to-v5."
        )

    context, reason = _native_context(self, debug=debug)
    if context is None:
        return _native_unavailable(self, reason, debug=debug)

    infer = df is InferFetch or hasattr(df, 'MRSM_INFER_FETCH')
    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    if chunksize == 0:
        chunksize = None
        sync_chunks = False

    params = kw.get('params', None)
    plugin_kwargs = {
        key: value
        for key, value in kw.items()
        if key not in ('params', 'begin', 'end')
    }
    plugin_kwargs.update({
        'sync_chunks': sync_chunks,
        'blocking': blocking,
        'safe_copy': True,
    })
    if debug:
        plugin_kwargs['debug'] = True

    def _call(data: Any, infer_fetch: bool = False) -> SyncResult:
        out = rust_sync(
            self,
            context['connector'],
            None if infer_fetch else data,
            begin=parsed_begin,
            end=parsed_end,
            check_existing=check_existing,
            enforce_dtypes=enforce_dtypes,
            chunksize=chunksize,
            infer=infer_fetch,
            params=params,
            force=force,
            retries=retries,
            min_seconds=min_seconds,
            workers=workers,
            kwargs=plugin_kwargs,
            connectors=context['connectors'],
            attributes=context['attributes'],
            inplace=inplace,
            debug=debug,
        )
        if 'declined' in out:
            raise _Declined(out['declined'])
        return _native_pipe_result(self, out, debug=debug)

    def _prepare() -> Optional[SyncResult]:
        nonlocal parsed_begin, parsed_end
        parsed_begin, parsed_end = self.parse_date_bounds(begin, end, debug=debug)
        self._invalidate_cache(debug=debug)
        ### Ensure that the pipe is registered (a plugin connector's `register()` is Python).
        if not self.temporary and self.id is None:
            register_success, register_msg = self.register(debug=debug)
            if not register_success and 'already' not in register_msg:
                self._invalidate_cache(debug=debug)
                return SyncResult(register_success, register_msg)
        indices_success, indices_msg = self.validate_indices(debug=debug)
        if not indices_success:
            self._invalidate_cache(debug=debug)
            return SyncResult(indices_success, indices_msg)
        return None

    parsed_begin, parsed_end = None, None

    def _sync_blocking() -> SyncResult:
        failed = _prepare()
        if failed is not None:
            return failed
        if infer:
            return _call(None, infer_fetch=True)
        from meerschaum.utils.dataframe import df_is_chunk_generator
        ### PARITY: v4 treats a tuple as a chunk generator and raises `TypeError` (`next(tuple)`);
        ### pandas accepts it as rows, so it is data here.
        if df_is_chunk_generator(df) and not isinstance(df, tuple):
            return _sync_generator_native(
                self,
                df,
                _call,
                workers=workers,
                debug=debug,
            )
        return _call(df)

    def _run() -> SyncResult:
        try:
            return _sync_blocking()
        except _Declined as declined:
            raise TypeError(f"Cannot sync data into {self}: {declined}.") from declined

    if blocking:
        return _run()

    from meerschaum.utils.threading import Thread
    from meerschaum.utils.debug import dprint

    def default_callback(result_tuple: SuccessTuple):
        dprint(f"Asynchronous result from {self}: {result_tuple}")

    def default_error_callback(x: Exception):
        dprint(f"Error received for {self}: {x}")

    if callback is None and debug:
        callback = default_callback
    if error_callback is None and debug:
        error_callback = default_error_callback
    try:
        thread = Thread(
            target=_run,
            daemon=False,
            callback=callback,
            error_callback=error_callback,
        )
        thread.start()
    except Exception as e:
        self._invalidate_cache(debug=debug)
        return SyncResult(False, str(e))

    self._invalidate_cache(debug=debug)
    return SyncResult(True, f"Spawned asyncronous sync for {self}.")


class _Declined(Exception):
    """The binding did not handle the data's shape."""


#: What the native data boundary accepts (the text `TypeError`s name; the Rust side,
#: `sync_api.rs::input_from_py`, has the same list).
ACCEPTED_DATA_TYPES = (
    "a list of dictionaries, a dictionary of lists, text of simple lines, a pandas, GeoPandas "
    "or Polars DataFrame, a pyarrow Table or RecordBatch, a NumPy array, or an iterator of "
    "those chunks"
)


def is_accepted_data(data: Any) -> bool:
    """
    Whether the native data boundary takes `data` (`Pipe.sync()`, `enforce_dtypes()`,
    `filter_existing()`): documents, rows, columns, text, DataFrames of pandas, GeoPandas and
    Polars, pyarrow tables and batches, NumPy arrays, or an iterator of any of those.
    """
    if isinstance(data, (list, tuple, dict, str)):
        return True
    module = type(data).__module__.split('.', maxsplit=1)[0]
    if module in ('pandas', 'geopandas', 'polars', 'numpy'):
        return True
    if module == 'pyarrow':
        return type(data).__name__ in ('Table', 'RecordBatch')
    from meerschaum.utils.dataframe import df_is_chunk_generator
    return df_is_chunk_generator(data) and module != 'dask'


def unsupported_data_error(data: Any, action: str = 'use this data') -> TypeError:
    """
    The `TypeError` for data the native boundary does not accept: one message, naming the
    accepted types (`ACCEPTED_DATA_TYPES`). Callers: `raise unsupported_data_error(data, ...)`
    when `not is_accepted_data(data)`.
    """
    return TypeError(
        f"Cannot {action}: the data is a '{type(data).__module__}.{type(data).__qualname__}'. "
        f"Accepted types: {ACCEPTED_DATA_TYPES}."
    )


def _sync_generator_native(
    self,
    df: Any,
    call: Callable[..., SyncResult],
    workers: Optional[int] = None,
    debug: bool = False,
) -> SyncResult:
    """
    The chunk-generator branch of v4's `_sync`: the first chunk is synced alone, the others get
    three attempts each, and the messages are sorted and combined. Every chunk is one native sync.
    """
    from meerschaum.utils.dataframe import df_is_chunk_generator
    from meerschaum.utils.debug import dprint

    dt_col = self.columns.get('datetime', None)
    num_workers = max(int(workers or 1), 1)
    if debug:
        dprint(f"Received {type(df)}. Attempting to sync first chunk...")

    df = iter(df)
    try:
        chunk = next(df)
    except StopIteration:
        return SyncResult(True, "Received an empty generator; nothing to do.")

    first = call(chunk)
    chunk_success, chunk_msg = first.success, first.message
    chunk_msg = '\n' + self._get_chunk_label(chunk, dt_col) + '\n' + chunk_msg
    if not chunk_success:
        return SyncResult(
            chunk_success,
            f"Unable to sync initial chunk for {self}:\n{chunk_msg}",
        )
    if debug:
        dprint("Successfully synced the first chunk, attemping the rest...")

    totals = [first]

    def _process_chunk(_chunk):
        _chunk_attempts = 0
        _max_chunk_attempts = 3
        _result = None
        while _chunk_attempts < _max_chunk_attempts:
            try:
                _result = call(_chunk)
                _chunk_success, _chunk_msg = _result.success, _result.message
            except Exception as e:
                _result = None
                _chunk_success, _chunk_msg = False, str(e)
            if _chunk_success:
                break
            _chunk_attempts += 1
            _sleep_seconds = _chunk_attempts ** 2
            warn(
                (
                    f"Failed to sync chunk to {self} "
                    + f"(attempt {_chunk_attempts} / {_max_chunk_attempts}).\n"
                    + f"Sleeping for {_sleep_seconds} second"
                    + ('s' if _sleep_seconds != 1 else '')
                    + f":\n{_chunk_msg}"
                ),
                stack=False,
            )
            time.sleep(_sleep_seconds)

        num_rows_str = (
            f"{num_rows:,} rows"
            if (num_rows := len(_chunk)) != 1
            else f"{num_rows} row"
        )
        _chunk_msg = (
            (
                "Synced"
                if _chunk_success
                else "Failed to sync"
            ) + f" a chunk ({num_rows_str}) to {self}:\n"
            + self._get_chunk_label(_chunk, dt_col)
            + '\n'
            + _chunk_msg
        )
        mrsm.pprint((_chunk_success, _chunk_msg), calm=True)
        if _result is not None:
            totals.append(_result)
        return _chunk_success, _chunk_msg

    if not df_is_chunk_generator(chunk) and num_workers != 1:
        from multiprocessing.pool import ThreadPool
        with ThreadPool(num_workers) as pool:
            rest = list(pool.imap(_process_chunk, df))
    else:
        rest = list(_process_chunk(_child_chunks) for _child_chunks in df)

    results = sorted([(chunk_success, chunk_msg)] + rest)
    chunk_messages = [chunk_msg for _, chunk_msg in results]
    success_bools = [chunk_success for chunk_success, _ in results]
    num_successes = len([chunk_success for chunk_success, _ in results if chunk_success])
    num_failures = len([chunk_success for chunk_success, _ in results if not chunk_success])
    success = all(success_bools)
    msg = (
        'Synced '
        + f'{len(chunk_messages):,} chunk'
        + ('s' if len(chunk_messages) != 1 else '')
        + f' to {self}\n({num_successes} succeeded, {num_failures} failed):\n\n'
        + '\n\n'.join(chunk_messages).lstrip().rstrip()
    ).lstrip().rstrip()

    def _sum(name: str) -> Optional[int]:
        values = [getattr(r, name) for r in totals if getattr(r, name) is not None]
        return sum(values) if values else None

    return SyncResult(
        success,
        msg,
        inserted=_sum('inserted'),
        updated=_sum('updated'),
        upserted=_sum('upserted'),
        deleted=_sum('deleted'),
        changed_intervals=_union_intervals(totals),
        begin=first.begin,
        end=totals[-1].end,
    )


def _union_intervals(results: List[SyncResult]) -> List[Tuple[Any, Any]]:
    """
    The distinct changed intervals of several sync results, in order of first appearance.
    """
    seen = []
    for result in results:
        for interval in (result.changed_intervals or []):
            if interval not in seen:
                seen.append(interval)
    return seen


def sync(
    self,
    df: Union[
        pd.DataFrame,
        Dict[str, List[Any]],
        List[Dict[str, Any]],
        str,
        InferFetch
    ] = InferFetch,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, None] = None,
    force: bool = False,
    retries: int = 10,
    min_seconds: int = 1,
    check_existing: bool = True,
    enforce_dtypes: bool = True,
    blocking: bool = True,
    workers: Optional[int] = None,
    callback: Optional[Callable[[Tuple[bool, str]], Any]] = None,
    error_callback: Optional[Callable[[Exception], Any]] = None,
    chunksize: Optional[int] = -1,
    sync_chunks: bool = True,
    parents: Union[bool, int, None] = None,
    children: Union[bool, int, None] = None,
    debug: bool = False,
    _inplace: bool = True,
    **kw: Any
) -> SuccessTuple:
    """
    Fetch new data from the source and update the pipe's table with new data.
    
    Get new remote data via fetch, get existing data in the same time period,
    and merge the two, only keeping the unseen data.

    Parameters
    ----------
    df: Union[None, pd.DataFrame, Dict[str, List[Any]]], default None
        An optional DataFrame to sync into the pipe. Defaults to `None`.
        If `df` is a string, it will be parsed via `meerschaum.utils.dataframe.parse_simple_lines()`.

    begin: Union[datetime, int, str, None], default ''
        Optionally specify the earliest datetime to search for data.

    end: Union[datetime, int, str, None], default None
        Optionally specify the latest datetime to search for data.

    force: bool, default False
        If `True`, keep trying to sync untul `retries` attempts.

    retries: int, default 10
        If `force`, how many attempts to try syncing before declaring failure.

    min_seconds: Union[int, float], default 1
        If `force`, how many seconds to sleep between retries. Defaults to `1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe.

    enforce_dtypes: bool, default True
        If `True`, enforce dtypes on incoming data.
        Set this to `False` if the incoming rows are expected to be of the correct dtypes.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise
        asyncronously sync (oxymoron?) and return success. Defaults to `True`.
        Only intended for specific scenarios.

    workers: Optional[int], default None
        If provided and the instance connector is thread-safe
        (`pipe.instance_connector.IS_THREAD_SAFE is True`),
        limit concurrent sync to this many threads.

    callback: Optional[Callable[[Tuple[bool, str]], Any]], default None
        Callback function which expects a SuccessTuple as input.
        Only applies when `blocking=False`.

    error_callback: Optional[Callable[[Exception], Any]], default None
        Callback function which expects an Exception as input.
        Only applies when `blocking=False`.

    chunksize: int, default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.

    sync_chunks: bool, default True
        If possible, sync chunks while fetching them into memory.

    parents: Union[bool, int, None], default None
        If `True`, sync every ancestor of this pipe first, in dependency order (the implicit
        pipe graph, including `{{ Pipe() }}` references). An `int` syncs that many generations
        (`parents=1` is the direct parents). Runs through the Rust Syncing Engine, which skips
        parents and children that are already caught up. A bare `sync()` (the default) never
        uses the engine and behaves exactly like v4.

    children: Union[bool, int, None], default None
        If `True`, sync this pipe, then propagate to every descendant. An `int` limits the
        generations. Combine with `parents` for the whole connected slice.
        See `meerschaum.Pipeline` to plan the run first and read the run history.

    on_event: Optional[Callable[[Dict[str, Any]], Any]], default None
        Only with `parents` or `children`: called on this thread with every event of the engine
        run as it happens (`run_started`, `node_started`, `chunk_finished`, `node_finished`,
        ..., `run_finished`), each a `dict` with `event`, `run_id`, `seq`, `ts`, `pipe_ref` and
        the event's own fields. An exception it raises is warned about and the run continues
        (see `meerschaum.Pipeline.sync`).

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    Returns
    -------
    A `SyncResult`: a `SuccessTuple` of success (`bool`) and message (`str`) that also carries
    the structured fields of the sync (`inserted`, `updated`, `upserted`, `deleted`,
    `changed_intervals`, `duration`, `begin` and `end`; `None` where the sync did not
    report them).
    With `parents` or `children`, a `meerschaum.core.Pipeline.RunResult` (a `SuccessTuple`
    with the run's `run_id`, `plan`, `outcomes` and `events()`).
    """
    if parents or children:
        from meerschaum.core.Pipeline import sync_with_graph
        return sync_with_graph(
            self,
            df,
            parents=parents,
            children=children,
            begin=begin,
            end=end,
            force=force,
            retries=retries,
            min_seconds=min_seconds,
            check_existing=check_existing,
            enforce_dtypes=enforce_dtypes,
            workers=workers,
            chunksize=chunksize,
            debug=debug,
            **kw
        )

    if (_rust := _rust_impl('sync_pipe')) is not None:
        _native_result = _sync_native(
            self,
            _rust,
            df,
            begin=begin,
            end=end,
            check_existing=check_existing,
            enforce_dtypes=enforce_dtypes,
            chunksize=chunksize,
            sync_chunks=sync_chunks,
            force=force,
            retries=retries,
            min_seconds=min_seconds,
            blocking=blocking,
            workers=workers,
            callback=callback,
            error_callback=error_callback,
            debug=debug,
            inplace=(
                _inplace
                and bool(mrsm.get_config('system', 'experimental', 'inplace_sync'))
            ),
            kw=kw,
        )
        if _native_result is not None:
            return _native_result

    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.formatting import get_console
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import df_is_chunk_generator, filter_keywords, filter_arguments
    from meerschaum.utils.pool import get_pool
    from meerschaum.config import get_config
    from meerschaum.utils.dtypes import are_dtypes_equal, get_current_timestamp

    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    _checkpoint(_total=2, **kw)

    if chunksize == 0:
        chunksize = None
        sync_chunks = False

    begin, end = self.parse_date_bounds(begin, end)
    kw.update({
        'begin': begin,
        'end': end,
        'force': force,
        'retries': retries,
        'min_seconds': min_seconds,
        'check_existing': check_existing,
        'blocking': blocking,
        'workers': workers,
        'callback': callback,
        'error_callback': error_callback,
        'sync_chunks': sync_chunks,
        'chunksize': chunksize,
        'safe_copy': True,
    })

    self._invalidate_cache(debug=debug)
    self._cache_value('sync_ts', get_current_timestamp('ms'), debug=debug)

    def _sync(
        p: mrsm.Pipe,
        df: Union[
            'pd.DataFrame',
            Dict[str, List[Any]],
            List[Dict[str, Any]],
            str,
            InferFetch
        ] = InferFetch,
    ) -> SuccessTuple:
        if df is None:
            p._invalidate_cache(debug=debug)
            return (
                False,
                f"You passed `None` instead of data into `sync()` for {p}.\n"
                + "Omit the DataFrame to infer fetching.",
            )
        ### Ensure that Pipe is registered.
        if not p.temporary and p.id is None:
            ### NOTE: This may trigger an interactive session for plugins!
            register_success, register_msg = p.register(debug=debug)
            if not register_success:
                if 'already' not in register_msg:
                    p._invalidate_cache(debug=debug)
                    return register_success, register_msg

        ### Checked here as well as in `register()` so that a pipe registered before
        ### this validation existed reports the reason rather than failing deep in
        ### Pandas with `unhashable type: 'dict'`.
        indices_success, indices_msg = p.validate_indices(debug=debug)
        if not indices_success:
            p._invalidate_cache(debug=debug)
            return indices_success, indices_msg

        if isinstance(df, str):
            from meerschaum.utils.dataframe import parse_simple_lines
            df = parse_simple_lines(df)

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be omitted for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if hasattr(df, 'MRSM_INFER_FETCH'):
            try:
                if isinstance(p.connector, str):
                    if ':' not in p.connector_keys:
                        return True, f"{p} does not support fetching; nothing to do."

                    msg = f"{p} does not have a valid connector."
                    if p.connector_keys.startswith('plugin:'):
                        msg += f"\n    Perhaps {p.connector_keys} has a syntax error?"
                    p._invalidate_cache(debug=debug)
                    return False, msg
            except Exception:
                p._invalidate_cache(debug=debug)
                return False, f"Unable to create the connector for {p}."

            ### Sync in place if possible.
            if (
                str(self.connector) == str(self.instance_connector)
                and 
                hasattr(self.instance_connector, 'sync_pipe_inplace')
                and
                _inplace
                and
                get_config('system', 'experimental', 'inplace_sync')
            ):
                with Venv(get_connector_plugin(self.instance_connector)):
                    p._invalidate_cache(debug=debug)
                    _args, _kwargs = filter_arguments(
                        p.instance_connector.sync_pipe_inplace,
                        p,
                        debug=debug,
                        **kw
                    )
                    return self.instance_connector.sync_pipe_inplace(
                        *_args,
                        **_kwargs
                    )

            ### Activate and invoke `sync(pipe)` for plugin connectors with `sync` methods.
            try:
                if getattr(p.connector, 'sync', None) is not None:
                    with Venv(get_connector_plugin(p.connector), debug=debug):
                        _args, _kwargs = filter_arguments(
                            p.connector.sync,
                            p,
                            debug=debug,
                            **kw
                        )
                        return_tuple = p.connector.sync(*_args, **_kwargs)
                    p._invalidate_cache(debug=debug)
                    if not isinstance(return_tuple, tuple):
                        return_tuple = (
                            False,
                            f"Plugin '{p.connector.label}' returned non-tuple value: {return_tuple}"
                        )
                    return return_tuple

            except Exception as e:
                get_console().print_exception()
                msg = f"Failed to sync {p} with exception: '" + str(e) + "'"
                if debug:
                    error(msg, silent=False)
                p._invalidate_cache(debug=debug)
                return False, msg

            ### Fetch the dataframe from the connector's `fetch()` method.
            try:
                with Venv(get_connector_plugin(p.connector), debug=debug):
                    df = p.fetch(
                        **filter_keywords(
                            p.fetch,
                            debug=debug,
                            **kw
                        )
                    )
                    kw['safe_copy'] = False
            except Exception as e:
                get_console().print_exception(
                    suppress=[
                        'meerschaum/core/Pipe/_sync.py',
                        'meerschaum/core/Pipe/_fetch.py',
                    ]
                )
                msg = f"Failed to fetch data from {p.connector}:\n    {e}"
                df = None

            if df is None:
                p._invalidate_cache(debug=debug)
                return False, f"No data were fetched for {p}."

            if isinstance(df, list):
                if len(df) == 0:
                    return True, f"No new rows were returned for {p}."

                ### May be a chunk hook results list.
                if isinstance(df[0], tuple):
                    success = all([_success for _success, _ in df])
                    message = '\n'.join([_message for _, _message in df])
                    return success, message

            if df is True:
                p._invalidate_cache(debug=debug)
                return True, f"{p} is being synced in parallel."

        ### CHECKPOINT: Retrieved the DataFrame.
        _checkpoint(**kw)

        ### Allow for dataframe generators or iterables.
        if df_is_chunk_generator(df):
            kw['workers'] = p.get_num_workers(kw.get('workers', None))
            dt_col = p.columns.get('datetime', None)
            pool = get_pool(workers=kw.get('workers', 1))
            if debug:
                dprint(f"Received {type(df)}. Attempting to sync first chunk...")

            try:
                chunk = next(df)
            except StopIteration:
                return True, "Received an empty generator; nothing to do."

            chunk_success, chunk_msg = _sync(p, chunk)
            chunk_msg = '\n' + self._get_chunk_label(chunk, dt_col) + '\n' + chunk_msg
            if not chunk_success:
                return chunk_success, f"Unable to sync initial chunk for {p}:\n{chunk_msg}"
            if debug:
                dprint("Successfully synced the first chunk, attemping the rest...")

            def _process_chunk(_chunk):
                _chunk_attempts = 0
                _max_chunk_attempts = 3
                while _chunk_attempts < _max_chunk_attempts:
                    try:
                        _chunk_success, _chunk_msg = _sync(p, _chunk)
                    except Exception as e:
                        _chunk_success, _chunk_msg = False, str(e)
                    if _chunk_success:
                        break
                    _chunk_attempts += 1
                    _sleep_seconds = _chunk_attempts ** 2
                    warn(
                        (
                            f"Failed to sync chunk to {self} "
                            + f"(attempt {_chunk_attempts} / {_max_chunk_attempts}).\n"
                            + f"Sleeping for {_sleep_seconds} second"
                            + ('s' if _sleep_seconds != 1 else '')
                            + f":\n{_chunk_msg}"
                        ),
                        stack=False,
                    )
                    time.sleep(_sleep_seconds)

                num_rows_str = (
                    f"{num_rows:,} rows"
                    if (num_rows := len(_chunk)) != 1
                    else f"{num_rows} row"
                )
                _chunk_msg = (
                    (
                        "Synced"
                        if _chunk_success
                        else "Failed to sync"
                    ) + f" a chunk ({num_rows_str}) to {p}:\n"
                    + self._get_chunk_label(_chunk, dt_col)
                    + '\n'
                    + _chunk_msg
                )

                mrsm.pprint((_chunk_success, _chunk_msg), calm=True)
                return _chunk_success, _chunk_msg

            results = sorted(
                [(chunk_success, chunk_msg)] + (
                    list(pool.imap(_process_chunk, df))
                    if (
                        not df_is_chunk_generator(chunk)  # Handle nested generators.
                        and kw.get('workers', 1) != 1
                    )
                    else list(
                        _process_chunk(_child_chunks)
                        for _child_chunks in df
                    )
                )
            )
            chunk_messages = [chunk_msg for _, chunk_msg in results]
            success_bools = [chunk_success for chunk_success, _ in results]
            num_successes = len([chunk_success for chunk_success, _ in results if chunk_success])
            num_failures = len([chunk_success for chunk_success, _ in results if not chunk_success])
            success = all(success_bools)
            msg = (
                'Synced '
                + f'{len(chunk_messages):,} chunk'
                + ('s' if len(chunk_messages) != 1 else '')
                + f' to {p}\n({num_successes} succeeded, {num_failures} failed):\n\n'
                + '\n\n'.join(chunk_messages).lstrip().rstrip()
            ).lstrip().rstrip()
            return success, msg

        ### Cast to a dataframe and ensure datatypes are what we expect.
        dtypes = p.get_dtypes(debug=debug)
        df = p.enforce_dtypes(
            df,
            chunksize=chunksize,
            enforce=enforce_dtypes,
            dtypes=dtypes,
            debug=debug,
        )
        if p.autotime:
            dt_col = p.columns.get('datetime', None)
            ts_col = dt_col or mrsm.get_config(
                'pipes', 'autotime', 'column_name_if_datetime_missing'
            )
            ts_typ = dtypes.get(ts_col, 'datetime') if ts_col else 'datetime'
            if ts_col and hasattr(df, 'columns') and ts_col not in df.columns:
                precision = p.get_precision(debug=debug)
                now = get_current_timestamp(
                    precision_unit=precision.get(
                        'unit',
                        STATIC_CONFIG['dtypes']['datetime']['default_precision_unit']
                    ),
                    precision_interval=precision.get('interval', 1),
                    round_to=(precision.get('round_to', 'down')),
                    as_int=(are_dtypes_equal(ts_typ, 'int')),
                )
                if debug:
                    dprint(f"Adding current timestamp to dataframe synced to {p}: {now}")

                df[ts_col] = now
                kw['check_existing'] = dt_col is not None

        ### Capture special columns.
        capture_success, capture_msg = self._persist_new_special_columns(
            df,
            dtypes=dtypes,
            debug=debug,
        )
        if not capture_success:
            warn(f"Failed to capture new special columns for {self}:\n{capture_msg}")

        if debug:
            dprint(
                "DataFrame to sync:\n"
                + (
                    str(df)[:255]
                    + '...'
                    if len(str(df)) >= 256
                    else str(df)
                ),
                **kw
            )

        ### if force, continue to sync until success
        return_tuple = False, f"Did not sync {p}."
        run = True
        _retries = 1
        while run:
            with Venv(get_connector_plugin(self.instance_connector)):
                return_tuple = p.instance_connector.sync_pipe(
                    pipe=p,
                    df=df,
                    debug=debug,
                    **kw
                )
            _retries += 1
            run = (not return_tuple[0]) and force and _retries <= retries
            if run and debug:
                dprint(f"Syncing failed for {p}. Attempt ( {_retries} / {retries} )", **kw)
                dprint(f"Sleeping for {min_seconds} seconds...", **kw)
                time.sleep(min_seconds)
            if _retries > retries:
                warn(
                    f"Unable to sync {p} within {retries} attempt" +
                        ("s" if retries != 1 else "") + "!"
                )

        ### CHECKPOINT: Finished syncing.
        _checkpoint(**kw)
        p._invalidate_cache(debug=debug)

        ### Automatically apply a compression policy if the pipe is configured for compression.
        if return_tuple[0] and p.parameters.get('compress', False):
            if hasattr(p.instance_connector, 'apply_compression_policy'):
                try:
                    with Venv(get_connector_plugin(p.instance_connector)):
                        compress_success, compress_msg = (
                            p.instance_connector.apply_compression_policy(p, debug=debug)
                        )
                    if not compress_success and debug:
                        dprint(f"Could not apply compression policy to {p}:\n{compress_msg}")
                except Exception as compress_e:
                    warn(
                        f"Failed to apply compression policy to {p}:\n{compress_e}",
                        stack=False,
                    )

        return return_tuple

    if blocking:
        return SyncResult.from_tuple(_sync(self, df=df))

    from meerschaum.utils.threading import Thread
    def default_callback(result_tuple: SuccessTuple):
        dprint(f"Asynchronous result from {self}: {result_tuple}", **kw)

    def default_error_callback(x: Exception):
        dprint(f"Error received for {self}: {x}", **kw)

    if callback is None and debug:
        callback = default_callback
    if error_callback is None and debug:
        error_callback = default_error_callback
    try:
        thread = Thread(
            target=_sync,
            args=(self,),
            kwargs={'df': df},
            daemon=False,
            callback=callback,
            error_callback=error_callback,
        )
        thread.start()
    except Exception as e:
        self._invalidate_cache(debug=debug)
        return SyncResult(False, str(e))

    self._invalidate_cache(debug=debug)
    return SyncResult(True, f"Spawned asyncronous sync for {self}.")


def get_sync_time(
    self,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    apply_backtrack_interval: bool = False,
    remote: bool = False,
    round_down: bool = False,
    debug: bool = False
) -> Union['datetime', int, None]:
    """
    Get the most recent datetime value for a Pipe.

    Parameters
    ----------
    params: Optional[Dict[str, Any]], default None
        Dictionary to build a WHERE clause for a specific column.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (`ASC` instead of `DESC`).

    apply_backtrack_interval: bool, default False
        If `True`, subtract the backtrack interval from the sync time.

    remote: bool, default False
        If `True` and the instance connector supports it, return the sync time
        for the remote table definition.

    round_down: bool, default False
        If `True`, round down the datetime value to the nearest minute.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `datetime` or int, if the pipe exists, otherwise `None`.

    """
    if (_rust := _rust_impl('pipe_get_sync_time')) is not None:
        from meerschaum.core.Pipe._data import _rust_context
        _context = _rust_context(self, debug=debug)
        if _context is not None:
            return _rust(
                self,
                _context,
                params=params,
                newest=newest,
                apply_backtrack_interval=apply_backtrack_interval,
                remote=remote,
                round_down=round_down,
                debug=debug,
            )

    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_keywords
    from meerschaum.utils.dtypes import round_time
    from meerschaum.utils.warnings import warn

    if not self.columns.get('datetime', None):
        return None

    connector = self.instance_connector if not remote else self.connector
    if isinstance(connector, str) or connector is None:
        return None

    with Venv(get_connector_plugin(connector)):
        if not hasattr(connector, 'get_sync_time'):
            warn(
                f"Connectors of type '{connector.type}' "
                "do not implement `get_sync_time().",
                stack=False,
            )
            return None
        sync_time = connector.get_sync_time(
            self,
            **filter_keywords(
                connector.get_sync_time,
                params=params,
                newest=newest,
                remote=remote,
                debug=debug,
            )
        )

    if round_down and isinstance(sync_time, datetime):
        sync_time = round_time(sync_time, timedelta(minutes=1))

    if apply_backtrack_interval and sync_time is not None:
        backtrack_interval = self.get_backtrack_interval(debug=debug)
        try:
            sync_time -= backtrack_interval
        except Exception as e:
            warn(f"Failed to apply backtrack interval:\n{e}")

    return self.parse_date_bounds(sync_time)


def exists(
    self,
    debug: bool = False
) -> bool:
    """
    See if a Pipe's table exists.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's underlying table exists.

    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.dtypes import get_current_timestamp
    now = get_current_timestamp('ms', as_int=True) / 1000
    cache_seconds = mrsm.get_config('pipes', 'sync', 'exists_cache_seconds')

    _exists = self._get_cached_value('_exists', debug=debug)
    if _exists:
        exists_timestamp = self._get_cached_value('_exists_timestamp', debug=debug)
        if exists_timestamp is not None:
            delta = now - exists_timestamp
            if delta < cache_seconds:
                if debug:
                    dprint(f"Returning cached `exists` for {self} ({round(delta, 2)} seconds old).")
                return _exists

    with Venv(get_connector_plugin(self.instance_connector)):
        _exists = (
            self.instance_connector.pipe_exists(pipe=self, debug=debug)
            if hasattr(self.instance_connector, 'pipe_exists')
            else False
        )

    self._cache_value('_exists', _exists, debug=debug)
    self._cache_value('_exists_timestamp', now, debug=debug)
    return _exists


def filter_existing(
    self,
    df: 'pd.DataFrame',
    safe_copy: bool = True,
    date_bound_only: bool = False,
    include_unchanged_columns: bool = False,
    enforce_dtypes: bool = False,
    chunksize: Optional[int] = -1,
    debug: bool = False,
    **kw
) -> Tuple['pd.DataFrame', 'pd.DataFrame', 'pd.DataFrame']:
    """
    Inspect a dataframe and filter out rows which already exist in the pipe.

    Parameters
    ----------
    df: 'pd.DataFrame'
        The dataframe to inspect and filter.
        A `polars.DataFrame` or `LazyFrame` is accepted,
        in which case the returned dataframes are `polars.DataFrame`.

    safe_copy: bool, default True
        If `True`, create a copy before comparing and modifying the dataframes.
        Setting to `False` may mutate the DataFrames.
        See `meerschaum.utils.dataframe.filter_unseen_df`.

    date_bound_only: bool, default False
        If `True`, only use the datetime index to fetch the sample dataframe.

    include_unchanged_columns: bool, default False
        If `True`, include the backtrack columns which haven't changed in the update dataframe.
        This is useful if you can't update individual keys.

    enforce_dtypes: bool, default False
        If `True`, ensure the given and intermediate dataframes are enforced to the correct dtypes.
        Setting `enforce_dtypes=True` may impact performance.

    chunksize: Optional[int], default -1
        The `chunksize` used when fetching existing data.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A tuple of three DataFrames: unseen, update, and delta.
    These are Polars DataFrames if `df` is a Polars DataFrame or LazyFrame, else Pandas.
    """
    if (_rust := _rust_impl('pipe_filter_existing')) is not None:
        from meerschaum.core.Pipe._data import _rust_context
        _context = _rust_context(self, debug=debug)
        if _context is not None:
            if df is not None and not is_accepted_data(df):
                raise unsupported_data_error(df, f"filter the existing rows of {self}")
            return _rust(
                self,
                _context,
                df,
                date_bound_only=date_bound_only,
                include_unchanged_columns=include_unchanged_columns,
                enforce_dtypes=enforce_dtypes,
                debug=debug,
            )

    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.dataframe import (
        filter_unseen_df,
        add_missing_cols_to_df,
        get_unhashable_cols,
        get_json_cols,
        to_pandas,
        to_polars,
    )
    from meerschaum.utils.dtypes import (
        to_pandas_dtype,
        none_if_null,
    )
    from meerschaum.config import get_config
    pd = import_pandas()
    pandas = attempt_import('pandas')
    ### Polars frames are filtered natively when possible;
    ### otherwise they are converted up-front and converted back on the way out.
    input_is_polars = df is not None and df.__class__.__module__.split('.')[0] == 'polars'
    if input_is_polars:
        polars_dfs = self._filter_existing_polars(
            df,
            safe_copy=safe_copy,
            date_bound_only=date_bound_only,
            include_unchanged_columns=include_unchanged_columns,
            enforce_dtypes=enforce_dtypes,
            chunksize=chunksize,
            debug=debug,
            **kw
        )
        if polars_dfs is not None:
            return polars_dfs

    df = to_pandas(df)

    def _return_dfs(unseen, update, delta):
        if not input_is_polars:
            return unseen, update, delta
        return tuple(
            to_polars(_df, json_cols=get_json_cols(_df))
            for _df in (unseen, update, delta)
        )

    if enforce_dtypes or 'dataframe' not in str(type(df)).lower():
        df = self.enforce_dtypes(df, chunksize=chunksize, debug=debug)
    is_dask = hasattr(df, '__module__') and 'dask' in df.__module__
    if is_dask:
        dd = attempt_import('dask.dataframe')
        merge = dd.merge
        NA = pandas.NA
    else:
        merge = pd.merge
        NA = pd.NA

    parameters = self.parameters
    pipe_columns = self.columns
    primary_key = pipe_columns.get('primary', None)
    dt_col = pipe_columns.get('datetime', None)
    dt_type = parameters.get('dtypes', {}).get(dt_col, 'datetime') if dt_col else None
    autoincrement = parameters.get('autoincrement', False)
    autotime = parameters.get('autotime', False)

    if primary_key and autoincrement and df is not None and primary_key in df.columns:
        if safe_copy:
            df = df.copy()
            safe_copy = False
        if df[primary_key].isnull().all():
            del df[primary_key]
            _ = self.columns.pop(primary_key, None)

    if dt_col and autotime and df is not None and dt_col in df.columns:
        if safe_copy:
            df = df.copy()
            safe_copy = False
        if df[dt_col].isnull().all():
            del df[dt_col]
            _ = self.columns.pop(dt_col, None)

    def get_empty_df():
        empty_df = pd.DataFrame([])
        dtypes = dict(df.dtypes) if df is not None else {}
        dtypes.update(self.dtypes) if self.enforce else {}
        pd_dtypes = {
            col: to_pandas_dtype(str(typ))
            for col, typ in dtypes.items()
        }
        return add_missing_cols_to_df(empty_df, pd_dtypes)

    if df is None:
        empty_df = get_empty_df()
        return _return_dfs(empty_df, empty_df, empty_df)

    if (df.empty if not is_dask else len(df) == 0):
        return _return_dfs(df, df, df)

    if autoincrement and primary_key == dt_col and dt_col not in df.columns:
        if enforce_dtypes:
            df = self.enforce_dtypes(df, chunksize=chunksize, debug=debug)
        return _return_dfs(df, get_empty_df(), df)

    if autotime and dt_col and dt_col not in df.columns:
        if enforce_dtypes:
            df = self.enforce_dtypes(df, chunksize=chunksize, debug=debug)
        return _return_dfs(df, get_empty_df(), df)

    try:
        min_dt_val = df[dt_col].min(skipna=True) if dt_col and dt_col in df.columns else None
        max_dt_val = df[dt_col].max(skipna=True) if dt_col and dt_col in df.columns else None
        if is_dask:
            min_dt_val = min_dt_val.compute() if min_dt_val is not None else None
            max_dt_val = max_dt_val.compute() if max_dt_val is not None else None
    except Exception:
        min_dt_val, max_dt_val = None, None

    ### `begin` is the oldest data in the new dataframe, `end` is the newest.
    begin, end = _get_delta_bounds(min_dt_val, max_dt_val, dt_type, dt_col)

    unique_index_vals = {
        col: df[col].unique()
        for col in (pipe_columns.values() if not primary_key else [primary_key])
        if col in df.columns and col != dt_col
    } if not date_bound_only else {}
    unique_index_lens = {
        col: len(unique_vals)
        for col, unique_vals in unique_index_vals.items()
    } if not date_bound_only else {}
    filter_params_index_limit = get_config('pipes', 'sync', 'filter_params_index_limit')
    _ = kw.pop('params', None)
    params = {
        col: [
            none_if_null(val)
            for val in unique_vals
        ]
        for col, unique_vals in unique_index_vals.items()
        if unique_index_lens[col] <= filter_params_index_limit
    } if not date_bound_only else {}

    if debug:
        dprint(
            (
                f"Looking at data between '{begin}' and '{end}' with index value lengths:\n"
                f"{json.dumps(unique_index_lens, indent=4)}\n"
            ),
            **kw
        )

    backtrack_df = self.get_data(
        begin=begin,
        end=end,
        chunksize=chunksize,
        params=params,
        debug=debug,
        **kw
    )
    if backtrack_df is None:
        if debug:
            dprint(f"No backtrack data was found for {self}.")
        return _return_dfs(df, get_empty_df(), df)

    if enforce_dtypes:
        backtrack_df = self.enforce_dtypes(backtrack_df, chunksize=chunksize, debug=debug)

    if debug:
        dprint(f"Existing data for {self}:\n" + str(backtrack_df), **kw)
        dprint(f"Existing dtypes for {self}:\n" + str(backtrack_df.dtypes))

    ### Separate new rows from changed ones.
    on_cols = [
        col
        for col_key, col in pipe_columns.items()
        if (
            col
            and
            col_key != 'value'
            and col in backtrack_df.columns
        )
    ] if not primary_key else [primary_key]

    self_dtypes = self.get_dtypes(debug=debug) if self.enforce else {}
    on_cols_dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in self_dtypes.items()
        if col in on_cols
    }

    ### Detect changes between the old target and new source dataframes.
    delta_df = add_missing_cols_to_df(
        filter_unseen_df(
            backtrack_df,
            df,
            dtypes={
                col: to_pandas_dtype(typ)
                for col, typ in self_dtypes.items()
            },
            safe_copy=safe_copy,
            coerce_mixed_numerics=(not self.static),
            debug=debug
        ),
        on_cols_dtypes,
    )
    if enforce_dtypes:
        delta_df = self.enforce_dtypes(delta_df, chunksize=chunksize, debug=debug)

    for col, typ in on_cols_dtypes.items():
        delta_df[col] = delta_df[col].astype(typ)
        backtrack_df[col] = backtrack_df[col].astype(typ)

    ### Cast dicts or lists to strings so we can merge.
    serializer = functools.partial(json.dumps, sort_keys=True, separators=(',', ':'), default=str)

    def deserializer(x):
        return json.loads(x) if isinstance(x, str) else x

    unhashable_delta_cols = get_unhashable_cols(delta_df)
    unhashable_backtrack_cols = get_unhashable_cols(backtrack_df)
    for col in unhashable_delta_cols:
        delta_df[col] = delta_df[col].apply(serializer)
    for col in unhashable_backtrack_cols:
        backtrack_df[col] = backtrack_df[col].apply(serializer)
    casted_cols = set(unhashable_delta_cols + unhashable_backtrack_cols)

    joined_df = merge(
        delta_df.infer_objects().fillna(NA),
        backtrack_df.infer_objects().fillna(NA),
        how='left',
        on=on_cols,
        indicator=True,
        suffixes=('', '_old'),
    ) if on_cols else delta_df
    for col in casted_cols:
        if col in joined_df.columns:
            joined_df[col] = joined_df[col].apply(deserializer)
        if col in delta_df.columns:
            delta_df[col] = delta_df[col].apply(deserializer)

    ### Determine which rows are completely new.
    new_rows_mask = (joined_df['_merge'] == 'left_only') if on_cols else None
    cols = list(delta_df.columns)

    unseen_df = (
        joined_df[new_rows_mask][cols].reset_index(drop=True)
    ) if on_cols else delta_df

    ### Rows that have already been inserted but values have changed.
    update_df = (
        joined_df[~new_rows_mask][cols].reset_index(drop=True)
    ) if on_cols else get_empty_df()

    if include_unchanged_columns and on_cols:
        unchanged_backtrack_cols = [
            col
            for col in backtrack_df.columns
            if col in on_cols or col not in update_df.columns
        ]
        if enforce_dtypes:
            update_df = self.enforce_dtypes(update_df, chunksize=chunksize, debug=debug)
        update_df = merge(
            backtrack_df[unchanged_backtrack_cols],
            update_df,
            how='inner',
            on=on_cols,
        )

    return _return_dfs(unseen_df, update_df, delta_df)


def _get_delta_bounds(
    min_dt_val: Any,
    max_dt_val: Any,
    dt_type: Optional[str],
    dt_col: Optional[str],
) -> Tuple[Any, Any]:
    """
    Return the `begin` and `end` bounds used to fetch the existing rows to compare against.

    Parameters
    ----------
    min_dt_val: Any
        The minimum value of the incoming datetime axis (`None` if there is no datetime axis).

    max_dt_val: Any
        The maximum value of the incoming datetime axis (`None` if there is no datetime axis).

    dt_type: Optional[str]
        The registered dtype of the datetime axis (e.g. `'datetime'` or `'int'`).

    dt_col: Optional[str]
        The name of the datetime axis column.

    Returns
    -------
    A tuple of the `begin` and `end` bounds (either may be `None`).
    """
    from meerschaum.utils.dtypes import (
        to_datetime,
        are_dtypes_equal,
        value_is_null,
        round_time,
    )

    def _coerce(val: Any) -> Any:
        try:
            return (
                to_datetime(val, as_pydatetime=True)
                if val is not None and are_dtypes_equal(dt_type, 'datetime')
                else val
            )
        except Exception:
            return None

    min_dt, max_dt = _coerce(min_dt_val), _coerce(max_dt_val)

    if not are_dtypes_equal('datetime', str(type(min_dt))) or value_is_null(min_dt):
        if not are_dtypes_equal('int', str(type(min_dt))):
            min_dt = None

    if not are_dtypes_equal('datetime', str(type(max_dt))) or value_is_null(max_dt):
        if not are_dtypes_equal('int', str(type(max_dt))):
            max_dt = None

    begin, end = None, None

    if isinstance(min_dt, datetime):
        rounded_min_dt = round_time(min_dt, to='down')
        try:
            begin = rounded_min_dt - timedelta(minutes=1)
        except OverflowError:
            begin = rounded_min_dt
    elif dt_type and 'int' in dt_type.lower():
        begin = min_dt
    elif dt_col is None:
        begin = None

    if isinstance(max_dt, datetime):
        end = round_time(max_dt, to='down') + timedelta(minutes=1)
    elif dt_type and 'int' in dt_type.lower() and max_dt is not None:
        end = max_dt + 1

    if max_dt is not None and min_dt is not None and min_dt > max_dt:
        warn("Detected minimum datetime greater than maximum datetime.")

    if begin is not None and end is not None and begin > end:
        if isinstance(begin, datetime):
            begin = end - timedelta(minutes=1)
        ### We might be using integers for the datetime axis.
        else:
            begin = end - 1

    return begin, end


def _is_polars_fallback_dtype(typ: Any) -> bool:
    """Return `True` if a registered dtype cannot be compared natively by Polars."""
    typ_str = str(typ).lower()
    return (
        typ_str in ('uuid', 'object')
        or typ_str.startswith(('geometry', 'geography'))
    )


def _polars_dtypes_reconcilable(left: Any, right: Any) -> bool:
    """
    Return `True` if two Polars dtypes may be safely widened to a common dtype.

    Pandas treats integers of different widths as equal and does not downcast,
    so the incoming and stored frames may disagree on width (e.g. Oracle's `int32`).
    Only same-family differences are reconcilable; anything else
    (an `int` against a `float`, for example) is left to the Pandas path,
    which coerces mixed numerics into `numeric` values.
    """
    if left == right:
        return True

    for check in ('is_integer', 'is_float', 'is_temporal'):
        if not getattr(left, check, lambda: False)():
            continue
        if not getattr(right, check, lambda: False)():
            return False
        if check == 'is_temporal':
            ### Naive and timezone-aware columns are not comparable,
            ### and Polars widens to the coarser time unit,
            ### which would round away a real difference.
            return (
                (getattr(left, 'time_zone', None) is None)
                == (getattr(right, 'time_zone', None) is None)
                and getattr(left, 'time_unit', None) == getattr(right, 'time_unit', None)
            )
        return True

    return False


def _has_polars_object_cols(df: Any, polars: Any) -> bool:
    """Return `True` if a Polars frame contains columns which cannot be joined."""
    return any(
        typ == polars.Object or str(typ).lower() in ('object', 'unknown')
        for typ in df.schema.values()
    )


def _filter_existing_polars(
    self,
    df: Any,
    safe_copy: bool = True,
    date_bound_only: bool = False,
    include_unchanged_columns: bool = False,
    enforce_dtypes: bool = False,
    chunksize: Optional[int] = -1,
    debug: bool = False,
    **kw
) -> Union[Tuple[Any, Any, Any], None]:
    """
    Filter a Polars DataFrame against the pipe's existing rows without converting to Pandas.

    The existing rows are fetched as Polars (`Pipe.get_data(as_polars=True)`),
    and both the delta anti-join and the unseen / update split are performed by Polars.

    Parameters
    ----------
    df: Any
        A `polars.DataFrame` or `polars.LazyFrame` to inspect and filter.

    See `meerschaum.Pipe.filter_existing` for the remaining parameters.

    Returns
    -------
    A tuple of three `polars.DataFrame` (unseen, update, delta),
    or `None` if the frames cannot be compared natively,
    in which case the caller falls back to the Pandas implementation.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.dtypes import none_if_null
    from meerschaum.config import get_config

    polars = attempt_import('polars', install=False, lazy=False, warn=False)
    if polars is None:
        return None

    ### NOTE: Polars frames are immutable, so `safe_copy` is a no-op on this path.
    ### Dtypes are always enforced here (regardless of `enforce_dtypes`)
    ### because Polars can only join frames which share a schema.
    _ = safe_copy, enforce_dtypes

    parameters = self.parameters
    pipe_columns = self.columns
    primary_key = pipe_columns.get('primary', None)
    dt_col = pipe_columns.get('datetime', None)
    dt_type = parameters.get('dtypes', {}).get(dt_col, 'datetime') if dt_col else None

    ### These paths drop columns and mutate `self.columns`; leave them to Pandas.
    if parameters.get('autoincrement', False) or parameters.get('autotime', False):
        return None

    pipe_dtypes = self.get_dtypes(debug=debug) if self.enforce else {}
    if any(_is_polars_fallback_dtype(typ) for typ in pipe_dtypes.values()):
        return None

    try:
        if df.__class__.__name__ == 'LazyFrame':
            df = df.collect()

        if df.is_empty():
            return None

        df = self.enforce_dtypes(df, chunksize=chunksize, as_polars=True, debug=debug)
        if df is None or df.__class__.__module__.split('.')[0] != 'polars':
            return None
        if _has_polars_object_cols(df, polars):
            return None

        min_dt_val = df[dt_col].min() if dt_col and dt_col in df.columns else None
        max_dt_val = df[dt_col].max() if dt_col and dt_col in df.columns else None
        begin, end = _get_delta_bounds(min_dt_val, max_dt_val, dt_type, dt_col)

        unique_index_vals = {
            col: df[col].unique().to_list()
            for col in (pipe_columns.values() if not primary_key else [primary_key])
            if col in df.columns and col != dt_col
        } if not date_bound_only else {}
        filter_params_index_limit = get_config('pipes', 'sync', 'filter_params_index_limit')
        _ = kw.pop('params', None)
        params = {
            col: [
                none_if_null(val)
                for val in unique_vals
            ]
            for col, unique_vals in unique_index_vals.items()
            if len(unique_vals) <= filter_params_index_limit
        } if not date_bound_only else {}

        if debug:
            dprint(
                f"Looking at Polars data between '{begin}' and '{end}' "
                f"with index value lengths:\n"
                + json.dumps(
                    {col: len(vals) for col, vals in unique_index_vals.items()},
                    indent=4,
                )
            )

        backtrack_df = self.get_data(
            begin=begin,
            end=end,
            chunksize=chunksize,
            params=params,
            as_polars=True,
            debug=debug,
            **kw
        )
    except Exception as e:
        if debug:
            dprint(f"Failed to filter {self} natively with Polars:\n{e}")
        return None

    if backtrack_df is None:
        if debug:
            dprint(f"No backtrack data was found for {self}.")
        return df, df.clear(), df

    try:
        if backtrack_df.__class__.__name__ == 'LazyFrame':
            backtrack_df = backtrack_df.collect()
        if backtrack_df.__class__.__module__.split('.')[0] != 'polars':
            return None
        if _has_polars_object_cols(backtrack_df, polars):
            return None

        if primary_key and primary_key not in backtrack_df.columns:
            return None

        ### Align both frames on the union of their columns.
        new_cols = list(df.columns)
        for col, typ in backtrack_df.schema.items():
            if col not in df.columns:
                df = df.with_columns(polars.lit(None, dtype=typ).alias(col))
        for col, typ in df.schema.items():
            if col not in backtrack_df.columns:
                backtrack_df = backtrack_df.with_columns(polars.lit(None, dtype=typ).alias(col))
        backtrack_df = backtrack_df.select(list(df.columns))

        ### Widen same-family dtype differences to their common dtype.
        ### Anything else is left to the Pandas path, which reconciles them.
        mismatched_cols = [
            col
            for col in df.columns
            if df.schema[col] != backtrack_df.schema[col]
        ]
        if mismatched_cols:
            if any(
                not _polars_dtypes_reconcilable(df.schema[col], backtrack_df.schema[col])
                for col in mismatched_cols
            ):
                if debug:
                    dprint(
                        "Irreconcilable Polars dtypes for "
                        + f"{mismatched_cols}; falling back to Pandas."
                    )
                return None
            common_schema = dict(
                polars.concat(
                    [df.clear(), backtrack_df.clear()],
                    how='vertical_relaxed',
                ).schema
            )
            df = df.cast(common_schema)
            backtrack_df = backtrack_df.cast(common_schema)

        on_cols = [
            col
            for col_key, col in pipe_columns.items()
            if (
                col
                and col_key != 'value'
                and col in backtrack_df.columns
            )
        ] if not primary_key else [primary_key]
        out_cols = new_cols + [col for col in on_cols if col not in new_cols]

        ### Preserve the incoming row order across the joins.
        row_col = '__mrsm_row_id'
        while row_col in df.columns:
            row_col = '_' + row_col

        join_cols = list(df.columns)
        delta_df = df.with_row_index(row_col).join(
            backtrack_df,
            on=join_cols,
            how='anti',
            nulls_equal=True,
        )

        if not on_cols:
            unseen_df, update_df = delta_df, delta_df.clear()
        else:
            backtrack_index_df = backtrack_df.select(on_cols).unique()
            unseen_df = delta_df.join(
                backtrack_index_df,
                on=on_cols,
                how='anti',
                nulls_equal=True,
            )
            update_df = delta_df.join(
                backtrack_index_df,
                on=on_cols,
                how='semi',
                nulls_equal=True,
            )

        def _finalize(_df):
            return _df.sort(row_col).drop(row_col).select(out_cols)

        unseen_df = _finalize(unseen_df)
        update_df = _finalize(update_df)
        delta_df = _finalize(delta_df)

        if include_unchanged_columns and on_cols:
            unchanged_backtrack_cols = [
                col
                for col in backtrack_df.columns
                if col in on_cols or col not in update_df.columns
            ]
            update_df = backtrack_df.select(unchanged_backtrack_cols).join(
                update_df,
                on=on_cols,
                how='inner',
                nulls_equal=True,
            )
    except Exception as e:
        if debug:
            dprint(f"Failed to filter {self} natively with Polars:\n{e}")
        return None

    return unseen_df, update_df, delta_df

@staticmethod
def _get_chunk_label(
    chunk: Union[
        'pd.DataFrame',
        List[Dict[str, Any]],
        Dict[str, List[Any]]
    ],
    dt_col: str,
) -> str:
    """
    Return the min - max label for the chunk.
    """
    from meerschaum.utils.dataframe import get_datetime_bound_from_df
    min_dt = get_datetime_bound_from_df(chunk, dt_col)
    max_dt = get_datetime_bound_from_df(chunk, dt_col, minimum=False)
    return (
        f"{min_dt} - {max_dt}"
        if min_dt is not None and max_dt is not None
        else ''
    )


def get_num_workers(self, workers: Optional[int] = None) -> int:
    """
    Get the number of workers to use for concurrent syncs.

    Parameters
    ----------
    The number of workers passed via `--workers`.

    Returns
    -------
    The number of workers, capped for safety.
    """
    is_thread_safe = getattr(self.instance_connector, 'IS_THREAD_SAFE', False)
    if not is_thread_safe:
        return 1

    engine_pool_size = (
        self.instance_connector.engine.pool.size()
        if self.instance_connector.type == 'sql'
        else None
    )
    current_num_threads = threading.active_count()
    current_num_connections = (
        self.instance_connector.engine.pool.checkedout()
        if engine_pool_size is not None
        else current_num_threads
    )
    desired_workers = (
        min(workers or engine_pool_size, engine_pool_size)
        if engine_pool_size is not None
        else workers
    )
    if desired_workers is None:
        desired_workers = (multiprocessing.cpu_count() if is_thread_safe else 1)

    return max(
        (desired_workers - current_num_connections),
        1,
    )


def _persist_new_special_columns(
    self,
    df: 'pd.DataFrame',
    dtypes: Optional[Dict[str, str]] = None,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Check for new special columns and update the parameters accordingly.
    """
    from meerschaum.utils.dataframe import get_special_cols
    from meerschaum.utils.dtypes import is_dtype_special
    from meerschaum.utils.warnings import dprint

    special_cols = get_special_cols(df)
    dtypes = dtypes or self.get_dtypes(debug=debug)
    existing_special_cols = {
        col: typ
        for col, typ in dtypes.items()
        if is_dtype_special(typ)
    }
    new_special_cols = {
        col: typ
        for col, typ in special_cols.items()
        if col not in existing_special_cols
    }
    self._cache_value('new_special_cols', new_special_cols, memory_only=True, debug=debug)
    if not new_special_cols:
        return True, "Success"

    if debug:
        dprint(f"New special columns:\n{new_special_cols}")

    self._clear_cache_key('_attributes_sync_time', debug=debug)
    return self.update_parameters({'dtypes': new_special_cols}, debug=debug)
