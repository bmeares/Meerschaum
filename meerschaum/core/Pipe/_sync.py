#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a pipe's data with its source via its connector.
"""

from __future__ import annotations

import os
import time
import threading
import multiprocessing
from datetime import datetime
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
from meerschaum.utils.warnings import warn

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


def _no_rust_driver(role: str, error: Any = None) -> str:
    """The reason for a SQL connector with no Rust driver (`SQLConnector._core_connector`)."""
    return str(error).rstrip('.') if error else f"{role} has no usable Rust driver"


def _missing_plugin_reason(self) -> Optional[str]:
    """
    Why a `plugin:` source cannot fetch because its plugin is not installed (the CLI's wording),
    or `None` when the source is not a plugin or the plugin is installed.
    """
    connector_keys = str(self.connector_keys)
    if not connector_keys.startswith('plugin:'):
        return None
    label = connector_keys.split(':', maxsplit=1)[1]
    from meerschaum.core import Plugin
    from meerschaum.plugins import _FIRST_PARTY_PLUGIN_MODULES
    from meerschaum.connectors.plugin.PluginConnector import _is_native_plugin
    try:
        if (
            label in _FIRST_PARTY_PLUGIN_MODULES
            or Plugin(label).is_installed()
            or _is_native_plugin(label)
        ):
            return None
    except Exception:
        return None
    return (
        f"Connector '{connector_keys}' is unavailable: plugin '{label}' is not installed.\n"
        f"    Install it with `mrsm install plugin {label}`."
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
    ### As in v4, an instance that cannot be parsed or built raises here
    ### (`Unable to parse connector keys '...'`).
    instance_connector = self.instance_connector
    if isinstance(instance_connector, str) or instance_connector is None:
        from meerschaum.utils.warnings import error
        error(f"Unable to parse connector keys '{self.instance_keys}'", stack=False)

    context: Dict[str, Any] = {'connector': None, 'connectors': {}, 'attributes': {}}
    instance_type = getattr(instance_connector, 'type', None)
    if instance_type == 'sql':
        try:
            context['connector'] = instance_connector._core_connector()
        except RuntimeError as e:
            return None, _no_rust_driver('the instance', e)
    ### Any other instance (`api:`, `valkey:`, a `@make_connector` type) is built by the native
    ### flow from its keys: the Rust clients for `api:` and `valkey:`, the worker for the rest.
    ### The native flow finds an `api:` / `valkey:` connector's client by its attributes: make the
    ### connector (re)build it for the current ones first (changed credentials, a forked child).
    if instance_type in ('api', 'valkey') and hasattr(instance_connector, '_rust_client'):
        try:
            instance_connector._rust_client()
        except Exception:
            pass
    ### A native plugin's connector type is resolved by the core's native registry from the
    ### config, never by a Python worker: it is not passed on.
    if not getattr(instance_connector, 'IS_NATIVE', False):
        context['attributes'][str(instance_connector)] = _connector_attributes(instance_connector)

    ### The source: connectors this process already holds are shared with the native flow
    ### (a SQL source keeps its pool); every other source is built by keys, from the
    ### config registry or a worker.
    connector_keys = str(self.connector_keys)
    if ':' in connector_keys and connector_keys != str(instance_connector):
        source_type, source_label = connector_keys.split(':', maxsplit=1)
        held = mrsm.connectors.connectors.get(source_type, {}).get(source_label, None)
        if (
            held is not None
            and source_type != 'plugin'
            and not getattr(held, 'IS_NATIVE', False)
        ):
            if source_type == 'sql':
                try:
                    context['connectors'][connector_keys] = held._core_connector()
                except RuntimeError as e:
                    return None, _no_rust_driver('the source', e)
            context['attributes'][connector_keys] = _connector_attributes(held)
    return context, ''


def _adopt_synced_columns_types(self, debug: bool = False) -> None:
    """
    Keep the table layout the native sync ended with as the pipe's cached `get_columns_types()`
    (in memory only, as if it had just been read) instead of reflecting the table again on the
    next read. The sync reflects the layout before it writes and again after any DDL of its own;
    the instance hands it out once, and nothing when the sync created the table. Any failure
    leaves the cache empty (the next read reflects the table).
    """
    instance = self.instance_connector
    if getattr(instance, 'type', None) != 'sql':
        return
    try:
        columns_types = instance._rust_method('take_synced_columns_types')(self)
    except Exception:
        return
    if not columns_types:
        return
    from meerschaum.utils.dtypes import get_current_timestamp
    self._cache_value('_columns_types', dict(columns_types), memory_only=True, debug=debug)
    self._cache_value(
        '_columns_types_timestamp',
        get_current_timestamp('ms', as_int=True) / 1000,
        memory_only=True,
        debug=debug,
    )


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

    if out['success']:
        _adopt_synced_columns_types(self, debug=debug)

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
    replace: bool = False,
) -> SyncResult:
    """Run `Pipe.sync()` through `_core.sync_pipe`."""
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
        self._invalidate_cache(debug=debug)
        return SyncResult(False, f"Cannot sync {self}: {reason}.")

    infer = df is InferFetch or hasattr(df, 'MRSM_INFER_FETCH')
    if infer:
        unavailable = _missing_plugin_reason(self)
        if unavailable:
            self._invalidate_cache(debug=debug)
            return SyncResult(
                False,
                f"{self} does not have a valid connector.\n    {unavailable}",
            )
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
            replace=replace,
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
            if replace:
                ### A replaced window is written in one transaction: every chunk at once.
                return _call(_concat_chunks(list(df)))
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


def _concat_chunks(chunks: List[Any]) -> Any:
    """
    One input from the chunks of a generator (for `replace=True`, which writes its window in one
    transaction): lists of documents are joined, pandas and Polars frames concatenated.
    """
    if not chunks:
        return []
    if all(isinstance(chunk, list) for chunk in chunks):
        return [doc for chunk in chunks for doc in chunk]
    module = type(chunks[0]).__module__.split('.', maxsplit=1)[0]
    if module in ('pandas', 'geopandas'):
        pd = mrsm.attempt_import('pandas')
        return pd.concat(chunks, ignore_index=True)
    if module == 'polars':
        pl = mrsm.attempt_import('polars')
        return pl.concat(chunks, how='diagonal_relaxed')
    if len(chunks) == 1:
        return chunks[0]
    raise TypeError(
        "Cannot replace a window with chunks of "
        f"'{type(chunks[0]).__module__}.{type(chunks[0]).__qualname__}': "
        "pass lists of documents, pandas or Polars DataFrames, or one chunk."
    )


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
    replace: bool = False,
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

    replace: bool, default False
        If `True`, the rows you sync replace the pipe's rows in `[begin, end)` instead of being
        merged into them: in one transaction, the rows of that window (matching `params`, if
        given) are deleted and the new rows inserted, so rows missing from the new data are
        removed. When you pass data, `begin` defaults to the earliest datetime in the data and
        `end` to just after the latest one, and rows outside `[begin, end)` are not written
        (the message says how many were skipped). An empty frame with `begin` and `end` clears
        the window. Without data (a fetch), the window is the fetch's own `[begin, end)`; set
        `fetch: {replace: true}` in the parameters to do this on every sync. A pipe without a
        datetime column has one window: the whole table. Valkey instances clear the window
        then write the rows (no transaction).

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
    if replace:
        kw['replace'] = True
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

    from meerschaum import _core
    return _sync_native(
        self,
        _core.sync_pipe,
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
        replace=bool(replace),
    )


def get_sync_time(
    self,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    apply_backtrack_interval: bool = False,
    remote: bool = False,
    round_down: bool = False,
    silent: bool = False,
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

    silent: bool, default False
        If `True`, return `None` instead of raising when the database cannot be read
        (Meerschaum 4's behavior).

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `datetime` or int, if the pipe exists, otherwise `None` (a pipe that was never synced
    has no sync time). Raises the database error if the instance cannot be read.

    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    try:
        context = _rust_context(self, debug=debug)
        return _core.pipe_get_sync_time(
            self,
            context,
            params=params,
            newest=newest,
            apply_backtrack_interval=apply_backtrack_interval,
            remote=remote,
            round_down=round_down,
            debug=debug,
        )
    except Exception:
        if silent:
            return None
        raise


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
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
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
        Documents, dictionaries and Arrow data come back in the library of the
        `meerschaum:dataframe` setting.

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

    as_polars: Optional[bool], default None
        For input that is not a DataFrame: `True` returns Polars frames, `False` pandas ones.

    as_pandas: Optional[bool], default None
        For input that is not a DataFrame: `True` returns pandas frames, `False` Polars ones.

    Returns
    -------
    A tuple of three DataFrames: unseen, update, and delta, of the library `df` came in
    (Polars for a Polars DataFrame or LazyFrame, pandas for a pandas DataFrame).
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    from meerschaum.utils.dataframe import (
        output_library_for, ensure_dataframe_library, frame_library_of,
    )
    context = _rust_context(self, debug=debug)
    if df is not None and not is_accepted_data(df):
        raise unsupported_data_error(df, f"filter the existing rows of {self}")
    library = (
        frame_library_of(df)
        or output_library_for(df, as_polars=as_polars, as_pandas=as_pandas)
    )
    ensure_dataframe_library(library)
    return _core.pipe_filter_existing(
        self,
        context,
        df,
        date_bound_only=date_bound_only,
        include_unchanged_columns=include_unchanged_columns,
        enforce_dtypes=enforce_dtypes,
        as_polars=(library == 'polars'),
        debug=debug,
    )


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

    engine_pool_size = None
    current_num_connections = threading.active_count()
    if self.instance_connector.type == 'sql':
        try:
            engine = self.instance_connector.engine
        except AttributeError:
            ### The base wheel has no SQLAlchemy engine (the `sql` extra): the Rust driver owns
            ### the pool, sized by the flavor's `pool_size`.
            engine = None
            try:
                engine_pool_size = int(
                    self.instance_connector._sys_config.get('create_engine', {}).get('pool_size')
                    or (os.cpu_count() or 5)
                )
            except (TypeError, ValueError):
                engine_pool_size = os.cpu_count() or 5
        if engine is not None:
            engine_pool_size = engine.pool.size()
            current_num_connections = engine.pool.checkedout()
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


