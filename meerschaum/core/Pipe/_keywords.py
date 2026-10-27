#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Warn about keyword arguments `Pipe.get_data()` and `Pipe.get_backtrack_data()` do not know.

`get_data(**kw)` accepts any keyword (as in Meerschaum 4: they are forwarded to the instance
connector, and a custom instance connector may read its own), so a typo such as
`get_data(start=...)` or `get_data(where={...})` used to be ignored silently and every row
came back. Now each call that passes keywords nothing reads warns once, with a suggestion.
`get_backtrack_data(**kw)` works the same way. The check itself never raises; only warning
filters that turn the warning into an error (`-W error`) make it one, as for any warning.
"""

from __future__ import annotations

import difflib
import functools
import inspect
import os
import sys

from typing import FrozenSet

from meerschaum.utils.typing import Any, Callable, Dict, List, Optional, Tuple

#: Keywords the SQL instance reads from `**kw` (passed on to its data query).
SQL_KEYWORDS = frozenset({
    'begin_add_minutes', 'end_add_minutes', 'replace_nulls', 'skip_existing_cols_check',
})

#: Keywords that are accepted and ignored: the `Pipe.sync()` options `Pipe.copy_to()` passes
#: through to `get_data()`, `silent` and options of the Meerschaum 4 SQL read.
PASSTHROUGH_KEYWORDS = frozenset({
    'silent', 'chunksize', 'sort_datetimes', 'workers', 'force', 'retries', 'min_seconds',
    'check_existing', 'enforce_dtypes', 'blocking', 'callback', 'error_callback',
    'sync_chunks', 'replace', '_inplace',
})

#: The `get_data()` options `Pipe.get_backtrack_data()` reads from its `**kw`.
BACKTRACK_KEYWORDS = frozenset({
    'select_columns', 'omit_columns', 'end', 'as_docs', 'as_polars', 'add_missing_columns',
    'order',
})

#: Common names for what `get_data()` (or `get_backtrack_data()`) calls something else.
ALIASES = {
    'start': 'begin',
    'start_time': 'begin',
    'begin_time': 'begin',
    'since': 'begin',
    'after': 'begin',
    'stop': 'end',
    'end_time': 'end',
    'until': 'end',
    'before': 'end',
    'where': 'params',
    'filter': 'params',
    'filters': 'params',
    'query': 'params',
    'param': 'params',
    'conditions': 'params',
    'columns': 'select_columns',
    'cols': 'select_columns',
    'select': 'select_columns',
    'fields': 'select_columns',
    'exclude': 'omit_columns',
    'exclude_columns': 'omit_columns',
    'drop_columns': 'omit_columns',
    'omit': 'omit_columns',
    'n': 'limit',
    'nrows': 'limit',
    'max_rows': 'limit',
    'top': 'limit',
    'head': 'limit',
    'sort': 'order',
    'order_by': 'order',
    'ascending': 'order',
    'descending': 'order',
    'polars': 'as_polars',
    'docs': 'as_docs',
    'as_dicts': 'as_docs',
    'as_records': 'as_docs',
    'chunks': 'as_chunks',
    'iterator': 'as_iterator',
    'minutes': 'backtrack_minutes',
    'backtrack': 'backtrack_minutes',
    'lookback': 'backtrack_minutes',
    'lookback_minutes': 'backtrack_minutes',
}

#: Parameters never suggested (they do not select data).
_UNSUGGESTED = frozenset({'self', 'debug', 'fresh'})

#: Instance types the Rust core serves itself: their reads take no keywords of their own.
_NATIVE_INSTANCE_TYPES = frozenset({'sql', 'api', 'valkey'})

_PIPE_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
_THIS_FILE = os.path.abspath(__file__)


@functools.lru_cache(maxsize=None)
def _parameters(function: Callable[..., Any]) -> Tuple[FrozenSet[str], bool]:
    """The named parameters of `function` and whether it takes `**kwargs`."""
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return frozenset(), True
    names = frozenset(
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    )
    takes_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    return names, takes_kwargs


def _instance_parameters(
    pipe: Any,
    methods: Tuple[str, ...],
    first: bool = False,
) -> Optional[FrozenSet[str]]:
    """
    The keywords a custom (Python) instance connector's read `methods` name, or `None` when one
    of them takes `**kwargs` (it may read any keyword) or the connector cannot be inspected.
    With `first`, only the first of `methods` the connector defines is read (the worker calls
    the connector's own `get_backtrack_data()` if it has one, else `get_pipe_data()`).
    The instances the Rust core serves (`sql:`, `api:`, `valkey:`, native plugins) read none.
    """
    try:
        connector = pipe.instance_connector
    except Exception:
        return None
    if connector is None or isinstance(connector, str):
        return None
    if (
        getattr(connector, 'type', None) in _NATIVE_INSTANCE_TYPES
        or getattr(connector, 'IS_NATIVE', False)
    ):
        return frozenset()
    names = set()
    for method_name in methods:
        method = getattr(type(connector), method_name, None)
        if method is None:
            continue
        method_names, takes_kwargs = _parameters(method)
        if takes_kwargs:
            return None
        names.update(method_names)
        if first:
            break
    return frozenset(names)


def _suggestion(key: str, public: List[str]) -> Optional[str]:
    alias = ALIASES.get(key.lower(), None)
    if alias is not None and alias in public:
        return alias
    matches = difflib.get_close_matches(key, public, n=1, cutoff=0.6)
    return matches[0] if matches else None


def _caller(method: str) -> Tuple[int, str]:
    """
    The `stacklevel` (for `meerschaum.utils.warnings.warn`, called from the same function as
    this one) of the first frame outside `meerschaum/core/Pipe`, and the name of the `Pipe`
    method that frame called (`get_data`, `get_docs`, `get_backtrack_data`, ...; `method`
    when the check was called directly).
    """
    ### `warn()` passes `stacklevel` on to `warnings.warn()`: 2 is the function calling `warn()`.
    frame = sys._getframe(1)
    level = 2
    while frame is not None and frame.f_code.co_filename.startswith(_PIPE_PACKAGE_DIR):
        if os.path.abspath(frame.f_code.co_filename) != _THIS_FILE:
            method = frame.f_code.co_name
        frame = frame.f_back
        level += 1
    return level, method


def _warn_unknown(
    pipe: Any,
    kw: Dict[str, Any],
    method: str,
    named: FrozenSet[str],
    methods: Tuple[str, ...],
    first: bool = False,
) -> None:
    """
    Warn once about the keys of `kw` (the `**kw` of the `Pipe` method `method`) that neither
    `named` nor the instance connector's read `methods` read.
    """
    accepted = named | SQL_KEYWORDS | PASSTHROUGH_KEYWORDS
    unknown = [key for key in kw if key not in accepted]
    if not unknown:
        return
    instance_names = _instance_parameters(pipe, methods, first=first)
    if instance_names is None:
        return
    unknown = [key for key in unknown if key not in instance_names]
    if not unknown:
        return
    public = sorted(name for name in named if name not in _UNSUGGESTED)
    parts = []
    suggestions = []
    for key in unknown:
        suggestion = _suggestion(key, public)
        suggestions.append(suggestion)
        parts.append(f"'{key}'" + (f" (did you mean '{suggestion}'?)" if suggestion else ''))
    listed = parts[0] if len(parts) == 1 else ', '.join(parts[:-1]) + ' and ' + parts[-1]
    stacklevel, method = _caller(method)
    message = (
        f"{pipe}.{method}() ignored the unknown keyword argument"
        + ('s ' if len(unknown) != 1 else ' ')
        + listed + '.'
    )
    if 'params' in suggestions:
        message += " Filter rows with `params`, e.g. `params={'station': 'KATL'}`."
    from meerschaum.utils.warnings import warn
    warn(message, stacklevel=stacklevel)


def warn_unknown_get_data_keywords(
    pipe: Any,
    kw: Dict[str, Any],
    as_docs: bool = False,
) -> None:
    """
    Warn once about the keywords in `kw` (`Pipe.get_data(**kw)`) that nothing reads,
    suggesting the parameter that was probably meant. Never raises, except for the warning
    itself when the warning filters make it an error.
    """
    if not kw:
        return
    try:
        from meerschaum.core.Pipe._data import get_data
        named, _ = _parameters(get_data)
        _warn_unknown(
            pipe,
            kw,
            'get_data',
            named,
            ('get_pipe_data', 'get_pipe_docs') if as_docs else ('get_pipe_data',),
        )
    except Warning:
        raise
    except Exception:
        return


def warn_unknown_backtrack_keywords(pipe: Any, kw: Dict[str, Any]) -> None:
    """
    Warn once about the keywords in `kw` (`Pipe.get_backtrack_data(**kw)`) that nothing reads,
    suggesting the parameter that was probably meant. Never raises, except for the warning
    itself when the warning filters make it an error.
    """
    if not kw:
        return
    try:
        from meerschaum.core.Pipe._data import get_backtrack_data
        named, _ = _parameters(get_backtrack_data)
        _warn_unknown(
            pipe,
            kw,
            'get_backtrack_data',
            named | BACKTRACK_KEYWORDS,
            ('get_backtrack_data', 'get_pipe_data'),
            first=True,
        )
    except Warning:
        raise
    except Exception:
        return
