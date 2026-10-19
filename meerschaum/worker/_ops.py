#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Worker operations. Each handler takes `(conn, call_id, args, frames)` and returns
`(value, frames)`; streaming handlers (`fetch`) send `chunk` messages before returning.

Every call into plugin code goes through the same v4 machinery the Python implementation uses
(`get_connector`, `Venv(get_connector_plugin(...))`, `filter_arguments` / `filter_keywords`), so
plugins receive exactly the arguments they received on v4.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from meerschaum.worker._arrow import ipc_to_df, is_dataframe, to_payload, df_to_ipc
from meerschaum.worker._server import Connection, WorkerCallError

Result = Tuple[Any, List[bytes]]


class BadRequest(Exception):
    """The host sent arguments this worker cannot use."""


def error_message(call_id: int, e: BaseException) -> Dict[str, Any]:
    """The `error` message for an exception raised by a handler."""
    if isinstance(e, NotImplementedError):
        kind = 'NotImplemented'
    elif isinstance(e, BadRequest):
        kind = 'BadRequest'
    else:
        kind = 'Exception'
    return {
        't': 'error',
        'id': call_id,
        'kind': kind,
        'message': str(e) or type(e).__name__,
        'exception': type(e).__name__,
        'traceback': ''.join(traceback.format_exception(type(e), e, e.__traceback__)),
    }


### -------------------------------------------------------------------------------------------
### Helpers
### -------------------------------------------------------------------------------------------

def _build_pipe(state: Optional[Dict[str, Any]]):
    """Reconstruct a v4 `Pipe` from `Pipe::to_state()` (keys plus in-memory parameters)."""
    import meerschaum as mrsm
    if not state:
        return None
    if not isinstance(state, dict):
        raise BadRequest("`pipe` must be a map of keys and parameters.")
    return mrsm.Pipe(
        state.get('connector_keys'),
        state.get('metric_key'),
        state.get('location_key', None),
        instance=state.get('instance_keys', None),
        parameters=(state.get('parameters', None) or {}),
        cache=False,
    )


def _pipe_state(pipe) -> Dict[str, Any]:
    try:
        parameters = pipe.parameters
    except Exception:
        parameters = pipe._attributes.get('parameters', {})
    return {
        'connector_keys': pipe.connector_keys,
        'metric_key': pipe.metric_key,
        'location_key': pipe.location_key,
        'instance_keys': pipe.instance_keys,
        'parameters': parameters,
    }


def _get_connector(keys: str, quiet: bool = False):
    """
    Build the connector named by `keys`. With `quiet`, warnings raised while building it are
    not printed, as v4's `Pipe.connector` does (`warnings.simplefilter('ignore')` around
    `parse_instance_keys`): the ops that act on a pipe's connector (describe, fetch, sync,
    subscribe, register) must not print e.g. "Cannot create Connector of type 'x'." for a pipe
    whose connector type is not installed. The warnings are kept and appended to the error when
    the connector cannot be built.
    """
    import meerschaum as mrsm
    if not keys or not isinstance(keys, str):
        raise BadRequest("Connector keys are required.")
    suppressed: List[str] = []
    if quiet:
        conn = _quietly(lambda: mrsm.get_connector(keys), suppressed)
    else:
        conn = mrsm.get_connector(keys)
    if conn is None:
        detail = ''.join(f"\n{line}" for line in suppressed)
        raise BadRequest(f"Could not build connector '{keys}'.{detail}")
    return conn


def _quietly(func: Callable[[], Any], suppressed: List[str]) -> Any:
    """
    Call `func` with warnings recorded instead of printed, appending their text to
    `suppressed`. Only warnings are captured (Meerschaum's stackless `warn()` is routed through
    `warnings` meanwhile); anything else written to stderr, e.g. by a plugin imported during the
    build, is left alone. `warnings.catch_warnings` is process-wide, so the window is this call.
    """
    import warnings
    import meerschaum.utils.warnings as mrsm_warnings
    from meerschaum.utils.misc import remove_ansi
    recording = mrsm_warnings._RECORDING
    mrsm_warnings._RECORDING = True
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            result = func()
    finally:
        mrsm_warnings._RECORDING = recording
    for warning in caught:
        for line in str(warning.message).splitlines():
            line = remove_ansi(line).strip()
            if line and line not in suppressed:
                suppressed.append(line)
    return result


def _kwargs(args: Dict[str, Any]) -> Dict[str, Any]:
    kwargs = args.get('kwargs', None) or {}
    if not isinstance(kwargs, dict):
        raise BadRequest("`kwargs` must be a map.")
    return {str(k): v for k, v in kwargs.items()}


def _clean_kwargs(kw: Dict[str, Any]) -> Dict[str, Any]:
    """The subset of keyword arguments that can travel back to the host."""
    out = {}
    for key, val in kw.items():
        if callable(val) and not isinstance(val, type):
            continue
        if key in ('df', 'chunk_hook', 'callback', 'error_callback', 'sync_method'):
            continue
        out[key] = val
    return out


def _encode_value(value: Any) -> Result:
    """Encode a plugin's return value: DataFrames become a data frame, tuples lists."""
    if is_dataframe(value):
        return {'kind': 'frame'}, [df_to_ipc(value)]
    if isinstance(value, tuple):
        return list(value), []
    return value, []


def _send_chunk(conn: Connection, call_id: int, chunk: Any) -> int:
    """Normalize and send one fetch chunk; return the number of chunks sent."""
    from meerschaum.utils.dataframe import df_is_chunk_generator
    if df_is_chunk_generator(chunk):
        count = 0
        for sub in chunk:
            count += _send_chunk(conn, call_id, sub)
        return count
    kind, value, frames = to_payload(chunk)
    if kind == 'none':
        return 0
    conn.send({'t': 'chunk', 'id': call_id, 'kind': kind, 'value': value}, frames)
    return 1


def _sync_callback(conn: Connection, pipe, df: Any, kw: Dict[str, Any]) -> Tuple[bool, str]:
    """Forward a plugin's `pipe.sync(df, **kw)` to the host's native sync path."""
    from meerschaum.utils.dataframe import df_is_chunk_generator
    if df is None:
        return (
            False,
            f"You passed `None` instead of data into `sync()` for {pipe}.\n"
            + "Omit the DataFrame to infer fetching.",
        )
    if hasattr(df, 'MRSM_INFER_FETCH'):
        value, _ = conn.callback('sync', {
            'pipe': _pipe_state(pipe),
            'kind': 'infer',
            'value': None,
            'kwargs': _clean_kwargs(kw),
        })
        return _as_tuple(value)

    if df_is_chunk_generator(df):
        results = [_sync_callback(conn, pipe, chunk, kw) for chunk in df]
        if not results:
            return True, "Received an empty generator; nothing to do."
        success = all(s for s, _ in results)
        return success, '\n\n'.join(m for _, m in results)

    kind, value, frames = to_payload(df)
    value, _ = conn.callback(
        'sync',
        {'pipe': _pipe_state(pipe), 'kind': kind, 'value': value, 'kwargs': _clean_kwargs(kw)},
        frames,
    )
    return _as_tuple(value)


def _as_tuple(value: Any) -> Tuple[bool, str]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return bool(value[0]), str(value[1])
    return False, f"Invalid sync result from the host: {value}"


def _patch_pipe_sync(conn: Connection, pipe) -> None:
    """Route `pipe.sync(df)` calls made by plugin code back to the host."""
    from meerschaum.core.Pipe._sync import InferFetch

    def sync(df=InferFetch, **kw):
        return _sync_callback(conn, pipe, df, kw)

    pipe.sync = sync


def _date_bounds(args: Dict[str, Any], kw: Dict[str, Any]) -> Dict[str, Any]:
    """`begin` / `end` from the call (v4 always passes both, `None` when unbounded)."""
    kw.pop('begin', None)
    kw.pop('end', None)
    return {'begin': args.get('begin', None), 'end': args.get('end', None)}


### -------------------------------------------------------------------------------------------
### Handlers
### -------------------------------------------------------------------------------------------

def op_ping(conn: Connection, call_id: int, args: Dict[str, Any], frames: List[bytes]) -> Result:
    import os
    return {'pong': True, 'pid': os.getpid(), 'echo': args.get('echo', None)}, []


def op_describe_connector(conn, call_id, args, frames) -> Result:
    """Describe a connector's capabilities (which optional methods it implements)."""
    connector = _get_connector(args.get('connector'), quiet=True)
    methods = sorted(
        name for name in dir(connector)
        if not name.startswith('_') and callable(getattr(connector, name, None))
    )
    return {
        'type': getattr(connector, 'type', None),
        'label': getattr(connector, 'label', None),
        'class_name': type(connector).__name__,
        'is_instance': bool(getattr(connector, 'IS_INSTANCE', False)),
        'is_thread_safe': bool(getattr(connector, 'IS_THREAD_SAFE', False)),
        'is_streaming': bool(getattr(connector, 'IS_STREAMING', False)),
        'methods': methods,
        'has_fetch': getattr(connector, 'fetch', None) is not None,
        'has_sync': getattr(connector, 'sync', None) is not None,
        'has_register': getattr(connector, 'register', None) is not None,
        'has_subscribe': getattr(connector, 'subscribe', None) is not None,
    }, []


def op_fetch(conn, call_id, args, frames) -> Result:
    """
    `Pipe.fetch()` for a plugin or custom connector: call `connector.fetch(pipe, begin, end,
    ...)` and stream each chunk back. A generator (of any accepted shape, nested or not) becomes
    one `chunk` message per item.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_arguments
    from meerschaum.utils.dataframe import df_is_chunk_generator

    connector = _get_connector(args.get('connector'), quiet=True)
    fetch_function = getattr(connector, 'fetch', None)
    if fetch_function is None:
        raise NotImplementedError(f"No `fetch()` function defined for connector '{connector}'")
    pipe = _build_pipe(args.get('pipe'))
    _patch_pipe_sync(conn, pipe)
    kw = _kwargs(args)
    debug = bool(kw.pop('debug', False))
    if args.get('params', None) is not None:
        kw['params'] = args['params']
    sync_chunks = bool(kw.pop('sync_chunks', False))
    ### `check_existing` is a named parameter of v4's `Pipe.fetch()`, never passed to `fetch()`.
    kw.pop('check_existing', None)
    try:
        kw['workers'] = pipe.get_num_workers(kw.get('workers', None))
    except Exception:
        pass

    bounds = _date_bounds(args, kw)
    chunk_hook = None
    if sync_chunks:
        def chunk_hook(chunk, **_kw):
            """
            v4's `Pipe.fetch()` chunk hook (wrapping `Pipe.sync()` with the chunk label prepended);
            the sync itself runs natively on the host.
            """
            from meerschaum.config._patch import apply_patch_to_config
            merged = apply_patch_to_config(kw, _kw)
            chunk_success, chunk_message = _sync_callback(conn, pipe, chunk, merged)
            try:
                chunk_label = pipe._get_chunk_label(chunk, pipe.columns.get('datetime', None))
            except Exception:
                chunk_label = None
            if chunk_label:
                chunk_message = '\n' + chunk_label + '\n' + chunk_message
            return chunk_success, chunk_message

    with Venv(get_connector_plugin(connector), debug=debug):
        call_args, call_kwargs = filter_arguments(
            fetch_function,
            pipe,
            chunk_hook=chunk_hook,
            debug=debug,
            **bounds,
            **kw
        )
        df = fetch_function(*call_args, **call_kwargs)

        if df is True:
            return {'chunks': 0, 'parallel': True, 'hook_results': None}, []
        if df is None:
            return {'chunks': 0, 'parallel': False, 'hook_results': None, 'none': True}, []
        if isinstance(df, list) and df and isinstance(df[0], tuple):
            ### A list of chunk-hook results.
            return {
                'chunks': 0,
                'parallel': False,
                'hook_results': [[bool(s), str(m)] for s, m in df],
            }, []
        if df_is_chunk_generator(df):
            count = 0
            for chunk in df:
                count += _send_chunk(conn, call_id, chunk)
            return {'chunks': count, 'parallel': False, 'hook_results': None, 'generator': True}, []

    count = _send_chunk(conn, call_id, df)
    return {'chunks': count, 'parallel': False, 'hook_results': None}, []


def op_sync(conn, call_id, args, frames) -> Result:
    """
    Call a plugin's (or custom connector's) `sync(pipe, **kw)`. Calls to `pipe.sync(df)` made by
    the plugin are forwarded to the host as `sync` callbacks. With `keep_alive`, the worker keeps
    serving after `sync()` returns, so v4-pattern streaming connectors (subscribe and return,
    callbacks call `pipe.sync()`) keep working.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_arguments

    connector = _get_connector(args.get('connector'), quiet=True)
    sync_function = getattr(connector, 'sync', None)
    if sync_function is None:
        raise NotImplementedError(f"No `sync()` function defined for connector '{connector}'")
    pipe = _build_pipe(args.get('pipe'))
    _patch_pipe_sync(conn, pipe)
    kw = _kwargs(args)
    debug = bool(kw.pop('debug', False))
    kw.update(_date_bounds(args, kw))
    try:
        with Venv(get_connector_plugin(connector), debug=debug):
            call_args, call_kwargs = filter_arguments(sync_function, pipe, debug=debug, **kw)
            return_tuple = sync_function(*call_args, **call_kwargs)
        if not isinstance(return_tuple, tuple):
            return_tuple = (
                False,
                f"Plugin '{connector.label}' returned non-tuple value: {return_tuple}"
            )
    except Exception as e:
        traceback.print_exc()
        return_tuple = (False, f"Failed to sync {pipe} with exception: '" + str(e) + "'")
    return [bool(return_tuple[0]), str(return_tuple[1])], []


def op_subscribe(conn, call_id, args, frames) -> Result:
    """
    Call `connector.subscribe(pipe, sink)` for streaming connectors (`IS_STREAMING = True`).
    `sink(df)` forwards each batch to the host as a `push` callback and returns its result.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_arguments

    connector = _get_connector(args.get('connector'), quiet=True)
    subscribe = getattr(connector, 'subscribe', None)
    if subscribe is None:
        raise NotImplementedError(f"No `subscribe()` function defined for connector '{connector}'")
    pipe = _build_pipe(args.get('pipe'))
    _patch_pipe_sync(conn, pipe)
    kw = _kwargs(args)

    class Sink:
        """The batch sink handed to `subscribe()`."""

        def __call__(self, df: Any, **_kw) -> Tuple[bool, str]:
            kind, value, data = to_payload(df)
            result, _ = conn.callback(
                'push',
                {'pipe': _pipe_state(pipe), 'kind': kind, 'value': value, 'kwargs': _clean_kwargs(_kw)},
                data,
            )
            return _as_tuple(result)

        push = __call__

    with Venv(get_connector_plugin(connector)):
        call_args, call_kwargs = filter_arguments(subscribe, pipe, Sink(), **kw)
        result = subscribe(*call_args, **call_kwargs)
    return _encode_value(result)


def _print_exception() -> None:
    """Print the exception being handled as v4 does (`get_console().print_exception()`)."""
    try:
        from meerschaum.utils.formatting import get_console
        console = get_console()
    except Exception:
        console = None
    if console is None:
        traceback.print_exc()
        return
    try:
        console.print_exception()
    except Exception:
        traceback.print_exc()


def op_register(conn, call_id, args, frames) -> Result:
    """The connector half of `Pipe.register()`: `connector.register(pipe)` → parameters."""
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    connector = _get_connector(args.get('connector'), quiet=True)
    register = getattr(connector, 'register', None)
    if register is None:
        return {'parameters': {}, 'valid': True, 'defined': False}, []
    pipe = _build_pipe(args.get('pipe'))
    try:
        with Venv(get_connector_plugin(connector)):
            params = register(pipe)
    except Exception:
        ### PARITY: `core/Pipe/_register.py`: `get_console().print_exception()` (a rich
        ### traceback); a plain one when `rich` is not installed. Both go to stderr: a worker's
        ### stdout is its stderr (`python/host.py`, never an MCP host's protocol stdout).
        _print_exception()
        params = None
    params = {} if params is None else params
    if not isinstance(params, dict):
        return {'parameters': {}, 'valid': False, 'defined': True, 'repr': str(params)}, []
    return {'parameters': params, 'valid': True, 'defined': True}, []


def op_setup(conn, call_id, args, frames) -> Result:
    """`Plugin(name).setup(*args, **kw)`."""
    import meerschaum as mrsm
    name = args.get('plugin', None)
    if not name:
        raise BadRequest("`plugin` is required.")
    plugin = mrsm.Plugin(name)
    kw = _kwargs(args)
    debug = bool(kw.pop('debug', False))
    result = plugin.setup(*(args.get('args', None) or []), debug=debug, **kw)
    if isinstance(result, tuple):
        return [bool(result[0]), str(result[1])], []
    return bool(result), []


def op_action(conn, call_id, args, frames) -> Result:
    """
    Run a plugin action exactly as v4's `entry_with_args()` does: resolve the function with
    `get_action()`, strip the leading action words, and call it inside the plugin's venv with
    `filter_keywords()` (`_do_action_wrapper`), honoring `--schedule`. Only plugin actions run
    here; core actions, the shell, `-d` and `api:` executors are handled natively by the host.
    """
    import functools
    from meerschaum.plugins import load_plugins
    from meerschaum.actions import get_action, _custom_actions_plugins
    from meerschaum._internal.arguments import remove_leading_action
    from meerschaum._internal.entry import _do_action_wrapper
    from meerschaum.utils.typing import is_success_tuple
    from meerschaum.utils.venv import active_venvs, deactivate_venv

    load_plugins(skip_if_loaded=True)
    if args.get('sysargs', None) is not None:
        from meerschaum._internal.arguments import parse_arguments
        kw = parse_arguments(list(args['sysargs']))
    else:
        kw = _kwargs(args)
    kw.update(_kwargs({'kwargs': args.get('patch', None) or {}}))
    action = list(kw.get('action', None) or [])
    if not action:
        raise BadRequest("No action was provided.")
    kw['action'] = action

    action_function = get_action(action)
    module_name = getattr(action_function, '__module__', '') or ''
    is_plugin_action = action_function is not None and (
        module_name.startswith('plugins.')
        or getattr(action_function, '__name__', None) in _custom_actions_plugins
    )
    if not is_plugin_action:
        raise BadRequest(f"'{' '.join(action)}' is not a plugin action.")

    ### PARITY: like `entry_with_args()`, only `plugins.*` modules get their venv activated.
    plugin_name = module_name.split('.')[1] if module_name.startswith('plugins.') else None
    kw['action'] = remove_leading_action(kw['action'])
    do_action = functools.partial(_do_action_wrapper, action_function, plugin_name, **kw)

    if kw.get('schedule', None):
        import time
        from datetime import timedelta
        from meerschaum.utils.schedule import schedule_function
        from meerschaum.utils.misc import interval_str
        start_time = time.perf_counter()
        schedule_function(do_action, **kw)
        delta = timedelta(seconds=(time.perf_counter() - start_time))
        result = True, f"Exited scheduler after {interval_str(delta)}."
    else:
        result = do_action()

    for venv in [venv for venv in active_venvs]:
        deactivate_venv(venv, debug=kw.get('debug', False), force=True)

    if not is_success_tuple(result):
        ### PARITY: `entry_with_args()` wraps non-tuple results as `(True, str(result))`.
        return [True, str(result)], []
    return [bool(result[0]), str(result[1])], []


def _run_hooks(conn, args, is_pre: bool) -> Result:
    """
    v4's `sync pipes` hook loop (`actions/sync.py`, `call_sync_hook`): every registered hook runs
    in its plugin's venv with `filter_keywords(hook, **sync_kwargs)`. A non-tuple return counts
    as `(True, "Success")`; an exception becomes a failure with the traceback.
    """
    import meerschaum as mrsm
    from meerschaum.plugins import load_plugins, _pre_sync_hooks, _post_sync_hooks
    from meerschaum.utils.misc import filter_keywords
    from meerschaum.utils.typing import is_success_tuple
    from meerschaum.utils.warnings import warn

    load_plugins(skip_if_loaded=True)
    pipe = _build_pipe(args.get('pipe'))
    _patch_pipe_sync(conn, pipe)
    sync_kwargs = _kwargs(args)
    if isinstance(sync_kwargs.get('success_tuple', None), list):
        sync_kwargs['success_tuple'] = tuple(sync_kwargs['success_tuple'])
    ### v4 passes the bound sync method (here: forwarded to the host).
    sync_kwargs.setdefault('sync_method', pipe.sync)
    only = args.get('plugins', None)

    results = []
    hooks = _pre_sync_hooks if is_pre else _post_sync_hooks
    for module_name, sync_hooks in list(hooks.items()):
        plugin_name = (
            module_name.split('.')[1]
            if module_name and module_name.startswith('plugins.')
            else module_name
        )
        if only is not None and plugin_name not in only:
            continue
        for sync_hook in sync_hooks:
            plugin = mrsm.Plugin(plugin_name) if plugin_name else None
            hook_result = (True, "Success")
            with mrsm.Venv(plugin):
                try:
                    _result = sync_hook(pipe, **filter_keywords(sync_hook, **sync_kwargs))
                    if is_success_tuple(_result):
                        hook_result = _result
                except Exception:
                    msg = (
                        f"Failed to execute sync hook '{sync_hook.__name__}' "
                        + f"from plugin '{plugin}':\n{traceback.format_exc()}"
                    )
                    warn(msg, stack=False)
                    hook_result = (False, msg)
            results.append({
                'plugin': plugin_name,
                'hook': getattr(sync_hook, '__name__', str(sync_hook)),
                'success': bool(hook_result[0]),
                'message': str(hook_result[1]),
            })
    return results, []


def op_pre_sync_hook(conn, call_id, args, frames) -> Result:
    return _run_hooks(conn, args, True)


def op_post_sync_hook(conn, call_id, args, frames) -> Result:
    return _run_hooks(conn, args, False)


def op_build_manifest(conn, call_id, args, frames) -> Result:
    from meerschaum.worker._manifest import build_manifest
    return build_manifest(debug=bool(args.get('debug', False))), []


def op_instance(conn, call_id, args, frames) -> Result:
    """
    Call an instance-connector method on a custom (`IS_INSTANCE = True`) connector:
    `method(pipe, *args, **kwargs)` with `filter_arguments`, inside the connector's plugin venv.
    A data frame sent with the call is passed as the keyword named by `frame_arg` (e.g. `df`
    for `sync_pipe`); a DataFrame result is returned as a data frame.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_arguments

    connector = _get_connector(args.get('connector'))
    method_name = args.get('method', None)
    if not isinstance(method_name, str) or not method_name or method_name.startswith('_'):
        raise BadRequest(f"Invalid method name '{method_name}'.")
    method = getattr(connector, method_name, None)
    if method is None or not callable(method):
        raise NotImplementedError(
            f"`{method_name}()` is not implemented for connector '{connector}'."
        )
    kw = _kwargs(args)
    frame_arg = args.get('frame_arg', None)
    if frame_arg and frames:
        kw[str(frame_arg)] = ipc_to_df(frames[0])
    positional = list(args.get('args', None) or [])
    pipe = _build_pipe(args.get('pipe')) if args.get('pipe', None) else None
    if pipe is not None:
        positional.insert(0, pipe)
    with Venv(get_connector_plugin(connector)):
        call_args, call_kwargs = filter_arguments(method, *positional, **kw)
        ### A method that does not take a keyword the host requires (`remote` for a remote
        ### rowcount) cannot answer the call.
        for required in (args.get('require_kwargs', None) or []):
            if required not in call_kwargs:
                raise NotImplementedError(
                    args.get('unsupported_message', None)
                    or f"`{method_name}()` does not take `{required}`."
                )
        value = method(*call_args, **call_kwargs)
    return _encode_value(value)


def op_shutdown(conn, call_id, args, frames) -> Result:
    return True, []


def op_crash(conn, call_id, args, frames) -> Result:
    """Exit immediately (only when `MRSM_WORKER_ALLOW_CRASH` is set; used by the tests)."""
    import os
    if not os.environ.get('MRSM_WORKER_ALLOW_CRASH'):
        raise BadRequest("Crashing is disabled.")
    os._exit(int(args.get('code', 70)))


HANDLERS = {
    'ping': op_ping,
    'shutdown': op_shutdown,
    'crash': op_crash,
    'describe_connector': op_describe_connector,
    'fetch': op_fetch,
    'sync': op_sync,
    'subscribe': op_subscribe,
    'register': op_register,
    'setup': op_setup,
    'action': op_action,
    'pre_sync_hook': op_pre_sync_hook,
    'post_sync_hook': op_post_sync_hook,
    'build_manifest': op_build_manifest,
    'instance': op_instance,
}
