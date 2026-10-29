#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Connector types declared by native (Rust) plugins.

A native plugin declares connector types with `#[make_connector] mod readings { ... }` (or in a
native directory's manifest). The CLI and the engine resolve `readings:<label>` keys in the
core; `meerschaum.connectors.get_connector()` builds a `NativeConnector` façade for them whose
methods run in the core (`meerschaum._core.NativeConnector`), so no Python module is imported.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import meerschaum as mrsm
from meerschaum.connectors._Connector import Connector
from meerschaum.connectors.instance._InstanceConnector import InstanceConnector

__all__ = (
    'NativeConnector',
    'NativeInstanceConnector',
    'make_native_connector_class',
)


def _attributes(connector: Connector) -> Dict[str, Any]:
    """
    The attributes the native connector is built with (`meta` without `type` and `label`).
    """
    from meerschaum.core.Pipe._sync import _connector_attributes
    return _connector_attributes(connector)


class NativeConnector(Connector):
    """
    A connector of a type a native (Rust) plugin declares (`#[make_connector] mod <type>`).
    `fetch()`, `register()` and the module's other methods run in the core.
    """

    IS_NATIVE: bool = True
    NATIVE_TYPE: str = ''
    NATIVE_PLUGIN: str = ''
    NATIVE_METHODS: List[str] = []

    def __init__(self, label: Optional[str] = None, **kw: Any):
        self.__dict__['type'] = self.NATIVE_TYPE
        super().__init__(self.NATIVE_TYPE, label=label, **kw)
        self.__dict__['_native_connector'] = None

    @property
    def is_native(self) -> bool:
        """Whether this connector is implemented by a native plugin (always `True`)."""
        return True

    @property
    def plugin_name(self) -> str:
        """The native plugin which declares this connector type."""
        return self.NATIVE_PLUGIN

    def _core_native(self):
        """
        The `meerschaum._core.NativeConnector` behind this façade (built on first use, and again
        in a forked child).
        """
        import os
        held = self.__dict__.get('_native_connector', None)
        if held is not None and held[0] == os.getpid():
            return held[1]
        from meerschaum import _core
        native = _core.NativeConnector(self.type, self.label, _attributes(self))
        self.__dict__['_native_connector'] = (os.getpid(), native)
        return native

    def fetch(self, pipe: mrsm.Pipe, **kw: Any) -> Any:
        """
        Fetch the pipe's rows from the native connector (through the core's `pipe.fetch()`).
        """
        if str(pipe.connector_keys) != str(self):
            pipe = mrsm.Pipe(
                str(self),
                pipe.metric_key,
                pipe.location_key,
                instance=pipe.instance_keys,
                parameters=pipe.parameters,
                temporary=True,
            )
        return pipe.fetch(**kw)

    def test_connection(self, **kw: Any) -> bool:
        """The module's `test_connection()` (`True` when it does not define one)."""
        return self._core_native().test_connection()

    def call(self, method: str, pipe: Optional[mrsm.Pipe] = None, **kw: Any) -> Any:
        """
        Run `method` of the native connector module with the pipe and keyword arguments as its
        context. Returns `None` when the module does not define the method.
        """
        kw.pop('debug', None)
        result = self._core_native().call(method, pipe, kw or None)
        return None if result is NotImplemented else result

    def __getattr__(self, name: str) -> Any:
        ### Only reached for attributes not found normally: route the module's other methods.
        if name.startswith('_') or name not in type(self).NATIVE_METHODS:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )

        def _method(pipe: Optional[mrsm.Pipe] = None, **kw: Any) -> Any:
            return self.call(name, pipe, **kw)

        _method.__name__ = name
        return _method


def _register(self, pipe: mrsm.Pipe, **kw: Any) -> Any:
    """
    Run the native connector's `register(pipe)` in the core and return its parameters (`None`
    without a result; a non-dict return value is passed through so that `Pipe.register()` warns
    about it, as for a Python connector).
    """
    return self.call('register', pipe)


def _instance(method: str):
    def _call(self, pipe: mrsm.Pipe, debug: bool = False, **kw: Any) -> Any:
        return self._core_native().instance_call(method, pipe, **kw)
    _call.__name__ = method
    _call.__doc__ = f"`{method}()` of the native instance connector, run in the core."
    return _call


class NativeInstanceConnector(InstanceConnector, NativeConnector):
    """
    An instance connector of a type a native plugin declares (`#[make_connector(instance)]`).
    The pipe methods run through the plugin's instance functions in the core, as `-i <type>:<label>`
    does in the CLI; users, plugins and tokens are built atop pipes as for any instance connector.
    """

    IS_INSTANCE: bool = True

    register_pipe = _instance('register_pipe')
    get_pipe_id = _instance('get_pipe_id')
    get_pipe_attributes = _instance('get_pipe_attributes')
    edit_pipe = _instance('edit_pipe')
    delete_pipe = _instance('delete_pipe')
    pipe_exists = _instance('pipe_exists')
    drop_pipe = _instance('drop_pipe')
    drop_pipe_indices = _instance('drop_pipe_indices')
    create_pipe_indices = _instance('create_pipe_indices')
    clear_pipe = _instance('clear_pipe')
    deduplicate_pipe = _instance('deduplicate_pipe')
    get_pipe_data = _instance('get_pipe_data')
    get_pipe_rowcount = _instance('get_pipe_rowcount')
    get_sync_time = _instance('get_sync_time')
    get_pipe_columns_types = _instance('get_pipe_columns_types')
    get_pipe_columns_indices = _instance('get_pipe_columns_indices')
    get_pipe_index_names = _instance('get_pipe_index_names')
    get_pipe_size = _instance('get_pipe_size')
    compress_pipe = _instance('compress_pipe')
    decompress_pipe = _instance('decompress_pipe')
    vacuum_pipe = _instance('vacuum_pipe')
    analyze_pipe = _instance('analyze_pipe')
    partition_pipe = _instance('partition_pipe')

    def sync_pipe(self, pipe: mrsm.Pipe, df: Any = None, debug: bool = False, **kw: Any) -> Any:
        """`sync_pipe()` of the native instance connector, run in the core."""
        return self._core_native().instance_call('sync_pipe', pipe, df=df, **kw)

    def fetch_pipes_keys(
        self,
        connector_keys: Optional[List[str]] = None,
        metric_keys: Optional[List[str]] = None,
        location_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw: Any
    ) -> Any:
        """`fetch_pipes_keys()` of the native instance connector, run in the core."""
        return self._core_native().instance_call(
            'fetch_pipes_keys',
            None,
            connector_keys=connector_keys,
            metric_keys=metric_keys,
            location_keys=location_keys,
            tags=tags,
            params=params,
        )


def make_native_connector_class(connector_type: str, info: Dict[str, Any]) -> type:
    """
    Build the façade class of the native connector type `connector_type`
    (`info` is its entry of `meerschaum._core.native_connector_types()`).
    """
    methods = [str(method) for method in (info.get('methods', None) or [])]
    is_instance = bool(info.get('is_instance', False))
    base = NativeInstanceConnector if is_instance else NativeConnector
    namespace = {
        'NATIVE_TYPE': connector_type,
        'NATIVE_PLUGIN': str(info.get('plugin', '')),
        'NATIVE_METHODS': methods,
        'IS_INSTANCE': is_instance,
        'IS_THREAD_SAFE': bool(info.get('is_thread_safe', False)),
        'IS_STREAMING': bool(info.get('is_streaming', False)),
        '__module__': __name__,
    }
    ### As for a Python connector, `register` exists only when the module defines it
    ### (`Pipe.register()` checks for it).
    namespace['register'] = _register if 'register' in methods else None
    class_name = ''.join(part.capitalize() for part in connector_type.split('_')) + 'Connector'
    return type(class_name, (base,), namespace)
