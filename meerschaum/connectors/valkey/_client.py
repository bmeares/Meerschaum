#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The commands plugins reach for through `ValkeyConnector.client`, run by the Rust client
(`meerschaum._core.ValkeyConnector.execute`) when the `valkey` package is not installed.

Strings come back as `bytes`, as `valkey.Valkey` returns them. Only the commands below exist;
anything else raises `NotImplementedError` (install `valkey` for the whole client).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple, Union


class ValkeyClient:
    """
    A small `valkey.Valkey`-like client over a `ValkeyConnector`.
    """

    def __init__(self, connector: 'meerschaum.connectors.valkey.ValkeyConnector'):
        self.connector = connector

    def _execute(self, command: str, *args: Any) -> Any:
        return self.connector._rust_client().execute(command, *args)

    def execute_command(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Run one of the supported commands."""
        return self._execute(command, *args)

    def ping(self) -> bool:
        return self._execute('PING')

    def get(self, name: str) -> Optional[bytes]:
        return self._execute('GET', name)

    def set(
        self,
        name: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
    ) -> Optional[bool]:
        args: List[Any] = [name, value]
        if ex is not None:
            args += ['EX', ex]
        if px is not None:
            args += ['PX', px]
        if nx:
            args.append('NX')
        return self._execute('SET', *args)

    def delete(self, *names: str) -> int:
        return self._execute('DEL', *names)

    def unlink(self, *names: str) -> int:
        return self._execute('UNLINK', *names)

    def exists(self, *names: str) -> int:
        return self._execute('EXISTS', *names)

    def incr(self, name: str) -> int:
        return self._execute('INCR', name)

    def type(self, name: str) -> bytes:
        return self._execute('TYPE', name)

    def keys(self, pattern: str = '*') -> List[bytes]:
        return self._execute('KEYS', pattern)

    def zadd(self, name: str, mapping: Dict[Any, float]) -> int:
        args: List[Any] = [name]
        for member, score in mapping.items():
            args += [score, member]
        return self._execute('ZADD', *args)

    def zrem(self, name: str, *values: Any) -> int:
        return self._execute('ZREM', name, *values)

    def zcard(self, name: str) -> int:
        return self._execute('ZCARD', name)

    def zrange(
        self,
        name: str,
        start: int,
        end: int,
        desc: bool = False,
        withscores: bool = False,
    ) -> Union[List[bytes], List[Tuple[bytes, float]]]:
        args: List[Any] = [name, start, end]
        if desc:
            args.append('REV')
        if withscores:
            args.append('WITHSCORES')
        return self._execute('ZRANGE', *args)

    def zrangebyscore(
        self,
        name: str,
        min: Union[int, float, str],
        max: Union[int, float, str],
    ) -> List[bytes]:
        return self._execute('ZRANGEBYSCORE', name, min, max)

    def sadd(self, name: str, *values: Any) -> int:
        return self._execute('SADD', name, *values)

    def srem(self, name: str, *values: Any) -> int:
        return self._execute('SREM', name, *values)

    def scard(self, name: str) -> int:
        return self._execute('SCARD', name)

    def smembers(self, name: str) -> Set[bytes]:
        return set(self._execute('SMEMBERS', name))

    def llen(self, name: str) -> int:
        return self._execute('LLEN', name)

    def rpush(self, name: str, *values: Any) -> int:
        return self._execute('RPUSH', name, *values)

    def lrange(self, name: str, start: int, end: int) -> List[bytes]:
        return self._execute('LRANGE', name, start, end)

    def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Any = None,
        mapping: Optional[Dict[Any, Any]] = None,
    ) -> int:
        args: List[Any] = [name]
        if key is not None:
            args += [key, value]
        for field, val in (mapping or {}).items():
            args += [field, val]
        return self._execute('HSET', *args)

    def hgetall(self, name: str) -> Dict[bytes, bytes]:
        return self._execute('HGETALL', name)

    def flushdb(self) -> bool:
        return self._execute('FLUSHDB')

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            raise AttributeError(name)
        raise NotImplementedError(
            f"The command '{name}' is not supported by the built-in Valkey client; "
            "install the `valkey` package for the full client."
        )
