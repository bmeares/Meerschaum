#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
MessagePack encoding for the worker protocol.

Uses the `msgpack` package when it is importable and falls back to a small stdlib-only codec
otherwise (the worker must run in a bare interpreter with only `pyarrow`). Both paths produce
the same bytes for the types the protocol uses.

Rich scalars travel as MessagePack extension types (see `crates/meerschaum-worker/README.md`):

| Code | Type | Payload |
|---|---|---|
| 1 | `datetime` (aware or naive) | `isoformat()` as UTF-8 |
| 2 | `date` | `isoformat()` as UTF-8 |
| 3 | `Decimal` | `str()` as UTF-8 |
| 4 | `UUID` | the 16 raw bytes |
| 5 | geometry | WKB bytes |
| 6 | `timedelta` | total seconds as a UTF-8 float string |
"""

from __future__ import annotations

import struct
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from typing import Any, Callable, Optional

EXT_DATETIME = 1
EXT_DATE = 2
EXT_DECIMAL = 3
EXT_UUID = 4
EXT_GEOMETRY = 5
EXT_TIMEDELTA = 6


class ExtType:
    """A MessagePack extension value (`code`, `data`)."""

    __slots__ = ('code', 'data')

    def __init__(self, code: int, data: bytes):
        self.code = code
        self.data = data

    def __eq__(self, other):
        return (
            isinstance(other, ExtType)
            and other.code == self.code
            and other.data == self.data
        )

    def __repr__(self):
        return f"ExtType({self.code}, {self.data!r})"


def _is_na(value: Any) -> bool:
    """`pd.isna()` for scalars without importing pandas."""
    if value is None:
        return True
    type_name = type(value).__name__
    if type_name in ('NAType', 'NaTType'):
        return True
    return False


def to_ext(value: Any) -> Any:
    """
    Convert a non-native value into something the codec can pack: an `ExtType`, a native
    container, or a string fallback.
    """
    if _is_na(value):
        return None
    if isinstance(value, datetime):
        return ExtType(EXT_DATETIME, value.isoformat().encode('utf-8'))
    if isinstance(value, date):
        return ExtType(EXT_DATE, value.isoformat().encode('utf-8'))
    if isinstance(value, Decimal):
        return ExtType(EXT_DECIMAL, str(value).encode('utf-8'))
    if isinstance(value, UUID):
        return ExtType(EXT_UUID, value.bytes)
    if isinstance(value, timedelta):
        return ExtType(EXT_TIMEDELTA, repr(value.total_seconds()).encode('utf-8'))
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, (set, frozenset)):
        return list(value)

    module = type(value).__module__ or ''
    if module.startswith('numpy'):
        if hasattr(value, 'tolist'):
            return value.tolist()
    if module.startswith('pandas'):
        if hasattr(value, 'to_pydatetime'):
            return to_ext(value.to_pydatetime())
        if hasattr(value, 'to_pytimedelta'):
            return to_ext(value.to_pytimedelta())
    if module.startswith('shapely'):
        wkb = getattr(value, 'wkb', None)
        if isinstance(wkb, bytes):
            return ExtType(EXT_GEOMETRY, wkb)
    if hasattr(value, 'model_dump'):
        return value.model_dump()
    return str(value)


def from_ext(code: int, data: bytes) -> Any:
    """Decode an extension value into its Python type."""
    if code == EXT_DATETIME:
        return datetime.fromisoformat(data.decode('utf-8'))
    if code == EXT_DATE:
        return date.fromisoformat(data.decode('utf-8'))
    if code == EXT_DECIMAL:
        return Decimal(data.decode('utf-8'))
    if code == EXT_UUID:
        return UUID(bytes=bytes(data))
    if code == EXT_TIMEDELTA:
        return timedelta(seconds=float(data.decode('utf-8')))
    if code == EXT_GEOMETRY:
        try:
            import shapely
            return shapely.from_wkb(bytes(data))
        except Exception:
            return bytes(data)
    return ExtType(code, bytes(data))


### -------------------------------------------------------------------------------------------
### Stdlib fallback codec
### -------------------------------------------------------------------------------------------

def _is_ext(obj: Any) -> bool:
    """
    Whether `obj` is an extension value: this module's `ExtType` or the `msgpack` package's
    (a namedtuple, so it must be checked before tuples).
    """
    if isinstance(obj, ExtType):
        return True
    return (
        type(obj).__name__ == 'ExtType'
        and isinstance(getattr(obj, 'code', None), int)
        and isinstance(getattr(obj, 'data', None), bytes)
    )


def _pack(obj: Any, out: bytearray, default: Callable[[Any], Any], depth: int = 0) -> None:
    if depth > 512:
        raise ValueError("Object is too deeply nested to pack.")
    if obj is None:
        out.append(0xc0)
    elif obj is True:
        out.append(0xc3)
    elif obj is False:
        out.append(0xc2)
    elif isinstance(obj, int):
        if 0 <= obj < 0x80:
            out.append(obj)
        elif -32 <= obj < 0:
            out.append(obj & 0xff)
        elif 0 <= obj <= 0xff:
            out += b'\xcc' + struct.pack('>B', obj)
        elif 0 <= obj <= 0xffff:
            out += b'\xcd' + struct.pack('>H', obj)
        elif 0 <= obj <= 0xffffffff:
            out += b'\xce' + struct.pack('>I', obj)
        elif 0 <= obj <= 0xffffffffffffffff:
            out += b'\xcf' + struct.pack('>Q', obj)
        elif -0x80 <= obj < 0:
            out += b'\xd0' + struct.pack('>b', obj)
        elif -0x8000 <= obj < 0:
            out += b'\xd1' + struct.pack('>h', obj)
        elif -0x80000000 <= obj < 0:
            out += b'\xd2' + struct.pack('>i', obj)
        elif -0x8000000000000000 <= obj < 0:
            out += b'\xd3' + struct.pack('>q', obj)
        else:
            _pack(str(obj), out, default, depth + 1)
    elif isinstance(obj, float):
        out += b'\xcb' + struct.pack('>d', obj)
    elif isinstance(obj, str):
        data = obj.encode('utf-8', 'surrogatepass')
        n = len(data)
        if n < 32:
            out.append(0xa0 | n)
        elif n <= 0xff:
            out += b'\xd9' + struct.pack('>B', n)
        elif n <= 0xffff:
            out += b'\xda' + struct.pack('>H', n)
        else:
            out += b'\xdb' + struct.pack('>I', n)
        out += data
    elif isinstance(obj, bytes):
        n = len(obj)
        if n <= 0xff:
            out += b'\xc4' + struct.pack('>B', n)
        elif n <= 0xffff:
            out += b'\xc5' + struct.pack('>H', n)
        else:
            out += b'\xc6' + struct.pack('>I', n)
        out += obj
    elif _is_ext(obj):
        n = len(obj.data)
        fixed = {1: 0xd4, 2: 0xd5, 4: 0xd6, 8: 0xd7, 16: 0xd8}
        if n in fixed:
            out.append(fixed[n])
        elif n <= 0xff:
            out += b'\xc7' + struct.pack('>B', n)
        elif n <= 0xffff:
            out += b'\xc8' + struct.pack('>H', n)
        else:
            out += b'\xc9' + struct.pack('>I', n)
        out += struct.pack('>b', obj.code)
        out += obj.data
    elif isinstance(obj, (list, tuple)):
        n = len(obj)
        if n < 16:
            out.append(0x90 | n)
        elif n <= 0xffff:
            out += b'\xdc' + struct.pack('>H', n)
        else:
            out += b'\xdd' + struct.pack('>I', n)
        for item in obj:
            _pack(item, out, default, depth + 1)
    elif isinstance(obj, dict):
        n = len(obj)
        if n < 16:
            out.append(0x80 | n)
        elif n <= 0xffff:
            out += b'\xde' + struct.pack('>H', n)
        else:
            out += b'\xdf' + struct.pack('>I', n)
        for key, val in obj.items():
            _pack(key, out, default, depth + 1)
            _pack(val, out, default, depth + 1)
    else:
        converted = default(obj)
        if converted is obj:
            raise TypeError(f"Cannot pack object of type {type(obj)}.")
        _pack(converted, out, default, depth + 1)


class _Reader:
    __slots__ = ('data', 'pos')

    def __init__(self, data: bytes):
        self.data = memoryview(data)
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise ValueError("Truncated MessagePack data.")
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return bytes(chunk)

    def unpack(self, fmt: str, n: int):
        return struct.unpack(fmt, self.take(n))[0]


def _unpack(r: _Reader, ext_hook: Callable[[int, bytes], Any]) -> Any:
    b = r.take(1)[0]
    if b <= 0x7f:
        return b
    if b >= 0xe0:
        return b - 0x100
    if 0xa0 <= b <= 0xbf:
        return r.take(b & 0x1f).decode('utf-8', 'surrogatepass')
    if 0x90 <= b <= 0x9f:
        return [_unpack(r, ext_hook) for _ in range(b & 0x0f)]
    if 0x80 <= b <= 0x8f:
        return _unpack_map(r, b & 0x0f, ext_hook)
    if b == 0xc0:
        return None
    if b == 0xc2:
        return False
    if b == 0xc3:
        return True
    if b == 0xc4:
        return r.take(r.unpack('>B', 1))
    if b == 0xc5:
        return r.take(r.unpack('>H', 2))
    if b == 0xc6:
        return r.take(r.unpack('>I', 4))
    if b in (0xc7, 0xc8, 0xc9):
        n = r.unpack({0xc7: '>B', 0xc8: '>H', 0xc9: '>I'}[b], {0xc7: 1, 0xc8: 2, 0xc9: 4}[b])
        code = r.unpack('>b', 1)
        return ext_hook(code, r.take(n))
    if b == 0xca:
        return r.unpack('>f', 4)
    if b == 0xcb:
        return r.unpack('>d', 8)
    if b == 0xcc:
        return r.unpack('>B', 1)
    if b == 0xcd:
        return r.unpack('>H', 2)
    if b == 0xce:
        return r.unpack('>I', 4)
    if b == 0xcf:
        return r.unpack('>Q', 8)
    if b == 0xd0:
        return r.unpack('>b', 1)
    if b == 0xd1:
        return r.unpack('>h', 2)
    if b == 0xd2:
        return r.unpack('>i', 4)
    if b == 0xd3:
        return r.unpack('>q', 8)
    if b in (0xd4, 0xd5, 0xd6, 0xd7, 0xd8):
        n = {0xd4: 1, 0xd5: 2, 0xd6: 4, 0xd7: 8, 0xd8: 16}[b]
        code = r.unpack('>b', 1)
        return ext_hook(code, r.take(n))
    if b == 0xd9:
        return r.take(r.unpack('>B', 1)).decode('utf-8', 'surrogatepass')
    if b == 0xda:
        return r.take(r.unpack('>H', 2)).decode('utf-8', 'surrogatepass')
    if b == 0xdb:
        return r.take(r.unpack('>I', 4)).decode('utf-8', 'surrogatepass')
    if b == 0xdc:
        return [_unpack(r, ext_hook) for _ in range(r.unpack('>H', 2))]
    if b == 0xdd:
        return [_unpack(r, ext_hook) for _ in range(r.unpack('>I', 4))]
    if b == 0xde:
        return _unpack_map(r, r.unpack('>H', 2), ext_hook)
    if b == 0xdf:
        return _unpack_map(r, r.unpack('>I', 4), ext_hook)
    raise ValueError(f"Invalid MessagePack byte 0x{b:02x}.")


def _unpack_map(r: _Reader, n: int, ext_hook) -> dict:
    out = {}
    for _ in range(n):
        key = _unpack(r, ext_hook)
        if isinstance(key, list):
            key = tuple(key)
        out[key] = _unpack(r, ext_hook)
    return out


def _fallback_packb(obj: Any, default: Callable[[Any], Any]) -> bytes:
    out = bytearray()
    _pack(obj, out, default)
    return bytes(out)


def _fallback_unpackb(data: bytes, ext_hook: Callable[[int, bytes], Any]) -> Any:
    r = _Reader(data)
    value = _unpack(r, ext_hook)
    if r.pos != len(r.data):
        raise ValueError("Extra bytes after MessagePack value.")
    return value


### -------------------------------------------------------------------------------------------
### Public API
### -------------------------------------------------------------------------------------------

def _load_msgpack():
    import os
    if os.environ.get('MRSM_WORKER_PURE_MSGPACK'):
        return None
    try:
        import msgpack
        return msgpack
    except Exception:
        return None


_msgpack = _load_msgpack()
USING_FALLBACK: bool = _msgpack is None


def _default(obj: Any) -> Any:
    converted = to_ext(obj)
    if _msgpack is not None and isinstance(converted, ExtType):
        return _msgpack.ExtType(converted.code, converted.data)
    return converted


def packb(obj: Any, default: Optional[Callable[[Any], Any]] = None) -> bytes:
    """Serialize `obj` to MessagePack bytes, converting rich scalars to extension types."""
    default = default or _default
    if _msgpack is not None:
        return _msgpack.packb(obj, default=default, use_bin_type=True, strict_types=False)
    return _fallback_packb(obj, default)


def unpackb(data: bytes) -> Any:
    """Deserialize MessagePack bytes, decoding the protocol's extension types."""
    if _msgpack is not None:
        return _msgpack.unpackb(
            data,
            raw=False,
            ext_hook=from_ext,
            strict_map_key=False,
            use_list=True,
        )
    return _fallback_unpackb(data, from_ext)
