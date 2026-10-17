#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wire framing for the worker protocol.

Every frame is a 5-byte header followed by the payload:

    u32 big-endian payload length | u8 kind | payload

`kind` is `1` for a MessagePack control message and `2` for an Arrow IPC stream (one
self-contained stream: schema, record batches, end-of-stream marker). A control message whose
`frames` field is `n` is immediately followed by `n` data frames.
"""

from __future__ import annotations

import struct
from typing import Any, BinaryIO, Dict, List, Tuple

from meerschaum.worker._msgpack import packb, unpackb

KIND_CONTROL = 1
KIND_ARROW = 2
HEADER = struct.Struct('>IB')
MAX_FRAME_BYTES = 0xFFFFFFFF


class ProtocolError(Exception):
    """The peer sent something this side cannot parse."""


def _read_exact(stream: BinaryIO, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            raise EOFError("The host closed the connection.")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def read_frame(stream: BinaryIO) -> Tuple[int, bytes]:
    """Read one frame and return `(kind, payload)`."""
    length, kind = HEADER.unpack(_read_exact(stream, HEADER.size))
    return kind, _read_exact(stream, length)


def read_message(stream: BinaryIO) -> Tuple[Dict[str, Any], List[bytes]]:
    """Read a control message and the data frames that follow it."""
    kind, payload = read_frame(stream)
    if kind != KIND_CONTROL:
        raise ProtocolError(f"Expected a control frame, got kind {kind}.")
    message = unpackb(payload)
    if not isinstance(message, dict):
        raise ProtocolError("A control message must be a map.")
    frames = []
    for _ in range(int(message.get('frames', 0) or 0)):
        data_kind, data = read_frame(stream)
        if data_kind != KIND_ARROW:
            raise ProtocolError(f"Expected a data frame, got kind {data_kind}.")
        frames.append(data)
    return message, frames


def encode_message(message: Dict[str, Any], frames: List[bytes] = ()) -> bytes:
    """Encode a control message and its data frames into one buffer."""
    message = dict(message)
    message['frames'] = len(frames)
    payload = packb(message)
    parts = [HEADER.pack(len(payload), KIND_CONTROL), payload]
    for frame in frames:
        if len(frame) > MAX_FRAME_BYTES:
            raise ProtocolError("A data frame exceeds 4 GiB; sync it in smaller chunks.")
        parts.append(HEADER.pack(len(frame), KIND_ARROW))
        parts.append(frame)
    return b''.join(parts)
