#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The worker loop: connect to the host's socket, say hello, and serve calls one at a time.

A reader thread demultiplexes incoming messages: `call` messages are queued for the main thread,
`cb_result` messages wake the thread waiting on that callback (plugin code may call `pipe.sync()`
from any thread, e.g. an MQTT client's network thread), and `shutdown` ends the loop. Writes are
serialized by a lock so callbacks from other threads never interleave with a response.

When the host goes away the worker exits with `os._exit()` so stray plugin threads cannot keep
the process alive.
"""

from __future__ import annotations

import itertools
import os
import queue
import socket
import sys
import threading
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from meerschaum.worker import PROTOCOL_VERSION
from meerschaum.worker._frames import encode_message, read_message

Handler = Callable[['Connection', int, Dict[str, Any], List[bytes]], Tuple[Any, List[bytes]]]


class WorkerCallError(Exception):
    """Raised inside the worker when the host answers a callback with an error."""


class _Slot:
    __slots__ = ('event', 'value', 'error', 'frames')

    def __init__(self):
        self.event = threading.Event()
        self.value = None
        self.error = None
        self.frames: List[bytes] = []


class Connection:
    """One connection to the host."""

    def __init__(self, stream):
        self.stream = stream
        self.write_lock = threading.Lock()
        self.calls: 'queue.Queue[Optional[Tuple[Dict[str, Any], List[bytes]]]]' = queue.Queue()
        self.pending: Dict[int, _Slot] = {}
        self.pending_lock = threading.Lock()
        self.cb_ids = itertools.count(1)
        self.current_call_id: int = 0
        self.closed = threading.Event()

    def send(self, message: Dict[str, Any], frames: List[bytes] = ()) -> None:
        """Send a control message followed by its data frames."""
        data = encode_message(message, list(frames))
        with self.write_lock:
            self.stream.write(data)
            self.stream.flush()

    def reader(self) -> None:
        """Read messages until the host disconnects."""
        try:
            while True:
                message, frames = read_message(self.stream)
                t = message.get('t', None)
                if t == 'call':
                    self.calls.put((message, frames))
                elif t == 'cb_result':
                    with self.pending_lock:
                        slot = self.pending.pop(int(message.get('cb', 0)), None)
                    if slot is not None:
                        slot.value = message.get('value', None)
                        slot.error = message.get('error', None)
                        slot.frames = frames
                        slot.event.set()
                elif t == 'shutdown':
                    break
        except EOFError:
            pass
        except Exception:
            traceback.print_exc()
        finally:
            self.closed.set()
            with self.pending_lock:
                for slot in self.pending.values():
                    slot.error = "The host closed the connection."
                    slot.event.set()
                self.pending.clear()
            self.calls.put(None)

    def callback(
        self,
        op: str,
        args: Dict[str, Any],
        frames: List[bytes] = (),
    ) -> Tuple[Any, List[bytes]]:
        """Ask the host to run `op` (e.g. `sync`) and wait for its answer."""
        if self.closed.is_set():
            raise WorkerCallError("The host closed the connection.")
        cb_id = next(self.cb_ids)
        slot = _Slot()
        with self.pending_lock:
            self.pending[cb_id] = slot
        self.send(
            {'t': 'callback', 'id': self.current_call_id, 'cb': cb_id, 'op': op, 'args': args},
            frames,
        )
        slot.event.wait()
        if slot.error is not None:
            raise WorkerCallError(str(slot.error))
        return slot.value, slot.frames


def _connect(address: str):
    if address.startswith('\\\\.\\pipe\\'):
        ### Windows named pipe: the host created it; open it like a file.
        return open(address, 'r+b', buffering=0)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(address)
    return sock.makefile('rwb', buffering=1024 * 1024)


def serve(address: str) -> int:
    """Connect to the host at `address` and serve calls until shutdown."""
    import platform
    stream = _connect(address)
    conn = Connection(stream)
    import signal
    interruptible = False

    def interrupt_active_call(signum, frame):
        # The Rust host owns terminal cancellation. Idle workers share its process group
        # and must survive SIGINT, including during handshake and between protocol writes.
        if interruptible:
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt_active_call)

    import meerschaum as mrsm
    from meerschaum.worker._ops import HANDLERS, error_message
    from meerschaum.worker._msgpack import USING_FALLBACK

    conn.send({
        't': 'hello',
        'protocol': PROTOCOL_VERSION,
        'pid': os.getpid(),
        'python': platform.python_version(),
        'meerschaum': mrsm.__version__,
        'msgpack': ('stdlib' if USING_FALLBACK else 'msgpack'),
    })

    reader = threading.Thread(target=conn.reader, name='mrsm-worker-reader', daemon=True)
    reader.start()

    while True:
        item = conn.calls.get()
        if item is None:
            break
        message, frames = item
        call_id = int(message.get('id', 0))
        op = message.get('op', None)
        args = message.get('args', None) or {}
        conn.current_call_id = call_id
        handler = HANDLERS.get(op, None)
        if handler is None:
            conn.send({
                't': 'error',
                'id': call_id,
                'kind': 'BadRequest',
                'message': f"Unknown worker operation '{op}'.",
                'traceback': None,
            })
            continue
        if op == 'shutdown':
            conn.send({'t': 'result', 'id': call_id, 'value': True})
            break
        try:
            try:
                interruptible = True
                value, out_frames = handler(conn, call_id, args, frames)
            finally:
                interruptible = False
            conn.send({'t': 'result', 'id': call_id, 'value': value}, out_frames)
        except (Exception, KeyboardInterrupt) as e:
            try:
                conn.send(error_message(call_id, e))
            except Exception:
                break
        finally:
            conn.current_call_id = 0
            sys.stdout.flush()
            sys.stderr.flush()

    try:
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(0)
    return 0
