"""A minimal MQTT 3.1.1 subscriber (QoS 0) on a background thread, standard library only."""

import socket
import struct
import threading
import uuid


def _remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _packet(kind: int, body: bytes) -> bytes:
    return bytes([kind]) + _remaining_length(len(body)) + body


def _string(s: str) -> bytes:
    data = s.encode()
    return struct.pack('!H', len(data)) + data


def _read_exact(sock, n: int) -> bytes:
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError('the broker closed the connection')
        data += chunk
    return data


def _read_packet(sock):
    kind = _read_exact(sock, 1)[0]
    multiplier, length = 1, 0
    while True:
        byte = _read_exact(sock, 1)[0]
        length += (byte & 0x7F) * multiplier
        multiplier *= 128
        if not byte & 0x80:
            break
    return kind, _read_exact(sock, length) if length else b''


def subscribe(self, topic, callback):
    """Connect, subscribe to `topic`, wait for the SUBACK, then deliver messages on a thread."""
    sock = socket.create_connection((self.host, self.port), timeout=10)
    client_id = 'w9-' + uuid.uuid4().hex[:12]
    sock.sendall(_packet(0x10, _string('MQTT') + bytes([4, 0x02]) + struct.pack('!H', 60)
                         + _string(client_id)))
    kind, _ = _read_packet(sock)
    assert kind >> 4 == 2, f'expected CONNACK, got {kind}'
    sock.sendall(_packet(0x82, struct.pack('!H', 1) + _string(topic) + bytes([0])))
    kind, _ = _read_packet(sock)
    assert kind >> 4 == 9, f'expected SUBACK, got {kind}'
    sock.settimeout(None)

    def loop():
        while True:
            try:
                kind, body = _read_packet(sock)
            except Exception:
                return
            if kind >> 4 != 3:
                continue
            (topic_len,) = struct.unpack('!H', body[:2])
            payload = body[2 + topic_len:]
            callback(payload)

    threading.Thread(target=loop, daemon=True).start()
