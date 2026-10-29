"""Refuse database/network I/O in the three synthetic SQL renderer generators."""

import os
import socket
import sqlite3
import sqlite3.dbapi2


class OfflineSQLIO(BaseException):
    """Escape the reference's best-effort metadata error handlers before any I/O."""


_attempts = []


def _deny(operation):
    def blocked(*args, **kwargs):
        _attempts.append(operation)
        raise OfflineSQLIO(f'Offline SQL fixture attempted {operation}; I/O was blocked.')
    return blocked


# Install before importing Meerschaum or its drivers. Reference dependencies must
# already be installed; no renderer may bootstrap packages through a subprocess.
os.environ['MRSM_NO_AUTO_INSTALL'] = '1'
os.environ['MRSM_NO_UPDATE_CHECK'] = '1'
for _name in ('connect', 'connect_ex'):
    setattr(socket.socket, _name, _deny('socket.' + _name))
socket.create_connection = _deny('socket.create_connection')
socket.getaddrinfo = _deny('socket.getaddrinfo')
sqlite3.connect = _deny('sqlite3.connect')
sqlite3.dbapi2.connect = _deny('sqlite3.dbapi2.connect')

from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

# Native DBAPIs (e.g. libpq) do not necessarily use Python's socket functions.
# Block the SQLAlchemy checkout boundary too, including SQLite file creation.
Engine.connect = _deny('Engine.connect')
Engine.raw_connection = _deny('Engine.raw_connection')
Pool.connect = _deny('Pool.connect')


def assert_no_io():
    """Call before writing the oracle, even if reference code swallowed a denial."""
    if _attempts:
        raise AssertionError('Offline SQL fixture attempted I/O: ' + ', '.join(_attempts))
