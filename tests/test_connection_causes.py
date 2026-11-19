"""
`test_connection()` says why a connection failed (5.1): with `warn=True`, the SQL and API
connectors print the cause the native retry loop knows (`cannot connect to host:port
(connection refused)`), in v4's layout (the cause on stdout, the `Connection to ... failed.`
warning on stderr); with `warn=False` they print nothing; a password or URI is never printed.
A Valkey connector raises with the cause, as v4's `ping()` did.
"""

import socket

import pytest

import meerschaum as mrsm

SECRET = 'tpr-never-printed-pw'


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _sql(label: str, **extra):
    attrs = dict(
        flavor='postgresql', host='127.0.0.1', port=_closed_port(),
        username='tpr', password=SECRET, database='tprdb',
    )
    attrs.update(extra)
    return mrsm.get_connector('sql', label, **attrs), attrs['port']


@pytest.mark.parametrize('schema', [None, 'public'])
def test_sql_says_why_with_warn(capsys, schema):
    extra = {'schema': schema} if schema else {}
    conn, port = _sql(f'tprcause{schema or "none"}', **extra)
    assert conn.test_connection(warn=True) is False
    out, err = capsys.readouterr()
    assert f"cannot connect to 127.0.0.1:{port} (connection refused)" in out
    assert f"Connection to '{conn}' failed." in err
    assert 'Failed connection attempt (1 / 1)' in out
    assert 'default schema' not in out + err
    assert SECRET not in out + err


def test_sql_is_silent_without_warn(capsys):
    conn, _ = _sql('tprcausesilent')
    assert conn.test_connection() is False
    out, err = capsys.readouterr()
    assert out == '' and err == ''


def test_sql_success(tmp_path):
    conn = mrsm.get_connector(
        'sql', 'tprcauseok', flavor='sqlite', database=str(tmp_path / 'ok.db'),
    )
    assert conn.test_connection(warn=True) is True


def test_api_says_why_with_warn(capsys):
    port = _closed_port()
    conn = mrsm.get_connector(
        'api', 'tprcauseapi', host='127.0.0.1', port=port, protocol='http',
        username='tpr', password=SECRET,
    )
    assert conn.test_connection(warn=True) is False
    out, err = capsys.readouterr()
    assert f"cannot connect to 127.0.0.1:{port} (connection refused)" in out
    assert f"Connection to '{conn}' failed." in err
    ### The server was never reached: nothing is said about the credentials.
    assert 'Failed to log into' not in out + err
    assert SECRET not in out + err

    assert conn.test_connection() is False
    out, err = capsys.readouterr()
    assert out == '' and err == ''


def test_valkey_raises_with_the_cause():
    port = _closed_port()
    conn = mrsm.get_connector(
        'valkey', 'tprcausevalkey', host='127.0.0.1', port=port, password=SECRET,
    )
    with pytest.raises(Exception) as excinfo:
        conn.test_connection()
    assert f"cannot connect to 127.0.0.1:{port} (connection refused)" in str(excinfo.value)
    assert SECRET not in str(excinfo.value)
