#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Debug output: `dprint()` writes to stderr (v5; v4 used stdout), and the Rust core's debug events
reach `dprint()` for a binding called with `debug=True` (or inside `_core.debugging()`).
"""

import os
import re
import uuid

import pytest

HEADER = re.compile(r'\d\d:\d\d:\d\d\.\d{3} \| (?P<module>[\w.:]+):(?P<line>\d+)')


def _finish_sync_output():
    """Drain background telemetry before changing debug or stderr capture windows."""
    from meerschaum._core import flush_sync_statuses
    flush_sync_statuses(timeout=10.0)


def test_dprint_writes_to_stderr(capfd):
    from meerschaum.utils.debug import dprint
    dprint("hello from dprint")
    out, err = capfd.readouterr()
    assert 'hello from dprint' not in out
    assert 'hello from dprint' in err
    match = HEADER.search(err)
    assert match is not None, err
    assert match.group('module') == __name__


def test_dprint_takes_the_rust_caller(capfd):
    from meerschaum.utils.debug import dprint
    dprint("from rust", _package='meerschaum_core::debug', _lineno=7)
    _, err = capfd.readouterr()
    assert 'meerschaum_core::debug:7' in err
    assert 'from rust' in err


@pytest.fixture
def sqlite_conn(tmp_path):
    import meerschaum as mrsm
    label = 'dbg_' + uuid.uuid4().hex[:8]
    return mrsm.get_connector(
        'sql', label, flavor='sqlite', database=str(tmp_path / 'debug.db'),
    )


def test_rust_debug_events_reach_dprint(sqlite_conn, capfd):
    import meerschaum as mrsm
    pipe = mrsm.Pipe('a', 'b', instance=sqlite_conn, columns={'datetime': 'dt'})
    docs = [{'dt': '2024-01-01', 'v': 1}]

    success, msg = pipe.sync(docs)
    assert success, msg
    _finish_sync_output()
    out, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' not in out + err

    success, msg = pipe.sync([{'dt': '2024-01-02', 'v': 2}], debug=True)
    assert success, msg
    _finish_sync_output()
    out, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' not in out, out
    modules = {m.group('module') for m in HEADER.finditer(err)}
    assert any(m.startswith('meerschaum_connector_sql') for m in modules), err
    assert 'SELECT' in err or 'INSERT' in err, err

    ### Nothing leaks once the call returned.
    assert pipe.get_rowcount() == 2
    out, err = capfd.readouterr()
    assert 'meerschaum_' not in out + err


def test_debugging_context_manager(sqlite_conn, capfd):
    import meerschaum as mrsm
    from meerschaum._core import debugging
    pipe = mrsm.Pipe('a', 'c', instance=sqlite_conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': '2024-01-01', 'v': 1}])
    assert success, msg
    _finish_sync_output()
    capfd.readouterr()

    with debugging():
        assert pipe.get_rowcount() == 1
    _, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' in err, err

    with debugging(False):
        assert pipe.get_rowcount() == 1
    _, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' not in err, err


@pytest.mark.parametrize('method,kwargs', [
    ('exists', {}),
    ('clear', {'begin': '2024-01-01'}),
    ('get_columns_types', {}),
    ('get_rowcount', {}),
])
def test_facade_methods_show_rust_debug_output(sqlite_conn, capfd, method, kwargs):
    """
    A façade method called with `debug=True` shows the Rust core's output even when it does not
    pass `debug` on to the binding it calls.
    """
    import meerschaum as mrsm
    pipe = mrsm.Pipe('a', 'f_' + method, instance=sqlite_conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': '2024-01-01', 'v': 1}])
    assert success, msg
    _finish_sync_output()
    capfd.readouterr()

    getattr(pipe, method)(**kwargs)
    _, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' not in err, err

    ### `exists` and `get_columns_types` cache their value (also on disk): skip the cache.
    fresh = mrsm.Pipe('a', 'f_' + method, instance=sqlite_conn, cache=False)
    getattr(fresh, method)(debug=True, **kwargs)
    out, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' in err, err
    assert 'meerschaum_connector_sql' not in out, out


def test_get_pipes_debug_shows_rust_debug_output(sqlite_conn, capfd):
    import meerschaum as mrsm
    mrsm.Pipe('a', 'gp', instance=sqlite_conn).register()
    capfd.readouterr()
    mrsm.get_pipes(instance=sqlite_conn, debug=True)
    _, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' in err, err


def test_overlapping_debug_actions_leave_debug_off(sqlite_conn, capfd):
    """
    Two `--debug` actions in one process that do not end in reverse order of starting must not
    leave debug output on.
    """
    import threading
    import time
    import meerschaum as mrsm
    pipe = mrsm.Pipe('a', 'overlap', instance=sqlite_conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': '2024-01-01', 'v': 1}])
    assert success, msg
    _finish_sync_output()

    first = threading.Thread(target=mrsm.entry, args=(['sh', 'sleep', '0.6', '--debug'],))
    second = threading.Thread(target=mrsm.entry, args=(['sh', 'sleep', '1.2', '--debug'],))
    first.start()
    time.sleep(0.3)
    second.start()
    first.join()
    second.join()
    capfd.readouterr()

    assert pipe.get_rowcount() == 1
    out, err = capfd.readouterr()
    assert 'meerschaum_' not in out + err, err


def test_debug_sql_hides_secret_filters(sqlite_conn, capfd):
    """
    The SQL that `debug=True` prints hides the literals compared to secret columns (the query that
    runs is unchanged).
    """
    import meerschaum as mrsm
    pipe = mrsm.Pipe('a', 'secret', instance=sqlite_conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': '2024-01-01', 'password': 'other', 'token': 'x'}])
    assert success, msg
    _finish_sync_output()
    capfd.readouterr()

    df = pipe.get_data(params={'password': 'hunter2', 'token': ['s3cr3t-a']}, debug=True)
    assert df is not None and len(df) == 0
    out, err = capfd.readouterr()
    assert 'meerschaum_connector_sql' in err, err
    assert '"password" = \'***\'' in err, err
    for secret in ('hunter2', 's3cr3t-a'):
        assert secret not in out + err, err
