"""
The `SQLConnector` façade (v5 parity phase): pipe, user and plugin methods route through
`meerschaum._core.SqlConnector` under `MRSM_CORE=rust` and through v4 Python otherwise. These
tests assert behavior that must hold in both modes, on a throwaway SQLite instance.
"""

import uuid
import pickle
from datetime import datetime, timezone

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'facade_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'facade.db'),
    )


def _rust_mode() -> bool:
    return _gate.mode() == _gate.RUST


def test_rust_connector_is_built_lazily_and_not_pickled(conn):
    pipe = mrsm.Pipe('facade', 'lazy', instance=conn)
    assert '_rust_connector' not in conn.__dict__
    pipe.exists()
    if _rust_mode():
        assert conn.__dict__.get('_rust_connector') not in (None, False)
    state = conn.__getstate__()
    assert '_rust_connector' not in state
    pickle.dumps(pipe)


def test_stub_self_runs_python():
    """Duck typing: the guarded functions accept stub objects as `self`."""
    from types import SimpleNamespace
    from meerschaum.connectors.sql._pipes import get_pipe_index_names
    stub = SimpleNamespace(flavor='sqlite', get_pipe_schema=lambda pipe: None)
    pipe = SimpleNamespace(
        parameters={}, target='stub_target', indices={'datetime': ['dt']},
        connector_keys='a', metric_key='b', location_key=None,
    )
    names = get_pipe_index_names(stub, pipe)
    assert names == {'datetime': 'IX_stub_target_dt'}


def test_sync_get_data_round_trip(conn):
    pipe = mrsm.Pipe('facade', 'round_trip', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    docs = [
        {'dt': datetime(2024, 1, 1), 'id': 1, 'val': 1.5},
        {'dt': datetime(2024, 1, 2), 'id': 2, 'val': 2.5},
    ]
    success, msg = pipe.sync(docs)
    assert success, msg
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'val': 3.5}])
    assert success, msg
    assert msg == 'Inserted 0, updated 1 rows.'
    df = pipe.get_data()
    assert list(df['val']) == [3.5, 2.5]
    assert str(df['dt'].dtype) == 'datetime64[us, UTC]'
    assert pipe.get_sync_time() == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert pipe.get_sync_time(newest=False) == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert pipe.get_rowcount() == 2
    assert pipe.get_rowcount(begin=datetime(2024, 1, 2)) == 1
    success, msg = pipe.clear(begin=datetime(2024, 1, 2))
    assert success, msg
    assert pipe.get_rowcount() == 1
    docs = pipe.get_docs()
    assert len(docs) == 1
    assert docs[0]['id'] == 1
    assert docs[0]['val'] == 3.5
    success, msg = pipe.drop()
    assert success, msg
    assert not pipe.exists()
    success, msg = pipe.delete()
    assert success, msg


def test_partial_row_update_keeps_other_columns(conn):
    """Design §4.4: a partial-row sync on a non-upsert pipe does not null the other columns."""
    pipe = mrsm.Pipe('facade', 'partial', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'a': 1, 'b': 'x'}])
    assert success, msg
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'b': 'y'}])
    assert success, msg
    doc = pipe.get_doc()
    assert doc['a'] == 1
    assert doc['b'] == 'y'


def test_attribute_aliases_persist(conn):
    pipe = mrsm.Pipe('facade', 'live', instance=conn, columns={'id': 'id'})
    success, msg = pipe.register()
    assert success, msg
    pipe.tags = ['a']
    success, msg = pipe.edit()
    assert success, msg
    assert conn.get_pipe_attributes(pipe)['parameters']['tags'] == ['a']
    fresh = mrsm.Pipe('facade', 'live', instance=conn, cache=False)
    assert fresh.tags == ['a']
    keys = conn.fetch_pipes_keys(tags=['a'])
    entries = keys.values() if isinstance(keys, dict) else keys
    assert [tuple(k[:3]) for k in entries] == [('facade', 'live', None)]
    pipe.delete()


def test_attribute_read_errors_propagate(conn):
    """
    A failed metadata read must raise, not report the pipe as unregistered
    (`tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered` checks the
    v4 path by mocking `exec`; this checks the routed path with a real failure).
    """
    if not _rust_mode():
        pytest.skip("v4 is covered by tests/test_pipe_attributes.py.")
    pipe = mrsm.Pipe('facade', 'errors', instance=conn, parameters={'target': 'x'})
    success, msg = pipe.register()
    assert success, msg
    conn.exec("ALTER TABLE mrsm_pipes RENAME COLUMN parameters TO parameters_renamed")
    with pytest.raises(RuntimeError):
        conn.get_pipe_attributes(pipe)


def test_users_and_plugins(conn):
    from meerschaum.core import User
    user = User('facade_user', 'password123', email='a@b.c', instance=conn)
    success, msg = conn.register_user(user)
    assert success, msg
    assert conn.get_user_id(user) is not None
    assert 'facade_user' in conn.get_users()
    assert conn.get_user_type(user) is None
    password_hash = conn.get_user_password_hash(user)
    from meerschaum.core.User import verify_password
    assert verify_password('password123', password_hash)
    user.type = 'admin'
    success, msg = conn.edit_user(user)
    assert success, msg
    assert conn.get_user_type(user) == 'admin'
    success, msg = conn.delete_user(user)
    assert success, msg
    assert conn.get_user_id(user) is None
