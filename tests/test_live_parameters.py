"""
Live `Pipe.parameters` (design §4.4): under `MRSM_CORE=rust` the property is a write-through view,
so nested mutation followed by `pipe.edit()` persists. Under `python` v4 returns a copy
(`PARITY.md` row 29); both behaviors are pinned here.
"""

import copy
import json
import pickle
import uuid
from types import SimpleNamespace

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'live_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'live.db'),
    )


rust_only = pytest.mark.skipif(_gate.mode() != _gate.RUST, reason="live view is a rust-mode feature")


def _stored(conn, pipe):
    return conn.get_pipe_attributes(pipe)['parameters']


def test_construction_never_touches_the_instance():
    pipe = mrsm.Pipe(
        'live', 'ctor', instance='sql:live_missing_' + uuid.uuid4().hex,
        parameters={'columns': {'id': 'id'}},
    )
    assert str(pipe)
    assert hash(pipe) == hash(mrsm.Pipe('live', 'ctor', instance=pipe.instance_keys))
    assert pipe._attributes['parameters']['columns'] == {'id': 'id'}
    assert '_connector' not in pipe.__dict__ and '_instance_connector' not in pipe.__dict__


@rust_only
def test_nested_mutation_persists(conn):
    pipe = mrsm.Pipe('live', 'nested', instance=conn, columns={'id': 'id'})
    success, msg = pipe.register()
    assert success, msg
    pipe.parameters['fetch'] = {'a': 1}
    pipe.parameters['fetch']['backtrack_minutes'] = 5
    held = pipe.parameters['fetch']
    held['b'] = [1]
    held['b'].append(2)
    success, msg = pipe.edit()
    assert success, msg
    stored = _stored(conn, pipe)
    assert stored['fetch'] == {'a': 1, 'backtrack_minutes': 5, 'b': [1, 2]}
    assert pipe.parameters['fetch']['backtrack_minutes'] == 5
    fresh = mrsm.Pipe('live', 'nested', instance=conn, cache=False)
    assert fresh.parameters['fetch']['backtrack_minutes'] == 5
    pipe.delete()


@rust_only
def test_deletion_and_update_methods(conn):
    pipe = mrsm.Pipe('live', 'delete', instance=conn, parameters={'x': 1, 'y': {'z': 2}, 'l': [3, 1, 2]})
    pipe.register()
    del pipe.parameters['x']
    pipe.parameters['y'].update({'w': 3})
    assert pipe.parameters['y'].pop('z') == 2
    pipe.parameters['l'].sort()
    pipe.parameters.setdefault('new', {})['k'] = 'v'
    success, msg = pipe.edit()
    assert success, msg
    stored = _stored(conn, pipe)
    assert 'x' not in stored
    assert stored['y'] == {'w': 3}
    assert stored['l'] == [1, 2, 3]
    assert stored['new'] == {'k': 'v'}
    pipe.delete()


@rust_only
def test_raw_templates_survive_when_only_read(conn):
    """Reading a resolved value keeps the raw template; only touched keys are written."""
    pipe = mrsm.Pipe(
        'live', 'raw', instance=conn,
        parameters={'sql': 'SELECT {{ self.parameters["n"] }} AS a', 'n': 7},
    )
    pipe.register()
    assert '{{' not in pipe.parameters['sql']
    pipe.parameters['other'] = 1
    success, msg = pipe.edit()
    assert success, msg
    stored = _stored(conn, pipe)
    assert '{{' in stored['sql']
    assert stored['other'] == 1
    pipe.delete()


@rust_only
def test_live_view_is_a_real_dict_and_serializes():
    pipe = mrsm.Pipe('live', 'dict', parameters={'a': {'b': [1, {'c': 2}]}})
    params = pipe.parameters
    assert isinstance(params, dict)
    assert json.loads(json.dumps(params)) == {'a': {'b': [1, {'c': 2}]}}
    assert type(copy.deepcopy(params)) is dict
    assert type(pickle.loads(pickle.dumps(params))) is dict
    assert {**params} == {'a': {'b': [1, {'c': 2}]}}
    params['a']['b'][1]['c'] = 3
    assert pipe.parameters['a']['b'][1]['c'] == 3
    assert pipe._attributes['parameters']['a']['b'][1]['c'] == 3


@rust_only
def test_inherited_value_is_created_in_raw_on_write(conn):
    base = mrsm.Pipe('live', 'base', instance=conn, parameters={'fetch': {'p': 1}})
    base.register()
    child = mrsm.Pipe('live', 'child', instance=conn, parameters={'reference': {'connector': 'live', 'metric': 'base'}})
    assert child.parameters['fetch']['p'] == 1
    child.parameters['fetch']['q'] = 2
    assert child._attributes['parameters']['fetch'] == {'q': 2}
    assert child.parameters['fetch'] == {'p': 1, 'q': 2}


@rust_only
def test_setter_accepts_a_live_view():
    pipe = mrsm.Pipe('live', 'setter', parameters={'a': {'b': 1}})
    pipe.parameters = pipe.parameters
    from meerschaum.core.Pipe._live_parameters import LiveDict
    assert not isinstance(pipe._attributes['parameters'], LiveDict)
    assert not isinstance(pipe._attributes['parameters']['a'], LiveDict)
    assert type(pipe._core_pipe.raw_parameters['a']) is dict


def test_aliases_stay_assignable(conn):
    pipe = mrsm.Pipe('live', 'aliases', instance=conn)
    pipe.columns = {'datetime': 'dt', 'id': 'id'}
    pipe.dtypes = {'dt': 'datetime'}
    pipe.tags = ['a', 'b']
    pipe.upsert = True
    pipe.indices = {'extra': 'val'}
    assert pipe.columns == {'datetime': 'dt', 'id': 'id'}
    assert pipe.tags == ['a', 'b']
    assert pipe.upsert is True
    assert pipe.indices['extra'] == 'val'
    success, msg = pipe.register()
    assert success, msg
    stored = _stored(conn, pipe)
    assert stored['columns'] == {'datetime': 'dt', 'id': 'id'}
    assert stored['upsert'] is True
    pipe.delete()


def test_pickle_roundtrip_and_equality():
    pipe = mrsm.Pipe('live', 'pickle', 'loc', parameters={'columns': {'id': 'id'}, 'x': {'y': 1}})
    clone = pickle.loads(pickle.dumps(pipe))
    assert clone == pipe and hash(clone) == hash(pipe)
    assert clone.parameters['x'] == {'y': 1}
    assert str(clone) == str(pipe)


def test_python_mode_parameters_are_a_copy():
    if _gate.mode() == _gate.RUST:
        pytest.skip("python-mode parity")
    pipe = mrsm.Pipe('live', 'copy', parameters={'a': {'b': 1}})
    pipe.parameters['a']['b'] = 2
    assert pipe.parameters['a']['b'] == 1


def test_duck_typed_stub_is_not_type_checked():
    stub = SimpleNamespace(parameters={'a': 1}, connector_keys='x', metric_key='y', location_key=None)
    assert mrsm.Pipe('x', 'y') != stub
