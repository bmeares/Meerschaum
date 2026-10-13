"""
`meerschaum._core.Pipe` (design §4.4): under `MRSM_CORE=rust` the Python `Pipe` keeps its keys and
raw parameters in a Rust object; every change of `pipe._attributes['parameters']` (direct, nested,
through `pipe.parameters`, `update_parameters`, the setter or a metadata load) reaches it.
"""

import pickle
import uuid

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate

pytestmark = pytest.mark.skipif(_gate.mode() != _gate.RUST, reason="rust-mode façade")


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'corepipe_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'corepipe.db'),
    )


def _core():
    from meerschaum import _core
    return _core


def test_core_pipe_keys_equality_pickle():
    core = _core().Pipe('plugin:x', 'm', 'loc', 'sql:main', {'a': {'b': 1}})
    same = _core().Pipe('plugin:x', 'm', 'loc', 'sql:main')
    other = _core().Pipe('plugin:x', 'm', None, 'sql:main')
    assert (core.connector_keys, core.metric_key, core.location_key, core.instance_keys) == (
        'plugin:x', 'm', 'loc', 'sql:main'
    )
    assert core == same and hash(core) == hash(same) and core != other
    assert core != 'plugin:x'
    clone = pickle.loads(pickle.dumps(core))
    assert clone == core and clone.raw_parameters == {'a': {'b': 1}}
    state = core.__getstate__()
    assert state['parameters'] == {'a': {'b': 1}} and state['location_key'] == 'loc'
    assert str(core) == "Pipe('plugin:x', 'm', 'loc', instance='sql:main')" or str(core).startswith(
        "Pipe('plugin:x', 'm', 'loc'"
    )


def test_core_pipe_paths():
    core = _core().Pipe('a', 'b', None, 'sql:main', {'l': [1, {'x': 2}]})
    core.set_path(['fetch', 'backtrack_minutes'], 5)
    core.set_path(['l', 1, 'x'], 3)
    assert core.get_path(['fetch', 'backtrack_minutes']) == 5
    assert core.get_path(['l', -1, 'x']) == 3
    core.del_path(['l', 0])
    assert core.raw_parameters['l'] == [{'x': 3}]
    with pytest.raises(KeyError):
        core.get_path(['nope'])
    with pytest.raises(KeyError):
        core.del_path(['nope'])
    core.update_raw_parameters({'fetch': {'other': 1}})
    assert core.raw_parameters['fetch'] == {'backtrack_minutes': 5, 'other': 1}
    core.set_raw_parameters({'z': 1})
    assert core.raw_parameters == {'z': 1}


def test_core_pipe_resolves_self_templates_and_target():
    core = _core().Pipe('a', 'b', None, 'sql:main', {'n': 7, 'sql': 'SELECT {{ self.parameters["n"] }}'})
    assert core.raw_parameters['sql'].startswith('SELECT {{')
    assert core.resolved_parameters()['sql'] == 'SELECT 7'
    assert core.target() == 'a_b'
    core.set_path(['target'], 'custom')
    assert core.target() == 'custom'


def test_construction_creates_core_without_touching_instance():
    pipe = mrsm.Pipe('plugin:x', 'm', 'loc', instance='sql:corepipe_missing_' + uuid.uuid4().hex)
    assert type(pipe._core_pipe).__name__ == 'Pipe'
    assert '_instance_connector' not in pipe.__dict__
    assert (pipe.connector_keys, pipe.metric_key, pipe.location_key) == ('plugin:x', 'm', 'loc')
    assert pipe.instance_keys == pipe._core_pipe.instance_keys
    assert pipe.metric == 'm' and pipe.location == 'loc'


def _assert_synced(pipe):
    assert pipe._core_pipe.raw_parameters == dict(pipe._attributes['parameters'])
    assert pipe._core_pipe.raw_parameters == pipe._attributes['parameters']


def test_every_raw_parameter_write_reaches_rust():
    pipe = mrsm.Pipe('plugin:x', 'sync', parameters={'columns': {'id': 'id'}}, instance='sql:main')
    _assert_synced(pipe)
    pipe._attributes['parameters']['fetch'] = {'a': 1}
    pipe._attributes['parameters']['fetch']['b'] = [1]
    pipe._attributes['parameters']['fetch']['b'].append(2)
    _assert_synced(pipe)
    assert pipe._core_pipe.get_path(['fetch', 'b']) == [1, 2]

    pipe.update_parameters({'fetch': {'c': 3}}, persist=False)
    _assert_synced(pipe)
    assert pipe._core_pipe.get_path(['fetch', 'c']) == 3

    pipe.parameters = {'replaced': {'x': 1}}
    _assert_synced(pipe)
    assert pipe._core_pipe.raw_parameters == {'replaced': {'x': 1}}

    pipe.parameters['replaced']['x'] = 2
    pipe.parameters['replaced']['y'] = {'z': [1]}
    pipe.parameters['replaced']['y']['z'].append(2)
    _assert_synced(pipe)
    assert pipe._core_pipe.get_path(['replaced', 'y', 'z']) == [1, 2]

    pipe.columns = {'id': 'id', 'datetime': 'dt'}
    pipe.tags = ['t']
    pipe.upsert = True
    _assert_synced(pipe)
    assert pipe._core_pipe.get_path(['columns', 'datetime']) == 'dt'
    assert pipe._core_pipe.get_path(['upsert']) is True


def test_registered_metadata_load_and_edit_are_mirrored(conn):
    pipe = mrsm.Pipe('facade', 'core', instance=conn, columns={'id': 'id'})
    success, msg = pipe.register()
    assert success, msg
    pipe.parameters['fetch'] = {'backtrack_minutes': 5}
    success, msg = pipe.edit()
    assert success, msg
    _assert_synced(pipe)

    fresh = mrsm.Pipe('facade', 'core', instance=conn, cache=False)
    assert fresh.parameters['fetch']['backtrack_minutes'] == 5   # loads the registered metadata
    _assert_synced(fresh)
    assert fresh._core_pipe.get_path(['fetch', 'backtrack_minutes']) == 5
    fresh.parameters['fetch']['backtrack_minutes'] = 9
    _assert_synced(fresh)
    success, msg = fresh.edit()
    assert success, msg
    assert conn.get_pipe_attributes(fresh)['parameters']['fetch']['backtrack_minutes'] == 9
    pipe.delete()


def test_python_pipe_pickle_equality_and_core_agree():
    pipe = mrsm.Pipe('plugin:x', 'p', 'loc', parameters={'a': {'b': 1}}, instance='sql:main')
    clone = pickle.loads(pickle.dumps(pipe))
    assert clone == pipe and hash(clone) == hash(pipe)
    assert clone._attributes['parameters'] == {'a': {'b': 1}}
    _assert_synced(clone)
    assert pipe._core_pipe == clone._core_pipe
    import copy
    assert copy.deepcopy(pipe) == pipe
