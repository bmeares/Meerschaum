"""
`meerschaum._core.Pipe` (design §4.4): the Python `Pipe` keeps its keys and
raw parameters in a Rust object; every change of `pipe._attributes['parameters']` (direct, nested,
through `pipe.parameters`, `update_parameters`, the setter or a metadata load) reaches it.
"""

import pickle
import uuid

import pytest

import meerschaum as mrsm


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


# ---------------------------------------------------------------------------------------------
# Wave 7 (task K): `_core.Pipe` is the only store; the Python views are read-through caches.

def test_raw_views_read_from_rust():
    pipe = mrsm.Pipe('plugin:x', 'views', parameters={'a': {'b': 1}, 'l': [{'k': 1}]}, instance='sql:main')
    raw = pipe._attributes['parameters']
    a = raw['a']
    lst = raw['l']
    ### Writes made straight into Rust are visible through every held view.
    pipe._core_pipe.set_path(['a', 'b'], 2)
    pipe._core_pipe.set_path(['a', 'c'], {'d': 3})
    pipe._core_pipe.set_path(['l', 0, 'k'], 9)
    assert a['b'] == 2 and a['c'] == {'d': 3} and lst[0]['k'] == 9
    assert raw['a'] is a and raw['l'] is lst
    ### And the other direction, with identity of nested containers kept across reads.
    a['b'] = 5
    a['c']['d'] = 6
    lst.append({'k': 2})
    assert pipe._core_pipe.get_path(['a', 'b']) == 5
    assert pipe._core_pipe.get_path(['a', 'c', 'd']) == 6
    assert pipe._core_pipe.get_path(['l', 1, 'k']) == 2
    assert raw['a'] is a and raw['a']['c'] is a['c']
    del a['c']
    assert 'c' not in a and 'c' not in pipe._core_pipe.raw_parameters['a']
    assert pipe._attributes['parameters'] is raw


def test_raw_views_stay_dict_and_list_duck_types():
    import copy
    import json
    pipe = mrsm.Pipe('plugin:x', 'duck', parameters={'a': {'b': [1, 2]}}, instance='sql:main')
    raw = pipe._attributes['parameters']
    assert isinstance(raw, dict) and isinstance(raw['a']['b'], list)
    assert json.loads(json.dumps(raw)) == {'a': {'b': [1, 2]}}
    assert {**raw} == {'a': {'b': [1, 2]}} and dict(raw) == {'a': {'b': [1, 2]}}
    assert type(copy.deepcopy(raw)) is dict and type(copy.deepcopy(raw['a']['b'])) is list
    assert pickle.loads(pickle.dumps(raw)) == {'a': {'b': [1, 2]}}
    pipe._core_pipe.set_path(['a', 'b'], [3])
    assert json.dumps(raw) == '{"a": {"b": [3]}}'
    assert list(raw['a']['b']) == [3] and len(raw['a']['b']) == 1


def test_resolved_memo_follows_rust_generation():
    pipe = mrsm.Pipe('plugin:x', 'memo', parameters={'columns': {'id': 'id'}}, instance='sql:main')
    assert pipe.columns == {'id': 'id'}
    generation = pipe._core_pipe.generation
    pipe._core_pipe.set_path(['columns', 'id'], 'z')
    assert pipe._core_pipe.generation > generation
    assert pipe.columns == {'id': 'z'}


def test_target_str_eq_hash_use_rust():
    pytest.importorskip('sqlalchemy')
    from meerschaum.utils.formatting._pipes import pipe_repr
    pipe = mrsm.Pipe('plugin:x', 'tg', 'loc', instance='sql:main', parameters={'target_name': 'custom_t'})
    assert pipe.target == 'custom_t'
    assert pipe._core_pipe.target(None, pipe.get_parameters()) == 'custom_t'
    plain_pipe = mrsm.Pipe('plugin:x', 'tg2', instance='sql:main')
    assert plain_pipe.target == 'plugin_x_tg2'
    assert str(pipe) == pipe_repr(pipe, ansi=False)
    other = mrsm.Pipe('plugin:x', 'tg', 'loc', instance='sql:main')
    assert pipe == other and hash(pipe) == hash(other) and pipe != plain_pipe
    assert {pipe: 1}[other] == 1
    assert pipe != 'plugin:x'


def test_cross_pipe_template_reresolves_on_every_access(conn):
    parent = mrsm.Pipe('plugin:p', 'par', instance=conn, columns={'datetime': 'dt'})
    assert parent.register()[0]
    child = mrsm.Pipe(
        'plugin:p', 'child', instance=conn,
        parameters={'note': "{{ Pipe('plugin:p', 'par', instance='" + str(conn) + "').columns['datetime'] }}"},
    )
    assert child.get_parameters()['note'] == 'dt'
    ### v4 keeps the memoized text; rust re-resolves because the template reads another pipe.
    parent.columns = {'datetime': 'ts'}
    assert parent.edit()[0]
    assert child.get_parameters()['note'] == 'ts'
    ### The raw parameters never take the resolved text.
    assert '{{' in child._attributes['parameters']['note']
    parent.delete()
