"""
`Pipe.sync()` of explicit data and of a pipe's own source, run by the Rust core
(`_core.sync_pipe`, the engine's ordinary one-pipe flow).

Each scenario runs a sequence of syncs on pipes with fixed keys (dropped in between) and
compares the `SuccessTuple`s (messages included), the rows `get_data(as_docs=True)` returns, the
columns' database types, the registered `dtypes` / `columns` after each sync, and that the
caller's data was not modified, with what the parity-phase Python mode produced for the same scenario. Those results are
recordings of that mode (`tests/oracle/test_native_sync.json`, see `tests/_oracle.py`), not v4.1.4
output. The runs also
assert that `_core.sync_pipe` handled the call (so a silent fall-through cannot pass).

Rows are compared as documents: no scenario needs pandas, and pandas, Polars and
GeoPandas frames are covered separately when those packages are installed.
"""

import copy
import os
import pickle
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import meerschaum as mrsm
from meerschaum import _core
from tests import _oracle as oracle

SCHEMA = 'w13h'
START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _flavors():
    selected = [
        f.strip()
        for f in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')
        if f.strip()
    ]
    flavors = ['sqlite']
    if 'postgis' in selected:
        flavors.append('postgis')
    return flavors


W15L_PLUGINS = ('w15l_custom', 'w15l_plugsrc', 'w15l_plugsync', 'w15l_plugdie')


@pytest.fixture(scope='module', autouse=True)
def plugin_venvs():
    """
    The virtual environments of the test plugins, created the way a v4 user's first
    `install plugin` / `setup plugins` does: importing a plugin activates its venv without
    creating it, so on a fresh `MRSM_ROOT_DIR` the first import would fail.
    """
    from meerschaum.plugins import _plugins_import_errors, reload_plugins
    from meerschaum.utils.venv import init_venv, venv_exists
    for name in W15L_PLUGINS:
        init_venv(name, verify=False)
    missing = [name for name in W15L_PLUGINS if not venv_exists(name)]
    assert not missing, f"Could not create the virtual environments of {missing}."
    ### An earlier test of the run may have imported the plugins before their venvs existed
    ### (the failure is remembered per process): import them again now.
    for name in W15L_PLUGINS:
        _plugins_import_errors.pop(name, None)
    reload_plugins(list(W15L_PLUGINS))
    yield


@pytest.fixture(scope='module')
def postgis_instance():
    """One schema (`w13h`) with its own instance tables for the whole module."""
    conn = mrsm.get_connector(
        'sql', 'w13h_postgis',
        flavor='postgis',
        username='test',
        password='test1234',
        database='testdb',
        port=5449,
        host='localhost',
        schema=SCHEMA,
    )
    conn.exec(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    conn.exec(f'CREATE SCHEMA "{SCHEMA}"')
    ### The instance tables (`mrsm_pipes`, ...) of a fresh schema.
    from meerschaum.connectors.sql.tables import get_tables
    get_tables(mrsm_instance=conn, create=True, refresh=True)
    yield conn
    conn.exec(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


@pytest.fixture(params=_flavors())
def instance(request, tmp_path):
    if request.param == 'sqlite':
        yield mrsm.get_connector(
            'sql', 'w13h_' + uuid.uuid4().hex[:8],
            flavor='sqlite',
            database=str(tmp_path / 'w13h.db'),
        )
        return
    yield request.getfixturevalue('postgis_instance')


class NativeCalls:
    """Counts the calls `_core.sync_pipe` handled and the ones it declined."""

    def __init__(self):
        self.handled = 0
        self.declined = []


@pytest.fixture
def native_calls(monkeypatch):
    calls = NativeCalls()
    original = _core.sync_pipe

    def counting(*args, **kwargs):
        out = original(*args, **kwargs)
        if 'declined' in out:
            calls.declined.append(out['declined'])
        else:
            calls.handled += 1
        return out

    monkeypatch.setattr(_core, 'sync_pipe', counting)
    return calls


def _docs(pipe):
    return pipe.get_data(as_docs=True, debug=False)


def _snapshot(pipe):
    """What a sync leaves behind, as plain comparable values."""
    pipe._invalidate_cache(hard=True)
    registered = pipe.get_parameters(refresh=True)
    return {
        'docs': _docs(pipe),
        'columns_types': dict(pipe.get_columns_types(refresh=True) or {}),
        'dtypes': dict(registered.get('dtypes', {}) or {}),
        'columns': dict(registered.get('columns', {}) or {}),
        'autoincrement': registered.get('autoincrement', None),
    }


def _fresh_pipe(instance, name, **params):
    pipe = mrsm.Pipe('w13h', name, instance=instance, **params)
    pipe.delete()
    pipe.drop()
    ### A pipe's cached values (its `id`) outlive the registration in the on-disk cache.
    pipe._invalidate_cache(hard=True)
    return mrsm.Pipe('w13h', name, instance=instance, **params)


def _run(instance, name, params, steps, native_calls=None):
    """
    Run `steps` (a list of `(data, sync_kwargs)`) on a fresh pipe.
    Return the `(tuple, snapshot)` after each step.
    """
    pipe = _fresh_pipe(instance, name, **copy.deepcopy(params))
    observed = []
    for data, kwargs in steps:
        data_before = copy.deepcopy(data)
        result = pipe.sync(copy.deepcopy(data), **kwargs)
        passed = copy.deepcopy(data)
        assert passed == data_before
        observed.append((tuple(result), _snapshot(pipe)))
    return observed


def _untimed(message):
    import re
    message = re.sub(r'in \d+(\.\d+)? seconds\.', 'in N seconds.', message)
    ### The labels of the throwaway instances end in a random suffix.
    message = re.sub(r'\b(w13h|w15l_source)_[0-9a-f]{6,8}\b', r'\1_X', message)
    ### A SQLite source hands v4 its datetimes as text, so v4's chunk label carries the stored
    ### microseconds; the native flow labels the parsed datetimes (PARITY.md, wave 15 L).
    return re.sub(r'(\d\d:\d\d:\d\d)\.000000', r'\1', message)


def _compare(instance, name, params, steps, native_calls, same_messages=True):
    expected = oracle.expected()
    handled_before = native_calls.handled
    actual = _run(instance, name, params, steps)
    assert native_calls.handled - handled_before == len(steps), native_calls.declined
    assert len(actual) == len(expected)
    for step, ((exp_tuple, exp_state), (act_tuple, act_state)) in enumerate(zip(expected, actual)):
        assert act_tuple[0] == exp_tuple[0], f"step {step}: {act_tuple} vs {exp_tuple}"
        if same_messages:
            ### `debug` names the table and the time the sync took (`in 0.39 seconds.`).
            assert _untimed(act_tuple[1]) == _untimed(exp_tuple[1]), f"step {step}"
        for key in exp_state:
            assert act_state[key] == exp_state[key], f"step {step}: {key}"
    return actual


def _rows(count, offset=0, extra=None):
    return [
        {
            'dt': START + timedelta(hours=i + offset),
            'id': (i + offset) % 3,
            'val': float(i + offset) / 2,
            'name': f'row-{i + offset}',
            **(extra or {}),
        }
        for i in range(count)
    ]


COLUMNS = {'datetime': 'dt', 'id': 'id'}


def test_documents_insert_update_and_unchanged(instance, native_calls):
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    steps = [
        (_rows(6), {}),
        (_rows(6), {}),
        (_rows(8, offset=2), {}),
        ([{**row, 'val': 99.5} for row in _rows(3)], {}),
    ]
    _compare(instance, 'docs_basic', params, steps, native_calls)


def test_documents_without_declared_dtypes(instance, native_calls):
    params = {'columns': COLUMNS}
    steps = [(_rows(5), {}), (_rows(7, offset=3), {})]
    _compare(instance, 'docs_inferred', params, steps, native_calls)


def test_sparse_documents_and_new_columns(instance, native_calls):
    params = {'columns': COLUMNS}
    first = [
        {'dt': START, 'id': 1, 'a': 1},
        {'dt': START + timedelta(hours=1), 'id': 1, 'b': 'x'},
        {'dt': START + timedelta(hours=2), 'id': 2, 'a': 3, 'b': 'y'},
    ]
    second = [{'dt': START + timedelta(hours=3), 'id': 1, 'c': 1.5, 'a': 4}]
    _compare(instance, 'docs_sparse', params, [(first, {}), (second, {})], native_calls)


def test_dict_of_lists_with_nulls_and_mixed_numbers(instance, native_calls):
    params = {'columns': COLUMNS}
    data = {
        'dt': [START + timedelta(hours=i) for i in range(4)],
        'id': [1, 2, 3, 4],
        'x': [1, 2.5, None, 4],
        'flag': [True, False, True, None],
        'text': ['a', None, 'c', 'd'],
    }
    _compare(instance, 'dict_lists', params, [(data, {}), (data, {})], native_calls)


def test_special_scalars_with_explicit_dtypes(instance, native_calls):
    params = {
        'columns': COLUMNS,
        'dtypes': {'u': 'uuid', 'b': 'bytes', 'n': 'numeric', 'j': 'json'},
    }
    rows = [
        {
            'dt': START + timedelta(hours=i),
            'id': i,
            'u': uuid.UUID(int=i + 1),
            'b': bytes([i, 255]),
            'n': Decimal('1.25') * (i + 1),
            'j': {'a': [i, {'b': None}], 'c': 'x'},
        }
        for i in range(3)
    ]
    _compare(instance, 'special_explicit', params, [(rows, {}), (rows, {})], native_calls)


def test_special_scalars_detected_and_persisted(instance, native_calls):
    """No declared dtypes: the sync detects the special columns and registers their dtypes."""
    params = {'columns': COLUMNS}
    rows = [
        {
            'dt': START + timedelta(hours=i),
            'id': i,
            'u': uuid.UUID(int=i + 1),
            'n': Decimal('0.50') * (i + 1),
            'j': {'k': i},
        }
        for i in range(3)
    ]
    actual = _compare(instance, 'special_detected', params, [(rows, {})], native_calls)
    dtypes = actual[0][1]['dtypes']
    assert dtypes.get('u') == 'uuid'
    assert dtypes.get('j') == 'json'
    assert str(dtypes.get('n', '')).startswith('numeric')


def test_datetime_timezones_and_precision(instance, native_calls):
    params = {'columns': COLUMNS}
    rows = [
        {'dt': datetime(2026, 3, 1, 12, 30, 45, 123456, tzinfo=timezone.utc), 'id': 1},
        {
            'dt': datetime(2026, 3, 1, 12, 30, 45, 654321, tzinfo=timezone(timedelta(hours=-5))),
            'id': 2,
        },
    ]
    _compare(instance, 'tz_precision', params, [(rows, {})], native_calls)


def test_naive_datetimes(instance, native_calls):
    params = {'columns': COLUMNS}
    rows = [
        {'dt': datetime(2026, 3, 1, 12, 0, 0), 'id': 1, 'v': 1},
        {'dt': datetime(2026, 3, 1, 13, 0, 0), 'id': 2, 'v': 2},
    ]
    _compare(instance, 'naive_dt', params, [(rows, {})], native_calls)


def test_iso_strings_in_a_datetime_column_are_parsed(instance, native_calls):
    params = {'columns': COLUMNS}
    rows = [
        {'dt': '2026-02-01 00:00:00+00:00', 'id': 1},
        {'dt': '2026-02-01 01:00:00+00:00', 'id': 2},
    ]
    _compare(instance, 'iso_strings', params, [(rows, {})], native_calls)


def test_empty_data(instance, native_calls):
    params = {'columns': COLUMNS, 'dtypes': {'v': 'int'}}
    steps = [([], {}), ({}, {}), (_rows(2), {}), ([], {})]
    _compare(instance, 'empty', params, steps, native_calls)


def test_all_null_column(instance, native_calls):
    params = {'columns': COLUMNS}
    rows = [{'dt': START, 'id': 1, 'empty': None}, {'dt': START + timedelta(hours=1), 'id': 2}]
    _compare(instance, 'all_null', params, [(rows, {})], native_calls)


def test_check_existing_false_and_chunksize(instance, native_calls):
    params = {'columns': COLUMNS}
    steps = [
        (_rows(10), {'check_existing': False, 'chunksize': 3}),
        (_rows(10), {'check_existing': False, 'chunksize': None}),
        (_rows(4), {'chunksize': 0}),
    ]
    _compare(instance, 'no_check', params, steps, native_calls)


def test_begin_and_end_do_not_change_the_data(instance, native_calls):
    params = {'columns': COLUMNS}
    steps = [
        (_rows(6), {'begin': START, 'end': START + timedelta(days=1)}),
        (_rows(6, offset=4), {'begin': None}),
        (_rows(6, offset=8), {'begin': ''}),
    ]
    _compare(instance, 'bounds', params, steps, native_calls)


def test_primary_key_upsert(instance, native_calls):
    params = {'columns': {'primary': 'id', 'datetime': 'dt'}, 'upsert': True}
    steps = [
        ([{'dt': START + timedelta(hours=i), 'id': i, 'v': i} for i in range(4)], {}),
        ([{'dt': START + timedelta(hours=i), 'id': i, 'v': i * 10} for i in range(2, 6)], {}),
    ]
    _compare(instance, 'upsert', params, steps, native_calls)


def test_autoincrement_primary_key_is_registered(instance, native_calls):
    params = {'columns': {'primary': 'pk', 'datetime': 'dt'}}
    rows = [{'dt': START + timedelta(hours=i), 'v': i} for i in range(3)]
    actual = _compare(instance, 'autoincrement', params, [(rows, {})], native_calls)
    assert actual[0][1]['autoincrement'] is True


def test_autotime_adds_the_timestamp_column(instance, native_calls):
    params = {'columns': {'id': 'id'}, 'autotime': True}
    rows = [{'id': 1, 'v': 1}, {'id': 2, 'v': 2}]
    expected = oracle.expected()
    actual = _run(instance, 'autotime', params, [(rows, {})])
    assert native_calls.handled == 1
    assert actual[0][0] == expected[0][0]
    ### Compare without the timestamps (and without editing the recorded value).
    stripped = []
    for state in (expected[0][1], actual[0][1]):
        assert all(doc.get('ts') is not None for doc in state['docs'])
        docs = [{k: v for k, v in doc.items() if k != 'ts'} for doc in state['docs']]
        stripped.append({**state, 'docs': docs})
    assert stripped[1] == stripped[0]


def test_unequal_columns_fail_like_pandas(instance, native_calls):
    params = {'columns': COLUMNS}
    data = {'dt': [START, START + timedelta(hours=1)], 'id': [1]}
    expected = oracle.expected()
    before = native_calls.handled
    actual = tuple(_fresh_pipe(instance, 'unequal', **params).sync(copy.deepcopy(data)))
    assert (actual[0], _untimed(actual[1])) == (expected[0], _untimed(expected[1]))
    assert actual[0] is False
    ### The data pandas rejects is answered by the binding (it is not a decline).
    assert native_calls.handled == before + 1 and not native_calls.declined


def test_the_native_result_carries_the_sync_fields(instance, native_calls):
    pipe = _fresh_pipe(instance, 'fields', columns=COLUMNS)
    result = pipe.sync(_rows(4), begin=START, end=START + timedelta(days=2))
    success, message = result
    assert success is True and result.success is True
    assert message == result.message
    assert result.inserted == 4
    assert result.updated == 0
    assert result.upserted == 0
    assert result.deleted == 0
    assert isinstance(result.duration, float) and result.duration >= 0
    assert result.begin == START
    assert result.end == START + timedelta(days=2)
    assert native_calls.handled == 1

    again = pipe.sync(_rows(4))
    assert tuple(again)[0] is True
    assert again.inserted == 0

    changed = pipe.sync([{**_rows(1)[0], 'val': -1.0}])
    assert changed.updated == 1


def test_sync_result_is_a_picklable_json_friendly_tuple():
    import json
    from meerschaum.core.Pipe._sync import SyncResult
    result = SyncResult(True, 'ok', inserted=3, duration=0.5, begin=START)
    assert result == (True, 'ok')
    assert json.dumps(result) == '[true, "ok"]'
    clone = pickle.loads(pickle.dumps(result))
    assert type(clone) is SyncResult
    assert clone.inserted == 3 and clone.begin == START and clone.updated is None
    assert copy.deepcopy(result).message == 'ok'
    assert SyncResult.from_tuple((False, 'no')).success is False
    assert type(SyncResult.from_tuple((1, 2, 3))) is tuple
    with pytest.raises(TypeError):
        SyncResult(True, 'x', bogus=1)


def test_registration_and_indices_validation_stay_in_the_facade(instance, native_calls):
    pipe = _fresh_pipe(
        instance, 'json_index', columns={'datetime': 'dt', 'id': 'doc'}, dtypes={'doc': 'json'},
    )
    success, message = pipe.sync([{'dt': START, 'doc': {'a': 1}}])
    assert success is False
    assert 'json' in message.lower()
    assert native_calls.handled == 0

    pipe = _fresh_pipe(instance, 'registers', columns=COLUMNS)
    assert pipe.id is None
    assert pipe.sync(_rows(1))[0] is True
    assert pipe.id is not None


def test_the_python_pipe_sees_the_parameters_the_sync_changed(instance, native_calls):
    pipe = _fresh_pipe(instance, 'patched', columns={'primary': 'pk', 'datetime': 'dt'})
    assert pipe.sync([{'dt': START, 'v': 1}])[0] is True
    assert pipe.parameters.get('autoincrement') is True
    ### Unsaved in-memory edits survive the sync (v4 persists them with the `autoincrement`
    ### edit as well: `update_parameters(..., persist=True)` writes the whole dictionary).
    pipe.parameters['unsaved'] = {'keep': 'me'}
    assert pipe.sync([{'dt': START + timedelta(hours=1), 'v': 2}])[0] is True
    assert pipe.parameters['unsaved'] == {'keep': 'me'}


@pytest.mark.parametrize(
    'kwargs',
    [
        {'force': True, 'retries': 2, 'min_seconds': 0},
        {'workers': 2},
        {'debug': True},
        {'plugin_flag': 1},
        {'callback': print},
        {'sync_chunks': False},
    ],
    ids=['force', 'workers', 'debug', 'plugin_flag', 'callback', 'sync_chunks'],
)
def test_keyword_arguments_run_natively(instance, native_calls, kwargs, capsys):
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    steps = [(_rows(4), kwargs), (_rows(6, offset=2), kwargs)]
    _compare(instance, 'kwargs', params, steps, native_calls)


def test_non_blocking_syncs_spawn_a_thread_and_call_back(instance, native_calls):
    import threading
    params = {'columns': COLUMNS}
    results = []
    done = threading.Event()

    def callback(result):
        results.append(result)
        done.set()

    pipe = _fresh_pipe(instance, 'nonblocking', **params)
    spawned = pipe.sync(_rows(3), blocking=False, callback=callback)
    assert tuple(spawned) == (True, f"Spawned asyncronous sync for {pipe}.")
    assert done.wait(30)
    assert results[0][0] is True
    assert native_calls.handled == 1


def _outcomes(instance, name, params, steps):
    """The `(tuple | exception)` of each step and the final rows."""
    pipe = _fresh_pipe(instance, name, **copy.deepcopy(params))
    out = []
    for data, kwargs in steps:
        try:
            out.append(tuple(pipe.sync(copy.deepcopy(data), **kwargs)))
        except Exception as e:
            out.append((type(e).__name__, str(e)))
    return out, _snapshot(pipe)


NO_ENFORCE_CASES = [
    ({'datetime': 'dt', 'id': 'id'}, {'dt': START, 'id': 1}),
    ({'datetime': 'ts', 'id': 'id'}, {'ts': 1, 'id': 1}),
    ({'id': 'id'}, {'id': 1}),
    ({'datetime': 'dt', 'id': 'id'}, {'dt': START.replace(tzinfo=None), 'id': 1}),
]
NO_ENFORCE_IDS = ['datetime-axis', 'integer-axis', 'no-axis', 'naive-datetime-axis']


def _no_enforce_steps(key):
    return [([{**key, 'v': 1}], {}), ([{**key, 'v': 2}], {}), ([{**key, 'v': 2}], {})]


@pytest.mark.parametrize('columns, key', NO_ENFORCE_CASES, ids=NO_ENFORCE_IDS)
def test_pipes_that_do_not_enforce_are_synced_natively(instance, native_calls, columns, key):
    """
    `enforce: False` pipes and `enforce_dtypes=False` syncs are no longer declined (PARITY H9).
    """
    params = {'columns': columns, 'enforce': False}
    pipe = _fresh_pipe(instance, 'no_enforce_native', **params)
    for data, kwargs in _no_enforce_steps(key):
        try:
            pipe.sync(copy.deepcopy(data), **kwargs)
        except ValueError:
            ### The timezone-naive axis raises v4's pandas merge error.
            pass
    assert native_calls.declined == []
    assert native_calls.handled == 3


@pytest.mark.parametrize('columns, key', NO_ENFORCE_CASES, ids=NO_ENFORCE_IDS)
def test_pipes_that_do_not_enforce_match_v4(instance, columns, key):
    params = {'columns': columns, 'enforce': False}
    steps = _no_enforce_steps(key)
    expected = oracle.expected()
    actual = _outcomes(instance, 'no_enforce', params, steps)
    assert actual == expected


@pytest.mark.parametrize('columns, key', NO_ENFORCE_CASES, ids=NO_ENFORCE_IDS)
def test_syncs_with_enforce_dtypes_false_match_v4(instance, columns, key):
    """`Pipe.sync(enforce_dtypes=False)` on a pipe that does enforce (the first sync does)."""
    params = {'columns': columns}
    first, *rest = _no_enforce_steps(key)
    steps = [first] + [(data, {'enforce_dtypes': False}) for data, _ in rest]
    expected = oracle.expected()
    actual = _outcomes(instance, 'no_enforce_kw', params, steps)
    assert actual == expected


def _naive_axis_error(instance):
    """The message of the pandas merge error the second sync raises."""
    columns, key = NO_ENFORCE_CASES[3]
    params = {'columns': columns, 'enforce': False}
    steps = _no_enforce_steps(key)
    pipe = _fresh_pipe(instance, 'no_enforce_naive_error', **copy.deepcopy(params))
    assert pipe.sync(copy.deepcopy(steps[0][0]))[0] is True
    with pytest.raises(
        ValueError, match=r"merge on datetime64\[us\] and datetime64\[us, UTC\]",
    ) as raised:
        pipe.sync(copy.deepcopy(steps[1][0]))
    return str(raised.value)


def test_a_naive_axis_raises_the_pandas_merge_error(instance):
    expected = oracle.expected()
    assert _naive_axis_error(instance) == expected


def _no_columns_error(instance):
    pipe = _fresh_pipe(instance, 'no_columns')
    with pytest.raises(ValueError, match="Expecting columns for table") as raised:
        pipe.sync([{}, {}])
    return str(raised.value)


def test_documents_without_any_column_raise_like_v4(instance, native_calls):
    expected = oracle.expected()
    assert _no_columns_error(instance) == expected


def test_integers_beyond_64_bits_sync_as_numeric_only_natively(instance, native_calls):
    """PARITY H8: v4 cannot build the DataFrame (`DataFrame is None`); the native path can."""
    v4_success = oracle.expected()
    assert v4_success is False
    pipe = _fresh_pipe(instance, 'bigint')
    assert pipe.sync([{'a': 2 ** 70}])[0] is True
    assert pipe.get_parameters(refresh=True)['dtypes']['a'] == 'numeric'


def _static_rows(instance, params, steps):
    pipe = _fresh_pipe(instance, 'static_new', **params)
    assert pipe.sync(steps[0][0])[0] is True
    ### Only the failure and the unchanged rows are shared: v4's message is a traceback text.
    assert pipe.sync(steps[1][0])[0] is False
    return _docs(pipe)


def test_a_static_pipe_refuses_new_columns(instance, native_calls):
    params = {'columns': {'datetime': 'dt'}, 'static': True}
    steps = [
        ([{'dt': START, 'v': 1}], {}),
        ([{'dt': START + timedelta(days=1), 'v': 2, 'w': 'new'}], {}),
    ]
    expected = oracle.expected()
    actual = _run(instance, 'static_new', params, steps[:1])
    assert actual[0][0] == expected[0][0]
    expected_rows = oracle.expected()
    assert _static_rows(instance, params, steps) == expected_rows


def _sync_none(instance):
    pipe = _fresh_pipe(instance, 'none_data', columns=COLUMNS)
    return tuple(pipe.sync(None))


def test_none_instead_of_data_is_refused_like_v4(instance, native_calls):
    expected = oracle.expected()
    actual = _sync_none(instance)
    assert (actual[0], _untimed(actual[1])) == (expected[0], _untimed(expected[1]))
    assert actual[0] is False


def _temporary_outcome(instance):
    pipe = mrsm.Pipe(
        'w13h', 'temporary_pipe', instance=instance, temporary=True, columns=COLUMNS,
    )
    pipe.drop()
    result = pipe.sync(_rows(3))
    again = pipe.sync(_rows(3))
    outcome = (
        tuple(result)[0],
        tuple(again),
        _docs(pipe),
        mrsm.Pipe('w13h', 'temporary_pipe', instance=instance).id,
    )
    pipe.drop()
    return outcome


def test_temporary_pipes_are_never_registered(instance, native_calls):
    expected = oracle.expected()
    actual = _temporary_outcome(instance)
    assert actual == expected
    assert actual[3] is None


def _references_outcome(instance):
    base_params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    base = _fresh_pipe(instance, 'ref_base', **copy.deepcopy(base_params))
    assert base.register()[0]
    referenced = _fresh_pipe(instance, 'ref_child', reference='w13h:ref_base')
    assert referenced.register()[0]
    first = tuple(referenced.sync(_rows(4)))
    second = tuple(referenced.sync(_rows(6, offset=2)))
    templated = _fresh_pipe(
        instance, 'ref_templated',
        columns=COLUMNS,
        tags=["{{ self.parameters['columns']['id'] }}"],
    )
    third = tuple(templated.sync(_rows(3)))
    return (first, second, _docs(referenced), third, _docs(templated))


def test_references_and_templates_resolve_in_the_native_flow(instance, native_calls):
    expected = oracle.expected()
    assert _references_outcome(instance) == expected


SHAPES = {
    'text': 'id:1,val:2.5\nid:2,val:3.5\n',
    'list_of_rows': [[1, 'a'], [2, 'b']],
    'list_of_scalars': [1, 2, 3],
    'scalar_broadcast': {'id': [1, 2, 3], 'tag': 'x'},
    'integer_keys': {1: [1, 2], 2: ['a', 'b']},
}


@pytest.mark.parametrize('shape', sorted(SHAPES), ids=sorted(SHAPES))
def test_other_data_shapes_match_v4(instance, native_calls, shape):
    steps = [(SHAPES[shape], {})]
    expected = oracle.expected()
    handled = native_calls.handled
    actual = _run(instance, f'shape_{shape}', {}, steps)
    assert native_calls.handled - handled == 1, native_calls.declined
    assert actual[0][0][0] == expected[0][0][0]
    assert actual[0][1]['docs'] == expected[0][1]['docs']


def test_a_tuple_of_documents_is_rows_like_pandas(instance, native_calls):
    # PARITY: v4 takes a tuple for a chunk generator and raises `TypeError: 'tuple' object is not an
    # iterator`; pandas (and the native path) read it as rows.
    as_tuple = _fresh_pipe(instance, 'tuple_docs', columns=COLUMNS)
    as_list = _fresh_pipe(instance, 'list_docs', columns=COLUMNS)
    assert tuple(as_tuple.sync(tuple(_rows(3))))[0] is True
    assert tuple(as_list.sync(list(_rows(3))))[0] is True
    assert _docs(as_tuple) == _docs(as_list)


def test_generators_sync_chunk_by_chunk_like_v4(instance, native_calls):
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}

    def chunks():
        for offset in (0, 3, 6):
            yield _rows(3, offset=offset)

    def outcome():
        pipe = _fresh_pipe(instance, 'generator', **copy.deepcopy(params))
        result = pipe.sync(chunks())
        empty = pipe.sync(iter(()))
        return (result[0], tuple(empty), _docs(pipe))

    expected = oracle.expected()
    actual = outcome()
    assert actual[0] == expected[0]
    assert actual[1] == expected[1]
    assert actual[2] == expected[2]


def test_dask_data_is_refused_with_the_migration_guide(instance):
    pipe = _fresh_pipe(instance, 'dask', columns=COLUMNS)

    ### The refusal is by type name, so it needs no Dask installed (no extra provides it).
    class _DaskLikeFrame:
        __module__ = 'dask.dataframe.dask_expr._collection'

    with pytest.raises(TypeError, match='migrating-to-v5'):
        pipe.sync(_DaskLikeFrame())
    dd = pytest.importorskip('dask.dataframe')
    pd = pytest.importorskip('pandas')
    with pytest.raises(TypeError, match='migrating-to-v5'):
        pipe.sync(dd.from_pandas(pd.DataFrame(_rows(3)), npartitions=1))


def test_pandas_and_polars_frames(instance, native_calls):
    pd = pytest.importorskip('pandas')
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float'}}
    frame = pd.DataFrame(_rows(5))
    frame2 = pd.DataFrame(_rows(5, offset=3))
    expected = oracle.expected()
    pipe = _fresh_pipe(instance, 'pandas_frames', **params)
    before = native_calls.handled
    for df, (exp_tuple, exp_docs) in zip((frame, frame2), expected):
        before_df = df.copy()
        result = tuple(pipe.sync(df))
        assert result == exp_tuple
        assert _docs(pipe) == exp_docs
        pd.testing.assert_frame_equal(df, before_df)
    assert native_calls.handled - before == 2

    pl = pytest.importorskip('polars')
    exp = oracle.expected()
    pipe = _fresh_pipe(instance, 'polars_frames', **params)
    got = (tuple(pipe.sync(pl.DataFrame(_rows(5)))), _docs(pipe))
    assert got == exp


def test_construct_register_sync_and_read_docs_need_no_sqlalchemy_or_dataframes(tmp_path):
    """
    A fresh interpreter constructs the pipe, registers it, syncs documents and
    reads them back without importing SQLAlchemy, pandas or Polars.
    """
    code = '''
import os, sys
### Importing any of these (which the SQL, dataframe and typing helpers do) fails loudly.
BLOCKED = ('sqlalchemy', 'pandas', 'polars', 'numpy', 'pyarrow', 'geopandas', 'dask')
for name in BLOCKED:
    sys.modules[name] = None
import meerschaum as mrsm
from datetime import datetime, timezone
conn = mrsm.get_connector('sql', 'w13h_lean', flavor='sqlite', database=os.environ['W13H_DB'])
pipe = mrsm.Pipe('w13h', 'lean', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
assert pipe.register()[0]
dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
result = pipe.sync([{'dt': dt, 'id': 1, 'v': 'a'}, {'dt': dt, 'id': 2, 'v': 'b'}])
assert result[0], result
assert result.inserted == 2, result.inserted
docs = pipe.get_data(as_docs=True)
assert [d['v'] for d in docs] == ['a', 'b'], docs
assert all(sys.modules[name] is None for name in BLOCKED), 'a blocked package was imported'
assert not any(m.startswith('sqlalchemy.') for m in sys.modules)
assert '_engine' not in conn.__dict__, 'the SQLAlchemy engine was created'
print('LEAN_OK')
'''
    env = {
        key: value for key, value in os.environ.items() if not key.startswith('MRSM_')
    }
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'W13H_DB': str(tmp_path / 'lean.db'),
        'PYTHONPATH': os.getcwd(),
    })
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'root').mkdir()
    completed = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=180,
    )
    assert 'LEAN_OK' in completed.stdout, completed.stdout + completed.stderr


###########################################################################################
### Wave 15 (task L): `Pipe.sync()` with no data runs the pipe's source natively.
###########################################################################################

def _source_pipe(instance, connector_keys, metric, **params):
    pipe = mrsm.Pipe(connector_keys, metric, instance=instance, **params)
    pipe.delete()
    pipe.drop()
    pipe._invalidate_cache(hard=True)
    return mrsm.Pipe(connector_keys, metric, instance=instance, **params)


def _source_outcomes(instance, connector_keys, metric, params, steps):
    """Run `steps` (sync keyword arguments, no data) on a fresh pipe."""
    pipe = _source_pipe(instance, connector_keys, metric, **copy.deepcopy(params))
    observed = []
    for kwargs in steps:
        result = pipe.sync(**kwargs)
        assert isinstance(result, tuple)
        observed.append((tuple(result), _snapshot(pipe)))
    return observed


def _compare_sources(
    instance, native_calls, connector_keys, metric, params, steps, messages=True,
):
    expected = oracle.expected()
    handled = native_calls.handled
    actual = _source_outcomes(instance, connector_keys, metric, params, steps)
    assert native_calls.handled - handled == len(steps), native_calls.declined
    for step, ((exp_tuple, exp_state), (act_tuple, act_state)) in enumerate(
        zip(expected, actual)
    ):
        assert act_tuple[0] == exp_tuple[0], f"step {step}: {act_tuple} vs {exp_tuple}"
        if messages:
            assert _untimed(act_tuple[1]) == _untimed(exp_tuple[1]), f"step {step}"
        for key in exp_state:
            assert act_state[key] == exp_state[key], f"step {step}: {key}"
    return actual


PLUGIN_SOURCE = 'plugin:w15l_plugsrc'


def test_a_plugin_source_syncs_natively(instance, native_calls):
    steps = [
        {},
        {},
        {'begin': START + timedelta(hours=3), 'end': START + timedelta(hours=8)},
    ]
    actual = _compare_sources(instance, native_calls, PLUGIN_SOURCE, 'w15l_frame', {}, steps)
    assert len(actual[0][1]['docs']) == 12


@pytest.mark.parametrize('mode', ['generator', 'hook', 'none', 'true'])
def test_plugin_fetch_results_other_than_a_frame(instance, native_calls, mode):
    ### `generator`: every chunk is a sync of its own; `hook`: the plugin syncs through
    ### `chunk_hook` and returns the results; `none` / `true`: v4's special return values.
    params = {'parameters': {'fetch': {'mode': mode}}}
    actual = _compare_sources(
        instance, native_calls, PLUGIN_SOURCE, f'w15l_{mode}', params, [{}, {}], messages=False,
    )
    expected_rows = {'generator': 12, 'hook': 12, 'none': 0, 'true': 0}[mode]
    assert len(actual[0][1]['docs']) == expected_rows


def test_a_plugins_own_sync_function_runs_natively(instance, native_calls):
    ### `sync()` of the plugin calls `pipe.sync(df)` itself, which re-enters the native flow.
    steps = [{}, {'flag': 7}]
    actual = _compare_sources(
        instance, native_calls, 'plugin:w15l_plugsync', 'w15l_sync', {}, steps,
    )
    assert len(actual[0][1]['docs']) == 2


def test_a_custom_connector_source_syncs_natively(instance, native_calls):
    params = {'columns': {'datetime': 'ts', 'id': 'sensor'}}
    actual = _compare_sources(
        instance, native_calls, 'w15lfeed:main', 'w15l_feed', params, [{}, {}],
    )
    assert len(actual[0][1]['docs']) == 4


def test_a_sql_source_with_a_definition_syncs_natively(instance, native_calls, tmp_path):
    source_conn = mrsm.get_connector(
        'sql', 'w15l_source_' + uuid.uuid4().hex[:6],
        flavor='sqlite',
        database=str(tmp_path / 'w15l_source.db'),
    )
    source = mrsm.Pipe('w15l', 'source', instance=source_conn, columns=COLUMNS)
    source.delete()
    source = mrsm.Pipe(
        'w15l', 'source', instance=source_conn, columns=COLUMNS,
        dtypes={'val': 'float', 'name': 'str'},
    )
    assert source.sync(_rows(10))[0]
    from meerschaum.utils.sql import sql_item_name
    table = sql_item_name(source.target, source_conn.flavor, None)
    params = {
        'columns': COLUMNS,
        'dtypes': {'val': 'float', 'name': 'str'},
        'parameters': {'fetch': {'definition': f'SELECT * FROM {table}'}},
    }
    steps = [{}, {}, {'begin': START + timedelta(hours=4)}]
    actual = _compare_sources(instance, native_calls, str(source_conn), 'w15l_sql', params, steps)
    assert len(actual[0][1]['docs']) == 10


def test_plugin_keyword_arguments_reach_the_plugin(instance, native_calls):
    pipe = _source_pipe(instance, 'plugin:w15l_plugsync', 'w15l_kwargs')
    assert pipe.sync(flag=11)[0]
    assert sorted(d['val'] for d in _docs(pipe)) == [1, 11]


def test_non_blocking_source_syncs_call_back(instance, native_calls):
    import threading
    results = []
    done = threading.Event()

    def callback(result):
        results.append(result)
        done.set()

    pipe = _source_pipe(instance, PLUGIN_SOURCE, 'w15l_nonblocking')
    spawned = pipe.sync(blocking=False, callback=callback)
    assert tuple(spawned) == (True, f"Spawned asyncronous sync for {pipe}.")
    assert done.wait(60)
    assert results[0][0] is True
    assert len(_docs(pipe)) == 12


def test_changed_intervals_are_filled(instance, native_calls):
    pipe = _source_pipe(instance, 'w13h', 'w15l_intervals', columns=COLUMNS)
    first = pipe.sync(_rows(4))
    assert first.changed_intervals, first.to_dict()
    ((begin, end),) = first.changed_intervals
    assert begin <= START < end
    again = pipe.sync(_rows(4))
    assert again.changed_intervals == []
    nothing = pipe.sync([])
    assert not nothing.changed_intervals
    ### Rows spread over several chunk intervals report one interval per run of cells.
    wide = _source_pipe(
        instance, 'w13h', 'w15l_intervals_wide',
        columns=COLUMNS, parameters={'verify': {'chunk_minutes': 60}},
    )
    result = wide.sync(_rows(3, offset=0) + _rows(3, offset=10))
    assert len(result.changed_intervals) == 2
    assert result.changed_intervals[0][1] <= result.changed_intervals[1][0]


def test_syncresult_is_public():
    assert mrsm.SyncResult.__name__ == 'SyncResult'
    result = mrsm.SyncResult(True, 'ok', inserted=1)
    assert tuple(result) == (True, 'ok')
    assert result.inserted == 1


EXTRA_INSTANCES = ['memory']


@pytest.fixture(params=EXTRA_INSTANCES)
def other_instance(request, tmp_path):
    if request.param == 'memory':
        yield mrsm.get_connector(
            'sql', 'w15l_mem_' + uuid.uuid4().hex[:8], flavor='sqlite', database=':memory:',
        )
        return


def test_in_memory_instances_sync_natively(other_instance, native_calls):
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    steps = [(_rows(6), {}), (_rows(6), {}), (_rows(8, offset=2), {})]
    _compare(other_instance, 'other_docs', params, steps, native_calls)


def _server_instance(flavor, tmp_path=None):
    if flavor == 'duckdb':
        return mrsm.get_connector(
            'sql', 'w15l_duck_' + uuid.uuid4().hex[:6],
            flavor='duckdb', database=str(tmp_path / 'w15l.duckdb'),
        )
    if flavor == 'mssql':
        return mrsm.get_connector(
            'sql', 'w15l_mssql',
            flavor='mssql', username='sa', password='supersecureSECRETPASSWORD123!',
            database='master', port=1439, host='localhost', schema='dbo',
        )
    if flavor == 'oracle':
        return mrsm.get_connector(
            'sql', 'w15l_oracle',
            flavor='oracle', host='localhost', database='xe', username='system',
            password='oracle', port=1529,
        )
    raise ValueError(flavor)


@pytest.mark.parametrize('flavor', ['duckdb', 'mssql', 'oracle'])
def test_duckdb_mssql_and_oracle_instances_sync_natively(flavor, native_calls, tmp_path):
    """
    The Rust drivers of `duckdb`, `mssql` and `oracle` run the whole flow (their Python drivers
    are not needed): the rows and the counts equal what the recorded Python mode gave on a SQLite instance.
    """
    if flavor != 'duckdb' and flavor not in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(','):
        pytest.skip(f"set MRSM_TEST_FLAVORS to include {flavor}")
    if flavor == 'oracle':
        from meerschaum.connectors.sql._SQLConnector import _oracle_client_available
        if not _oracle_client_available():
            pytest.skip("the Oracle client library (libclntsh) is not installed")
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    steps = [(_rows(6), {}), (_rows(6), {}), (_rows(8, offset=2), {})]
    # The recorded result is the SQLite-instance run of this scenario, which does not depend on the
    # instance flavor: the `[duckdb]` and `[mssql]` nodes are byte-identical, and `[oracle]` (no v4
    # Oracle driver where it was recorded) reads the same node instead of a hand-extended copy.
    expected = oracle.expected(
        'tests/test_native_sync.py::test_duckdb_mssql_and_oracle_instances_sync_natively[duckdb]'
    )
    instance = _server_instance(flavor, tmp_path)
    handled = native_calls.handled
    actual = _run(instance, f'w15l_{flavor}', params, steps)
    assert native_calls.handled - handled == len(steps), native_calls.declined
    for (exp_tuple, exp_state), (act_tuple, act_state) in zip(expected, actual):
        assert act_tuple[0] == exp_tuple[0] is True
        assert act_tuple[1] == exp_tuple[1]
        key = lambda doc: (doc['id'], doc['name'])  # noqa: E731
        strip = lambda docs: sorted(  # noqa: E731
            ({k: v for k, v in doc.items() if k != 'dt'} for doc in docs), key=key,
        )
        assert strip(act_state['docs']) == strip(exp_state['docs'])
    _fresh_pipe(instance, f'w15l_{flavor}', **params)


def test_a_custom_instance_connector_syncs_through_the_worker(native_calls):
    """`@make_connector` instances (`IS_INSTANCE = True`) are driven by the worker adapter."""
    def outcome():
        instance = mrsm.get_connector('w15lmem', f'w15l_{uuid.uuid4().hex[:6]}')
        pipe = mrsm.Pipe('w13h', 'w15l_custom_instance', instance=instance, columns=COLUMNS)
        first = tuple(pipe.sync(_rows(4)))
        return (first, len(pipe.get_data(as_docs=True) or []))

    expected = oracle.expected()
    actual = outcome()
    assert native_calls.handled == 1, native_calls.declined
    assert actual[0][0] is True
    assert actual == expected


def _resp_keys(pattern, port=6399):
    """`KEYS pattern` over a raw RESP socket (no valkey package needed)."""
    import socket
    command = f'*2\r\n$4\r\nKEYS\r\n${len(pattern)}\r\n{pattern}\r\n'.encode()
    with socket.create_connection(('localhost', port), timeout=10) as sock:
        sock.sendall(command)
        data = b''
        while not data.endswith(b'\r\n') or data.startswith(b'*') and data.count(b'\r\n') < 1:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
            if data.startswith(b'*0\r\n'):
                break
            header, _, rest = data.partition(b'\r\n')
            expected = int(header[1:])
            if rest.count(b'\r\n') >= 2 * expected:
                break
    lines = data.split(b'\r\n')
    return [line.decode() for line in lines[1:] if line and not line.startswith(b'$')]


def _resp_delete(patterns, port=6399):
    import socket
    for pattern in patterns:
        keys = _resp_keys(pattern, port=port)
        if not keys:
            continue
        parts = [b'DEL'] + [key.encode() for key in keys]
        command = f'*{len(parts)}\r\n'.encode() + b''.join(
            f'${len(part)}\r\n'.encode() + part + b'\r\n' for part in parts
        )
        with socket.create_connection(('localhost', port), timeout=10) as sock:
            sock.sendall(command)
            sock.recv(1024)


def test_a_valkey_instance_syncs_natively(native_calls):
    """The Rust `ValkeyConnector` is the instance: rows land under the pipe's index keys."""
    if 'valkey' not in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(','):
        pytest.skip("set MRSM_TEST_FLAVORS to include valkey")
    instance = mrsm.get_connector('valkey', 'w15l_valkey', host='localhost', port=6399)
    pipe = mrsm.Pipe(
        'w13h', 'w15l_valkey_' + uuid.uuid4().hex[:6], instance=instance, columns=COLUMNS,
    )
    pattern = f'{pipe.target}:indices:*'
    result = pipe.sync(_rows(5))
    assert result[0] is True, result
    assert native_calls.handled == 1, native_calls.declined
    assert len(_resp_keys(pattern)) == 5
    again = pipe.sync(_rows(5))
    assert again[0] is True
    assert len(_resp_keys(pattern)) == 5
    _resp_delete([f'{pipe.target}*', f'mrsm_pipe:w13h:{pipe.metric_key}:*'])


@pytest.fixture(scope='module')
def api_server():
    """A loopback `mrsm start api` (random port, temporary root; never 8989)."""
    from tests._binary import binary_or_skip
    binary_or_skip()
    from tests.console.conftest import Server
    server = Server('w15l-sync', native=False)
    try:
        server.start()
    except Exception as e:
        server.stop()
        pytest.skip(f"cannot start a loopback API server: {e}")
    yield server
    server.stop()


def test_an_api_instance_syncs_natively(api_server, native_calls):
    """The Rust `APIConnector` is the instance and leaves the rows v4 leaves on the server."""
    from tests.console.conftest import USERNAME, PASSWORD

    def outcome():
        instance = mrsm.get_connector(
            'api', 'w15l_api',
            host='127.0.0.1', port=api_server.port, username=USERNAME, password=PASSWORD,
            protocol='http',
        )
        pipe = mrsm.Pipe(
            'w13h', 'w15l_api', instance=instance, columns=COLUMNS,
            dtypes={'val': 'float', 'name': 'str'},
        )
        pipe.delete()
        pipe = mrsm.Pipe(
            'w13h', 'w15l_api', instance=instance, columns=COLUMNS,
            dtypes={'val': 'float', 'name': 'str'},
        )
        first = tuple(pipe.sync(_rows(4)))
        second = tuple(pipe.sync(_rows(6, offset=2)))
        result = (first[0], second[0], sorted(d['id'] for d in _docs(pipe)))
        pipe.delete()
        return result

    expected = oracle.expected()
    actual = outcome()
    assert native_calls.handled == 2, native_calls.declined
    assert actual[:2] == (True, True)
    assert actual == expected


def _sync_with_deadline(pipe, seconds=90, **kwargs):
    """`pipe.sync()` on a thread: a hang fails the test instead of the whole run."""
    import threading
    box = {}

    def run():
        try:
            box['result'] = pipe.sync(**kwargs)
        except Exception as e:  # noqa: BLE001
            box['error'] = e

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), f"{pipe}.sync() hung for {seconds} seconds"
    return box


def test_a_crashing_worker_is_an_error_not_a_hang(instance, native_calls):
    """The plugin kills its worker process mid-`fetch` (after calling back into the host)."""
    pipe = _source_pipe(instance, 'plugin:w15l_plugdie', 'w15l_die')
    for _ in range(2):
        box = _sync_with_deadline(pipe)
        assert 'error' in box or box['result'][0] is False, box
    ### The pool replaced the dead worker: a healthy plugin syncs right after.
    healthy = _source_pipe(instance, PLUGIN_SOURCE, 'w15l_after_death')
    box = _sync_with_deadline(healthy)
    assert box['result'][0] is True, box


def test_a_worker_killed_while_idle_is_replaced(instance, native_calls):
    import signal
    import subprocess
    pipe = _source_pipe(instance, PLUGIN_SOURCE, 'w15l_idle_kill')
    assert _sync_with_deadline(pipe)['result'][0] is True
    children = subprocess.run(
        ['pgrep', '-P', str(os.getpid()), '-f', 'meerschaum.worker'],
        capture_output=True, text=True,
    ).stdout.split()
    for pid in children:
        os.kill(int(pid), signal.SIGKILL)
    import time
    time.sleep(0.5)
    again = _sync_with_deadline(pipe)
    assert again['result'][0] is True, again


###########################################################################################
### Wave 15 (L) follow-up: exact `numeric` read-back and the live config in a native sync.
###########################################################################################

def _numeric_instance(flavor, tmp_path):
    if flavor == 'sqlite':
        return mrsm.get_connector(
            'sql', 'w15l_num_' + uuid.uuid4().hex[:8],
            flavor='sqlite', database=str(tmp_path / 'num.db'),
        )
    selected = os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')
    if flavor not in selected:
        pytest.skip(f"set MRSM_TEST_FLAVORS to include {flavor}")
    if flavor == 'postgis':
        return mrsm.get_connector(
            'sql', 'w15l_num_postgis',
            flavor='postgis', username='test', password='test1234', database='testdb',
            port=5449, host='localhost', schema='w15l_num',
        )
    if flavor == 'mariadb':
        return mrsm.get_connector(
            'sql', 'w15l_num_mariadb',
            flavor='mariadb', username='test', password='test1234', database='testdb',
            port=3309, host='localhost',
        )
    if flavor == 'mysql':
        return mrsm.get_connector(
            'sql', 'w15l_num_mysql',
            flavor='mysql', username='root', password='my-secret-pw', database='mysql',
            port=3310, host='localhost',
        )
    raise ValueError(flavor)


NUMERIC_VALUES = [
    Decimal('3'), Decimal('2.5'), Decimal('-0.10'),
]
FORTY_DIGITS = Decimal('1234567890123456789012345678901234567890')


@pytest.mark.parametrize('flavor', ['sqlite', 'postgis', 'mariadb', 'mysql'])
def test_numeric_values_read_back_like_v4(flavor, tmp_path, native_calls):
    """
    Each `numeric` value keeps the scale the database stored (`3`, `2.5`, `-0.10`), as v4
    reads them: a PostgreSQL `NUMERIC` of unconstrained scale used to come back padded to the
    widest scale of the result (`Decimal('3.0')`). A 40-digit value only fits PostgreSQL.
    """
    instance = _numeric_instance(flavor, tmp_path)
    if flavor == 'postgis':
        instance.exec('CREATE SCHEMA IF NOT EXISTS "w15l_num"')
    values = list(NUMERIC_VALUES) + ([FORTY_DIGITS] if flavor == 'postgis' else [])
    docs = [
        {'dt': START + timedelta(hours=i), 'id': i, 'n': value}
        for i, value in enumerate(values)
    ]
    def outcome():
        pipe = mrsm.Pipe(
            'w15l', 'numeric', flavor, instance=instance,
            columns=COLUMNS, dtypes={'n': 'numeric'},
        )
        pipe.delete()
        pipe = mrsm.Pipe(
            'w15l', 'numeric', flavor, instance=instance,
            columns=COLUMNS, dtypes={'n': 'numeric'},
        )
        assert pipe.sync(copy.deepcopy(docs))[0]
        frame = pipe.get_data()
        result = (
            [repr(d['n']) for d in sorted(_docs(pipe), key=lambda d: d['id'])],
            sorted(repr(v) for v in frame['n'].tolist()),
        )
        pipe.delete()
        return result

    expected = oracle.expected()
    actual = outcome()
    assert actual == expected
    if flavor == 'postgis':
        assert "Decimal('2.5')" in actual[0]
        assert f"Decimal('{FORTY_DIGITS}')" in actual[0]


def test_mrsm_symlinks_follow_the_live_config(instance, native_calls):
    """
    A change to the live config dictionary (what `set_config()` / `get_config()` edit) is seen
    by the native sync: the `id` column of the pipe is
    `MRSM{w15l_live:idcol}`, and changing the configured column between two syncs changes the
    key the rows are matched on (v4 resolves the symlink on every `pipe.parameters` read).
    """
    import meerschaum.config as mrsm_config
    rows = [
        {'dt': START, 'id': 1, 'tag': 'a'},
        {'dt': START, 'id': 2, 'tag': 'a'},
    ]
    def outcome():
        seen = []
        for column in ('id', 'tag'):
            mrsm_config.config['w15l_live'] = {'idcol': column}
            pipe = _fresh_pipe(
                instance, f'live_{column}',
                columns={'datetime': 'dt', 'id': 'MRSM{w15l_live:idcol}'},
            )
            results = tuple(tuple(pipe.sync([copy.deepcopy(row)])) for row in rows)
            seen.append((results, len(_docs(pipe))))
        mrsm_config.config.pop('w15l_live', None)
        return seen

    expected = oracle.expected()
    actual = outcome()
    assert actual == expected
    ### Matched on `id` the two rows are two rows; matched on `tag` they are one.
    assert [count for _, count in actual] == [2, 1]


def test_arrow_tables_sync_natively_and_other_types_are_refused(instance, native_calls):
    pa = pytest.importorskip('pyarrow')
    rows = _rows(5)
    table = pa.Table.from_pylist(rows)
    batch = pa.RecordBatch.from_pylist(rows)
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float', 'name': 'str'}}
    expected = _fresh_pipe(instance, 'arrow_list', **params)
    assert expected.sync(copy.deepcopy(rows))[0]
    for name, data in (('arrow_table', table), ('arrow_batch', batch)):
        pipe = _fresh_pipe(instance, name, **params)
        assert pipe.sync(data)[0], name
        assert [d['id'] for d in _docs(pipe)] == [d['id'] for d in _docs(expected)]
        assert [d['val'] for d in _docs(pipe)] == [d['val'] for d in _docs(expected)]
    assert native_calls.handled >= 3
    pipe = _fresh_pipe(instance, 'bad_type', **params)
    with pytest.raises(TypeError, match='list of dictionaries'):
        pipe.sync(object())
    with pytest.raises(TypeError, match='list of dictionaries'):
        pipe.sync(iter([object()]))


def test_in_memory_sqlite_is_one_database_for_pipes_and_sqlalchemy(native_calls):
    """
    `Pipe` methods (the Rust pool) and `read()` / `exec()` / `.engine` (SQLAlchemy) open one
    database, so they see the same tables (W15L-007 is gone).
    """
    pytest.importorskip('sqlalchemy')
    from meerschaum.utils.sql import sql_item_name
    conn = mrsm.get_connector(
        'sql', 'w15l_shared_' + uuid.uuid4().hex[:6], flavor='sqlite', database=':memory:',
    )
    assert conn.URI.startswith('sqlite:///') and 'mrsm_mem_' in conn.URI
    pipe = mrsm.Pipe('w13h', 'w15l_shared', instance=conn, columns=COLUMNS)
    assert pipe.sync(_rows(4))[0]
    assert native_calls.handled == 1, native_calls.declined
    table = sql_item_name(pipe.target, 'sqlite', None)
    assert int(conn.value(f'SELECT COUNT(*) FROM {table}')) == 4
    ### A table SQLAlchemy creates is visible to the Pipe methods' pool.
    conn.exec('CREATE TABLE w15l_made_by_sqlalchemy (a INTEGER)')
    conn.exec('INSERT INTO w15l_made_by_sqlalchemy VALUES (7)')
    other = mrsm.Pipe(
        'w13h', 'w15l_shared_other', instance=conn, columns=COLUMNS,
        parameters={'target': 'w15l_made_by_sqlalchemy'},
    )
    assert other.get_rowcount() == 1
    assert _docs(pipe) and len(_docs(pipe)) == 4


def test_postgres_numeric_special_values_read_back_like_v4(tmp_path):
    """`NaN` / `Infinity` / `-Infinity` read from a `NUMERIC` column are `Decimal` objects."""
    instance = _numeric_instance('postgis', tmp_path)
    instance.exec('CREATE SCHEMA IF NOT EXISTS "w15l_num"')
    def outcome():
        pipe = mrsm.Pipe(
            'w15l', 'numeric_special', instance=instance,
            columns=COLUMNS, dtypes={'n': 'numeric'},
        )
        pipe.delete()
        pipe = mrsm.Pipe(
            'w15l', 'numeric_special', instance=instance,
            columns=COLUMNS, dtypes={'n': 'numeric'},
        )
        assert pipe.sync([{'dt': START, 'id': 0, 'n': Decimal('1.5')}])[0]
        from meerschaum.utils.sql import sql_item_name
        table = sql_item_name(pipe.target, 'postgis', 'w15l_num')
        specials = ("'NaN'::numeric", "'Infinity'::numeric", "'-Infinity'::numeric", 'NULL')
        for i, special in enumerate(specials, start=1):
            instance.exec(
                f"INSERT INTO {table} (dt, id, n) VALUES "
                f"('2026-01-01 0{i}:00:00+00', {i}, {special})"
            )
        pipe._invalidate_cache(hard=True)
        ### The instance's documents (`Pipe.get_data(as_docs=True)` adds nothing to them).
        result = [
            repr(d['n'])
            for d in sorted(pipe.instance_connector.get_pipe_docs(pipe), key=lambda d: d['id'])
        ]
        pipe.delete()
        return result

    expected = oracle.expected()
    actual = outcome()
    assert actual == expected
    assert actual[1:] == [
        "Decimal('NaN')", "Decimal('Infinity')", "Decimal('-Infinity')", 'None',
    ]


def test_the_accepted_data_helper_is_shared():
    from meerschaum.core.Pipe._sync import (
        ACCEPTED_DATA_TYPES, is_accepted_data, unsupported_data_error,
    )
    for value in ([], (), {}, 'a:1', iter([{'a': 1}])):
        assert is_accepted_data(value)
    assert not is_accepted_data(object())
    error = unsupported_data_error(object(), 'enforce dtypes')
    assert isinstance(error, TypeError)
    assert ACCEPTED_DATA_TYPES in str(error) and 'enforce dtypes' in str(error)


@pytest.mark.parametrize('flavor', ['sqlite', 'postgis', 'mariadb', 'mysql'])
def test_nan_and_infinity_numerics_are_stored_like_v4(flavor, tmp_path, native_calls):
    """
    v4.1.4 stores `Decimal('NaN')` (and `None`) as SQL NULL and `Decimal('±Infinity')` as the
    value where the column can hold it (PostgreSQL `NUMERIC`, SQLite text); the MySQL family
    cannot store an infinity and the sync fails in both implementations (PARITY.md, W15L-012).
    """
    pytest.importorskip('sqlalchemy')
    from meerschaum.utils.sql import sql_item_name
    instance = _numeric_instance(flavor, tmp_path)
    if flavor == 'postgis':
        instance.exec('CREATE SCHEMA IF NOT EXISTS "w15l_num"')
    specials = [Decimal('NaN'), None]
    if flavor in ('sqlite', 'postgis'):
        specials += [Decimal('Infinity'), Decimal('-Infinity')]
    docs = [{'dt': START, 'id': 0, 'n': Decimal('1.5')}] + [
        {'dt': START + timedelta(hours=i), 'id': i, 'n': value}
        for i, value in enumerate(specials, start=1)
    ]
    def outcome():
        make = lambda: mrsm.Pipe(  # noqa: E731
            'w15l', 'nan_inf', flavor, instance=instance,
            columns=COLUMNS, dtypes={'n': 'numeric'},
        )
        pipe = make()
        pipe.delete()
        pipe = make()
        assert pipe.sync(copy.deepcopy(docs))[0]
        table = sql_item_name(pipe.target, instance.flavor, instance.get_pipe_schema(pipe))
        cast = 'CHAR' if flavor in ('mariadb', 'mysql') else 'TEXT'
        rows = instance.read(
            f'SELECT id, CAST(n AS {cast}) AS txt, '
            f'CASE WHEN n IS NULL THEN 1 ELSE 0 END AS nullflag FROM {table} ORDER BY id'
        )
        result = (
            [(r['id'], r['txt'] if r['txt'] == r['txt'] else None, r['nullflag'])
             for r in rows.to_dict(orient='records')],
            [repr(d.get('n')) for d in pipe.instance_connector.get_pipe_docs(pipe)],
        )
        pipe.delete()
        return result

    expected = oracle.expected()
    actual = outcome()
    assert actual == expected
    stored = actual[0]
    assert stored[1][2] == 1 and stored[2][2] == 1  # NaN and None are NULL
    if flavor in ('sqlite', 'postgis'):
        assert [row[1] for row in stored[3:]] == ['Infinity', '-Infinity']


def test_a_forked_child_does_not_delete_the_memory_database(tmp_path):
    """The `sql:memory` file is removed by the process that created it only."""
    code = '''
import os, sys
import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'w15l_fork', flavor='sqlite', database=':memory:')
path = conn._shared_memory_database()
conn.exec('CREATE TABLE forked (a INTEGER)')
pid = os.fork()
if pid == 0:
    sys.exit(0)   # a normal exit runs the child's finalizers
os.waitpid(pid, 0)
assert os.path.exists(path), 'the forked child deleted the parent database'
assert int(conn.value('SELECT COUNT(*) FROM sqlite_master')) == 1
print('FORK_OK', path)
'''
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': os.environ['MRSM_ROOT_DIR'],
        'MRSM_PLUGINS_DIR': str(tmp_path),
        'PYTHONPATH': os.getcwd(),
    })
    done = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=180,
    )
    assert 'FORK_OK' in done.stdout, done.stdout + done.stderr
    path = done.stdout.split('FORK_OK', 1)[1].strip()
    assert not os.path.exists(path), 'the creating process left its database behind'


def test_a_fresh_root_plugin_sync_with_debug_does_not_deadlock(tmp_path):
    """
    The first plugin sync of a root builds the plugin manifest with a Python worker whose
    messages (debug output) reach the extension's `tracing` sink, which takes the interpreter
    lock on a runtime thread: the façade must not wait for the runtime while holding the lock
    (a fresh root used to hang forever, with a defunct worker).
    """
    code = '''
import os
import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'w15l_fresh', flavor='sqlite', database=os.environ['W15L_DB'])
pipe = mrsm.Pipe('plugin:w15l_plugsrc', 'fresh', instance=conn)
result = pipe.sync(debug=True)
assert result[0], result
print('FRESH_OK', result.inserted)
'''
    root = tmp_path / 'root'
    root.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': os.path.abspath('tests/plugins'),
        'W15L_DB': str(tmp_path / 'fresh.db'),
        'PYTHONPATH': os.getcwd(),
    })
    done = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=240,
    )
    assert 'FRESH_OK' in done.stdout, done.stdout[-1500:] + done.stderr[-1500:]
