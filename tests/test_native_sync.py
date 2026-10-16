"""
Wave 13 (task H): `Pipe.sync()` of explicit data routed to Rust under `MRSM_CORE=rust`
(`_core.sync_pipe`, the engine's ordinary one-pipe flow).

Each scenario runs the same sequence of syncs in `python` mode and in `rust` mode in one process
(the gate is re-read with `set_mode`), on pipes with the same keys (dropped in between), and
compares the `SuccessTuple`s (messages included), the rows `get_data(as_docs=True)` returns, the
columns' database types, the registered `dtypes` / `columns` after each sync, and that the
caller's data was not modified. The rust runs also assert that `_core.sync_pipe` handled the
call (so a silent fall-through to the Python body cannot pass).

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
from meerschaum._core import _gate

try:
    from meerschaum import _core
    HAS_CORE = _core.load() is not None
except Exception:
    HAS_CORE = False

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")

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


@pytest.fixture(autouse=True)
def restore_mode():
    original = (_gate._MODE, _gate._FEATURES)
    yield
    _gate._MODE, _gate._FEATURES = original


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


def _run(mode, instance, name, params, steps, native_calls=None):
    """
    Run `steps` (a list of `(data, sync_kwargs)`) on a fresh pipe in `mode`.
    Return the `(tuple, snapshot)` after each step.
    """
    _gate.set_mode(mode)
    pipe = _fresh_pipe(instance, name, **copy.deepcopy(params))
    observed = []
    for data, kwargs in steps:
        data_before = copy.deepcopy(data)
        result = pipe.sync(copy.deepcopy(data), **kwargs)
        passed = copy.deepcopy(data)
        assert passed == data_before
        observed.append((tuple(result), _snapshot(pipe)))
    return observed


def _compare(instance, name, params, steps, native_calls, same_messages=True):
    expected = _run('python', instance, name, params, steps)
    handled_before = native_calls.handled
    actual = _run('rust', instance, name, params, steps)
    assert native_calls.handled - handled_before == len(steps), native_calls.declined
    assert len(actual) == len(expected)
    for step, ((exp_tuple, exp_state), (act_tuple, act_state)) in enumerate(zip(expected, actual)):
        assert act_tuple[0] == exp_tuple[0], f"step {step}: {act_tuple} vs {exp_tuple}"
        if same_messages:
            assert act_tuple[1] == exp_tuple[1], f"step {step}"
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
    expected = _run('python', instance, 'autotime', params, [(rows, {})])
    actual = _run('rust', instance, 'autotime', params, [(rows, {})])
    assert native_calls.handled == 1
    assert actual[0][0] == expected[0][0]
    for state in (expected[0][1], actual[0][1]):
        assert all(doc.pop('ts', None) is not None for doc in state['docs'])
    assert actual[0][1] == expected[0][1]


def test_unequal_columns_fail_like_pandas(instance, native_calls):
    params = {'columns': COLUMNS}
    data = {'dt': [START, START + timedelta(hours=1)], 'id': [1]}
    _gate.set_mode('python')
    expected = tuple(_fresh_pipe(instance, 'unequal', **params).sync(copy.deepcopy(data)))
    _gate.set_mode('rust')
    before = native_calls.handled
    actual = tuple(_fresh_pipe(instance, 'unequal', **params).sync(copy.deepcopy(data)))
    assert actual == expected
    assert actual[0] is False
    ### The data pandas rejects is answered by the binding (it is not a decline).
    assert native_calls.handled == before + 1 and not native_calls.declined


def test_the_native_result_carries_the_sync_fields(instance, native_calls):
    _gate.set_mode('rust')
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


def test_python_mode_returns_the_same_class_with_empty_fields(instance):
    _gate.set_mode('python')
    pipe = _fresh_pipe(instance, 'py_fields', columns=COLUMNS)
    result = pipe.sync(_rows(2))
    assert isinstance(result, tuple)
    assert type(result).__name__ == 'SyncResult'
    assert result.success is True
    assert result.inserted is None and result.duration is None
    success, message = result
    assert success is True and isinstance(message, str)


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
    _gate.set_mode('rust')
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
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'patched', columns={'primary': 'pk', 'datetime': 'dt'})
    assert pipe.sync([{'dt': START, 'v': 1}])[0] is True
    assert pipe.parameters.get('autoincrement') is True
    ### Unsaved in-memory edits survive the sync (v4 persists them with the `autoincrement`
    ### edit as well: `update_parameters(..., persist=True)` writes the whole dictionary).
    pipe.parameters['unsaved'] = {'keep': 'me'}
    assert pipe.sync([{'dt': START + timedelta(hours=1), 'v': 2}])[0] is True
    assert pipe.parameters['unsaved'] == {'keep': 'me'}


@pytest.mark.parametrize(
    'kwargs, reason',
    [
        ({'force': True}, 'force'),
        ({'blocking': False}, 'non-blocking'),
        ({'workers': 2}, 'workers'),
        ({'debug': True}, 'debug'),
        ({'callback': print}, 'callbacks'),
        ({'plugin_flag': 1}, 'keyword'),
    ],
)
def test_declined_paths_name_their_reason(instance, kwargs, reason):
    from meerschaum.core.Pipe._sync import _rust_sync_eligible
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'declines', columns=COLUMNS)
    kwargs = dict(kwargs)
    explicit = {
        name: kwargs.pop(name)
        for name in ('force', 'blocking', 'workers', 'debug', 'callback')
        if name in kwargs
    }
    connector, message = _rust_sync_eligible(pipe, _rows(1), kw=kwargs, **explicit)
    assert connector is None
    assert reason in message


def _outcomes(mode, instance, name, params, steps):
    """The `(tuple | exception)` of each step and the final rows, for either mode."""
    _gate.set_mode(mode)
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
    _gate.set_mode('rust')
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
    expected = _outcomes('python', instance, 'no_enforce', params, steps)
    actual = _outcomes('rust', instance, 'no_enforce', params, steps)
    assert actual == expected


@pytest.mark.parametrize('columns, key', NO_ENFORCE_CASES, ids=NO_ENFORCE_IDS)
def test_syncs_with_enforce_dtypes_false_match_v4(instance, columns, key):
    """`Pipe.sync(enforce_dtypes=False)` on a pipe that does enforce (the first sync does)."""
    params = {'columns': columns}
    first, *rest = _no_enforce_steps(key)
    steps = [first] + [(data, {'enforce_dtypes': False}) for data, _ in rest]
    expected = _outcomes('python', instance, 'no_enforce_kw', params, steps)
    actual = _outcomes('rust', instance, 'no_enforce_kw', params, steps)
    assert actual == expected


def test_a_naive_axis_raises_the_pandas_merge_error_in_both_modes(instance):
    columns, key = NO_ENFORCE_CASES[3]
    params = {'columns': columns, 'enforce': False}
    steps = _no_enforce_steps(key)
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _fresh_pipe(instance, 'no_enforce_naive_error', **copy.deepcopy(params))
        assert pipe.sync(copy.deepcopy(steps[0][0]))[0] is True
        with pytest.raises(ValueError, match=r"merge on datetime64\[us\] and datetime64\[us, UTC\]"):
            pipe.sync(copy.deepcopy(steps[1][0]))


def test_documents_without_any_column_raise_like_v4(instance, native_calls):
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _fresh_pipe(instance, 'no_columns')
        with pytest.raises(ValueError, match="Expecting columns for table"):
            pipe.sync([{}, {}])


def test_integers_beyond_64_bits_sync_as_numeric_only_natively(instance, native_calls):
    """PARITY H8: v4 cannot build the DataFrame (`DataFrame is None`); the native path can."""
    _gate.set_mode('python')
    pipe = _fresh_pipe(instance, 'bigint')
    assert pipe.sync([{'a': 2 ** 70}])[0] is False
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'bigint')
    assert pipe.sync([{'a': 2 ** 70}])[0] is True
    assert pipe.get_parameters(refresh=True)['dtypes']['a'] == 'numeric'


def test_a_static_pipe_refuses_new_columns_in_both_modes(instance, native_calls):
    params = {'columns': {'datetime': 'dt'}, 'static': True}
    steps = [
        ([{'dt': START, 'v': 1}], {}),
        ([{'dt': START + timedelta(days=1), 'v': 2, 'w': 'new'}], {}),
    ]
    expected = _run('python', instance, 'static_new', params, steps[:1])
    actual = _run('rust', instance, 'static_new', params, steps[:1])
    assert actual[0][0] == expected[0][0]
    rows = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _fresh_pipe(instance, 'static_new', **params)
        assert pipe.sync(steps[0][0])[0] is True
        ### Only the failure and the unchanged rows are shared: v4's message is a traceback text.
        assert pipe.sync(steps[1][0])[0] is False
        rows[mode] = _docs(pipe)
    assert rows['python'] == rows['rust']


def test_other_declines(instance):
    from meerschaum.core.Pipe._sync import InferFetch, _rust_sync_eligible
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'declines2', columns=COLUMNS)
    connector, _ = _rust_sync_eligible(pipe, _rows(1))
    assert connector is not None
    assert _rust_sync_eligible(pipe, InferFetch)[0] is None
    assert _rust_sync_eligible(pipe, None)[0] is None

    referenced = _fresh_pipe(instance, 'declines3', reference='w13h:declines2')
    assert 'reference' in _rust_sync_eligible(referenced, _rows(1))[1]

    templated = _fresh_pipe(
        instance, 'declines4', columns=COLUMNS, parameters={'x': "{{ self.parameters['columns'] }}"},
    )
    assert 'templates' in _rust_sync_eligible(templated, _rows(1))[1]

    temporary = mrsm.Pipe('w13h', 'declines5', instance=instance, temporary=True)
    assert 'temporary' in _rust_sync_eligible(temporary, _rows(1))[1]


def test_unsupported_data_shapes_keep_the_python_path(instance, native_calls):
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'shapes', columns={'id': 'id'})
    csv = 'id,v\n1,2\n'
    assert pipe.sync(csv)[0] is True
    assert native_calls.declined and 'str' in native_calls.declined[-1]
    assert native_calls.handled == 0


def test_python_mode_never_calls_the_binding(instance, native_calls):
    _gate.set_mode('python')
    pipe = _fresh_pipe(instance, 'python_mode', columns=COLUMNS)
    assert pipe.sync(_rows(2))[0] is True
    assert native_calls.handled == 0 and native_calls.declined == []


def test_pandas_and_polars_frames(instance, native_calls):
    pd = pytest.importorskip('pandas')
    params = {'columns': COLUMNS, 'dtypes': {'val': 'float'}}
    frame = pd.DataFrame(_rows(5))
    frame2 = pd.DataFrame(_rows(5, offset=3))
    _gate.set_mode('python')
    expected = []
    pipe = _fresh_pipe(instance, 'pandas_frames', **params)
    for df in (frame, frame2):
        expected.append((tuple(pipe.sync(df.copy())), _docs(pipe)))
    _gate.set_mode('rust')
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
    _gate.set_mode('python')
    pipe = _fresh_pipe(instance, 'polars_frames', **params)
    exp = (tuple(pipe.sync(pl.DataFrame(_rows(5)))), _docs(pipe))
    _gate.set_mode('rust')
    pipe = _fresh_pipe(instance, 'polars_frames', **params)
    got = (tuple(pipe.sync(pl.DataFrame(_rows(5)))), _docs(pipe))
    assert got == exp


def test_construct_register_sync_and_read_docs_need_no_sqlalchemy_or_dataframes(tmp_path):
    """
    A fresh interpreter in `rust` mode constructs the pipe, registers it, syncs documents and
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
        'MRSM_CORE': 'rust',
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
