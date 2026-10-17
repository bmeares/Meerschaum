"""
Wave 15 (task L2): the `Pipe` methods that wrap an instance connector run in Rust under
`MRSM_CORE=rust` (`_core.pipe_get_data`, `pipe_get_backtrack_data`, `pipe_get_sync_time`,
`pipe_get_rowcount`, `pipe_infer_dtypes`, `pipe_enforce_dtypes`, `pipe_filter_existing`,
`pipe_deduplicate`, `pipe_fetch`, `pipe_get_backtrack_interval`).

Each scenario runs the same call in `python` mode (the v4 bodies, the oracle) and in `rust` mode in
one process (`_gate.set_mode`), on the same pipe, and compares the results: the rows, the column
order and the pandas dtype of every column. The `rust` calls assert that the binding handled the
call (a silent fall-through to the Python body cannot pass).

Instances: SQLite (always), PostgreSQL (`MRSM_TEST_FLAVORS` has `postgis`, schema `w15l2`), a custom
`@make_connector` instance (`w15lmem`, driven by the Python worker) and Valkey (`MRSM_TEST_FLAVORS`
has `valkey`). The Python `valkey` package is not installed, so a Valkey pipe has no `python` mode:
it is compared with the same data on SQLite.
"""

import copy
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate

try:
    from meerschaum import _core
    HAS_CORE = _core.load() is not None
except Exception:
    HAS_CORE = False

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")

SCHEMA = 'w15l2'
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
COLUMNS = {'datetime': 'dt', 'id': 'id'}
DTYPES = {'val': 'float', 'name': 'str'}


def _flavors():
    selected = [
        f.strip()
        for f in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')
        if f.strip()
    ]
    kinds = ['sqlite', 'custom']
    if 'postgis' in selected:
        kinds.append('postgis')
    if 'valkey' in selected:
        kinds.append('valkey')
    return kinds


def _rows(n, offset=0):
    return [
        {
            'dt': START + timedelta(hours=i),
            'id': i % 3,
            'val': float(i) / 2,
            'name': f'n{i}',
        }
        for i in range(offset, offset + n)
    ]


@pytest.fixture(autouse=True)
def restore_mode():
    original = (_gate._MODE, _gate._FEATURES)
    yield
    _gate._MODE, _gate._FEATURES = original


@pytest.fixture(scope='module')
def postgis_instance():
    conn = mrsm.get_connector(
        'sql', 'w15l2_postgis',
        flavor='postgis', username='test', password='test1234', database='testdb',
        port=5449, host='localhost', schema=SCHEMA,
    )
    conn.exec(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    conn.exec(f'CREATE SCHEMA "{SCHEMA}"')
    from meerschaum.connectors.sql.tables import get_tables
    get_tables(mrsm_instance=conn, create=True, refresh=True)
    yield conn
    conn.exec(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _resp(command, port=6399):
    import socket
    parts = [part.encode() for part in command]
    payload = f'*{len(parts)}\r\n'.encode() + b''.join(
        f'${len(part)}\r\n'.encode() + part + b'\r\n' for part in parts
    )
    with socket.create_connection(('localhost', port), timeout=10) as sock:
        sock.sendall(payload)
        return sock.recv(1 << 20)


def _valkey_cleanup(pattern):
    import socket
    reply = _resp(['KEYS', pattern])
    keys = [
        line.decode() for line in reply.split(b'\r\n')[1:]
        if line and not line.startswith(b'$') and not line.startswith(b'*')
    ]
    if keys:
        _resp(['DEL', *keys])


def _plugin_venv(name):
    """A plugin's venv: a fresh root has none, and the plugin does not import without it."""
    from meerschaum.utils.venv import init_venv
    from meerschaum.plugins import import_plugins
    assert init_venv(name), f"cannot create the venv of '{name}'"
    ### The plugins import once per process: one that failed for want of its venv is retried.
    import_plugins(name, warn=False)


@pytest.fixture(scope='module', autouse=True)
def plugin_venvs():
    for name in ('w15l_custom', 'w15l2_custom'):
        _plugin_venv(name)


@pytest.fixture(params=_flavors())
def instance(request, tmp_path):
    kind = request.param
    request.node.kind = kind
    if kind == 'sqlite':
        yield mrsm.get_connector(
            'sql', 'w15l2_' + uuid.uuid4().hex[:8],
            flavor='sqlite', database=str(tmp_path / 'w15l2.db'),
        )
    elif kind == 'postgis':
        yield request.getfixturevalue('postgis_instance')
    elif kind == 'custom':
        _plugin_venv('w15l_custom')
        conn = mrsm.get_connector('w15lmem', 'w15l2_' + uuid.uuid4().hex[:6])
        assert conn is not None, "the w15l_custom plugin did not import"
        yield conn
    else:
        yield mrsm.get_connector('valkey', 'w15l2_valkey', host='localhost', port=6399)


@pytest.fixture
def kind(request, instance):
    return request.node.kind


@pytest.fixture
def reference_instance(tmp_path):
    return mrsm.get_connector(
        'sql', 'w15l2_ref_' + uuid.uuid4().hex[:8],
        flavor='sqlite', database=str(tmp_path / 'w15l2_ref.db'),
    )


class Counter:
    """Counts the calls each `_core.pipe_*` binding handled (and the ones it declined)."""

    def __init__(self):
        self.handled = {}
        self.declined = {}


@pytest.fixture
def native_calls(monkeypatch):
    counter = Counter()
    for name in (
        'pipe_get_data', 'pipe_get_data_chunks', 'pipe_get_backtrack_data',
        'pipe_get_sync_time', 'pipe_get_rowcount', 'pipe_infer_dtypes', 'pipe_enforce_dtypes',
        'pipe_filter_existing', 'pipe_deduplicate', 'pipe_fetch',
        'pipe_get_backtrack_interval',
    ):
        original = getattr(_core, name)

        def counting(*args, _original=original, _name=name, **kwargs):
            out = _original(*args, **kwargs)
            bucket = counter.declined if out is NotImplemented else counter.handled
            bucket[_name] = bucket.get(_name, 0) + 1
            return out

        monkeypatch.setattr(_core, name, counting)
    return counter


def _is_null(value):
    try:
        import pandas as pd
        if value is pd.NaT or value is pd.NA:
            return True
    except Exception:
        pass
    return value is None or (isinstance(value, float) and value != value)


def _norm(value):
    if _is_null(value):
        return None
    if hasattr(value, 'wkt'):
        return value.wkt
    if hasattr(value, 'is_nan') and hasattr(value, 'as_tuple') and value.is_nan():
        return 'Decimal(NaN)'
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, 'isoformat') and hasattr(value, 'to_pydatetime'):
        return value.to_pydatetime().isoformat()
    if isinstance(value, dict):
        return {k: _norm(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_norm(v) for v in value]
    return value


def snapshot(obj):
    """A comparable view: rows, column order and the dtype of every column of a frame."""
    if obj is None:
        return None
    if isinstance(obj, tuple):
        return tuple(snapshot(o) for o in obj)
    if isinstance(obj, list):
        return [snapshot(o) for o in obj]
    if isinstance(obj, dict):
        return {k: snapshot(v) for k, v in obj.items()}
    if hasattr(obj, 'dtypes') and hasattr(obj, 'to_dict') and hasattr(obj, 'columns'):
        columns = list(obj.columns)
        if hasattr(obj, 'to_pandas'):
            return {
                'cols': columns,
                'dtypes': [str(t) for t in obj.dtypes],
                'rows': [_norm(row) for row in obj.to_dicts()],
            }
        return {
            'cols': columns,
            'dtypes': [str(t) for t in obj.dtypes],
            'rows': [_norm(row) for row in obj.to_dict('records')],
        }
    if hasattr(obj, 'dtypes') and hasattr(obj, 'to_dicts'):
        return {
            'cols': list(obj.columns),
            'dtypes': [str(t) for t in obj.dtypes],
            'rows': [_norm(row) for row in obj.to_dicts()],
        }
    if hasattr(obj, '__next__'):
        return [snapshot(o) for o in obj]
    return _norm(obj)


def families(snap):
    """A snapshot with the pandas backend of every dtype reduced to its family."""
    if isinstance(snap, dict) and 'dtypes' in snap:
        def family(dtype):
            name = dtype.lower().replace('[pyarrow]', '').replace('large_', '')
            if name.startswith('timestamp') or name.startswith('datetime'):
                return 'datetime'
            if name.startswith('int') or name.startswith('uint'):
                return 'int'
            if name.startswith('float') or name == 'double':
                return 'float'
            if name in ('string', 'object', 'str'):
                return 'str'
            return name
        return {**snap, 'dtypes': [family(t) for t in snap['dtypes']]}
    return snap


def by_column(snap):
    """
    A snapshot with the columns in alphabetical order (Valkey stores documents as hashes and
    returns their columns in the hash's order; the sets of columns, rows and dtypes are equal).
    """
    if isinstance(snap, list):
        return [by_column(s) for s in snap]
    if isinstance(snap, dict) and 'cols' in snap:
        order = sorted(range(len(snap['cols'])), key=lambda i: snap['cols'][i])
        return {
            'cols': [snap['cols'][i] for i in order],
            'dtypes': [snap['dtypes'][i] for i in order],
            'rows': snap['rows'],
        }
    return snap


def _pipe(instance, label=None, parameters=None, columns=None, **kwargs):
    params = dict(parameters or {})
    dtypes = {**DTYPES, **params.pop('dtypes', {})}
    return mrsm.Pipe(
        'w15l2', label or 'x' + uuid.uuid4().hex[:8], instance=instance,
        columns=columns or COLUMNS, dtypes=dtypes, parameters=params, **kwargs,
    )


def _loaded(instance, n=12, label=None, **kwargs):
    """A pipe with `n` rows, synced in `rust` mode."""
    _gate.set_mode('rust')
    pipe = _pipe(instance, label, **kwargs)
    pipe.delete()
    pipe = _pipe(instance, pipe.metric_key, **kwargs)
    success, message = pipe.sync(_rows(n))
    assert success, message
    return pipe


def _modes(kind):
    return ('rust',) if kind == 'valkey' else ('python', 'rust')


def _compare(
    kind, instance, reference_instance, op, native_calls, binding, n=12, normalize=None, **kwargs
):
    """
    `op(pipe)` in `python` mode (on the pipe itself, or on SQLite for Valkey) and in `rust` mode.
    Returns the rust snapshot after asserting the equality and that `binding` handled a call.
    """
    pipe = _loaded(instance, n=n, **kwargs)
    try:
        if kind == 'valkey':
            ref = _loaded(reference_instance, n=n, **kwargs)
            _gate.set_mode('python')
            expected = snapshot(op(ref))
        else:
            _gate.set_mode('python')
            expected = snapshot(op(pipe))
        before = native_calls.handled.get(binding, 0)
        _gate.set_mode('rust')
        actual = snapshot(op(pipe))
        assert native_calls.handled.get(binding, 0) > before, native_calls.declined
        if kind == 'valkey':
            actual, expected = by_column(actual), by_column(expected)
        if normalize is not None:
            actual, expected = normalize(actual), normalize(expected)
        assert actual == expected
        return actual
    finally:
        _gate.set_mode('rust')
        pipe.delete()


# ---------------------------------------------------------------------------------------------
# get_data

GET_DATA_CASES = {
    'all': {},
    'select': {'select_columns': ['dt', 'val']},
    'select_string': {'select_columns': 'val'},
    'select_star': {'select_columns': '*'},
    'omit': {'omit_columns': ['name']},
    'select_and_omit': {'select_columns': ['dt', 'id', 'val'], 'omit_columns': ['val']},
    'begin': {'begin': START + timedelta(hours=4)},
    'end': {'end': START + timedelta(hours=7)},
    'begin_end': {'begin': START + timedelta(hours=2), 'end': START + timedelta(hours=9)},
    'params': {'params': {'id': 1}},
    'params_list': {'params': {'id': [0, 2]}},
    'params_negated': {'params': {'id': '_1'}},
    'limit': {'limit': 5},
    'desc': {'order': 'desc'},
    'desc_limit': {'order': 'desc', 'limit': 3},
    'missing_column': {'select_columns': ['dt', 'nope']},
    'add_missing_columns': {'add_missing_columns': True, 'select_columns': ['dt', 'id']},
    'docs': {'as_docs': True},
    'docs_limit': {'as_docs': True, 'limit': 4, 'order': 'desc'},
    'docs_select': {'as_docs': True, 'select_columns': ['dt', 'val']},
}


@pytest.mark.parametrize('case', sorted(GET_DATA_CASES))
def test_get_data(case, kind, instance, reference_instance, native_calls):
    kwargs = GET_DATA_CASES[case]
    if kind == 'valkey' and (
        'end' in kwargs
        or kwargs.get('as_docs')
    ):
        pytest.skip(
            "Valkey's `end` is inclusive (ZRANGEBYSCORE) and its documents are raw, unsorted "
            "and unlimited, as in v4: not comparable with SQLite"
        )
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_data(**copy.deepcopy(kwargs)),
        native_calls, 'pipe_get_data',
    )


def test_get_data_without_an_order_keeps_every_row(
    kind, instance, reference_instance, native_calls,
):
    if kind == 'valkey':
        pytest.skip("Valkey's documents are raw: see `test_get_data`")
    """`order=None` does not sort: the same rows come back (in the instance's order)."""
    _compare(
        kind, instance, reference_instance,
        lambda pipe: sorted(pipe.get_data(order=None, as_docs=True), key=lambda d: d['dt']),
        native_calls, 'pipe_get_data',
    )


def test_get_data_of_a_missing_pipe(kind, instance, native_calls):
    _gate.set_mode('rust')
    pipe = _pipe(instance)
    assert pipe.get_data() is None
    assert pipe.get_data(as_docs=True) == []
    assert list(pipe.get_data(as_iterator=True)) == []
    assert native_calls.handled['pipe_get_data'] >= 2


def test_get_data_as_polars(kind, instance, reference_instance, native_calls):
    polars = pytest.importorskip('polars')
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_data(as_polars=True, select_columns=['dt', 'id', 'val']),
        native_calls, 'pipe_get_data',
    )


@pytest.mark.parametrize('case', ['small_chunks', 'one_chunk', 'bounded'])
def test_get_data_as_iterator(case, kind, instance, reference_instance, native_calls):
    if kind == 'valkey' and case == 'bounded':
        pytest.skip("Valkey's `end` is inclusive")
    kwargs = {
        'small_chunks': {'chunk_interval': timedelta(hours=5)},
        'one_chunk': {'chunk_interval': timedelta(days=30)},
        'bounded': {
            'chunk_interval': timedelta(hours=4),
            'begin': START + timedelta(hours=3),
            'end': START + timedelta(hours=10),
        },
    }[case]
    ### The iterator's chunks (the binding returns the iterator itself).
    pipe = _loaded(instance)
    try:
        if kind == 'valkey':
            ref = _loaded(reference_instance)
            _gate.set_mode('python')
            expected = by_column(snapshot(list(ref.get_data(as_iterator=True, **kwargs))))
        else:
            _gate.set_mode('python')
            expected = snapshot(list(pipe.get_data(as_iterator=True, **kwargs)))
        _gate.set_mode('rust')
        before = native_calls.handled.get('pipe_get_data_chunks', 0)
        actual = snapshot(list(pipe.get_data(as_iterator=True, **kwargs)))
        if kind == 'valkey':
            actual = by_column(actual)
        assert native_calls.handled.get('pipe_get_data_chunks', 0) > before
        assert actual == expected
        if case == 'small_chunks':
            assert len(actual) > 1
    finally:
        _gate.set_mode('rust')
        pipe.delete()


def test_get_data_as_docs_iterator(kind, instance, reference_instance, native_calls):
    pipe = _loaded(instance)
    try:
        _gate.set_mode('rust')
        chunks = list(pipe.get_data(as_iterator=True, as_docs=True, chunk_interval=timedelta(hours=5)))
        assert all(isinstance(chunk, list) for chunk in chunks)
        assert sum(len(chunk) for chunk in chunks) == 12
    finally:
        pipe.delete()


# ---------------------------------------------------------------------------------------------
# get_backtrack_data, get_sync_time, get_rowcount

@pytest.mark.parametrize('case', ['default', 'minutes', 'begin', 'limit', 'params'])
def test_get_backtrack_data(case, kind, instance, reference_instance, native_calls):
    kwargs = {
        'default': {},
        'minutes': {'backtrack_minutes': 180},
        'begin': {'begin': START + timedelta(hours=6), 'backtrack_minutes': 120},
        'limit': {'backtrack_minutes': 600, 'limit': 2},
        'params': {'backtrack_minutes': 600, 'params': {'id': 1}},
    }[case]
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_backtrack_data(**kwargs),
        native_calls, 'pipe_get_backtrack_data',
    )


@pytest.mark.parametrize(
    'kwargs',
    [
        {},
        {'newest': False},
        {'params': {'id': 2}},
        {'round_down': True},
        {'apply_backtrack_interval': True},
        {'newest': False, 'params': {'id': 1}},
    ],
    ids=lambda k: ','.join(f'{a}={b}' for a, b in k.items()) or 'default',
)
def test_get_sync_time(kwargs, kind, instance, reference_instance, native_calls):
    if kind == 'valkey' and 'params' in kwargs:
        pytest.skip("Valkey's `get_sync_time` ignores `params`, as in v4")
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_sync_time(**kwargs),
        native_calls, 'pipe_get_sync_time',
    )


def test_get_sync_time_without_a_datetime_axis(kind, instance, native_calls):
    _gate.set_mode('rust')
    pipe = _pipe(instance, columns={'id': 'id'})
    pipe.delete()
    pipe = _pipe(instance, pipe.metric_key, columns={'id': 'id'})
    assert pipe.sync(_rows(3))[0]
    try:
        assert pipe.get_sync_time() is None
    finally:
        pipe.delete()


@pytest.mark.parametrize(
    'kwargs',
    [
        {},
        {'begin': START + timedelta(hours=3)},
        {'end': START + timedelta(hours=5)},
        {'begin': START + timedelta(hours=2), 'end': START + timedelta(hours=8)},
        {'params': {'id': 0}},
    ],
    ids=lambda k: ','.join(k) or 'all',
)
def test_get_rowcount(kwargs, kind, instance, reference_instance, native_calls):
    if kind == 'valkey' and 'end' in kwargs:
        pytest.skip("Valkey's `end` is inclusive")
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_rowcount(**kwargs),
        native_calls, 'pipe_get_rowcount',
    )


def test_get_rowcount_of_a_missing_pipe(kind, instance, native_calls):
    _gate.set_mode('rust')
    assert _pipe(instance).get_rowcount() == 0


def test_get_backtrack_interval(kind, instance, native_calls):
    pipe = _pipe(instance, parameters={'fetch': {'backtrack_minutes': 90}})
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        outcomes[mode] = (
            pipe.get_backtrack_interval(),
            pipe.get_backtrack_interval(check_existing=False),
        )
    assert outcomes['rust'] == outcomes['python']
    assert outcomes['rust'][0] == timedelta(minutes=90)
    assert native_calls.handled['pipe_get_backtrack_interval'] >= 2


# ---------------------------------------------------------------------------------------------
# dtypes

def test_infer_dtypes(kind, instance, reference_instance, native_calls):
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.infer_dtypes(refresh=True),
        native_calls, 'pipe_infer_dtypes',
        normalize=lambda d: {c: families({'dtypes': [t]})['dtypes'][0] for c, t in d.items()},
    )


def test_infer_dtypes_of_a_missing_pipe(kind, instance, native_calls):
    _gate.set_mode('rust')
    assert _pipe(instance).infer_dtypes() == {}
    assert native_calls.handled['pipe_infer_dtypes'] == 1


def test_infer_dtypes_persists(kind, instance, native_calls):
    if kind in ('valkey', 'custom'):
        pytest.skip("these instances report no columns types to persist")
    _gate.set_mode('rust')
    pipe = _pipe(instance)
    pipe.delete()
    pipe = mrsm.Pipe('w15l2', pipe.metric_key, instance=instance, columns=COLUMNS)
    assert pipe.sync(_rows(3))[0]
    try:
        remote = pipe.infer_dtypes(persist=True)
        assert 'val' in remote
        assert 'val' in pipe.parameters['dtypes']
    finally:
        pipe.delete()


ENFORCE_INPUTS = {
    'docs': lambda: [{'dt': '2026-01-01 00:00:00', 'id': '1', 'val': '2.5', 'name': 5}],
    'columns': lambda: {'dt': ['2026-01-01', '2026-01-02'], 'id': [1, 2], 'val': [1, 2.5]},
    'tuple': lambda: ({'dt': datetime(2026, 1, 1), 'id': 3, 'val': 1, 'name': 'a'},),
    'json_text': lambda: '[{"dt": "2026-01-03T00:00:00Z", "id": 4, "val": "7", "name": "b"}]',
    'simple_lines': lambda: 'dt:2026-01-04,id:5,val:1.5\ndt:2026-01-05,id:6,val:2.5',
    'empty_docs': lambda: [],
}


@pytest.mark.parametrize('case', sorted(ENFORCE_INPUTS))
def test_enforce_dtypes_of_raw_data(case, kind, instance, reference_instance, native_calls):
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.enforce_dtypes(ENFORCE_INPUTS[case]()),
        native_calls, 'pipe_enforce_dtypes', normalize=families,
    )


def test_enforce_dtypes_of_frames(kind, instance, reference_instance, native_calls):
    pd = pytest.importorskip('pandas')

    def op(pipe):
        frame = pd.DataFrame(_rows(5))
        frame['val'] = frame['val'].astype(str)
        frame['name'] = frame['name'].astype(object)
        frame['extra'] = ['a', 'b', 'c', 'd', 'e']
        return pipe.enforce_dtypes(frame)

    _compare(kind, instance, reference_instance, op, native_calls, 'pipe_enforce_dtypes')


def test_enforce_dtypes_to_polars(kind, instance, reference_instance, native_calls):
    pytest.importorskip('polars')
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.enforce_dtypes(_rows(4), as_polars=True),
        native_calls, 'pipe_enforce_dtypes',
    )


def test_enforce_dtypes_with_explicit_dtypes(kind, instance, reference_instance, native_calls):
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.enforce_dtypes(_rows(3), dtypes={'id': 'str', 'val': 'int'}),
        native_calls, 'pipe_enforce_dtypes', normalize=families,
    )


def test_enforce_dtypes_of_nothing(kind, instance, native_calls):
    _gate.set_mode('rust')
    assert _pipe(instance).enforce_dtypes(None) is None


# ---------------------------------------------------------------------------------------------
# filter_existing

def _by_family(kind):
    """
    The string backends of the unseen and update rows follow pandas' merge against the existing
    rows' backend, which differs for an instance whose frames are Arrow-backed (PARITY, wave 15
    L2); every value, column and dtype family is compared.
    """
    if kind != 'custom':
        return None
    return lambda snap: tuple(families(s) for s in snap)


def _incoming(pipe):
    """Existing rows 3-8, changed rows 6-8, new rows 12-15."""
    rows = _rows(6, offset=3)
    for row in rows[3:]:
        row['val'] = row['val'] + 100.0
    return rows + _rows(4, offset=12)


@pytest.mark.parametrize('flags', [{}, {'include_unchanged_columns': True}, {'date_bound_only': True}])
def test_filter_existing(flags, kind, instance, reference_instance, native_calls):
    pd = pytest.importorskip('pandas')
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.filter_existing(pd.DataFrame(_incoming(pipe)), **flags),
        native_calls, 'pipe_filter_existing', normalize=_by_family(kind),
    )


def test_filter_existing_of_documents(kind, instance, reference_instance, native_calls):
    pytest.importorskip('pandas')
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.filter_existing(_incoming(pipe)),
        native_calls, 'pipe_filter_existing', normalize=_by_family(kind),
    )


def test_filter_existing_with_polars(kind, instance, reference_instance, native_calls):
    pl = pytest.importorskip('polars')
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.filter_existing(pl.DataFrame(_incoming(pipe))),
        native_calls, 'pipe_filter_existing',
    )


def test_filter_existing_of_a_missing_pipe(kind, instance, native_calls):
    pd = pytest.importorskip('pandas')
    _gate.set_mode('rust')
    pipe = _pipe(instance)
    unseen, update, delta = pipe.filter_existing(pd.DataFrame(_rows(3)))
    assert len(unseen) == 3 and len(update) == 0 and len(delta) == 3


def test_filter_existing_of_nothing(kind, instance, native_calls):
    _gate.set_mode('rust')
    unseen, update, delta = _pipe(instance).filter_existing(None)
    assert len(unseen) == len(update) == len(delta) == 0


# ---------------------------------------------------------------------------------------------
# deduplicate

DUPLICATED = [
    {'dt': '2023-01-01 00:00:00', 'id': 1},
    {'dt': '2023-01-01 00:00:00', 'id': 1},
    {'dt': '2023-01-02 00:00:00', 'id': 2},
    {'dt': '2023-01-02 00:00:00', 'id': 2},
    {'dt': '2023-01-02 00:00:00', 'id': 2},
    {'dt': '2023-01-03 00:00:00', 'id': 3},
    {'dt': '2023-01-03 00:00:00', 'id': 4},
]


def _duplicated_pipe(instance, kind):
    """A pipe whose table holds `DUPLICATED` (SQL: written around the sync; custom: appended)."""
    from meerschaum.utils.dataframe import parse_df_datetimes
    pipe = mrsm.Pipe('w15l2', 'x' + uuid.uuid4().hex[:8], instance=instance, columns=COLUMNS)
    df = parse_df_datetimes(DUPLICATED, strip_timezone=False)
    if kind == 'custom':
        assert pipe.register()[0]
        assert instance.sync_pipe(pipe, df)[0]
    else:
        instance.to_sql(df, pipe.target)
    return pipe


@pytest.mark.parametrize('use_instance_method', [True, False])
def test_deduplicate(use_instance_method, kind, instance, native_calls):
    """
    Duplicated rows are removed: by the SQL instance's own implementation, by the generic chunk
    loop (any instance, or `_use_instance_method=False`; the custom connector's `sync_pipe`
    appends every row it is given).
    """
    if kind == 'valkey':
        pytest.skip("Valkey keys rows by index, so it holds no duplicates")
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _duplicated_pipe(instance, kind)
        before = pipe.get_rowcount()
        success, message = pipe.deduplicate(_use_instance_method=use_instance_method)
        outcomes[mode] = (
            success,
            before,
            pipe.get_rowcount(),
            snapshot(pipe.get_data()),
        )
        pipe.delete()
    assert outcomes['rust'] == outcomes['python']
    assert outcomes['rust'][0] is True
    assert outcomes['rust'][2] == len({json.dumps(doc) for doc in DUPLICATED})
    assert native_calls.handled['pipe_deduplicate'] >= 1


def test_deduplicate_without_index_columns(kind, instance, native_calls):
    if kind == 'valkey':
        pytest.skip("Valkey keys rows by index")
    _gate.set_mode('rust')
    pipe = mrsm.Pipe('w15l2', 'x' + uuid.uuid4().hex[:8], instance=instance)
    success, message = pipe.deduplicate(_use_instance_method=False)
    assert not success


# ---------------------------------------------------------------------------------------------
# fetch

def _fetched(result):
    """Chunks of a fetch as `(columns, rows without the datetime axis)`: the Rust driver parses
    timestamps where SQLAlchemy over SQLite hands back text (PARITY, wave 15 L2)."""
    chunks = list(result) if hasattr(result, '__next__') else [result]
    out = []
    for chunk in chunks:
        if chunk is None:
            out.append(None)
            continue
        out.append((
            list(chunk.columns),
            [
                {k: _norm(v) for k, v in row.items() if k != 'dt'}
                for row in chunk.to_dict('records')
            ],
        ))
    return out


def test_fetch_from_a_sql_source(kind, instance, reference_instance, native_calls, tmp_path):
    """`Pipe.fetch()` reads the source's newest rows through the source's definition."""
    pytest.importorskip('pandas')
    if kind == 'valkey':
        pytest.skip("no `python` mode for Valkey; the fetch does not depend on the instance")
    source = reference_instance
    source_pipe = _loaded(source, n=8, label='fetch_source')
    try:
        definition = f'SELECT * FROM {source_pipe.target}'
        outcomes = {}
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)

            def make():
                return mrsm.Pipe(
                    str(source), 'w15l2_fetch', instance=instance, columns=COLUMNS,
                    dtypes=DTYPES, parameters={'fetch': {'definition': definition}},
                )

            make().delete()
            pipe = make()
            first = _fetched(pipe.fetch())
            if kind != 'valkey':
                assert pipe.sync()[0]
                second = _fetched(pipe.fetch())
                pipe.delete()
            else:
                second = None
            outcomes[mode] = (first, second)
        assert outcomes['rust'] == outcomes['python']
        assert native_calls.handled.get('pipe_fetch', 0) >= 1
    finally:
        _gate.set_mode('rust')
        source_pipe.delete()


def test_fetch_of_a_connector_without_fetch(kind, instance, native_calls):
    _gate.set_mode('rust')
    pipe = _pipe(instance)
    assert pipe.fetch() is None


# ---------------------------------------------------------------------------------------------
# Python-mode oracle is untouched

def test_python_mode_does_not_call_the_bindings(kind, instance, native_calls):
    if kind == 'valkey':
        pytest.skip("no `python` mode for Valkey")
    pipe = _loaded(instance, n=4)
    try:
        _gate.set_mode('python')
        pipe.get_data()
        pipe.get_sync_time()
        pipe.get_rowcount()
        assert not native_calls.handled and not native_calls.declined
    finally:
        _gate.set_mode('rust')
        pipe.delete()


def test_valkey_cleanup(kind):
    if kind != 'valkey':
        pytest.skip("Valkey only")
    _valkey_cleanup('w15l2*')
    _valkey_cleanup('mrsm_pipe:w15l2:*')


# ---------------------------------------------------------------------------------------------
# Wave 15 (L2) follow-ups: the connector's own backtrack hook, remote reads from plugin and
# Valkey sources, chunk hooks, the accepted input types, `workers`, exact `NUMERIC` specials.

@pytest.fixture(scope='module')
def w15l2_plugin():
    _plugin_venv('w15l2_custom')


def _back_instance(delay=0):
    attributes = {'delay': delay} if delay else {}
    conn = mrsm.get_connector('w15l2back', 'b' + uuid.uuid4().hex[:6], **attributes)
    assert conn is not None, "the w15l2_custom plugin did not import"
    return conn


def _calls(instance):
    import pathlib
    from meerschaum.config.paths import ROOT_DIR_PATH
    path = pathlib.Path(ROOT_DIR_PATH) / f'w15l2back_{instance.label}_calls.json'
    return json.loads(path.read_text()) if path.exists() else []


@pytest.mark.parametrize('kwargs', [{'backtrack_minutes': 180}, {}, {'limit': 2}])
def test_a_custom_connectors_own_get_backtrack_data_is_called(
    kwargs, w15l2_plugin, native_calls,
):
    """v4 calls the instance connector's `get_backtrack_data()` when it defines one."""
    instance = _back_instance()
    pipe = _loaded(instance)
    try:
        counts = {}
        results = {}
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)
            before = len(_calls(instance))
            results[mode] = snapshot(pipe.get_backtrack_data(**kwargs))
            counts[mode] = len(_calls(instance)) - before
        assert counts == {'python': 1, 'rust': 1}
        assert native_calls.handled['pipe_get_backtrack_data'] >= 1
        assert families(results['rust']) == families(results['python'])
    finally:
        _gate.set_mode('rust')
        pipe.delete()


def _src_pipe(instance, source='w15l2src:a', label=None):
    return mrsm.Pipe(
        source, label or 'x' + uuid.uuid4().hex[:8], instance=instance,
        columns=COLUMNS, dtypes=DTYPES,
    )


@pytest.mark.parametrize('newest', [True, False])
def test_remote_sync_time_and_rowcount_of_a_plugin_source(
    newest, w15l2_plugin, reference_instance, native_calls,
):
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _src_pipe(reference_instance, label='remote')
        outcomes[mode] = (
            _norm(pipe.get_sync_time(remote=True, newest=newest)),
            _norm(pipe.get_sync_time(newest=newest)),
            pipe.get_rowcount(remote=True),
            pipe.get_rowcount(),
        )
    assert outcomes['rust'] == outcomes['python']
    assert outcomes['rust'][2] == 12 and outcomes['rust'][3] == 0
    assert outcomes['rust'][0] is not None and outcomes['rust'][1] is None
    assert native_calls.handled['pipe_get_sync_time'] >= 2
    assert native_calls.handled['pipe_get_rowcount'] >= 2


def test_remote_reads_of_a_source_that_cannot_answer(
    w15l2_plugin, reference_instance, native_calls,
):
    """No `get_sync_time()` is `None` and a rowcount without a `remote` keyword is `0`."""
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _src_pipe(reference_instance, source='w15l2nosrc:a', label='noremote')
        outcomes[mode] = (pipe.get_sync_time(remote=True), pipe.get_rowcount(remote=True))
    assert outcomes['rust'] == outcomes['python'] == (None, 0)


def test_remote_sync_time_of_a_valkey_source(reference_instance, native_calls):
    if 'valkey' not in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(','):
        pytest.skip("set MRSM_TEST_FLAVORS to include valkey")
    _gate.set_mode('rust')
    valkey = mrsm.get_connector('valkey', 'w15l2_valkey', host='localhost', port=6399)
    label = 'rv' + uuid.uuid4().hex[:6]
    remote = mrsm.Pipe(str(valkey), label, instance=valkey, columns=COLUMNS, dtypes=DTYPES)
    assert remote.sync(_rows(5))[0]
    try:
        local = mrsm.Pipe(str(valkey), label, instance=reference_instance, columns=COLUMNS)
        assert local.get_sync_time(remote=True) == remote.get_sync_time()
        assert local.get_sync_time(remote=True) is not None
        ### Valkey's `get_pipe_rowcount()` takes no `remote`: v4 counts 0.
        assert local.get_rowcount(remote=True) == 0
    finally:
        remote.delete()


def _fetch_outcome(result):
    return [(bool(r[0]), str(r[1]).splitlines()[-1]) for r in result]


def test_fetch_sync_chunks_from_a_plugin_source(w15l2_plugin, reference_instance, native_calls):
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _src_pipe(reference_instance, label='chunks')
        pipe.delete()
        pipe = _src_pipe(reference_instance, label='chunks')
        results = pipe.fetch(sync_chunks=True)
        outcomes[mode] = (_fetch_outcome(results), pipe.get_rowcount())
        pipe.delete()
    assert outcomes['rust'] == outcomes['python']
    assert len(outcomes['rust'][0]) == 3 and outcomes['rust'][1] == 12
    assert native_calls.handled['pipe_fetch'] >= 1


def test_fetch_with_a_chunk_hook(w15l2_plugin, reference_instance, native_calls):
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        seen = []

        def hook(chunk, **kw):
            seen.append(len(chunk))
            return True, f'seen {len(chunk)}'

        pipe = _src_pipe(reference_instance, label='hook')
        outcomes[mode] = (_fetch_outcome(pipe.fetch(chunk_hook=hook)), seen)
    assert outcomes['rust'] == outcomes['python']
    assert outcomes['rust'][1] == [4, 4, 4]


def test_fetch_sync_chunks_from_a_sql_source_returns_the_chunks(
    reference_instance, tmp_path, native_calls,
):
    """`SQLConnector.fetch()` takes no `chunk_hook`: v4 hands the chunks back unsynced."""
    source = mrsm.get_connector(
        'sql', 'w15l2_src_' + uuid.uuid4().hex[:6],
        flavor='sqlite', database=str(tmp_path / 'src.db'),
    )
    source_pipe = _loaded(source, n=8, label='hooksource')
    definition = f'SELECT * FROM {source_pipe.target}'
    outcomes = {}
    try:
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)

            def make():
                return mrsm.Pipe(
                    str(source), 'w15l2_hooked', instance=reference_instance, columns=COLUMNS,
                    dtypes=DTYPES, parameters={'fetch': {'definition': definition}},
                )

            make().delete()
            pipe = make()
            outcomes[mode] = (_fetched(pipe.fetch(sync_chunks=True)), pipe.get_rowcount())
        assert outcomes['rust'] == outcomes['python']
        assert outcomes['rust'][1] == 0 and len(outcomes['rust'][0][0][1]) == 8
    finally:
        _gate.set_mode('rust')
        source_pipe.delete()


def test_unsupported_input_is_a_type_error_naming_the_accepted_types(
    kind, instance, native_calls,
):
    if kind == 'valkey':
        pytest.skip("same boundary on every instance")
    _gate.set_mode('rust')
    pipe = _pipe(instance)
    for call in (pipe.enforce_dtypes, pipe.filter_existing):
        with pytest.raises(TypeError, match='Accepted types'):
            call(object())


def test_arrow_tables_are_accepted(kind, instance, reference_instance, native_calls):
    pa = pytest.importorskip('pyarrow')
    pytest.importorskip('pandas')
    if kind == 'valkey':
        pytest.skip("same boundary on every instance")
    pipe = _loaded(instance)
    try:
        table = pa.Table.from_pylist(_incoming(pipe))
        unseen, update, delta = pipe.filter_existing(table)
        assert (len(unseen), len(update), len(delta)) == (4, 3, 7)
        assert len(pipe.enforce_dtypes(table)) == 10
    finally:
        pipe.delete()


def _max_inflight(instance):
    import pathlib
    from meerschaum.config.paths import ROOT_DIR_PATH
    path = pathlib.Path(ROOT_DIR_PATH) / f'w15l2back_{instance.label}_inflight.json'
    return json.loads(path.read_text())['max'] if path.exists() else 0


def _edge_docs(case):
    fmt = '%Y-%m-%d %H:%M:%S'
    if case == 'edges':
        ### One row on every chunk edge (6 h chunks), the first three twice.
        docs = [{'dt': (START + timedelta(hours=6 * i)).strftime(fmt), 'id': i} for i in range(8)]
        return docs + docs[:3]
    if case == 'dense':
        docs = [{'dt': (START + timedelta(hours=i)).strftime(fmt), 'id': i} for i in range(24)]
        return docs + docs
    ### A row on each edge and one a minute before it (inside the neighbor's diff window).
    docs = []
    for i in range(1, 6):
        edge = START + timedelta(hours=6 * i)
        docs.append({'dt': edge.strftime(fmt), 'id': i})
        docs.append({'dt': (edge - timedelta(minutes=1)).strftime(fmt), 'id': 100 + i})
    return docs + docs


@pytest.mark.parametrize('case', ['edges', 'dense', 'near_edges'])
def test_deduplicate_honors_workers(case, w15l2_plugin, native_calls):
    """
    The chunks run concurrently on a thread-safe instance (the instance records how many
    `get_pipe_data()` calls are in flight at once) and no chunk loses or gains a row, with rows
    on the chunk edges. The test instance serializes its writes with a file lock, as an
    `IS_THREAD_SAFE` connector must.
    """
    from meerschaum.utils.dataframe import parse_df_datetimes
    docs = _edge_docs(case)
    expected_rows = len({json.dumps(doc) for doc in docs})
    observed = {}
    for workers in (1, 64):
        _gate.set_mode('rust')
        instance = _back_instance(delay=0.2)
        pipe = mrsm.Pipe('w15l2', 'x' + uuid.uuid4().hex[:8], instance=instance, columns=COLUMNS)
        assert pipe.register()[0]
        assert instance.sync_pipe(pipe, parse_df_datetimes(docs, strip_timezone=False))[0]
        assert pipe.get_rowcount() == len(docs)
        success, message = pipe.deduplicate(
            workers=workers, chunk_interval=timedelta(hours=6), _use_instance_method=False,
        )
        assert success, message
        assert pipe.get_rowcount() == expected_rows
        remaining = pipe.get_data(select_columns=['dt', 'id'])
        assert len(remaining.drop_duplicates()) == expected_rows == len(remaining)
        observed[workers] = (_max_inflight(instance), pipe.get_num_workers(workers=workers))
        pipe.delete()
    assert observed[1][0] == 1
    ### `get_num_workers()` subtracts the threads alive in this process.
    assert observed[64][0] >= min(observed[64][1], 2), observed


def test_numeric_specials_survive_get_data(kind, instance, native_calls):
    """`Decimal('NaN')` and the infinities are values, not nulls (PostgreSQL `NUMERIC`)."""
    if kind != 'postgis':
        pytest.skip("PostgreSQL only")
    from decimal import Decimal
    _gate.set_mode('rust')
    values = [Decimal('1.5'), Decimal('NaN'), Decimal('Infinity'), Decimal('-Infinity')]
    pipe = _pipe(instance, parameters={'dtypes': {'n': 'numeric'}})
    pipe.delete()
    pipe = _pipe(instance, pipe.metric_key, parameters={'dtypes': {'n': 'numeric'}})
    assert pipe.sync([
        {'dt': START + timedelta(hours=i), 'id': i, 'val': 0.0, 'name': 'x', 'n': value}
        for i, value in enumerate(values)
    ])[0]
    try:
        docs = sorted(pipe.get_data(as_docs=True), key=lambda d: d['id'])
        ### `Decimal('NaN')` is stored as `NULL` by the sync (the table says `n IS NULL`), so the
        ### documents hold `None` there; the infinities are values.
        assert [repr(d['n']) for d in docs if d['id'] != 1] == [
            repr(v) for i, v in enumerate(values) if i != 1
        ]
        assert docs[1]['n'] is None or docs[1]['n'].is_nan()
        frame = pipe.get_data()
        assert [repr(v) for v in frame.sort_values('id')['n']] == [repr(v) for v in values]
    finally:
        pipe.delete()


SPECIAL_DTYPES = {
    'g': 'geometry[Point, 4326]',
    'gb': 'geometry',
    'n': 'numeric',
    'j': 'json',
    'u': 'uuid',
    'b': 'bytes',
    'i': 'int',
    't': 'datetime',
}


def _special_rows(n, offset=0):
    import shapely
    from decimal import Decimal
    return [
        {
            'dt': START + timedelta(hours=i),
            'id': i,
            'g': shapely.Point(i, i),
            'gb': shapely.Point(-i, i),
            'n': Decimal(str(i)),
            'j': {'a': i},
            'u': uuid.UUID(int=i + 1),
            'b': bytes([i]),
            ### Rows 0-3 (the stored ones) have a value; the new rows 4+ alternate with nulls.
            ### (v4 on PostgreSQL reports a stored row with a null `int` as changed: PARITY W15L2-013.)
            'i': i if (i < 4 or i % 2) else None,
            't': START + timedelta(days=i),
        }
        for i in range(offset, offset + n)
    ]


@pytest.mark.parametrize(
    'operation', ['enforce_dtypes', 'filter_existing', 'filter_geo', 'filter_wkt', 'docs'],
)
def test_special_dtypes_match_python_mode(operation, kind, instance, native_calls):
    """
    Geometry (with and without an SRID, as shapely objects and as WKT text), nullable `int`,
    `numeric`, `json`, `uuid`, `bytes` and timezone-aware datetime columns come back with the same
    values and pandas dtypes (and, for documents, the same objects) on SQLite and PostgreSQL.
    """
    if kind not in ('sqlite', 'postgis'):
        pytest.skip("SQL instances with geometry support")
    pd = pytest.importorskip('pandas')
    pytest.importorskip('shapely')
    gpd = pytest.importorskip('geopandas') if operation == 'filter_geo' else None
    _gate.set_mode('rust')
    pipe = mrsm.Pipe(
        'w15l2', 'special' + uuid.uuid4().hex[:6], instance=instance,
        columns=COLUMNS, dtypes=SPECIAL_DTYPES,
    )
    pipe.delete()
    pipe = mrsm.Pipe(
        'w15l2', pipe.metric_key, instance=instance, columns=COLUMNS,
        dtypes=SPECIAL_DTYPES,
    )
    assert pipe.sync(_special_rows(4))[0]
    try:
        def call():
            if operation == 'enforce_dtypes':
                return pipe.enforce_dtypes(pd.DataFrame(_special_rows(3)))
            if operation == 'docs':
                return sorted(pipe.get_data(as_docs=True), key=lambda d: d['id'])
            rows = _special_rows(3, 2) + _special_rows(2, 6)
            if operation == 'filter_wkt':
                for row in rows:
                    row['g'], row['gb'] = row['g'].wkt, row['gb'].wkt
            frame = (
                gpd.GeoDataFrame(rows, geometry='g', crs=4326)
                if gpd is not None
                else pd.DataFrame(rows)
            )
            return pipe.filter_existing(frame)

        outcomes = {}
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)
            outcomes[mode] = call()
        if operation == 'docs':
            ### The same Python objects, type for type (a hex EWKB string stays text).
            for rust, python in zip(outcomes['rust'], outcomes['python']):
                assert {k: type(v).__name__ for k, v in rust.items() if k != 'dt'} == {
                    k: type(v).__name__ for k, v in python.items() if k != 'dt'
                }
                assert snapshot(rust) == snapshot(python)
        else:
            outcomes = {mode: snapshot(result) for mode, result in outcomes.items()}
            assert outcomes['rust'] == outcomes['python']
            ### A frame of WKT text is a GeoDataFrame with a `geometry` column in both modes.
            if operation == 'filter_wkt':
                assert snapshot(call())[0]['dtypes'][2] == 'geometry'
    finally:
        _gate.set_mode('rust')
        pipe.delete()


def test_enforce_dtypes_of_a_timezone_aware_frame(
    reference_instance, native_calls,
):
    pd = pytest.importorskip('pandas')
    pipe = _pipe(reference_instance)
    outcomes = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        frame = pd.DataFrame({
            'dt': pd.to_datetime(['2026-01-01', '2026-01-02'], utc=True).astype('datetime64[ns, UTC]'),
            'id': [1, 2],
        })
        outcomes[mode] = snapshot(pipe.enforce_dtypes(frame))
    assert outcomes['rust'] == outcomes['python']


def test_get_data_forwards_its_keywords(kind, instance, reference_instance, native_calls):
    """`begin_add_minutes` and `end_add_minutes` move the bounds of a SQL read, as in v4."""
    if kind not in ('sqlite', 'postgis'):
        pytest.skip("SQL instances read these keywords")
    _compare(
        kind, instance, reference_instance,
        lambda pipe: pipe.get_data(
            begin=START + timedelta(hours=4), begin_add_minutes=-120,
            end=START + timedelta(hours=8), end_add_minutes=60,
        ),
        native_calls, 'pipe_get_data',
    )


def test_a_custom_instance_receives_the_keywords_of_get_data(w15l2_plugin, native_calls):
    """Unknown keywords reach a custom connector's `get_pipe_data()` (through the worker)."""
    instance = _back_instance()
    pipe = _loaded(instance)
    try:
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)
            assert len(pipe.get_data(w15l2_marker='x')) == 12
        calls = _kwargs_seen(instance)
        assert calls.count('x') >= 2, calls
        ### `get_backtrack_data` and `deduplicate` forward theirs too, with no fallback.
        for mode in ('python', 'rust'):
            _gate.set_mode(mode)
            assert pipe.get_backtrack_data(backtrack_minutes=60, w15l2_marker='y') is not None
        assert _kwargs_seen(instance).count('y') >= 2
    finally:
        _gate.set_mode('rust')
        pipe.delete()


def _kwargs_seen(instance):
    import pathlib
    from meerschaum.config.paths import ROOT_DIR_PATH
    path = pathlib.Path(ROOT_DIR_PATH) / f'w15l2back_{instance.label}_markers.json'
    return json.loads(path.read_text()) if path.exists() else []


def test_a_failed_rust_build_is_an_error_not_a_silent_python_fallback(tmp_path, native_calls):
    """
    A SQL connector whose Rust build failed (the state `SQLConnector._rust_method` leaves: the
    connector marked `False` and the cause kept) has no other implementation: every read raises,
    none runs the v4 SQLAlchemy body and returns rows.
    """
    pd = pytest.importorskip('pandas')
    conn = mrsm.get_connector(
        'sql', 'w15l2_broken_' + uuid.uuid4().hex[:6],
        flavor='sqlite', database=str(tmp_path / 'broken.db'),
    )
    pipe = _loaded(conn, n=4)
    conn.__dict__['_rust_connector'] = False
    conn.__dict__['_rust_error'] = 'forced failure'
    pipe = _pipe(conn, pipe.metric_key)
    _gate.set_mode('rust')
    calls = {
        'get_data': lambda: pipe.get_data(),
        'get_data_as_docs': lambda: pipe.get_data(as_docs=True),
        'get_backtrack_data': lambda: pipe.get_backtrack_data(),
        'get_sync_time': lambda: pipe.get_sync_time(),
        'get_rowcount': lambda: pipe.get_rowcount(),
        'filter_existing': lambda: pipe.filter_existing(pd.DataFrame(_rows(2))),
        'enforce_dtypes': lambda: pipe.enforce_dtypes(_rows(2)),
        'infer_dtypes': lambda: pipe.infer_dtypes(),
        'fetch': lambda: pipe.fetch(),
        'deduplicate': lambda: pipe.deduplicate(),
    }
    for name, call in calls.items():
        with pytest.raises(RuntimeError, match='no usable Rust driver'):
            call()
            pytest.fail(f"{name} ran the Python body")
