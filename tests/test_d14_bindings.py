"""
D14 runway parity (wave 8): the remaining Python-core paths routed to Rust under `MRSM_CORE=rust`.

Each test runs the same call in `python` mode and in `rust` mode in one process (the gate is
re-read with `set_mode`) and compares the results. The SQL text of `sql_item_name`,
`build_where` and `get_pipe_metadef` must match v4 exactly, per flavor.
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest

import meerschaum as mrsm
from meerschaum._core import _gate

try:
    from meerschaum import _core
    _core.load()
    HAS_CORE = True
except Exception:
    HAS_CORE = False

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")

FLAVORS = [
    'sqlite', 'postgresql', 'postgis', 'timescaledb', 'mysql', 'mariadb', 'mssql', 'oracle',
    'cockroachdb', 'duckdb', 'geopackage', 'citus',
]


@pytest.fixture(autouse=True)
def restore_mode():
    original = (_gate._MODE, _gate._FEATURES)
    yield
    _gate._MODE, _gate._FEATURES = original


def both(func, *args, **kwargs):
    """Run `func` in python mode, then rust mode; return `(python_result, rust_result)`."""
    _gate.set_mode('python')
    expected = func(*args, **kwargs)
    _gate.set_mode('rust')
    actual = func(*args, **kwargs)
    return expected, actual


ITEMS = [
    'table', 'Table', 'MyColumn', '_leading', 'a' * 80, 'a' * 200, '#temp', 'with space',
    'float', 'Date', 'ROW', 'x"y', 123, 'définition', 'definition',
]


@pytest.mark.parametrize('flavor', FLAVORS + ['nonsense'])
def test_sql_item_name_matches_python(flavor):
    from meerschaum.utils.sql import sql_item_name
    for item in ITEMS:
        for schema in (None, 'my_schema', 'Schema'):
            expected, actual = both(sql_item_name, item, flavor, schema)
            assert actual == expected, (flavor, item, schema)


PARAMS = [
    {},
    {'a': 1},
    {'a': 'red', 'b': 'blue'},
    {'a': ['x', 'y', 'z']},
    {'a': '_x'},
    {'a': ['_x', '_y']},
    {'a': ['x', '_y']},
    {'a': None},
    {'a': 'None'},
    {'a': '_None'},
    {'a': [None, 'x']},
    {'a': ['_None', '_x']},
    {'a': {'k': 'v', 'n': 1}},
    {'a': "it's", 'b': ["o'k"]},
    {'A': True, 'b': 1.5, 'c': ''},
    {'a': ('t', 'u')},
    {'a': []},
    {'a': 'DROP TABLE x'},
    {'a': 'x;y'},
    {'a': '--x'},
]


@pytest.mark.parametrize('flavor', ['sqlite', 'postgresql', 'mysql', 'mssql', 'oracle', 'timescaledb'])
@pytest.mark.parametrize('with_where', [True, False])
def test_build_where_matches_python(flavor, with_where):
    from meerschaum.utils.sql import build_where
    for params in PARAMS:
        expected, actual = both(build_where, params, with_where=with_where, flavor=flavor)
        assert actual == expected, (flavor, params)


def test_build_where_takes_the_connector_flavor(tmp_path):
    from meerschaum.utils.sql import build_where
    conn = mrsm.get_connector(
        'sql', 'd14_bw_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'a.db'),
    )
    expected, actual = both(build_where, {'A': 'x'}, connector=conn)
    assert actual == expected


def test_build_where_injection_warns_in_both_modes():
    from meerschaum.utils.sql import build_where
    import warnings
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            assert build_where({'a': 'x; drop table y'}) == ''
        assert any('SQL injection' in str(w.message) for w in caught), mode


def _server_connector(name: str):
    """A disposable test-container connector (skip when it is not reachable)."""
    from tests.connectors import conns
    conn = conns[name]
    try:
        reachable = conn.test_connection(debug=False)
    except Exception:
        reachable = False
    if not reachable:
        pytest.skip(f"The '{name}' test container is not reachable.")
    return conn


def _metadef_pipe(conn, dt_typ='datetime', **kwargs):
    metric = uuid.uuid4().hex[:8]
    parameters = {
        'columns': {'datetime': 'dt'} if dt_typ == 'datetime' else {'datetime': 'ts'},
        'dtypes': {'dt': 'datetime'},
        'sql': 'SELECT * FROM remote_table',
    }
    if dt_typ != 'datetime':
        parameters['dtypes'] = {'ts': dt_typ}
    parameters.update(kwargs)
    return mrsm.Pipe(str(conn), 'metadef_' + metric, instance=conn, parameters=parameters)


UTC = timezone.utc
BEGINS = [
    datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
    datetime(2024, 1, 2, 3, 4, 5),
    datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=timezone(timedelta(hours=-5))),
    '2024-01-02 03:04:05',
    None,
]
ENDS = [None, datetime(2024, 2, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC)]


@pytest.mark.parametrize('flavor', ['postgis', 'timescaledb', 'mysql', 'mariadb', 'sqlite'])
def test_get_pipe_metadef_matches_python(flavor, tmp_path):
    if flavor == 'sqlite':
        conn = mrsm.get_connector(
            'sql', 'd14_md_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'm.db'),
        )
    else:
        conn = _server_connector(flavor)
    pipe = _metadef_pipe(conn)
    for begin in BEGINS:
        for end in ENDS:
            for check_existing in (True, False):
                for params in (None, {'id': 'a', 'v': ['1', '_2']}):
                    # PARITY: v4 raises `TypeError` comparing a naive `begin` with an aware
                    # `end`, or a string with a datetime; those combinations are not compared.
                    try:
                        expected, actual = both(
                            conn.get_pipe_metadef, pipe,
                            params=params, begin=begin, end=end, check_existing=check_existing,
                        )
                    except TypeError:
                        continue
                    assert actual == expected, (flavor, begin, end, check_existing, params)


def test_get_pipe_metadef_default_begin_uses_sync_time(tmp_path):
    conn = mrsm.get_connector(
        'sql', 'd14_st_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 's.db'),
    )
    pipe = mrsm.Pipe('d14', 'synctime', instance=conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': datetime(2024, 3, 4, 5, 6, 7), 'v': 1}])
    assert success, msg
    source = mrsm.Pipe(
        str(conn), 'src', instance=conn, columns={'datetime': 'dt'},
        parameters={'sql': 'SELECT * FROM ' + pipe.target, 'fetch': {'backtrack_minutes': 90}},
    )
    for check_existing in (True, False):
        expected, actual = both(
            conn.get_pipe_metadef, source, check_existing=check_existing,
        )
        assert actual == expected


@pytest.mark.parametrize('dt_typ', ['int', 'int64'])
def test_get_pipe_metadef_integer_axis(dt_typ, tmp_path):
    conn = mrsm.get_connector(
        'sql', 'd14_int_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'i.db'),
    )
    pipe = _metadef_pipe(conn, dt_typ=dt_typ)
    for begin in (5, 100, None, datetime(2024, 1, 1, tzinfo=UTC)):
        for end in (None, 1000):
            expected, actual = both(conn.get_pipe_metadef, pipe, begin=begin, end=end)
            assert actual == expected, (begin, end)


def test_get_pipe_metadef_parent_pushdown(tmp_path):
    conn = mrsm.get_connector(
        'sql', 'd14_par_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'p.db'),
    )
    parent = mrsm.Pipe('d14', 'parent', instance=conn, columns={'datetime': 'dt'})
    child = mrsm.Pipe(
        str(conn), 'child', instance=conn, columns={'datetime': 'dt'},
        parameters={
            'sql': f'SELECT * FROM "{parent.target}" WHERE x > 1',
            'parent': parent.keys(),
        },
    )
    for begin in (datetime(2024, 1, 1, tzinfo=UTC), '', None):
        expected, actual = both(conn.get_pipe_metadef, child, begin=begin, end=None)
        assert actual == expected, begin


def test_get_pipe_metadef_without_definition_raises_in_both_modes(tmp_path):
    conn = mrsm.get_connector(
        'sql', 'd14_nd_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'n.db'),
    )
    pipe = mrsm.Pipe(str(conn), 'nodef', instance=conn, columns={'datetime': 'dt'})
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        with pytest.raises(ValueError):
            conn.get_pipe_metadef(pipe)


def test_sync_pipe_inplace_matches_python(tmp_path):
    results = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        conn = mrsm.get_connector(
            'sql', f'd14_ip_{mode}_' + uuid.uuid4().hex, flavor='sqlite',
            database=str(tmp_path / f'ip_{mode}.db'),
        )
        src = mrsm.Pipe('d14', 'inplace_src', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
        docs = [
            {'dt': datetime(2024, 1, i + 1), 'id': i % 2, 'val': float(i)} for i in range(6)
        ]
        success, msg = src.sync(docs)
        assert success, msg
        dest = mrsm.Pipe(
            str(conn), 'inplace_dest', instance=conn,
            columns={'datetime': 'dt', 'id': 'id'},
            parameters={'sql': f'SELECT * FROM "{src.target}"'},
        )
        first = conn.sync_pipe_inplace(dest, check_existing=True)
        assert first[0], first
        second = conn.sync_pipe_inplace(dest, check_existing=True)
        assert second[0], second
        df = dest.get_data(order='asc')
        results[mode] = (
            len(df), sorted(df.columns), df['val'].tolist(), dest.get_rowcount(),
        )
    assert results['rust'] == results['python']
    assert results['rust'][0] == 6


def _sqlite(tmp_path, name='d14'):
    return mrsm.get_connector(
        'sql', f'{name}_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / f'{name}.db'),
    )


def _make_pipe(conn, metric, **kwargs):
    return mrsm.Pipe('d14', metric, instance=conn, **kwargs)


def _fresh(conn, metric, **kwargs):
    """A new pipe object (no memoized parameters), registered when it has no references."""
    return mrsm.Pipe('d14', metric, instance=conn, **kwargs)


def test_get_parameters_resolves_references_and_symlinks(tmp_path):
    conn = _sqlite(tmp_path)
    base = _make_pipe(conn, 'base', columns={'datetime': 'dt', 'id': 'id'}, tags=['a'])
    base.register()
    results = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        child = _fresh(
            conn, 'child_' + mode,
            parameters={
                'reference': base.keys(),
                'tags': ['b'],
                'sql': "SELECT * FROM {{ Pipe('d14', 'base', instance='%s') }}" % conn,
                'nested': {'items': ['x', "{{ Pipe('d14', 'base', instance='%s').target }}" % conn]},
            },
        )
        params = child.get_parameters()
        results[mode] = (params, dict(child._symlinks))
    assert results['rust'][0] == results['python'][0]
    assert results['rust'][1] == results['python'][1]
    assert results['rust'][1], "expected `_symlinks` entries"
    assert results['rust'][0]['columns'] == {'datetime': 'dt', 'id': 'id'}


def test_get_parameters_symlinks_with_config_symlink(tmp_path):
    conn = _sqlite(tmp_path)
    results = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        pipe = _fresh(
            conn, 'cfg',
            parameters={
                'user': 'MRSM{meerschaum:instance}',
                'sql': "SELECT '{{ self.metric }}'",
            },
        )
        results[mode] = (pipe.get_parameters(), dict(pipe._symlinks))
    assert results['rust'] == results['python']


def test_get_parameters_circular_reference_warns(tmp_path):
    import warnings
    conn = _sqlite(tmp_path)
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        a = _fresh(conn, 'cir_a', parameters={'reference': f"Pipe('d14', 'cir_b', instance='{conn}')"})
        b = _fresh(conn, 'cir_b', parameters={'reference': f"Pipe('d14', 'cir_a', instance='{conn}')"})
        a.register()
        b.register()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            a_params = _fresh(conn, 'cir_a').get_parameters()
        # PARITY: v4 memoizes reference parameters process-wide, so only the first resolution
        # in a process warns in python mode; the Rust path warns on every resolution.
        if mode == 'rust':
            assert any('Circular reference' in str(w.message) for w in caught)
        assert isinstance(a_params, dict)


CHUNK_CASES = [
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC)),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC), bounded=True),
    dict(begin=datetime(2024, 1, 1, 7, 5, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC),
         align=True),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 1, 2, tzinfo=UTC)),
    dict(begin=datetime(2024, 1, 5, tzinfo=UTC), end=datetime(2024, 1, 1, tzinfo=UTC)),
    dict(begin=datetime(2024, 1, 5, tzinfo=UTC), end=datetime(2024, 1, 1, tzinfo=UTC),
         bounded=True),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 1, 1, tzinfo=UTC)),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC),
         chunk_interval=timedelta(days=7)),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC), end=datetime(2024, 3, 1, tzinfo=UTC),
         chunk_interval=timedelta(days=7), align=True),
    dict(begin='2024-01-01', end='2024-02-15 10:00:00', chunk_interval=1440),
    dict(),
    dict(begin=datetime(2024, 1, 1, tzinfo=UTC)),
    dict(end=datetime(2024, 2, 1, tzinfo=UTC)),
    dict(bounded=True),
]


@pytest.mark.parametrize('case', range(len(CHUNK_CASES)))
def test_get_chunk_bounds_matches_python_on_datetime_axis(case, tmp_path):
    conn = _sqlite(tmp_path)
    pipe = _make_pipe(conn, 'chunks', columns={'datetime': 'dt'})
    _gate.set_mode('python')
    success, msg = pipe.sync([
        {'dt': datetime(2024, 1, 10, 12, 30), 'v': 1},
        {'dt': datetime(2024, 2, 20, 6, 15), 'v': 2},
    ])
    assert success, msg
    kwargs = CHUNK_CASES[case]
    try:
        expected, actual = both(pipe.get_chunk_bounds, **kwargs)
    except (TypeError, ValueError) as e:
        pytest.skip(f"v4 raises for this combination: {e}")
    assert actual == expected


@pytest.mark.parametrize('parameters', [
    {},
    {'verify': {'chunk_minutes': 90}},
    {'verify': {'chunk_hours': 6}},
    {'verify': {'chunk_days': 2, 'chunk_hours': 6}},
    {'verify': {'chunk_weeks': 1}},
    {'verify': {'chunk_years': 1}},
    {'verify': {'chunk_seconds': 30}},
])
@pytest.mark.parametrize('arg', [None, timedelta(hours=3), 60])
def test_get_chunk_interval_matches_python(parameters, arg, tmp_path):
    conn = _sqlite(tmp_path)
    pipe = _make_pipe(conn, 'ci', columns={'datetime': 'dt'}, parameters=parameters)
    expected, actual = both(pipe.get_chunk_interval, arg)
    assert actual == expected
    assert type(actual) is type(expected)


@pytest.mark.parametrize('parameters,arg', [
    ({}, None),
    ({'precision': 'ms'}, None),
    ({'precision': 'ms'}, timedelta(minutes=1)),
    ({'precision': 'second', 'verify': {'chunk_hours': 1}}, None),
    ({'verify': {'chunk_range': 5000}}, None),
    ({}, 77),
])
def test_get_chunk_interval_integer_axis(parameters, arg, tmp_path):
    conn = _sqlite(tmp_path)
    pipe = _make_pipe(
        conn, 'ci_int', columns={'datetime': 'ts'}, dtypes={'ts': 'int'}, parameters=parameters,
    )
    expected, actual = both(pipe.get_chunk_interval, arg)
    assert actual == expected


def test_get_chunk_bounds_integer_axis(tmp_path):
    conn = _sqlite(tmp_path)
    pipe = _make_pipe(
        conn, 'cb_int', columns={'datetime': 'ts'}, dtypes={'ts': 'int'},
        parameters={'verify': {'chunk_range': 100}},
    )
    for kwargs in (
        dict(begin=0, end=450),
        dict(begin=17, end=450, align=True),
        dict(begin=17, end=450, bounded=True),
        dict(begin=17, end=450),
    ):
        expected, actual = both(pipe.get_chunk_bounds, **kwargs)
        assert actual == expected, kwargs
