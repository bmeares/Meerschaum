#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Tests for the `meerschaum._core` bridge (crates/meerschaum-py).

Results the parity-phase Python mode produced are recorded in `tests/oracle/test_core.json` (see
`tests/_oracle.py`) and replayed, so the Rust bindings are still compared with what that mode returned (recordings of the
parity-phase mode, not v4.1.4 output; they cannot be re-recorded).

Run from the repository root (PYTHONPATH matters: a stale `meerschaum` may shadow the tree):

    scripts/rust/build_py.sh
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest crates/meerschaum-py/tests/test_core.py -q

The extension is required: without it `import meerschaum` raises `CoreNotBuiltError`, so this
module cannot even be collected on an unbuilt tree. pyarrow, polars and pandas are test-only
requirements; the extension itself needs none.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]

import meerschaum._core as core  # noqa: E402
from meerschaum.utils.packages import attempt_import  # noqa: E402
from tests import _oracle as oracle  # noqa: E402
from tests._binary import skip_or_fail  # noqa: E402

### The data libraries come from the `mrsm` venv, as for the façade: the recorded dtypes
### depend on the pandas version (3.x), so the replay has to load the same libraries.
attempt_import('pandas', 'pyarrow', 'shapely', 'geopandas', lazy=False)



def run_python(code: str, **env_overrides: str) -> subprocess.CompletedProcess:
    """Run `code` in a fresh interpreter with the working tree on `sys.path`."""
    env = {
        **os.environ,
        'PYTHONPATH': os.pathsep.join(filter(None, (str(REPO), os.environ.get('PYTHONPATH')))),
        'MRSM_ROOT_DIR': os.environ.get('MRSM_ROOT_DIR', str(REPO / 'test_root')),
        'MRSM_PLUGINS_DIR': os.environ.get('MRSM_PLUGINS_DIR', str(REPO / 'tests' / 'plugins')),
    }
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(code)],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


# A prelude that makes `import meerschaum._core._native` fail, simulating an unbuilt tree without
# touching the real build artifact.
BLOCK_EXTENSION = "import sys; sys.modules['meerschaum._core._native'] = None\n"


# ---------------------------------------------------------------------------------------------
# Loader: with and without the extension
# ---------------------------------------------------------------------------------------------

def test_tree_is_imported_from_the_repo():
    """Guard against the stale `~/.local` install (CLAUDE.md test-harness gotcha)."""
    import meerschaum
    assert Path(meerschaum.__file__).resolve().is_relative_to(REPO)
    assert Path(core.__file__).resolve().parent == REPO / 'meerschaum' / '_core'


def test_import_is_lazy(monkeypatch):
    """
    Importing `meerschaum`, its config or the loader must not load the extension.

    `MRSM_<TYPE>_<LABEL>` connector variables (which `scripts/test.sh` exports) make the import
    apply them over `meerschaum.yaml`, as v4 does, and reading that file legitimately loads the
    native YAML codec: drop every `MRSM_*` variable but the root and plugins dirs so the bare
    import is what is measured.
    """
    for key in list(os.environ):
        if key.startswith('MRSM_') and key not in ('MRSM_ROOT_DIR', 'MRSM_PLUGINS_DIR'):
            monkeypatch.delenv(key)
    proc = run_python("""
        import sys
        import meerschaum
        import meerschaum.config
        import meerschaum._core
        print('meerschaum._core._native' in sys.modules)
    """)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == 'False'


@pytest.mark.parametrize('statement', ['import meerschaum', 'import meerschaum._core'])
def test_import_without_extension_fails(statement):
    """
    The extension is required: with the file missing, importing the package raises
    `CoreNotBuiltError` (an `ImportError`) whose message names the build script and the wheel.
    """
    proc = run_python(BLOCK_EXTENSION + textwrap.dedent(f"""
        try:
            {statement}
        except ImportError as e:
            print(type(e).__name__)
            print(str(e))
        else:
            raise AssertionError('`{statement}` must fail without the extension')
    """))
    assert proc.returncode == 0, proc.stderr
    name, _, message = proc.stdout.partition('\n')
    assert name == 'CoreNotBuiltError', proc.stdout
    assert 'scripts/rust/build_py.sh' in message, message
    assert 'wheel' in message, message


def test_import_with_extension():
    assert core.AVAILABLE is True
    assert core.LOAD_ERROR is None
    assert Path(core.EXTENSION_PATH).name.startswith('_native')
    assert core.__version__.startswith('5.')

    info = core.build_info()
    assert info['version'] == core.__version__
    assert info['version_pep440'].startswith('5.')
    assert info['rustc'].startswith('rustc ')
    assert info['profile'] in ('debug', 'release')
    assert info['python_abi'] == 'abi3-py311'
    assert 'extension-module' in info['features']
    assert info['static_config'] is core.HAS_STATIC_CONFIG
    assert 'polars' not in info['features']
    assert not hasattr(core, 'HAS_POLARS')


def test_extension_has_no_pyarrow_import_dependency():
    """Loading `_core` and round-tripping a polars frame must not import pyarrow."""
    proc = run_python("""
        import sys
        import meerschaum._core as core
        assert core.AVAILABLE
        before = 'pyarrow' in sys.modules
        try:
            import polars as pl
        except ImportError:
            print('skip')
            raise SystemExit(0)
        s = core.roundtrip_arrow(pl.DataFrame({'a': [1, None, 3], 's': ['x', None, 'z']}))
        out = pl.DataFrame(s)
        assert out.to_dict(as_series=False) == {'a': [1, None, 3], 's': ['x', None, 'z']}
        print(before, 'pyarrow' in sys.modules)
    """)
    assert proc.returncode == 0, proc.stderr
    if proc.stdout.strip() != 'skip':
        assert proc.stdout.strip() == 'False False'


# ---------------------------------------------------------------------------------------------
# static_config()
# ---------------------------------------------------------------------------------------------

def _normalize(value):
    """TOML has no sets/tuples: compare sets as sorted lists and tuples as lists."""
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_normalize(v) for v in value), key=repr)
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    return value


def _diff(left, right, path=''):
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right)):
            if key not in left or key not in right:
                yield f"{path}.{key}: only in {'TOML' if key in left else 'v4'}"
            else:
                yield from _diff(left[key], right[key], f"{path}.{key}")
    elif left != right:
        yield f"{path}: {left!r} != {right!r}"


### Keys v5 adds to the v4 table (an overlay: the recorded v4 dictionary is never edited), as
### `(table, key)`. W18-FX4-001: `environment.compose_root`, the `MRSM__COMPOSE_ROOT` label of jobs
### started from a root compose file. Mirrors `static_toml_matches_python` in
### `crates/meerschaum-core/tests/static_config.rs`.
V5_ONLY_STATIC_KEYS = (('environment', 'compose_root'),)


def test_static_config_matches_v4():
    """`static_config()` equals the v4 `STATIC_CONFIG` dictionary (recorded, normalized)."""
    if not core.HAS_STATIC_CONFIG:
        assert core.static_config() is None
        pytest.skip('crates/meerschaum-core/static.toml did not exist when _core was built')

    expected = oracle.expected()
    rust = core.static_config()
    assert isinstance(rust, dict)
    rust = _normalize(rust)
    for table, key in V5_ONLY_STATIC_KEYS:
        assert key in rust[table], f"v5-only key {table}.{key} is not in static.toml"
        del rust[table][key]
    differences = list(_diff(rust, expected))
    assert not differences, '\n'.join(differences)


def test_static_config_returns_fresh_dicts():
    if not core.HAS_STATIC_CONFIG:
        pytest.skip('static.toml not embedded')
    first = core.static_config()
    first.clear()
    assert core.static_config()


# ---------------------------------------------------------------------------------------------
# Arrow round trips
# ---------------------------------------------------------------------------------------------

def _same(left, right) -> bool:
    """Deep equality where NaN equals NaN (pyarrow's `equals` treats NaN as unequal)."""
    if isinstance(left, float) and isinstance(right, float) and left != left and right != right:
        return True
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_same(left[k], right[k]) for k in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right))
    return type(left) is type(right) and left == right


def assert_tables_equal(out, expected):
    """Schemas (with schema- and field-level metadata) and values must match exactly."""
    assert out.schema.equals(expected.schema, check_metadata=True), (out.schema, expected.schema)
    assert out.num_rows == expected.num_rows
    assert _same(out.to_pylist(), expected.to_pylist()), (out, expected)


def _pyarrow_columns():
    """(name, pyarrow array) for every dtype family the kernel must carry."""
    pa = pytest.importorskip('pyarrow')
    t0 = dt.datetime(2024, 1, 2, 3, 4, 5, 678901)
    return [
        ('int8', pa.array([-1, None, 127], pa.int8())),
        ('int16', pa.array([1, None, -3], pa.int16())),
        ('int32', pa.array([1, None, 2**31 - 1], pa.int32())),
        ('int64', pa.array([-(2**63), None, 2**63 - 1], pa.int64())),
        ('uint8', pa.array([0, None, 255], pa.uint8())),
        ('uint64', pa.array([2**64 - 1, None, 0], pa.uint64())),
        ('float32', pa.array([1.5, None, float('nan')], pa.float32())),
        ('float64', pa.array([-0.0, None, float('inf')], pa.float64())),
        ('bool', pa.array([True, None, False], pa.bool_())),
        ('string', pa.array(['héllo', None, ''], pa.string())),
        ('large_string', pa.array(['a', None, 'c'], pa.large_string())),
        ('string_view', pa.array(['a string longer than 12 bytes', None, 'x'], pa.string_view())),
        ('ts_utc_us', pa.array([t0, None, dt.datetime(1970, 1, 1)], pa.timestamp('us', tz='UTC'))),
        ('ts_ny_ns', pa.array([t0, None, t0], pa.timestamp('ns', tz='America/New_York'))),
        ('ts_naive_ms', pa.array([t0, None, t0], pa.timestamp('ms'))),
        ('date32', pa.array([dt.date(2024, 1, 2), None, dt.date(1970, 1, 1)], pa.date32())),
        ('time64', pa.array([dt.time(1, 2, 3, 4), None, dt.time(0)], pa.time64('us'))),
        ('duration', pa.array([dt.timedelta(seconds=5), None, dt.timedelta(0)], pa.duration('us'))),
        ('decimal128', pa.array(
            [decimal.Decimal('1234.5678901234'), None, decimal.Decimal('-1E-10')],
            pa.decimal128(38, 10),
        )),
        ('decimal256', pa.array(
            [decimal.Decimal('1' * 45 + '.12345'), None, decimal.Decimal('0')],
            pa.decimal256(50, 5),
        )),
        ('binary', pa.array([b'\x00\x01', None, b''], pa.binary())),
        ('large_binary', pa.array([b'wkb', None, b'\xff'], pa.large_binary())),
        ('binary_view', pa.array([b'x' * 20, None, b'y'], pa.binary_view())),
        ('uuid_fixed16', pa.array([uuid.UUID(int=1).bytes, None, uuid.UUID(int=2).bytes], pa.binary(16))),
        ('list_int64', pa.array([[1, 2], None, []], pa.list_(pa.int64()))),
        ('large_list_str', pa.array([['a', None], None, ['b']], pa.large_list(pa.string()))),
        ('fixed_list', pa.array([[1.0, 2.0], None, [3.0, 4.0]], pa.list_(pa.float64(), 2))),
        ('struct', pa.array(
            [{'x': 1, 'y': 'a'}, None, {'x': None, 'y': 'c'}],
            pa.struct([('x', pa.int32()), ('y', pa.string())]),
        )),
        ('list_struct', pa.array(
            [[{'k': 'a', 'v': 1.0}], None, []],
            pa.list_(pa.struct([('k', pa.string()), ('v', pa.float64())])),
        )),
        ('map', pa.array([[('a', 1)], None, []], pa.map_(pa.string(), pa.int64()))),
        ('dictionary', pa.array(['red', None, 'red']).dictionary_encode()),
        ('null', pa.array([None, None, None], pa.null())),
    ]


def _pyarrow_table():
    pa = pytest.importorskip('pyarrow')
    columns = _pyarrow_columns()
    fields = []
    for name, arr in columns:
        field = pa.field(name, arr.type)
        if name == 'large_binary':
            # Unregistered extension metadata must survive the boundary (D10).
            # A registered name (e.g. geoarrow.wkb after geometry tests) makes
            # PyArrow's own C-stream importer promote this raw binary field into
            # an ExtensionType. These arbitrary bytes are not geometry values.
            field = field.with_metadata({
                'ARROW:extension:name': 'meerschaum.test.unregistered_large_binary',
            })
        fields.append(field)
    schema = pa.schema(fields, metadata={'mrsm': 'v5'})
    return pa.Table.from_arrays([arr for _, arr in columns], schema=schema)


PYARROW_COLUMN_NAMES = [
    'int8', 'int16', 'int32', 'int64', 'uint8', 'uint64', 'float32', 'float64', 'bool',
    'string', 'large_string', 'string_view', 'ts_utc_us', 'ts_ny_ns', 'ts_naive_ms', 'date32',
    'time64', 'duration', 'decimal128', 'decimal256', 'binary', 'large_binary', 'binary_view',
    'uuid_fixed16', 'list_int64', 'large_list_str', 'fixed_list', 'struct', 'list_struct', 'map',
    'dictionary', 'null',
]


@pytest.mark.parametrize('column', PYARROW_COLUMN_NAMES)
def test_roundtrip_pyarrow_column(column):
    pa = pytest.importorskip('pyarrow')
    table = _pyarrow_table().select([column])
    stream = core.roundtrip_arrow(table)
    assert isinstance(stream, core.ArrowStream)
    assert_tables_equal(pa.table(stream), table)


def test_roundtrip_pyarrow_table_all_columns():
    pa = pytest.importorskip('pyarrow')
    table = _pyarrow_table()
    assert table.column_names == PYARROW_COLUMN_NAMES
    stream = core.roundtrip_arrow(table)
    assert len(stream) == stream.num_rows == table.num_rows
    assert stream.num_columns == table.num_columns
    assert stream.column_names == table.column_names
    assert 'ArrowStream(rows=3' in repr(stream)

    assert_tables_equal(pa.table(stream), table)
    # The stream can be consumed more than once.
    assert_tables_equal(pa.table(stream), table)
    # Schema export alone.
    assert pa.schema(stream).equals(table.schema, check_metadata=True)


def test_roundtrip_is_zero_copy():
    pa = pytest.importorskip('pyarrow')
    table = pa.table({'a': pa.array(range(1_000), pa.int64())})
    out = pa.table(core.roundtrip_arrow(table))
    src = table.column('a').chunk(0).buffers()[1]
    dst = out.column('a').chunk(0).buffers()[1]
    assert src.address == dst.address


def test_roundtrip_preserves_batches_and_accepts_readers_and_batches():
    pa = pytest.importorskip('pyarrow')
    table = _pyarrow_table()
    batches = table.to_batches() + table.slice(1, 2).to_batches()
    reader = pa.RecordBatchReader.from_batches(table.schema, batches)
    stream = core.roundtrip_arrow(reader)
    assert stream.num_batches == len(batches)
    assert_tables_equal(pa.table(stream), pa.Table.from_batches(batches))

    # A single RecordBatch (`__arrow_c_array__`).
    single = core.roundtrip_arrow(batches[0])
    assert_tables_equal(pa.table(single), pa.Table.from_batches([batches[0]]))

    # An empty stream keeps its schema.
    empty = core.roundtrip_arrow(table.schema.empty_table())
    assert len(empty) == 0
    assert pa.table(empty).schema.equals(table.schema, check_metadata=True)


def test_roundtrip_honors_requested_schema():
    pa = pytest.importorskip('pyarrow')
    stream = core.roundtrip_arrow(pa.table({'a': pa.array([1, 2], pa.int32())}))
    capsule = stream.__arrow_c_stream__(pa.schema([('a', pa.int64())]).__arrow_c_schema__())
    out = pa.RecordBatchReader._import_from_c_capsule(capsule).read_all()
    assert out.schema.field('a').type == pa.int64()


def test_roundtrip_rejects_non_arrow_objects():
    with pytest.raises((TypeError, ValueError)):
        core.roundtrip_arrow({'a': [1, 2, 3]})


def _polars():
    """polars, which CI installs: missing, it fails the test in CI and skips it locally."""
    try:
        import polars
    except ImportError:
        skip_or_fail('polars is not installed')
    return polars


def _polars_frame():
    pl = _polars()
    t0 = dt.datetime(2024, 1, 2, 3, 4, 5, 678901)
    return pl.DataFrame([
        pl.Series('int8', [-1, None, 127], pl.Int8),
        pl.Series('int16', [1, None, -3], pl.Int16),
        pl.Series('int32', [1, None, 3], pl.Int32),
        pl.Series('int64', [-(2**63), None, 2**63 - 1], pl.Int64),
        pl.Series('uint8', [0, None, 255], pl.UInt8),
        pl.Series('uint64', [2**64 - 1, None, 0], pl.UInt64),
        pl.Series('float32', [1.5, None, float('nan')], pl.Float32),
        pl.Series('float64', [-0.0, None, float('inf')], pl.Float64),
        pl.Series('bool', [True, None, False], pl.Boolean),
        pl.Series('string', ['héllo', None, 'a string longer than 12 bytes'], pl.String),
        pl.Series('binary', [b'\x00\x01', None, b'x' * 20], pl.Binary),
        pl.Series('dt_utc_us', [t0, None, t0], pl.Datetime('us', 'UTC')),
        pl.Series('dt_ny_ns', [t0, None, t0], pl.Datetime('ns', 'America/New_York')),
        pl.Series('dt_naive_ms', [t0, None, t0], pl.Datetime('ms')),
        pl.Series('date', [dt.date(2024, 1, 2), None, dt.date(1970, 1, 1)], pl.Date),
        pl.Series('time', [dt.time(1, 2, 3), None, dt.time(0)], pl.Time),
        pl.Series('duration', [dt.timedelta(seconds=5), None, dt.timedelta(0)], pl.Duration('us')),
        pl.Series(
            'decimal',
            [decimal.Decimal('1234.5678901234'), None, decimal.Decimal('-0.0000000001')],
            pl.Decimal(38, 10),
        ),
        pl.Series('list_int64', [[1, 2], None, []], pl.List(pl.Int64)),
        pl.Series('array_f64', [[1.0, 2.0], None, [3.0, 4.0]], pl.Array(pl.Float64, 2)),
        pl.Series(
            'struct',
            [{'x': 1, 'y': 'a'}, None, {'x': None, 'y': 'c'}],
            pl.Struct({'x': pl.Int32, 'y': pl.String}),
        ),
        pl.Series('list_struct', [[{'k': 'a'}], None, []], pl.List(pl.Struct({'k': pl.String}))),
        pl.Series('categorical', ['red', None, 'red'], pl.Categorical),
        pl.Series('enum', ['lo', None, 'hi'], pl.Enum(['lo', 'hi'])),
        pl.Series('null', [None, None, None], pl.Null),
    ])


POLARS_COLUMN_NAMES = [
    'int8', 'int16', 'int32', 'int64', 'uint8', 'uint64', 'float32', 'float64', 'bool', 'string',
    'binary', 'dt_utc_us', 'dt_ny_ns', 'dt_naive_ms', 'date', 'time', 'duration', 'decimal',
    'list_int64', 'array_f64', 'struct', 'list_struct', 'categorical', 'enum', 'null',
]


@pytest.mark.parametrize('column', POLARS_COLUMN_NAMES)
def test_roundtrip_arrow_from_polars(column):
    pl = _polars()
    from polars.testing import assert_frame_equal
    df = _polars_frame().select(column)
    out = pl.DataFrame(core.roundtrip_arrow(df))
    assert_frame_equal(out, df, categorical_as_str=True)


# `polars_roundtrip` reads the frame's Arrow C stream with Polars' own FFI, so every Polars type
# (`Enum` included, which keeps its categories through the field metadata) round-trips without
# pyarrow.
@pytest.mark.parametrize('column', POLARS_COLUMN_NAMES)
def test_polars_roundtrip(column):
    pl = _polars()
    from polars.testing import assert_frame_equal
    df = _polars_frame().select(column)
    out = core.polars_roundtrip(df)
    assert isinstance(out, pl.DataFrame)
    assert_frame_equal(out, df, categorical_as_str=True)


def test_polars_roundtrip_multi_chunk_frame():
    pl = _polars()
    from polars.testing import assert_frame_equal
    df = pl.concat([pl.DataFrame({'a': [1, 2]}), pl.DataFrame({'a': [3]})], rechunk=False)
    assert df.n_chunks() == 2
    assert_frame_equal(core.polars_roundtrip(df), df)



def test_polars_roundtrip_empty_frame_keeps_schema():
    pl = _polars()
    from polars.testing import assert_frame_equal
    df = _polars_frame().clear()
    assert df.height == 0
    out = core.polars_roundtrip(df)
    assert out.schema == df.schema
    assert_frame_equal(out, df)


def test_polars_roundtrip_rejects_objects_without_a_stream():
    with pytest.raises(TypeError, match='__arrow_c_stream__'):
        core.polars_roundtrip([1, 2, 3])

def _pandas_frame():
    pd = pytest.importorskip('pandas')
    pytest.importorskip('pyarrow')
    t0 = pd.Timestamp('2024-01-02 03:04:05.678901')
    return pd.DataFrame({
        'Int64': pd.array([1, None, 3], dtype='Int64'),
        'Int8': pd.array([1, None, -3], dtype='Int8'),
        'UInt64': pd.array([2**64 - 1, None, 0], dtype='UInt64'),
        'Float64': pd.array([1.5, None, 3.0], dtype='Float64'),
        'float64_nan': [1.5, float('nan'), 3.0],
        'boolean': pd.array([True, None, False], dtype='boolean'),
        'string_python': pd.array(['a', None, 'c'], dtype='string[python]'),
        'string_pyarrow': pd.array(['a', None, 'c'], dtype='string[pyarrow]'),
        'object_str': ['a', None, 'c'],
        'dt_utc_ns': pd.Series([t0, pd.NaT, t0]).dt.tz_localize('UTC'),
        'dt_ny_us': pd.Series([t0, pd.NaT, t0]).astype('datetime64[us]').dt.tz_localize('America/New_York'),
        'dt_naive_ns': pd.Series([t0, pd.NaT, t0]),
        'timedelta': pd.Series([pd.Timedelta(seconds=5), pd.NaT, pd.Timedelta(0)]),
        'decimal': [decimal.Decimal('1234.5678901234'), None, decimal.Decimal('-1E-10')],
        'bytes': [b'\x00\x01', None, b''],
        'list': [[1, 2], None, []],
        'struct': [{'x': 1, 'y': 'a'}, None, {'x': None, 'y': 'c'}],
        'category': pd.Categorical(['red', None, 'red']),
        'int64_arrow': pd.array([1, None, 3], dtype='int64[pyarrow]'),
        'ts_arrow': pd.array([t0, None, t0], dtype='timestamp[us, tz=UTC][pyarrow]'),
    })


PANDAS_COLUMN_NAMES = [
    'Int64', 'Int8', 'UInt64', 'Float64', 'float64_nan', 'boolean', 'string_python',
    'string_pyarrow', 'object_str', 'dt_utc_ns', 'dt_ny_us', 'dt_naive_ns', 'timedelta', 'decimal',
    'bytes', 'list', 'struct', 'category', 'int64_arrow', 'ts_arrow',
]


@pytest.mark.parametrize('column', PANDAS_COLUMN_NAMES)
def test_roundtrip_arrow_from_pandas(column):
    """pandas -> `pa.Table.from_pandas` -> Rust -> pyarrow -> pandas restores the frame."""
    pa = pytest.importorskip('pyarrow')
    pd = pytest.importorskip('pandas')
    df = _pandas_frame()[[column]]
    table = pa.Table.from_pandas(df, preserve_index=False)
    out_table = pa.table(core.roundtrip_arrow(table))
    # Schema metadata carries pandas' `b'pandas'` block, which restores extension dtypes.
    assert_tables_equal(out_table, table)
    pd.testing.assert_frame_equal(out_table.to_pandas(), table.to_pandas())


def test_roundtrip_arrow_from_pandas_dataframe_directly():
    """pandas >= 2.2 implements `__arrow_c_stream__` itself."""
    pa = pytest.importorskip('pyarrow')
    pd = pytest.importorskip('pandas')
    df = _pandas_frame()
    if not hasattr(df, '__arrow_c_stream__'):
        pytest.skip('pandas < 2.2 has no __arrow_c_stream__')
    out = pa.table(core.roundtrip_arrow(df))
    expected = pa.table(df)
    assert_tables_equal(out, expected)
    pd.testing.assert_frame_equal(out.to_pandas(), expected.to_pandas())


# ---------------------------------------------------------------------------------------------
# SqlConnector routing
# ---------------------------------------------------------------------------------------------

def test_sql_connector_routing(tmp_path, monkeypatch):
    """`SQLConnector._core_connector` builds one `_core.SqlConnector` per connector and process."""
    import meerschaum as mrsm
    conn = mrsm.get_connector(
        'sql', 'route_' + uuid.uuid4().hex, flavor='sqlite', database=str(tmp_path / 'g.db'),
    )
    core_connector = conn._core_connector()
    assert core_connector is conn._core_connector()
    method = conn._rust_method('pipe_exists')
    assert method is not None
    assert conn.__dict__['_rust_connector'] is core_connector
    assert method.__self__ is core_connector
    for name in ('read', 'no_such_method'):  # raw SQL is `sql_*` (`_native.py`), not `read`
        with pytest.raises(AttributeError, match=f"no method '{name}'"):
            conn._rust_method(name)
    assert '_rust_connector' not in conn.__getstate__()

    memory = mrsm.get_connector('sql', 'route_mem_' + uuid.uuid4().hex, flavor='sqlite',
                                database=':memory:')
    ### An in-memory SQLite connector is routed too. The Rust pool and SQLAlchemy
    ### open one database file (`_shared_memory_database`), so both see the same tables.
    assert memory._rust_method('pipe_exists') is not None
    path = memory._shared_memory_database()
    assert path and memory.URI == f'sqlite:///{path}'
    memory.exec('CREATE TABLE route_shared (a INTEGER)')
    memory.exec('INSERT INTO route_shared VALUES (5)')
    pipe = mrsm.Pipe(
        'route', 'shared', instance=memory, parameters={'target': 'route_shared'},
    )
    assert pipe.get_rowcount() == 1
    assert int(memory.value('SELECT COUNT(*) FROM route_shared')) == 1

    ### A connector with no usable driver raises `RuntimeError` naming the cause.
    import meerschaum.connectors.sql._SQLConnector  # noqa: F401
    monkeypatch.setattr(
        sys.modules['meerschaum.connectors.sql._SQLConnector'],
        '_oracle_client_available',
        lambda: False,
    )
    oracle_conn = mrsm.get_connector(
        'sql', 'route_ora_' + uuid.uuid4().hex, flavor='oracle',
        host='localhost', port=1521, username='u', password='p', database='x',
    )
    for call in (oracle_conn._core_connector, lambda: oracle_conn._rust_method('pipe_exists')):
        with pytest.raises(RuntimeError, match='libclntsh'):
            call()


# ---------------------------------------------------------------------------------------------
# Results recorded from v4
# ---------------------------------------------------------------------------------------------

def _snapshot(df):
    """Plain, comparable data of a pandas frame: labels, dtypes, index and every typed cell."""
    return {
        'columns': [str(c) for c in df.columns],
        'dtypes': [str(t) for t in df.dtypes],
        'index_dtype': str(df.index.dtype),
        'index': [repr(i) for i in df.index.tolist()],
        'cells': [[(type(v).__name__, repr(v)) for v in df[c].tolist()] for c in df.columns],
    }


@pytest.mark.parametrize('infer_string', [False, True])
@pytest.mark.parametrize('old_dtype,new_dtype', [
    (None, None),
    ('str', 'str'),
    ('str', 'string'),
    ('string', 'str'),
    ('string', 'string'),
    ('string[python]', 'string[python]'),
    ('string[pyarrow]', 'string[pyarrow]'),
])
@pytest.mark.parametrize('values', [(None,), ('b',), (None, 'b'), ('a',)])
def test_filter_unseen_string_merge_dtypes(infer_string, old_dtype, new_dtype, values):
    """NaN-backed strings merge differently from nullable strings, even for null deltas."""
    pd = pytest.importorskip('pandas')
    pytest.importorskip('pyarrow')
    from meerschaum.utils.dataframe import filter_unseen_df

    # pandas 2.3 exposes pandas 3's inferred string dtype through this option, so the
    # standalone suite covers it without depending on which package earlier tests import.
    with pd.option_context('future.infer_string', infer_string):
        old = pd.DataFrame({'s': pd.Series(['a'], dtype=old_dtype)})
        new = pd.DataFrame({'s': pd.Series(values, dtype=new_dtype)})
        old.index = [42]
        new.index = range(100, 100 + len(new))
        expected = oracle.expected()
        result = filter_unseen_df(old, new)
        actual = _snapshot(result)

    assert actual == expected
    if expected['dtypes'] == ['object']:
        for actual_value, (type_name, _) in zip(result['s'], expected['cells'][0]):
            if type_name == 'NAType':
                assert actual_value is pd.NA
    if old_dtype == new_dtype == 'string':
        assert str(result['s'].dtype) == 'string'


@pytest.mark.parametrize('infer_string', [False, True])
def test_filter_unseen_mixed_object_string_nulls(infer_string):
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df

    with pd.option_context('future.infer_string', infer_string):
        old = pd.DataFrame({'s': pd.Series(['a', 1], dtype=object)})
        new = pd.DataFrame({'s': pd.Series([None, 'b', 2], dtype=object)})
        expected = oracle.expected()
        result = filter_unseen_df(old, new)
        actual = _snapshot(result)

    assert actual == expected
    assert result['s'].iloc[0] is pd.NA
    assert result['s'].tolist()[1:] == ['b', 2]


@pytest.mark.parametrize('declare_json', [False, True])
def test_filter_unseen_object_string_keeps_special_nulls(declare_json):
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df

    with pd.option_context('future.infer_string', False):
        old = pd.DataFrame({
            's': pd.Series(['a'], dtype=object),
            'j': pd.Series([{'x': 1}], dtype=object),
            'n': pd.Series([decimal.Decimal('1.1')], dtype=object),
        })
        new = pd.DataFrame({
            's': pd.Series([None, 'b'], dtype=object),
            'j': pd.Series([None, {'x': 2}], dtype=object),
            'n': pd.Series([None, decimal.Decimal('2.2')], dtype=object),
        })
        options = {'dtypes': {'s': 'object', 'j': 'json', 'n': 'object'}} if declare_json else {}
        expected = oracle.expected()
        result = filter_unseen_df(old, new, **options)
        actual = _snapshot(result)

    assert actual == expected
    assert result['s'].iloc[0] is pd.NA
    assert result['j'].iloc[0] is None
    assert result['n'].iloc[0].is_nan()


def test_filter_unseen_declared_json_encoded_strings():
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df

    with pd.option_context('future.infer_string', False):
        old = pd.DataFrame({
            's': pd.Series(['a'], dtype=object),
            'j': pd.Series(['{"x":1}'], dtype=object),
        })
        new = pd.DataFrame({
            's': pd.Series([None, 'b'], dtype=object),
            'j': pd.Series([None, '{"x":2}'], dtype=object),
        })
        options = {'dtypes': {'s': 'object', 'j': 'json'}}
        expected = oracle.expected()
        result = filter_unseen_df(old, new, **options)
        actual = _snapshot(result)

    assert actual == expected
    assert result['s'].iloc[0] is pd.NA
    assert result['j'].iloc[0] is pd.NA
    assert result['j'].iloc[1] == '{"x":2}'


def test_filter_unseen_polars_object_null_stays_none():
    pl = _polars()
    old = pl.DataFrame({'s': pl.Series(['a'], dtype=pl.Object)})
    new = pl.DataFrame({'s': pl.Series([None], dtype=pl.Object)})
    actual = core.filter_unseen_df(old, new)
    assert isinstance(actual, pl.DataFrame)
    assert actual.shape == (1, 1)
    assert actual.columns == ['s']
    assert actual['s'][0] is None


def test_core_matches_v4_results():
    """Every bound function returns the values and pandas dtypes the recorded Python mode returned."""
    pd = pytest.importorskip('pandas')
    from meerschaum.utils import dataframe as dfu
    from meerschaum.utils.dtypes import (
        are_dtypes_equal, to_pandas_dtype, is_dtype_numeric, get_geometry_type_srid,
    )
    from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type
    from meerschaum.utils.pipes import replace_pipes_syntax
    from meerschaum.config import get_config
    from meerschaum.core.User._User import hash_password, verify_password

    mixed = pd.DataFrame({
        'dt': [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)],
        'id': [1, 2],
        'v': [1.5, None],
        's': ['a', None],
        'j': [{'a': 1}, None],
        'n': [decimal.Decimal('1.1'), decimal.Decimal('2')],
        'u': [uuid.UUID(int=1), uuid.UUID(int=2)],
        'b': [b'x', b'y'],
    })

    def frame(df):
        return (df.dtypes.astype(str).to_dict(), list(df.index), df.astype(object).where(df.notna(), None).to_dict('records'))

    def run_all():
        old = pd.DataFrame({'id': [1, 2], 'v': [1.0, 2.0]})
        new = pd.DataFrame({'id': [2, 3], 'v': [2.0, 3.0]})
        return (
            frame(dfu.filter_unseen_df(old, new)),
            frame(dfu.filter_unseen_df(mixed.iloc[:1], mixed)),
            frame(dfu.enforce_dtypes(pd.DataFrame({'a': ['1', '2']}), {'a': 'int'})),
            frame(dfu.enforce_dtypes(mixed, {'dt': 'datetime', 'id': 'int', 'v': 'float', 'n': 'numeric'})),
            frame(dfu.query_df(new, {'id': 3})),
            frame(dfu.query_df(mixed, {'id': [1]})),
            frame(dfu.query_df(mixed, begin=dt.datetime(2024, 1, 2), datetime_column='dt')),
            dfu.get_json_cols(mixed), dfu.get_numeric_cols(mixed), dfu.get_uuid_cols(mixed),
            dfu.get_bytes_cols(mixed), dfu.get_datetime_cols(mixed),
            dfu.get_datetime_cols(mixed, with_tz_precision=True), dfu.get_geometry_cols(mixed),
            are_dtypes_equal('int', 'Int64'),
            are_dtypes_equal('datetime', 'string'),
            are_dtypes_equal({'a': 'int'}, {'a': 'int64'}),
            [to_pandas_dtype(d) for d in ('datetime', 'int', 'numeric[10,2]', 'BIGINT', 'str')],
            is_dtype_numeric('float64'), is_dtype_numeric('string'),
            get_geometry_type_srid('geometry[Point, 4326]'),
            get_geometry_type_srid('geometry[POINT, ESRI:102003]'),
            get_pd_type_from_db_type('TIMESTAMPTZ'), get_pd_type_from_db_type('NUMERIC(10, 2)'),
            replace_pipes_syntax('no pipes here'),
            replace_pipes_syntax("SELECT * FROM {{ Pipe('a', 'b', instance='sql:local') }}"),
            get_config('meerschaum', 'instance', warn=False),
            get_config('does', 'not', 'exist', warn=False),
            hash_password('pw', salt=b'0123456789abcdef', rounds=1000),
            verify_password('pw', hash_password('pw', rounds=1000)),
        )

    expected = oracle.expected()
    actual = run_all()
    assert len(expected) == len(actual)
    for i, (e, a) in enumerate(zip(expected, actual)):
        assert e == a, f"item {i}: v4={e!r} rust={a!r}"


def test_out_of_range_floats_do_not_saturate():
    """
    Integral floats outside int64 (`1e20`, `-1e20`, `2**63`, `2**64`) keep their values in float,
    numeric and untyped columns, and a failed int cast leaves them alone, as in v4 (they used to
    saturate to `i64::MAX` / `i64::MIN`).
    """
    pd = pytest.importorskip('pandas')
    from meerschaum.utils import dataframe as dfu

    values = [1e20, -1e20, float(2**63), float(2**64)]

    def frame(df):
        return (
            {str(c): str(t) for c, t in df.dtypes.items()},
            [repr(v) for v in df[df.columns[0]].tolist()],
        )

    def run_all():
        out = []
        for typ in ('float', 'numeric', 'int', None):
            dtypes = {'a': typ} if typ else {}
            for series in (
                pd.Series(values),
                pd.Series(values, dtype=object),
                pd.Series([1] + values, dtype=object),
            ):
                out.append(frame(dfu.enforce_dtypes(pd.DataFrame({'a': series}), dtypes)))
        new = pd.DataFrame({'a': values})
        out.append(frame(dfu.filter_unseen_df(pd.DataFrame({'a': [1.5]}), new)))
        out.append(frame(dfu.filter_unseen_df(pd.DataFrame({'a': [1]}), new)))
        out.append(frame(dfu.query_df(new, {'a': values[0]})))
        return out

    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        expected = oracle.expected()
        actual = run_all()
    assert len(expected) == len(actual)
    for i, (e, a) in enumerate(zip(expected, actual)):
        assert e == a, f"item {i}: v4={e!r} rust={a!r}"
    assert "'9223372036854775807'" not in repr(actual)
