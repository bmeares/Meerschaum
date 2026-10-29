#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The DataFrame library setting (`meerschaum:dataframe`, `MRSM_DATAFRAME`; rc15 Q54).

- a fresh root defaults to Polars (its `meerschaum.yaml` is written with `dataframe: polars`) and is
  never mistaken for an old root on its second run;
- a Meerschaum 4.1-shaped root and an rc14-shaped root get `dataframe: pandas` once, with a
  one-line notice, from the Python boundary and from the CLI;
- `MRSM_DATAFRAME` overrides the setting, `as_polars` / `as_pandas` override both per call;
- every frame-returning API follows it, transforms return the library they were given, geometry
  comes back as WKB + CRS (Polars) or a GeoDataFrame (pandas), chunk hooks and custom instance
  connectors are handed the configured library, and a missing library names its extra.

The suite pins `MRSM_DATAFRAME=pandas` (`tests/conftest.py`); these tests set it per test.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
from datetime import datetime, timezone
from uuid import uuid4

import pytest

import meerschaum as mrsm
from meerschaum.utils.dataframe import (
    frame_from_arrow,
    frame_library_of,
    get_dataframe_library,
    resolve_dataframe_library,
    ensure_dataframe_library,
)
from tests.connectors import conns, get_flavors

REPO = pathlib.Path(__file__).resolve().parent.parent

### `meerschaum.yaml` as Meerschaum 4.1 wrote it (with a comment the user added).
V4_MEERSCHAUM_YAML = """
#####################################################################
#                                                                   #
#  Edit or add credentials for connectors.                          #
#  You can read more about connectors at https://meerschaum.io.     #
#                                                                   #
#  Connectors inherit from `default`, and flavor-dependent defaults #
#  are defined for SQL connectors (e.g. port 5432 for PostgreSQL).  #
#                                                                   #
#####################################################################

instance: sql:local

api_instance: MRSM{meerschaum:instance}

repository: api:mrsm

connectors:
  sql:
    default: {}
    # My database (a comment the pin must keep).
    local:
      flavor: sqlite
      database: '{SQLITE_DB_PATH}'
  api:
    default:
      protocol: http
"""

### `meerschaum.yaml` as 5.0.0rc14 wrote it: v4's keys (rc14 had no `dataframe` key), with
### `executor` and the rc14 connectors.
RC14_MEERSCHAUM_YAML = V4_MEERSCHAUM_YAML.replace(
    "repository: api:mrsm\n",
    "repository: api:mrsm\n\nexecutor: local\n",
) + """  valkey:
    main:
      host: localhost
      username: default
      password: mrsm
      port: 6379
"""

LIBRARY_PROBE = (
    "from meerschaum.utils.dataframe import get_dataframe_library; "
    "print('LIB=' + get_dataframe_library())"
)

PIN_NOTICE = "added `dataframe: pandas`"


def _env(root: pathlib.Path, **extra: str) -> dict:
    """An isolated environment: no inherited `MRSM_*` (so no `MRSM_DATAFRAME` either)."""
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
    })
    env.update(extra)
    return env


def _python(root: pathlib.Path, code: str, **extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-c', code],
        env=_env(root, **extra),
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=300,
    )


def _new_root(tmp_path: pathlib.Path, name: str = 'root') -> pathlib.Path:
    root = tmp_path / name
    (root / 'plugins').mkdir(parents=True)
    return root


def _old_root(tmp_path: pathlib.Path, meerschaum_yaml: str, name: str = 'old') -> pathlib.Path:
    """A root an earlier release created: config files, a venvs and a jobs directory."""
    root = _new_root(tmp_path, name)
    config = root / 'config'
    config.mkdir()
    (config / 'meerschaum.yaml').write_text(meerschaum_yaml, encoding='utf-8')
    (config / 'system.json').write_text('{"connectors": {"sql": {"chunksize": 100000}}}')
    (root / 'venvs' / 'mrsm').mkdir(parents=True)
    (root / 'jobs').mkdir()
    return root


def _library(df) -> str:
    library = frame_library_of(df)
    assert library is not None, f"Not a DataFrame: {type(df)}"
    return library


def _ids(df) -> list:
    return df['id'].to_list() if frame_library_of(df) == 'polars' else df['id'].tolist()


### -------------------------------------------------------------------------------------------
### The root: fresh roots, old roots, the override
### -------------------------------------------------------------------------------------------

def test_fresh_root_defaults_to_polars(tmp_path):
    """A fresh root writes `dataframe: polars` and is never pinned on its second run."""
    root = _new_root(tmp_path)
    first = _python(root, LIBRARY_PROBE)
    assert first.returncode == 0, first.stderr
    assert 'LIB=polars' in first.stdout
    assert PIN_NOTICE not in first.stderr
    text = (root / 'config' / 'meerschaum.yaml').read_text(encoding='utf-8')
    assert 'dataframe: polars' in text
    second = _python(root, LIBRARY_PROBE)
    assert second.returncode == 0, second.stderr
    assert 'LIB=polars' in second.stdout
    assert PIN_NOTICE not in second.stderr
    assert (root / 'config' / 'meerschaum.yaml').read_text(encoding='utf-8') == text


@pytest.mark.parametrize('shape', ['4.1', 'rc14'])
def test_old_root_is_pinned_to_pandas_once(tmp_path, shape):
    """A 4.1 or rc14 root gets `dataframe: pandas` appended once, with a one-line notice."""
    original = V4_MEERSCHAUM_YAML if shape == '4.1' else RC14_MEERSCHAUM_YAML
    root = _old_root(tmp_path, original)
    path = root / 'config' / 'meerschaum.yaml'

    first = _python(root, LIBRARY_PROBE)
    assert first.returncode == 0, first.stderr
    assert 'LIB=pandas' in first.stdout
    notices = [line for line in first.stderr.splitlines() if PIN_NOTICE in line]
    assert len(notices) == 1, first.stderr
    assert 'polars' in notices[0]

    text = path.read_text(encoding='utf-8')
    ### Every byte of the user's file is kept (comments included); one line is appended.
    assert text.startswith(original)
    assert text[len(original):].strip() == 'dataframe: pandas'

    second = _python(root, LIBRARY_PROBE)
    assert second.returncode == 0, second.stderr
    assert 'LIB=pandas' in second.stdout
    assert PIN_NOTICE not in second.stderr
    assert path.read_text(encoding='utf-8') == text


def test_old_root_is_pinned_by_the_cli(tmp_path):
    """The command line pins an old root before it reads the config (the Python CLI too)."""
    root = _old_root(tmp_path, V4_MEERSCHAUM_YAML)
    proc = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'show', 'version'],
        env=_env(root),
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    assert PIN_NOTICE in proc.stderr
    text = (root / 'config' / 'meerschaum.yaml').read_text(encoding='utf-8')
    assert text.rstrip().endswith('dataframe: pandas')
    after = _python(root, LIBRARY_PROBE)
    assert 'LIB=pandas' in after.stdout
    assert PIN_NOTICE not in after.stderr


def test_old_root_with_an_explicit_setting_is_respected(tmp_path):
    """An old root whose user already chose (`dataframe: polars`) is not pinned."""
    original = V4_MEERSCHAUM_YAML + "\ndataframe: polars\n"
    root = _old_root(tmp_path, original)
    proc = _python(root, LIBRARY_PROBE)
    assert proc.returncode == 0, proc.stderr
    assert 'LIB=polars' in proc.stdout
    assert PIN_NOTICE not in proc.stderr
    assert (root / 'config' / 'meerschaum.yaml').read_text(encoding='utf-8') == original


def test_environment_overrides_the_setting(tmp_path):
    """`MRSM_DATAFRAME` wins over `meerschaum:dataframe` (an old root's pin included)."""
    root = _old_root(tmp_path, V4_MEERSCHAUM_YAML)
    probe = "import meerschaum as mrsm; mrsm.get_config('meerschaum', 'instance'); " + LIBRARY_PROBE
    proc = _python(root, probe, MRSM_DATAFRAME='polars')
    assert proc.returncode == 0, proc.stderr
    assert 'LIB=polars' in proc.stdout
    ### The root is still pinned for the processes without the variable.
    assert 'dataframe: pandas' in (root / 'config' / 'meerschaum.yaml').read_text()
    assert 'LIB=pandas' in _python(root, LIBRARY_PROBE).stdout


def test_get_dataframe_library_env(monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', 'polars')
    assert get_dataframe_library() == 'polars'
    monkeypatch.setenv('MRSM_DATAFRAME', ' Pandas ')
    assert get_dataframe_library() == 'pandas'
    monkeypatch.setenv('MRSM_DATAFRAME', 'arrow')
    with pytest.raises(ValueError, match='MRSM_DATAFRAME'):
        get_dataframe_library()


@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_resolve_dataframe_library(monkeypatch, setting):
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    assert resolve_dataframe_library() == setting
    assert resolve_dataframe_library(as_polars=True) == 'polars'
    assert resolve_dataframe_library(as_polars=False) == 'pandas'
    assert resolve_dataframe_library(as_pandas=True) == 'pandas'
    assert resolve_dataframe_library(as_pandas=False) == 'polars'
    assert resolve_dataframe_library(as_polars=True, as_pandas=False) == 'polars'
    with pytest.raises(ValueError):
        resolve_dataframe_library(as_polars=True, as_pandas=True)
    with pytest.raises(ValueError):
        resolve_dataframe_library(as_polars=False, as_pandas=False)


### -------------------------------------------------------------------------------------------
### Every frame-returning API follows the setting
### -------------------------------------------------------------------------------------------

def _synced_pipe(conn, metric: str):
    pipe = mrsm.Pipe(
        'r3', metric, uuid4().hex[:8],
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'meta': 'json', 'val': 'float'},
    )
    success, msg = pipe.sync([
        {'dt': datetime(2026, 1, 1), 'id': 1, 'val': 1.5, 'meta': {'a': 1}},
        {'dt': datetime(2026, 1, 2), 'id': 2, 'val': None, 'meta': None},
    ])
    assert success, msg
    return pipe


@pytest.mark.parametrize('flavor', get_flavors())
@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_reads_follow_the_setting(flavor, setting, monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    pipe = _synced_pipe(conns[flavor], 'reads')
    try:
        df = pipe.get_data()
        assert _library(df) == setting
        assert sorted(_ids(df)) == [1, 2]

        chunks = list(pipe.get_data(as_iterator=True))
        assert chunks and all(_library(chunk) == setting for chunk in chunks)
        assert all(_library(chunk) == setting for chunk in pipe.get_data(as_chunks=True))

        backtrack = pipe.get_backtrack_data(begin=datetime(2026, 1, 3), backtrack_minutes=60 * 72)
        assert _library(backtrack) == setting

        ### Per call, whatever the setting.
        assert _library(pipe.get_data(as_polars=True)) == 'polars'
        assert _library(pipe.get_data(as_pandas=True)) == 'pandas'
        assert _library(pipe.get_data(as_polars=False)) == 'pandas'
        assert _library(pipe.get_data(as_pandas=False)) == 'polars'
        assert _library(pipe.get_backtrack_data(as_pandas=True)) == 'pandas'
        assert _library(pipe.get_backtrack_data(as_polars=True)) == 'polars'
        assert all(
            _library(chunk) == 'pandas'
            for chunk in pipe.get_data(as_iterator=True, as_pandas=True)
        )
        with pytest.raises(ValueError):
            pipe.get_data(as_polars=True, as_pandas=True)
        with pytest.raises(ValueError):
            pipe.get_data(as_pandas=True, as_docs=True)

        ### Documents need no library.
        docs = pipe.get_data(as_docs=True)
        assert isinstance(docs, list) and len(docs) == 2
    finally:
        pipe.delete()


@pytest.mark.parametrize('flavor', get_flavors())
@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_fetch_and_chunk_hooks_follow_the_setting(flavor, setting, monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    conn = conns[flavor]
    if conn.type != 'sql':
        pytest.skip("A SQL source is needed.")
    definition = 'SELECT 1 AS id, 2 AS val' + (' FROM DUAL' if conn.flavor == 'oracle' else '')
    pipe = mrsm.Pipe(
        str(conn), 'r3_fetch', uuid4().hex[:8],
        instance=conn,
        temporary=True,
        parameters={'fetch': {'definition': definition}},
    )
    fetched = pipe.fetch(begin=None, end=None, check_existing=False)
    chunks = [fetched] if frame_library_of(fetched) else list(fetched)
    assert chunks and all(_library(chunk) == setting for chunk in chunks)

    fetched = pipe.fetch(begin=None, end=None, check_existing=False, as_pandas=True)
    chunks = [fetched] if frame_library_of(fetched) else list(fetched)
    assert all(_library(chunk) == 'pandas' for chunk in chunks)


@pytest.fixture
def plugin_source():
    """The `w15l_plugsrc` test plugin (a Python `fetch()` source) with its venv."""
    from meerschaum.plugins import _plugins_import_errors, reload_plugins
    from meerschaum.utils.venv import init_venv
    init_venv('w15l_plugsrc', verify=False)
    _plugins_import_errors.pop('w15l_plugsrc', None)
    reload_plugins(['w15l_plugsrc'])
    return 'plugin:w15l_plugsrc'


@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_plugin_fetch_and_chunk_hooks_follow_the_setting(setting, monkeypatch, plugin_source):
    """A Python source's documents and the chunks handed to `chunk_hook` follow the setting."""
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    conn = conns['sqlite'] if 'sqlite' in conns else conns[get_flavors()[0]]
    pipe = mrsm.Pipe(plugin_source, 'r3_hook', uuid4().hex[:8], instance=conn, temporary=True)
    fetched = pipe.fetch(begin=None, end=None, check_existing=False)
    assert _library(fetched) == setting
    assert len(fetched) == 12

    hooked = mrsm.Pipe(
        plugin_source, 'r3_hook', uuid4().hex[:8],
        instance=conn,
        temporary=True,
        parameters={'fetch': {'mode': 'generator'}},
    )
    ### The caller's hook runs on the host, once for every chunk the source's generator yields.
    seen = []

    def hook(chunk, **kwargs):
        seen.append(frame_library_of(chunk))
        return True, "Success"

    results = hooked.fetch(begin=None, end=None, check_existing=False, chunk_hook=hook)
    assert results and all(result[0] for result in results)
    assert seen and set(seen) == {setting}


@pytest.mark.parametrize('flavor', get_flavors())
@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_transforms_return_the_library_they_were_given(flavor, setting, monkeypatch):
    pd = pytest.importorskip('pandas')
    pl = pytest.importorskip('polars')
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    pipe = _synced_pipe(conns[flavor], 'transforms')
    rows = [{'dt': datetime(2026, 1, 2), 'id': 2, 'val': 2.5, 'meta': {'b': 2}}]
    try:
        assert _library(pipe.enforce_dtypes(pl.DataFrame(rows))) == 'polars'
        assert _library(pipe.enforce_dtypes(pd.DataFrame(rows))) == 'pandas'
        assert _library(pipe.enforce_dtypes(rows)) == setting
        assert _library(pipe.enforce_dtypes(pl.DataFrame(rows), as_pandas=True)) == 'pandas'

        for frames, expected in [
            (pipe.filter_existing(pl.DataFrame(rows)), 'polars'),
            (pipe.filter_existing(pd.DataFrame(rows)), 'pandas'),
            (pipe.filter_existing(rows), setting),
        ]:
            assert len(frames) == 3
            assert {_library(frame) for frame in frames} == {expected}

        from meerschaum.utils.dataframe import enforce_dtypes
        assert _library(enforce_dtypes(pl.DataFrame({'a': [1]}), {'a': 'float'})) == 'polars'
        assert _library(enforce_dtypes(pd.DataFrame({'a': [1]}), {'a': 'float'})) == 'pandas'
        assert _library(
            enforce_dtypes(pl.DataFrame({'a': [1]}), {'a': 'float'}, as_pandas=True)
        ) == 'pandas'
    finally:
        pipe.delete()


@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_plan_to_dataframe_follows_the_setting(setting, monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    pipe = _synced_pipe(conns['sqlite'] if 'sqlite' in conns else conns[get_flavors()[0]], 'plan')
    try:
        plan = mrsm.Pipeline(pipe).plan()
        df = plan.to_dataframe()
        assert _library(df) == setting
        assert list(df.columns) == ['node', 'action', 'bounds', 'chunks', 'reason']
        assert _library(plan.to_dataframe(as_pandas=True)) == 'pandas'
        assert _library(plan.to_dataframe(as_polars=True)) == 'polars'
    finally:
        pipe.delete()


@pytest.mark.parametrize('flavor', [f for f in get_flavors() if f not in ('api', 'valkey')])
@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_sql_read_follows_the_setting(flavor, setting, monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    conn = conns[flavor]
    query = 'SELECT 1 AS id' + (' FROM DUAL' if conn.flavor == 'oracle' else '')
    df = conn.read(query)
    assert _library(df) == setting
    assert _ids(df) == [1]
    assert _library(conn.read(query, as_pandas=True)) == 'pandas'
    assert _library(conn.read(query, as_polars=True)) == 'polars'


### -------------------------------------------------------------------------------------------
### The conversion helper, geometry, custom connectors, a missing library
### -------------------------------------------------------------------------------------------

@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_frame_from_arrow(setting, monkeypatch):
    pa = pytest.importorskip('pyarrow')
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    table = pa.table({
        'id': pa.array([1, None], pa.int64()),
        'ts': pa.array(
            [datetime(2026, 1, 1, tzinfo=timezone.utc), None],
            pa.timestamp('us', tz='UTC'),
        ),
        'name': ['a', None],
    })
    df = frame_from_arrow(table)
    assert _library(df) == setting
    if setting == 'pandas':
        assert str(df['id'].dtype) == 'Int64'
        assert str(df['ts'].dtype) == 'datetime64[us, UTC]'
        ### Hints restore a backend within the column's kind.
        hinted = frame_from_arrow(table, dtypes={'id': 'int64[pyarrow]'})
        assert str(hinted['id'].dtype) == 'int64[pyarrow]'
    else:
        assert str(df.schema['id']) == 'Int64'
    ### Batches, an empty stream, an Arrow stream from the extension, frames.
    assert len(frame_from_arrow(table.to_batches())) == 2
    empty = frame_from_arrow(table.schema.empty_table())
    assert len(empty) == 0 and list(empty.columns) == ['id', 'ts', 'name']
    from meerschaum import _core
    assert len(frame_from_arrow(_core.roundtrip_arrow(table))) == 2
    assert frame_from_arrow(None) is None
    pandas_df = frame_from_arrow(table, as_pandas=True)
    assert _library(frame_from_arrow(pandas_df, as_polars=True)) == 'polars'
    assert frame_from_arrow(pandas_df, as_pandas=True) is pandas_df


@pytest.mark.parametrize('flavor', [f for f in get_flavors() if f in ('sqlite', 'postgis', 'timescaledb_ha')])
def test_geometry_both_ways(flavor, monkeypatch):
    gpd = pytest.importorskip('geopandas')
    shapely = pytest.importorskip('shapely')
    pipe = mrsm.Pipe(
        'r3', 'geometry', uuid4().hex[:8],
        instance=conns[flavor],
        columns={'id': 'id'},
        dtypes={'geom': 'geometry[Point, 4326]'},
    )
    try:
        success, msg = pipe.sync(gpd.GeoDataFrame(
            {'id': [1, 2], 'geom': [shapely.Point(1, 2), shapely.Point(3, 4)]},
            geometry='geom', crs='EPSG:4326',
        ))
        assert success, msg
        monkeypatch.setenv('MRSM_DATAFRAME', 'polars')
        df = pipe.get_data()
        assert _library(df) == 'polars'
        geom = df['geom']
        values = geom.ext.storage().to_list() if hasattr(geom, 'ext') else geom.to_list()
        assert shapely.from_wkb(values[0]).equals(shapely.Point(1, 2))
        assert '4326' in str(geom.dtype)
        gdf = pipe.get_data(as_pandas=True)
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert gdf.crs is not None and gdf.crs.to_epsg() == 4326
    finally:
        pipe.delete()


@pytest.mark.parametrize('setting', ['polars', 'pandas'])
def test_custom_instance_connectors_get_the_setting(setting, monkeypatch):
    """The worker hands a custom instance connector's `sync_pipe(pipe, df)` the configured library."""
    pytest.importorskip('pyarrow')
    pd = pytest.importorskip('pandas')
    from meerschaum.worker._arrow import df_to_ipc, ipc_to_df
    monkeypatch.setenv('MRSM_DATAFRAME', setting)
    data = df_to_ipc(pd.DataFrame({'id': [1, 2], 'meta': [{'a': 1}, None]}))
    df = ipc_to_df(data)
    assert _library(df) == setting
    assert _ids(df) == [1, 2]


def test_missing_library_names_the_extra(monkeypatch):
    monkeypatch.setenv('MRSM_DATAFRAME', 'polars')
    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    monkeypatch.setitem(sys.modules, 'polars', None)
    with pytest.raises(ImportError, match=r'meerschaum\[polars\]'):
        ensure_dataframe_library()
    with pytest.raises(ImportError, match=r'meerschaum\[polars\]'):
        mrsm.Pipe('r3', 'missing', instance='sql:memory').get_data()


POLARS_ONLY = r'''
import sys
sys.modules['pandas'] = None          # `import pandas` raises ImportError
from datetime import datetime
import polars as pl
import meerschaum as mrsm

conn = mrsm.get_connector('sql', 'polars_only', flavor='sqlite', database=sys.argv[1])
pipe = mrsm.Pipe('r3', 'polars_only', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
rows = pl.DataFrame({'dt': [datetime(2026, 1, 1), datetime(2026, 1, 2)], 'id': [1, 2], 'val': [1.5, None]})
success, msg = pipe.sync(rows)
assert success, msg
df = pipe.get_data()
assert type(df).__module__.startswith('polars'), type(df)
assert df['id'].to_list() == [1, 2]
assert all(type(c).__module__.startswith('polars') for c in pipe.get_data(as_iterator=True))
assert type(pipe.get_backtrack_data(begin=datetime(2026, 1, 3), backtrack_minutes=4320)).__module__.startswith('polars')
assert type(pipe.enforce_dtypes(rows)).__module__.startswith('polars')
frames = pipe.filter_existing(rows)
assert all(type(f).__module__.startswith('polars') for f in frames)
plan = mrsm.Pipeline(pipe).plan()
assert type(plan.to_dataframe()).__module__.startswith('polars')
source = mrsm.Pipe(str(conn), 'polars_only_fetch', instance=conn, temporary=True,
                   parameters={'fetch': {'definition': 'SELECT 1 AS id'}})
fetched = source.fetch(begin=None, end=None, check_existing=False)
chunks = [fetched] if hasattr(fetched, 'columns') else list(fetched)
assert all(type(c).__module__.startswith('polars') for c in chunks)
success, msg = conn.to_sql(rows, 'polars_only_raw', as_tuple=True)
assert success, msg
raw = conn.read('SELECT * FROM polars_only_raw ORDER BY id')
assert type(raw).__module__.startswith('polars') and raw['id'].to_list() == [1, 2]
assert 'pandas' not in sys.modules or sys.modules['pandas'] is None
print('POLARS_ONLY_OK')
'''


def test_polars_only_install_never_needs_pandas(tmp_path):
    """The default (Polars) path syncs, reads, enforces, filters and fetches without pandas."""
    root = _new_root(tmp_path)
    proc = subprocess.run(
        [sys.executable, '-c', POLARS_ONLY, str(tmp_path / 'polars_only.db')],
        env=_env(root, MRSM_DATAFRAME='polars'),
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert 'POLARS_ONLY_OK' in proc.stdout
