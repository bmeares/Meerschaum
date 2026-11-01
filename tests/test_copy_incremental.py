"""
`copy pipes --sync-data <mode>` data selection, through the wheel's console path
(`python -m meerschaum`) against two SQLite instances in a temporary root. The data is synthetic.

`incremental` copies only each destination pipe's rows from its own sync time minus its own
backtrack window (filtered by `--params`, capped by `--begin` / `--end`); it never reads the
source's whole history into a destination that already has rows, and never rewrites the
destination's older rows. A destination without data is filled from the source (from `--begin`
when given). `all` keeps copying everything; bounds it would ignore are refused instead.
`MRSM_BIN` (a standalone `mrsm`) is passed through, so the same tests check a freshly built binary.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)
CK = 'sql:src'
INT_BASE_MS = 1_700_000_000_000


def _env(home: pathlib.Path) -> dict:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(home / 'root'),
        'MRSM_PLUGINS_DIR': str(home / 'plugins'),
        'MRSM_SQL_SRC': f"sqlite:///{home / 'src.db'}",
        'MRSM_SQL_DST': f"sqlite:///{home / 'dst.db'}",
        'MRSM_CONFIG': json.dumps({'shell': {'updates': {'check': False}}}),
        'MRSM_NO_UPDATE_CHECK': '1',
        'NO_COLOR': '1',
        'COLUMNS': '160',
    })
    if os.environ.get('MRSM_BIN'):
        env['MRSM_BIN'] = os.environ['MRSM_BIN']
    return env


def _mrsm(home: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        [sys.executable, '-P', '-m', 'meerschaum', *args],
        capture_output=True,
        text=True,
        env=_env(home),
        timeout=300,
        cwd=str(home),
    )
    result.stdout = ANSI.sub('', result.stdout)
    result.stderr = ANSI.sub('', result.stderr)
    return result


def _cli(home: pathlib.Path, *args: str) -> str:
    result = _mrsm(home, *args)
    assert result.returncode == 0, f"{args}:\n{result.stdout}\n{result.stderr}"
    return result.stdout + result.stderr


def _copy(home: pathlib.Path, *args: str, ok: bool = True) -> str:
    result = _mrsm(home, 'copy', 'pipes', 'sql:dst', '-i', 'sql:src', '-c', CK, *args)
    text = result.stdout + result.stderr
    assert (result.returncode == 0) == ok, f"{args}: exit {result.returncode}\n{text}"
    return text


_SEED = r'''
import json, sys
import meerschaum as mrsm
for spec in json.loads(sys.argv[1]):
    pipe = mrsm.Pipe(
        spec['ck'], spec['mk'], spec['lk'],
        instance=spec['instance'], parameters=spec['parameters'],
    )
    for docs in spec['batches']:
        success, message = pipe.sync(docs)
        assert success, message
'''


def _seed(home: pathlib.Path, *specs: dict) -> None:
    """Register and sync each pipe (one `sync` per batch) in a separate interpreter."""
    result = subprocess.run(
        [sys.executable, '-P', '-c', _SEED, json.dumps(list(specs))],
        capture_output=True, text=True, env=_env(home), timeout=300, cwd=str(home),
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _ts(i: int) -> str:
    return (BASE + timedelta(hours=i)).isoformat()


def _hourly(rows, station: str = 'x', overrides: dict | None = None) -> list:
    overrides = overrides or {}
    return [
        {'ts': _ts(i), 'station': station, 'n': i, 'val': overrides.get(i, i * 10)}
        for i in rows
    ]


def _hourly_params(**extra) -> dict:
    return {
        'columns': {'datetime': 'ts', 'id': 'station'},
        'dtypes': {'ts': 'datetime64[ns, UTC]', 'n': 'int', 'val': 'int'},
        **extra,
    }


def _spec(instance: str, lk: str, parameters: dict, *batches) -> dict:
    return {
        'ck': CK, 'mk': 'temp', 'lk': lk, 'instance': instance,
        'parameters': parameters, 'batches': list(batches),
    }


def _rows(home: pathlib.Path, db: str, lk: str, station: str | None = None) -> dict:
    """`{n: val}` of a pipe's target table (empty when the table does not exist)."""
    table = f"sql_src_temp_{lk}"
    with sqlite3.connect(home / f'{db}.db') as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        if not exists:
            return {}
        query = f'SELECT n, val FROM "{table}"'
        args: tuple = ()
        if station is not None:
            query += ' WHERE station = ?'
            args = (station,)
        return dict(connection.execute(query, args).fetchall())


def _parameters(home: pathlib.Path, db: str, lk: str) -> dict:
    with sqlite3.connect(home / f'{db}.db') as connection:
        row = connection.execute(
            "SELECT parameters FROM mrsm_pipes WHERE connector_keys = ? AND location_key = ?",
            (CK, lk),
        ).fetchone()
    assert row is not None
    return json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})


@pytest.fixture
def home(tmp_path):
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    return tmp_path


def test_incremental_starts_at_each_destination_watermark(home):
    """
    Per pipe: the destination's own sync time minus its own backtrack window. Older destination
    rows (one missing, one edited) stay exactly as they were.
    """
    # `a`: the default backtrack (1440 minutes = 24 rows). Destination: 0..149, without row 10,
    # row 20 edited.
    # `b`: `fetch.backtrack_minutes: 120` (2 rows). Destination: 0..99 with rows 50 and 98 edited;
    # only row 98 is inside the window [97, ...).
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(), _hourly(range(200))),
        _spec('sql:src', 'b', _hourly_params(fetch={'backtrack_minutes': 120}), _hourly(range(200))),
        _spec(
            'sql:dst', 'a', _hourly_params(),
            _hourly([i for i in range(150) if i != 10], overrides={20: -1}),
        ),
        _spec(
            'sql:dst', 'b', _hourly_params(fetch={'backtrack_minutes': 120}),
            _hourly(range(100), overrides={50: -1, 98: -1}),
        ),
    )
    _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental')

    a = _rows(home, 'dst', 'a')
    assert 10 not in a, 'an incremental copy rewrote history before the watermark'
    assert a[20] == -1, 'an incremental copy overwrote a row before the watermark'
    assert {n: a[n] for n in range(126, 200)} == {n: n * 10 for n in range(126, 200)}
    assert len(a) == 199

    b = _rows(home, 'dst', 'b')
    assert b[50] == -1, "the copy ignored the pipe's own backtrack_minutes"
    assert b[98] == 980, 'rows inside the backtrack window are copied again'
    assert {n: b[n] for n in range(100, 200)} == {n: n * 10 for n in range(100, 200)}
    assert len(b) == 200


def test_incremental_without_force_copies_data_and_keeps_attributes(home):
    """`-y` without `--force`: an existing destination keeps its attributes and gets new rows."""
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(tags=['source']), _hourly(range(60))),
        _spec('sql:dst', 'a', _hourly_params(tags=['mirror']), _hourly(range(40), overrides={1: -1})),
    )
    # Each side has its own lock, and the destination its own access.
    keys = ('-c', CK, '-m', 'temp', '-l', 'a')
    _cli(home, 'lock', 'pipes', *keys, '--locks', 'delete', '-i', 'sql:src')
    _cli(home, 'lock', 'pipes', *keys, '--locks', 'drop', '-i', 'sql:dst')
    _cli(home, 'edit', 'pipes', 'access', *keys, '--visibility', 'private', '--noask', '-i', 'sql:dst')
    before = _parameters(home, 'dst', 'a')
    assert before['locks'] == ['drop'] and before['access']['visibility'] == 'private'

    _copy(home, '-y', '--noask', '--sync-data', 'incremental')
    rows = _rows(home, 'dst', 'a')
    assert rows[1] == -1
    assert len(rows) == 60 and rows[59] == 590
    after = _parameters(home, 'dst', 'a')
    for key in ('tags', 'locks', 'access', 'columns', 'fetch'):
        assert after.get(key) == before.get(key), key

    # Other modes keep skipping existing pipes without `--force` (v4).
    _seed(home, _spec('sql:src', 'a', _hourly_params(tags=['source']), _hourly(range(60, 70))))
    text = _copy(home, '-y', '--noask', '--sync-data', 'all')
    assert 'skipped' in text
    assert len(_rows(home, 'dst', 'a')) == 60


def test_incremental_fills_an_empty_destination_within_explicit_bounds(home):
    """No watermark and no rows: the source's rows, from `--begin` and before `--end` if given."""
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(), _hourly(range(50))),
        _spec('sql:src', 'b', _hourly_params(), _hourly(range(50))),
    )
    _copy(home, '-y', '--noask', '--sync-data', 'incremental', '-l', 'a')
    assert len(_rows(home, 'dst', 'a')) == 50

    _copy(
        home, '-y', '--noask', '--sync-data', 'incremental', '-l', 'b',
        '--begin', _ts(30), '--end', _ts(40),
    )
    assert sorted(_rows(home, 'dst', 'b')) == list(range(30, 40))


def test_incremental_bounds_and_params(home):
    """
    `--params` selects both the rows and the watermark (each station has its own); `--end` caps
    the window; `--begin` only raises the watermark.
    """
    source = _hourly(range(200), 'x') + _hourly(range(200), 'y')
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(), source),
        _spec(
            'sql:dst', 'a', _hourly_params(),
            _hourly(range(150), 'x') + _hourly(range(100), 'y', overrides={70: -1}),
        ),
    )
    _copy(
        home, '-f', '-y', '--noask', '--sync-data', 'incremental',
        '--params', json.dumps({'station': 'y'}), '--end', _ts(180),
    )
    y = _rows(home, 'dst', 'a', 'y')
    x = _rows(home, 'dst', 'a', 'x')
    assert sorted(x) == list(range(150)), 'rows outside --params were copied'
    assert y[70] == -1
    assert sorted(y) == list(range(180)), 'the watermark of station y is its own'

    # `--begin` after the watermark raises it; it never lowers it.
    _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental', '--begin', _ts(190))
    x = _rows(home, 'dst', 'a', 'x')
    assert sorted(x) == list(range(150)) + list(range(190, 200))
    _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental', '--begin', _ts(0))
    assert _rows(home, 'dst', 'a', 'y')[70] == -1


def test_an_empty_incremental_window_reads_nothing(home):
    """`--end` before the watermark: no source read and no write, and the copy succeeds."""
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(), _hourly(range(200))),
        _spec('sql:dst', 'a', _hourly_params(), _hourly(range(100), overrides={90: -1})),
    )
    # Watermark: row 99 minus 24 hours = row 75, after --end.
    text = _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental', '--end', _ts(50))
    assert 'No new rows to copy' in text, text
    assert 'Fetching data from' not in text, text
    rows = _rows(home, 'dst', 'a')
    assert len(rows) == 100 and rows[90] == -1


def test_incremental_integer_axis_uses_the_precision(home):
    """
    An integer axis in milliseconds: `backtrack_minutes: 1` is 60,000 axis units, not 1, and the
    bounds stay integers.
    """
    params = {
        'columns': {'datetime': 'ts_ms', 'id': 'station'},
        'dtypes': {'ts_ms': 'int', 'n': 'int', 'val': 'int'},
        'precision': 'ms',
        'fetch': {'backtrack_minutes': 1},
    }

    def docs(rows, overrides=None):
        overrides = overrides or {}
        return [
            {'ts_ms': INT_BASE_MS + i * 10_000, 'station': 'x', 'n': i, 'val': overrides.get(i, i)}
            for i in rows
        ]

    _seed(
        home,
        _spec('sql:src', 'i', params, docs(range(100))),
        # Watermark: row 59 minus 60 s = row 53. Row 55 is inside the window, row 5 is not.
        _spec('sql:dst', 'i', params, docs(range(60), overrides={5: -1, 55: -1})),
    )
    _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental')
    rows = _rows(home, 'dst', 'i')
    assert rows[5] == -1
    assert rows[55] == 55, 'the backtrack window was not converted to milliseconds'
    assert len(rows) == 100


def test_incremental_refuses_a_destination_without_a_watermark(home):
    """Rows but no datetime axis: refuse that pipe rather than copy the whole history."""
    params = {'columns': {'id': 'n'}, 'dtypes': {'n': 'int', 'val': 'int'}}
    _seed(
        home,
        _spec('sql:src', 'noaxis', params, [{'n': i, 'val': i} for i in range(20)]),
        _spec('sql:dst', 'noaxis', params, [{'n': i, 'val': -1} for i in range(5)]),
        _spec('sql:src', 'a', _hourly_params(), _hourly(range(30))),
        _spec('sql:dst', 'a', _hourly_params(), _hourly(range(20))),
    )
    text = _copy(home, '-f', '-y', '--noask', '--sync-data', 'incremental', ok=False)
    assert 'no datetime' in text.lower(), text
    assert _rows(home, 'dst', 'noaxis') == {i: -1 for i in range(5)}
    assert len(_rows(home, 'dst', 'a')) == 30, 'the other pipes are still copied'


def test_sync_data_all_copies_everything_and_refuses_ignored_bounds(home):
    """`all` keeps overwriting the destination with the whole source; bounds it would ignore fail."""
    _seed(
        home,
        _spec('sql:src', 'a', _hourly_params(), _hourly(range(50))),
        _spec('sql:dst', 'a', _hourly_params(), _hourly([i for i in range(30) if i != 3], overrides={4: -1})),
    )
    text = _copy(home, '-f', '-y', '--noask', '--sync-data', 'all', '--begin', _ts(40), ok=False)
    assert '--sync-data filter' in text, text
    assert len(_rows(home, 'dst', 'a')) == 29

    _copy(home, '-f', '-y', '--noask', '--sync-data', 'all')
    assert _rows(home, 'dst', 'a') == {n: n * 10 for n in range(50)}


def test_sync_data_backtrack_uses_the_source_pipes_window(home):
    """`backtrack`: the source's sync time minus the source pipe's own `backtrack_minutes`."""
    _seed(home, _spec('sql:src', 'b', _hourly_params(fetch={'backtrack_minutes': 120}), _hourly(range(50))))
    _copy(home, '-y', '--noask', '--sync-data', 'backtrack')
    assert sorted(_rows(home, 'dst', 'b')) == [47, 48, 49]
