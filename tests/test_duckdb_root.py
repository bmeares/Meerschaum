"""
A `duckdb` connector looks for (and installs) DuckDB's library under the Meerschaum root in
effect when it is built, also after `set_root()` changed it: the same root `mrsm install driver`
and `mrsm show drivers` use (not the stale `MRSM_ROOT_DIR` of the process environment).

Runs in a subprocess (the library is loaded once per process). Needs the real library for the
seeded cache (`MRSM_DUCKDB_LIB`, a symlink to it is placed in the new root's cache; the child
never sees the variable, so only the cache can supply the library) and the built extension.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests._binary import in_ci

REPO = Path(__file__).resolve().parent.parent
LIB_SOURCE = REPO / 'crates' / 'meerschaum-connector-sql' / 'src' / 'duckdb_lib' / 'mod.rs'

CHILD = """
import sys
from pathlib import Path
import meerschaum as mrsm
from meerschaum.config import paths

new_root = Path(sys.argv[1])
database = sys.argv[2]
paths.set_root(new_root)
conn = mrsm.get_connector('sql', 'w17b_duck', flavor='duckdb', database=database)
ok = conn.test_connection()
detail = ''
if not ok:
    try:
        conn._rust_method('get_users')()
    except Exception as e:
        detail = str(e)
print('RESULT', 'connected' if ok else 'failed', detail)
"""


def _real_library() -> str:
    """
    The DuckDB library for the seeded cache (`MRSM_DUCKDB_LIB`). Without it the test is skipped,
    except in CI, where a missing library is a failure (the same rule as `tests/_binary.py`).
    """
    real = os.environ.get('MRSM_DUCKDB_LIB')
    if real and Path(real).is_file():
        return real
    message = 'set MRSM_DUCKDB_LIB to the DuckDB library for this test'
    if in_ci():
        pytest.fail(message)
    pytest.skip(message)


def _version() -> str:
    match = re.search(r'pub const VERSION: &str = "([^"]+)";', LIB_SOURCE.read_text())
    assert match, 'VERSION not found in duckdb_lib/mod.rs'
    return match.group(1)


def _env(tmp_path: Path) -> dict:
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    (tmp_path / 'env_root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'env_root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_SQL_MAIN': f"sqlite:///{tmp_path / 'main.db'}",
        'PYTHONPATH': str(REPO),
    })
    return env


def _run(tmp_path: Path, new_root: Path):
    return subprocess.run(
        [sys.executable, '-c', CHILD, str(new_root), str(tmp_path / 'data.duckdb')],
        env=_env(tmp_path), capture_output=True, text=True, timeout=180,
    )


def test_a_connector_built_after_set_root_uses_the_new_roots_cache(tmp_path):
    real = _real_library()
    new_root = tmp_path / 'new_root'
    cache = new_root / 'drivers' / 'duckdb' / _version()
    cache.mkdir(parents=True)
    (cache / Path(real).name).symlink_to(Path(real).resolve())
    completed = _run(tmp_path, new_root)
    output = completed.stdout + completed.stderr
    assert 'RESULT connected' in completed.stdout, output[-3000:]
    # Nothing was created under the stale environment root.
    assert not (tmp_path / 'env_root' / 'drivers').exists()


def test_without_a_cache_in_the_new_root_the_lookup_fails_there(tmp_path):
    new_root = tmp_path / 'new_root'
    new_root.mkdir()
    # The stale environment root has the library, the new root does not: the new root wins.
    real = _real_library()
    stale = tmp_path / 'env_root' / 'drivers' / 'duckdb' / _version()
    stale.mkdir(parents=True)
    (stale / Path(real).name).symlink_to(Path(real).resolve())
    completed = _run(tmp_path, new_root)
    assert 'RESULT failed' in completed.stdout, (completed.stdout + completed.stderr)[-3000:]
    assert 'MRSM_NO_AUTO_INSTALL' in completed.stdout, completed.stdout[-3000:]
