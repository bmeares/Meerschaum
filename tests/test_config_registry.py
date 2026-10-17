"""
Wave 15 (task O): the config registry owned by the Rust extension, seen from Python.

Every scenario runs in a fresh interpreter against a temporary root (`MRSM_ROOT_DIR`), because the
config tree, the environment patches and the files are process-global. Coverage: lossless YAML
values (W15O-1), a failed write never touching the file (W15O-2 and the atomic write), a read-only
file raising `PermissionError`, `MRSM_CONFIG` / `MRSM_PATCH` / `MRSM_<TYPE>_<LABEL>`, the identity
of `meerschaum.config.config`, assigning to it, and the stale-extension error.
"""

import json
import os
import pathlib
import subprocess
import sys

import pytest

from meerschaum import _core

HAS_CORE = _core.load() is not None
REPO = pathlib.Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")

MODES = ['rust', 'python']


def run_python(code: str, root: pathlib.Path, mode: str = 'rust', env=None):
    """Run `code` in a fresh interpreter; return `(returncode, last stdout line parsed, stderr)`."""
    full_env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('MRSM_') or k in ('MRSM_TEST_FLAVORS',)
    }
    full_env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'MRSM_CORE': mode,
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    full_env.update(env or {})
    proc = subprocess.run(
        [sys.executable, '-c', code],
        env=full_env, capture_output=True, text=True, timeout=120, cwd=str(root),
    )
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    parsed = None
    if lines:
        try:
            parsed = json.loads(lines[-1])
        except ValueError:
            parsed = lines[-1]
    return proc.returncode, parsed, proc.stderr


def config_dir(root: pathlib.Path) -> pathlib.Path:
    path = root / 'config'
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_dates_bytes_and_big_ints_survive_get_config_and_write_config(tmp_path):
    """W15O-1: values JSON cannot hold are not projected or lost (rust mode)."""
    (config_dir(tmp_path) / 'custom.yaml').write_text(
        "when: 2024-01-01\n"
        "stamp: 2024-01-01 10:00:00\n"
        "big: 123456789012345678901234567890\n"
        "link: MRSM{meerschaum:instance}\n"
    )
    code = """
import json, datetime
from meerschaum.config import get_config, write_config, read_config
vals = (
    get_config('custom', 'when'), get_config('custom', 'stamp'),
    get_config('custom', 'big'), get_config('custom', 'link'),
)
assert vals[0] == datetime.date(2024, 1, 1) and type(vals[0]) is datetime.date, vals
assert vals[1] == datetime.datetime(2024, 1, 1, 10, 0), vals
assert vals[2] == 123456789012345678901234567890, vals
assert vals[3] == 'sql:main', vals
assert write_config({'custom': {'when': datetime.date(2025, 2, 3), 'raw': b'ab'}}) is True
back = read_config(keys=['custom'], substitute=False)['custom']
assert back['when'] == datetime.date(2025, 2, 3), back
assert back['raw'] == b'ab', back
print(json.dumps('ok'))
"""
    rc, out, err = run_python(code, tmp_path)
    assert rc == 0 and out == 'ok', err


def test_a_failed_dump_leaves_the_file_untouched(tmp_path):
    """W15O-2: an object `yaml.dump` cannot represent fails the key; the file is not deleted."""
    target = config_dir(tmp_path) / 'custom.yaml'
    original = "# my header\nsecret: hunter2\n"
    target.write_text(original)
    code = """
import json
from meerschaum.config import write_config
result = write_config({'custom': {'bad': (lambda: None)}})
print(json.dumps(result))
"""
    rc, out, err = run_python(code, tmp_path)
    assert rc == 0, err
    assert out is False
    assert target.read_text() == original
    assert [p.name for p in target.parent.iterdir() if p.name.startswith('.')] == []


@pytest.mark.parametrize('mode', MODES)
def test_a_read_only_file_raises_permission_error_and_stays_intact(tmp_path, mode):
    if hasattr(os, 'geteuid') and os.geteuid() == 0:
        pytest.skip("root ignores file modes")
    target = config_dir(tmp_path) / 'custom.json'
    original = '{"password": "hunter2"}\n'
    target.write_text(original)
    target.chmod(0o444)
    code = """
import json
from meerschaum.config import write_config
try:
    write_config({'custom': {'password': 'new'}})
except PermissionError:
    print(json.dumps('PermissionError'))
else:
    print(json.dumps('no error'))
"""
    rc, out, err = run_python(code, tmp_path, mode=mode)
    assert rc == 0, err
    assert out == 'PermissionError'
    assert target.read_text() == original
    assert oct(target.stat().st_mode & 0o777) == '0o444'
    assert [p.name for p in target.parent.iterdir() if p.name.startswith('.')] == []


def test_a_successful_write_replaces_the_file_atomically_and_keeps_its_mode(tmp_path):
    target = config_dir(tmp_path) / 'custom.yaml'
    target.write_text("old: 1\n")
    target.chmod(0o640)
    code = """
import json
from meerschaum.config import write_config
print(json.dumps(write_config({'custom': {'new': 2}})))
"""
    rc, out, err = run_python(code, tmp_path)
    assert rc == 0 and out is True, err
    assert target.read_text() == "new: 2\n"
    assert oct(target.stat().st_mode & 0o777) == '0o640'
    assert [p.name for p in target.parent.iterdir() if p.name.startswith('.')] == []


@pytest.mark.parametrize('mode', MODES)
def test_environment_patches_and_connector_uris(tmp_path, mode):
    code = """
import json
from meerschaum.config import get_config
print(json.dumps([
    get_config('custom', 'a'),
    get_config('custom', 'b'),
    get_config('meerschaum', 'connectors', 'sql', 'foo', 'uri'),
    get_config('meerschaum', 'connectors', 'api', 'bar', 'uri'),
]))
"""
    env = {
        'MRSM_CONFIG': json.dumps({'custom': {'a': 1, 'b': 1}}),
        'MRSM_PATCH': json.dumps({'custom': {'b': 2}}),
        'MRSM_SQL_FOO': 'sqlite:////tmp/mrsm_w15o_foo.db',
        'MRSM_API_BAR': 'http://api.example.invalid:8000',
    }
    rc, out, err = run_python(code, tmp_path, mode=mode, env=env)
    assert rc == 0, err
    assert out == [1, 2, 'sqlite:////tmp/mrsm_w15o_foo.db', 'http://api.example.invalid:8000'], out


@pytest.mark.parametrize('mode', MODES)
def test_the_module_attribute_is_the_dict_get_config_mutates(tmp_path, mode):
    code = """
import json
import meerschaum.config as c
from meerschaum.config import config as imported
d = c.get_config('system', 'connectors', 'sql')
d['chunksize'] = 7
whole = c.get_config()
checks = [
    c.config is c.get_config(),
    c.config is imported,
    c.config['system']['connectors']['sql'] is d,
    c.get_config('system', 'connectors', 'sql', 'chunksize') == 7,
]
print(json.dumps(checks))
"""
    rc, out, err = run_python(code, tmp_path, mode=mode)
    assert rc == 0, err
    assert out == [True, True, True, True]


@pytest.mark.parametrize('mode', MODES)
def test_assigning_the_module_attribute_replaces_the_live_config(tmp_path, mode):
    code = """
import json
import meerschaum.config as c
c.get_config('meerschaum')
replacement = {'meerschaum': {'instance': 'sql:assigned'}, 'custom': {'x': 3}}
c.config = replacement
checks = [
    c.config is replacement,
    c.get_config('meerschaum', 'instance') == 'sql:assigned',
    c.get_config('custom', 'x') == 3,
    c.get_config() is replacement,
]
try:
    c.config = 'not a dict'
except Exception:
    checks.append(True)
else:
    checks.append(False)
print(json.dumps(checks))
"""
    rc, out, err = run_python(code, tmp_path, mode=mode)
    assert rc == 0, err
    assert out == [True, True, True, True, True]


def test_a_stale_extension_is_reported_as_not_built(monkeypatch):
    """An extension without the config registry symbols is "not built", naming the script."""
    import importlib

    class Stale:
        """An extension built before the config registry moved to Rust."""

    for name, value in (
        ('_loaded', False), ('_extension', None), ('AVAILABLE', True),
        ('LOAD_ERROR', None), ('EXTENSION_PATH', None),
    ):
        monkeypatch.setattr(_core, name, value, raising=False)
    monkeypatch.setattr(importlib, 'import_module', lambda *_a, **_k: Stale())
    assert _core.load() is None
    assert _core.AVAILABLE is False
    assert isinstance(_core.LOAD_ERROR, _core.CoreNotBuiltError)
    assert 'scripts/rust/build_py.sh' in str(_core.LOAD_ERROR)


@pytest.mark.parametrize('mode', MODES)
def test_importing_the_config_does_not_load_the_extension(tmp_path, mode):
    code = """
import json, sys
import meerschaum, meerschaum.config
import meerschaum.config as c
assert c.config == {}
print(json.dumps('meerschaum._core._core' in sys.modules))
"""
    rc, out, err = run_python(code, tmp_path, mode=mode)
    assert rc == 0, err
    assert out is False


@pytest.mark.parametrize('mode', MODES)
def test_environment_connectors_apply_at_import_and_may_load_the_extension(tmp_path, mode):
    """With `MRSM_SQL_*` set the import applies them (loading the extension is legitimate)."""
    code = """
import json
import meerschaum.config as c
print(json.dumps(c.config['meerschaum']['connectors']['sql']['foo']['uri']))
"""
    env = {'MRSM_SQL_FOO': 'sqlite:////tmp/mrsm_w15o_import.db'}
    rc, out, err = run_python(code, tmp_path, mode=mode, env=env)
    assert rc == 0, err
    assert out == 'sqlite:////tmp/mrsm_w15o_import.db'


@pytest.mark.parametrize('mode', MODES)
def test_the_extension_adopts_the_dict_created_before_it_loaded(tmp_path, mode):
    code = """
import json, sys
import meerschaum.config as c
d = c.get_config('system', 'connectors', 'sql')
d['chunksize'] = 11
from meerschaum import _core
assert _core.load() is not None
checks = [
    _core.config_dict() is c.config,
    _core.get_config('system', 'connectors', 'sql', 'chunksize') == 11,
]
c.config = {'meerschaum': {'instance': 'sql:adopted'}}
checks.append(_core.config_dict() is c.config)
checks.append(_core.get_config('meerschaum', 'instance') == 'sql:adopted')
print(json.dumps(checks))
"""
    rc, out, err = run_python(code, tmp_path, mode=mode)
    assert rc == 0, err
    assert out == [True, True, True, True]


@pytest.mark.parametrize('mode', MODES)
def test_debug_prints_the_lines_v4_prints(tmp_path, mode):
    (config_dir(tmp_path) / 'custom.yaml').write_text("a: 1\n")
    code = """
from meerschaum.config import get_config
from meerschaum.config._backup import backup_config_key
get_config('meerschaum', 'instance', debug=True)
backup_config_key('absent', debug=True)
backup_config_key('custom', debug=True)
backup_config_key('custom', debug=True)
backup_config_key('custom')
"""
    out = subprocess_output(code, tmp_path, mode)
    assert 'Traceback' not in out, out
    assert "Indexing keys: ('meerschaum', 'instance')" in out
    assert "No config file to back up for 'absent'." in out
    assert "Backed up 'custom' to '" in out
    assert "'custom' unchanged since last backup; skipping." in out
    assert out.count("unchanged since last backup") == 1


def subprocess_output(code, root, mode):
    """The combined stdout and stderr of `code` (debug lines go to either)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'PYTHONPATH': str(REPO), 'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'), 'MRSM_CORE': mode,
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    proc = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True,
        timeout=120, cwd=str(root),
    )
    return proc.stdout + proc.stderr


def test_replace_config_writes_no_derivative_files(tmp_path):
    """W15O-8: under `replace_config` (writes off) the first `stack` load syncs nothing."""
    code = """
import json, os
import meerschaum.config as c
from meerschaum.config import replace_config, get_config
with replace_config({'meerschaum': {'instance': 'sql:x'}}):
    get_config('stack', 'docker-compose.yaml')
import meerschaum.config.paths as p
print(json.dumps(os.path.exists(p.STACK_COMPOSE_PATH)))
"""
    rc, out, err = run_python(code, tmp_path)
    assert rc == 0, err
    assert out is False


def test_the_native_config_carries_the_plugins_and_venvs_dirs(tmp_path):
    """`MRSM_PLUGINS_DIR` (several, separated like `PATH`) and `MRSM_VENVS_DIR` reach the native view."""
    first, second = tmp_path / 'plugins_a', tmp_path / 'plugins_b'
    first.mkdir()
    second.mkdir()
    venvs = tmp_path / 'custom_venvs'
    code = """
import json
from meerschaum import _core
assert _core.load() is not None
paths = _core.live_config_paths()
print(json.dumps(paths))
"""
    env = {
        'MRSM_PLUGINS_DIR': json.dumps([str(first), str(second)]),
        'MRSM_VENVS_DIR': str(venvs),
    }
    rc, out, err = run_python(code, tmp_path, env=env)
    assert rc == 0, err
    assert out['plugins_dirs'] == [str(first.resolve()), str(second.resolve())], out
    assert out['venvs_dir'] == str(venvs.resolve()), out
    assert out['root'] == str(tmp_path.resolve()), out


def test_the_native_config_defaults_to_the_root_plugins_and_venvs(tmp_path):
    code = """
import json
from meerschaum import _core
print(json.dumps(_core.live_config_paths()))
"""
    rc, out, err = run_python(code, tmp_path)
    assert rc == 0, err
    root = str(tmp_path.resolve())
    assert out['plugins_dirs'] == [f'{root}/plugins'], out
    assert out['venvs_dir'] is None, out
