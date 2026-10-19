"""
Wave 17c (K1): the stack's database setup and defaults, seen from Python.

The setup script (`create-extensions.sql`) has one Rust copy (`meerschaum_core::config::sync::
CREATE_EXTENSIONS_TEXT`, which `mrsm stack` writes) and the Python façade's
`meerschaum.config.stack.CREATE_EXTENSIONS_TEXT` (`_write_initdb`): they must stay byte-identical.
The façade's default `stack` config (`meerschaum.config._default.default_config['stack']`) must
equal the Rust defaults (`default_config.json`) the registry loads.

Every scenario runs in a fresh interpreter against a temporary root (the config is process-global).
"""

import json
import os
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent


def run_python(code: str, root: pathlib.Path):
    """Run `code` in a fresh interpreter; return the last stdout line parsed as JSON."""
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    (root / 'plugins').mkdir(parents=True, exist_ok=True)
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    proc = subprocess.run(
        [sys.executable, '-c', code],
        env=env, capture_output=True, text=True, timeout=120, cwd=str(root),
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_the_python_setup_text_is_the_rust_text(tmp_path):
    """`_core.sync_files(['stack'])` (Rust) and `_write_initdb()` (Python) write the same bytes."""
    rust_root, python_root = tmp_path / 'rust', tmp_path / 'python'
    rust_text = run_python(
        """
import json
from meerschaum import _core
import meerschaum.config.paths as paths
_core.sync_files(['stack'])
print(json.dumps(paths.DB_CREATE_EXTENSIONS_PATH.read_text(encoding='utf-8')))
""",
        rust_root,
    )
    python_text = run_python(
        """
import json
import meerschaum.config.paths as paths
from meerschaum.config.stack import _write_initdb, CREATE_EXTENSIONS_TEXT
_write_initdb()
written = paths.DB_CREATE_EXTENSIONS_PATH.read_bytes().decode('utf-8')
assert written == CREATE_EXTENSIONS_TEXT
print(json.dumps(written))
""",
        python_root,
    )
    assert python_text == rust_text
    source = REPO / 'crates' / 'meerschaum-core' / 'src' / 'config' / 'create-extensions.sql'
    assert rust_text == source.read_bytes().decode('utf-8')
    assert 'pg_available_extensions' in rust_text and rust_text.rstrip().endswith('\\gexec')


def test_the_python_stack_defaults_are_the_rust_defaults(tmp_path):
    """`default_config['stack']` (façade) equals the registry's default `stack` (Rust)."""
    result = run_python(
        """
import json
from meerschaum.config import get_config
from meerschaum.config._default import default_config
rust = get_config('stack', patch=False, substitute=False, sync_files=False)
print(json.dumps({'rust': rust, 'python': default_config['stack']}))
""",
        tmp_path,
    )
    assert result['python'] == result['rust']
    db = result['rust']['docker-compose.yaml']['services']['db']
    setup = (tmp_path / 'db' / 'initdb' / 'create-extensions.sql').as_posix()
    assert f'{setup}:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro' in db['volumes']
    assert db['healthcheck']['test'] == [
        'CMD-SHELL',
        'pg_isready -h 127.0.0.1 -p 5432 -d <DOLLAR>POSTGRES_DB -U <DOLLAR>POSTGRES_USER',
    ]
    assert db['healthcheck']['start_period'] == '300s'
    datasource = result['rust']['grafana']['datasource']['datasources'][0]
    assert datasource['jsonData']['postgresVersion'] == 1800


def test_the_test_stack_runs_the_same_setup():
    """The test stack's `timescaledb_ha` service keeps the v4 directory mount (the legacy style)
    and runs the same script."""
    source = REPO / 'crates' / 'meerschaum-core' / 'src' / 'config' / 'create-extensions.sql'
    copy = REPO / 'tests' / 'connector_resources' / 'timescaledb_ha' / 'initdb' / 'create-extensions.sql'
    assert copy.read_bytes() == source.read_bytes()


def test_an_upgraded_v4_root_gets_no_v5_defaults_merged(tmp_path):
    """
    W17K1-006: on a root v4.1.4 wrote (`v4_root.json`, recorded by
    `crates/meerschaum-core/tests/fixtures/gen_v4_root.py`), looking up the v5-only
    `system:engine` defaults merges nothing into the loaded config: `stack.yaml`'s compose section
    (with `retries` deleted by the user in the `edited` case) stays as written.
    """
    fixture = json.loads(
        (REPO / 'crates' / 'meerschaum-core' / 'tests' / 'fixtures' / 'v4_root.json')
        .read_text(encoding='utf-8')
    )
    for case in fixture['cases']:
        root = tmp_path / case['name']
        config_dir = root / 'config'
        config_dir.mkdir(parents=True)
        for name, text in case['before']['config_files'].items():
            (config_dir / name).write_text(text.replace('{ROOT}', str(root)), encoding='utf-8')
        healthcheck = run_python(
            """
import json
from meerschaum.config import get_config
get_config('system', 'engine', 'python_workers', warn=False)
get_config('system', 'engine', 'workers', warn=False)
print(json.dumps(get_config('stack', 'docker-compose.yaml', 'services', 'db', 'healthcheck')))
""",
            root,
        )
        assert 'start_period' not in healthcheck, (case['name'], healthcheck)
        assert ('retries' in healthcheck) == (case['name'] != 'edited'), healthcheck
