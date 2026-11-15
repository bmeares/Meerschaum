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


def test_the_default_api_image_is_this_releases_full_image(tmp_path):
    """
    W18PK-001: the stack's `api` service runs `bmeares/meerschaum:<version>-full` (the image the
    release publishes; a prerelease never moves the floating `:api` tag), in the façade's defaults
    and in the Rust registry's alike, and the `:api` tag is not the default any more.
    """
    from meerschaum.config._version import __version__

    result = run_python(
        """
import json
from meerschaum.config import get_config
from meerschaum.config._default import default_config
rust = get_config('stack', 'docker-compose.yaml', 'services', 'api', 'image', patch=False, substitute=False, sync_files=False)
python = default_config['stack']['docker-compose.yaml']['services']['api']['image']
print(json.dumps({'rust': rust, 'python': python}))
""",
        tmp_path,
    )
    expected = f'bmeares/meerschaum:{__version__}-full'
    assert result == {'rust': expected, 'python': expected}


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


def test_python_never_writes_an_unhardened_compose_file(tmp_path):
    """
    5.1: the compose file of a stack with Grafana is written by `mrsm stack` only (with the
    read-only role's setup service and, with `api:grafana`, the auth proxy). Python's first read
    of `stack`, `sync_files` and `write_stack()` leave it alone; the data source file never holds
    `sql:main`'s user. A stack without Grafana still gets its compose file from Python.
    """
    result = run_python(
        """
import json, os
import meerschaum.config.paths as paths
from meerschaum.config import get_config
from meerschaum.config.stack import write_stack
from meerschaum import _core
get_config('stack', 'docker-compose.yaml')
_core.sync_files(['stack'])
write_stack()
print(json.dumps({
    'compose': os.path.exists(paths.STACK_COMPOSE_PATH),
    'datasource': paths.GRAFANA_DATASOURCE_PATH.read_text(encoding='utf-8'),
}))
""",
        tmp_path / 'with_grafana',
    )
    assert result['compose'] is False, result
    assert 'user: mrsm__main_ro' in result['datasource'], result['datasource']

    ### 5.1: without the `api` service's configuration too (`mrsm stack` moves it to files).
    without = tmp_path / 'without_grafana'
    (without / 'config').mkdir(parents=True)
    (without / 'config' / 'stack.yaml').write_text(
        "docker-compose.yaml:\n  services:\n    db:\n      image: postgres:18\n"
        "    api: null\n    grafana: null\n"
    )
    result = run_python(
        """
import json
import meerschaum.config.paths as paths
from meerschaum.config.stack import write_stack
write_stack()
print(json.dumps(paths.STACK_COMPOSE_PATH.read_text(encoding='utf-8')))
""",
        without,
    )
    assert 'image: postgres:18' in result, result
    assert 'image: grafana' not in result and 'grafana_readonly' not in result, result


def test_the_stack_passes_the_api_its_configuration_by_file(tmp_path):
    """
    5.1: `mrsm stack` writes the `api` service's `MRSM_CONFIG` / `MRSM_PATCH` to files under
    `<root>/stack/secrets/api/` (an owner-only directory) and the compose file names them with
    `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`: no connector's password is in the compose file, and
    Python's writers never put it back. (A stand-in `docker` that fails keeps Docker untouched.)
    """
    import stat
    root = tmp_path / 'root'
    (root / 'config').mkdir(parents=True)
    (root / 'plugins').mkdir()
    (root / 'config' / 'meerschaum.yaml').write_text(
        "connectors:\n  sql:\n    main:\n      flavor: timescaledb\n      username: mrsm\n"
        "      password: pg-s3cret-xyz\n      database: meerschaum\n      host: localhost\n"
    )
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    (fake_bin / 'docker').write_text('#!/bin/sh\nexit 1\n')
    (fake_bin / 'docker').chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PATH': str(fake_bin) + os.pathsep + env.get('PATH', ''),
    })
    proc = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'stack', 'ps'],
        env=env, capture_output=True, text=True, timeout=120, cwd=str(root),
    )
    assert 'Docker' in proc.stdout + proc.stderr, proc.stdout + proc.stderr
    compose = (root / 'stack' / 'docker-compose.yaml').read_text(encoding='utf-8')
    assert 'pg-s3cret-xyz' not in compose, compose
    assert 'MRSM_CONFIG_FILE: /run/secrets/meerschaum/mrsm_config.json' in compose, compose
    secrets = root / 'stack' / 'secrets'
    config = json.loads((secrets / 'api' / 'mrsm_config.json').read_text(encoding='utf-8'))
    assert config['meerschaum']['connectors']['sql']['main']['password'] == 'pg-s3cret-xyz'
    if os.name != 'nt':
        assert stat.S_IMODE(secrets.stat().st_mode) == 0o700
        assert stat.S_IMODE((secrets / 'api' / 'mrsm_config.json').stat().st_mode) == 0o644

    result = run_python(
        """
import json
import meerschaum.config.paths as paths
from meerschaum.config import get_config
from meerschaum.config.stack import write_stack
get_config('stack', 'docker-compose.yaml')
write_stack()
print(json.dumps(paths.STACK_COMPOSE_PATH.read_text(encoding='utf-8')))
""",
        root,
    )
    assert result == compose
