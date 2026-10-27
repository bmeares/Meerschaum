#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Helpers for the API host tests: run Python in host mode with a temporary root, a temporary
plugins directory and a temporary SQLite instance (never the real config).
"""

import os
import sys
import json
import pathlib
import tempfile
import textwrap
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]


def make_env(root: pathlib.Path, plugins: pathlib.Path, host_mode: bool = True) -> dict:
    env = {
        key: val
        for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key != 'PYTHONPATH'
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_SQL_APIHOSTTEST': f"sqlite:///{root / 'instance.db'}",
        'MRSM_API_HOST_CONFIG': json.dumps({'mrsm_instance': 'sql:apihosttest'}),
    })
    ### Starlette 1.9 warns when `starlette.testclient` falls back to `httpx` (it wants `httpx2`,
    ### which CI and `scripts/test.sh` install): the tests behave the same either way, so a
    ### developer venv without `httpx2` prints no deprecation noise into the stderr they check.
    env['PYTHONWARNINGS'] = ','.join(filter(None, [
        os.environ.get('PYTHONWARNINGS', ''),
        STARLETTE_HTTPX_WARNING_FILTER,
    ]))
    if host_mode:
        env['MRSM_API_HOST'] = '1'
    return env


### `-W action:message`: the message is matched literally from its start (no category, so the
### filter does not import Starlette at interpreter startup).
STARLETTE_HTTPX_WARNING_FILTER = 'ignore:Using `httpx` with `starlette.testclient` is deprecated'


def run_python(root, plugins, code: str, host_mode: bool = True, extra_env: dict = None):
    env = make_env(root, plugins, host_mode)
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(code)],
        env=env,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=180,
    )


def write_plugin(plugins: pathlib.Path, name: str, source: str) -> None:
    path = plugins / name
    path.mkdir(parents=True, exist_ok=True)
    (path / '__init__.py').write_text(textwrap.dedent(source), encoding='utf-8')


@pytest.fixture(scope='module')
def sandbox():
    """A temporary root and plugins directory, skipped when the host dependencies are missing."""
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-', dir=scratch))
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-plugins-', dir=scratch))
    import shutil
    try:
        check = run_python(
            root, plugins,
            "import meerschaum.utils.venv as v; v.activate_venv('mrsm'); "
            "import fastapi, uvicorn, dash, dash_bootstrap_components, dash_ace, a2wsgi, jwt",
        )
        if check.returncode != 0:
            pytest.skip(f"The API host's dependencies are missing:\n{check.stderr[-1500:]}")
        yield root, plugins
    finally:
        ### Also when the check skips or fails: the directories are never left behind.
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(plugins, ignore_errors=True)
