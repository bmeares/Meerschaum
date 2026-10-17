#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The request ledger (`venvs/<name>/.mrsm_requests.json`, design D20) around a REAL install and
uninstall in a throwaway virtual environment (wave 9). No network: the packages are three tiny
pure-Python wheels built in the test, installed with `--no-index --find-links`.

The native side (`mrsm prune venvs`, `crates/meerschaum-cli/tests/w9_venvs.rs`) reads what these
functions write; `test_native_show_venvs_reads_the_python_ledger` checks that the two agree when
a debug `mrsm` binary has been built.

Every test runs inside a temporary root (`replace_root_dir`), never `~/.config/meerschaum`.

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_w9_venvs.py -p no:cacheprovider
"""

import base64
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import zipfile

import pytest

from meerschaum.config._paths import replace_root_dir
from meerschaum.utils.venv import _requests

pytestmark = pytest.mark.skipif(
    shutil.which('uv') is None or sys.platform == 'win32',
    reason="`uv` is needed to build the throwaway virtual environment",
)

REPO = pathlib.Path(__file__).resolve().parent.parent
VENV = 'w9t_demo'


def _make_wheel(out: pathlib.Path, name: str, version: str, requires=()):
    dist = f'{name}-{version}'
    files = {
        f'{name}/__init__.py': f'VALUE = {version!r}\n',
        f'{dist}.dist-info/METADATA': (
            f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n'
            + ''.join(f'Requires-Dist: {r}\n' for r in requires)
        ),
        f'{dist}.dist-info/WHEEL': (
            'Wheel-Version: 1.0\nGenerator: w9t\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
        ),
    }
    record = []
    for path, text in files.items():
        digest = base64.urlsafe_b64encode(
            hashlib.sha256(text.encode()).digest()
        ).rstrip(b'=').decode()
        record.append(f'{path},sha256={digest},{len(text.encode())}')
    record.append(f'{dist}.dist-info/RECORD,,')
    files[f'{dist}.dist-info/RECORD'] = '\n'.join(record) + '\n'
    with zipfile.ZipFile(out / f'{dist}-py3-none-any.whl', 'w') as z:
        for path, text in files.items():
            z.writestr(path, text)


@pytest.fixture
def throwaway(tmp_path):
    """A temporary root with an empty `venvs/w9t_demo` made with the running interpreter."""
    pytest.importorskip('uv')
    wheels = tmp_path / 'wheels'
    wheels.mkdir()
    _make_wheel(wheels, 'w9t_wanted', '1.0', ['w9t_dep>=1.0'])
    _make_wheel(wheels, 'w9t_dep', '1.0')
    _make_wheel(wheels, 'w9t_stale', '1.0')
    with replace_root_dir(tmp_path):
        (tmp_path / 'venvs').mkdir(exist_ok=True)
        made = subprocess.run(
            ['uv', 'venv', '--quiet', '--python', sys.executable, str(tmp_path / 'venvs' / VENV)],
            capture_output=True,
        )
        assert made.returncode == 0, made.stderr
        _requests._recorded.clear()
        yield tmp_path, wheels
    _requests._recorded.clear()


def _site(root):
    (site,) = (root / 'venvs' / VENV / 'lib').glob('python*/site-packages')
    return site


def _installed(root):
    return sorted(
        p.name[:-len('.dist-info')]
        for p in _site(root).iterdir()
        if p.name.endswith('.dist-info') and p.name.startswith('w9t_')
    )


def _ledger(root):
    with open(root / 'venvs' / VENV / '.mrsm_requests.json') as f:
        return json.load(f)


def _install(wheels, *names):
    from meerschaum.utils.packages import pip_install
    return pip_install(
        *names,
        args=['--no-index', '--find-links', str(wheels)],
        venv=VENV,
        check_update=False,
        check_pypi=False,
        check_wheel=False,
        silent=True,
    )


def test_install_records_the_requester_and_dependencies_stay_unattributed(throwaway):
    root, wheels = throwaway
    assert _install(wheels, 'w9t-wanted>=1.0')
    assert _installed(root) == ['w9t_dep-1.0', 'w9t_wanted-1.0']
    data = _ledger(root)
    assert data['schema_version'] == 1
    assert list(data['packages']) == ['w9t-wanted']
    assert list(data['packages']['w9t-wanted']['requesters']) == ['meerschaum']
    ### `w9t_dep` came in as a dependency: no one asked for it by name, so no ledger entry.
    assert 'w9t-dep' not in data['packages']


def test_uninstall_removes_the_package_and_forgets_only_its_entry(throwaway):
    from meerschaum.utils.packages import pip_uninstall
    root, wheels = throwaway
    assert _install(wheels, 'w9t-wanted>=1.0')
    assert _install(wheels, 'w9t-stale')
    assert _requests.record_request(VENV, 'w9t_stale', 'gone_plugin', force=True)
    assert set(_ledger(root)['packages']) == {'w9t-wanted', 'w9t-stale'}
    assert set(_ledger(root)['packages']['w9t-stale']['requesters']) == {'meerschaum', 'gone_plugin'}

    assert pip_uninstall('w9t-stale', venv=VENV, silent=True)
    assert _installed(root) == ['w9t_dep-1.0', 'w9t_wanted-1.0']
    assert not (_site(root) / 'w9t_stale').exists()
    assert list(_ledger(root)['packages']) == ['w9t-wanted']
    ### The requested package and its dependency are untouched.
    assert (_site(root) / 'w9t_wanted' / '__init__.py').exists()
    ### Uninstalling again is a no-op failure for the installer; the ledger stays consistent.
    assert list(_ledger(root)['packages']) == ['w9t-wanted']


def test_uninstall_action_with_yes_flag(throwaway):
    from meerschaum.actions import actions
    root, wheels = throwaway
    assert _install(wheels, 'w9t-stale')
    success, message = actions['uninstall'](['packages', 'w9t-stale'], venv=VENV, yes=True, sub_args=[])
    assert success, message
    assert _installed(root) == []
    assert _ledger(root)['packages'] == {}


def test_uninstall_action_without_consent_flags(throwaway):
    from meerschaum.actions import actions
    root, wheels = throwaway
    assert _install(wheels, 'w9t-stale')
    success, message = actions['uninstall'](['packages', 'w9t-stale'], venv=VENV, sub_args=[])
    assert success, message
    assert _installed(root) == []
    assert _ledger(root)['packages'] == {}


def test_native_show_venvs_reads_the_python_ledger(throwaway):
    binary = REPO / 'target' / 'debug' / 'mrsm'
    if not binary.exists():
        pytest.skip("build the debug binary first (`cargo build -p meerschaum-cli`)")
    root, wheels = throwaway
    assert _install(wheels, 'w9t-wanted>=1.0')
    assert _requests.record_request(VENV, 'w9t_stale', 'gone_plugin')
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
    })
    (root / 'plugins').mkdir()
    result = subprocess.run(
        [str(binary), 'show', 'venvs'], env=env, capture_output=True, text=True, cwd=str(root),
    )
    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert VENV in out
    assert 'w9t_wanted 1.0' in out and '[meerschaum]' in out
    ### Installed without a request from the ledger's point of view.
    assert 'w9t_dep 1.0' in out and '[unattributed]' in out
