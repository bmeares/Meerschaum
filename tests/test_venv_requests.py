#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Tests for the venv request ledger (`venvs/<name>/.mrsm_requests.json`, design D20).
No network: the installer is monkeypatched.

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_venv_requests.py
"""

import json
import multiprocessing
import pathlib
import sys
import types

import pytest

import meerschaum.utils.packages as packages
from meerschaum.config._paths import replace_root_dir
from meerschaum.utils.venv import _requests


@pytest.fixture
def temp_root(tmp_path):
    with replace_root_dir(tmp_path):
        (tmp_path / 'venvs' / 'mrsm').mkdir(parents=True)
        (tmp_path / 'venvs' / 'myplug').mkdir(parents=True)
        _requests._recorded.clear()
        yield tmp_path
    _requests._recorded.clear()


def _ledger(root, venv='mrsm'):
    with open(root / 'venvs' / venv / '.mrsm_requests.json') as f:
        return json.load(f)


def test_normalize_package_name():
    assert _requests.normalize_package_name('Dash_Bootstrap.Components>=1.0') == 'dash-bootstrap-components'
    assert _requests.normalize_package_name('psycopg[binary]>=3') == 'psycopg'
    assert _requests.normalize_package_name('pandas ; python_version>"3"') == 'pandas'


def test_record_request_schema_and_timestamps(temp_root):
    assert _requests.record_request('mrsm', 'pandas>=2.0', 'noaa')
    data = _ledger(temp_root)
    assert data['schema_version'] == 1
    entry = data['packages']['pandas']['requesters']['noaa']
    assert entry['spec'] == 'pandas>=2.0'
    assert entry['first_requested'] == entry['last_requested']

    ### Same process, same triple: no rewrite unless forced.
    assert not _requests.record_request('mrsm', 'pandas>=2.0', 'noaa')
    assert _requests.record_request('mrsm', 'pandas>=2.1', 'noaa', force=True)
    entry2 = _ledger(temp_root)['packages']['pandas']['requesters']['noaa']
    assert entry2['spec'] == 'pandas>=2.1'
    assert entry2['first_requested'] == entry['first_requested']

    assert _requests.record_request('mrsm', 'pandas', 'meerschaum')
    assert set(_ledger(temp_root)['packages']['pandas']['requesters']) == {'noaa', 'meerschaum'}


def test_record_request_untracked_cases(temp_root):
    assert not _requests.record_request(None, 'pandas', 'noaa')
    ### A missing venv directory is never created for accounting.
    assert not _requests.record_request('nonexistent', 'pandas', 'noaa')
    assert not (temp_root / 'venvs' / 'nonexistent').exists()
    ### A corrupt ledger is replaced, not fatal.
    (temp_root / 'venvs' / 'mrsm' / '.mrsm_requests.json').write_text('{not json')
    assert _requests.record_request('mrsm', 'requests', 'noaa')
    assert 'requests' in _ledger(temp_root)['packages']


def _record_in_subprocess(root, index):
    with replace_root_dir(root):
        _requests.record_request('mrsm', f'package{index}', f'plugin{index}')


def test_concurrent_writers_do_not_lose_updates(temp_root):
    ctx = multiprocessing.get_context('spawn' if sys.platform == 'win32' else 'fork')
    procs = [ctx.Process(target=_record_in_subprocess, args=(temp_root, i)) for i in range(8)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(60)
        assert proc.exitcode == 0
    assert set(_ledger(temp_root)['packages']) == {f'package{i}' for i in range(8)}
    leftovers = [p.name for p in (temp_root / 'venvs' / 'mrsm').iterdir() if p.name.endswith('.tmp')]
    assert not leftovers


def _fake_plugin_module(name):
    """A module named like a v4 plugin, so `get_requester()` attributes calls to it."""
    module = types.ModuleType(f'plugins.{name}')
    code = (
        "def call(fn, *a, **kw):\n"
        "    return fn(*a, **kw)\n"
    )
    exec(compile(code, f'<plugins.{name}>', 'exec'), module.__dict__)
    return module


def test_get_requester_from_plugin_and_core():
    plugin = _fake_plugin_module('noaa')
    assert plugin.call(_requests.get_requester) == 'noaa'
    assert _requests.get_requester() == 'meerschaum'


def test_attempt_import_records_install_by_plugin(temp_root, monkeypatch):
    ### The ledger records implicit installs: they are off under `MRSM_NO_AUTO_INSTALL`.
    monkeypatch.delenv('MRSM_NO_AUTO_INSTALL', raising=False)
    installed = set()

    def fake_pip_install(*names, venv='mrsm', **kw):
        installed.update(names)
        return True

    monkeypatch.setattr(packages, 'pip_install', fake_pip_install)
    monkeypatch.setattr(packages, 'is_installed', lambda name, **kw: name in installed or name == 'json')
    monkeypatch.setattr(packages, 'venv_contains_package', lambda name, **kw: name in installed)
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: False)
    ### Importing the fake package fails; the ledger is what is under test.
    plugin = _fake_plugin_module('noaa')
    plugin.call(packages.attempt_import, 'bs4', lazy=False, warn=False, venv='mrsm')
    data = _ledger(temp_root)
    requesters = data['packages']['bs4']['requesters']
    assert list(requesters) == ['noaa']

    ### Core install is attributed to `meerschaum`.
    packages.attempt_import('mypackagexyz', lazy=False, warn=False, venv='myplug')
    assert list(_ledger(temp_root, 'myplug')['packages']['mypackagexyz']['requesters']) == ['meerschaum']

    ### A plugin asking for a package that is already installed is recorded too.
    plugin2 = _fake_plugin_module('other')
    plugin2.call(packages.attempt_import, 'bs4', lazy=False, warn=False, venv='mrsm')
    assert set(_ledger(temp_root)['packages']['bs4']['requesters']) == {'noaa', 'other'}


def test_manifest_flags_undeclared_default_venv_imports(tmp_path):
    from meerschaum.worker._manifest import find_undeclared_default_venv_imports
    source = tmp_path / 'sketchy.py'
    source.write_text(
        "from meerschaum.utils.packages import attempt_import\n"
        "import meerschaum as mrsm\n"
        "required = ['pypdf>=3']\n"
        "pypdf = attempt_import('pypdf')\n"
        "pil = mrsm.attempt_import('PIL.Image')\n"
        "icalendar = attempt_import('icalendar', venv='sketchy')\n"
        "pandas = attempt_import('pandas')\n"
        "explicit = attempt_import('markdown', venv='mrsm')\n"
        "dynamic = attempt_import(some_name)\n"
    )
    warnings_found = find_undeclared_default_venv_imports(str(source), ['pypdf>=3'])
    named = sorted(w.split("'")[1] for w in warnings_found)
    ### `pypdf` is required, `icalendar` has its own venv, `pandas` is a Meerschaum extra.
    assert named == ['PIL', 'markdown']
    assert all('required' in w and 'venv=' in w for w in warnings_found)
    assert find_undeclared_default_venv_imports(None, []) == []


def test_pip_install_ledger_hook_records_and_forgets(temp_root):
    ### The hook `pip_install()` runs after a successful install / uninstall.
    packages._update_request_ledger(['pandas>=2', 'git+https://example.invalid/x.git'], 'mrsm', False)
    data = _ledger(temp_root)
    assert list(data['packages']) == ['pandas']
    assert list(data['packages']['pandas']['requesters']) == ['meerschaum']
    packages._update_request_ledger(['pandas'], 'mrsm', True)
    assert _ledger(temp_root)['packages'] == {}
    ### The system environment (venv=None) is not tracked.
    packages._update_request_ledger(['pandas'], None, False)
