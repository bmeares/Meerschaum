#! /usr/bin/env python3

from contextlib import nullcontext


def _hold_package_install_lock(root, active, max_active, start):
    """Process target used to verify that environment install locks serialize."""
    import time
    from pathlib import Path
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages

    paths.VIRTENV_RESOURCES_PATH = Path(root) / 'venvs'
    start.wait()
    with packages._pip_install_lock('shared'):
        with active.get_lock(), max_active.get_lock():
            active.value += 1
            max_active.value = max(max_active.value, active.value)
        time.sleep(0.15)
        with active.get_lock():
            active.value -= 1


def test_attempt_import_install_cache_is_scoped(monkeypatch):
    """Installed-package checks must not leak between virtual environments."""
    import meerschaum.utils.packages as packages

    checks = []
    monkeypatch.setattr(packages, 'Venv', lambda *args, **kw: nullcontext())
    monkeypatch.setattr(
        packages,
        'is_installed',
        lambda name, **kw: checks.append((name, kw['venv'], kw['allow_outside_venv'])) or True,
    )
    packages._is_installed_first_check.clear()

    for venv, allow_outside_venv in (('one', True), ('one', False), ('two', True)):
        packages.attempt_import(
            'json',
            venv=venv,
            allow_outside_venv=allow_outside_venv,
            install=False,
            lazy=False,
        )
    packages.attempt_import('json', venv='one', install=False, lazy=False)

    assert checks == [
        ('json', 'one', True),
        ('json', 'one', False),
        ('json', 'two', True),
    ]


def test_attempt_import_no_auto_install(monkeypatch):
    """Locked environments may disable import-time package installation."""
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', 'true')
    monkeypatch.setattr(packages, 'Venv', lambda *args, **kw: nullcontext())
    monkeypatch.setattr(packages, 'is_installed', lambda *args, **kw: False)
    monkeypatch.setattr(
        packages,
        'pip_install',
        lambda *args, **kw: (_ for _ in ()).throw(AssertionError("pip_install was called")),
    )
    packages._is_installed_first_check.clear()

    assert packages.attempt_import(
        '_mrsm_missing_package_',
        lazy=False,
        warn=False,
    ) is None


def test_attempt_import_warns_once_before_auto_install(monkeypatch):
    """Implicit dependency downloads must announce themselves once per process."""
    import warnings
    import meerschaum.utils.packages as packages

    monkeypatch.setattr(packages, 'Venv', lambda *args, **kw: nullcontext())
    monkeypatch.setattr(packages, 'is_installed', lambda *args, **kw: False)
    monkeypatch.setattr(packages, 'pip_install', lambda *args, **kw: True)
    monkeypatch.setattr(packages, 'emitted_auto_install_warning', False)
    packages._is_installed_first_check.clear()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        for _ in range(2):
            packages.attempt_import('pandas', lazy=False)

    assert len([
        warning
        for warning in caught
        if 'installing a missing runtime dependency' in str(warning.message)
    ]) == 1


def test_attempt_import_rechecks_missing_packages(monkeypatch):
    """A dependency installed by another process must not stay cached as missing."""
    import meerschaum.utils.packages as packages

    checks = iter((False, True))
    monkeypatch.setattr(packages, 'Venv', lambda *args, **kw: nullcontext())
    monkeypatch.setattr(packages, 'is_installed', lambda *args, **kw: next(checks))
    packages._is_installed_first_check.clear()

    for _ in range(2):
        packages.attempt_import('json', install=False, warn=False, lazy=False)

    assert packages._is_installed_first_check[('json', 'mrsm', True, True)] is True


def test_pip_install_preflight_is_read_only(monkeypatch, tmp_path):
    """Preflight reports policy without invoking package or environment mutations."""
    import meerschaum.utils.packages as packages
    import meerschaum.config.paths as paths

    def fail(*args, **kw):
        raise AssertionError("preflight attempted a mutation")

    monkeypatch.setattr(packages, 'run_python_package', fail)
    monkeypatch.setattr(packages, 'init_venv', fail)
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: False)
    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', 'true')
    monkeypatch.setattr(paths, 'VIRTENV_RESOURCES_PATH', tmp_path / 'venvs')
    monkeypatch.setattr(paths, 'VENVS_CACHE_RESOURCES_PATH', tmp_path / 'cache')
    plan = packages.get_pip_install_plan('example>=1', venv='preflight')

    assert plan['packages'] == ['example>=1']
    assert plan['environment'] == 'preflight'
    assert plan['installer'] == 'pip'
    assert plan['pip_fallback'] is False
    assert plan['operation'] == 'install'
    assert plan['auto_install_enabled'] is False
    assert list(tmp_path.iterdir()) == []


def test_run_python_package_forwards_explicit_environment(monkeypatch):
    """Package subprocesses must receive the caller's isolated environment."""
    import meerschaum.utils.packages as packages
    import meerschaum.utils.process as process

    captured = {}
    monkeypatch.setattr(packages, 'venv_executable', lambda **kwargs: 'python')
    monkeypatch.setattr(
        process,
        'run_process',
        lambda *args, **kwargs: captured.update(kwargs) or 0,
    )

    assert packages.run_python_package('example', env={'ONLY_THIS': 'value'}, venv=None) == 0
    assert captured['env'] == {'ONLY_THIS': 'value'}


def test_pip_install_dry_run_skips_lock_and_mutation(monkeypatch):
    """A dry run must not create a lock or execute an installer."""
    import meerschaum.utils.packages as packages

    monkeypatch.setattr(
        packages,
        '_pip_install_lock',
        lambda *args, **kw: (_ for _ in ()).throw(AssertionError("lock was created")),
    )
    monkeypatch.setattr(
        packages,
        'get_pip_install_plan',
        lambda *args, **kw: {'packages': list(args)},
    )
    monkeypatch.setattr(
        packages,
        'run_python_package',
        lambda *args, **kw: (_ for _ in ()).throw(AssertionError("installer was executed")),
    )

    assert packages.pip_install('example', dry_run=True, silent=True)


def test_pip_install_lock_serializes_processes(tmp_path):
    """Two installers targeting one environment may not overlap."""
    import multiprocessing

    context = multiprocessing.get_context('spawn')
    active = context.Value('i', 0)
    max_active = context.Value('i', 0)
    start = context.Event()
    processes = [
        context.Process(
            target=_hold_package_install_lock,
            args=(str(tmp_path), active, max_active, start),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    assert max_active.value == 1


def test_pip_install_lock_is_reentrant(tmp_path, monkeypatch):
    """Nested package installs in one thread must not deadlock."""
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages

    monkeypatch.setattr(paths, 'VIRTENV_RESOURCES_PATH', tmp_path / 'venvs')
    with packages._pip_install_lock('nested'):
        with packages._pip_install_lock('nested'):
            pass


def test_pip_install_lock_path_tracks_target_not_root(tmp_path, monkeypatch):
    """Different Meerschaum roots must share a lock for the current interpreter."""
    import sys
    from pathlib import Path
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages

    monkeypatch.setattr(paths, 'VENVS_CACHE_RESOURCES_PATH', tmp_path / 'root-one')
    first_lock_path = packages.get_pip_install_lock_path(None)
    monkeypatch.setattr(paths, 'VENVS_CACHE_RESOURCES_PATH', tmp_path / 'root-two')
    second_lock_path = packages.get_pip_install_lock_path(None)

    assert first_lock_path == second_lock_path
    assert packages._get_pip_install_target_path(None) == Path(sys.prefix)


def test_interprocess_lock_windows(monkeypatch, tmp_path):
    """The Windows path must acquire and release a byte lock."""
    import sys
    import types
    import meerschaum.utils.locks as locks

    calls = []
    fake_msvcrt = types.SimpleNamespace(
        LK_NBLCK=1,
        LK_UNLCK=2,
        locking=lambda fileno, operation, length: calls.append((operation, length)),
    )
    monkeypatch.setattr(locks, '_IS_WINDOWS', True)
    monkeypatch.setitem(sys.modules, 'msvcrt', fake_msvcrt)

    with locks.InterProcessLock(tmp_path / 'install.lock'):
        assert calls == [(1, 1)]

    assert calls == [(1, 1), (2, 1)]


### V4-002: `MRSM_NO_AUTO_INSTALL` disables every implicit install.

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_NO_AUTO_INSTALL_VALUES = ('1', 'true', 'yes', 'TRUE', 'Yes')


def _is_venv_creation(package_name, args):
    """Creating the (empty) `mrsm` venv is not a package installation."""
    return package_name == 'venv' or (package_name == 'uv' and bool(args) and args[0] == 'venv')


@pytest.fixture
def deny_installs(monkeypatch):
    """Record every installation attempt and refuse to run any of them."""
    import meerschaum.utils.packages as packages

    attempts = []
    run_python_package = packages.run_python_package

    def _refuse(kind):
        def _inner(*args, **kw):
            attempts.append((kind, args, kw))
            return False
        return _inner

    def _run_python_package(package_name, args=None, *a, **kw):
        if _is_venv_creation(package_name, args):
            return run_python_package(package_name, args, *a, **kw)
        attempts.append(('run_python_package', (package_name, args), kw))
        return 1

    monkeypatch.setattr(packages, 'pip_install', _refuse('pip_install'))
    monkeypatch.setattr(packages, 'get_pip', _refuse('get_pip'))
    monkeypatch.setattr(packages, 'run_python_package', _run_python_package)
    packages._is_installed_first_check.clear()
    monkeypatch.setattr(packages, '_checked_for_updates', set())
    return attempts


def _hide_semver(monkeypatch):
    """Make `semver` look uninstalled, whatever the test environment has."""
    import meerschaum.utils.packages as packages

    get_module_path = packages.get_module_path
    attempt_import = packages.attempt_import
    monkeypatch.setattr(
        packages,
        'get_module_path',
        lambda name, *a, **kw: None if name == 'semver' else get_module_path(name, *a, **kw),
    )
    monkeypatch.setattr(
        packages,
        'attempt_import',
        lambda *names, **kw: (
            None if names == ('semver',) else attempt_import(*names, **kw)
        ),
    )


@pytest.mark.parametrize('value', _NO_AUTO_INSTALL_VALUES)
def test_auto_install_guard_truthy_values(monkeypatch, value):
    """One helper parses the variable for every caller."""
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', value)
    assert packages._auto_install_allowed() is False


@pytest.mark.parametrize('value', (None, '', '0', 'false', 'no'))
def test_auto_install_guard_default_allows(monkeypatch, value):
    """Unset or falsy leaves installation on, as in v4."""
    import meerschaum.utils.packages as packages

    if value is None:
        monkeypatch.delenv('MRSM_NO_AUTO_INSTALL', raising=False)
    else:
        monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', value)
    assert packages._auto_install_allowed() is True


def test_no_auto_install_precheck_chain_never_installs(monkeypatch, deny_installs):
    """Imports and dependency prechecks make no installation attempt."""
    import meerschaum as mrsm
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    _hide_semver(monkeypatch)

    assert mrsm.Pipe('a', 'b').metric_key == 'b'
    assert packages.attempt_import(
        'nonexistent_pkg_xyz', lazy=False, warn=False,
    ) is None
    assert packages.is_installed('nonexistent_pkg_xyz') is False
    ### A declared package with a minimum version: this is the `need_update()` -> `semver` path.
    assert isinstance(packages.is_installed('pandas'), bool)
    assert deny_installs == []


def test_no_auto_install_blocks_implicit_pip_install(monkeypatch):
    """`_implicit=True` installs nothing and does not even take the install lock."""
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    monkeypatch.setattr(
        packages, '_pip_install_lock',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("lock was taken")),
    )
    monkeypatch.setattr(
        packages, 'run_python_package',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("an installer ran")),
    )
    assert packages.pip_install('example', _implicit=True) is False


def test_no_auto_install_keeps_explicit_pip_install_without_bootstrap(monkeypatch, tmp_path):
    """An explicit install still runs, but never bootstraps uv, wheel, setuptools or pip."""
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages

    calls = []
    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    monkeypatch.setattr(paths, 'VIRTENV_RESOURCES_PATH', tmp_path / 'venvs')
    monkeypatch.setattr(paths, 'VENVS_CACHE_RESOURCES_PATH', tmp_path / 'cache')
    monkeypatch.setattr(packages, 'get_pip_install_lock_path', lambda *a, **kw: tmp_path / 'l.lock')
    monkeypatch.setattr(packages, '_get_uv_bin', lambda: None)
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: True)
    monkeypatch.setattr(packages, 'Venv', lambda *a, **kw: nullcontext())
    monkeypatch.setattr(packages, 'venv_contains_package', lambda *a, **kw: False)
    monkeypatch.setattr(packages, '_update_request_ledger', lambda *a, **kw: None)
    monkeypatch.setattr(
        packages, 'get_pip',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("get-pip.py was fetched")),
    )
    monkeypatch.setattr(
        packages, 'run_python_package',
        lambda name, args=None, *a, **kw: calls.append((name, list(args or []))) or 0,
    )

    assert packages.pip_install('example>=1', venv=None, silent=True) is True
    assert len(calls) == 1
    name, args = calls[0]
    assert name == 'pip'
    assert 'example>=1' in args
    for bootstrap in ('uv', 'wheel', 'setuptools', 'PyYAML'):
        assert bootstrap not in args


def test_no_auto_install_without_installer_does_not_fetch_get_pip(monkeypatch, tmp_path):
    """No `uv` and no `pip`: report the failure instead of downloading `get-pip.py`."""
    import builtins
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages

    real_import = builtins.__import__

    def _no_pip(name, *args, **kw):
        if name == 'pip':
            raise ImportError(name)
        return real_import(name, *args, **kw)

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    monkeypatch.setattr(builtins, '__import__', _no_pip)
    monkeypatch.setattr(paths, 'VIRTENV_RESOURCES_PATH', tmp_path / 'venvs')
    monkeypatch.setattr(packages, 'get_pip_install_lock_path', lambda *a, **kw: tmp_path / 'l.lock')
    monkeypatch.setattr(packages, '_get_uv_bin', lambda: None)
    monkeypatch.setattr(packages, 'venv_contains_package', lambda *a, **kw: False)
    monkeypatch.setattr(
        packages, 'get_pip',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("get-pip.py was fetched")),
    )
    monkeypatch.setattr(
        packages, 'run_python_package',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("an installer ran")),
    )
    assert packages.pip_install('example', venv='mrsm', silent=True) is False


def test_init_venv_does_not_seed_when_auto_install_is_disabled(monkeypatch, tmp_path):
    """`uv venv --seed` downloads pip, setuptools and wheel, so it is left out."""
    import meerschaum.config.paths as paths
    import meerschaum.utils.packages as packages
    import meerschaum.utils.venv as venv_module

    seen = []
    monkeypatch.setattr(paths, 'VIRTENV_RESOURCES_PATH', tmp_path / 'venvs')
    monkeypatch.setattr(paths, 'VENVS_CACHE_RESOURCES_PATH', tmp_path / 'cache')
    (tmp_path / 'cache').mkdir()
    monkeypatch.setattr(packages, '_get_uv_bin', lambda: 'uv')
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: True)

    def _record(package_name, args=None, *a, **kw):
        seen.append((package_name, list(args or [])))
        raise RuntimeError("stop after the first call")

    monkeypatch.setattr(packages, 'run_python_package', _record)
    for value, expect_seed in (('1', False), ('', True)):
        seen.clear()
        monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', value)
        try:
            venv_module.init_venv('mrsm_v4_002', force=True)
        except RuntimeError:
            pass
        assert seen and seen[0][0] == 'uv'
        assert ('--seed' in seen[0][1]) is expect_seed


def test_need_update_without_a_version_parser_compares_pep440(monkeypatch, deny_installs):
    """No semver and no installs: the Rust (or `packaging`) comparison answers."""
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    _hide_semver(monkeypatch)

    def _need_update(name, installed):
        packages._checked_for_updates.clear()
        return packages.need_update(
            None,
            install_name=f'{name}>=2.0.0',
            import_name=name,
            version=installed,
            check_pypi=True,
        )

    assert _need_update('_mrsm_fake_a', '1.9.9') is True
    assert _need_update('_mrsm_fake_b', '2.0.0') is False
    assert _need_update('_mrsm_fake_c', '2.10.1') is False
    assert _need_update('_mrsm_fake_d', '2.0') is False
    assert deny_installs == []


def test_need_update_with_a_version_parser_uses_it(monkeypatch, deny_installs):
    """An already-installed semver keeps answering, with no install either."""
    import meerschaum.utils.packages as packages

    class _Version:
        def __init__(self, parts):
            self.parts = parts

        @classmethod
        def parse(cls, value):
            return cls(tuple(int(p) for p in value.split('.')))

        def match(self, requirement):
            return self.parts >= tuple(int(p) for p in requirement.lstrip('>=').split('.'))

    attempt_import = packages.attempt_import
    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    monkeypatch.setattr(packages, 'get_module_path', lambda name, *a, **kw: Path('semver.py'))
    monkeypatch.setattr(
        packages,
        'attempt_import',
        lambda *names, **kw: (
            SimpleNamespace(Version=_Version) if names == ('semver',)
            else attempt_import(*names, **kw)
        ),
    )
    kwargs = dict(install_name='_mrsm_fake_e>=2.0.0', import_name='_mrsm_fake_e')
    assert packages.need_update(None, version='1.0.0', **kwargs) is True
    packages._checked_for_updates.clear()
    assert packages.need_update(None, version='2.1.0', **kwargs) is False
    assert deny_installs == []


def test_need_update_without_any_parser_assumes_satisfied(monkeypatch, deny_installs):
    """With no semver, no Rust binding and no `packaging`, the answer is 'no update needed'."""
    import meerschaum
    import meerschaum.utils.packages as packages

    monkeypatch.setenv('MRSM_NO_AUTO_INSTALL', '1')
    _hide_semver(monkeypatch)
    monkeypatch.setattr(meerschaum, '_core', SimpleNamespace())
    attempt_import = packages.attempt_import
    monkeypatch.setattr(
        packages,
        'attempt_import',
        lambda *names, **kw: (
            (None,) * len(names) if len(names) > 1 else None
        ) if names[0] in ('semver', 'packaging.specifiers', 'packaging.version') else (
            attempt_import(*names, **kw)
        ),
    )
    assert packages._version_satisfies_without_semver('1.0.0', '>=2.0.0') is None
    assert packages.need_update(
        None, install_name='_mrsm_fake_f>=2.0.0', import_name='_mrsm_fake_f', version='1.0.0',
    ) is False
    assert deny_installs == []


def test_version_matching_falls_back_to_packaging(monkeypatch):
    """Without a Rust binding, `packaging` answers when it is importable."""
    import meerschaum
    import meerschaum.utils.packages as packages

    pytest.importorskip('packaging')
    monkeypatch.setattr(meerschaum, '_core', SimpleNamespace())
    assert packages._version_satisfies_without_semver('2.1.0', '>=2.0.0') is True
    assert packages._version_satisfies_without_semver('1.1.0', '>=2.0.0,<3') is False
    assert packages._version_satisfies_without_semver('1.0.0', '') is True


def test_rust_version_binding_agrees_with_packaging():
    """`_core.version_satisfies` (when built) matches PEP 440 the way `packaging` does."""
    from meerschaum import _core

    if not _core.load() or not hasattr(_core, 'version_satisfies'):
        pytest.skip("the built extension does not bind version_satisfies")
    packaging_specifiers = pytest.importorskip('packaging.specifiers')
    packaging_version = pytest.importorskip('packaging.version')
    for version, spec in (
        ('3.0.2', '>=3.0.2'),
        ('3.0.1', '>=3.0.2'),
        ('1.5', '>=1.2,<2'),
        ('2.0.0rc1', '>=2.0.0rc1'),
        ('2.0.0rc1', '>=2.0.0'),
        ('1.0.post1', '>=1.0'),
        ('1.0+local', '>=1.0'),
    ):
        expected = packaging_specifiers.SpecifierSet(spec, prereleases=True).contains(
            packaging_version.Version(version)
        )
        assert _core.version_satisfies(version, spec) is expected, (version, spec)
    assert _core.version_satisfies('not a version', '>=1.0') is None
    assert _core.version_satisfies('1.0', 'not a specifier') is None


def test_unset_variable_keeps_the_v4_install_attempts(monkeypatch, deny_installs):
    """Without the opt-out the semver bootstrap and import installs still happen."""
    import meerschaum.utils.packages as packages

    monkeypatch.delenv('MRSM_NO_AUTO_INSTALL', raising=False)
    _hide_semver(monkeypatch)
    ### A failed semver install is the v4 outcome: no update is reported.
    assert packages.need_update(
        None, install_name='_mrsm_fake_g>=2.0.0', import_name='_mrsm_fake_g', version='1.0.0',
    ) is False
    semver_attempts = [a for a in deny_installs if a[0] == 'pip_install']
    assert semver_attempts and semver_attempts[0][1] == ('semver>=3.0.2',)

    deny_installs.clear()
    packages.attempt_import('_mrsm_missing_pkg_', lazy=False, warn=False)
    assert [a[1] for a in deny_installs if a[0] == 'pip_install'] == [('_mrsm_missing_pkg_',)]


_PROBE_PRELUDE = textwrap.dedent('''
    import os, sys
    import meerschaum.utils.packages as packages

    marker = os.environ['MRSM_TEST_ATTEMPTS']

    def _record(kind):
        def _inner(*args, **kw):
            with open(marker, 'a') as f:
                f.write(kind + ' ' + repr(args)[:200] + '\\n')
            return False
        return _inner

    run_python_package = packages.run_python_package

    def _run_python_package(package_name, args=None, *a, **kw):
        if package_name == 'venv' or (package_name == 'uv' and args and args[0] == 'venv'):
            return run_python_package(package_name, args, *a, **kw)
        return _record('run_python_package')(package_name, args)

    packages.pip_install = _record('pip_install')
    packages.get_pip = _record('get_pip')
    packages.run_python_package = _run_python_package
''')


def _clean_env(tmp_path, **extra):
    """An isolated environment: a temp root, no plugins, no unrelated `MRSM_*` variables."""
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir(exist_ok=True)
    plugins.mkdir(exist_ok=True)
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('MRSM_') or k == 'MRSM_CORE'
    }
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': str(_REPO_ROOT),
        'HOME': str(tmp_path),
        'MRSM_TEST_ATTEMPTS': str(tmp_path / 'attempts.txt'),
    })
    env.update(extra)
    return env


def test_facade_startup_makes_no_install_attempt(tmp_path):
    """`import meerschaum`, `Pipe(...)`, an unknown import and `is_installed` install nothing."""
    code = _PROBE_PRELUDE + textwrap.dedent('''
        import meerschaum as mrsm
        mrsm.Pipe('a', 'b')
        assert packages.attempt_import('nonexistent_pkg_xyz', lazy=False, warn=False) is None
        assert packages.is_installed('nonexistent_pkg_xyz') is False
        packages.is_installed('pandas')
        print('probe ok')
    ''')
    result = subprocess.run(
        [sys.executable, '-c', code],
        env=_clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert 'probe ok' in result.stdout
    assert not (tmp_path / 'attempts.txt').exists(), (tmp_path / 'attempts.txt').read_text()
    assert 'semver' not in result.stderr


def test_api_host_help_makes_no_install_attempt(tmp_path):
    """The API host imports without installing or warning about `uv` or `semver`."""
    env = _clean_env(tmp_path)
    ### Probe the API extras inside the very environment the host will run in (same interpreter,
    ### HOME and roots): the parent's user site-packages may not be visible there.
    probe = subprocess.run(
        [
            sys.executable, '-c',
            'import fastapi, fastapi_login, uvicorn, aiofiles, multipart, '
            + 'importlib_metadata, typing_extensions',
        ],
        env=env,
        capture_output=True,
        cwd=str(tmp_path),
    )
    api_extras_importable = probe.returncode == 0

    code = _PROBE_PRELUDE + textwrap.dedent('''
        import runpy
        sys.argv = ['meerschaum.api.host', '--help']
        runpy.run_module('meerschaum.api.host', run_name='__main__', alter_sys=True)
    ''')
    result = subprocess.run(
        [sys.executable, '-c', code],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
    )
    attempts = tmp_path / 'attempts.txt'
    assert not attempts.exists(), attempts.read_text()
    output = result.stdout + result.stderr
    assert 'semver' not in output
    assert "'uv'" not in output and 'package uv' not in output.lower()
    if api_extras_importable:
        assert result.returncode == 0, output
        assert '--socket' in result.stdout


def test_api_package_does_not_import_uv():
    """uv is found with `shutil.which`, never imported or installed at API import time."""
    source = (_REPO_ROOT / 'meerschaum' / 'api' / '__init__.py').read_text()
    assert "attempt_import('uv'" not in source
    assert 'attempt_import("uv"' not in source


def test_offline_fresh_environment_imports_without_pip_or_uv(tmp_path):
    """A bare interpreter with no index and no installer starts the facade silently."""
    import re
    import venv

    if os.environ.get('MRSM_CORE', 'python').split(':')[0] == 'rust':
        from meerschaum import _core
        if not _core.load():
            pytest.skip("MRSM_CORE=rust needs the built extension")
    env_dir = tmp_path / 'bare'
    venv.EnvBuilder(with_pip=False, clear=True).create(env_dir)
    python = env_dir / ('Scripts' if os.name == 'nt' else 'bin') / (
        'python.exe' if os.name == 'nt' else 'python'
    )
    env = _clean_env(tmp_path, PIP_NO_INDEX='1')
    result = subprocess.run(
        [str(python), '-c', "import meerschaum as m; m.Pipe('a', 'b'); print('ok')"],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'ok'
    assert not re.search(r'\b(pip|uv)\b', result.stderr, re.IGNORECASE), result.stderr
