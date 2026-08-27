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


def test_stdlib_interprocess_lock_windows(monkeypatch, tmp_path):
    """The source-checkout fallback must acquire and release a Windows byte lock."""
    import sys
    import types
    import platform
    import meerschaum.utils.packages as packages

    calls = []
    fake_msvcrt = types.SimpleNamespace(
        LK_NBLCK=1,
        LK_UNLCK=2,
        locking=lambda fileno, operation, length: calls.append((operation, length)),
    )
    monkeypatch.setattr(platform, 'system', lambda: 'Windows')
    monkeypatch.setitem(sys.modules, 'msvcrt', fake_msvcrt)

    with packages._stdlib_interprocess_lock(tmp_path / 'install.lock'):
        pass

    assert calls == [(fake_msvcrt.LK_NBLCK, 1), (fake_msvcrt.LK_UNLCK, 1)]
