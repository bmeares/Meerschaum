#! /usr/bin/env python3

from contextlib import nullcontext


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
