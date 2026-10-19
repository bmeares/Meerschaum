"""Base activation may reuse a visible system site when HOME is read-only; no installs."""
import errno
import os
from pathlib import Path
import platform
import site
import sys

import pytest
import meerschaum.utils.venv as venvs


@pytest.fixture
def base_sites(tmp_path, monkeypatch):
    user = tmp_path / 'home' / '.local' / 'site-packages'
    base = tmp_path / 'base' / 'site-packages'
    base.mkdir(parents=True)
    monkeypatch.setattr(site, 'getusersitepackages', lambda: str(user))
    monkeypatch.setattr(site, 'getsitepackages', lambda: [str(base)])
    monkeypatch.setattr(platform, 'system', lambda: 'Linux')
    monkeypatch.setattr(os, 'geteuid', lambda: 1000, raising=False)
    monkeypatch.delenv('MRSM_RUNTIME', raising=False)
    monkeypatch.setattr(venvs, 'inside_venv', lambda: False)
    monkeypatch.setattr(sys, 'path', [*sys.path, str(base)])
    for name, value in (
        ('active_venvs', set()), ('active_venvs_counts', {}), ('active_venvs_order', []),
        ('threads_active_venvs', {}),
    ):
        monkeypatch.setattr(venvs, name, value)
    return user, base


def deny_user_mkdir(monkeypatch, user, code):
    original = Path.mkdir
    error = OSError(code, os.strerror(code), str(user))
    def mkdir(path, *args, **kwargs):
        if path == user:
            raise error
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'mkdir', mkdir)
    return error


def forbid_init(*args, **kwargs):
    raise AssertionError('Base activation must not create or install a named environment')


@pytest.mark.parametrize('code', [errno.EACCES, errno.EROFS])
def test_readonly_base_activation_reuses_visible_global_site(base_sites, monkeypatch, code):
    user, base = base_sites
    deny_user_mkdir(monkeypatch, user, code)
    monkeypatch.setattr(venvs, 'init_venv', forbid_init)
    with venvs.Venv(None):
        assert sys.path[0] == str(base)
        assert None in venvs.active_venvs
    assert None not in venvs.active_venvs
    assert not user.exists()
    assert sys.path.count(str(base)) == 1
    # Install-target resolution must still report the original inaccessible user site.
    with pytest.raises(OSError) as caught:
        venvs.venv_target_path(None, allow_nonexistent=True)
    assert caught.value.errno == code


@pytest.mark.parametrize('existing', [False, True])
def test_writable_or_existing_user_site_keeps_precedence(base_sites, monkeypatch, existing):
    user, base = base_sites
    if existing:
        user.mkdir(parents=True)
        deny_user_mkdir(monkeypatch, user, errno.EROFS)
    monkeypatch.setattr(venvs, 'init_venv', forbid_init)
    assert venvs.Venv(None).activate()
    assert user.is_dir()
    assert sys.path[0] == str(user)
    assert sys.path.index(str(user)) < sys.path.index(str(base))


@pytest.mark.parametrize('code', [errno.EACCES, errno.EROFS, errno.EIO])
def test_base_error_without_eligible_fallback_is_preserved(base_sites, monkeypatch, code):
    user, base = base_sites
    error = deny_user_mkdir(monkeypatch, user, code)
    if code != errno.EIO:
        monkeypatch.setattr(site, 'getsitepackages', lambda: [str(base / 'missing')])
    monkeypatch.setattr(venvs, 'init_venv', forbid_init)
    with pytest.raises(OSError) as caught:
        venvs.Venv(None).activate()
    assert caught.value is error
    assert venvs.active_venvs == set()
    assert venvs.threads_active_venvs == {}


def test_named_environment_keeps_oserror_recovery(base_sites, monkeypatch):
    _, base = base_sites
    calls = []
    targets = iter([OSError(errno.EROFS, 'first activation failed'), base])
    def target(name, **kwargs):
        assert name == 'plugin'
        value = next(targets)
        if isinstance(value, OSError):
            raise value
        return value
    def init(venv, force=False, **kwargs):
        calls.append((venv, force))
        return True
    monkeypatch.setattr(venvs, 'venv_target_path', target)
    monkeypatch.setattr(venvs, 'init_venv', init)
    assert venvs.Venv('plugin').activate()
    assert calls == [('plugin', False), ('plugin', True), ('plugin', False)]
    assert sys.path[0] == str(base)


def test_real_venv_layout_does_not_use_user_site_fallback(base_sites, tmp_path, monkeypatch):
    user, _ = base_sites
    root = tmp_path / 'actual-venv'
    expected = root / 'lib' / f'python{sys.version_info.major}.{sys.version_info.minor}' / 'site-packages'
    expected.mkdir(parents=True)
    monkeypatch.setattr(sys, 'prefix', str(root))
    monkeypatch.setattr(sys, 'base_prefix', str(tmp_path / 'system-python'))
    monkeypatch.setattr(venvs, 'inside_venv', lambda: sys.prefix != sys.base_prefix)
    deny_user_mkdir(monkeypatch, user, errno.EROFS)
    monkeypatch.setattr(venvs, 'init_venv', forbid_init)
    assert venvs.Venv(None).activate()
    assert sys.path[0] == str(expected)
    assert not user.exists()


def test_fallback_preserves_plugin_base_shared_order(base_sites, tmp_path, monkeypatch):
    user, base = base_sites
    deny_user_mkdir(monkeypatch, user, errno.EROFS)
    named = {name: tmp_path / name / 'site-packages' for name in ('mrsm', 'plugin')}
    for path in named.values():
        path.mkdir(parents=True)
    original = venvs.venv_target_path
    def target(name, **kwargs):
        return original(name, **kwargs) if name is None else named[name]
    monkeypatch.setattr(venvs, 'venv_target_path', target)
    monkeypatch.setattr(venvs, 'init_venv', forbid_init)
    assert venvs.activate_venv('mrsm', init_if_not_exists=False)
    assert venvs.activate_venv(None, init_if_not_exists=False)
    assert venvs.activate_venv('plugin', init_if_not_exists=False)
    assert sys.path[:3] == [str(named['plugin']), str(base), str(named['mrsm'])]
    assert {'mrsm', 'plugin', None} <= venvs.active_venvs
