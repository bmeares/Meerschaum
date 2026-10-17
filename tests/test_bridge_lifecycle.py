"""Worker interrupts and fresh plugin venvs use reusable, isolated resources."""

import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import time

import pytest

from meerschaum.config._paths import replace_root_dir, replace_venvs_dir_path
from meerschaum.worker._frames import encode_message, read_message


def _subprocess_env(root, plugins):
    # MRSM_TEST_* variables are also interpreted as connector definitions. Bare
    # bootstrap interpreters must not inherit the surrounding integration-test setup.
    return {
        **{key: value for key, value in os.environ.items() if not key.startswith('MRSM_')},
        'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_VENVS_DIR': str(root / 'venvs'),
    }


def test_external_uv_installs_without_pip_or_uv_module(tmp_path):
    uv_bin = shutil.which('uv')
    if uv_bin is None:
        pytest.skip('uv executable required')
    from tests.test_w9_venvs import _make_wheel
    wheels = tmp_path / 'wheels'
    wheels.mkdir()
    _make_wheel(wheels, 'bridge_dependency', '1.0')
    interpreter_dir = tmp_path / 'interpreter'
    subprocess.run(
        [uv_bin, 'venv', '--python', sys.executable, '--offline', '--no-project', str(interpreter_dir)],
        capture_output=True, check=True, timeout=30,
    )
    interpreter = interpreter_dir / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    code = """
import importlib.util, pathlib, sys
assert importlib.util.find_spec('pip') is None
assert importlib.util.find_spec('uv') is None
assert importlib.util.find_spec('yaml') is None
from meerschaum.utils.packages import pip_install, pip_uninstall
from meerschaum.utils.venv import venv_target_path
assert pip_install('bridge-dependency', args=['--no-index', '--find-links', sys.argv[1]],
                   venv='bootstrap', silent=True)
site = venv_target_path('bootstrap')
assert site.is_relative_to(pathlib.Path(sys.argv[1]).parent)
assert (site / 'bridge_dependency' / '__init__.py').exists()
assert pip_uninstall('bridge-dependency', venv='bootstrap', silent=True)
assert not (site / 'bridge_dependency').exists()
assert importlib.util.find_spec('pip') is None
assert importlib.util.find_spec('uv') is None
"""
    result = subprocess.run(
        [str(interpreter), '-c', code, str(wheels)],
        env={**_subprocess_env(tmp_path, plugins), 'PYTHONNOUSERSITE': '1', 'UV_OFFLINE': '1',
             'MRSM_NO_AUTO_INSTALL': '1'},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'Traceback' not in result.stderr

    # Exercise the native host with legacy plugin discovery in the same bare interpreter.
    binary = Path(__file__).resolve().parents[1] / 'target' / 'debug' / ('mrsm.exe' if os.name == 'nt' else 'mrsm')
    if binary.exists():
        (plugins / 'bridge_demo.py').write_text(
            "from meerschaum.plugins import make_action\n"
            "@make_action\ndef bridge_demo(**kw):\n    return True, 'bridge ready'\n"
        )
        for command, expected in [
            (['show', 'version', '+', 'show', 'version', ':', '--nopretty'], 'Meerschaum v'),
            (['bridge', 'demo'], 'bridge ready'),
        ]:
            native = subprocess.run(
                [str(binary), *command],
                env={**_subprocess_env(tmp_path, plugins), 'PYTHONNOUSERSITE': '1', 'UV_OFFLINE': '1',
                     'MRSM_PYTHON': str(interpreter), 'MRSM_NO_AUTO_INSTALL': '1'},
                capture_output=True, text=True, timeout=30,
            )
            assert native.returncode == 0, native.stdout + native.stderr
            assert expected in native.stdout
            assert not native.stderr, native.stderr


def test_version_check_survives_unavailable_bootstrap_dependencies(monkeypatch):
    from types import SimpleNamespace
    from meerschaum.utils import packages
    monkeypatch.setattr(packages, 'attempt_import', lambda *a, **kw: None)
    monkeypatch.setattr(packages, 'get_module_path', lambda *a, **kw: None)
    monkeypatch.setattr(packages, 'pip_install', lambda *a, **kw: False)
    packages._checked_for_updates.discard('bridge_dependency>=1.0')
    assert not packages.need_update(
        SimpleNamespace(__name__='bridge_dependency'), version='1.0.0',
        install_name='bridge_dependency>=1.0',
    )


def test_package_install_uses_running_python(tmp_path, monkeypatch):
    from meerschaum.utils.packages import _get_pip_os_env
    preference = str(tmp_path / 'another-python')
    monkeypatch.setenv('UV_PYTHON', preference)
    assert _get_pip_os_env()['UV_PYTHON'] == sys.executable
    assert os.environ['UV_PYTHON'] == preference


def test_package_install_rechecks_uv_after_bootstrap(monkeypatch):
    from contextlib import nullcontext
    from meerschaum.utils import packages

    monkeypatch.delenv('MRSM_NO_AUTO_INSTALL', raising=False)
    calls = []
    monkeypatch.setattr(packages, '_get_uv_bin', lambda: '/external/uv' if calls else None)
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: True)
    monkeypatch.setattr(packages, 'Venv', lambda *a, **kw: nullcontext())

    def run(package, args, **kw):
        calls.append((package, args))
        return 0

    monkeypatch.setattr(packages, 'run_python_package', run)
    assert packages.pip_install(
        'bridge-dependency', venv=None, check_wheel=False,
        check_update=False, check_pypi=False, silent=True,
    )
    assert [package for package, _ in calls] == ['pip', 'uv']
    assert calls[1][1][0] == 'pip'


def test_venv_releases_creation_lock_before_verification(tmp_path, monkeypatch):
    from meerschaum.utils import packages, venv

    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: False)
    monkeypatch.setattr(packages, 'run_python_package', lambda *a, **kw: 0)
    monkeypatch.setattr(venv, 'venv_exists', lambda *a, **kw: False)
    verified = []

    def verify(name, **kw):
        assert not (tmp_path / '.cache' / 'venvs' / (name + '.lock')).exists()
        verified.append(name)

    monkeypatch.setattr(venv, 'verify_venv', verify)
    name = 'bridge_verification'
    try:
        with replace_root_dir(tmp_path), replace_venvs_dir_path(tmp_path / 'venvs'):
            assert venv.init_venv(name)
            assert verified == [name]
    finally:
        venv.verified_venvs.discard(name)


def test_venv_uses_running_python_despite_uv_preference(tmp_path, monkeypatch):
    uv = pytest.importorskip('uv')
    from meerschaum.utils import packages, venv

    monkeypatch.setenv('UV_PYTHON', str(tmp_path / 'unavailable-python'))
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: True)
    monkeypatch.setattr(packages, 'attempt_import', lambda *a, **kw: uv)
    calls = []

    def run(package, args, **kw):
        assert package == 'uv', 'uv must create the venv without falling back'
        calls.append(args)
        # Seeding pip is unrelated to interpreter selection; keep this test offline.
        result = subprocess.run(
            [sys.executable, '-m', 'uv', *[a for a in args if a != '--seed'], '--offline'],
            env=kw['env'], capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        return result.returncode

    monkeypatch.setattr(packages, 'run_python_package', run)
    name = 'bridge_python'
    venv.verified_venvs.discard(name)
    try:
        with replace_root_dir(tmp_path), replace_venvs_dir_path(tmp_path / 'venvs'):
            assert venv.init_venv(name)
            assert len(calls) == 1
            assert venv.venv_exists(name)
            version = subprocess.check_output(
                [venv.venv_executable(name), '-c', 'import sys; print(sys.version_info[:2])'],
                text=True,
            ).strip()
            assert version == str(sys.version_info[:2])
            assert not (tmp_path / '.cache' / 'venvs' / (name + '.lock')).exists()
    finally:
        venv.verified_venvs.discard(name)


@pytest.mark.parametrize('exception', [RuntimeError, KeyboardInterrupt])
def test_venv_creation_releases_lock_on_failure(tmp_path, monkeypatch, exception):
    from meerschaum.utils import packages, venv
    monkeypatch.setattr(packages, 'is_uv_enabled', lambda: False)

    def fail(*args, **kw):
        raise exception('creation interrupted')

    monkeypatch.setattr(packages, 'run_python_package', fail)
    with replace_root_dir(tmp_path), replace_venvs_dir_path(tmp_path / 'venvs'):
        with pytest.raises(exception):
            venv.init_venv('bridge_failure')
        assert not (tmp_path / '.cache' / 'venvs' / 'bridge_failure.lock').exists()


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX SIGINT and Unix sockets')
def test_worker_survives_idle_and_active_interrupts(tmp_path):
    address = str(tmp_path / 'worker.sock')
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(address)
    server.listen(1)
    server.settimeout(15)
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    marker = tmp_path / 'active'
    code = """
import pathlib, time
from meerschaum.worker import main, _ops

def block(conn, call_id, args, frames):
    pathlib.Path(args['marker']).touch()
    time.sleep(60)
    return True, []

_ops.HANDLERS['interrupt_test'] = block
main()
"""
    proc = subprocess.Popen(
        [sys.executable, '-c', code, '--socket', address],
        env=_subprocess_env(tmp_path, plugins),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        conn, _ = server.accept()
        conn.settimeout(10)
        with conn, conn.makefile('rwb') as stream:
            hello, _ = read_message(stream)
            assert hello['pid'] == proc.pid

            def send(call_id, op, args=None):
                stream.write(encode_message({'t': 'call', 'id': call_id, 'op': op, 'args': args or {}}))
                stream.flush()

            for call_id in (1, 2):
                proc.send_signal(signal.SIGINT)
                send(call_id, 'ping')
                reply, _ = read_message(stream)
                assert reply['t'] == 'result' and reply['id'] == call_id

            for call_id in (3, 5):
                marker.unlink(missing_ok=True)
                send(call_id, 'interrupt_test', {'marker': str(marker)})
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert marker.exists(), 'worker did not enter the blocking handler'
                proc.send_signal(signal.SIGINT)
                reply, _ = read_message(stream)
                assert reply['t'] == 'error' and reply['exception'] == 'KeyboardInterrupt'
                assert reply['id'] == call_id
                send(call_id + 1, 'ping')
                reply, _ = read_message(stream)
                assert reply['t'] == 'result' and reply['id'] == call_id + 1
            send(7, 'shutdown')
            reply, _ = read_message(stream)
            assert reply['t'] == 'result'
            assert proc.wait(timeout=10) == 0
        _, stderr = proc.communicate(timeout=10)
        assert b'Traceback' not in stderr, stderr.decode()
    finally:
        server.close()
        if proc.poll() is None:
            proc.kill()
        proc.communicate(timeout=10)
