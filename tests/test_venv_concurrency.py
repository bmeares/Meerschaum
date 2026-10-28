"""Simultaneous plugin imports must not race while repairing shared Python aliases."""

import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest


def _verify_shared_venv(root, start, initialize):
    from meerschaum.config._paths import replace_root_dir
    import meerschaum.utils.misc as misc
    import meerschaum.utils.packages as packages
    import meerschaum.utils.process as process
    import meerschaum.utils.venv as venvs

    original_run = process.run_process
    original_symlink = misc.make_symlink
    active = root / 'version-probe-active'
    conflict = root / 'overlapping-version-probes'

    def slow_run(*args, **kwargs):
        # Widen the real version probe so overlapping verifiers are reliably observed.
        try:
            fd = os.open(active, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            conflict.touch()
            raise RuntimeError('Another process is repairing this environment')
        try:
            time.sleep(0.02)
            return original_run(*args, **kwargs)
        finally:
            os.close(fd)
            active.unlink()

    def slow_symlink(*args, **kwargs):
        time.sleep(0.04)
        return original_symlink(*args, **kwargs)

    def create_without_downloads(package, args, **kwargs):
        # Exercise init's real creation and recursive verification without pip or network.
        import venv
        assert package == 'venv'
        venv.EnvBuilder(with_pip=False, symlinks=True).create(args[0])
        return 0

    process.run_process = slow_run
    misc.make_symlink = slow_symlink
    packages.is_uv_enabled = lambda: False
    packages.run_python_package = create_without_downloads
    with replace_root_dir(root):
        venvs.verified_venvs.clear()
        assert start.wait(30)
        for _ in range(4):
            if initialize:
                assert venvs.init_venv('shared')
            else:
                venvs.verify_venv('shared')
            subprocess.run([venvs.venv_executable('shared'), '-V'], check=True, capture_output=True)


@pytest.mark.skipif(sys.platform == 'win32', reason='Exercises POSIX interpreter symlinks.')
@pytest.mark.parametrize('existing', [False, True])
def test_processes_share_creation_and_verification_lock(tmp_path, existing, monkeypatch):
    # Spawn pickles this function by module name. Earlier venv tests may put a dependency's
    # unrelated `tests` package ahead of this checkout; make the worker target unambiguous.
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parent.parent))
    binary = tmp_path / 'venvs' / 'shared' / 'bin'
    binary.mkdir(parents=True)
    version = f'python{sys.version_info.major}.{sys.version_info.minor}'
    site = binary.parent / 'lib' / version / 'site-packages'
    if existing:
        site.mkdir(parents=True)
    # An indirect, valid alias needs normalization; it must remain executable during repair.
    executable = binary / version
    if existing:
        (binary / 'python').symlink_to(Path(sys.executable).resolve())
        executable.symlink_to('python')

    ctx = multiprocessing.get_context('spawn')
    start = ctx.Event()
    processes = [ctx.Process(target=_verify_shared_venv, args=(tmp_path, start, i % 2))
                 for i in range(8)]
    try:
        for child in processes:
            child.start()
        start.set()
        deadline = time.monotonic() + 60
        executable_seen = existing
        while any(child.is_alive() for child in processes) and time.monotonic() < deadline:
            executable_seen = executable_seen or executable.exists()
            if executable_seen:
                subprocess.run([str(executable), '-V'], check=True, capture_output=True)
            time.sleep(0.005)
        for child in processes:
            child.join(5)
            assert child.exitcode == 0
    finally:
        for child in processes:
            if child.is_alive():
                child.terminate()
                child.join(5)
    assert not (tmp_path / 'overlapping-version-probes').exists()
    assert executable.resolve() == Path(sys.executable).resolve()
    assert not list(binary.glob('.*'))


@pytest.mark.skipif(sys.platform == 'win32', reason='Exercises POSIX interpreter symlinks.')
def test_probe_timeout_reaps_child_and_keeps_valid_alias(tmp_path, monkeypatch):
    from meerschaum.config._paths import replace_root_dir
    import meerschaum.utils.process as process
    import meerschaum.utils.venv as venvs

    binary = tmp_path / 'venvs' / 'shared' / 'bin'
    binary.mkdir(parents=True)
    executable = binary / f'python{sys.version_info.major}.{sys.version_info.minor}'
    executable.symlink_to(Path(sys.executable).resolve())

    class SlowProbe:
        killed = False
        reaped = False

        def communicate(self, timeout=None):
            if timeout is not None:
                raise subprocess.TimeoutExpired(str(executable), timeout)
            assert self.killed
            self.reaped = True
            return b'', b''

        def poll(self):
            return None

        def kill(self):
            self.killed = True

    probe = SlowProbe()
    monkeypatch.setattr(process, 'run_process', lambda *a, **kw: probe)
    with replace_root_dir(tmp_path):
        venvs.verify_venv('shared')
    assert probe.killed and probe.reaped
    assert executable.is_symlink() and executable.exists()


def test_verified_init_needs_no_writable_lock(tmp_path, monkeypatch):
    import meerschaum.utils.venv as venvs
    from meerschaum.utils.locks import InterProcessLock

    def forbid_acquire(*args, **kwargs):
        raise PermissionError('cache is read-only')

    monkeypatch.setattr(venvs, 'verified_venvs', {'shared'})
    monkeypatch.setattr(InterProcessLock, 'acquire', forbid_acquire)
    assert venvs.init_venv('shared')
