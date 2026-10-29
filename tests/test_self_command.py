"""
How Meerschaum starts another copy of itself, and how `meerschaum.__main__.main()` ends.

- `main()` called from a script which imported `meerschaum` eagerly ends in `sys.exit` (the script's
  `atexit` handlers and buffered files must survive, as in v4); only the CLI process (`mrsm`,
  `python -m meerschaum`) skips the interpreter's teardown.
- The command which re-runs the CLI (`self_command()`, native jobs, compose, `systemd` units) is
  `<python> -P -m meerschaum`: a `meerschaum/` or `meerschaum.py` in the working directory never
  replaces the package.
- `MRSM_BIN` naming a launcher for the console script (pip's `mrsm.exe` on Windows) is ignored in the
  process it starts, instead of starting itself again forever.
- `sys.executable` which is not a Python (mod_wsgi, uWSGI) falls back to `sys._base_executable`.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent


def _env(tmp_path: pathlib.Path, **extra: str) -> dict[str, str]:
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_SQL_MAIN': 'sqlite:///' + str(tmp_path / 'main.db'),
        'MRSM_NO_AUTO_INSTALL': '1',
        **extra,
    }
    for name in ('MRSM_BIN', 'MRSM_BIN_EXEC'):
        if name not in extra:
            env.pop(name, None)
    return env


def _run(code: str, tmp_path: pathlib.Path, **extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True, text=True, env=_env(tmp_path, **extra), timeout=120, cwd=str(tmp_path),
    )


def test_main_from_an_eager_import_runs_atexit_handlers_and_flushes_files(tmp_path):
    out = tmp_path / 'out.txt'
    result = _run(
        "import atexit, sys\n"
        "import meerschaum\n"
        "assert not meerschaum._LAZY\n"
        f"handle = open({str(out)!r}, 'w')\n"
        "handle.write('buffered data')\n"
        f"atexit.register(lambda: open({str(tmp_path / 'atexit.txt')!r}, 'w').write('ran'))\n"
        "import meerschaum.__main__\n"
        "meerschaum.__main__.main(['show', 'version'])\n"
        "print('not reached')\n",
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert 'not reached' not in result.stdout
    assert (tmp_path / 'atexit.txt').read_text() == 'ran'
    ### The unflushed file was flushed by the interpreter's teardown.
    assert out.read_text() == 'buffered data'


def test_main_in_the_cli_process_skips_the_interpreters_teardown(tmp_path):
    result = _run(
        "import atexit, sys\n"
        "sys._meerschaum_cli = True\n"
        "import meerschaum\n"
        "assert meerschaum._LAZY\n"
        f"atexit.register(lambda: open({str(tmp_path / 'atexit.txt')!r}, 'w').write('ran'))\n"
        "import meerschaum.__main__\n"
        "meerschaum.__main__.main(['--version'])\n",
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()
    assert not (tmp_path / 'atexit.txt').exists()


def test_the_self_command_keeps_the_working_directory_off_the_path():
    from meerschaum._internal import binary
    assert binary.interpreter() == sys.executable
    os.environ.pop('MRSM_BIN', None)
    assert binary.self_command() == [sys.executable, '-P', '-m', 'meerschaum']


def test_a_shadowing_package_in_the_working_directory_is_not_imported(tmp_path):
    shadow = tmp_path / 'project'
    (shadow / 'meerschaum').mkdir(parents=True)
    (shadow / 'meerschaum' / '__init__.py').write_text(
        "raise RuntimeError('SHADOWED: the working directory replaced the package')\n"
    )
    (shadow / 'meerschaum.py').write_text("raise RuntimeError('SHADOWED')\n")
    env = _env(tmp_path)

    ### The control: a plain `python -m meerschaum` finds the shadow before `PYTHONPATH`.
    plain = subprocess.run(
        [sys.executable, '-m', 'meerschaum', '--version'],
        capture_output=True, text=True, env=env, timeout=120, cwd=str(shadow),
    )
    assert 'SHADOWED' in plain.stderr, plain.stderr

    from meerschaum._internal import binary
    command = binary.self_command()
    safe = subprocess.run(
        [*command, '--version'],
        capture_output=True, text=True, env=env, timeout=120, cwd=str(shadow),
    )
    assert safe.returncode == 0, safe.stderr
    assert 'SHADOWED' not in safe.stderr
    assert safe.stdout.strip()


def test_a_host_program_is_not_the_interpreter(monkeypatch):
    from meerschaum._internal import binary
    monkeypatch.delenv('MRSM_BIN', raising=False)
    monkeypatch.setattr(sys, 'executable', '/usr/sbin/uwsgi')
    monkeypatch.setattr(sys, '_base_executable', '/usr/bin/python3.12')
    assert binary.self_command() == ['/usr/bin/python3.12', '-P', '-m', 'meerschaum']
    monkeypatch.setattr(sys, '_base_executable', '/usr/sbin/uwsgi')
    with pytest.raises(RuntimeError, match='not a Python interpreter'):
        binary.self_command()
    monkeypatch.setattr(sys, 'executable', '')
    with pytest.raises(RuntimeError, match='not a Python interpreter'):
        binary.self_command()
    for yes in ('python', 'python3.13', 'C:\\Python313\\python.exe', '/opt/pypy3.11/bin/pypy3'):
        assert binary.is_python_executable(yes), yes
    for no in ('', None, '/usr/sbin/httpd', 'C:\\app\\app.exe', '/bin/mrsm'):
        assert not binary.is_python_executable(no), no


@pytest.mark.skipif(os.name == 'nt' or shutil.which('env') is None, reason="needs a POSIX `env` binary")
def test_the_launcher_passes_a_guard_and_a_guarded_binary_is_ignored(tmp_path):
    ### A program (not a script) which runs its arguments, standing in for a launcher.
    launcher = tmp_path / 'launcher'
    shutil.copy(shutil.which('env'), launcher)
    launcher.chmod(0o755)

    ### `main()` replaces the process with `MRSM_BIN`, naming it in `MRSM_BIN_EXEC`.
    result = _run(
        "import meerschaum.__main__ as m\nm.main(['printenv', 'MRSM_BIN_EXEC'])\n",
        tmp_path,
        MRSM_BIN=str(launcher),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(launcher)

    ### The process the launcher starts sees the guard and does not hand over again.
    guarded = _run(
        ### The CLI process (lazy package): the façade's warning filters are not installed.
        "import sys\nsys._meerschaum_cli = True\nimport meerschaum.__main__ as m\nm.main(['--version'])\n",
        tmp_path,
        MRSM_BIN=str(launcher),
        MRSM_BIN_EXEC=str(launcher),
    )
    assert guarded.returncode == 0, guarded.stderr
    assert guarded.stdout.strip()
    assert 'started this process again' in guarded.stderr

    ### Another binary's guard does not apply.
    from meerschaum._internal import binary
    env = dict(os.environ)
    try:
        os.environ['MRSM_BIN'] = str(launcher)
        os.environ['MRSM_BIN_EXEC'] = '/somewhere/else'
        assert binary.explicit_binary() == str(launcher)
        os.environ['MRSM_BIN_EXEC'] = str(launcher)
        with pytest.warns(RuntimeWarning, match='started this process again'):
            assert binary.explicit_binary() is None
        assert binary.self_command()[0] == binary.interpreter()
    finally:
        os.environ.clear()
        os.environ.update(env)


def test_the_extension_picks_the_interpreter_like_the_launcher(tmp_path):
    """Under mod_wsgi / uWSGI `sys.executable` is the host program: the extension uses the base one."""
    result = _run(
        "import sys\n"
        "from meerschaum import _core\n"
        "from meerschaum.jobs import _native\n"
        "_native.core()\n"
        "sys.executable = '/usr/sbin/uwsgi'\n"
        "sys._base_executable = '/usr/bin/python3.12'\n"
        "print(_core.jobs_launcher()[1])\n"
        "sys._base_executable = '/usr/sbin/uwsgi'\n"
        "try:\n"
        "    _core.jobs_launcher()\n"
        "except RuntimeError as e:\n"
        "    print('ERROR', 'not a Python interpreter' in str(e))\n",
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "['/usr/bin/python3.12', '-P', '-m', 'meerschaum']" in result.stdout
    assert 'ERROR True' in result.stdout


def test_a_job_started_in_a_shadowing_directory_runs_the_real_package(tmp_path):
    """The supervisor and its job re-run `<python> -P -m meerschaum` in the user's directory."""
    import time

    shadow = tmp_path / 'project'
    (shadow / 'meerschaum').mkdir(parents=True)
    (shadow / 'meerschaum' / '__init__.py').write_text("raise RuntimeError('SHADOWED')\n")
    env = _env(tmp_path)
    ### `-P` for the parent as well: it is what `mrsm` (the console script) always was.
    cli = [sys.executable, '-P', '-m', 'meerschaum']

    def mrsm(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [*cli, *args], capture_output=True, text=True, env=env, timeout=60, cwd=str(shadow),
        )

    started = mrsm('show', 'version', ':', '--loop', '--min-seconds', '1', '--name', 'shadow-job', '-d', '--noask')
    try:
        assert started.returncode == 0, started.stderr
        logs = ''
        for _ in range(30):
            logs = mrsm('show', 'logs', 'shadow-job', '--nopretty').stdout
            if 'Meerschaum v' in logs:
                break
            time.sleep(1)
        assert 'Meerschaum v' in logs, logs
        assert 'SHADOWED' not in logs
    finally:
        mrsm('stop', 'job', 'shadow-job', '-y')
        time.sleep(1)
        mrsm('delete', 'job', 'shadow-job', '-y')


PROBE_PLUGIN = '''
__version__ = '0.1.0'

from meerschaum.plugins import make_action


@make_action
def f1probe(**kw):
    """Report the hand-over guard this worker process inherited."""
    import os
    print('GUARD=' + repr(os.environ.get('MRSM_BIN_EXEC')))
    return True, 'ok'
'''


def test_a_standalone_binary_does_not_pass_the_hand_over_guard_to_its_children(tmp_path):
    """
    `MRSM_BIN_EXEC` only blocks the immediate re-entry of a launcher: the real binary a Python
    process handed over to drops it, so its Python worker (and everything else it starts) does not
    think it was "started again" and keeps honoring `MRSM_BIN`.
    """
    from tests._binary import binary_or_skip

    binary = binary_or_skip()
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'f1probe.py').write_text(PROBE_PLUGIN)
    env = _env(
        tmp_path,
        MRSM_PLUGINS_DIR=str(plugins),
        MRSM_PYTHON=sys.executable,
        MRSM_BIN=binary,
        MRSM_BIN_EXEC=binary,
    )
    result = subprocess.run(
        [binary, 'f1probe'], capture_output=True, text=True, env=env, timeout=120, cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'GUARD=None' in result.stdout, result.stdout + result.stderr
