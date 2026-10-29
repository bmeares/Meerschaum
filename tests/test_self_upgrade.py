"""An offline Python upgrade changes the launcher environment, not MRSM_PYTHON."""
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

REPO = Path(__file__).resolve().parents[1]


def _wheel(directory, version, sdk=False):
    dist = f'meerschaum-{version}.dist-info'
    files = {
        'meerschaum/__init__.py': f'__version__ = {version!r}\n',
        # This fixture exposes only --version, which the fresh-engine verification runs.
        'meerschaum/__main__.py': f'print("Meerschaum v{version}")\n',
        f'{dist}/METADATA': f'Metadata-Version: 2.1\nName: meerschaum\nVersion: {version}\n',
        f'{dist}/WHEEL': 'Wheel-Version: 1.0\nGenerator: mrsm-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n',
    }
    if sdk:
        for source in (REPO / 'meerschaum').rglob('*'):
            if source.is_file() and '__pycache__' not in source.parts and source.suffix in ('.py', '.so', '.pyd', '.json'):
                files[source.relative_to(REPO).as_posix()] = source.read_bytes()
    files[f'{dist}/RECORD'] = ''.join(f'{name},,\n' for name in files)
    path = directory / f'meerschaum-{version}-py3-none-any.whl'
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=1) as wheel:
        for name, contents in files.items():
            wheel.writestr(name, contents)
    return path


@pytest.mark.parametrize('shell', [False, True])
def test_self_upgrade_owns_the_launcher_environment(tmp_path, shell):
    from meerschaum.config._version import __version__
    baseline = __version__
    target = baseline.split('.', 1)[0] + '.999.0'
    wheels = tmp_path / 'wheels'
    wheels.mkdir()
    initial = _wheel(wheels, baseline, sdk=True)
    _wheel(wheels, target)
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(('MRSM_', 'PYTHON', 'PIP_', 'UV_'))
        and key not in ('TMUX', 'TMUX_PANE', 'VIRTUAL_ENV')
    }
    interpreters = []
    for name in ('owner', 'host'):
        directory = tmp_path / name
        made = subprocess.run([sys.executable, '-m', 'venv', str(directory)], env=env, capture_output=True)
        if made.returncode:
            pytest.skip('This test requires venv with ensurepip.')
        python = directory / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        seeded = subprocess.run([str(python), '-m', 'pip', 'install', '--no-index', str(initial)], env=env, capture_output=True, text=True)
        assert seeded.returncode == 0, seeded.stdout + seeded.stderr
        interpreters.append(python)
    owner, host = interpreters
    root, plugins = tmp_path / 'root', tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env.update({
        'MRSM_ROOT_DIR': str(root), 'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_PYTHON': str(host), 'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1', 'MRSM_NOASK': '1',
        'PIP_NO_INDEX': '1', 'PIP_FIND_LINKS': str(wheels),
        'UV_NO_INDEX': '1', 'UV_FIND_LINKS': str(wheels),
    })
    args = ['--shell'] if shell else ['upgrade', 'mrsm', '-y', '-A', '--version', target]
    run = subprocess.run(
        [str(owner), '-P', '-m', 'meerschaum', *args],
        input=f'upgrade mrsm -y -A --version {target}\nEOF\n' if shell else None,
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=90,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    found = []
    for python in (owner, host):
        version = subprocess.run(
            [str(python), '-I', '-c', 'import importlib.metadata, json; print(json.dumps(importlib.metadata.version("meerschaum")))'],
            env=env, capture_output=True, text=True, check=True,
        )
        found.append(json.loads(version.stdout))
    assert found == [target, baseline], run.stdout + run.stderr

    if shell:
        assert run.stdout.rstrip().endswith(f'Meerschaum v{target}'), run.stdout + run.stderr
