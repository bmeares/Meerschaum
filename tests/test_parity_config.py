"""Config fixture children must reuse the reference venv rather than bootstrap another."""

import json
import os
from pathlib import Path
import subprocess
import venv

import pytest


@pytest.fixture
def reference(tmp_path):
    """A dependency-free interpreter makes accidental package imports fail this test."""
    prefix = tmp_path / 'reference'
    venv.EnvBuilder(with_pip=False, symlinks=os.name != 'nt').create(prefix)
    executable = prefix / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
    venvs_dir = tmp_path / 'venvs'
    venvs_dir.mkdir()
    (venvs_dir / 'mrsm').symlink_to(prefix, target_is_directory=True)
    root = tmp_path / 'root'
    plugins = tmp_path / 'caller-plugins'
    root.mkdir()
    plugins.mkdir()
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('MRSM_')
        and k not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME', 'VIRTUAL_ENV')
    }
    env.update(
        MRSM_PARITY_REFERENCE='4.1.4', MRSM_VENVS_DIR=str(venvs_dir),
        MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
        MRSM_NO_AUTO_INSTALL='1',
    )
    return executable, venvs_dir, env


def test_config_child_reuses_reference_without_caller_state(tmp_path, reference):
    executable, venvs_dir, env = reference
    repo = Path(__file__).resolve().parents[1]
    generator = repo / 'crates/meerschaum-core/tests/fixtures/gen_config.py'
    case = tmp_path / 'case'
    (case / 'root').mkdir(parents=True)
    script = '''
import json, os, pathlib, runpy, subprocess, sys
generator, case = sys.argv[1:]
ns = runpy.run_path(generator)
# Poison only this parent process; the actual child must receive the isolated environment.
os.environ.update(TMUX='unused', TMUX_PANE='%99', PYTHONPATH='unused', PYTHONHOME='unused',
                  VIRTUAL_ENV='unused', MRSM_SQL_PRIVATE='unused', MRSM_NO_AUTO_INSTALL='0')
child_env = ns['child_environment'](case, pathlib.Path(case) / 'root',
                                     {'MRSM_CONFIG': '{"case_root":"{ROOT}"}'})
assert 'meerschaum' not in sys.modules
keys = ('HOME', 'MRSM_ROOT_DIR', 'MRSM_PLUGINS_DIR', 'MRSM_VENVS_DIR', 'MRSM_CONFIG',
        'MRSM_PARITY_REFERENCE', 'MRSM_NO_AUTO_INSTALL', 'TMUX', 'TMUX_PANE', 'PYTHONPATH',
        'PYTHONHOME', 'VIRTUAL_ENV', 'MRSM_SQL_PRIVATE')
probe = ('import json,os,sys; print(json.dumps({"env":{k:os.environ[k] for k in '
         + repr(keys) + ' if k in os.environ},"prefix":sys.prefix}))')
print(subprocess.check_output([sys.executable, '-s', '-E', '-c', probe], env=child_env, text=True))
'''
    result = subprocess.run(
        [str(executable), '-s', '-E', '-c', script, str(generator), str(case)],
        cwd=repo, env=env, text=True, capture_output=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    probe = json.loads(result.stdout)
    child_env = probe['env']
    assert (Path(child_env['MRSM_VENVS_DIR']) / 'mrsm').resolve() == Path(probe['prefix']).resolve()
    assert child_env['MRSM_VENVS_DIR'] == str(venvs_dir)
    assert 'MRSM_PARITY_REFERENCE' not in child_env  # v4 would treat this as a connector.
    assert child_env['MRSM_NO_AUTO_INSTALL'] == '1'
    assert child_env['HOME'] == str(case)
    assert child_env['MRSM_ROOT_DIR'] == str(case / 'root')
    assert json.loads(child_env['MRSM_CONFIG']) == {'case_root': str(case / 'root')}
    assert list(Path(child_env['MRSM_PLUGINS_DIR']).iterdir()) == []
    for key in ('TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME', 'VIRTUAL_ENV', 'MRSM_SQL_PRIVATE'):
        assert key not in child_env
    assert not (case / 'root/venvs').exists()


@pytest.mark.parametrize('pin', ['missing', 'foreign'])
def test_config_child_refuses_unpinned_reference_before_import(tmp_path, reference, pin):
    executable, _, env = reference
    repo = Path(__file__).resolve().parents[1]
    if pin == 'missing':
        del env['MRSM_VENVS_DIR']
    else:
        env['MRSM_VENVS_DIR'] = str(tmp_path / 'foreign-venvs')
    result = subprocess.run(
        [str(executable), '-s', '-E', '-c', '''
import pathlib, runpy, sys
ns = runpy.run_path(sys.argv[1])
ns['child_environment'](sys.argv[2], pathlib.Path(sys.argv[2]) / 'root', {})
''', str(repo / 'crates/meerschaum-core/tests/fixtures/gen_config.py'), str(tmp_path / 'case')],
        cwd=repo, env=env, text=True, capture_output=True, timeout=20,
    )
    assert result.returncode != 0
    assert 'requires MRSM_VENVS_DIR/mrsm' in result.stderr
    assert 'ModuleNotFoundError' not in result.stderr
    assert not (tmp_path / 'case').exists()
