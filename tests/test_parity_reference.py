"""The golden generators and their children must use the pinned released reference."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from tests._binary import skip_or_fail


def test_reference_generator_and_children_ignore_the_callers_source_and_root(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    reference = repo / '.venv-ref'
    if not (reference / 'bin/python').exists():
        skip_or_fail('Run scripts/parity/setup_reference.sh before checking reference isolation.')
    caller_root = tmp_path / 'caller-root'
    caller_root.mkdir()
    (caller_root / 'sentinel').write_text('unchanged')
    output = tmp_path / 'probe.json'
    generator = tmp_path / 'probe.py'
    generator.write_text('''
import json, os, pathlib, subprocess, sys
import meerschaum
repo, output = sys.argv[1:]
child_env = {**os.environ, 'PYTHONPATH': repo}
child = subprocess.check_output(
    [sys.executable, '-c', 'import meerschaum; print(meerschaum.__version__); print(meerschaum.__file__)'],
    cwd=repo, env=child_env, text=True,
).splitlines()
cli = subprocess.check_output(
    [sys.executable, '-m', 'meerschaum', 'show', 'version', '--nopretty'],
    cwd=repo, env=child_env, text=True,
).strip()
pandas = meerschaum.attempt_import('pandas', lazy=False)
pathlib.Path(output).write_text(json.dumps({
    'parent': meerschaum.__version__, 'origin': meerschaum.__file__,
    'child': child[0], 'child_origin': child[1], 'cli': cli,
    'pandas': pandas.__version__, 'root': os.environ['MRSM_ROOT_DIR'],
    'plugins': os.listdir(os.environ['MRSM_PLUGINS_DIR']),
    'tmux': 'TMUX' in os.environ or 'TMUX_PANE' in os.environ,
    'no_color': 'NO_COLOR' in os.environ,
}))
''')
    result = subprocess.run(
        ['bash', str(repo / 'scripts/parity/run.sh'), str(generator), str(repo), str(output)],
        cwd=repo,
        env={
            **os.environ,
            'PYTHONPATH': str(repo),
            'NO_COLOR': '1',
            'MRSM_ROOT_DIR': str(caller_root),
            'MRSM_VENVS_DIR': str(tmp_path / 'caller-venvs'),
            'TMUX': '/unused-parity-test-socket,1,0',
            'TMUX_PANE': '%99',
        },
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    probe = json.loads(output.read_text())
    assert probe['parent'] == probe['child'] == probe['cli'] == '4.1.4', probe
    for key in ['origin', 'child_origin']:
        assert Path(probe[key]).resolve().is_relative_to(reference.resolve()), probe
    pandas_pin = next(
        line.split('==', 1)[1]
        for line in (repo / 'scripts/parity/requirements.txt').read_text().splitlines()
        if line.startswith('pandas==')
    )
    assert probe['pandas'] == pandas_pin
    assert Path(probe['root']) != caller_root
    assert not Path(probe['root']).exists(), 'The reference root must be cleaned up.'
    assert not probe['plugins'] and not probe['tmux'] and not probe['no_color']
    assert list(caller_root.iterdir()) == [caller_root / 'sentinel']


@pytest.mark.parametrize('outcome,expected_status', [
    ('matching', 0), ('exception', 1), ('exit', 23), ('drift', 1),
])
def test_parity_reporting_keeps_details_and_preserves_failures(tmp_path, outcome, expected_status):
    """Exercise the real parity command with a tiny reference-runner stand-in."""
    source = Path(__file__).resolve().parents[1]
    repo = tmp_path / 'repo'
    for directory in ('scripts/rust', 'scripts/parity', 'crates/probe/tests/fixtures'):
        (repo / directory).mkdir(parents=True)
    for relative in ('scripts/rust/ci.sh', 'scripts/parity/check_fixture_hygiene.sh'):
        shutil.copy2(source / relative, repo / relative)
    for name, body in {
        'setup_reference.sh': '#!/usr/bin/env bash\nexit 0\n',
        'run.sh': '#!/usr/bin/env bash\nset -euo pipefail\nexec python3 "$1"\n',
    }.items():
        script = repo / 'scripts/parity' / name
        script.write_text(body)
        script.chmod(0o755)
    fixtures = repo / 'crates/probe/tests/fixtures'
    (fixtures / 'probe.json').write_text('{"value": 1}\n')
    (fixtures / 'gen_probe.py').write_text(f'''
from pathlib import Path
import traceback
try:
    raise ValueError('expected negative fixture case')
except ValueError:
    traceback.print_exc()
outcome = {outcome!r}
if outcome == 'exception':
    raise RuntimeError('unexpected fixture failure')
if outcome == 'exit':
    raise SystemExit(23)
if outcome == 'drift':
    Path(__file__).with_name('probe.json').write_text('{{"value": 2}}\\n')
''')
    subprocess.run(['git', 'init', '-q', str(repo)], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    github_env = tmp_path / 'github-env'
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_')
        and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME', 'CARGO_TARGET_DIR')
    }
    env.update(MRSM_CI_ACCEL='0', GITHUB_ENV=str(github_env))
    result = subprocess.run(
        ['bash', str(repo / 'scripts/rust/ci.sh'), 'parity'],
        cwd=repo, env=env, text=True, capture_output=True, timeout=30,
    )
    console = result.stdout + result.stderr
    assert result.returncode == expected_status, console
    key, value = github_env.read_text().strip().split('=', 1)
    assert key == 'MRSM_PARITY_LOGS_DIR'
    logs = list(Path(value).glob('*.log'))
    assert len(logs) == 1
    assert 'expected negative fixture case' in logs[0].read_text()
    if outcome in ('exception', 'exit'):
        assert '[parity] failed:' in console
        assert 'Traceback' in console
        if outcome == 'exception':
            assert 'unexpected fixture failure' in console
    else:
        assert '[parity] passed:' in console
        assert 'Traceback' not in console
        if outcome == 'drift':
            assert 'Parity fixtures differ' in console
