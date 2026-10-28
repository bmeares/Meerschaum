"""The golden generators and their children must use the pinned released reference."""

import json
import os
from pathlib import Path
import subprocess

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
