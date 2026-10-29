#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Generate the instance- and job-action transcripts for the `meerschaum-cli` crate
(`src/native/`, milestone 5.0.0b1).

Run through the reference interpreter from the repository root:

    scripts/parity/run.sh crates/meerschaum-cli/tests/fixtures/gen_actions.py

Output (next to this script): `actions_transcript.json`:

    {
      "columns": 100,
      "steps": [
        {"kind": "mrsm", "args": [...], "stdin": "...", "env": {...},
         "stdout": "...", "stderr": "...", "rc": 0, "compare": ["stdout", "stderr", "rc"]},
        {"kind": "sql", "queries": [...]},           # run on the root's SQLite file
        {"kind": "wait_jobs", "names": [...]},       # wait until these jobs have stopped
        {"kind": "sleep", "seconds": 4},
        ...
      ]
    }

Each `mrsm` step runs `python -m meerschaum ARGS` (v4.1.4) in a fresh temporary root with an
empty plugins directory, a SQLite instance (`sql:local`) seeded with synthetic data, and
`COLUMNS=100`. Recorded text is normalized (see `normalize()`, mirrored by `tests/actions_parity.rs`):
the temporary root becomes `<ROOT>`, durations become `<DURATION>`, PIDs become `<PID>`, and log
timestamps become `<TS>`.

`--binary PATH` runs the same script against another executable (e.g. `target/debug/mrsm`) and
writes `actions_transcript.rust.json` in the current directory instead, for local diffing.

Every job daemon started by the run is killed at the end (v4 daemons detach from the process
group, so they are found by their `MRSM_ROOT_DIR` in `/proc`).
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
COLUMNS = 100

PIPE_PARAMS = {
    'fetch': {'definition': 'SELECT * FROM src_weather'},
    'columns': {'datetime': 'ts', 'id': 'station'},
}
SEED = [
    'CREATE TABLE src_weather (ts TIMESTAMP, station INTEGER, temperature REAL)',
] + [
    (
        "INSERT INTO src_weather VALUES ('2024-01-%02d 00:00:00', %d, %s)"
        % (day, station, round(day * 1.5 + station, 1))
    )
    for day in range(1, 11)
    for station in (1, 2)
]
MORE = [
    (
        "INSERT INTO src_weather VALUES ('2024-01-%02d 00:00:00', %d, %s)"
        % (day, station, round(day * 2.5 - station, 1))
    )
    for day in range(11, 14)
    for station in (1, 2)
]

I = ['-i', 'sql:local']
### A second SQLite instance (`{root}` is replaced with the temporary root).
OTHER = {'MRSM_SQL_OTHER': 'sqlite:///{root}/other.db'}
PIPE = ['-c', 'sql:local', '-m', 'weather'] + I


def mrsm(*args, stdin='', env=None, compare=('stdout', 'stderr', 'rc')):
    """A step running `mrsm ARGS`; `compare` lists the recorded fields the Rust test checks."""
    return {
        'kind': 'mrsm',
        'args': list(args),
        'stdin': stdin,
        'env': env or {},
        'compare': list(compare),
    }


### Closed stdin at a prompt: v4's `prompt_toolkit` renders the question for a non-terminal and
### raises `EOFError`; only the exit code is comparable.
EOF_PROMPT = ('rc',)
### An action raised: v4 logs a Python traceback to stderr, the Rust CLI the exception line.
TRACEBACK = ('stdout', 'rc')


STEPS = [
    {'kind': 'sql', 'queries': SEED},
    mrsm('show', 'pipes', *I),
    mrsm('show', 'pipes', *I, '--nopretty'),
    mrsm('register', 'pipes', *PIPE, '--params', json.dumps(PIPE_PARAMS)),
    mrsm('register', 'pipes', *PIPE, '--params', json.dumps(PIPE_PARAMS)),
    mrsm('show', 'pipes', *I),
    mrsm('show', 'pipes', *I, '--nopretty'),
    mrsm('show', 'rowcounts', *I),
    mrsm('sync', 'pipes', *I),
    mrsm('show', 'rowcounts', *I),
    mrsm('show', 'rowcounts', *I, '--nopretty'),
    mrsm('show', 'columns', *I),
    mrsm('show', 'data', *I, '--begin', '2024-01-08', '--nopretty'),
    mrsm('show', 'data', *I, '--begin', '2024-01-08'),
    {'kind': 'sql', 'queries': MORE},
    mrsm('sync', 'pipes', *I, '--nopretty'),
    mrsm('show', 'rowcounts', *I, '--nopretty'),
    mrsm('sync', 'pipes', *I, '--begin', '2024-01-01', '--end', '2024-01-05'),
    mrsm('verify', 'pipes', *I, '--chunk-days', '5', '-w', '1'),
    mrsm('verify', 'rowcounts', *I, '--chunk-days', '5', '-w', '1'),
    mrsm('deduplicate', 'pipes', *I),
    mrsm('index', 'pipes', *I),
    mrsm('tag', 'pipes', *I, 'production', 'weather'),
    mrsm('show', 'tags', *I),
    mrsm('show', 'tags', *I, '--nopretty'),
    mrsm('tag', 'pipes', *I, '_weather'),
    mrsm('show', 'pipes', *I, '--tags', 'production', '--nopretty'),
    mrsm('edit', 'pipes', *I, '--params', '{"verify": {"chunk_days": 3}}', '-y'),
    mrsm('show', 'pipes', *I, '--nopretty'),
    mrsm('vacuum', 'pipes', *I, '-y'),
    mrsm('analyze', 'pipes', *I, '-y'),
    mrsm('show', 'sizes', *I),
    mrsm('show', 'sizes', *I, '--nopretty'),
    mrsm('show', 'partitions', *I),
    mrsm('compress', 'pipes', *I, '-y'),
    mrsm('decompress', 'pipes', *I, '-y'),
    mrsm('partition', 'pipes', *I, '-y'),
    mrsm('copy', 'pipes', 'sql:other', *I, '-y', env=OTHER),
    mrsm('show', 'pipes', '-i', 'sql:other', '--nopretty', env=OTHER),
    mrsm('show', 'rowcounts', '-i', 'sql:other', '--nopretty', env=OTHER),
    mrsm('show', 'connectors', 'sql:other', env=OTHER),
    mrsm('delete', 'pipes', '-i', 'sql:other', '-y', env=OTHER),
    mrsm('show', 'plugins'),
    mrsm('show', 'plugins', '--nopretty'),
    mrsm('show', 'plugins', 'all', '-r', 'sql:local', compare=TRACEBACK),
    mrsm('register', 'plugins'),
    mrsm('register', 'plugins', 'noaa'),
    mrsm('show', 'users', *I),
    mrsm('show', 'users', *I, '--nopretty'),
    mrsm('register', 'users', 'alice', *I, compare=EOF_PROMPT),
    mrsm('delete', 'users', 'alice', *I, '-y'),
    mrsm('clear', 'pipes', *I, '--begin', '2024-01-12', '-y'),
    mrsm('show', 'rowcounts', *I, '--nopretty'),
    mrsm('clear', 'pipes', *I, '--begin', '2024-01-12', compare=EOF_PROMPT),
    mrsm('clear', 'pipes', *I, '--begin', '2024-01-12', '--noask'),
    mrsm('drop', 'pipes', *I, '-y'),
    mrsm('show', 'rowcounts', *I, '--nopretty'),
    mrsm('sync', 'pipes', *I, '-m', 'weather'),
    mrsm('drop', 'pipes', *I, compare=EOF_PROMPT),
    mrsm('drop', 'pipes', *I, '--noask'),
    mrsm('delete', 'pipes', *I, compare=EOF_PROMPT),
    mrsm('delete', 'pipes', *I, '--noask'),
    mrsm('delete', 'pipes', *I, '-y'),
    mrsm('show', 'pipes', *I),

    ### Jobs.
    mrsm('register', 'pipes', *PIPE, '--params', json.dumps(PIPE_PARAMS)),
    mrsm('show', 'jobs'),
    mrsm('show', 'jobs', '--nopretty'),
    ### v4 drops Python-level output of short-lived jobs (unflushed at exit), so the logged job
    ### runs a subprocess.
    mrsm('sh', 'echo', 'hello', '-d', '--name', 'listpipes', '-y'),
    {'kind': 'wait_jobs', 'names': ['listpipes']},
    mrsm('show', 'jobs', '--nopretty'),
    mrsm('show', 'jobs'),
    mrsm('show', 'logs', 'listpipes', '--nopretty'),
    mrsm('start', 'jobs', 'listpipes', '-y'),
    {'kind': 'wait_jobs', 'names': ['listpipes']},
    mrsm('show', 'jobs', 'listpipes'),
    mrsm('start', 'job', 'show', 'pipes', *I, '--name', 'emptyjob', '-y'),
    {'kind': 'wait_jobs', 'names': ['emptyjob']},
    mrsm('delete', 'jobs', 'emptyjob', '-f'),
    mrsm('sync', 'pipes', *I, '--loop', '--min-seconds', '600', '-d', '--name', 'looper', '-y'),
    {'kind': 'sleep', 'seconds': 4},
    mrsm('show', 'jobs', 'looper', '--nopretty'),
    mrsm('pause', 'jobs', 'looper', '-y'),
    mrsm('show', 'jobs', 'looper', '--nopretty'),
    mrsm('start', 'jobs', 'looper', '-y'),
    mrsm('stop', 'jobs', 'looper', '-y'),
    mrsm('stop', 'jobs', 'looper', '-y'),
    mrsm('stop', 'jobs', 'listpipes', '-y'),
    mrsm('pause', 'jobs', 'listpipes', '-y'),
    mrsm('delete', 'jobs', 'listpipes', '-y'),
    mrsm('delete', 'jobs', 'nosuchjob', '-y'),
    mrsm('show', 'jobs', '--nopretty'),
    mrsm('delete', 'jobs', '-f'),
    mrsm('show', 'jobs'),
]

REPO = str(HERE.parents[3])
HOME = os.path.expanduser('~')


def normalize(text: str, root: str) -> str:
    text = text.replace(root, '<ROOT>')
    ### Python tracebacks (stderr) name the reference interpreter's paths.
    text = text.replace(REPO, '<REPO>')
    # A reference package may be reached through lib64 or the pinned mrsm-venv symlink.
    # Keep the recorded path spellings independent of which alias Python imported through.
    text = re.sub(
        r'(<REPO>/\.venv-ref/)(?:parity_venvs/mrsm/)?lib(?:64)?/',
        r'\1lib/', text,
    )
    text = re.sub(
        r'(<REPO>/\.venv-ref/)lib/(python[^/]+/site-packages/meerschaum/)',
        r'\1lib64/\2', text,
    )
    if HOME and HOME != '/':
        text = text.replace(HOME, '<HOME>')
    text = re.sub(r'It took .+? to sync', 'It took <DURATION> to sync', text)
    text = re.sub(r'PID: \d+', 'PID: <PID>', text)
    text = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ', '<TS> | ', text)
    return text


def kill_root_processes(root: str) -> None:
    """Kill every process whose environment has `MRSM_ROOT_DIR=<root>`."""
    marker = ('MRSM_ROOT_DIR=' + root).encode()
    me = os.getpid()
    for pid_dir in pathlib.Path('/proc').iterdir():
        if not pid_dir.name.isdigit() or int(pid_dir.name) == me:
            continue
        try:
            environ = (pid_dir / 'environ').read_bytes()
        except Exception:
            continue
        if marker + b'\0' in environ + b'\0':
            try:
                os.kill(int(pid_dir.name), signal.SIGKILL)
            except Exception:
                pass


def run_sql(root: str, queries) -> None:
    db = pathlib.Path(root) / 'sqlite' / 'mrsm_local.db'
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    for q in queries:
        conn.execute(q)
    conn.commit()
    conn.close()


def wait_jobs(root: str, names, timeout: float = 60.0) -> None:
    """Wait until each job's `properties.json` records an `ended` at or after `began`."""
    deadline = time.time() + timeout
    for name in names:
        path = pathlib.Path(root) / 'jobs' / name / 'properties.json'
        while time.time() < deadline:
            try:
                process = json.loads(path.read_text()).get('process', {})
                ended, began = process.get('ended'), process.get('began')
                if ended and (not began or ended >= began):
                    break
            except Exception:
                pass
            time.sleep(0.2)
    time.sleep(0.5)


# Synthetic plugin coverage is recorded separately so the original 87-command fixture
# remains byte-identical. Both files are generated by this reference-harness entry point.
WIRING_PLUGINS = {
    'wiring.py': '''from meerschaum.actions import make_action
from meerschaum._internal.arguments import add_plugin_argument
from meerschaum.connectors import Connector, make_connector
@make_connector
class WiringfeedConnector(Connector):
    REQUIRED_ATTRIBUTES = []
    def fetch(self, pipe, **kw):
        return [{'id': 6, 'val': 60}]
add_plugin_argument('--wiring-count', type=int, default=2, help='Synthetic row count.')
@make_action
def wiring_echo(action=None, wiring_count=2, **kw):
    print('wiring output: ' + ' '.join(action or []))
    return True, 'count=' + str(wiring_count)
def _complete_wiring_echo(action=None, **kw):
    return ['alpha', 'beta']
@make_action
def wiring_write(mrsm_instance='sql:local', **kw):
    import meerschaum as mrsm
    pipe = mrsm.Pipe('plugin:wiring', 'wiring_action', instance=mrsm_instance, parameters={'columns': {'id': 'id'}, 'target': 'wiring_action'})
    result = pipe.sync([{'id': 5, 'val': 50}])
    return result[0], 'Synthetic action callback complete.'
def fetch(pipe, **kw):
    return [{'id': 1, 'val': 10}, {'id': 2, 'val': 20}]
def setup(**kw):
    return True, 'Synthetic setup complete.'
''',
    'wiring_sync.py': '''def sync(pipe, **kw):
    result = pipe.sync([{'id': 3, 'val': 30}, {'id': 4, 'val': 40}])
    return result[0], 'Synthetic callback complete.'
''',
}
for _source in (HERE.parents[3] / 'crates/meerschaum-worker/tests/plugins/wk_custom').glob('*.py'):
    WIRING_PLUGINS['wk_custom/' + _source.name] = _source.read_text()

WIRING_CASES = [
    ['wiring', 'echo', 'hello', '--wiring-count', '3', '-i', 'sql:local'],
    ['setup', 'plugins', 'wiring', '--noask'],
    ['wiring', 'write', '-i', 'sql:local'],
    ['register', 'pipes', '-c', 'wiringfeed:source', '-m', 'wiring_custom', '-i', 'sql:local', '--params', '{"columns":{"id":"id"},"target":"wiring_custom"}'],
    ['sync', 'pipes', '-c', 'wiringfeed:source', '-i', 'sql:local', '--nopretty', '-w', '1'],
    ['register', 'pipes', '-c', 'plugin:wiring_sync', '-m', 'wiring_instance', '-i', 'wkmem:wiring', '--params', '{"columns":{"id":"id"}}'],
    ['sync', 'pipes', '-i', 'wkmem:wiring', '--nopretty', '-w', '1'],
    ['sync', 'pipes', '-c', 'plugin:wiring', '-i', 'sql:local', '--nopretty', '-w', '1'],
    ['sync', 'pipes', '-c', 'plugin:wiring_sync', '-i', 'sql:local', '--nopretty', '-w', '1'],
    ['sync', 'pipes', '-m', 'wiring_fetch', 'wiring_child', '-i', 'sql:local', '--nopretty', '-w', '1'],
]


def generate_wiring():
    with tempfile.TemporaryDirectory(prefix='mrsm-wiring-reference-') as temporary:
        root = pathlib.Path(temporary) / 'root'
        plugins = pathlib.Path(temporary) / 'plugins'
        root.mkdir()
        plugins.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
        env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
                   MRSM_NOASK='true', PYTHONPATH=REPO,
                   MRSM_CONFIG=json.dumps({'meerschaum': {'instance': 'sql:local'},
                       'shell': {'updates': {'check': False}},
                       'formatting': {'ansi': False, 'unicode': False}}))
        venvs = root / 'venvs'
        venvs.mkdir()
        env['MRSM_VENVS_DIR'] = str(venvs)
        for name in ('mrsm', 'wiring', 'wiring_sync', 'wk_custom'):
            (venvs / name).symlink_to(pathlib.Path(sys.prefix), target_is_directory=True)
        for name, source in WIRING_PLUGINS.items():
            (plugins / name).parent.mkdir(parents=True, exist_ok=True)
            (plugins / name).write_text(source)
        seed = '''import meerschaum as mrsm
import pathlib, sys
assert mrsm.__version__ == '4.1.4'
assert pathlib.Path(mrsm.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix))
for connector, metric, target in [('plugin:wiring', 'wiring_fetch', 'wiring_fetch'), ('plugin:wiring_sync', 'wiring_callback', 'wiring_callback')]:
    p = mrsm.Pipe(connector, metric, instance='sql:local', parameters={'columns': {'id': 'id'}, 'target': target})
    assert p.instance_connector.register_pipe(p)[0]
p = mrsm.Pipe('sql:local', 'wiring_child', instance='sql:local', parameters={'columns': {'id': 'id'}, 'target': 'wiring_child', 'sql': 'SELECT id, val * 2 AS val FROM wiring_fetch', 'parents': [{'connector': 'plugin:wiring', 'metric': 'wiring_fetch'}]})
assert p.instance_connector.register_pipe(p)[0]
'''
        try:
            subprocess.run([sys.executable, '-s', '-E', '-c', seed], env=env, cwd=temporary, check=True,
                           capture_output=True, timeout=120)
            # v4's lazy dependency imports can race the plugin fetch thread on first use.
            # Load these pinned libraries before starting the CLI, without importing or
            # initializing Meerschaum before the scratch environment is in place.
            command = [sys.executable, '-s', '-E', '-c',
                       'import runpy; import sqlalchemy.orm; import requests; '
                       'runpy.run_module("meerschaum", run_name="__main__", alter_sys=True)']
            steps = []
            for args in WIRING_CASES:
                process = subprocess.run([*command, *args],
                                         env=env, cwd=temporary, capture_output=True, timeout=120)
                if process.returncode or b'"success": false' in process.stdout:
                    raise RuntimeError(
                        f'The released reference failed a wiring command: {args!r}\n'
                        + process.stdout.decode(errors='replace')
                        + process.stderr.decode(errors='replace')
                    )
                step = {'args': args, 'rc': process.returncode,
                        'stdout': normalize(process.stdout.decode(), str(root)),
                        'stderr': normalize(process.stderr.decode(), str(root))}
                # The action transcript is deterministic; sync messages contain timing.
                step['compare'] = ['rc'] if args[0] == 'sync' else ['rc', 'stdout']
                if 'wiring_child' in args:
                    step['rust_args'] = ['sync', 'pipes', '-m', 'wiring_child', '-i', 'sql:local', '--parents', '--nopretty', '-w', '1']
                steps.append(step)
            with sqlite3.connect(root / 'sqlite' / 'mrsm_local.db') as db:
                tables = {name: db.execute('SELECT id, val FROM ' + name + ' ORDER BY id').fetchall()
                          for name in ('wiring_fetch', 'wiring_callback', 'wiring_action', 'wiring_child', 'wiring_custom')}
            (HERE / 'wiring_transcript.json').write_text(json.dumps({
                'plugins': WIRING_PLUGINS, 'steps': steps, 'tables': tables,
                'instance_rows': json.loads((root / 'wkmem_wiring.json').read_text())['pipes']['plugin:wiring_sync|wiring_instance|None']['rows'],
            }, indent=2) + '\n')
        finally:
            kill_root_processes(str(root))

def main() -> None:
    if "--wiring-only" in sys.argv:
        generate_wiring()
        return
    binary = None
    argv = sys.argv[1:]
    if '--binary' in argv:
        binary = argv[argv.index('--binary') + 1]
    tmp = tempfile.TemporaryDirectory()
    root = os.path.join(tmp.name, 'root')
    plugins = os.path.join(tmp.name, 'plugins')
    os.makedirs(root)
    os.makedirs(plugins)
    original_root = os.environ.get('MRSM_ROOT_DIR')
    venvs = os.environ.get('MRSM_VENVS_DIR') or (
        os.path.join(original_root, 'venvs') if original_root else None
    )
    base_env = {
        key: val
        for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('NO_COLOR', 'TERM', 'COLUMNS', 'LINES')
    }
    base_env.update({
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': plugins,
        'COLUMNS': str(COLUMNS),
        'LINES': '40',
        'PYTHONWARNINGS': 'ignore::ImportWarning',
    })
    if venvs and not binary:
        base_env['MRSM_VENVS_DIR'] = venvs
    command = [binary] if binary else [sys.executable, '-s', '-E', '-m', 'meerschaum']

    steps = []
    try:
        for step in STEPS:
            step = dict(step)
            if step['kind'] == 'sql':
                run_sql(root, step['queries'])
            elif step['kind'] == 'wait_jobs':
                wait_jobs(root, step['names'])
            elif step['kind'] == 'sleep':
                time.sleep(step['seconds'])
            else:
                env = dict(base_env)
                env.update({k: v.replace('{root}', root) for k, v in step['env'].items()})
                proc = subprocess.run(
                    command + step['args'],
                    input=step['stdin'].encode(),
                    capture_output=True,
                    env=env,
                    cwd=tmp.name,
                    timeout=300,
                )
                step['stdout'] = normalize(proc.stdout.decode(), root)
                step['stderr'] = normalize(proc.stderr.decode(), root)
                step['rc'] = proc.returncode
                print(f"{proc.returncode} mrsm {' '.join(step['args'])}", file=sys.stderr)
            steps.append(step)
    finally:
        kill_root_processes(root)

    out_path = (
        pathlib.Path('actions_transcript.rust.json')
        if binary
        else HERE / 'actions_transcript.json'
    )
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({'columns': COLUMNS, 'steps': steps}, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"Wrote {len(steps)} steps to {out_path}.", file=sys.stderr)
    if not binary:
        generate_wiring()


if __name__ == '__main__':
    main()
