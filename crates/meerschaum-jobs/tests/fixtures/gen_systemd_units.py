#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `systemd_executor.json`: what the released `SystemdExecutor` does for create, start, stop,
pause and delete, recorded with `run_command` replaced by a scripted fake (nothing here ever runs
`systemctl`, so no service of the host is touched).

Run from the repository root against the released reference (what CI does; the output must be
byte-identical from run to run):

    MRSM_PLUGINS_DIR=$(mktemp -d) scripts/parity/run.sh \
        crates/meerschaum-jobs/tests/fixtures/gen_systemd_units.py

or against the working tree:

    PYTHONPATH=$(pwd) python crates/meerschaum-jobs/tests/fixtures/gen_systemd_units.py

Like `gen_jobs.py`, the work happens in a child process with `env -i` plus `PATH`, a temporary
`HOME`, `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, `MRSM_VENVS_DIR`, `MRSM_NO_AUTO_INSTALL`, `TZ=UTC` and
`MRSM_CONFIG` (short stop timeouts). Per scenario it records the `systemctl --user` commands the
executor issued (consecutive repeats of the status polling collapsed), its return value, and the
files it left under the temporary root and home directory (the unit text with the interpreter and
temporary paths normalized, the symlink target, the stdin FIFO, the sister job's properties).
"""

import json
import os
import pathlib
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
OUT = HERE / 'systemd_executor.json'
MRSM_VENV = (
    pathlib.Path(sys.prefix)
    if os.environ.get('MRSM_PARITY_REFERENCE')
    else REPO / 'test_root' / 'venvs' / 'mrsm'
)

CASES = [
    {
        'name': 'sync-weather',
        'sysargs': ['sync', 'pipes', '-c', 'plugin:noaa', '--loop', '--min-seconds', '3600'],
    },
    {
        'name': 'my job',
        'sysargs': ['show', 'version', '--nopretty'],
        'properties': {'env': {'FOO': 'bar baz', 'QUOTE': "it's"}},
    },
    {
        'name': 'cleanup',
        'sysargs': ['delete', 'pipes', '-c', 'sql:main', '-y'],
        'properties': {'delete_after_completion': True},
    },
]

TIMESTAMPS = [
    'ActiveEnterTimestamp=Tue 2000-01-04 10:00:00 UTC',
    'ActiveEnterTimestamp=Mon 2024-02-29 23:59:59 UTC',
    'ActiveEnterTimestamp=Tue 2000-01-04 10:00:00 GMT',
    'ActiveEnterTimestamp=',
    'ActiveEnterTimestamp=n/a',
    'InactiveEnterTimestamp=Wed 2026-10-14 01:02:03 UTC',
]

STATUS_TABLE = [
    {'is_active': 'activating', 'pid': '0', 'process': None},
    {'is_active': 'active', 'pid': '4242', 'process': 'running'},
    {'is_active': 'active', 'pid': '4242', 'process': 'stopped'},
    {'is_active': 'active', 'pid': '0', 'process': None},
    {'is_active': 'active', 'pid': '4242', 'process': 'gone'},
    {'is_active': 'inactive', 'pid': '0', 'process': None},
    {'is_active': 'failed', 'pid': '0', 'process': None},
    {'is_active': '', 'pid': '0', 'process': None},
]


def collapse(commands):
    """Collapse consecutive repeats of the same command (status polling)."""
    out = []
    for command in commands:
        if not out or out[-1] != command:
            out.append(command)
    return out


def render_tree(tmp, home):
    """Every file under `systemd/` and the user unit directory, as `{path: description}`."""
    files = {}
    roots = [
        (tmp / 'root' / 'systemd', 'root/systemd'),
        (tmp / 'root' / 'jobs', 'root/jobs'),
        (home / '.config' / 'systemd' / 'user', 'home/systemd/user'),
    ]
    for base, label in roots:
        if not base.exists():
            continue
        for path in sorted(base.rglob('*')):
            rel = f"{label}/{path.relative_to(base).as_posix()}"
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                target = os.readlink(path)
                files[rel] = {
                    'type': 'symlink',
                    'target': target.replace(str(tmp), '{TMP}'),
                    'dangling': not path.exists(),
                }
            elif stat.S_ISDIR(mode):
                continue
            elif stat.S_ISFIFO(mode):
                files[rel] = {'type': 'fifo'}
            elif path.name == 'pickle.pkl':
                files[rel] = {'type': 'pickle'}
            elif path.name == 'properties.json':
                props = json.loads(path.read_text(encoding='utf-8'))
                files[rel] = {'type': 'json', 'text': json.dumps(props, sort_keys=True)}
            else:
                text = path.read_text(encoding='utf-8')
                files[rel] = {
                    'type': 'text',
                    'text': text.replace(sys.executable, '{PYTHON}').replace(str(tmp), '{TMP}'),
                }
    return files


def child(tmp, out_path, source):
    tmp = pathlib.Path(tmp)
    home = tmp / 'userhome'
    if source == '--repo':
        sys.path.insert(0, str(REPO))
    import meerschaum as mrsm
    expected = source.split('=', 1)[1] if source.startswith('--reference=') else None
    if expected is not None:
        assert mrsm.__version__ == expected, mrsm.__version__
        assert not pathlib.Path(mrsm.__file__).resolve().is_relative_to(REPO / 'meerschaum')
    else:
        assert pathlib.Path(mrsm.__file__).resolve().is_relative_to(REPO / 'meerschaum')

    from meerschaum.jobs.systemd import SystemdExecutor

    state = {
        'commands': [],
        'is_active': ['inactive'],
        'pid': '0',
        'process': None,
        'timestamp': '',
        'fail': set(),
    }

    class FakeProcess:
        def __init__(self, status):
            self._status = status

        def status(self):
            if self._status == 'gone':
                raise RuntimeError('NoSuchProcess')
            return self._status

    def fake_run_command(self, command_args, as_output=False, debug=False):
        if command_args[:2] != ['systemctl', '--user']:
            command_args = ['systemctl', '--user'] + command_args
        rendered = shlex.join(command_args[2:])
        state['commands'].append(rendered)
        sub = command_args[2]
        if as_output:
            if sub == 'is-active':
                seq = state['is_active']
                return (seq.pop(0) if len(seq) > 1 else seq[0]) if seq else ''
            if sub == 'show':
                prop = command_args[-1].split('=', 1)[1]
                if prop == 'MainPID':
                    return f"MainPID={state['pid']}"
                return f"{prop}={state['timestamp']}"
            return ''
        if sub in state['fail']:
            return False, f"Failed to execute command `{shlex.join(command_args)}`."
        return True, 'Success'

    def fake_get_job_process(self, name, debug=False):
        pid = self.get_job_pid(name, debug=debug)
        if pid is None or state['process'] is None:
            return None
        return FakeProcess(state['process'])

    SystemdExecutor.run_command = fake_run_command
    SystemdExecutor.get_job_process = fake_get_job_process
    executor = SystemdExecutor('systemd')

    results = {'create': [], 'create_failures': [], 'lifecycle': [], 'status': [], 'timestamps': []}

    def reset(**kwargs):
        state.update({
            'commands': [],
            'is_active': ['inactive'],
            'pid': '0',
            'process': None,
            'timestamp': '',
            'fail': set(),
        })
        state.update(kwargs)

    def tup(value):
        return [bool(value[0]), value[1]]

    ### create
    for case in CASES:
        reset()
        result = executor.create_job(case['name'], case['sysargs'], case.get('properties'))
        results['create'].append({
            **case,
            'result': tup(result),
            'commands': collapse(state['commands']),
            'files': render_tree(tmp, home),
        })
        ### Remove the files again so that the next case has a clean tree.
        for sub in ('systemd', 'jobs', 'logs'):
            shutil.rmtree(tmp / 'root' / sub, ignore_errors=True)
        shutil.rmtree(home / '.config' / 'systemd', ignore_errors=True)

    ### create with failing commands
    for failing in (['enable'], ['daemon-reload', 'enable'], ['daemon-reload', 'enable', 'start']):
        reset(fail=set(failing))
        result = executor.create_job('failing', ['show', 'version'], None)
        results['create_failures'].append({
            'fail': failing,
            'result': tup(result),
            'commands': collapse(state['commands']),
        })
        for sub in ('systemd', 'jobs', 'logs'):
            shutil.rmtree(tmp / 'root' / sub, ignore_errors=True)
        shutil.rmtree(home / '.config' / 'systemd', ignore_errors=True)

    ### start / stop / pause / delete on a created job
    def fresh_job(name='lifecycle', props=None):
        reset()
        executor.create_job(name, ['sync', 'pipes', '--loop'], props)
        state['commands'] = []

    def record(label, name, result, extra=None, files=False):
        entry = {
            'scenario': label,
            'result': tup(result),
            'commands': collapse(state['commands']),
        }
        if files:
            entry['files'] = render_tree(tmp, home)
        if extra:
            entry.update(extra)
        results['lifecycle'].append(entry)

    fresh_job()
    state['is_active'] = ['inactive']
    record('start_inactive', 'lifecycle', executor.start_job('lifecycle'))

    fresh_job()
    state.update(is_active=['active'], pid='4242', process='stopped')
    record('start_paused', 'lifecycle', executor.start_job('lifecycle'))

    fresh_job()
    state.update(is_active=['active', 'inactive'], pid='4242', process='running')
    record('stop_graceful', 'lifecycle', executor.stop_job('lifecycle'))

    fresh_job()
    state.update(is_active=['inactive'])
    record('stop_already_stopped', 'lifecycle', executor.stop_job('lifecycle'))

    fresh_job()
    state.update(is_active=['active'], pid='4242', process='running')
    record('stop_timeout', 'lifecycle', executor.stop_job('lifecycle'))

    fresh_job()
    state.update(is_active=['active'], pid='4242', process='running')
    record('pause', 'lifecycle', executor.pause_job('lifecycle'))

    fresh_job()
    state.update(is_active=['inactive'])
    record('delete', 'lifecycle', executor.delete_job('lifecycle'), files=True)

    fresh_job('cleanup', {'delete_after_completion': True})
    state.update(is_active=['inactive'])
    record('delete_after_completion', 'cleanup', executor.delete_job('cleanup'), files=True)

    ### Metadata and status of a created job.
    fresh_job('meta')
    state.update(is_active=['inactive'])
    metadata = executor.get_job_metadata('meta')
    metadata['daemon']['properties'].pop('label', None)
    results['metadata'] = json.loads(json.dumps(metadata, sort_keys=True))
    results['sysargs'] = executor.get_job_sysargs('meta')
    results['restart'] = executor.get_job_restart('meta')
    results['names'] = sorted(executor.get_job_names())
    results['exists'] = [executor.get_job_exists('meta'), executor.get_job_exists('missing')]

    for row in STATUS_TABLE:
        reset(is_active=[row['is_active']], pid=row['pid'], process=row['process'])
        results['status'].append({**row, 'status': executor.get_job_status('meta')})

    for text in TIMESTAMPS:
        prop, value = text.split('=', 1)
        reset(timestamp=value)
        method = executor.get_job_began if prop.startswith('Active') else executor.get_job_ended
        results['timestamps'].append({'output': text, 'value': method('meta')})

    reset(pid='4242')
    results['pid'] = [executor.get_job_pid('meta')]
    reset(pid='0')
    results['pid'].append(executor.get_job_pid('meta'))

    ### Result file.
    result_path = executor.get_result_path('meta')
    results['result'] = {'missing': list(executor.get_job_result('meta'))}
    result_path.write_text(json.dumps([True, 'done']), encoding='utf-8')
    results['result']['present'] = list(executor.get_job_result('meta'))
    result_path.write_text('not json', encoding='utf-8')
    results['result']['broken'] = list(executor.get_job_result('meta'))

    pathlib.Path(out_path).write_text(
        json.dumps(results, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        child(sys.argv[2], sys.argv[3], sys.argv[4])
        return

    reference = os.environ.get('MRSM_PARITY_REFERENCE')
    interpreter_flags = ['-s'] if reference else []
    source = f'--reference={reference}' if reference else '--repo'
    if not (MRSM_VENV / 'lib').exists():
        sys.exit(f"gen_systemd_units.py: {MRSM_VENV} is missing (it provides daemon, dill, psutil).")

    tmp = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-systemd-')).resolve()
    try:
        for sub in ('root', 'plugins', 'userhome', 'venvs', 'work'):
            (tmp / sub).mkdir()
        (tmp / 'venvs' / 'mrsm').symlink_to(MRSM_VENV)
        env = {
            'PATH': '/usr/bin:/bin',
            'HOME': (tmp / 'userhome').as_posix(),
            'MRSM_ROOT_DIR': (tmp / 'root').as_posix(),
            'MRSM_PLUGINS_DIR': (tmp / 'plugins').as_posix(),
            'MRSM_VENVS_DIR': (tmp / 'venvs').as_posix(),
            'MRSM_NO_AUTO_INSTALL': '1',
            'MRSM_CONFIG': json.dumps({
                'jobs': {'timeout_seconds': 0.3, 'check_timeout_interval_seconds': 0.05},
            }),
            'PYTHONHASHSEED': '0',
            'TZ': 'UTC',
        }
        if not reference:
            env['PYTHONPATH'] = str(REPO)
        out_path = tmp / 'out.json'
        proc = subprocess.run(
            [sys.executable, *interpreter_flags, __file__, '--child', str(tmp), str(out_path), source],
            env=env,
            cwd=str(tmp / 'work'),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout + proc.stderr)
            raise RuntimeError("gen_systemd_units.py: the child process failed.")
        OUT.write_text(out_path.read_text(encoding='utf-8'), encoding='utf-8')
        print(f"Wrote {OUT.relative_to(REPO)}.")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == '__main__':
    main()
