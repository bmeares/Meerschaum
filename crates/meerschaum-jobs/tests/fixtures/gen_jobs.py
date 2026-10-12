#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the `meerschaum-jobs` layout fixtures (`jobs.json`) from the Python `Job` / `Daemon`
implementation.

Run from the repository root against the released reference (what CI does; the output must be
byte-identical from run to run):

    MRSM_PLUGINS_DIR=$(mktemp -d) scripts/parity/run.sh \
        crates/meerschaum-jobs/tests/fixtures/gen_jobs.py

or against the working tree:

    PYTHONPATH=$(pwd) python crates/meerschaum-jobs/tests/fixtures/gen_jobs.py

The work happens in a child process of the same interpreter with a minimal, controlled
environment (`env -i` plus only `PATH`, `HOME` (temporary), `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`,
`MRSM_VENVS_DIR`, `MRSM_NO_AUTO_INSTALL` and `PYTHONHASHSEED=0`): a temporary root, an empty plugins
directory, a home directory, and a venvs directory whose `mrsm` venv links to
`test_root/venvs/mrsm` (the job machinery imports `daemon`, `dill` and `psutil` from it; nothing
is installed). Nothing from the host environment reaches the daemons or the fixture; the only
environment maps recorded are the synthetic `env` given to a job and Python's job defaults.

The child creates jobs with `meerschaum.jobs.Job` whose sysargs exit immediately
(`show version --nopretty`, `bootstrap foo`) or sleep in a pipeline loop
(`show version --nopretty : --loop --min-seconds 60`) until the generator pauses or stops them,
and records, per scenario:

- `files`: the snapshot of `jobs/<name>/` and `logs/` (every regular file with its contents, the
  stdin FIFO as `{"type": "fifo"}`, `pickle.pkl` as `{"type": "pickle"}`);
- `state`: what the Python `Job` reports (`status`, `pid`, `sysargs`, `result`, `get_logs()`, ...).

Timestamps and PIDs are normalized so that the fixture is stable: `process.began/paused/ended`
become `2000-01-01T00:00:0N.00000N`, the stop file's `stop_time` becomes
`2000-01-01T00:00:04.000004+00:00`, log line prefixes become `2000-01-01 00:00 | `, and PIDs
become `{PID}`.

It also records deterministic `RotatingFile` sessions (rotation, `read()`, `readlines()` and
the cursor) with a fixed timestamp prefix, the `FileDescriptorInterceptor` transform, and the
`systemd` unit and socket texts.
"""

import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
OUT = HERE / 'jobs.json'
MRSM_VENV = REPO / 'test_root' / 'venvs' / 'mrsm'

TS_LOG = '2000-01-01 00:00 | '
TS_PROCESS = {
    'began': '2000-01-01T00:00:01.000001',
    'paused': '2000-01-01T00:00:02.000002',
    'ended': '2000-01-01T00:00:03.000003',
}
TS_STOP = '2000-01-01T00:00:04.000004+00:00'
LOG_TS_RE = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ')
LOOP_SYSARGS = ['show', 'version', '--nopretty', ':', '--loop', '--min-seconds', '60']


def norm_log(text):
    if text is None:
        return None
    return LOG_TS_RE.sub(TS_LOG, text)


def norm_properties(props):
    props = json.loads(json.dumps(props))
    process = props.get('process', None)
    if isinstance(process, dict):
        for key in list(process):
            if key in TS_PROCESS and process[key] is not None:
                process[key] = TS_PROCESS[key]
    return props


def snapshot(root, name):
    """Snapshot `jobs/<name>/` and the job's log files."""
    files = {}
    job_dir = root / 'jobs' / name
    paths = sorted(job_dir.rglob('*')) if job_dir.exists() else []
    logs_dir = root / 'logs'
    if logs_dir.exists():
        paths += sorted(
            p for p in logs_dir.iterdir()
            if p.name.startswith(name + '.log') or p.name == f'.{name}.log.offset'
        )
    for path in paths:
        rel = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if stat.S_ISFIFO(mode):
            files[rel] = {'type': 'fifo'}
            continue
        if path.name == 'pickle.pkl':
            files[rel] = {'type': 'pickle'}
            continue
        text = path.read_text(encoding='utf-8')
        if path.name == 'properties.json':
            files[rel] = {
                'type': 'json',
                'text': json.dumps(norm_properties(json.loads(text))),
            }
        elif path.name == '.stop.json':
            data = json.loads(text)
            data['stop_time'] = TS_STOP
            files[rel] = {'type': 'json', 'text': json.dumps(data)}
        elif path.name == 'process.pid':
            files[rel] = {'type': 'pid', 'text': '{PID}'}
        else:
            files[rel] = {'type': 'text', 'text': norm_log(text)}
    return files


def _truncate_logs(scenario, needle):
    """Keep the log text up to and including `needle` (in the files and in the state)."""
    def _cut(text):
        ix = text.index(needle) + len(needle)
        return text[:ix]
    for rel, entry in scenario['files'].items():
        if rel.startswith('logs/') and needle in entry['text']:
            entry['text'] = _cut(entry['text'])
    scenario['state']['logs'] = _cut(scenario['state']['logs'])
    scenario['truncated_after'] = needle


def job_state(job):
    """Everything the Python `Job` reports about a local job."""
    from meerschaum.jobs._Job import Job

    def _ts(key, value):
        if value is None:
            return None
        return TS_PROCESS[key]

    status = job.status
    pid = job.pid
    stop_time = job.stop_time
    return {
        'status': status,
        'pid': '{PID}' if pid is not None else None,
        'sysargs': job.sysargs,
        'executor_keys': job.executor_keys,
        'result': list(job.result),
        'logs': norm_log(job.get_logs()),
        'label': job.label,
        'daemon_label': job.daemon.label,
        'restart': job.restart,
        'began': _ts('began', job.began),
        'paused': _ts('paused', job.paused),
        'ended': _ts('ended', job.ended),
        'stop_time': TS_STOP if stop_time is not None else None,
        'hidden': job.hidden,
        'exists': job.exists(),
        'env': job.env,
        'delete_after_completion': job.delete_after_completion,
        'is_blocking_on_stdin': job.is_blocking_on_stdin(),
        'str': str(job),
        'properties': norm_properties(job.daemon.properties),
    }


def wait_for(predicate, timeout=60.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError("Timed out while waiting for a job.")


def child(tmp, out_path, source):
    tmp = pathlib.Path(tmp)
    root = tmp / 'root'
    if source == '--repo':
        sys.path.insert(0, str(REPO))
    import meerschaum as mrsm
    expected = source.split('=', 1)[1] if source.startswith('--reference=') else None
    if expected is not None:
        assert mrsm.__version__ == expected, mrsm.__version__
        assert not pathlib.Path(mrsm.__file__).resolve().is_relative_to(REPO / 'meerschaum')
    else:
        assert pathlib.Path(mrsm.__file__).resolve().is_relative_to(REPO / 'meerschaum')

    from meerschaum.jobs import Job
    from meerschaum.utils.daemon import Daemon

    results = {'scenarios': {}, 'rotating': [], 'interceptor': [], 'systemd': [], 'names': {}}
    jobs = []

    def record(name, job, files=None):
        results['scenarios'][name] = {
            'files': files if files is not None else snapshot(root, job.name),
            'state': job_state(job),
        }

    def log_has(job, needle):
        return needle in (job.get_logs() or '')

    try:
        ### Quick jobs run shell scripts (the shell-command fallback of `entry`) which sleep after
        ### printing: Python's interceptor threads are daemon threads, so output written right
        ### before the process exits may or may not reach the log.
        work = pathlib.Path.cwd()
        (work / 'ok.sh').write_text('echo hello\necho "line two"\nsleep 0.5\n', encoding='utf-8')
        (work / 'fail.sh').write_text('echo "about to fail"\nsleep 0.5\nexit 3\n', encoding='utf-8')
        (work / 'env.sh').write_text('echo "FOO=$FOO N=$MRSM_TEST_N"\nsleep 0.5\n', encoding='utf-8')
        ### Relative to the daemon's working directory (the generator's cwd), so the recorded
        ### sysargs and labels contain no temporary path.
        ok_sysargs = ['bash', 'ok.sh']

        ### A job which exits with a success.
        job = Job('done-ok', ok_sysargs, executor_keys='local')
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: job.status == 'stopped' and job.daemon.properties.get('result'))
        record('done-ok', job)

        ### `Daemon.readlines()` persists its cursor in `logs/.<name>.log.offset`.
        daemon = Daemon(daemon_id='done-ok')
        lines = daemon.readlines()
        offset_text = daemon.log_offset_path.read_text(encoding='utf-8')
        results['readlines'] = {
            'name': 'done-ok',
            'lines': [norm_log(line) for line in lines],
            'offset': offset_text,
            'again': [norm_log(line) for line in daemon.readlines()],
        }

        ### A job which fails. The daemon prints the failed result right before exiting, so
        ### only the text before it is kept.
        job = Job('failed', ['bash', 'fail.sh'], executor_keys='local')
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: job.status == 'stopped' and job.daemon.properties.get('result'))
        record('failed', job)
        _truncate_logs(results['scenarios']['failed'], 'about to fail\n')

        ### Environment variables are stored in the properties.
        job = Job(
            'with-env',
            ['bash', 'env.sh'],
            env={'FOO': 'bar', 'MRSM_TEST_N': '1'},
            executor_keys='local',
        )
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: job.status == 'stopped' and job.daemon.properties.get('result'))
        record('with-env', job)

        ### A hidden job.
        job = Job('.hidden-job', ok_sysargs, executor_keys='local')
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: job.status == 'stopped' and job.daemon.properties.get('result'))
        record('.hidden-job', job)

        ### A restarting job, snapshotted while it sleeps between laps.
        job = Job('running', LOOP_SYSARGS, executor_keys='local')
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: log_has(job, 'Sleeping for 60'))
        record('running', job)

        ### The same job, paused.
        assert job.pause()[0]
        wait_for(lambda: job.status == 'paused')
        record('paused', job)

        ### Stopping a paused job kills it: no result, no `ended`.
        assert job.stop()[0]
        wait_for(lambda: job.status == 'stopped')
        record('killed', job)

        ### A restarting job stopped with SIGINT (the stop file records `quit`).
        job = Job('stopped', LOOP_SYSARGS, executor_keys='local')
        jobs.append(job)
        assert job.start()[0]
        wait_for(lambda: log_has(job, 'Sleeping for 60'))
        assert job.stop()[0]
        wait_for(lambda: job.status == 'stopped' and job.daemon.properties.get('result'))
        record('stopped', job)
        ### stdout and stderr are intercepted by two threads: their relative order after the
        ### SIGINT is not deterministic, so only the lines before it are kept.
        _truncate_logs(results['scenarios']['stopped'], 'Sleeping for 60.0 seconds...')

        ### Deleting a job removes its directory, its logs and its log offset file.
        job = Job('done-ok', executor_keys='local')
        delete_result = job.delete()
        results['deleted'] = {
            'name': 'done-ok',
            'result': list(delete_result),
            'files': snapshot(root, 'done-ok'),
            'exists': job.exists(),
        }
    finally:
        for job in jobs:
            try:
                if job.status != 'stopped':
                    job.daemon.kill(timeout=3)
            except Exception:
                pass

    results['rotating'] = rotating_sessions(tmp / 'rotating')
    results['interceptor'] = interceptor_sessions(tmp / 'interceptor')
    results['systemd'] = systemd_cases(tmp)
    results['strip_timestamp'] = strip_timestamp_cases()
    from meerschaum.utils.daemon._names import _bank
    results['names'] = _bank

    text = json.dumps(results, indent=1, ensure_ascii=False, sort_keys=False) + '\n'
    text = text.replace(str(tmp), '{TMP}')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(text)


def rotating_sessions(base):
    """Deterministic `RotatingFile` sessions: each op and its result."""
    from meerschaum.utils.daemon import RotatingFile

    class FixedRotatingFile(RotatingFile):
        def get_timestamp_prefix_str(self):
            return TS_LOG

    sessions = []
    cases = [
        {
            'name': 'plain',
            'kwargs': {'num_files_to_keep': 3, 'max_file_size': 100, 'write_timestamps': False},
            'ops': (
                [['write', f'line {i:02d}: ' + ('x' * (i % 7)) + '\n'] for i in range(20)]
                + [['readlines'], ['write', 'tail without newline'], ['write', ' ... continued\n'],
                   ['readlines'], ['read'], ['fresh_read'], ['fresh_readlines']]
            ),
        },
        {
            'name': 'timestamps',
            'kwargs': {'num_files_to_keep': 2, 'max_file_size': 150, 'write_timestamps': True},
            'ops': (
                [['write', f'message {i}']for i in range(12)]
                + [['fresh_read'], ['fresh_readlines'], ['write', 'two\nlines'], ['fresh_read']]
            ),
        },
        {
            'name': 'unicode',
            'kwargs': {'num_files_to_keep': 5, 'max_file_size': 100, 'write_timestamps': False},
            'ops': (
                [['write', 'é' * 30 + '\n'], ['write', '💬 ' * 20 + '\n'], ['write', 'ascii\n']]
                + [['fresh_read'], ['fresh_readlines']]
            ),
        },
        {
            'name': 'seek',
            'kwargs': {'num_files_to_keep': 4, 'max_file_size': 100, 'write_timestamps': False},
            'ops': (
                [['write', f'{i}' * 30 + '\n'] for i in range(8)]
                + [['seek', 0], ['readlines'], ['seek', 0], ['read'], ['readlines']]
            ),
        },
    ]
    for case in cases:
        path = base / case['name'] / f"{case['name']}.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        rf = FixedRotatingFile(path, **case['kwargs'])
        results = []
        for op in case['ops']:
            kind = op[0]
            if kind == 'write':
                rf.write(op[1])
                ### `write()` reopens the latest subfile on every call (`lost_latest_handle` is
                ### always true: `_current_file_obj.name` is a full path, so its index parses as
                ### -1), and the replaced handle is only flushed when it is garbage-collected on
                ### the next write. Flush here so each snapshot shows every write.
                if rf._current_file_obj is not None:
                    rf._current_file_obj.flush()
                value = None
            elif kind == 'read':
                value = rf.read()
            elif kind == 'readlines':
                value = rf.readlines()
            elif kind == 'seek':
                rf.seek(op[1])
                value = None
            elif kind == 'fresh_read':
                value = FixedRotatingFile(path, **case['kwargs']).read()
            elif kind == 'fresh_readlines':
                value = FixedRotatingFile(path, **case['kwargs']).readlines()
            results.append({
                'op': op,
                'value': value,
                'cursor': list(rf._cursor),
                'files': {
                    p.name: p.read_text(encoding='utf-8')
                    for p in sorted(path.parent.iterdir(), key=lambda p: p.name)
                },
            })
        rf.close()
        results.append({
            'op': ['close'],
            'value': None,
            'cursor': list(rf._cursor),
            'files': {
                p.name: p.read_text(encoding='utf-8')
                for p in sorted(path.parent.iterdir(), key=lambda p: p.name)
            },
        })
        sessions.append({'name': case['name'], 'kwargs': case['kwargs'], 'ops': results})
    return sessions


def interceptor_sessions(base):
    """Drive `FileDescriptorInterceptor` with chunks separated by pauses."""
    from meerschaum.utils.daemon.FileDescriptorInterceptor import FileDescriptorInterceptor
    from meerschaum.utils.threading import Thread

    cases = [
        ['hello\n'],
        ['hello\n', 'a\nb', 'c\n\n'],
        ['no newline', ' at all'],
        ['\n', '\n'],
    ]
    sessions = []
    base.mkdir(parents=True, exist_ok=True)
    for i, chunks in enumerate(cases):
        path = base / f'{i}.log'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        interceptor = FileDescriptorInterceptor(fd, lambda: TS_LOG)
        thread = Thread(target=interceptor.start_interception, daemon=True)
        thread.start()
        for chunk in chunks:
            os.write(fd, chunk.encode('utf-8'))
            time.sleep(0.3)
        interceptor.stop_interception()
        thread.join()
        os.close(fd)
        sessions.append({'chunks': chunks, 'output': path.read_text(encoding='utf-8')})
    return sessions


def systemd_cases(tmp):
    """Unit and socket texts from `SystemdExecutor` (no `systemctl` calls)."""
    from meerschaum.jobs.systemd import SystemdExecutor
    executor = SystemdExecutor('systemd')
    cases = [
        {'name': 'sync-weather', 'sysargs': ['sync', 'pipes', '-c', 'plugin:noaa', '--loop', '--min-seconds', '3600']},
        {'name': 'my job', 'sysargs': ['show', 'version', '--nopretty'], 'properties': {'env': {'FOO': 'bar baz', 'QUOTE': "it's"}}},
        {'name': 'cleanup', 'sysargs': ['delete', 'pipes', '-c', 'sql:main', '-y'], 'properties': {'delete_after_completion': True}},
        {'name': 'quoted', 'sysargs': ['sql', 'main', "SELECT 'x' AS y", '--name', 'quoted']},
    ]
    out = []
    for case in cases:
        if case.get('properties'):
            job = executor.get_hidden_job(case['name'], case['sysargs'], properties=case['properties'])
            ### Like `create_job`: without the pickle, the next `Daemon(daemon_id=name)` cannot
            ### rebuild the daemon from a target-less properties file and deletes it.
            job._set_externally_managed()
            job.daemon.write_pickle()
            job.daemon.write_properties()
        unit = executor.get_service_file_text(case['name'], case['sysargs'])
        unit = unit.replace(sys.executable, '{PYTHON}')
        out.append({
            **case,
            'service_name': executor.get_service_name(case['name']),
            'service_file_path': executor.get_service_file_path(case['name']).as_posix(),
            'symlink_path': executor.get_service_symlink_file_path(case['name']).as_posix(),
            'logs_path': executor.get_service_logs_path(case['name']).as_posix(),
            'socket_path': executor.get_socket_path(case['name']).as_posix(),
            'result_path': executor.get_result_path(case['name']).as_posix(),
            'unit': unit,
            'socket': executor.get_socket_file_text(case['name']),
            'parsed_sysargs': _parse_exec_start(unit),
            'restart': any(flag in case['sysargs'] for flag in ('-s', '--restart', '--loop', '--schedule', '--cron')),
        })
    return out


def _parse_exec_start(unit):
    """Port of `SystemdExecutor.get_job_sysargs` on unit text."""
    import shlex
    for line in unit.splitlines(keepends=True):
        if line.startswith('ExecStart='):
            sysargs_str = line.split(' -m meerschaum ')[-1].split('<')[0]
            return shlex.split(sysargs_str)
    return []


def strip_timestamp_cases():
    from meerschaum.utils.formatting._jobs import strip_timestamp_from_line
    lines = [
        '2000-01-01 00:00 | hello\n',
        '2024-12-31 23:59 | x',
        '2024-12-31 23:59 |',
        '2024-13-31 23:59 | bad month',
        'no timestamp here',
        '',
        '2024-01-01 00:00',
        '2024-01-01 00:00 | ',
        '   2024-01-01 00:00 | leading space',
    ]
    return [{'line': line, 'stripped': strip_timestamp_from_line(line)} for line in lines]


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        child(sys.argv[2], sys.argv[3], sys.argv[4])
        return

    reference = os.environ.get('MRSM_PARITY_REFERENCE')
    interpreter_flags = ['-s'] if reference else []
    source = f'--reference={reference}' if reference else '--repo'
    if not (MRSM_VENV / 'lib').exists():
        sys.exit(f"gen_jobs.py: {MRSM_VENV} is missing (it provides daemon, dill and psutil).")

    tmp = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-jobs-')).resolve()
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
            'PYTHONHASHSEED': '0',
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
            raise RuntimeError("gen_jobs.py: the child process failed.")
        OUT.write_text(out_path.read_text(encoding='utf-8'), encoding='utf-8')
        print(f"Wrote {OUT.relative_to(REPO)}.")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == '__main__':
    main()
