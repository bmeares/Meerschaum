"""Hold publication for one exact native job in the owned container."""
import json
import os
from pathlib import Path
import re
import signal
import sys
import time


def stat(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return fields[0], int(fields[1]), fields[19]


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < deadline, 'owned process did not reach the required state'
        time.sleep(.01)


mode, control, name, revision, *args = sys.argv[1:]
context = name.removeprefix('mrsm-')
assert name == f'mrsm-{context}' and re.fullmatch('[a-f0-9]{32}', context)
assert revision.isdecimal() and int(revision) > 0
job = Path(control, 'jobs', name)
properties = job / 'properties.json'

if mode == 'result':
    data = json.loads(properties.read_text())
    print(json.dumps({'ended': data['process']['ended'], 'result': data['result']}))
elif mode == 'pause':
    record = Path(control, 'execution-contexts', context, f'job-lifecycle-{revision}.json')
    before = record.read_bytes()
    lifecycle = json.loads(before)
    generation = lifecycle['generation']
    assert generation['context'] == context and generation['revision'] == int(revision)
    assert not lifecycle['cleanup_complete']

    worker = lifecycle['worker']['pid']
    supervisor = lifecycle['supervisor']['pid']
    assert worker > 1 and supervisor > 1 and worker != supervisor
    assert int((job / 'process.pid').read_text()) == worker
    supervisor_state, _, supervisor_birth = stat(supervisor)
    worker_state, parent, worker_birth = stat(worker)
    assert supervisor_state not in ('Z', 'X', 'T')
    assert worker_state not in ('Z', 'X', 'T') and parent == supervisor

    supervisor_fd = os.pidfd_open(supervisor)
    worker_fd = os.pidfd_open(worker)
    try:
        assert stat(supervisor)[2] == supervisor_birth
        assert stat(worker)[1:] == (supervisor, worker_birth)
        assert record.read_bytes() == before
        signal.pidfd_send_signal(supervisor_fd, signal.SIGSTOP)
        try:
            wait_for(lambda: stat(supervisor)[0] == 'T')
            signal.pidfd_send_signal(worker_fd, signal.SIGKILL)
            wait_for(lambda: stat(worker) == ('Z', supervisor, worker_birth))
            data = json.loads(properties.read_text())
            assert not data['process'].get('ended')
            assert data.get('result') is None
            print(json.dumps({'pid': supervisor, 'birth': supervisor_birth}))
        except BaseException:
            signal.pidfd_send_signal(supervisor_fd, signal.SIGCONT)
            raise
    finally:
        os.close(worker_fd)
        os.close(supervisor_fd)
elif mode == 'resume':
    paused = json.loads(args[0])
    supervisor = paused['pid']
    assert supervisor > 1
    fd = os.pidfd_open(supervisor)
    try:
        state, _, birth = stat(supervisor)
        assert state == 'T' and birth == paused['birth']
        signal.pidfd_send_signal(fd, signal.SIGCONT)
    finally:
        os.close(fd)
else:
    raise AssertionError('unknown fixture operation')
