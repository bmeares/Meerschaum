"""Seed a released install, then verify and update the same SQLite instance with the candidate."""

import argparse
import json
import os
from pathlib import Path
import re

COMMAND = ['show', 'version']


def previous_release(changelog, version):
    versions = re.findall(r'^## (\d+\.\d+\.\d+)$', changelog.read_text(), re.MULTILINE)
    return next(v for v in versions if tuple(map(int, v.split('.'))) < version)


def check_state(stage):
    import meerschaum as mrsm
    root = Path(os.environ['MRSM_ROOT_DIR']).resolve()
    conn = mrsm.get_connector('sql:upgrade')
    assert Path(conn.value("SELECT file FROM pragma_database_list WHERE name = 'main'")) == root / 'state.db'
    pipe = mrsm.Pipe('sql:upgrade', 'readings', instance=conn)
    job_name = 'retained-command'
    expected = root / 'upgrade-state.json'
    if stage == 'seed':
        assert not expected.exists()
        pipe = mrsm.Pipe('sql:upgrade', 'readings', instance=conn, target='samples',
                         columns={'datetime': 'ts', 'id': 'id'})
        assert pipe.sync([{'ts': '2025-01-01', 'id': 1, 'value': 10}])[0]
        job = mrsm.Job(job_name, COMMAND, executor_keys='local', _properties={'owner': 'reader'})
        assert job.daemon.write_properties()[0]
        expected.write_text(json.dumps([pipe.get_id(), str(pipe.get_sync_time())]))
    pipe_id, sync_time = json.loads(expected.read_text())
    assert pipe_id is not None and sync_time.startswith('2025-01-01')
    assert pipe.get_id() == pipe_id and str(pipe.get_sync_time()) == sync_time
    job = mrsm.Job(job_name, executor_keys='local')
    assert job.sysargs == COMMAND and job.owner == 'reader'
    if stage == 'verify':
        assert conn.value('SELECT value FROM samples WHERE id=1') == 10
    if stage != 'seed':
        for _ in range(2):
            assert pipe.sync([{'ts': '2025-01-01', 'id': 1, 'value': 20}])[0]
            assert conn.value('SELECT COUNT(*) FROM samples') == 1
            assert conn.value('SELECT value FROM samples WHERE id=1') == 20
            assert pipe.get_id() == pipe_id and str(pipe.get_sync_time()) == sync_time
    print(f'{stage}: {mrsm.__version__}, pipe {pipe_id}, sync time {sync_time}, job {job.sysargs}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['seed', 'verify', 'reopen', 'previous'])
    parser.add_argument('--changelog', type=Path)
    parser.add_argument('--version')
    args = parser.parse_args()
    if args.stage == 'previous':
        version = tuple(map(int, re.match(r'^\d+\.\d+\.\d+', args.version)[0].split('.')))
        print(previous_release(args.changelog, version))
    else:
        check_state(args.stage)
