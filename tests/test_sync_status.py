#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The per-pipe sync status (rc15 contract C2) from Python: `pipe.sync()` records its outcome on
the pipe's instance, `pipe.get_sync_status()` reads it and `mrsm.get_sync_statuses()` reads every
pipe's, failures count until a success, and a job's syncs carry the job's name. The status is
written in the background: a read sees the latest sync, a script keeps its last sync when it
exits, and a removed pipe's status never comes back.
"""

import os
import subprocess
import sys
import textwrap
import time
import uuid

import pytest

import meerschaum as mrsm

KEYS = (
    'pipe_ref', 'connector_keys', 'metric_key', 'location_key', 'instance_keys',
    'last_attempt', 'last_success', 'last_failure', 'success', 'message', 'rows_inserted',
    'rows_updated', 'duration_seconds', 'consecutive_failures', 'source', 'job',
)


@pytest.fixture
def instance(tmp_path):
    label = f"rc15g_{uuid.uuid4().hex[:8]}"
    return mrsm.get_connector(
        'sql', label, flavor='sqlite', database=str(tmp_path / f"{label}.db"),
    )


def _pipe(instance, metric, **kwargs):
    return mrsm.Pipe('rc15g', metric, instance=instance, columns={'datetime': 'ts'}, **kwargs)


def test_a_sync_records_its_outcome(instance):
    pipe = _pipe(instance, 'weather')
    assert pipe.get_sync_status() is None
    result = pipe.sync([{'ts': '2026-01-01', 'v': 1}, {'ts': '2026-01-02', 'v': 2}])
    assert result.success, result.message
    status = pipe.get_sync_status()
    assert set(KEYS) <= set(status), status
    assert status['success'] is True
    assert status['rows_inserted'] == 2
    assert status['consecutive_failures'] == 0
    assert status['source'] == 'python'
    assert status['job'] is None
    assert status['last_failure'] is None
    assert status['last_attempt'] == status['last_success']
    assert status['pipe_ref'] == f"{instance}|rc15g|weather|None"
    assert status['location_key'] is None
    from datetime import datetime
    assert datetime.fromisoformat(status['last_success']).tzinfo is not None

    statuses = mrsm.get_sync_statuses(instance)
    assert statuses[status['pipe_ref']] == status
    assert mrsm.get_sync_statuses(str(instance)) == statuses


def test_failures_count_until_a_success(instance):
    source = mrsm.get_connector(
        'sql', f"{instance.label}_src", flavor='sqlite', database=instance.database,
    )
    pipe = mrsm.Pipe(
        str(source), 'rollup', instance=instance,
        columns={'datetime': 'ts'},
        parameters={'fetch': {'definition': 'SELECT ts, v FROM rc15g_missing'}},
    )
    for attempt in (1, 2):
        result = pipe.sync()
        assert not result.success
        status = pipe.get_sync_status()
        assert status['success'] is False
        assert status['consecutive_failures'] == attempt
        assert status['last_success'] is None
        assert status['message']

    source.exec("CREATE TABLE rc15g_missing (ts TEXT, v INTEGER)")
    source.exec("INSERT INTO rc15g_missing VALUES ('2026-01-01 00:00:00', 1)")
    result = pipe.sync()
    assert result.success, result.message
    status = pipe.get_sync_status()
    assert status['success'] is True
    assert status['consecutive_failures'] == 0
    assert status['last_failure'] is not None and status['last_success'] is not None

    ### Deleting the pipe forgets its status.
    assert pipe.delete()[0]
    assert pipe.get_sync_status() is None


def test_a_job_names_itself(instance, monkeypatch):
    monkeypatch.setenv('MRSM_DAEMON_ID', 'rc15g-job')
    pipe = _pipe(instance, 'in_a_job')
    assert pipe.sync([{'ts': '2026-01-01', 'v': 1}]).success
    status = pipe.get_sync_status()
    assert (status['source'], status['job']) == ('job', 'rc15g-job')


def test_internal_and_temporary_pipes_have_no_status(instance):
    pipe = _pipe(instance, 'scratch', temporary=True)
    assert pipe.sync([{'ts': '2026-01-01', 'v': 1}]).success
    assert pipe.get_sync_status() is None
    assert f"{instance}|rc15g|scratch|None" not in mrsm.get_sync_statuses(instance)


def test_a_read_sees_the_latest_sync(instance):
    pipe = _pipe(instance, 'latest')
    assert pipe.sync([{'ts': '2026-01-01', 'v': 1}, {'ts': '2026-01-02', 'v': 2}]).success
    ### The second success comes within the debounce window: it is held, yet a read sees it.
    assert pipe.sync([{'ts': '2026-01-03', 'v': 3}]).success
    assert pipe.get_sync_status()['rows_inserted'] == 1
    assert mrsm.get_sync_statuses(instance)[pipe_ref(pipe)]['rows_inserted'] == 1


def pipe_ref(pipe):
    return f"{pipe.instance_keys}|{pipe.connector_keys}|{pipe.metric_key}|{pipe.location_key}"


def test_a_removed_pipe_keeps_no_status(instance):
    pipes = {op: _pipe(instance, f'removed_{op}') for op in ('delete', 'drop', 'clear')}
    for pipe in pipes.values():
        ### The second success is held by the debounce.
        assert pipe.sync([{'ts': '2026-01-01', 'v': 1}]).success
        assert pipe.sync([{'ts': '2026-01-02', 'v': 2}]).success
    for op, pipe in pipes.items():
        assert getattr(pipe, op)()[0]
        assert pipe.get_sync_status() is None, op

    ### The debounce window ends: nothing brings a status back.
    time.sleep(6)
    statuses = mrsm.get_sync_statuses(instance)
    for op, pipe in pipes.items():
        assert pipe.get_sync_status() is None, op
        assert pipe_ref(pipe) not in statuses, op


def test_a_script_keeps_its_last_sync_when_it_exits(instance):
    script = textwrap.dedent(
        f"""
        import meerschaum as mrsm
        conn = mrsm.get_connector(
            'sql', {instance.label!r}, flavor='sqlite', database={instance.database!r},
        )
        pipe = mrsm.Pipe('rc15g', 'exiting', instance=conn, columns={{'datetime': 'ts'}})
        assert pipe.sync([{{'ts': '2026-01-01', 'v': 1}}, {{'ts': '2026-01-02', 'v': 2}}]).success
        ### Held by the debounce: written when the script exits.
        assert pipe.sync([{{'ts': '2026-01-03', 'v': 3}}]).success
        """
    )
    done = subprocess.run(
        [sys.executable, '-c', script],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    status = _pipe(instance, 'exiting').get_sync_status()
    assert status['success'] is True
    assert status['rows_inserted'] == 1, status


def test_python_syncs_after_the_command_line_ran_in_process_are_python(instance):
    """`_native.main()` (the `mrsm` console script) runs the command line inside Python: the
    syncs the same process makes afterwards are `python` again, not `cli`."""
    script = textwrap.dedent(
        f"""
        import meerschaum as mrsm
        from meerschaum._core import _native
        conn = mrsm.get_connector(
            'sql', {instance.label!r}, flavor='sqlite', database={instance.database!r},
        )
        try:
            _native.main(['show', 'version'])
        except SystemExit:
            pass
        pipe = mrsm.Pipe('rc15g', 'after_main', instance=conn, columns={{'datetime': 'ts'}})
        assert pipe.sync([{{'ts': '2026-01-01', 'v': 1}}]).success
        print('SOURCE=' + pipe.get_sync_status()['source'])
        """
    )
    done = subprocess.run(
        [sys.executable, '-c', script],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert 'SOURCE=python' in done.stdout, done.stdout + done.stderr
