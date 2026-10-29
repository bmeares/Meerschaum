"""Native numeric history survives readers and excludes ungranted metrics before aggregation."""

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import meerschaum as mrsm
from meerschaum import _core
from tests.connectors import conns, get_flavors


METRICS_FLAVORS = [flavor for flavor in get_flavors() if flavor in conns and flavor != 'api']


@pytest.mark.parametrize('flavor', METRICS_FLAVORS)
def test_native_metrics_capture_replacements_on_each_instance_backend(flavor):
    """Best-effort telemetry must not hide a broken backend schema or affected-row decoder."""
    if flavor != 'sqlite' and os.environ.get('MRSM_TEST_ISOLATED') != '1':
        from tests._binary import skip_or_fail
        skip_or_fail('Live metrics backend checks require scripts/ci/services.py isolation.')
    conn = conns[flavor]
    pipe = mrsm.Pipe(str(conn), 'rc16_metrics_' + uuid.uuid4().hex[:12], instance=conn,
                     columns={'id': 'id'}, dtypes={'id': 'int', 'value': 'int'})
    try:
        mrsm.initialize_engine_metrics(instance=conn)
        first = pipe.sync([{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
        assert first.success, first.message
        replacement = pipe.sync([{'id': 2, 'value': 30}], replace=True)
        assert replacement.success, replacement.message
        _core.flush_sync_statuses(timeout=10.0)
        result = mrsm.get_engine_metrics(instance=conn, pipe=pipe, limit=10)
        summary = result['summary']
        assert summary['syncs'] == 2, result
        assert summary['succeeded'] == 2 and summary['failed'] == 0, result
        assert summary['inserted'] == 3 and summary['deleted'] == 2, result
        assert summary['rows'] == 5, result
        assert summary['duration_seconds'] >= 0, result
        assert all(item['metric_key'] == pipe.metric_key for item in result['history'])
    finally:
        pipe.delete()


def test_sdk_metrics_are_persisted_and_visibility_is_applied_before_aggregation(tmp_path):
    conn = mrsm.get_connector('sql', 'metrics_sdk', flavor='sqlite',
                              database=str(tmp_path / 'metrics.db'))
    public = mrsm.Pipe(str(conn), 'public', instance=conn, columns={'id': 'id'})
    private = mrsm.Pipe(str(conn), 'private', instance=conn,
                        parameters={'access': {'owner': 'owner', 'visibility': 'private'}},
                        columns={'id': 'id'})
    for pipe, rows in ((public, 2), (private, 7)):
        success, message = pipe.sync([{'id': i} for i in range(rows)])
        assert success, message
    _core.flush_sync_statuses(timeout=5.0)
    end = datetime.now(timezone.utc) + timedelta(seconds=1)
    result = mrsm.get_engine_metrics(instance=conn, pipe=public, end=end,
                                      visible_refs=[f'{conn}|{conn}|public|None'],
                                      visible_pipelines=[])
    assert result['summary']['syncs'] == 1
    assert result['summary']['rows'] == 2
    assert result['summary']['success_rate'] == 1.0
    assert len(result['history']) == 1
    assert 'private' not in json.dumps(result)
    assert mrsm.get_engine_metrics(instance=conn, visible_refs=[])['summary']['syncs'] == 0
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')}
    root = tmp_path / 'reader-root'
    root.mkdir()
    plugins = tmp_path / 'reader-plugins'
    plugins.mkdir()
    env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins))
    completed = subprocess.run(
        [sys.executable, '-c',
         "import json,sys; import meerschaum as m; "
         "c=m.get_connector('sql','metrics_sdk',flavor='sqlite',database=sys.argv[1]); "
         "print(json.dumps(m.get_engine_metrics(instance=c)['summary']))",
         str(tmp_path / 'metrics.db')],
        env=env, capture_output=True, text=True, check=True, timeout=30,
    )
    summary = json.loads(completed.stdout.strip().splitlines()[-1])
    assert summary['syncs'] == 2
    assert summary['rows'] == 9


def test_sdk_metrics_initialization_and_invalid_bounds(tmp_path):
    conn = mrsm.get_connector('sql', 'metrics_init', flavor='sqlite',
                              database=str(tmp_path / 'empty.db'))
    assert mrsm.get_engine_metrics(instance=conn)['history'] == []
    mrsm.initialize_engine_metrics(instance=conn)
    assert mrsm.get_engine_metrics(instance=conn)['summary']['syncs'] == 0
    with pytest.raises((ValueError, RuntimeError)):
        mrsm.get_engine_metrics(instance=conn, begin='not-a-date')
    with pytest.raises((ValueError, RuntimeError)):
        mrsm.get_engine_metrics(instance=conn, begin=datetime.now(timezone.utc),
                                end=datetime.now(timezone.utc) - timedelta(hours=1))


def test_equal_connector_aliases_keep_their_status_and_metric_queues_on_their_own_database(tmp_path):
    from meerschaum.connectors.sql import SQLConnector
    label = 'metrics_owners_' + uuid.uuid4().hex[:10]
    first = SQLConnector(label=label, flavor='sqlite', database=str(tmp_path / 'first.db'))
    second = SQLConnector(label=label, flavor='sqlite', database=str(tmp_path / 'second.db'))
    assert str(first) == str(second)
    pipe_a = mrsm.Pipe('plugin:synthetic', 'shared', instance=first, columns={'id': 'id'})
    pipe_b = mrsm.Pipe('plugin:synthetic', 'shared', instance=second, columns={'id': 'id'})
    try:
        mrsm.initialize_engine_metrics(instance=first)
        mrsm.initialize_engine_metrics(instance=second)
        assert pipe_a.sync([{'id': 0}]).success
        assert pipe_a.get_sync_status()['rows_inserted'] == 1
        assert pipe_a.sync([{'id': i} for i in range(4)]).success
        assert pipe_b.sync([{'id': i} for i in range(7)]).success
        _core.flush_sync_statuses(timeout=5.0)
        assert pipe_a.get_sync_status()['rows_inserted'] == 3
        assert pipe_b.get_sync_status()['rows_inserted'] == 7
        metrics_a = mrsm.get_engine_metrics(instance=first, pipe=pipe_a)['summary']
        metrics_b = mrsm.get_engine_metrics(instance=second, pipe=pipe_b)['summary']
        assert (metrics_a['syncs'], metrics_a['rows']) == (2, 4)
        assert (metrics_b['syncs'], metrics_b['rows']) == (1, 7)
    finally:
        pipe_a.delete()
        pipe_b.delete()

