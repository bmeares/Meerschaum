"""Built-in Stack dashboards use native metrics, existing provisioning and one data source."""

import json
from pathlib import Path
import re

import pytest

RESOURCES = Path(__file__).resolve().parents[1] / 'crates/meerschaum-core/src/config/grafana'
DASHBOARDS = sorted(RESOURCES.glob('*.json'))


def test_all_dashboard_resources_are_present_and_have_distinct_stable_ids():
    dashboards = [json.loads(path.read_text()) for path in DASHBOARDS]
    assert {dashboard['uid'] for dashboard in dashboards} == {
        'mrsm-performance', 'mrsm-pipe-performance', 'mrsm-pipeline-performance',
    }


@pytest.mark.parametrize('resource', DASHBOARDS, ids=lambda path: path.stem)
def test_dashboards_keep_filters_units_and_empty_history_safe(resource):
    dashboard = json.loads(resource.read_text())
    variables = {variable['name']: variable for variable in dashboard['templating']['list']}
    assert {'datasource', 'schema', 'metrics_table', 'runs_table', 'pipeline', 'pipe', 'result'} <= variables.keys()
    assert variables['datasource']['current']['text'] == 'Meerschaum Main'
    assert variables['schema']['current']['value'] == '_mrsm_internal'
    assert 'information_schema.tables' in variables['schema']['query']
    assert 'information_schema.tables' in variables['metrics_table']['query']
    assert '${metrics_table:sqlstring}' in variables['runs_table']['query']
    for name in ('pipeline', 'pipe', 'result'):
        # A custom, quoted SQL All value remains valid even with no variable-query rows.
        assert variables[name]['includeAll']
        assert variables[name]['allValue'] == "'__all'"
    ids = [panel['id'] for panel in dashboard['panels']]
    assert len(set(ids)) == len(ids)
    for panel in dashboard['panels']:
        assert panel['datasource']['uid'] == '${datasource}'
        for target in panel['targets']:
            sql = target['rawSql']
            assert '${metrics_table:raw}' in sql and '${runs_table:raw}' in sql
            assert '$__timeFilter(m.ts)' in sql
            assert '${pipe:sqlstring}' in sql and '${pipeline:sqlstring}' in sql
            assert 'NULLIF' in sql or panel['type'] == 'table' or 'count(' in sql or 'sum(' in sql or 'avg(' in sql
            assert not re.search(r'\b(?:INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b', sql)
            assert not re.search(r'https?://|password|credentials|message', sql, re.I)
    assert any(panel['fieldConfig']['defaults'].get('unit') == 's' for panel in dashboard['panels'])


@pytest.mark.parametrize('resource', DASHBOARDS, ids=lambda path: path.stem)
def test_dashboards_label_pipes_as_pipe_calls_without_the_default_instance(resource):
    """Pipes read `Pipe('ck', 'mk'[, 'lk'][, instance='ik'])`, as `str(pipe)` writes them."""
    dashboard = json.loads(resource.read_text())
    variables = {variable['name']: variable for variable in dashboard['templating']['list']}
    default = variables['default_instance']
    # Filled with the configured default instance when Meerschaum writes the dashboard.
    assert default['type'] == 'constant' and default['hide'] == 2
    assert default['query'] == '__MRSM_DEFAULT_INSTANCE__'
    names = [variable['name'] for variable in dashboard['templating']['list']]
    assert names.index('default_instance') < names.index('pipe')
    assert "'Pipe(''' || connector_keys" in variables['pipe']['query']
    assert '${default_instance:sqlstring}' in variables['pipe']['query']
    assert "' / '" not in variables['pipe']['query']
    for panel in dashboard['panels']:
        for target in panel['targets']:
            sql = target['rawSql']
            assert not re.search(r'pipe_ref AS (metric|"Pipe")', sql), panel['title']
            if 'pipe_label' in sql:
                assert "'Pipe(''' || m.connector_keys" in sql and '${default_instance:sqlstring}' in sql


def test_native_stack_sync_preserves_undecodable_and_linked_dashboard_collisions(tmp_path):
    import os
    import subprocess
    import sys

    root = tmp_path / 'root'
    plugins = root / 'plugins'
    plugins.mkdir(parents=True)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH')}
    env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
               PYTHONPATH=str(RESOURCES.parents[4]), MRSM_NO_AUTO_INSTALL='1')
    result = subprocess.run([sys.executable, '-c', '''
import os
from pathlib import Path
from meerschaum import _core
from meerschaum.config import paths
directory = paths.GRAFANA_DASHBOARD_PATH.parent
directory.mkdir(parents=True, exist_ok=True)
collision = directory / 'meerschaum-performance.json'
collision.write_bytes(b'\\xffcustom')
linked = directory / 'meerschaum-pipes.json'
if os.name == 'posix':
    linked.unlink(missing_ok=True)
    target = Path.cwd() / 'custom-dashboard.json'
    target.write_text('{"uid":"mrsm-pipe-performance"}')
    linked.symlink_to(target)
_core.sync_files(['stack'])
assert collision.read_bytes() == b'\\xffcustom', 'An unreadable UID collision was overwritten'
if os.name == 'posix':
    assert linked.is_symlink(), 'A user-managed dashboard link was replaced'
assert (directory / 'meerschaum-pipelines.json').exists()
'''], cwd=root, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr[-4000:]
