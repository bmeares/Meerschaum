"""Built-in Stack dashboards use native metrics, existing provisioning and one data source."""

import json
from pathlib import Path
import re

import pytest

RESOURCES = Path(__file__).resolve().parents[1] / 'crates/meerschaum-core/src/config/grafana'
RETIRED = Path(__file__).resolve().parents[1] / 'crates/meerschaum-core/tests/fixtures/grafana_retired'
DASHBOARDS = sorted(RESOURCES.glob('*.json'))
SHARED_VARIABLES = (
    'datasource', 'default_instance', 'schema', 'metrics_table', 'runs_table', 'pipeline', 'pipe',
)
ROWS = {
    'mrsm-syncs': ['Overview', 'Pipes', 'Pipelines'],
    'mrsm-engine': ['Runs', 'Freshness', 'Dirty-range backlog', 'Nodes', 'Streams'],
}
ENGINE_TABLES = {
    'runs_table': 'runs', 'nodes_table': 'nodes', 'events_table': 'events', 'ledger_table': 'ledger',
    'cursors_table': 'cursors', 'status_table': 'sync_status',
}
WRITES = r'\b(?:INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|CALL|DO)\b'


def load(path):
    return json.loads(path.read_text())


def variables_of(dashboard):
    return {variable['name']: variable for variable in dashboard['templating']['list']}


def panels_of(dashboard):
    """Every panel, including those nested in collapsed rows."""
    for panel in dashboard['panels']:
        yield panel
        yield from panel.get('panels', [])


def queries_of(dashboard):
    for panel in panels_of(dashboard):
        for target in panel.get('targets', []):
            yield panel, target['rawSql']


def test_the_stack_ships_the_syncs_and_engine_dashboards():
    dashboards = {load(path)['uid']: load(path) for path in DASHBOARDS}
    assert set(dashboards) == {'mrsm-syncs', 'mrsm-engine'}
    # They live in the "Meerschaum Dashboards" folder: no "Meerschaum" prefix.
    assert dashboards['mrsm-syncs']['title'] == 'Syncs'
    assert dashboards['mrsm-engine']['title'] == 'Engine'
    assert {path.name for path in DASHBOARDS} == {'meerschaum-syncs.json', 'meerschaum-engine.json'}


@pytest.mark.parametrize('resource', DASHBOARDS, ids=lambda path: path.stem)
def test_dashboards_group_their_panels_in_rows(resource):
    dashboard = load(resource)
    rows = [panel for panel in dashboard['panels'] if panel['type'] == 'row']
    assert [row['title'] for row in rows] == ROWS[dashboard['uid']]
    assert dashboard['panels'][0]['type'] == 'row'
    for row in rows:
        assert row['gridPos']['w'] == 24 and row['gridPos']['h'] == 1
        if not row['collapsed']:
            assert row['panels'] == []
        else:
            assert row['panels'], row['title']
    assert not rows[0]['collapsed']
    ids = [panel['id'] for panel in panels_of(dashboard)]
    assert len(set(ids)) == len(ids)
    titles = [panel['title'] for panel in panels_of(dashboard) if panel['type'] != 'row']
    assert len(titles) == len(set(titles))
    for panel in panels_of(dashboard):
        assert all(panel['gridPos'][key] >= 0 for key in 'xywh')
        assert panel['gridPos']['x'] + panel['gridPos']['w'] <= 24


def test_syncs_keeps_every_measurement_of_the_retired_dashboards():
    syncs = load(RESOURCES / 'meerschaum-syncs.json')
    titles = {panel['title'] for panel in panels_of(syncs)}
    retired = set()
    for path in (RETIRED / 'pipe-labels').glob('*.json'):
        retired |= {panel['title'] for panel in load(path)['panels']}
    # Stats that repeated the overview's (same selection) were merged into it.
    merged = {'Syncs', 'Average duration', 'Attributed syncs', 'Retry attempts'}
    assert retired - merged <= titles
    assert {'Average sync duration', 'Retry attempts'} <= titles


@pytest.mark.parametrize('resource', DASHBOARDS, ids=lambda path: path.stem)
def test_dashboards_keep_filters_units_and_empty_history_safe(resource):
    dashboard = load(resource)
    variables = variables_of(dashboard)
    assert set(SHARED_VARIABLES) <= variables.keys()
    assert variables['datasource']['current']['text'] == 'Meerschaum Main'
    assert variables['schema']['current']['value'] == '_mrsm_internal'
    for name in ('schema', 'metrics_table'):
        assert 'information_schema.tables' in variables[name]['query']
        # Partitions of a native partitioned table are not instance schemas.
        assert 'pg_inherits' in variables[name]['query']
    assert '${metrics_table:sqlstring}' in variables['runs_table']['query']
    filters = ('pipeline', 'pipe', 'result') if dashboard['uid'] == 'mrsm-syncs' else ('pipeline', 'pipe')
    for name in filters:
        # A custom, quoted SQL All value remains valid even with no variable-query rows.
        assert variables[name]['includeAll']
        assert variables[name]['allValue'] == "'__all'"
    for panel in panels_of(dashboard):
        if panel['type'] != 'row':
            assert panel['datasource']['uid'] == '${datasource}'
    for panel, sql in queries_of(dashboard):
        assert '${pipe:sqlstring}' in sql and '${pipeline:sqlstring}' in sql, panel['title']
        assert '$__timeFilter(' in sql, panel['title']
        assert '${runs_table:raw}' in sql, panel['title']
        assert 'NULLIF' in sql or panel['type'] == 'table' or re.search(r'\b(count|sum|avg|max)\(', sql)
        assert not re.search(WRITES, sql, re.I), panel['title']
        assert not re.search(r'https?://|password|credentials|message', sql, re.I), panel['title']
    assert any(panel.get('fieldConfig', {}).get('defaults', {}).get('unit') == 's' for panel in panels_of(dashboard))


def test_syncs_reads_the_metrics_of_completed_syncs():
    dashboard = load(RESOURCES / 'meerschaum-syncs.json')
    for panel, sql in queries_of(dashboard):
        assert '${metrics_table:raw}' in sql and '$__timeFilter(m.ts)' in sql, panel['title']
        assert '${result:sqlstring}' in sql, panel['title']


def test_engine_reads_every_engine_table_of_the_selected_instance_schema():
    dashboard = load(RESOURCES / 'meerschaum-engine.json')
    variables = variables_of(dashboard)
    for name, table in ENGINE_TABLES.items():
        variable = variables[name]
        assert variable['hide'] == 2
        assert f"'^mrsm_engine_metrics', 'mrsm_engine_{table}'" in variable['query']
        assert variable['current']['value'] == f'mrsm_engine_{table}'
    sql = '\n'.join(sql for _, sql in queries_of(dashboard))
    for name in ENGINE_TABLES:
        assert f'${{{name}:raw}}' in sql, name
    # Engine state is read, never the raw messages, errors or tracebacks of a run.
    assert not re.search(r"payload\s*->>?\s*'(error|traceback|result)'", sql)
    assert not re.search(r'\bpayload\b(?!\s*->)', sql)
    for title in ('Pipe freshness', 'Lag behind parents', 'Backlog by child', 'Failed and retried nodes',
                  'Runs by outcome', 'Run duration', 'Data watermarks'):
        assert title in {panel['title'] for panel in panels_of(dashboard)}, title


@pytest.mark.parametrize('resource', DASHBOARDS, ids=lambda path: path.stem)
def test_dashboards_label_pipes_as_pipe_calls_without_the_default_instance(resource):
    """Pipes read `Pipe('ck', 'mk'[, 'lk'][, instance='ik'])`, as `str(pipe)` writes them."""
    dashboard = load(resource)
    variables = variables_of(dashboard)
    default = variables['default_instance']
    # Filled with the configured default instance when Meerschaum writes the dashboard.
    assert default['type'] == 'constant' and default['hide'] == 2
    assert default['query'] == '__MRSM_DEFAULT_INSTANCE__'
    names = [variable['name'] for variable in dashboard['templating']['list']]
    assert names.index('default_instance') < names.index('pipe')
    assert "'Pipe('''" in variables['pipe']['query']
    assert '${default_instance:sqlstring}' in variables['pipe']['query']
    assert "' / '" not in variables['pipe']['query']
    for panel, sql in queries_of(dashboard):
        assert not re.search(r'pipe_ref AS (metric|"Pipe"|"Parent"|"Child")', sql), panel['title']
        if 'pipe_label' in sql and dashboard['uid'] == 'mrsm-syncs':
            assert "'Pipe(''' || m.connector_keys" in sql and '${default_instance:sqlstring}' in sql
        if "'Pipe('''" in sql:
            assert '${default_instance:sqlstring}' in sql, panel['title']
            assert ", instance=''' ||" in sql, panel['title']
    if dashboard['uid'] == 'mrsm-engine':
        # Labels from a `pipe_ref` keep a location key that contains `|`.
        assert "'^([^|]*\\|){3}'" in variables['pipe']['query']


def test_native_stack_sync_preserves_collisions_and_removes_only_unmodified_retired_dashboards(tmp_path):
    import os
    import subprocess
    import sys

    root = tmp_path / 'root'
    plugins = root / 'plugins'
    plugins.mkdir(parents=True)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH')}
    env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
               PYTHONPATH=str(RESOURCES.parents[4]), MRSM_NO_AUTO_INSTALL='1',
               RETIRED_FIXTURES=str(RETIRED))
    result = subprocess.run([sys.executable, '-c', '''
import json, os
from pathlib import Path
from meerschaum import _core
from meerschaum.config import paths
retired = Path(os.environ['RETIRED_FIXTURES'])
directory = paths.GRAFANA_DASHBOARD_PATH.parent
directory.mkdir(parents=True, exist_ok=True)
collision = directory / 'meerschaum-syncs.json'
collision.write_bytes(b'\\xffcustom')
linked = directory / 'meerschaum-engine.json'
if os.name == 'posix':
    target = Path.cwd() / 'custom-dashboard.json'
    target.write_text('{"uid":"mrsm-engine"}')
    linked.symlink_to(target)
# An unmodified retired built-in (as 5.0.0rc16 to rc34 wrote it) and a customized one.
(directory / 'meerschaum-pipes.json').write_text((retired / 'rc16-rc34' / 'meerschaum-pipes.json').read_text())
customized = json.loads((retired / 'rc16-rc34' / 'meerschaum-pipelines.json').read_text())
customized['title'] = 'My Pipelines'
(directory / 'meerschaum-pipelines.json').write_text(json.dumps(customized))
_core.sync_files(['stack'])
assert collision.read_bytes() == b'\\xffcustom', 'An unreadable UID collision was overwritten'
if os.name == 'posix':
    assert linked.is_symlink(), 'A user-managed dashboard link was replaced'
else:
    assert json.loads(linked.read_text())['uid'] == 'mrsm-engine'
assert not (directory / 'meerschaum-pipes.json').exists(), 'A managed retired dashboard was kept'
assert json.loads((directory / 'meerschaum-pipelines.json').read_text())['title'] == 'My Pipelines'
'''], cwd=root, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr[-4000:]
