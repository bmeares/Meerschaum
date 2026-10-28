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
