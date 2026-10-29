#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Pipes pages (rc15 Q3, Q4, Q27, B5): the table's rows, filters, search, sort, pages
and slow columns; a pipe's page (routes, tabs, actions only for sessions that may run them,
confirmations, Copy); New pipe (flavor-correct preview limits, the datetime guess, parameters,
registration rules). In process, on the sandbox's SQLite instance; no server.
"""

import json

from tests.api_host._helpers import AS_VIEWER, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


SEED = '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
instance = get_api_connector()
def make(ck, mk, lk=None, **params):
    pipe = mrsm.Pipe(ck, mk, lk, instance=instance, parameters=params)
    if pipe.get_id() is None:
        assert pipe.register()[0]
    return mrsm.Pipe(ck, mk, lk, instance=instance)
'''


def test_routes_build_each_pipes_page_with_its_keys(sandbox):
    """`/dash/pipes`, `/dash/pipes/new` and a pipe's page (each encoded key round-trips)."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + '''
import json
from urllib.parse import urlsplit
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.pages import pipes as pages
from meerschaum.api.dash import engine, pipes_access

def ids(layout):
    return {getattr(node, 'id', None) for node in pages._walk(layout)}

def store(layout, store_id):
    return next(node for node in pages._walk(layout) if getattr(node, 'id', None) == store_id).data

out = {}
base = 'http://localhost'
out['table'] = sorted({'pipes-table-markdown', 'pipes-search', 'pipes-connector-keys-dropdown',
    'pipes-tags-dropdown', 'pipes-status-select', 'pipes-pagination'} - ids(pages.layout('/dash/pipes', base + '/dash/pipes')))
NEW_IDS = {'new-pipe-connector', 'new-pipe-metric', 'new-pipe-location-key', 'new-pipe-instance',
    'new-pipe-query', 'new-pipe-preview', 'new-pipe-datetime', 'new-pipe-id', 'new-pipe-team',
    'new-pipe-visibility', 'new-pipe-register', 'new-pipe-register-sync'}
### Without a session the form has no buttons (a button the viewer may not use is not shown).
out['new_anonymous'] = sorted(NEW_IDS - ids(pages.layout('/dash/pipes/new', base + '/dash/pipes/new')))
metas = []
for metric, location in [('counts', 'bytes'), ('a/b + c', '%3A'), ('café', None)]:
    ref = '|'.join(map(str, ('sql:apihosttest', 'sql:main', metric, location)))
    url = urlsplit(engine.pipe_href(ref))
    layout = pages.layout(url.path, base + url.path + '?' + url.query)
    metas.append(store(layout, 'pipe-detail-meta'))
out['metas'] = metas
detail = pages.layout('/dash/pipes/sql:main/counts', base + '/dash/pipes/sql:main/counts?tab=runs')
out['tab'] = next(node for node in pages._walk(detail) if getattr(node, 'id', None) == 'pipe-detail-tabs').active_tab
### Without a session (or one that may not run actions), the page has no actions.
out['anonymous_actions'] = sorted(ids(detail) & {'pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-menu'})
as_viewer()
out['new'] = sorted(NEW_IDS - ids(pages.layout('/dash/pipes/new', base + '/dash/pipes/new')))
detail = pages.layout('/dash/pipes/sql:main/counts', base + '/dash/pipes/sql:main/counts')
out['admin_actions'] = sorted(ids(detail) & {'pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-menu',
    'pipe-manage-clear', 'pipe-manage-drop', 'pipe-manage-delete', 'pipe-manage-copy'})
### No subtitles: no muted helper text on the pages.
out['captions'] = 'text-muted' in str(detail) or 'text-muted' in str(pages.layout('/dash/pipes', base + '/dash/pipes'))
print('JSON:' + json.dumps(out))
'''))
    assert out['table'] == [] and out['new'] == []
    assert out['new_anonymous'] == ['new-pipe-register', 'new-pipe-register-sync']
    assert out['metas'] == [
        {'connector_keys': 'sql:main', 'metric_key': 'counts', 'location_key': 'bytes', 'instance_keys': 'sql:apihosttest'},
        {'connector_keys': 'sql:main', 'metric_key': 'a/b + c', 'location_key': '%3A', 'instance_keys': 'sql:apihosttest'},
        {'connector_keys': 'sql:main', 'metric_key': 'café', 'location_key': None, 'instance_keys': 'sql:apihosttest'},
    ]
    assert out['tab'] == 'runs'
    assert out['anonymous_actions'] == []
    assert out['admin_actions'] == [
        'pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-clear',
        'pipe-manage-copy', 'pipe-manage-delete', 'pipe-manage-drop', 'pipe-manage-menu',
    ]
    assert out['captions'] is False


def test_table_rows_filters_search_sort_pages_and_slow_columns(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash import pipes_table as pt
import pandas as pd
make('tbl', 'alpha', columns={'datetime': 'ts'}, tags=['prod', 'gvl'])
beta = make('tbl', 'beta', 'north', columns={'datetime': 'ts'}, tags=['prod'])
make('tbl', 'gamma', columns={'datetime': 'ts'}, tags=['dev'], target='custom_gamma')
make('mrsm', 'pipeline', 'internal')
assert beta.sync(pd.DataFrame({'ts': pd.to_datetime(['2026-01-01', '2026-01-02', '2026-01-03'])}))[0]

out = {}
rows = pt.list_rows(instance, connector_keys=['tbl'])
out['labels'] = [row['label'] for row in rows]
out['internal_hidden'] = all(row['ck'] != 'mrsm' for row in pt.list_rows(instance))
out['internal_asked'] = any(row['ck'] == 'mrsm' for row in pt.list_rows(instance, connector_keys=['mrsm']))
out['tags_filter'] = [row['mk'] for row in pt.list_rows(instance, connector_keys=['tbl'], tags=['prod'])]
out['search_target'] = [row['mk'] for row in pt.search_rows(rows, 'custom_g')]
out['search_words'] = [row['mk'] for row in pt.search_rows(rows, 'TBL north')]
stats = pt.compute_stats(str(instance), rows)
beta_ref = pt.make_ref(str(instance), 'tbl', 'beta', 'north')
out['beta_stats'] = {key: stats[beta_ref][key] for key in ('rows', 'estimated', 'sync_time')}
out['never'] = pt.status_kind(stats[pt.make_ref(str(instance), 'tbl', 'alpha', None)])
out['by_rows_desc'] = [row['mk'] for row in pt.sort_rows(rows, stats, 'rows', False)]
out['by_pipe_desc'] = [row['mk'] for row in pt.sort_rows(rows, stats, 'pipe', False)]
### Statuses (C2) by `pipe_ref`: failing, then never synced, then synced.
fake = {
    beta_ref: {'success': True, 'last_success': '2026-01-03T00:00:00+00:00', 'consecutive_failures': 0},
    pt.make_ref(str(instance), 'tbl', 'gamma', None): {
        'success': False, 'consecutive_failures': 3, 'message': 'boom', 'last_failure': '2026-01-03T00:00:00+00:00'},
}
for ref, status in fake.items():
    stats[ref] = dict(stats[ref], status=status)
out['by_status'] = [row['mk'] for row in pt.sort_rows(rows, stats, 'status', True)]
out['failing'] = [row['mk'] for row in pt.filter_status(rows, stats, 'failing')]
out['cells'] = [pt.status_cell(stats[ref])[0] for ref in sorted(fake)]
out['failing_title'] = pt.status_cell(stats[pt.make_ref(str(instance), 'tbl', 'gamma', None)])[1]
### Pages of the table.
many = [dict(rows[0], ref=f'x|tbl|m{i:03d}|None', mk=f'm{i:03d}', search=f'm{i:03d}') for i in range(250)]
page, page_number, num_pages, matching = pt.visible_rows(many, {}, page=3)
out['pages'] = [len(page), page_number, num_pages, matching, page[0]['mk']]
out['summary'] = pt.summary_text(matching, 250, page_number, num_pages)
### Only the page on screen needs its statistics, unless a slow column sorts.
out['needed_page'] = len(pt.rows_needing_stats(many, {}, page=1))
out['needed_sort'] = len(pt.rows_needing_stats(many, {}, column='rows'))
out['needed_status'] = len(pt.rows_needing_stats(many, {}, status='failing'))
out['needed_any'] = len(pt.rows_needing_stats(many, {}, status='any'))
### Rows: exact with separators, estimates abbreviated.
out['rows_text'] = [pt.rows_text({'rows': 1234567, 'estimated': False}), pt.rows_text({'rows': 1234567, 'estimated': True}),
                    pt.rows_text({'rows': 350000, 'estimated': True}), pt.rows_text(None), pt.rows_text({'rows': None})]
### An estimate is only ever flagged on a flavor with estimates, above the exact threshold.
class Fake:
    def __init__(self, rows, flavor):
        self._rows, self.instance_connector = rows, type('C', (), {'flavor': flavor})()
    def get_rowcount(self, estimate=False):
        return self._rows
out['estimated'] = [pt.get_rowcount(Fake(5_000_000, 'postgresql')), pt.get_rowcount(Fake(5_000_000, 'sqlite')),
                    pt.get_rowcount(Fake(99_999, 'postgresql'))]
class Old:
    instance_connector = type('C', (), {'flavor': 'postgresql'})()
    def get_rowcount(self):
        return 5_000_000
out['no_estimate_kwarg'] = pt.get_rowcount(Old())
### Filters round-trip through the URL.
search = pt.search_from_filters(['sql:main'], ['a b'], ['None'], ['x', 'y'], 'failing', instance='sql:other')
out['search'] = search
out['filters'] = pt.filters_from_search(search)
out['any'] = pt.filters_from_search('?status=any')['status']
print('JSON:' + json.dumps(out))
'''))
    assert out['labels'] == ["Pipe('tbl', 'alpha')", "Pipe('tbl', 'beta', 'north')", "Pipe('tbl', 'gamma')"]
    assert out['internal_hidden'] is True and out['internal_asked'] is True
    assert out['tags_filter'] == ['alpha', 'beta']
    assert out['search_target'] == ['gamma'] and out['search_words'] == ['beta']
    assert out['beta_stats']['rows'] == 3 and out['beta_stats']['estimated'] is False
    assert out['beta_stats']['sync_time'].startswith('2026-01-03')
    assert out['never'] == 'never'
    assert out['by_rows_desc'][0] == 'beta'
    assert out['by_pipe_desc'] == ['gamma', 'beta', 'alpha']
    assert out['by_status'] == ['gamma', 'alpha', 'beta']
    assert out['failing'] == ['gamma']
    assert out['cells'][0].startswith('Synced') and out['cells'][1] == 'Failed 3 times'
    assert 'boom' in out['failing_title']
    assert out['pages'] == [50, 3, 3, 250, 'm200']
    assert out['summary'] == '201–250 of 250 pipes'
    assert out['needed_page'] == 100 and out['needed_sort'] == 250 and out['needed_status'] == 250
    assert out['needed_any'] == 100
    assert out['rows_text'] == ['1,234,567', '~1.2M', '~350K', '…', '—']
    assert out['estimated'] == [[5_000_000, True], [5_000_000, False], [99_999, False]]
    assert out['no_estimate_kwarg'] == [5_000_000, False]
    assert out['search'] == '?connector_keys=sql%3Amain&metric_keys=a+b&location_keys=None&tags=x,y&status=failing&instance=sql%3Aother'
    assert out['filters']['metric_keys'] == ['a b'] and out['filters']['tags'] == ['x', 'y']
    assert out['filters']['status'] == 'failing' and out['filters']['instance'] == 'sql:other'
    assert out['any'] == ''


def test_jobs_index_matches_sync_jobs_and_pipeline_schedules(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash import pipes_table as pt
import meerschaum.jobs as jobs_module
from meerschaum.core.Pipeline._saved import save_pipeline
make('job', 'a', tags=['prod'])
make('job', 'b', 'loc', tags=['dev'])
make('job', 'c')
save_pipeline('nightly', ['|'.join((str(instance), 'job', 'c', 'None'))], str(instance))
class Job:
    def __init__(self, name, sysargs, status='running', env=None):
        self.name, self.sysargs, self.status, self.env = name, sysargs, status, env or {}
        self.daemon = type('D', (), {'properties': {}})()
    def get_daemon_properties(self):
        return {}
from meerschaum._internal.arguments._parse_arguments import get_pipeline_sysargs
### An action chain (`a + b : --loop`) is stored as `chain --loop -P {...}`.
chain = get_pipeline_sysargs(
    ['show', 'version', '+', 'verify', 'pipes', '-c', 'job', '-m', 'b', '-l', 'loc', '-i', str(instance)],
    ['--loop', '--min-seconds', '30'],
)
compose_env = {'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': str(instance)}})}
fakes = {
    'every-a': Job('every-a', ['sync', 'pipes', '-c', 'job', '-m', 'a', '-i', str(instance), '-s', 'every 5 minutes']),
    'prod': Job('prod', ['sync', 'pipes', '-t', 'prod', '-i', str(instance), '--loop', '--min-seconds', '60'], 'paused'),
    'not-b': Job('not-b', ['sync', 'pipes', '-c', 'job', '-m', '_b', '-i', str(instance)]),
    'other-instance': Job('other-instance', ['sync', 'pipes', '-c', 'job', '-i', 'sql:elsewhere']),
    'pipeline': Job('pipeline', ['sync', 'pipelines', 'nightly', '-i', str(instance), '-s', 'daily']),
    'execute-123': Job('execute-123', ['sync', 'pipes', '-c', 'job', '-i', str(instance)]),
    'show': Job('show', ['show', 'pipes']),
    'verify-c': Job('verify-c', ['verify', 'pipes', '-c', 'job', '-m', 'c', '-i', str(instance), '-s', 'daily']),
    'chained': Job('chained', chain),
    ### A compose project's job names no `-i`: its instance is in its environment.
    'compose-dev': Job('compose-dev', ['sync', 'pipes', '-t', 'dev', '--loop'], env=compose_env),
    'compose-elsewhere': Job('compose-elsewhere', ['sync', 'pipes', '-c', 'job', '--loop'],
                             env={'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': 'sql:elsewhere'}})}),
}
jobs_module.get_jobs = lambda *args, **kwargs: fakes
rows = pt.list_rows(instance, connector_keys=['job'])
index = pt.get_jobs_index(str(instance), rows)
out = {row['mk']: sorted(job['name'] for job in index.get(row['ref'], [])) for row in rows}
summaries = {job['name']: job for jobs in index.values() for job in jobs}
out['schedules'] = {name: summaries[name]['schedule'] for name in sorted(summaries)}
out['via'] = summaries['pipeline']['via']
out['badges'] = [badge.children for badge in pt.job_badges(index[rows[0]['ref']])]
out['chain_command'] = summaries['chained']['command']
print('JSON:' + json.dumps(out))
'''))
    assert out['a'] == ['every-a', 'not-b', 'prod']
    assert out['b'] == ['chained', 'compose-dev']
    assert out['c'] == ['not-b', 'pipeline', 'verify-c']
    assert out['schedules'] == {
        'chained': 'every 30 s', 'compose-dev': 'continuously', 'every-a': 'every 5 minutes',
        'not-b': '', 'pipeline': 'daily', 'prod': 'every 60 s', 'verify-c': 'daily',
    }
    assert out['via'] == 'nightly'
    assert sorted(out['badges']) == ['every-a', 'not-b', 'prod']
    assert out['chain_command'].startswith('show version + verify pipes -c job -m b -l loc')
    assert out['chain_command'].endswith(': --loop --min-seconds 30')


def test_pipe_tabs_edit_mode_and_actions(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, extra_env={
        'MRSM_SQL_ELSEWHERE': f"sqlite:///{root / 'elsewhere.db'}",
    }, code=SEED + '''
import pandas as pd
from meerschaum.api.dash import pipe_detail as pd_
from meerschaum.api.dash.pages import pipes as pages
pipe = make('det', 'm', columns={'datetime': 'ts', 'id': 'station'}, dtypes={'value': 'float64'},
            tags=['t1'], fetch={'backtrack_minutes': 5})
assert pipe.sync(pd.DataFrame({'ts': pd.to_datetime(['2026-01-01', '2026-01-02']), 'station': [1, 2],
                               'value': [1.0, 2.0]}))[0]
sql = make('sql:apihosttest', 'from_sql', sql="SELECT * FROM {{ Pipe('det', 'm') }}")

def ids(component):
    return {node.id if isinstance(node.id, str) else node.id.get('type')
            for node in pages._walk(component) if getattr(node, 'id', None)}

out = {}
for tab in pd_.TAB_IDS:
    out[tab] = sorted(ids(pd_.build_tab(tab, pipe, allowed=True, edit=False)))
out['edit_parameters'] = sorted(ids(pd_.build_tab('parameters', pipe, allowed=True, edit=True)))
out['edit_definition'] = sorted(ids(pd_.build_tab('definition', pipe, allowed=True, edit=True)))
out['edit_data'] = sorted(ids(pd_.build_tab('data', pipe, allowed=True, edit=True)))
out['readonly_data'] = sorted(ids(pd_.build_tab('data', pipe, allowed=False, edit=True)))
out['readonly_parameters'] = sorted(ids(pd_.build_tab('parameters', pipe, allowed=False, edit=True)))
overview = str(pd_.build_tab('overview', pipe, allowed=True))
### Without a table yet, each declared column is listed once.
empty = make('det', 'no_table', columns={'datetime': 'dt'}, dtypes={'dt': 'datetime'})
out['empty_columns'] = [len(table.children[-1].children) for table in pages._walk(pd_.columns_table(empty))
                        if getattr(table, 'className', '') == 'table table-sm pipes-columns-table']
out['overview'] = [text in overview for text in ('station', 'float64', 'declared', 'ts', 'Count', "'t1'")]
definition = pd_.build_tab('definition', sql, allowed=True, edit=True)
editor = next(node for node in pages._walk(definition) if isinstance(getattr(node, 'id', None), dict)
              and node.id.get('type') == 'sql-editor')
out['sql_as_written'] = editor.value
out['fetch_text'] = pd_.fetch_text(pipe)
unregistered = __import__('meerschaum').Pipe('nope', 'nope', instance=instance)
out['unregistered'] = 'not registered' in str(pd_.build_tab('overview', unregistered))
meta = pd_.meta_of(pipe)
out['sync'] = pd_.action_sysargs(meta, 'sync')[0]
out['clear'] = pd_.action_sysargs(meta, 'clear', begin='2026-01-01', end=' ')[0]
out['delete'] = pd_.action_sysargs(meta, 'delete')[0]
out['bad'] = pd_.action_sysargs(meta, 'python')
### Copy: new keys register a copy with the same parameters (not the explicit target).
make('det', 'targeted', target='shared_table', columns={'datetime': 'ts'})
targeted = __import__('meerschaum').Pipe('det', 'targeted', instance=instance)
destination, message = pd_.copy_destination(targeted, 'det', 'targeted2', '', str(instance))
(success, message), copy = pd_.copy_pipe(targeted, destination)
copied = __import__('meerschaum').Pipe('det', 'targeted2', instance=instance).get_parameters(apply_symlinks=False, refresh=True)
out['copy'] = [success, destination['same_keys'], copied.get('target'), copied.get('columns')]
out['copy_same'] = pd_.copy_destination(targeted, 'det', 'targeted', None, str(instance))
out['copy_exists'] = pd_.copy_destination(targeted, 'det', 'targeted2', None, str(instance))[1]
out['copy_other'] = pd_.copy_destination(targeted, 'det', 'targeted', 'None', 'sql:elsewhere')[0]['same_keys']
### An instance that cannot be read is not taken for one without the pipe.
out['copy_unreadable'] = pd_.copy_destination(targeted, 'det', 'targeted', None, 'sql:nowhere')
out['copy_sysargs'] = pd_.copy_sysargs(targeted, 'sql:elsewhere', copy_rows=False)
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['overview'] == [True, True, True, True, True, True]
    assert out['empty_columns'] == [1]
    assert 'pipe-download-csv-button' in out['data'] and 'query-data-button' in out['data']
    assert 'update-sync-button' not in out['data'] and 'update-sync-button' in out['edit_data']
    assert 'query-data-button' not in out['readonly_data']
    assert 'pipe-download-csv-button' not in out['readonly_data']
    assert 'update-parameters-button' not in out['parameters']
    assert 'update-parameters-button' in out['edit_parameters']
    assert 'resolve-symlinks-parameters-switch' in out['parameters']
    assert 'resolve-symlinks-parameters-switch' not in out['edit_parameters']
    assert 'update-parameters-button' not in out['readonly_parameters']
    assert 'pipe-fetch-update' in out['edit_definition'] and 'pipe-fetch-update' not in out['definition']
    assert out['sql_as_written'] == "SELECT * FROM {{ Pipe('det', 'm') }}"
    assert 'backtrack_minutes: 5' in out['fetch_text']
    assert 'pipe-relation-add-dropdown' not in out['lineage']
    assert out['unregistered'] is True
    assert out['sync'] == ['sync', 'pipes', '-c', 'det', '-m', 'm', '-l', 'None', '-i', 'sql:apihosttest']
    assert out['clear'] == ['clear', 'pipes', '-c', 'det', '-m', 'm', '-l', 'None', '-i', 'sql:apihosttest',
                            '--yes', '--begin', '2026-01-01']
    assert out['delete'][-1] == '--yes'
    assert out['bad'][0] is None
    assert out['copy'] == [True, False, None, {'datetime': 'ts'}]
    assert out['copy_same'][0] is None and 'Change the keys' in out['copy_same'][1]
    assert 'already registered' in out['copy_exists']
    assert out['copy_other'] is True
    assert out['copy_unreadable'][0] is None
    assert out['copy_unreadable'][1].startswith('Could not read sql:nowhere: ')
    assert out['copy_sysargs'] == ['copy', 'pipes', 'sql:elsewhere', '-c', 'det', '-m', 'targeted', '-l', 'None',
                                   '-i', 'sql:apihosttest', '--sync-data', 'nothing', '--yes']


def test_new_pipe_preview_limits_guess_and_registration(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import pandas as pd
from meerschaum.api.dash import new_pipe as np_
from meerschaum.api.dash import pipes_access
src = make('np', 'source', columns={'datetime': 'ts', 'id': 'station'})
assert src.sync(pd.DataFrame({
    'ts': pd.date_range('2026-01-01', periods=30, freq='h'), 'station': [i % 3 for i in range(30)],
    'value': [float(i) for i in range(30)],
}))[0]
out = {}
query = "SELECT * FROM {{ Pipe('np', 'source') }} ORDER BY ts DESC;"
df, message = np_.preview_query('sql:apihosttest', query, str(instance))
out['preview'] = [message, len(df), list(df.columns)]
out['guess'] = np_.guess_datetime_column(df)
out['text_guess'] = np_.guess_datetime_column(pd.DataFrame({'id': [1], 'created_at': ['2026-01-01 00:00:00'],
                                                            'note': ['x']}))
out['no_guess'] = np_.guess_datetime_column(pd.DataFrame({'id': [1], 'time_zone': ['UTC']}))
out['mixed_guess'] = np_.guess_datetime_column(pd.DataFrame({'dt': ['2026-01-01 00:00:00', '2026-02-01 00:00:00+00:00']}))
out['bad_query'] = np_.preview_query('sql:apihosttest', 'SELECT * FROM missing_table', str(instance))[1] != ''
out['not_sql'] = np_.preview_query('plugin:noaa', 'SELECT 1', str(instance))[1]
out['limits'] = {flavor: np_.limit_query('WITH a AS (SELECT 1 AS x) SELECT DISTINCT x FROM a ORDER BY x;', flavor)
                 for flavor in ('postgresql', 'mssql', 'oracle', 'sqlite')}
out['mssql_top'] = np_.limit_query('SELECT TOP 5 * FROM t', 'mssql')
out['default_connector'] = [np_.default_connector(['sql:a', 'sql:main'], 'sql:main'),
                            np_.default_connector(['api:x', 'sql:b', 'sql:c'], 'api:x'),
                            np_.default_connector(['plugin:noaa'], 'sql:main')]
out['parameters'] = np_.build_parameters('sql:main', 'SELECT 1;', 'ts', '', '{"tags": ["x"], "columns": {"value": "v"}}')
out['parameters_yaml'] = np_.build_parameters('plugin:noaa', 'ignored', None, 'station', 'fetch:\\n  stations: [KGMU]\\n')
out['parameters_bad'] = np_.build_parameters('plugin:noaa', None, None, None, '[1, 2]')
out['keys'] = [np_.validate_keys('sql:main', 'weather', 'None'), np_.validate_keys('', ' ', None)[1],
               np_.validate_keys('sql:main', 'a|b', None)[1]]
(success, message), pipe = np_.register_pipe('sql:apihosttest', 'from_form', None, str(instance),
                                             out['parameters'][0])
registered = __import__('meerschaum').Pipe('sql:apihosttest', 'from_form', instance=instance)
out['registered'] = [success, registered.get_parameters(apply_symlinks=False, refresh=True)]
out['again'] = np_.register_pipe('sql:apihosttest', 'from_form', None, str(instance), {})[0]
### Registration follows `api:permissions:registration:pipes`.
import meerschaum.config as config
original = config.get_config
config.get_config = lambda *keys, **kwargs: (False if keys == ('api', 'permissions', 'registration', 'pipes')
                                             else original(*keys, **kwargs))
pipes_access.get_config = config.get_config
import meerschaum.api.dash.pipes_access as access_module
out['refused'] = access_module.can_register({'session-id': 'x'})
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['preview'] == ['', 10, ['ts', 'station', 'value']]
    assert out['guess'] == 'ts'
    assert out['text_guess'] == 'created_at'
    assert out['no_guess'] is None
    assert out['mixed_guess'] == 'dt'
    assert out['bad_query'] is True
    assert 'sql:' in out['not_sql']
    assert out['limits']['postgresql'].endswith('AS mrsm_preview\nLIMIT 10')
    assert out['limits']['sqlite'].startswith('SELECT *\nFROM (\nWITH a AS')
    assert out['limits']['mssql'] == 'WITH a AS (SELECT 1 AS x) SELECT DISTINCT TOP 10 x FROM a ORDER BY x'
    assert out['limits']['oracle'].endswith('WHERE ROWNUM <= 10')
    assert out['mssql_top'] == 'SELECT TOP 5 * FROM t'
    assert out['default_connector'] == ['sql:main', 'sql:b', '']
    assert out['parameters'] == [
        {'tags': ['x'], 'columns': {'value': 'v', 'datetime': 'ts'}, 'fetch': {'definition': 'SELECT 1'}}, '',
    ]
    assert out['parameters_yaml'] == [{'fetch': {'stations': ['KGMU']}, 'columns': {'id': 'station'}}, '']
    assert out['parameters_bad'][0] is None and 'dictionary' in out['parameters_bad'][1]
    assert out['keys'][0] == [['sql:main', 'weather', None], {}]
    assert set(out['keys'][1]) == {'connector', 'metric'}
    assert 'metric' in out['keys'][2]
    assert out['registered'][0] is True
    assert out['registered'][1]['fetch'] == {'definition': 'SELECT 1'}
    assert out['registered'][1]['columns']['datetime'] == 'ts'
    assert out['again'][0] is False and 'already registered' in out['again'][1]
    assert out['refused'][0] is False and 'registration' in out['refused'][1]


def test_callbacks_refuse_sessions_that_may_not_act_and_need_the_metric_typed(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + SEED + '''
from unittest import mock
from dash._callback_context import context_value
from dash._utils import AttributeDict
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipes as callbacks
from meerschaum.api.dash import pipe_detail, pipes_access
pipe = make('cb', 'metric')
meta = pipe_detail.meta_of(pipe)
ref = pipe_detail.pipe_ref(pipe)

def trigger(prop_id, value=1):
    context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': prop_id, 'value': value}]}))

started = []
def fake_run(sysargs, username, metadata=None):
    started.append(sysargs)
    return {'success': True, 'msg': '', 'job': 'execute-x', 'stream': '/logs/execute-x/stream'}

out = {}
with mock.patch('meerschaum.api.dash.execute.run_action', fake_run):
    ### A Viewer may not sync (`pipes:write`): the callback refuses whatever the browser sends.
    as_viewer('cb-viewer', admin=False, role='viewer')
    trigger('pipe-action-sync.n_clicks')
    store, alert, _ = callbacks.run_pipe_action(1, None, None, None, None, None, None, meta, {'session-id': 'x'})
    out['refused'] = [store is callbacks.dash.no_update, 'may not sync' in str(alert)]
    as_viewer('cb-admin')
    if True:
        trigger('pipe-action-sync.n_clicks')
        store, alert, _ = callbacks.run_pipe_action(1, None, None, None, None, None, None, meta, {'session-id': 'x'})
        out['sync'] = [store['job'], store['stream'], store['action'], store['command']]
        trigger('pipe-confirm-button.n_clicks')
        try:
            callbacks.run_pipe_action(None, None, 1, {'action': 'drop', 'ref': ref}, 'metri', None, None, meta, {'session-id': 'x'})
            out['mistyped'] = 'ran'
        except PreventUpdate:
            out['mistyped'] = 'prevented'
        store, alert, is_open = callbacks.run_pipe_action(None, None, 1, {'action': 'drop', 'ref': ref}, ' metric ', None, None, meta, {'session-id': 'x'})
        out['drop'] = [store['action'], is_open, started[-1]]
        trigger('pipe-manage-delete.n_clicks')
        opened = callbacks.open_pipe_confirm(None, None, 1, None, meta, {'session-id': 'x'})
        out['confirm'] = [opened[0], opened[1], opened[2], opened[4], opened[6]]
        trigger('pipe-manage-clear.n_clicks')
        out['clear_bounds_shown'] = callbacks.open_pipe_confirm(1, None, None, None, meta, {'session-id': 'x'})[4] is False
### Sorting: a header sorts by its column, again the other way.
trigger('pipes-sort-click.data', {'column': 'rows', 'at': 1})
out['sort_rows'] = callbacks.sort_pipes({'column': 'rows', 'at': 1}, 'pipe:asc', {'column': 'pipe', 'ascending': True})
out['sort_rows_again'] = callbacks.sort_pipes({'column': 'rows', 'at': 2}, 'rows:desc', {'column': 'rows', 'ascending': False})
try:
    callbacks.sort_pipes({'column': 'nope', 'at': 3}, 'rows:desc', {'column': 'rows', 'ascending': False})
    out['sort_unknown'] = 'sorted'
except PreventUpdate:
    out['sort_unknown'] = 'prevented'
trigger('pipes-sort-select.value', 'sync_time:asc')
out['sort_select'] = callbacks.sort_pipes(None, 'sync_time:asc', {'column': 'pipe', 'ascending': True})[0]
### Filters become the URL; an unchanged URL does not update.
trigger('pipes-tags-dropdown.value', ['a'])
out['search'] = callbacks.update_location_on_pipes_filter_change(['sql:main'], [], [], ['a'], 'any', None, None, '', {'instance': 'x', 'global': 'x'})
try:
    callbacks.update_location_on_pipes_filter_change(['sql:main'], [], [], ['a'], 'any', None, None, out['search'], {'instance': 'x', 'global': 'x'})
    out['unchanged'] = 'updated'
except PreventUpdate:
    out['unchanged'] = 'prevented'
out['deep_link'] = callbacks.update_location_on_pipes_filter_change([], [], [], [], 'failing', None, None, '', {'instance': 'sql:b', 'global': 'sql:a'})
### The owner filter is in the URL too.
out['scope_link'] = callbacks.update_location_on_pipes_filter_change([], [], [], [], 'any', None, 'mine', '', {'instance': 'x', 'global': 'x'})
### Preview runs the query with the connector's credentials: only for those who may register pipes.
previewed = []
with mock.patch.object(pipes_access, 'can_register', return_value=(False, 'Registering is off here.')), \
        mock.patch('meerschaum.api.dash.new_pipe.preview_query', lambda *args, **kwargs: previewed.append(args) or (None, '')):
    trigger('new-pipe-preview.n_clicks')
    refused = callbacks.preview_new_pipe(1, 'sql:apihosttest', 'SELECT 1', str(instance), None, {'session-id': 'x'})
    out['preview_refused'] = ['Registering is off here.' in str(refused[0]), previewed == []]
print('JSON:' + json.dumps(out))
'''))
    assert out['refused'] == [True, True]
    assert out['sync'][:3] == ['execute-x', '/logs/execute-x/stream', 'sync']
    assert out['sync'][3].startswith('sync pipes -c cb -m metric')
    assert out['mistyped'] == 'prevented'
    assert out['drop'][0] == 'drop' and out['drop'][1] is False and out['drop'][2][-1] == '--yes'
    assert out['confirm'][0] is True and out['confirm'][1]['action'] == 'delete'
    assert out['confirm'][2] == 'Delete pipe' and out['confirm'][3] is True and out['confirm'][4] == 'Delete'
    assert out['clear_bounds_shown'] is True
    assert out['sort_rows'] == [{'column': 'rows', 'ascending': False}, 'rows:desc']
    assert out['sort_rows_again'] == [{'column': 'rows', 'ascending': True}, 'rows:asc']
    assert out['sort_unknown'] == 'prevented'
    assert out['sort_select'] == {'column': 'sync_time', 'ascending': True}
    assert out['search'] == '?connector_keys=sql%3Amain&tags=a'
    assert out['unchanged'] == 'prevented'
    assert out['deep_link'] == '?status=failing&instance=sql%3Ab'
    assert out['scope_link'] == '?scope=mine'
    assert out['preview_refused'] == [True, True]


def test_secure_mode_leaves_actions_and_registration_to_administrators(sandbox):
    """
    `start api --secure` reaches the host as `actions_non_admin: false` (the server's own value):
    only administrators run actions, edit or register pipes in the console; everyone signed in reads.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import os, json
os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({'mrsm_instance': 'sql:apihosttest', 'actions_non_admin': False})
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.core import User
from meerschaum.api.dash import sessions, pipes_access
conn = get_api_connector()
assert conn.register_user(User('secadmin', 'correct-horse-9', instance=conn, type='admin'))[0]
assert conn.register_user(User('secuser', 'correct-horse-9', instance=conn))[0]
sessions.set_session('00000000-0000-4000-8000-0000000b5a01', {'username': 'secadmin'})
sessions.set_session('00000000-0000-4000-8000-0000000b5a02', {'username': 'secuser'})
admin = {'session-id': '00000000-0000-4000-8000-0000000b5a01'}
user = {'session-id': '00000000-0000-4000-8000-0000000b5a02'}
print('JSON:' + json.dumps({
    'act': [pipes_access.can_act(admin), pipes_access.can_act(user)],
    'register': [pipes_access.can_register(admin)[0], pipes_access.can_register(user)[0]],
    'write': [pipes_access.can_write_pipes(admin)[0], pipes_access.can_write_pipes(user)[0]],
    'read': [pipes_access.can_read(admin), pipes_access.can_read(user), pipes_access.can_read({})],
}))
'''))
    assert out['act'] == [True, False]
    assert out['register'] == [True, False]
    assert out['write'] == [True, False]
    assert out['read'] == [True, True, False]


def test_a_pipe_whose_instance_cannot_be_read_is_not_called_unregistered(sandbox):
    """
    A database outage reads as an error naming the instance (its secrets masked), not as
    "not registered"; Sync, Verify, Edit and Manage show only for a pipe read as registered; an
    unregistered pipe offers Register (New pipe with its keys) to those who may register; Delete
    returns to the table only once the pipe is read as gone.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, extra_env={
        'MRSM_SQL_W7DOWN': 'postgresql://w7user:W7-down-s3cret@127.0.0.1:1/w7',
    }, code=AS_VIEWER + SEED + '''
from unittest import mock
from dash._callback_context import context_value
from dash._utils import AttributeDict
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipes as callbacks
from meerschaum.api.dash.pages import pipes as pages
from meerschaum.api.dash import pipe_detail, pipes_access

def trigger(prop_id, value=1):
    context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': prop_id, 'value': value}]}))

def ids(component):
    return {node.id for node in pages._walk(component) if isinstance(getattr(node, 'id', None), str)}

registered = make('reg', 'here')
down = mrsm.Pipe('demo:x', 'y', instance='sql:w7down')
missing = mrsm.Pipe('demo:x', 'gone', 'loc', instance=instance)
out = {}
out['states'] = [list(pipe_detail.registration(pipe)) for pipe in (registered, missing)]
state, problem = pipe_detail.registration(down)
out['down_state'] = [state, problem.startswith('Could not read sql:w7down: '), 'W7-down-s3cret' in problem,
                     'not registered' in problem]
down_tab = pipe_detail.build_tab('overview', down, allowed=True)
out['down_tab'] = sorted(ids(down_tab) & {'pipe-detail-read-error', 'pipe-detail-unregistered'})
out['down_secret'] = 'W7-down-s3cret' in str(down_tab)
out['missing_tab'] = sorted(ids(pipe_detail.build_tab('data', missing, allowed=True)))
missing_register = pipe_detail.build_tab('overview', missing, allowed=True, can_register=True)
button = next(node for node in pages._walk(missing_register) if getattr(node, 'id', None) == 'pipe-detail-register')
out['register_href'] = button.href
### The callback tags each answer with the pipe's registration (the page hides the actions).
session = {'session-id': '00000000-0000-4000-8000-0000000b5f01'}
results = {}
### A viewer who is not an administrator: their pipe's access is read first, so an outage still
### reads as an error (not as a pipe they may not see, which reads as not registered).
as_viewer('down-editor', admin=False, role='editor')
trigger('pipe-detail-tabs.active_tab', 'overview')
results['down_editor'] = callbacks.render_pipe_detail_tab('overview', False, 0, pipe_detail.meta_of(down), session)['registered']
as_viewer()
with mock.patch.object(pipes_access, 'can_read', return_value=True), \\
        mock.patch.object(pipes_access, 'can_act', return_value=True), \\
        mock.patch.object(pipes_access, 'can_register', return_value=(True, '')):
    for name, pipe in (('registered', registered), ('missing', missing), ('down', down)):
        trigger('pipe-detail-tabs.active_tab', 'overview')
        result = callbacks.render_pipe_detail_tab('overview', False, 0, pipe_detail.meta_of(pipe), session)
        results[name] = [result['registered'], result['tab'], 'pipe-detail-register' in str(result['children'])]
    ### After Delete: back to the table only when the pipe reads as gone.
    for name, pipe in (('registered', registered), ('missing', missing), ('down', down)):
        trigger('pipe-detail-refresh.data', 5)
        try:
            results['after_delete_' + name] = callbacks.after_pipe_action(5, {'action': 'delete'}, pipe_detail.meta_of(pipe), session)
        except PreventUpdate:
            results['after_delete_' + name] = 'stayed'
out['results'] = results
### The header's actions have their own container (hidden by the page while not registered).
detail = pages.layout('/dash/pipes/reg/here', 'http://localhost/dash/pipes/reg/here')
out['actions_container'] = 'pipe-detail-actions' in ids(detail)
### Register opens New pipe with the keys and the instance filled in.
href = 'http://localhost' + out['register_href']
new = pages.layout(href.split('?')[0][len('http://localhost'):], href)
values = {node.id: getattr(node, 'value', None) for node in pages._walk(new) if isinstance(getattr(node, 'id', None), str)}
out['prefill'] = [values.get('new-pipe-connector'), values.get('new-pipe-metric'),
                  values.get('new-pipe-location-key'), values.get('new-pipe-instance')]
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['states'] == [[True, ''], [False, '']]
    assert out['down_state'] == [None, True, False, False]
    assert out['down_tab'] == ['pipe-detail-read-error'] and out['down_secret'] is False
    assert 'pipe-detail-unregistered' in out['missing_tab'] and 'pipe-detail-register' not in out['missing_tab']
    assert out['register_href'] == (
        '/dash/pipes/new?connector_keys=demo%3Ax&metric_key=gone&instance=sql%3Aapihosttest&location_key=loc'
    )
    assert out['results']['registered'] == [True, 'overview', False]
    assert out['results']['missing'] == [False, 'overview', True]
    assert out['results']['down'] == [None, 'overview', False]
    assert out['results']['down_editor'] is None
    assert out['results']['after_delete_missing'] == '/dash/pipes'
    assert out['results']['after_delete_registered'] == 'stayed'
    assert out['results']['after_delete_down'] == 'stayed'
    assert out['actions_container'] is True
    assert out['prefill'] == ['demo:x', 'gone', 'loc', 'sql:apihosttest']


def test_connector_lists_leave_out_the_hosts_own_variables(sandbox):
    """
    New pipe's connectors and instances and the Copy dialog's instances never offer Meerschaum's
    own environment variables read as connectors (`api:host`, `api:host_config`, the proxy token).
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, extra_env={
        'MRSM_API_HOST_PROXY_TOKEN': 'w7-proxy-token-s3cret',
    }, code=SEED + '''
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import new_pipe, pipe_detail
from meerschaum.api.dash.pages import pipes as pages
from meerschaum.utils.misc import get_connector_labels
pipe = make('copy', 'source')
dialogs = pipe_detail.build_dialogs(pipe)
select = next(node for node in pages._walk(dialogs) if getattr(node, 'id', None) == 'pipe-copy-instance')
new = pages.layout('/dash/pipes/new', 'http://localhost/dash/pipes/new')
datalist = next(node for node in pages._walk(new) if getattr(node, 'id', None) == 'new-pipe-connectors')
instance_select = next(node for node in pages._walk(new) if getattr(node, 'id', None) == 'new-pipe-instance')
print('JSON:' + json.dumps({
    'raw': [label for label in get_connector_labels() if label.startswith('api:host')],
    'connectors': new_pipe.connector_options(),
    'datalist': [option.value for option in datalist.children],
    'instances': [option['value'] for option in instance_select.options],
    'copy': [option['value'] for option in select.options],
    'secret': 'w7-proxy-token-s3cret' in str(dialogs) + str(new),
}))
'''))
    assert out['raw'] == ['api:host', 'api:host_config', 'api:host_proxy_token']
    for key in ('connectors', 'datalist', 'instances', 'copy'):
        assert not any(label.startswith('api:host') for label in out[key]), (key, out[key])
        assert 'sql:apihosttest' in out[key], (key, out[key])
    assert out['secret'] is False


def test_the_search_is_in_the_url_and_only_key_filters_reload_the_rows(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + SEED + '''
from dash._callback_context import context_value
from dash._utils import AttributeDict
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipes as callbacks
from meerschaum.api.dash import pipes_table as pt, pipes_access
from meerschaum.api.dash.pages import pipes as pages
make('srch', 'store_1', tags=['retail'])
make('srch', 'store_2', tags=['retail'])

def trigger(prop_id, value=1):
    context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': prop_id, 'value': value}]}))

out = {}
meta = {'instance': str(instance), 'global': str(instance)}
search = pt.search_from_filters(['srch'], [], [], ['retail'], 'failing', q='  store 1 ')
out['search'] = search
out['q'] = pt.filters_from_search(search)['q']
### The filters keep the search box's text in the URL; Clear all drops it.
trigger('pipes-tags-dropdown.value', ['retail'])
out['with_text'] = callbacks.update_location_on_pipes_filter_change(['srch'], [], [], ['retail'], 'any', None, None, '', meta, 'store_1')
trigger('pipes-clear-all-button.n_clicks', 1)
out['cleared'] = callbacks.update_location_on_pipes_filter_change(['srch'], [], [], ['retail'], 'any', 1, 'mine', out['with_text'], meta, 'store_1')
### A shared link fills the search box.
layout = pages.layout('/dash/pipes', 'http://localhost/dash/pipes?q=store_1')
out['box'] = next(node for node in pages._walk(layout) if getattr(node, 'id', None) == 'pipes-search').value
### The rows are read again only when the keys, tags or instance change (or on Refresh).
as_viewer()
trigger('pipes-location.search', '?connector_keys=srch')
store, _ = callbacks.load_pipes_rows('?connector_keys=srch', 0, '/dash/pipes', meta, {}, None)
out['loaded'] = [row['mk'] for row in store['rows']]
reloads = {}
for name, url in (
    ('search', '?connector_keys=srch&q=store_2'),
    ('status', '?connector_keys=srch&status=failing'),
    ('tags', '?connector_keys=srch&tags=retail'),
):
    trigger('pipes-location.search', url)
    try:
        callbacks.load_pipes_rows(url, 0, '/dash/pipes', meta, {}, store)
        reloads[name] = True
    except PreventUpdate:
        reloads[name] = False
trigger('pipes-refresh-button.n_clicks', 1)
reloads['refresh'] = bool(callbacks.load_pipes_rows('?connector_keys=srch', 1, '/dash/pipes', meta, {}, store)[0]['rows'])
out['reloads'] = reloads
print('JSON:' + json.dumps(out))
'''))
    assert out['search'] == '?connector_keys=srch&tags=retail&status=failing&q=store+1'
    assert out['q'] == 'store 1'
    assert out['with_text'] == '?connector_keys=srch&tags=retail&q=store_1'
    assert out['cleared'] == ''
    assert out['box'] == 'store_1'
    assert out['loaded'] == ['store_1', 'store_2']
    assert out['reloads'] == {'search': False, 'status': False, 'tags': True, 'refresh': True}


def test_switches_say_what_they_show(sandbox):
    """The read-only Parameters and Definition tabs name their switches in plain words."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import pipe_detail
make('lbl', 'raw', columns={'datetime': 'ts'})
sql = make('sql:apihosttest', 'labels', sql="SELECT * FROM {{ Pipe('lbl', 'raw') }}")
texts = [str(pipe_detail.build_tab(tab, sql, allowed=True)) for tab in ('parameters', 'definition')]
print('JSON:' + json.dumps({
    'parameters': 'Show resolved values' in texts[0],
    'definition': 'Show resolved query' in texts[1],
    'jargon': any('symlink' in text.lower().replace('resolve-symlinks', '') for text in texts),
}))
'''))
    assert out == {'parameters': True, 'definition': True, 'jargon': False}


def test_the_table_is_escaped_html_and_its_statistics_follow_each_render(sandbox):
    """
    The table is one HTML block (Dash renders hundreds of components slowly): keys, tags and
    targets are escaped (quotes, markup, JSX braces, newlines), rows link in place (`dccLink`), the
    sorted header says so (`aria-sort`); each render asks for the statistics the page still needs.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + SEED + '''
from html.parser import HTMLParser
from unittest import mock
from dash._callback_context import context_value
from dash._utils import AttributeDict
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipes as callbacks
from meerschaum.api.dash import pipes_table as pt, pipes_access
make('html', "o'brien <b>&{x}", 'loc/with slash', tags=['t"1', '{t2}', 'two\\nlines'])
make('html', 'plain')
rows = pt.list_rows(instance, connector_keys=['html'])
stats = pt.compute_stats(str(instance), rows)

class Parser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags, self.stack, self.texts = [], [], {}
    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))
        self.stack.append(tag)
    def handle_endtag(self, tag):
        assert self.stack.pop() == tag, tag
    def handle_data(self, data):
        self.texts.setdefault(self.stack[-1] if self.stack else '', []).append(data)

table = pt.render_table_html(rows, stats, global_instance='sql:other', sort={'column': 'rows', 'ascending': False})
parser = Parser()
parser.feed(table)
parser.close()
tricky = next(row for row in rows if row['mk'] != 'plain')
trs = [attrs for tag, attrs in parser.tags if tag == 'tr' and attrs.get('classname') == 'pipes-table-row']
links = [attrs for tag, attrs in parser.tags if tag == 'dcclink']
headers = {attrs['id']: attrs.get('aria-sort') for tag, attrs in parser.tags if tag == 'th' and 'id' in attrs}
buttons = {attrs['id']: attrs.get('data-sort') for tag, attrs in parser.tags if tag == 'button'}
cell_text = ''.join(parser.texts.get('span', []))
out = {
    'balanced': parser.stack == [],
    'raw_specials': [char for char in '{}\\n' if char in table],
    'refs': sorted(attrs['data-ref'] for attrs in trs) == sorted(row['ref'] for row in rows),
    'hrefs': sorted(attrs['href'] for attrs in links) == sorted(row['href'] for row in rows),
    'link_titles': sorted(attrs['title'] for attrs in links) == sorted(row['label'] for row in rows),
    'link_class': sorted({attrs.get('classname') for attrs in links}),
    'label_in_text': tricky['label'] in cell_text,
    'tags': [tag in cell_text for tag in ('t"1', '{t2}')],
    'headers': headers,
    'buttons': buttons,
    'instance_badge': 'pipes-instance-badge' in table,
    'empty': 'No pipes yet.' in pt.render_table_html([], {}, empty_text='No pipes yet.'),
    'loading': 'spinner-border' in pt.render_table_html(rows, stats, loading=True),
}
### Each render hands the statistics callback its generation and page; it fills what is missing.
meta = {'instance': str(instance), 'global': str(instance)}
store = {'instance': str(instance), 'rows': rows, 'generation': 0}
context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': 'pipes-rows-store.data', 'value': store}]}))
table, summary, num_pages, page, hidden, rendered = callbacks.render_pipes_table(
    store, None, '', 'any', {'column': 'pipe', 'ascending': True}, 1, None, meta,
)
out['first_render'] = [table.count('>…<'), summary, num_pages, page, hidden, rendered['generation'], rendered['page']]
pipes_table_session = {'session-id': '00000000-0000-4000-8000-0000000b5f02'}
as_viewer()
if True:
    filled = callbacks.fill_pipes_stats(rendered, store, None, '', 'any', None, pipes_table_session)
    out['filled'] = sorted(filled['stats']) == sorted(row['ref'] for row in rows)
    try:
        callbacks.fill_pipes_stats(rendered, store, None, '', 'any', filled, pipes_table_session)
        out['again'] = 'filled'
    except PreventUpdate:
        out['again'] = 'prevented'
    try:
        callbacks.fill_pipes_stats(dict(rendered, generation=7), store, None, '', 'any', None, pipes_table_session)
        out['stale'] = 'filled'
    except PreventUpdate:
        out['stale'] = 'prevented'
context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': 'pipes-stats-store.data', 'value': filled}]}))
out['second_render'] = callbacks.render_pipes_table(
    store, filled, '', 'any', {'column': 'pipe', 'ascending': True}, 1, None, meta,
)[0].count('>…<')
print('JSON:' + json.dumps(out))
'''))
    assert out['balanced'] is True and out['raw_specials'] == []
    assert out['refs'] and out['hrefs'] and out['link_titles'] and out['label_in_text']
    assert out['link_class'] == ['pipes-table-link']
    assert out['tags'] == [True, True]
    assert out['headers'] == {
        'pipes-header-pipe': 'none', 'pipes-header-rows': 'descending',
        'pipes-header-sync_time': 'none', 'pipes-header-status': 'none', 'pipes-header-team': 'none',
    }
    assert out['buttons'] == {
        'pipes-sort-pipe': 'pipe', 'pipes-sort-rows': 'rows',
        'pipes-sort-sync_time': 'sync_time', 'pipes-sort-status': 'status', 'pipes-sort-team': 'team',
    }
    assert out['instance_badge'] is True and out['empty'] is True and out['loading'] is True
    ### Two rows, three slow cells each, before the statistics.
    assert out['first_render'] == [6, '2 pipes', 1, 1, True, 0, 1]
    assert out['filled'] is True and out['again'] == 'prevented' and out['stale'] == 'prevented'
    assert out['second_render'] == 0
