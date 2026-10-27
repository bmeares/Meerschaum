#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Jobs, Users, Plugins and Tokens pages (rc15): the New job form's command, job
health in the status line, the compact jobs table, the users' admin actions with the last-admin
guard, the Installed plugins listing and the New token button. In process (host mode), with a
temporary root, plugins directory and SQLite instance; the browser flows are in
`tests/console/test_rc15_pages.py`.
"""

import json
import pathlib
import shutil
import tempfile

import pytest

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def _json(result):
    assert result.returncode == 0, result.stderr[-4000:]
    return json.loads(result.stdout.split('JSON:')[-1])


def test_new_job_form_builds_the_start_job_command(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash.jobs import (
    build_new_job_sysargs, job_command_preview, validate_new_job_name, job_action_options,
    action_selects_pipes, action_selects_pipeline,
)
omitted = frozenset({'sh', 'os', 'api'})
b = lambda **kw: build_new_job_sysargs(omitted=omitted, instance='sql:main', **kw)
out = {
    'default_needs_schedule': b(action='sync pipes', repeat='schedule'),
    'schedule': b(action='sync pipes', connector_keys=['sql:a'], metric_keys=['m1', 'm2'],
                  location_keys=[None], tags=['t'], repeat='schedule', schedule='every 10 minutes'),
    'bad_schedule': b(action='sync pipes', repeat='schedule', schedule='every 10 lightyears')[1] != '',
    'loop': b(action='sync pipes', repeat='loop', min_seconds='30'),
    'once_restart': b(action='show version', repeat='once', restart=True),
    'pipeline': b(action='sync pipelines', pipeline='daily', repeat='loop'),
    'pipeline_missing': b(action='sync pipelines', repeat='once'),
    'arguments': b(action='verify pipes', arguments='--workers 4 -i sql:other', repeat='once'),
    'owned_flag': b(action='sync pipes', arguments='-d', repeat='once'),
    'owned_name': b(action='sync pipes', arguments='--name x', repeat='once'),
    'omitted': b(action='sh ls', repeat='once'),
    'negative': b(action='sync pipes', repeat='loop', min_seconds=-1),
    'preview': job_command_preview(['sync', 'pipes', '-s', 'every 10 minutes'], 'my-job'),
    'names': [validate_new_job_name(n, existing=['taken'])[0] for n in ('ok-name', '', 'bad name', 'a;b', 'taken')],
    'selects': [action_selects_pipes('sync pipes'), action_selects_pipes('show version'),
                action_selects_pipeline('sync pipelines'), action_selects_pipeline('sync pipes')],
}
options = job_action_options(omitted=omitted)
out['first_option'] = options[0]['value']
out['options'] = [o['value'] for o in options]
print('JSON:' + json.dumps(out))
'''))
    assert out['default_needs_schedule'][0] is None
    assert 'schedule' in out['default_needs_schedule'][1]
    assert out['schedule'] == [[
        'sync', 'pipes', '-c', 'sql:a', '-m', 'm1', 'm2', '-l', 'None', '-t', 't',
        '-i', 'sql:main', '-s', 'every 10 minutes',
    ], '']
    assert out['bad_schedule']
    assert out['loop'] == [['sync', 'pipes', '-i', 'sql:main', '--loop', '--min-seconds', '30'], '']
    assert out['once_restart'] == [['show', 'version', '--restart'], '']
    assert out['pipeline'] == [['sync', 'pipelines', 'daily', '-i', 'sql:main', '--loop'], '']
    assert out['pipeline_missing'][0] is None
    ### The arguments name another instance: the console's is not added.
    assert out['arguments'] == [['verify', 'pipes', '--workers', '4', '-i', 'sql:other'], '']
    assert out['owned_flag'][0] is None and '-d' in out['owned_flag'][1]
    assert out['owned_name'][0] is None
    assert out['omitted'][0] is None
    assert out['negative'][0] is None
    assert out['preview'] == "mrsm sync pipes -s 'every 10 minutes' --name my-job -d"
    assert out['names'] == [True, False, False, False, False]
    assert out['selects'] == [True, False, True, False]
    assert out['first_option'] == 'sync pipes'
    assert 'sync pipelines' in out['options'] and 'show version' in out['options']
    assert not any(option.split()[0] in ('sh', 'os', 'api') for option in out['options'])


def test_new_job_form_lists_the_instances_pipes(sandbox):
    """
    The New job form's Connectors, Metrics, Locations and Tags list the instance's pipes (SQL
    instances answer `(ck, mk, lk, parameters)`), a pick is in the command, and a failure to read
    the pipes is shown in the form rather than leaving it empty.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, shlex
from types import SimpleNamespace
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import dash_app
from meerschaum.core.Pipeline._saved import save_pipeline
import meerschaum.api.dash.callbacks.jobs as cj
from meerschaum.api.dash.jobs import pipes_keys_options

conn = get_api_connector()
for keys, tags in (
    (('c-demo:sales', 'store_00', None), ['retail']),
    (('c-demo:odd', "o'brien metric", 'loc/with slash'), []),
    (('c-demo:weather', 'temp', 'atl'), ['weather', 'daily']),
):
    assert mrsm.Pipe(*keys, instance=conn, tags=tags).register()[0]
### A saved Pipeline (its registry pipe is an internal `mrsm` pipe, never an option).
save_pipeline('c-line', ['sql:apihosttest|c-demo:sales|store_00|None'], 'sql:apihosttest')

cj.may_manage_jobs = lambda session_id: True
cj.is_state_authenticated = lambda data: True
cj._new_job_instance = lambda: conn
cj.dash = SimpleNamespace(callback_context=SimpleNamespace(triggered_id='jobs-new-button'))
session = {'session-id': 'sid'}
opened = cj.toggle_new_job_modal(1, None, None, session)

def preview(**kw):
    fields = dict(
        action='sync pipes', connector_keys=[], metric_keys=[], location_keys=[], tags=[],
        pipeline=None, arguments='', repeat='once', schedule='', min_seconds=None, restart=False,
        name='c-job',
    )
    fields.update(kw)
    return cj.preview_new_job(*fields.values(), session)[0]

previews = [
    preview(connector_keys=['c-demo:sales']),
    preview(metric_keys=["o'brien metric"], location_keys=['None'], tags=['retail']),
    preview(action='sync pipelines', pipeline='c-line'),
]

class Down:
    def __str__(self):
        return 'sql:down'
    def fetch_pipes_keys(self, **kwargs):
        raise RuntimeError('database is down')

class Plain:
    """An instance whose keys come without parameters (a 3-tuple `fetch_pipes_keys()`)."""
    def __str__(self):
        return 'example:plain'
    def fetch_pipes_keys(self, **kwargs):
        return [('a', 'b', 'None'), ('c', 'd', 'e')]

cj._new_job_instance = lambda: Down()
failed = cj.toggle_new_job_modal(1, None, None, session)
cj._new_job_instance = lambda: Plain()
cj.mrsm = SimpleNamespace(get_pipes=lambda **kw: {'from-the-pipes': []})
plain = cj.toggle_new_job_modal(1, None, None, session)

print('JSON:' + json.dumps({
    'alert': opened[4],
    'connectors': opened[5],
    'metrics': opened[6],
    'locations': opened[7],
    'tags': opened[8],
    'pipelines': opened[9],
    'previews': previews,
    'quoted_metric': shlex.quote("o'brien metric"),
    'failed_alert': str(failed[4]),
    'failed_connectors': failed[5],
    'plain': [plain[5], plain[8], plain[4]],
    'raw': [
        pipes_keys_options([('a', 'b', 'None'), ('mrsm', 'pipeline', 'x')]),
        pipes_keys_options({
            1: ('a', 'b', None, '{"tags": ["t1"]}'),
            2: ('c', 'd', 'e', ['t2', None]),
            3: ('f', 'g', 'h', {'tags': 'solo'}),
            4: ('mrsm', 'pipeline', 'p', {'tags': ['internal']}),
        }),
    ],
}))
'''))
    assert out['alert'] == []
    assert out['connectors'] == ['c-demo:odd', 'c-demo:sales', 'c-demo:weather']
    assert out['metrics'] == ["o'brien metric", 'store_00', 'temp']
    assert out['locations'] == ['atl', 'loc/with slash', 'None']
    assert out['tags'] == ['daily', 'retail', 'weather']
    assert out['pipelines'] == ['c-line']
    assert out['previews'][0] == 'mrsm sync pipes -c c-demo:sales -i sql:apihosttest --name c-job -d'
    assert out['previews'][1] == (
        f"mrsm sync pipes -m {out['quoted_metric']} -l None -t retail -i sql:apihosttest --name c-job -d"
    )
    assert out['previews'][2] == 'mrsm sync pipelines c-line -i sql:apihosttest --name c-job -d'
    assert 'database is down' in out['failed_alert'] and 'sql:down' in out['failed_alert']
    assert out['failed_connectors'] == []
    ### Without parameters in the keys, the tags come from the pipes.
    assert out['plain'] == [['a', 'c'], ['from-the-pipes'], []]
    assert out['raw'][0] == [
        {'connector_keys': ['a'], 'metric_keys': ['b'], 'location_keys': ['None'], 'tags': []}, False,
    ]
    assert out['raw'][1][0]['connector_keys'] == ['a', 'c', 'f']
    assert out['raw'][1][0]['location_keys'] == ['e', 'h', 'None']
    assert out['raw'][1][0]['tags'] == ['solo', 't1', 't2'] and out['raw'][1][1] is True


def test_job_status_line_shows_health(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from meerschaum.api.dash.jobs import job_status_summary, time_ago

now = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
ago = lambda **kw: (now - timedelta(**kw)).isoformat()
def job(status, sysargs=('sync', 'pipes'), result=(True, 'ok'), stop_time=None):
    return SimpleNamespace(status=status, sysargs=list(sysargs), result=result, stop_time=stop_time)
health = lambda state, **kw: {'state': state, 'consecutive_failures': kw.pop('failures', 0), **kw}
out = {
    'healthy': job_status_summary(job('running'), health('healthy', last_success=ago(minutes=2)), now=now),
    'stalled': job_status_summary(job('running'), health('stalled', last_success=ago(hours=3)), now=now),
    'stalled_never': job_status_summary(job('running'), health('stalled'), now=now),
    'failing': job_status_summary(job('running'), health('failing', failures=4, last_success=ago(days=2)), now=now),
    'failing_one_run': job_status_summary(job('running', ('show', 'version')), health('failing', failures=1), now=now),
    'no_health': job_status_summary(job('running'), {}, now=now),
    'paused': job_status_summary(job('paused'), health('healthy', last_success=ago(minutes=30)), now=now),
    'stopped': job_status_summary(job('stopped'), health('stopped', last_success=ago(days=1)), now=now),
    'stopped_failed': job_status_summary(job('stopped', result=(False, 'boom')), {}, now=now),
    ### The health of a stopped one-shot job says `stopped` with no failed laps: its result counts.
    'stopped_failed_health': job_status_summary(
        job('stopped', ('show', 'pipes'), result=(False, 'boom')), health('stopped'), now=now,
    ),
    'stopped_by_hand': job_status_summary(job('stopped', result=(False, 'x'), stop_time=now), {}, now=now),
    'missing': job_status_summary(None),
    'ago': [time_ago(ago(seconds=10), now), time_ago(ago(minutes=59), now), time_ago(ago(hours=5), now),
            time_ago(ago(days=1, hours=2), now), time_ago(ago(days=12), now), time_ago(None, now),
            time_ago('2026-10-27T11:58:00', now)],
}
print('JSON:' + json.dumps(out))
'''))
    assert out['healthy'] == ['running', 'Running · last good sync 2 min ago']
    assert out['stalled'] == ['stalled', 'Stalled · last good sync 3 h ago']
    assert out['stalled_never'] == ['stalled', 'Stalled · no good sync yet']
    assert out['failing'] == ['failing', 'Failing · 4 failed syncs']
    assert out['failing_one_run'] == ['failing', 'Failing · 1 failed run']
    assert out['no_health'] == ['running', 'Running']
    assert out['paused'] == ['paused', 'Paused · last good sync 30 min ago']
    assert out['stopped'] == ['stopped', 'Stopped · last good sync 1 day ago']
    assert out['stopped_failed'] == ['failed', 'Stopped · last sync failed']
    assert out['stopped_failed_health'] == ['failed', 'Stopped · last run failed']
    assert out['stopped_by_hand'] == ['stopped', 'Stopped']
    assert out['missing'][0] == 'dne'
    assert out['ago'] == ['just now', '59 min ago', '5 h ago', '1 day ago', '12 days ago', '', '2 min ago']


def test_job_status_dot_follows_the_health_state(sandbox):
    """
    The status line's dot and color follow the job's state, so a running job that fails or
    stalls is not shown with its process's green (the Overview's colors: red, amber).
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from types import SimpleNamespace
from meerschaum.api.dash.jobs import build_status_children, STATUS_EMOJI

def line(status, health=None, result=(True, 'ok')):
    job = SimpleNamespace(status=status, sysargs=['sync', 'pipes'], health=health, result=result,
                          stop_time=None)
    p = build_status_children(job)
    return {'dot': p.children[0].children, 'class': p.className.split(), 'text': p.children[2].children}

print('JSON:' + json.dumps({
    'running': line('running', {'state': 'healthy', 'consecutive_failures': 0}),
    'failing': line('running', {'state': 'failing', 'consecutive_failures': 20}),
    'stalled': line('running', {'state': 'stalled'}),
    'paused': line('paused', {'state': 'healthy'}),
    'stopped': line('stopped', {}),
    'failed': line('stopped', {}, result=(False, 'boom')),
    'emoji': STATUS_EMOJI,
}))
'''))
    emoji = out['emoji']
    assert out['running']['dot'] == emoji['running']
    assert {'running-job', 'job-state-running'} <= set(out['running']['class'])
    assert out['failing']['dot'] == '🔴' and out['failing']['text'] == 'Failing · 20 failed syncs'
    assert 'job-state-failing' in out['failing']['class'] and 'running-job' not in out['failing']['class']
    assert out['stalled']['dot'] == '🟠' and 'job-state-stalled' in out['stalled']['class']
    assert 'running-job' not in out['stalled']['class']
    assert out['paused']['dot'] == emoji['paused'] and 'paused-job' in out['paused']['class']
    assert out['stopped']['dot'] == emoji['stopped'] and 'stopped-job' in out['stopped']['class']
    assert out['failed']['dot'] == '🔴' and 'job-state-failed' in out['failed']['class']
    assert out['failed']['text'] == 'Stopped · last sync failed'


def test_new_job_creates_starts_and_restarts_a_job(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, time
import meerschaum.api.dash.callbacks.jobs as cj
from meerschaum.api.dash import sessions
from meerschaum.jobs import Job

### Never a real systemd unit from a test: the console's executor is pinned to `local`.
cj.CONSOLE_EXECUTOR_KEYS = 'local'
cj.is_session_authenticated = lambda session_id: True
cj.jobs_route_group_allowed = lambda: True
sessions.session_is_admin = lambda session_id: session_id == 'admin'
out = {}
out['created'] = cj.create_job_from_form(['show', 'version'], 'c-console-job', 'admin')
job = Job('c-console-job', executor_keys='local')
deadline = time.time() + 30
while time.time() < deadline and job.status != 'stopped':
    time.sleep(0.2)
out['exists'] = job.exists()
out['result'] = job.result[0]
out['taken'] = cj.create_job_from_form(['show', 'version'], 'c-console-job', 'admin')
out['invalid_name'] = cj.create_job_from_form(['show', 'version'], 'bad name', 'admin')
### A non-administrator's command is checked by the server, which this test has none of.
out['non_admin'] = cj.create_job_from_form(['show', 'version'], 'c-other', 'bob')
out['other_exists'] = Job('c-other', executor_keys='local').exists()
cj.is_session_authenticated = lambda session_id: False
out['refused'] = cj.create_job_from_form(['show', 'version'], 'c-third', 'nobody')
out['restart'] = cj.restart_job(job)
time.sleep(2)
out['deleted'] = cj.delete_job(job)[0]
out['gone'] = not Job('c-console-job', executor_keys='local').exists()
print('JSON:' + json.dumps(out))
'''))
    assert out['created'][0], out['created']
    assert out['exists'] and out['result']
    assert not out['taken'][0] and 'already exists' in out['taken'][1]
    assert not out['invalid_name'][0]
    assert not out['non_admin'][0] and 'server' in out['non_admin'][1]
    assert not out['other_exists']
    assert not out['refused'][0]
    assert out['restart'][0], out['restart']
    assert out['deleted'] and out['gone']


def test_jobs_tab_is_a_table_past_eight_jobs_with_a_filter(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from types import SimpleNamespace
from meerschaum.api.dash import dash_app
import meerschaum.api.dash.callbacks.jobs as cj

def ids(component):
    found = []
    def walk(c):
        if isinstance(c, (list, tuple)):
            for x in c: walk(x)
            return
        cid = getattr(c, 'id', None)
        if cid is not None: found.append(cid)
        walk(getattr(c, 'children', None) or [])
    walk(component)
    return found

def fake(name):
    return SimpleNamespace(name=name, sysargs=['sync', 'pipes', '-m', name], status='stopped',
                           label='sync pipes -m ' + name, began=None, paused=None, ended=None,
                           result=(True, 'ok'), stop_time=None)

jobs = {}
cj._listed_jobs = lambda is_runs, session_id: dict(jobs)
cj.may_manage_jobs = lambda session_id: True
import meerschaum.api.dash.jobs as jm
jm.is_automatic_job = lambda job: False

jobs.update({f'job-{i}': fake(f'job-{i}') for i in range(3)})
few, few_names, few_total = cj.build_jobs_tab(False, 'sid', with_total=True)
jobs.update({f'job-{i}': fake(f'job-{i}') for i in range(3, 12)})
many, many_names, many_total = cj.build_jobs_tab(False, 'sid', with_total=True)
filtered, filtered_names, _ = cj.build_jobs_tab(False, 'sid', filter_text='job-1', with_total=True)
### `?status=` (the Overview's Jobs tile): the jobs in one state.
jobs['job-0'].result = (False, 'boom')
stopped, stopped_names, _ = cj.build_jobs_tab(False, 'sid', status='stopped', with_total=True)
failing, failing_names, _ = cj.build_jobs_tab(False, 'sid', status='failing', with_total=True)
running = cj.build_jobs_tab(False, 'sid', status='running')
jobs.clear()
empty = cj.build_jobs_tab(False, 'sid')
print('JSON:' + json.dumps({
    'few_table': 'jobs-table' in ids(few),
    'few_cards': sum(1 for i in ids(few) if isinstance(i, dict) and i.get('type') == 'job-card'),
    'many_table': 'jobs-table' in ids(many),
    'many_rows': sum(1 for i in ids(many) if isinstance(i, dict) and i.get('type') == 'manage-job-status-div'),
    'totals': [few_total, many_total],
    'filtered': filtered_names,
    'row_menu': [i['action'] for i in ids(many) if isinstance(i, dict) and i.get('type') == 'manage-job-button'][:2],
    'empty_text': str(empty),
    'stopped': len(stopped_names),
    'failing': failing_names,
    'running': str(running),
}))
'''))
    assert not out['few_table'] and out['few_cards'] == 3
    assert out['many_table'] and out['many_rows'] == 12
    assert out['totals'] == [3, 12]
    assert out['filtered'] == ['job-1', 'job-10', 'job-11']
    assert out['row_menu'] == ['start', 'start']
    assert 'No jobs' in out['empty_text'] and 'New job' in out['empty_text']
    assert out['stopped'] == 11 and out['failing'] == ['job-0']
    assert 'No running jobs' in out['running']


def test_users_admin_actions_and_the_last_admin(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.core import User
from meerschaum.core.User import verify_password
from meerschaum.api import get_api_connector
from meerschaum.api.dash import dash_app
import meerschaum.api.dash.users as users
import meerschaum.api.dash.callbacks.users as cu
from meerschaum.api.dash import sessions

conn = get_api_connector()
out = {}
out['add_admin'] = users.add_user('c-admin', 'correct-horse-1', 'admin', 'admin@example.com', conn=conn)
out['add_user'] = users.add_user('c-user', 'correct-horse-2', 'user', None, conn=conn)
out['add_dupe'] = users.add_user('c-user', 'correct-horse-2', 'user', None, conn=conn)[0]
out['validate'] = [
    users.validate_new_user('', 'correct-horse-1', 'correct-horse-1', None)[0],
    users.validate_new_user('ok', 'abc', 'abc', None)[0],
    users.validate_new_user('ok', 'correct-horse-1', 'different-1', None)[0],
    users.validate_new_user('ok', 'correct-horse-1', 'correct-horse-1', 'not-an-email')[0],
    users.validate_new_user('ok', 'correct-horse-1', 'correct-horse-1', 'ok@example.com')[0],
]
rows = users.get_user_rows(conn)
out['rows'] = [r for r in rows if r['username'].startswith('c-')]
out['guard'] = [
    users.check_last_admin(rows, 'c-admin', new_type='user')[0],
    users.check_last_admin(rows, 'c-admin', deleting=True)[0],
    users.check_last_admin(rows, 'c-user', deleting=True)[0],
    users.check_last_admin(rows, 'c-admin', new_type='admin')[0],
]

### The callbacks: only administrators change users; the only admin is never demoted or deleted.
cu.viewer = lambda data: {'username': 'c-admin', 'active': True, 'admin': True, 'sees_all': True}
target = lambda name, action: {'username': name, 'action': action}
out['demote_last'] = str(cu.submit_change_type(1, target('c-admin', 'type'), 'user', 0, {})[1])
out['delete_last'] = str(cu.submit_delete_user(1, target('c-admin', 'delete'), 'c-admin', 0, {})[1])
out['delete_untyped'] = str(cu.submit_delete_user(1, target('c-user', 'delete'), 'wrong', 0, {})[1])

### Sessions of a promoted user keep going with their rights read again; a demoted, reset or
### deleted user's sessions end.
sessions.set_session('00000000-0000-4000-8000-0000000c5e01', {'username': 'c-user', 'admin': False, 'authenticated': False})
promoted = cu.submit_change_type(1, target('c-user', 'type'), 'admin', 0, {})
out['promoted'] = [promoted[0], users.normalize_user_type(conn.get_user_type(User('c-user', instance=conn)))]
out['promoted_session'] = sessions.get_session_data('00000000-0000-4000-8000-0000000c5e01', use_cache=False)
out['demote_now_allowed'] = cu.submit_change_type(1, target('c-admin', 'type'), 'user', 0, {})[0]
sessions.set_session('00000000-0000-4000-8000-0000000c5ea1', {'username': 'c-admin', 'admin': True})
cu.submit_change_type(1, target('c-admin', 'type'), 'admin', 0, {})
cu.viewer = lambda data: {'username': 'c-user', 'active': True, 'admin': True, 'sees_all': True}
sessions.set_session('00000000-0000-4000-8000-0000000c5e02', {'username': 'c-user', 'admin': True})
reset = cu.submit_reset_password(1, target('c-user', 'password'), 'brand-new-pass-1', 'brand-new-pass-1', 0,
                                 {'session-id': '00000000-0000-4000-8000-0000000c5e02'})
out['reset'] = reset[0]
out['password'] = verify_password('brand-new-pass-1', conn.get_user_password_hash(User('c-user', instance=conn)))
out['reset_keeps_own_session'] = sessions.get_session_data('00000000-0000-4000-8000-0000000c5e02', use_cache=False) is not None
out['reset_ended_other'] = sessions.get_session_data('00000000-0000-4000-8000-0000000c5e01', use_cache=False) is None
cu.viewer = lambda data: {'username': 'c-admin', 'active': True, 'admin': True, 'sees_all': True}
sessions.set_session('00000000-0000-4000-8000-0000000c5e03', {'username': 'c-user'})
deleted = cu.submit_delete_user(1, target('c-user', 'delete'), 'c-user', 0, {})
out['deleted'] = [deleted[0], 'c-user' in (conn.get_users() or [])]
out['deleted_session'] = sessions.get_session_data('00000000-0000-4000-8000-0000000c5e03', use_cache=False)

cu.viewer = lambda data: {'username': 'bob', 'active': True, 'admin': False, 'sees_all': True}
out['non_admin'] = str(cu.submit_change_type(1, target('c-admin', 'type'), 'user', 0, {})[1])
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['add_admin'][0], out['add_admin']
    assert out['add_user'][0], out['add_user']
    assert out['add_dupe'] is False
    assert out['validate'] == [False, False, False, False, True]
    assert out['rows'] == [
        {'username': 'c-admin', 'type': 'admin', 'email': 'admin@example.com'},
        {'username': 'c-user', 'type': 'user', 'email': None},
    ]
    assert out['guard'] == [False, False, True, True]
    assert 'only admin' in out['demote_last']
    assert 'only admin' in out['delete_last']
    assert 'Type the username' in out['delete_untyped']
    assert out['promoted'] == [False, 'admin']
    assert out['promoted_session'] == {'username': 'c-user'}
    assert out['demote_now_allowed'] is False
    assert out['reset'] is False
    assert out['password'] is True
    assert out['reset_keeps_own_session'] and out['reset_ended_other']
    assert out['deleted'] == [False, False]
    assert out['deleted_session'] is None
    assert 'Only administrators' in out['non_admin']


@pytest.fixture
def plugins_root():
    """A root and plugins directory of fake installed plugins (Python and Rust) with manifests."""
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-c-plugins-', dir=scratch))
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-c-plugins-dir-', dir=scratch))
    try:
        (plugins / 'py_pkg').mkdir()
        (plugins / 'py_pkg' / '__init__.py').write_text(
            "__version__ = '1.2.3'\nrequired = ['json5_definitely_missing_xyz', 'json']\n"
        )
        (plugins / 'py_file.py').write_text("__version__ = '0.1'\n")
        (plugins / 'rs_single.rs').write_text("// a single-file Rust plugin\n")
        (plugins / 'rs_dir').mkdir()
        (plugins / 'rs_dir' / 'plugin.toml').write_text('name = "rs_dir"\nversion = "0.4.0"\n')
        (plugins / 'not_a_plugin.txt').write_text('ignored\n')
        cache = root / 'cache'
        cache.mkdir()
        (cache / 'plugins_manifest.json').write_text(json.dumps({'plugins': {
            'py_pkg': {
                'name': 'py_pkg', 'version': '1.2.3',
                'required': ['json5_definitely_missing_xyz', 'json', 'plugin:py_file'],
                'data_functions': ['fetch'], 'actions': [{'name': 'py_pkg_report'}],
                'web_pages': ['/dash/py-pkg'], 'api_plugins': ['routes'],
                'mcp_tools': [{'name': 'a'}, {'name': 'b'}], 'connectors': ['pykind'],
            },
            'py_file': {'name': 'py_file', 'version': '0.1', 'import_error': 'ImportError: nope'},
        }}))
        (cache / 'native_plugins_manifest.json').write_text(json.dumps({
            'plugins': {'rs_dir': {'name': 'rs_dir', 'version': '0.4.0', 'actions': [{'name': 'rs_go'}]}},
            'connector_types': {'rskind': {'plugin': 'rs_dir'}},
        }))
        (plugins / '.mrsm_origins.json').write_text(json.dumps({'py_pkg': 'api:mrsm'}))
        yield root, plugins
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(plugins, ignore_errors=True)


def test_installed_plugins_list_kinds_versions_and_provides(sandbox, plugins_root):
    root, plugins = plugins_root
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash.plugins import (
    get_installed_plugins, plugin_kind_label, packages_label, plugin_action_sysargs, THIS_SERVER_REPOSITORY,
    build_installed_plugins_table,
)
import meerschaum.api.dash.plugins as mp
mp.this_server_env = lambda username=None: {'MRSM_API_THIS_SERVER': 'http://127.0.0.1:1'}
listed = {p['name']: p for p in get_installed_plugins()}
out = {
    'names': sorted(listed),
    'kinds': {name: plugin_kind_label(p) for name, p in listed.items()},
    'versions': {name: p['version'] for name, p in listed.items()},
    'provides': {name: p['provides'] for name, p in listed.items()},
    'packages': {name: packages_label(p) for name, p in listed.items()},
    'origin': listed['py_pkg']['origin'],
    'import_error': listed['py_file']['import_error'],
    'not_loaded': {name: p.get('not_loaded') for name, p in listed.items()},
    'sysargs': [
        plugin_action_sysargs('uninstall', 'py_pkg'),
        plugin_action_sysargs('update', 'py_pkg', origin='api:mrsm'),
        plugin_action_sysargs('update', 'py_file'),
        plugin_action_sysargs('install', 'rs_dir'),
        plugin_action_sysargs('reinstall', 'rs_dir'),
        plugin_action_sysargs('install', '../etc'),
    ],
    'table_admin': str(build_installed_plugins_table(list(listed.values()), admin=True)).count("'plugin-action'"),
    'table_user': str(build_installed_plugins_table(list(listed.values()), admin=False)).count("'plugin-action'"),
}
print('JSON:' + json.dumps(out))
'''))
    assert out['names'] == ['py_file', 'py_pkg', 'rs_dir', 'rs_single']
    assert out['kinds'] == {
        'py_file': 'Python', 'py_pkg': 'Python',
        'rs_dir': 'Rust (directory)', 'rs_single': 'Rust (single file)',
    }
    assert out['versions'] == {'py_file': '0.1', 'py_pkg': '1.2.3', 'rs_dir': '0.4.0', 'rs_single': None}
    assert out['provides']['py_pkg'] == [
        'Connector: pykind', 'Data source', 'Action: py pkg report', '1 page', 'API routes', '2 MCP tools',
    ]
    assert out['provides']['rs_dir'] == ['Connector: rskind', 'Action: rs go']
    assert out['packages']['py_pkg'] == ['1 of 2 missing', True]
    assert out['packages']['rs_dir'] == ['—', False]
    assert out['origin'] == 'api:mrsm'
    assert out['import_error'] == 'ImportError: nope'
    assert out['not_loaded']['rs_single'] is True and not out['not_loaded']['rs_dir']
    assert out['sysargs'][0] == [['uninstall', 'plugins', 'py_pkg', '--yes'], {}, '']
    assert out['sysargs'][1] == [['install', 'plugins', 'py_pkg', '-r', 'api:mrsm'], {}, '']
    assert out['sysargs'][2][0] is None
    assert out['sysargs'][3] == [
        ['install', 'plugins', 'rs_dir', '-r', 'api:this_server'],
        {'MRSM_API_THIS_SERVER': 'http://127.0.0.1:1'}, '',
    ]
    assert out['sysargs'][4][0][-1] == '--force'
    assert out['sysargs'][5][0] is None
    ### Update (with an origin) and Uninstall for administrators only.
    assert out['table_admin'] == 5 and out['table_user'] == 0


def test_installs_from_a_private_server_read_its_plugins_with_a_short_lived_token(sandbox):
    """
    On a `--private` server (plugins are read with credentials), an install from this server's
    own repository (Install, Reinstall, Update) gets a connector carrying a token of the
    administrator who asked, which may only read plugins and expires in 30 minutes; elsewhere
    the connector is the server's URL alone. The connector works as `api:this_server`.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, time
import jwt
import meerschaum.api as api
import meerschaum.api.dash.execute as execute
from meerschaum.api import get_api_connector
from meerschaum.api._oauth2 import manager
from meerschaum.core import User
import meerschaum.api.dash.plugins as mp

conn = get_api_connector()
assert conn.register_user(User('c-plug-admin', 'correct-horse-1', type='admin', instance=conn))[0]
execute.server_url = lambda: 'http://127.0.0.1:1'
public = mp.plugin_action_sysargs('install', 'w7_x', username='c-plug-admin')
api.private = True
install = mp.plugin_action_sysargs('install', 'w7_x', username='c-plug-admin')
update = mp.plugin_action_sysargs('update', 'w7_x', origin='api:this_server', username='c-plug-admin')
elsewhere = mp.plugin_action_sysargs('update', 'w7_x', origin='api:elsewhere', username='c-plug-admin')
connector = json.loads(install[1]['MRSM_API_THIS_SERVER'])
claims = jwt.decode(connector['api_key'], manager.secret, algorithms=[manager.algorithm])
api.no_auth = True
no_auth = mp.plugin_action_sysargs('install', 'w7_x', username='c-plug-admin')
print('JSON:' + json.dumps({
    'public': public,
    'install': [install[0], install[2]],
    'uri': connector['uri'],
    'env': install[1]['MRSM_API_THIS_SERVER'],
    'claims': [claims['sub'], claims['scopes'], (claims['exp'] - time.time()) / 60],
    'update': [update[0], sorted(json.loads(update[1]['MRSM_API_THIS_SERVER']))],
    'elsewhere': elsewhere,
    'no_auth': no_auth[1],
}))
'''))
    assert out['public'] == [
        ['install', 'plugins', 'w7_x', '-r', 'api:this_server'],
        {'MRSM_API_THIS_SERVER': 'http://127.0.0.1:1'}, '',
    ]
    assert out['install'] == [['install', 'plugins', 'w7_x', '-r', 'api:this_server'], '']
    assert out['uri'] == 'http://127.0.0.1:1'
    sub, scopes, minutes = out['claims']
    assert sub == 'c-plug-admin' and scopes == ['plugins:read'] and 29 < minutes <= 30
    assert out['update'] == [['install', 'plugins', 'w7_x', '-r', 'api:this_server'], ['api_key', 'uri']]
    assert out['elsewhere'] == [['install', 'plugins', 'w7_x', '-r', 'api:elsewhere'], {}, '']
    assert out['no_auth'] == {'MRSM_API_THIS_SERVER': 'http://127.0.0.1:1'}

    ### The job's environment defines a working `api:this_server` that sends the token.
    checked = _json(run_python(root, plugins, '''
import json
import meerschaum as mrsm
conn = mrsm.get_connector('api:this_server')
print('JSON:' + json.dumps([conn.login_scheme, conn.host, conn.port, bool(conn.api_key)]))
''', extra_env={'MRSM_API_THIS_SERVER': out['env']}))
    assert checked == ['api_key', '127.0.0.1', 1, True]


def test_tokens_page_has_a_new_token_button_and_no_caption(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import dash_app, pages
import meerschaum.api.dash.callbacks.tokens as ct
import meerschaum.api.dash.tokens as tokens
from meerschaum.core import User
from meerschaum.api import get_api_connector

def find(component, cid):
    if isinstance(component, (list, tuple)):
        for c in component:
            found = find(c, cid)
            if found is not None: return found
        return None
    if getattr(component, 'id', None) == cid:
        return component
    return find(getattr(component, 'children', None) or [], cid)

button = find(pages.tokens.layout, 'tokens-create-button')
ct.is_state_authenticated = lambda data: True
ct.get_tokens_table = lambda session_id: ([], [])
ct.get_user_from_session = lambda session_id: None
empty = ct.refresh_tokens_button_click(None, {'session-id': 'x'})
conn = get_api_connector()
conn.register_user(User('c-tok-admin', 'correct-horse-1', type='admin', instance=conn))
conn.register_user(User('c-tok-user', 'correct-horse-1', attributes={'scopes': ['pipes:read', 'jobs:read']}, instance=conn))
print('JSON:' + json.dumps({
    'button': button.children,
    'empty': str(empty[0]),
    'form_title': str(empty[1][0]),
    'admin_scopes': tokens.default_token_scopes(User('c-tok-admin', instance=conn))[0],
    'user_scopes': [tokens.default_token_scopes(User('c-tok-user', instance=conn))[0],
                    sorted(tokens.default_token_scopes(User('c-tok-user', instance=conn))[1])],
    'no_auth_scopes': tokens.default_token_scopes(None)[0],
}))
'''))
    assert out['button'] == 'New token'
    assert 'No tokens' in out['empty'] and 'Click' not in out['empty'] and '`+`' not in out['empty']
    assert 'New token' in out['form_title']
    assert out['admin_scopes'] is True and out['no_auth_scopes'] is True
    assert out['user_scopes'] == [False, ['jobs:read', 'pipes:read']]


def test_token_forms_fit_a_phone_and_say_grant_all_in_plain_words(sandbox):
    """
    The New token and Edit token forms share their fields (the IDs the callbacks read): Name and
    Expiration take the full width on phones, the scopes flow in columns set by the stylesheet
    (not a fixed three), the dialogs are fullscreen on phones, and the "Grant all scopes" label
    is plain text.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from types import SimpleNamespace
from meerschaum.api.dash import dash_app, pages
import meerschaum.api.dash.tokens as tokens

def walk(component):
    if isinstance(component, (list, tuple)):
        for child in component:
            yield from walk(child)
        return
    yield component
    children = getattr(component, 'children', None)
    if children is not None and not isinstance(children, (str, int, float)):
        yield from walk(children)

def describe(components):
    ids = {}
    for c in components:
        cid = getattr(c, 'id', None)
        if isinstance(cid, dict):
            ids[cid['type']] = cid['index']
        elif cid:
            ids[cid] = None
    return {
        'ids': ids,
        'switch_labels': [c.label for c in components if type(c).__name__ == 'Switch'],
        'columns': [getattr(c, 'xs', None) for c in components if type(c).__name__ == 'Col'],
        'inline_columns': [c.id for c in components
                           if 'columnCount' in (getattr(c, 'style', None) or {})],
        'checklist_class': [c.class_name for c in components if type(c).__name__ == 'Checklist'],
    }

token = SimpleNamespace(id='11111111-1111-1111-1111-111111111111', label='label', expiration=None,
                        is_valid=True, scopes=['pipes:read'])
edit_modal = tokens.build_edit_token_modal(token)
layout = {getattr(c, 'id', None): c for c in walk(pages.tokens.layout) if isinstance(getattr(c, 'id', None), str)}
print('JSON:' + json.dumps({
    'new': describe(list(walk(tokens.build_tokens_register_input_modal(None)))),
    'edit': describe(list(walk(edit_modal))),
    'fullscreen': [layout['tokens-register-input-modal'].fullscreen,
                   layout['tokens-register-output-modal'].fullscreen, edit_modal.fullscreen],
    'label': tokens.GRANT_ALL_SCOPES_LABEL,
}))
'''))
    fields = {
        'tokens-name-input', 'tokens-expiration-datepickersingle', 'tokens-toggle-scopes-switch',
        'tokens-deselect-scopes-button', 'tokens-scopes-checklist', 'tokens-scopes-checklist-div',
    }
    for form in ('new', 'edit'):
        described = out[form]
        assert fields <= set(described['ids']), (form, described['ids'])
        assert described['switch_labels'] == [out['label']]
        assert described['columns'] == [12, 12]
        assert described['inline_columns'] == []
        assert described['checklist_class'] == ['tokens-scopes-checklist']
    assert {out['edit']['ids'][kind] for kind in fields} == {'11111111-1111-1111-1111-111111111111'}
    assert {out['new']['ids'][kind] for kind in fields} == {None}
    assert '`' not in out['label'] and out['label'].startswith('Grant all scopes')
    assert out['fullscreen'] == ['sm-down', 'sm-down', 'sm-down']
