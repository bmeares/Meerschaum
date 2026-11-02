#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Jobs page of rc28 in process: the one listing every callback reads (`get_job_snapshots`,
cached briefly, from `meerschaum._core.jobs_get_summaries`) and the table's refresh in place
(`table_refresh_outputs`: only what changed, a job moved to its new group by its row's class, a
job that went away hidden, a new one rebuilding the table). The browser flows are in
`tests/console/test_rc28_jobs.py`.
"""

import json

from tests.api_host._helpers import AS_VIEWER, run_python, sandbox  # noqa: F401


def _json(result):
    assert result.returncode == 0, result.stderr[-4000:]
    return json.loads(result.stdout.split('JSON:')[-1])


def test_snapshots_come_from_one_cached_listing(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, time
from meerschaum.jobs import Job
import meerschaum.api.dash.jobs as jm
from meerschaum.jobs import _native

job = Job('rc28-snapshot', ['show', 'version'], executor_keys='local')
assert job.start()[0]
deadline = time.time() + 30
while time.time() < deadline and job.status != 'stopped':
    time.sleep(0.2)

calls = []
core = _native.core()
real = core.jobs_get_summaries
class Core:
    def __getattr__(self, name):
        return getattr(core, name)
    def jobs_get_summaries(self, *args):
        calls.append(args)
        return real(*args)
_native.core = lambda: Core()
jm.invalidate_job_snapshots()
first = jm.get_job_snapshots()
second = jm.get_job_snapshots()
jm.invalidate_job_snapshots()
third = jm.get_job_snapshots()
snapshot = first['rc28-snapshot']
out = {
    'calls': len(calls),
    'status': snapshot.status,
    'sysargs': snapshot.sysargs,
    'label': snapshot.label,
    'result': list(snapshot.result),
    'began': snapshot.began is not None and snapshot.began == job.began,
    'ended': snapshot.ended is not None and snapshot.ended == job.ended,
    'health': snapshot.health['state'],
    'kind': snapshot.get_daemon_properties().get('mrsm_execution', {}).get('kind'),
    'automatic': jm.is_automatic_job(snapshot),
    'state': jm.job_status_summary(snapshot),
    'last_run': jm.job_last_run(snapshot)[0],
    'same_as_job': jm.job_status_summary(job),
}
job.delete()
print('JSON:' + json.dumps(out))
'''))
    ### Two reads within the cache's age share one listing; an action forgets it.
    assert out['calls'] == 2
    assert out['status'] == 'stopped' and out['sysargs'] == ['show', 'version']
    assert out['label'] == 'show version' and out['result'][0] is True
    assert out['began'] and out['ended'] and out['health'] == 'stopped'
    assert out['kind'] == 'persistent_job' and out['automatic'] is False
    assert out['state'] == out['same_as_job'] == ['stopped', 'Stopped']
    assert out['last_run'] == 'just now'


def test_the_table_refreshes_only_what_changed(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + '''
import json
from types import SimpleNamespace
from dash import no_update
from meerschaum.api.dash import dash_app
import meerschaum.api.dash.callbacks.jobs as cj
import meerschaum.api.dash.jobs as jm

as_viewer()
jm.is_automatic_job = lambda job: False
cj.is_automatic_job = jm.is_automatic_job

def fake(name, status='running', health=None):
    return SimpleNamespace(name=name, sysargs=['sync', 'pipes', '-m', name], status=status,
                           label='sync pipes -m ' + name, began=None, paused=None, ended=None,
                           result=(True, 'ok'), stop_time=None, health=health)

def table_state(jobs, rendered):
    """The table's current props, as the browser would send them."""
    table = jm.build_jobs_table(list(jobs.values()), authenticated=True)
    found = {}
    def walk(c):
        if isinstance(c, (list, tuple)):
            for x in c: walk(x)
            return
        cid = getattr(c, 'id', None)
        if isinstance(cid, dict):
            found.setdefault(cid['type'], []).append(c)
        children = getattr(c, 'children', None)
        if children is not None and not isinstance(children, str):
            walk(children)
    walk(table)
    return found

def refresh(found, snapshots, rendered):
    rows = [c.id['index'] for c in found['job-row']]
    groups = [c.id['index'] for c in found['jobs-group']]
    return cj.table_refresh_outputs(
        rows,
        [c.className for c in found['job-row']],
        [c.className for c in found['job-row-status']],
        [c.children for c in found['job-row-last-run']],
        [c.title for c in found['job-row-last-run']],
        [c.className for c in found['job-row-actions']],
        groups,
        [c.className for c in found['jobs-group']],
        [c.children for c in found['jobs-group-count']],
        rendered, 'sid', snapshots=snapshots,
    )

def plain(outputs):
    def one(x):
        if x is no_update: return 'no_update'
        if isinstance(x, list): return [one(y) for y in x]
        if isinstance(x, (str, int, float, dict)) or x is None: return x
        return type(x).__name__
    return [one(o) for o in outputs]

jobs = {'a': fake('a'), 'b': fake('b'), 'c': fake('c', 'stopped')}
rendered = {'tab': 'jobs', 'names': list(jobs), 'filter': '', 'status': None}
found = table_state(jobs, rendered)
unchanged = plain(refresh(found, dict(jobs), rendered))
### `b` was paused and `c`'s laps now fail (it runs again); `a` went away; `d` appeared.
moved = {'b': fake('b', 'paused'), 'c': fake('c', 'running', {'state': 'failing', 'consecutive_failures': 3}),
         'd': fake('d')}
changed = refresh(found, moved, rendered)
appeared = changed[-1]
changed = plain(changed[:-1])
### The filter of the page still applies: a job no longer matching it is hidden.
filtered = plain(refresh(found, dict(jobs), dict(rendered, filter='-m a'))[:1])
print('JSON:' + json.dumps({
    'unchanged': unchanged, 'changed': changed,
    'appeared': appeared['appeared'] if isinstance(appeared, dict) else None, 'filtered': filtered,
}))
'''))
    ### Nothing changed: nothing is sent.
    rows, status, status_class, last, title, menu, menu_class, group_classes, group_counts, structure = (
        out['unchanged']
    )
    assert rows == status == status_class == last == title == menu == menu_class == ['no_update'] * 3
    assert group_classes == group_counts == ['no_update'] * 3 and structure == 'no_update'

    rows, status, status_class, last, title, menu, menu_class, group_classes, group_counts = out['changed']
    assert rows == ['job-row job-group-running job-row-gone', 'job-row job-group-paused', 'job-row job-group-running']
    assert status == ['no_update', 'Paused', 'Failing']
    assert status_class == ['no_update', 'job-status job-state-paused', 'job-status job-state-failing']
    ### A menu is rebuilt only for a job whose status changed (`b`; `c` runs again too).
    assert menu[0] == 'no_update' and menu[1] == 'DropdownMenu' and menu[2] == 'DropdownMenu'
    assert menu_class == ['no_update', 'job-row-actions job-actions-paused', 'job-row-actions job-actions-running']
    ### Running 2 -> 1 (c), Paused 0 -> 1 (b), Stopped 1 -> 0 (hidden).
    assert group_counts == ['1', '1', '0']
    assert group_classes == ['no_update', 'jobs-group-row jobs-group-paused',
                             'jobs-group-row jobs-group-stopped jobs-group-empty']
    assert out['appeared'] == ['d']
    assert out['filtered'] == [['no_update', 'job-row job-group-running job-row-gone',
                                'job-row job-group-stopped job-row-gone']]
