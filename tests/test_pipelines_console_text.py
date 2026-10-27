#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Pipelines pages' words (rc15 T1): sync windows in words instead of dictionaries, run counts
without the zeros, plain event lines, "dependencies" on the canvas, and Pipeline cards that count
the Pipes a Pipeline syncs.
"""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stderr[-6000:]


def test_engine_words(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import engine
assert engine.bounds_label({'begin': '', 'end': None}, '1 day') == 'last sync − 1 day → latest data'
assert engine.bounds_label({'begin': '', 'end': None}) == 'last sync → latest data'
assert engine.bounds_label({'begin': None, 'end': None}) == 'all data'
assert engine.bounds_label({'begin': '2026-10-12T09:00:00Z', 'end': '2026-10-12T12:30:15Z'}) == (
    '2026-10-12 09:00 → 2026-10-12 12:30:15')
assert engine.bounds_label(None) == '—'
assert engine.counts_label({'succeeded': 2, 'skipped': 1, 'failed': 0, 'upstream_failed': 0}) == (
    '2 synced, 1 up to date')
assert engine.counts_label({'upstream_failed': 1}) == '1 blocked by a failed parent'
assert engine.counts_label({}) == 'Nothing ran'
assert engine.action_label('observe') == 'Listens for new data'
assert engine.action_label('upstream_failed') == 'Blocked: a parent failed'

ref = 'sql:apihosttest|sql:apihosttest|daily|None'
line = lambda event, **payload: engine.describe_event(
    {'event': event, 'ts': '2026-10-27T12:00:01.250000', 'pipe_ref': ref, 'payload': payload})
label = engine.ref_label(ref)
assert line('node_finished', result={'success': True, 'inserted': 1200, 'updated': 3}) == (
    f"12:00:01.250 Synced {label}: 1,200 inserted, 3 updated")
assert line('node_finished', result={'success': True}).endswith('no new rows')
assert line('node_failed', error='boom\\nmore', attempt=1, will_retry=True).endswith(
    "failed: boom; trying again")
assert line('chunk_started', chunk=0, total=2, rows=5).endswith('chunk 1 of 2 started (5 rows)')
assert line('node_started', action='root', attempt=2).endswith(f"Syncing {label} (attempt 2)")
for text in (line('node_planned', action='skip'), line('watermark_advanced', to='2026-10-12T09:00:00Z')):
    assert '_' not in text.split(' ', 1)[1].split('Pipe(')[0], text
'''))


def test_canvas_and_cards_words(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks.pipelines import build_pipeline_canvas, build_plan_table, build_runs_table
from meerschaum.api.dash.pipeline_management import pipes_label

a, b, c = ('sql:x|x|a|None', 'sql:x|x|b|None', 'sql:x|x|c|None')
graph = {'nodes': [{'pipe_ref': r, 'label': r, 'action': 'skip', 'status': 'caught_up'} for r in (a, b, c)],
         'edges': [{'parent': a, 'child': b}, {'parent': b, 'child': c}], 'warnings': []}
canvas = str(build_pipeline_canvas(graph, [a, b, c], {}, None))
assert '3 Pipes · 2 dependencies' in canvas, canvas
assert 'connection' not in canvas.lower().replace('aria-label', '')

plan = str(build_plan_table({**graph, 'nodes': [
    {'pipe_ref': a, 'action': 'root', 'reason': 'no parents in this run', 'bounds': {'begin': '', 'end': None},
     'bounds_text': 'last sync − 1 day → latest data'},
    {'pipe_ref': b, 'action': 'skip', 'reason': 'up to date', 'bounds': None},
    {'pipe_ref': c, 'action': 'skip', 'reason': 'up to date', 'bounds': None},
]}))
assert 'last sync − 1 day → latest data' in plan and "{'begin'" not in plan and 'Readiness' not in plan

runs = str(build_runs_table([{'run': {'run_id': 'r1', 'started': '2026-10-27T12:00:00Z'}, 'status': 'succeeded',
                              'duration_seconds': 1.5, 'counts': {'succeeded': 2, 'skipped': 1, 'failed': 0}}]))
assert '2 synced, 1 up to date' in runs and 'succeeded: 2' not in runs

assert pipes_label({'targets': [a]}) == '1 Pipe'
assert pipes_label({'targets': [a, b]}) == '2 Pipes'
assert pipes_label({'targets': [a], 'children': True}) == '1 Pipe + children'
assert pipes_label({'targets': [a], 'parents': 1, 'children': True}) == '1 Pipe + parents and children'
run = {'run': {'selection': json.dumps({'resolved_targets': [a, b, c]})}}
assert pipes_label({'targets': [a], 'children': True}, run) == '3 Pipes'
'''))
