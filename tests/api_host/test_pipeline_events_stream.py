"""
The Pipelines page follows live runs over the Rust server's
`GET /engine/runs/{run_id}/events/stream` instead of polling their persisted detail: the
callbacks hand the stream's path to the page, the client asset (`assets/pipeline_events.js`)
paints live statuses and chunk progress and asks for refreshes, and a run without a stream keeps
a slow fallback poll.
"""

import json
import pathlib
import shutil
import subprocess
import textwrap

import pytest

from tests.api_host._helpers import REPO, run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_stream_path_and_poll_intervals(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import engine
from meerschaum.api.dash.callbacks import pipelines as page
assert engine.run_events_stream_path('canvas-abc') == '/engine/runs/canvas-abc/events/stream'
assert engine.run_events_stream_path('r 1/x', 'sql:main', wait=30) == (
    '/engine/runs/r%201%2Fx/events/stream?instance_keys=sql%3Amain&wait=30')
assert page.poll_interval(active=True, streaming=False) == page.FALLBACK_POLL_MS
assert page.poll_interval(active=True, streaming=True) == page.STREAM_POLL_MS
assert page.poll_interval(active=False, streaming=False) == page.STREAM_POLL_MS
assert page.FALLBACK_POLL_MS < page.STREAM_POLL_MS
'''))


def test_callbacks_are_wired_to_the_stream(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import build_dash_app, dash_app
build_dash_app()
from meerschaum.api.dash.pages import pipelines as layout_page
from meerschaum.api.dash.callbacks import pipelines as page
ids = set()
def walk(node):
    if isinstance(node, (list, tuple)):
        for child in node:
            walk(child)
        return
    if getattr(node, 'id', None):
        ids.add(node.id)
    walk(getattr(node, 'children', None) or [])
walk(layout_page.detail_layout('daily', 'sql:apihosttest'))
for key in ('pipeline-run-follow', 'pipeline-run-streams', 'pipeline-run-tick',
            'pipeline-run-follow-dummy', 'pipelines-interval'):
    assert key in ids, key
dash_app._setup_server()
inputs = {}
for spec in dash_app._callback_list:
    inputs[spec['output']] = [f"{i['id']}.{i['property']}" for i in spec['inputs']]
follow = next(value for key, value in inputs.items() if 'pipeline-run-follow-dummy.children' in key)
assert 'pipeline-run-follow.data' in follow and 'pipeline-node-run.data' in follow, follow
renders = [key for key in inputs if 'pipelines-gantt-div.children' in key]
assert renders and 'pipeline-run-tick.data' in inputs[renders[0]], inputs[renders[0]]
assert 'pipeline-run-tick.data' in next(
    value for key, value in inputs.items() if 'pipelines-events-pre.children' in key)
nodes = [key for key in inputs if 'pipeline-node-events.data' in key]
assert nodes and 'pipeline-run-tick.data' in inputs[nodes[0]]
'''))


def test_live_selected_run_is_followed_and_finished_runs_are_not(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
page.is_session_authenticated = lambda _: True
page._engine.get_instance = lambda *a, **k: 'sql:apihosttest'
status = {'value': 'running'}
def summary():
    return {'status': status['value'], 'counts': {}, 'run': {'run_id': 'live-run'}}
page._engine.get_runs = lambda *a, **k: [summary()]
page._engine.get_run_detail = lambda *a: {'summary': summary(), 'bars': []}
page._engine.run_belongs_to_pipeline = lambda *a: True
page.build_runs_overview = lambda *a: []
page.build_runs_table = lambda *a, **k: []
updates = {}
dash.set_props = lambda key, value: updates.setdefault(key, {}).update(value)
dash.ctx = SimpleNamespace(triggered_id='pipeline-run-tick')
definition = {'name': 'daily', 'registry': 'sql:apihosttest'}
def render(streams):
    return page.render_pipelines('/dash/pipelines', '', definition, None, 'live-run', [], 0, 0,
                                 {}, None, {'session-id': 'test'}, 0, streams)
render({})
follow = updates['pipeline-run-follow']['data']
assert follow['run_id'] == 'live-run'
assert follow['path'] == '/engine/runs/live-run/events/stream?instance_keys=sql%3Aapihosttest', follow
# Until the stream is open the page polls the fallback interval; then only slowly.
assert updates['pipelines-interval']['interval'] == page.FALLBACK_POLL_MS
render({'live-run': 'open'})
assert updates['pipelines-interval']['interval'] == page.STREAM_POLL_MS
render({'live-run': 'failed'})
assert updates['pipelines-interval']['interval'] == page.FALLBACK_POLL_MS
status['value'] = 'succeeded'
render({'live-run': 'finished'})
assert updates['pipeline-run-follow']['data'] is None
assert updates['pipelines-interval']['interval'] == page.STREAM_POLL_MS
'''))


def test_canvas_sync_streams_its_run(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
from meerschaum.api.dash import execute, sessions
page.is_session_authenticated = lambda _: True
sessions.get_username_from_session = lambda _: 'alice'
updates = {}
dash.set_props = lambda key, value: updates.setdefault(key, {}).update(value)
dash.ctx = SimpleNamespace(triggered_id='pipeline-context')
execute.run_action = lambda args, user, **kw: {'success': True, 'job': 'execute-x', 'stream': '/logs/execute-x/stream', 'msg': ''}
ref = 'sql:apihosttest|sql:src|m|None'
page.sync_selected_pipe(0, {'action': 'sync', 'ref': ref, 'scope': 'children'}, {'ref': ref},
                        'children', None, None, None, None, {'session-id': 'current'})
run = updates['pipeline-node-run']['data']
assert '-canvas-' in run['run_id']
assert run['events_stream'] == (
    '/engine/runs/' + run['run_id'] + '/events/stream?instance_keys=sql%3Aapihosttest&wait='
    + str(page.NODE_RUN_STREAM_WAIT)), run
assert updates['pipelines-interval']['interval'] == page.FALLBACK_POLL_MS
'''))


#: Drives `assets/pipeline_events.js` with a fake browser: `EventSource`, `document`, Dash.
JS_HARNESS = r'''
const assert = require('assert');
const fs = require('fs');
const props = [];
const sources = [];
const painted = [];
class FakeEventSource {
    constructor(url, options) {
        this.url = url; this.options = options; this.readyState = 0; this.listeners = {};
        this.closed = false; sources.push(this);
    }
    addEventListener(name, fn) { (this.listeners[name] = this.listeners[name] || []).push(fn); }
    close() { this.closed = true; this.readyState = 2; }
    emit(event) {
        (this.listeners[event.event] || []).forEach(fn => fn({data: JSON.stringify(event)}));
    }
}
const badge = {textContent: 'running'};
const card = {dataset: {ref: 'i|c|m|None', status: 'running'},
              querySelector: () => badge};
global.window = {
    dash_clientside: {set_props: (id, p) => props.push([id, p])},
    mrsmPaintPipelineStatuses: s => painted.push(s),
};
global.document = {querySelectorAll: () => [card]};
global.EventSource = FakeEventSource;
global.setTimeout = (fn) => { fn(); return 1; };
global.clearTimeout = () => {};
eval(fs.readFileSync(process.argv[1], 'utf8'));
const w = global.window;
const last = id => props.filter(p => p[0] === id).map(p => p[1]).pop();

w.mrsmFollowOnly('selected', 'run-1', '/engine/runs/run-1/events/stream');
assert.strictEqual(sources.length, 1);
assert.strictEqual(sources[0].url, '/engine/runs/run-1/events/stream');
assert.deepStrictEqual(last('pipeline-run-streams').data, {'run-1': 'connecting'});
assert.strictEqual(last('pipelines-interval').interval, 15000);
// Asking again for the same run does not open a second stream.
w.mrsmFollowOnly('selected', 'run-1', '/engine/runs/run-1/events/stream');
assert.strictEqual(sources.length, 1);

const s = sources[0];
s.onopen();
assert.deepStrictEqual(last('pipeline-run-streams').data, {'run-1': 'open'});
const ev = (seq, event, extra) => Object.assign(
    {run_id: 'run-1', seq, ts: '2026-01-01T00:00:00Z', pipe_ref: 'i|c|m|None', event}, extra || {});
s.emit(ev(0, 'node_started', {action: 'incremental', attempt: 1}));
assert.strictEqual(w.mrsmLiveRunStatuses()['i|c|m|None'], 'running');
s.emit(ev(1, 'chunk_started', {chunk: 0, total: 3, source: 'window'}));
s.emit(ev(2, 'chunk_finished', {chunk: 0, success: true, rows: 7, inserted: 7}));
assert.strictEqual(w.mrsmChunkProgress('i|c|m|None'), 'chunk 1/3 · 7 rows');
assert.strictEqual(badge.textContent, 'running · chunk 1/3 · 7 rows');
const ticks = props.filter(p => p[0] === 'pipeline-run-tick').length;
assert.ok(ticks >= 1, 'chunk_finished asks for a refresh');
// Another run's events (a shared stream must never mix runs) are ignored.
s.emit(Object.assign(ev(3, 'node_failed'), {run_id: 'other'}));
assert.strictEqual(w.mrsmLiveRunStatuses()['i|c|m|None'], 'running');
s.emit(ev(3, 'node_finished', {result: {success: false, message: 'boom'}}));
assert.strictEqual(w.mrsmLiveRunStatuses()['i|c|m|None'], 'failed');
assert.strictEqual(painted.pop()['i|c|m|None'], 'failed');
s.emit(ev(4, 'run_finished', {success: false, message: 'done'}));
assert.ok(s.closed, 'closed after run_finished (no reconnect)');
assert.deepStrictEqual(last('pipeline-run-streams').data, {'run-1': 'finished'});
assert.deepStrictEqual(w.mrsmLiveRunStatuses(), {}, 'a finished run no longer overrides');

// A refused stream falls back to polling.
w.mrsmFollowEngineRun('run-2', '/engine/runs/run-2/events/stream?wait=30', 'node');
const refused = sources[1];
refused.readyState = 2;
refused.onerror();
assert.strictEqual(last('pipeline-run-streams').data['run-2'], 'failed');
assert.strictEqual(last('pipelines-interval').interval, 5000);
// Selecting another run drops the previous selected one but keeps node runs.
w.mrsmFollowOnly('selected', null, null);
assert.ok(!('run-1' in last('pipeline-run-streams').data));
assert.ok('run-2' in last('pipeline-run-streams').data);

// A run that will not finish ends with the server's `run_closed`: no reconnect, and its live
// statuses stop overriding the persisted ones.
w.mrsmFollowEngineRun('run-3', '/engine/runs/run-3/events/stream', 'node');
const killed = sources[2];
killed.onopen();
killed.emit(Object.assign(ev(0, 'node_started'), {run_id: 'run-3'}));
assert.strictEqual(w.mrsmLiveRunStatuses()['i|c|m|None'], 'running');
killed.emit({run_id: 'run-3', seq: 0, ts: '2026-01-01T00:00:00Z', pipe_ref: null,
             event: 'run_closed', reason: 'holder_gone', finished: false});
assert.ok(killed.closed, 'closed after run_closed');
assert.strictEqual(last('pipeline-run-streams').data['run-3'], 'finished');
assert.deepStrictEqual(w.mrsmLiveRunStatuses(), {});
// `idle` gives the stream up and polls instead.
w.mrsmFollowEngineRun('run-4', '/engine/runs/run-4/events/stream', 'node');
const quiet = sources[3];
quiet.onopen();
quiet.emit({run_id: 'run-4', seq: 3, ts: '2026-01-01T00:00:00Z', pipe_ref: null,
            event: 'run_closed', reason: 'idle', finished: false});
assert.ok(quiet.closed);
assert.strictEqual(last('pipeline-run-streams').data['run-4'], 'failed');
// A stream that keeps failing to resume (the browser reconnects: readyState 0) is given up.
w.mrsmFollowEngineRun('run-5', '/engine/runs/run-5/events/stream', 'node');
const flaky = sources[4];
flaky.onopen();
flaky.emit(Object.assign(ev(0, 'node_started'), {run_id: 'run-5'}));
for (let i = 0; i < 4; i++) { flaky.readyState = 0; flaky.onerror(); }
assert.ok(!flaky.closed, 'a few reconnects are the browser\'s to make');
flaky.readyState = 0; flaky.onerror();
assert.ok(flaky.closed, 'closed after repeated failures');
assert.strictEqual(last('pipeline-run-streams').data['run-5'], 'failed');
assert.deepStrictEqual(w.mrsmLiveRunStatuses(), {}, 'a failed follow paints nothing');
// The page drops a canvas sync's follow when its execution ends.
w.mrsmUnfollowEngineRun('run-5', 'node');
assert.ok(!('run-5' in last('pipeline-run-streams').data));

w.mrsmUnfollowAllEngineRuns();
assert.deepStrictEqual(last('pipeline-run-streams').data, {});
console.log('ok');
'''


def test_ended_node_runs_are_unfollowed():
    """The canvas-sync output callback drops the run's follow once its job has ended."""
    source = (REPO / 'meerschaum' / 'api' / 'dash' / 'callbacks' / 'pipelines.py').read_text()
    assert "window.mrsmUnfollowEngineRun(item.run_id, 'node')" in source
    assert source.count("mrsmUnfollowEngineRun(item.run_id, 'node')") == 2


def test_client_asset_follows_streams():
    node = shutil.which('node')
    if node is None:
        pytest.skip("Node.js is not installed.")
    asset = REPO / 'meerschaum' / 'api' / 'dash' / 'assets' / 'pipeline_events.js'
    result = subprocess.run(
        [node, '-e', JS_HARNESS, '--', str(asset)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith('ok')
