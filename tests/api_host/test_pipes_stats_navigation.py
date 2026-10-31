"""Queued table statistics must not update a Store after its page has unmounted."""
import ast
from pathlib import Path
import shutil
import subprocess

import pytest


def test_queued_statistics_stop_after_navigation():
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node.js is not installed.')
    path = Path(__file__).resolve().parents[2]/'meerschaum/api/dash/callbacks/pipes.py'
    callbacks = [item for item in ast.walk(ast.parse(path.read_text()))
                 if isinstance(item, ast.Call) and isinstance(item.func, ast.Attribute)
                 and item.func.attr == 'clientside_callback']
    source = next(item.args[0].value for item in callbacks
                  if isinstance(item.args[0], ast.Constant)
                  and 'pipes-stats-request' in str(item.args[0].value))
    harness = r'''
const assert = require('node:assert/strict');
let timers = [], updates = [], mounted = true;
global.window = {location: {pathname: '/dash/pipes'}};
global.document = {getElementById: () => mounted ? {} : null};
global.dash_clientside = {no_update: {}, set_props: (id,props) => updates.push([id,props])};
global.setTimeout = fn => timers.push(fn);
const render = eval('(' + process.argv[1] + ')');
const flush = () => { for (const timer of timers.splice(0)) timer(); };
render({generation: 1});
window.location.pathname = '/dash/pipes/new';
mounted = false;
flush();
assert.equal(updates.length, 0, 'navigation left a statistics update targeting an unmounted Store');
window.location.pathname = '/dash/pipes';
render({generation: 2});
flush();
assert.equal(updates.length, 0, 'the next page has not mounted yet');
mounted = true;
render({generation: 3});
flush();
assert.deepEqual(updates, [['pipes-stats-request', {data: {generation: 3}}]]);
console.log('ok');
'''
    result = subprocess.run([node, '-e', harness, source], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout+result.stderr
