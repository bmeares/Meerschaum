#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Parents & Children row of a pipe card adds and removes edges by editing `parameters['parents']`
and `parameters['children']`, keeping the shapes the Syncing Engine reads.
"""

import json

from tests.api_host._helpers import AS_VIEWER, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_add_and_remove_parents_and_children(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import engine
from meerschaum.api.dash.pipes import (
    add_pipe_relation, remove_pipe_relation, build_relations_body, _relation_rows,
)

instance = get_api_connector()
def make(mk, lk=None, **params):
    pipe = mrsm.Pipe('rel', mk, lk, instance=instance, parameters=params)
    pipe.register()
    return mrsm.Pipe('rel', mk, lk, instance=instance)

def raw(pipe):
    return mrsm.Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key, instance=instance).get_parameters(
        apply_symlinks=False, refresh=True,
    )

src = make('src')
other = make('other', 'loc')
mid = make('mid', parent={'connector': 'rel', 'metric': 'src', 'lookback_days': 7, 'axis': {'unit': 'second'}})
leaf = make('leaf', children=["Pipe('rel', 'mid')"])

out = {}
out['add_parent'] = add_pipe_relation(mid, 'parents', other)
out['mid_after_add'] = raw(mid)
out['add_dup'] = add_pipe_relation(mid, 'parents', other)
out['add_self'] = add_pipe_relation(mid, 'children', mid)
out['add_inverse_dup'] = add_pipe_relation(mid, 'parents', leaf)
out['parents_labels'] = sorted(r['label'] for r in engine.relations_of(mrsm.Pipe('rel', 'mid', instance=instance))['parents'])

rows = _relation_rows(mrsm.Pipe('rel', 'mid', instance=instance))
out['removable'] = {r['label']: r['removable'] for r in rows['parents']}

out['remove_other'] = remove_pipe_relation(mid, 'parents', other)
out['mid_after_remove'] = raw(mid)
out['remove_inverse'] = remove_pipe_relation(mid, 'parents', leaf)
out['leaf_after_remove'] = raw(leaf)
out['remove_missing'] = remove_pipe_relation(mid, 'parents', other)

out['add_child'] = add_pipe_relation(src, 'children', other)
out['src_after_add_child'] = raw(src)

body = build_relations_body(mrsm.Pipe('rel', 'src', instance=instance), authenticated=True)
out['body_ids'] = sorted(
    json.dumps(c.id, sort_keys=True)
    for c in [body, *body._traverse()]
    if isinstance(getattr(c, 'id', None), dict)
)
anon = build_relations_body(mrsm.Pipe('rel', 'src', instance=instance), authenticated=False)
out['anon_types'] = sorted({c.id['type'] for c in anon._traverse() if isinstance(getattr(c, 'id', None), dict)})
print('JSON:' + json.dumps(out))
'''))
    assert out['add_parent'][0] is True, out['add_parent']
    ### The singular key moves to the plural list and keeps its edge options.
    assert 'parent' not in out['mid_after_add']
    assert out['mid_after_add']['parents'] == [
        {'connector': 'rel', 'metric': 'src', 'lookback_days': 7, 'axis': {'unit': 'second'}},
        "Pipe('rel', 'other', 'loc')",
    ]
    assert out['add_dup'][0] is False and 'already a parent' in out['add_dup'][1]
    assert out['add_self'][0] is False
    assert out['add_inverse_dup'][0] is False and 'already declares' in out['add_inverse_dup'][1]
    assert out['parents_labels'] == [
        "Pipe('rel', 'leaf', instance='sql:apihosttest')",
        "Pipe('rel', 'other', 'loc', instance='sql:apihosttest')",
        "Pipe('rel', 'src', instance='sql:apihosttest')",
    ]
    assert all(out['removable'].values()), out['removable']

    assert out['remove_other'][0] is True, out['remove_other']
    assert out['mid_after_remove']['parents'] == [
        {'connector': 'rel', 'metric': 'src', 'lookback_days': 7, 'axis': {'unit': 'second'}},
    ]
    ### An edge declared on the other side (leaf's `children`) is removed there.
    assert out['remove_inverse'][0] is True, out['remove_inverse']
    assert 'children' not in out['leaf_after_remove']
    assert out['remove_missing'][0] is False

    assert out['add_child'][0] is True
    assert out['src_after_add_child']['children'] == ["Pipe('rel', 'other', 'loc')"]

    types = {json.loads(i)['type'] for i in out['body_ids']}
    assert {'pipe-relations-div', 'pipe-relations-tabs', 'pipe-relation-add-dropdown', 'pipe-relation-remove-button'} <= types
    assert 'pipe-relation-add-dropdown' not in out['anon_types']
    assert 'pipe-relation-remove-button' not in out['anon_types']


def test_relations_callback_adds_and_refuses_anonymous_sessions(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + '''
import json
import meerschaum as mrsm
from dash.exceptions import PreventUpdate
from dash._callback_context import context_value
from dash._utils import AttributeDict
from meerschaum.api import get_api_connector
from meerschaum.api.dash.callbacks import pipes as callbacks

instance = get_api_connector()
a = mrsm.Pipe('cb', 'a', instance=instance)
b = mrsm.Pipe('cb', 'b', instance=instance)
assert a.register()[0] and b.register()[0]
index = json.dumps(a.meta, sort_keys=True)
value = json.dumps(b.meta, sort_keys=True)

def call(authenticated, triggered_id, value_in, n_clicks):
    prop = json.dumps(triggered_id, sort_keys=True)
    context_value.set(AttributeDict(**{
        'triggered_inputs': [{'prop_id': prop + ('.value' if value_in else '.n_clicks'), 'value': value_in or n_clicks}],
    }))
    ### An administrator, an Editor who may not change the pipe (its edit mode is `owner`), or
    ### an anonymous visitor (who sees no pipe at all).
    if authenticated == 'editor':
        as_viewer('relations-editor', admin=False, role='editor')
    elif authenticated:
        as_viewer('relations-admin')
    else:
        as_viewer(None)
    try:
        children = callbacks.edit_pipe_relations(
            [value_in], [n_clicks] if n_clicks else [], 'children', {'session-id': 'x'},
        )
    except PreventUpdate:
        return 'prevented'
    return str(children)

out = {}
add_id = {'type': 'pipe-relation-add-dropdown', 'index': index, 'relation': 'children'}
out['anon'] = call(False, add_id, value, None)
out['anon_params'] = mrsm.Pipe('cb', 'a', instance=instance).get_parameters(apply_symlinks=False, refresh=True)
locked_out = mrsm.Pipe('cb', 'owned', instance=instance, parameters={'access': {'owner': 'someone', 'edit': 'owner'}})
assert locked_out.register()[0]
owned_id = {'type': 'pipe-relation-add-dropdown', 'index': json.dumps(locked_out.meta, sort_keys=True), 'relation': 'children'}
out['not_editor'] = 'You may not change this pipe.' in call('editor', owned_id, value, None)
out['owned_params'] = mrsm.Pipe('cb', 'owned', instance=instance).get_parameters(apply_symlinks=False, refresh=True)
out['added'] = 'Added' in call(True, add_id, value, None)
out['params'] = mrsm.Pipe('cb', 'a', instance=instance).get_parameters(apply_symlinks=False, refresh=True)
remove_id = {'type': 'pipe-relation-remove-button', 'index': index, 'relation': 'children', 'other': value}
out['removed'] = 'Removed' in call(True, remove_id, None, 1)
out['params_after'] = mrsm.Pipe('cb', 'a', instance=instance).get_parameters(apply_symlinks=False, refresh=True)
print('JSON:' + json.dumps(out))
'''))
    ### An anonymous visitor sees no pipe: nothing happens (not even a message naming it).
    assert out['anon'] == 'prevented' and 'children' not in out['anon_params']
    ### An Editor sees a pipe whose edit mode is `owner` but may not change it.
    assert out['not_editor'] is True and 'children' not in out['owned_params']
    assert out['added'] is True and out['params']['children'] == ["Pipe('cb', 'b')"]
    assert out['removed'] is True and 'children' not in out['params_after']
