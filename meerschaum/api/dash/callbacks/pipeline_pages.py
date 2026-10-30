#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Callbacks of the Pipelines list (`/dash/pipelines`), the New pipeline stepper
(`/dash/pipelines/new`) and the Pipe explorer (`/dash/pipelines/explore`).
"""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlencode, quote

import dash
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash import engine as _engine
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.api.dash.pipeline_management import (
    pipeline_title, count_label, pipes_label, runs_placeholder,
)
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

#: The New pipeline stepper's steps, in order.
STEPS = ('name', 'members', 'traversal', 'review')


def _authenticated(session) -> bool:
    """The Pipelines pages are for viewers who may read pipes (`pipes:read`)."""
    from meerschaum.api.dash import pipes_access
    return pipes_access.can_read(session)


def _viewer(session):
    from meerschaum.api.dash import pipes_access
    return pipes_access.viewer(session)


def _instance(search):
    return _engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])


def pipeline_path(name, **query) -> str:
    """The detail page of a Pipeline, with optional query parameters."""
    query = {key: value for key, value in query.items() if value}
    return '/dash/pipelines/' + quote(str(name), safe='') + (('?' + urlencode(query)) if query else '')


def scheduled_jobs(name, registry, jobs) -> List[str]:
    """The schedules of the Jobs that sync this Pipeline on its registry."""
    schedules = []
    for job in jobs.values():
        args = list(getattr(job, 'sysargs', None) or [])
        if args[:3] not in (['sync', 'pipeline', name], ['sync', 'pipelines', name]):
            continue
        if '-i' in args and (args.index('-i') + 1 >= len(args) or args[args.index('-i') + 1] != registry):
            continue
        for flag in ('--schedule', '-s', '--cron'):
            if flag in args and args.index(flag) + 1 < len(args):
                schedules.append(args[args.index(flag) + 1])
                break
    return schedules


def build_list_cards(registry: str, viewer: Any = None) -> List[Any]:
    """
    One runbook entry per visible Pipeline. Keep one bulk registry / recent-runs read;
    each entry's full history loads only when its Details button is opened.
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    instance = _engine.get_instance(registry)
    registry_parameters = dict(access._pipeline_registry(instance))
    definitions = access.visible_pipelines(instance, viewer=viewer, registry_parameters=registry_parameters)
    if not definitions:
        return []
    try:
        runs = _engine.get_runs(instance, limit=100, viewer=viewer)
    except Exception:
        runs = []
    try:
        from meerschaum.jobs import get_jobs
        jobs = get_jobs()
    except Exception:
        jobs = {}
    rows = []
    for name, definition in sorted(definitions.items()):
        targets = definition.get('targets', [])
        instances = {ref.split('|', 1)[0] for ref in targets}
        latest = next((run for run in runs if _engine.run_belongs_to_pipeline(run, name, definition)), None)
        schedules = scheduled_jobs(name, registry, jobs)
        status = str((latest or {}).get('status') or '')
        started_raw = str(((latest or {}).get('run') or {}).get('started') or '')
        if latest:
            last_run = [html.Span(status, className=f'run-status-{status}'),
                        html.Time(started_raw[:16].replace('T', ' '), dateTime=started_raw)]
        else:
            last_run = 'No recent run'
        identity = json.dumps({'name': name, 'instance': registry}, sort_keys=True)
        members = [html.Span(pipes_label(definition, latest))]
        if definition.get('hidden_members'):
            members += [html.Small(access.hidden_label(int(definition['hidden_members'])))]
        complete = not definition.get('hidden_members')
        may_sync = complete and viewer.can('pipes:write')
        may_edit = access.can_edit_pipe(viewer, registry_parameters.get(name, {}))
        manage = [
            (action, label) for action, label in (
                ('analyze', 'Analyze tags'), ('schedule', 'Schedule'), ('copy', 'Copy'),
                ('rename', 'Rename'), ('clear', 'Clear'), ('drop', 'Drop'), ('delete', 'Delete'),
            )
            if may_edit and (complete or action in ('rename', 'delete'))
        ]
        preview = [html.Li(html.Code(_engine.ref_label(ref))) for ref in targets[:8]]
        if len(targets) > 8:
            preview.append(html.Li(f'{len(targets) - 8} more visible members'))
        traversal = ', '.join(label for key, label in (
            ('parents', 'Includes parents'), ('children', 'Includes children')
        ) if definition.get(key))
        rows.append(html.Tbody([
            html.Tr([
                html.Td(html.A(name, href=pipeline_path(name, instance=registry)),
                        className='pipeline-list-name', **{'data-label': 'Pipeline'}),
                html.Td(members, **{'data-label': 'Members'}),
                html.Td(count_label(len(instances), 'instance'), **{'data-label': 'Instances'}),
                html.Td(last_run, className='pipeline-list-last-run', **{'data-label': 'Last run'}),
                html.Td(', '.join(schedules) if schedules else 'Not scheduled',
                        **{'data-label': 'Schedule'}),
                html.Td(
                [dbc.Button('Open', href=pipeline_path(name, instance=registry), size='sm')]
                + ([dbc.Button('Sync', id={'type': 'pipelines-list-sync', 'index': name}, n_clicks=0,
                               size='sm', color='secondary')] if may_sync else [])
                + [dbc.Button('Plan', href=pipeline_path(name, instance=registry, open='plan'),
                              size='sm', color='secondary'),
                   html.Button('Details', id={'type': 'pipeline-row-toggle', 'index': identity},
                               n_clicks=0, className='btn btn-secondary btn-sm',
                               **{'aria-expanded': 'false', 'aria-label': f'Details for {name}'})]
                + ([dbc.DropdownMenu([
                    dbc.DropdownMenuItem(label, href=pipeline_path(name, instance=registry, open=action))
                    for action, label in manage
                ], label='Manage', direction='up', size='sm', color='secondary', menu_variant='dark')]
                   if manage else []),
                className='pipeline-list-actions', **{'data-label': 'Actions'}),
            ], className='pipeline-list-row'),
            html.Tr(html.Td(dbc.Collapse([
                html.Div([
                    html.H4('Members'),
                    html.Ul(preview) if preview else html.P('No visible members'),
                    (html.P('Members sync independently.')
                     if definition.get('graph') is False and not definition.get('parents')
                     and not definition.get('children') else None),
                    html.P(traversal) if traversal else None,
                ], className='pipeline-member-preview'),
                html.Div([
                    html.H4('Recent runs'),
                    html.Div(runs_placeholder(), id={'type': 'pipeline-row-history', 'index': identity},
                             className='pipeline-card-runs'),
                ], className='pipeline-row-runs'),
            ], id={'type': 'pipeline-row-collapse', 'index': identity}, is_open=False),
                colSpan=6), className='pipeline-list-detail'),
        ], className='pipeline-list-entry', **{
            'data-pipeline': name, 'data-status': status, 'data-started': started_raw,
            'data-members': len(targets),
            'data-search': ' '.join([name, status, *schedules, *(_engine.ref_label(ref) for ref in targets)]),
        }))
    return rows


def build_runbook(rows):
    """Keep the table and its empty states in the same list loading boundary."""
    if not rows:
        return html.P('No Pipelines on this registry yet.', className='pipelines-empty')
    return [html.Table([
        html.Thead(html.Tr([html.Th(label, scope='col') for label in (
            'Pipeline', 'Members', 'Instances', 'Last run', 'Schedule', 'Actions'
        )])), *rows,
    ], className='pipelines-runbook', **{'aria-label': 'Pipelines runbook'}),
        html.P('No Pipelines match your search.', id='pipelines-no-matches', hidden=True)]


@dash_app.callback(
    Output({'type': 'pipeline-row-collapse', 'index': MATCH}, 'is_open'),
    Output({'type': 'pipeline-row-toggle', 'index': MATCH}, 'aria-expanded'),
    Input({'type': 'pipeline-row-toggle', 'index': MATCH}, 'n_clicks'),
    State({'type': 'pipeline-row-collapse', 'index': MATCH}, 'is_open'),
    prevent_initial_call=True,
)
def toggle_runbook_details(clicks, is_open):
    if not clicks:
        raise PreventUpdate
    return not is_open, str(not is_open).lower()


@dash_app.callback(
    Output({'type': 'pipeline-row-history', 'index': MATCH}, 'children'),
    Input({'type': 'pipeline-row-collapse', 'index': MATCH}, 'is_open'),
    State('session-store', 'data'), prevent_initial_call=True,
)
def load_runbook_history(is_open, session):
    """Resolve the viewer again when history is requested, including after revocation."""
    if not is_open or not _authenticated(session):
        raise PreventUpdate
    from meerschaum.api.dash.callbacks.pipelines import build_runs_table
    meta = json.loads(dash.ctx.triggered_id['index'])
    return build_runs_table(_engine.get_runs(
        _engine.get_instance(meta['instance'], strict=True), limit=20,
        run_name=meta['name'], viewer=_viewer(session),
    ), table_id='pipeline-row-history-' + meta['name'],
        registry=meta['instance'], pipeline=meta['name'])


@dash_app.callback(
    Output('pipelines-list', 'children'),
    Output('pipelines-find-pipe', 'options'),
    Input('pipelines-list-location', 'pathname'),
    Input('pipelines-list-location', 'search'),
    State('session-store', 'data'),
)
def render_list(pathname, search, session):
    """The registry's Pipelines, and every Pipe of the registry for Find Pipe."""
    if str(pathname).rstrip('/') != '/dash/pipelines' or not _authenticated(session):
        raise PreventUpdate
    instance = _instance(search)
    try:
        cards = build_runbook(build_list_cards(str(instance), viewer=_viewer(session)))
    except Exception as exc:
        cards = [alert_from_success_tuple((False, f'Could not read the Pipelines on {instance}: {exc}'))]
    try:
        options = [{'label': _engine.ref_label(ref), 'value': ref} for ref in sorted(
            '|'.join((str(pipe.instance_keys), pipe.connector_keys, pipe.metric_key,
                      pipe.location_key or 'None'))
            for pipe in _engine._instance_pipes(instance, viewer=_viewer(session)))]
    except Exception:
        options = []
    return cards, options


dash_app.clientside_callback(
    """function(url) {
        // Server callbacks move between Pipelines pages the way links do.
        if (url && window.mrsmNavigate) window.mrsmNavigate(url);
        return dash_clientside.no_update;
    }""",
    Output('pipelines-navigate', 'modified_timestamp'),
    Input('pipelines-navigate', 'data'), prevent_initial_call=True,
)


dash_app.clientside_callback(
    """function(ref) {
        if (!ref || !window.mrsmNavigate) return dash_clientside.no_update;
        window.mrsmNavigate('/dash/pipelines/explore?pipe=' + encodeURIComponent(ref));
        return dash_clientside.no_update;
    }""",
    Output('pipelines-find-pipe', 'title'),
    Input('pipelines-find-pipe', 'value'), prevent_initial_call=True,
)


@dash_app.callback(
    Output('pipelines-list-alert', 'children'),
    Input({'type': 'pipelines-list-sync', 'index': ALL}, 'n_clicks'),
    State('pipelines-list-location', 'search'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def sync_from_list(clicks, search, session):
    """Start a Pipeline run in the background; its output is on the Pipeline's Runs tab."""
    trigger = dash.ctx.triggered_id
    if not isinstance(trigger, dict) or not any(clicks or []) or not _authenticated(session):
        raise PreventUpdate
    from meerschaum.api.dash.execute import run_action
    from meerschaum.api.dash.sessions import get_username_from_session
    from meerschaum.api.dash.callbacks.pipelines import new_console_run_id
    from meerschaum.core.Pipeline._saved import list_pipelines
    name = trigger['index']
    try:
        instance = _instance(search)
        from meerschaum.api.dash import access
        allowed, reason = access.may_sync_pipeline(_viewer(session), name, instance)
        if not allowed:
            raise PermissionError(reason)
        definition = list_pipelines(instance).get(name)
        if not definition:
            raise ValueError(f"Pipeline '{name}' is not registered on {instance}.")
        run_id = new_console_run_id('pipeline')
        result = run_action(['sync', 'pipelines', name, '-i', str(instance), '--run-id', run_id],
                            get_username_from_session((session or {}).get('session-id')),
                            metadata={'run_id': run_id, 'pipeline': {
                                'name': name, 'registry': str(instance),
                                'id': definition.get('id'), 'revision': definition.get('revision')}})
        if not result['success']:
            return alert_from_success_tuple((False, result['msg']))
        return dbc.Alert([html.Span(pipeline_title(name)), ' ',
                          html.A(run_id, href=pipeline_path(name, run_id=run_id))],
                         color='info', dismissable=True)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-explore-graph', 'children'),
    Output('pipeline-explore-title', 'children'),
    Output('pipeline-explore-save', 'href'),
    Input('pipeline-explore-location', 'pathname'),
    Input('pipeline-explore-location', 'search'),
    State('session-store', 'data'),
)
def render_explore(pathname, search, session):
    """The connected graph of one Pipe, read-only; Save as Pipeline pre-fills the stepper."""
    if not str(pathname).startswith('/dash/pipelines/explore') or not _authenticated(session):
        raise PreventUpdate
    from meerschaum.api.dash.callbacks.pipelines import build_pipeline_canvas
    ref = parse_qs((search or '').lstrip('?')).get('pipe', [''])[0]
    if not ref or ref.count('|') < 3:
        return [html.P('Choose a Pipe with Find Pipe on the Pipelines page.')], 'Pipe', '/dash/pipelines/new'
    try:
        instance = _engine.get_instance(_engine.split_ref(ref)[0], strict=True)
        graph = _engine.focus_graph(_engine.plan_graph(instance, viewer=_viewer(session)), ref)
        if ref not in {node['pipe_ref'] for node in graph['nodes']}:
            raise ValueError(f'{_engine.ref_label(ref)} is not registered.')
        refs = sorted(node['pipe_ref'] for node in graph['nodes'])
        canvas = build_pipeline_canvas({**graph, 'focus': ref}, refs, {}, None,
                                       registry=str(instance), read_only=True)
        href = '/dash/pipelines/new?' + urlencode({'members': refs}, doseq=True)
        return canvas, html.Code(_engine.ref_label(ref)), href
    except Exception as exc:
        return [alert_from_success_tuple((False, str(exc)))], html.Code(ref), '/dash/pipelines/new'


@dash_app.callback(
    Output('pipeline-new-prefill', 'data'),
    Input('pipeline-new-location', 'search'),
    State('pipeline-new-prefill', 'data'),
)
def read_new_prefill(search, current):
    """`?members=` (Save as Pipeline) pre-fills the stepper; the registry is the page's."""
    query = parse_qs((search or '').lstrip('?'))
    members = [ref for value in query.get('members', []) for ref in value.split(',') if ref]
    prefill = {'members': members, 'registry': (current or {}).get('registry') or str(_instance(search))}
    if prefill == current:
        raise PreventUpdate
    return prefill


def _depth(flag, depth):
    if not flag:
        return None
    try:
        return int(depth) if depth else True
    except (TypeError, ValueError):
        return True


def _new_draft(members=None):
    return {'step': 'name', 'name': '', 'members': list(members or []), 'new_pipes': {},
            'parents': False, 'parents_depth': None, 'children': False, 'children_depth': None}


def _validate_step(step, draft, registry):
    """Why the stepper cannot leave `step` (forward), or `None`."""
    from meerschaum.core.Pipeline._saved import validate_name, list_pipelines
    if step == 'name':
        if not draft.get('name'):
            return 'Choose a name.'
        try:
            validate_name(draft.get('name') or '')
        except Exception as exc:
            return str(exc) or 'Choose a name.'
        if draft['name'] in list_pipelines(_engine.get_instance(registry)):
            return f"Pipeline '{draft['name']}' already exists on {registry}."
    if step == 'members' and not draft.get('members'):
        return 'Choose at least one member.'
    return None


@dash_app.callback(
    Output('pipeline-new-draft', 'data'),
    Output('pipeline-new-draft-members', 'options'),
    Output('pipeline-new-alert', 'children'),
    Output('pipeline-new-restore', 'hidden'),
    Input('pipeline-new-prefill', 'data'),
    Input({'type': 'pipeline-new-step', 'index': ALL}, 'n_clicks'),
    Input('pipeline-new-back', 'n_clicks'), Input('pipeline-new-next', 'n_clicks'),
    Input('pipeline-new-name', 'value'),
    Input('pipeline-new-draft-members', 'value'),
    Input('pipeline-new-member-options', 'data'),
    Input('pipeline-new-parents', 'value'), Input('pipeline-new-parents-depth', 'value'),
    Input('pipeline-new-children', 'value'), Input('pipeline-new-children-depth', 'value'),
    Input('pipeline-new-resume', 'n_clicks'), Input('pipeline-new-start-over', 'n_clicks'),
    State('pipeline-new-draft', 'data'), State('pipeline-new-drafts', 'data'),
    State('session-store', 'data'),
)
def update_new_draft(prefill, steps, back, forward, name, members, available, parents, parents_depth,
                     children, children_depth, resume, start_over, draft, drafts, session):
    """The stepper's state; nothing is written before Create."""
    if not _authenticated(session):
        raise PreventUpdate
    trigger = dash.ctx.triggered_id
    registry = (prefill or {}).get('registry') or str(_engine.get_instance())
    stored = (drafts or {}).get(registry)
    restore_hidden = True
    alert = []
    ### Fields echo back only when the draft is replaced, so typing is never overwritten.
    replaced = trigger in (None, 'pipeline-new-prefill', 'pipeline-new-resume', 'pipeline-new-start-over')
    if trigger in (None, 'pipeline-new-prefill'):
        prefilled = list((prefill or {}).get('members') or [])
        if draft:
            ### The user is already ahead of the page's first responses.
            draft = {**draft, 'members': list(dict.fromkeys(list(draft.get('members') or []) + prefilled))}
        else:
            draft = _new_draft(prefilled)
        restore_hidden = not stored or bool(prefilled)
        replaced = True
    elif trigger == 'pipeline-new-resume':
        draft = {**_new_draft(), **(stored or {})}
    elif trigger == 'pipeline-new-start-over':
        draft = _new_draft()
    else:
        ### The page's first response may not have arrived yet.
        draft = dict(draft or _new_draft((prefill or {}).get('members')))
        draft.update(name=(name or '').strip(), members=list(members or draft['members']),
                     parents=bool(parents), parents_depth=parents_depth,
                     children=bool(children), children_depth=children_depth)
        if trigger == 'pipeline-new-member-options':
            ref = (available or {}).get('add')
            if ref and ref not in draft['members']:
                draft['members'] = draft['members'] + [ref]
                draft['new_pipes'] = {**(draft.get('new_pipes') or {}), **((available or {}).get('new') or {})}
        elif trigger in ('pipeline-new-back', 'pipeline-new-next') or isinstance(trigger, dict):
            current = STEPS.index(draft.get('step', 'name'))
            target = (max(0, current - 1) if trigger == 'pipeline-new-back'
                      else min(len(STEPS) - 1, current + 1) if trigger == 'pipeline-new-next'
                      else STEPS.index(trigger['index']))
            if target > current:
                for step in STEPS[current:target]:
                    problem = _validate_step(step, draft, registry)
                    if problem:
                        target = STEPS.index(step)
                        alert = alert_from_success_tuple((False, problem))
                        break
            draft['step'] = STEPS[target]
    options = {}
    for ref in draft['members']:
        label = _engine.ref_label(ref) + (' (new)' if ref in (draft.get('new_pipes') or {}) else '')
        options[ref] = {'label': label, 'value': ref}
    for row in (available or {}).get('options', []):
        options.setdefault(row['value'], row)
    ### Fields are written back only when the draft is replaced (a fresh page's fields are
    ### already empty: echoing them could erase early typing).
    if trigger in ('pipeline-new-resume', 'pipeline-new-start-over'):
        dash.set_props('pipeline-new-name', {'value': draft['name']})
        for key in ('parents', 'parents-depth', 'children', 'children-depth'):
            dash.set_props(f'pipeline-new-{key}', {'value': draft[key.replace('-', '_')]})
    if replaced or trigger == 'pipeline-new-member-options':
        ### Options first: a value outside the options would be dropped.
        dash.set_props('pipeline-new-draft-members', {'options': list(options.values()), 'value': draft['members']})
    return (draft, list(options.values()), alert, restore_hidden if replaced else dash.no_update)


dash_app.clientside_callback(
    """function(draft, prefill, drafts) {
        // Show the current step, and keep an unfinished draft in this browser.
        const page = document.getElementById('pipeline-new-page');
        if (!draft || !page) return dash_clientside.no_update;
        const steps = ['name', 'members', 'traversal', 'review'];
        page.dataset.step = draft.step;
        document.querySelectorAll('#pipeline-new-stepper li').forEach(item => {
            const index = steps.indexOf(item.dataset.step), current = steps.indexOf(draft.step);
            item.dataset.state = index === current ? 'current' : index < current ? 'done' : 'todo';
            const button = item.querySelector('button');
            if (button) button.setAttribute('aria-current', index === current ? 'step' : 'false');
        });
        const registry = (prefill || {}).registry || '';
        const next = Object.assign({}, drafts || {});
        if (draft.name || (draft.members || []).length) next[registry] = draft;
        else if (!(prefill || {}).members || !(prefill.members || []).length) return dash_clientside.no_update;
        return next;
    }""",
    Output('pipeline-new-drafts', 'data'),
    Input('pipeline-new-draft', 'data'), State('pipeline-new-prefill', 'data'),
    State('pipeline-new-drafts', 'data'),
)


def review(draft, registry) -> Dict[str, Any]:
    """
    What Create would do: the plan, missing-reference and cycle checks, and the exact
    `pipeline-{name}` tag changes.
    """
    from meerschaum.api.dash.pipeline_editing import make_pipe, validate_edits
    from meerschaum.api.dash.callbacks.pipelines import classify_nodes
    from meerschaum.api.dash import access
    viewer = access.current_viewer()
    new_pipes = draft.get('new_pipes') or {}
    members = list(draft.get('members') or [])
    existing = [ref for ref in members if ref not in new_pipes]
    problems = []
    ### A member the viewer may not see reads as missing (its keys came from the browser).
    missing = [
        ref for ref in existing
        if not access.ref_is_visible(viewer, ref) or make_pipe(ref).get_id() is None
    ]
    if missing:
        problems.append('Missing Pipes: ' + ', '.join(_engine.ref_label(ref) for ref in missing))
    try:
        validate_edits({}, [ref for ref in existing if ref not in missing])
    except ValueError as exc:
        problems.append(str(exc))
    graph = {'nodes': [], 'edges': [], 'warnings': []}
    present = [ref for ref in existing if ref not in missing]
    if present:
        graph = _engine.plan_graph(_engine.get_instance(registry), [make_pipe(ref) for ref in present],
                                   parents=_depth(draft.get('parents'), draft.get('parents_depth')),
                                   children=_depth(draft.get('children'), draft.get('children_depth')))
    planned = {node['pipe_ref'] for node in graph['nodes']}
    graph['nodes'].extend({'pipe_ref': ref, 'label': _engine.ref_label(ref), 'action': 'root',
                           'reason': 'New Pipe', 'status': 'never_run'}
                          for ref in members if ref not in planned and ref in new_pipes)
    roles = classify_nodes(graph, members)
    tag = f"pipeline-{draft.get('name', '')}"
    return {'graph': graph, 'problems': problems, 'tag': tag,
            'tags': [(ref, roles.get(ref, 'explicit')) for ref in sorted(roles, key=_engine.ref_label)],
            'new': [ref for ref in members if ref in new_pipes]}


@dash_app.callback(
    Output('pipeline-new-review', 'children'),
    Output('pipeline-new-create', 'disabled'),
    Input('pipeline-new-draft', 'data'),
    State('pipeline-new-prefill', 'data'), State('session-store', 'data'),
)
def render_review(draft, prefill, session):
    if not draft or draft.get('step') != 'review' or not _authenticated(session):
        raise PreventUpdate
    from meerschaum.api.dash.callbacks.pipelines import build_plan_table
    registry = (prefill or {}).get('registry') or str(_engine.get_instance())
    try:
        result = review(draft, registry)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc))), True
    checks = [html.Li(problem, className='text-danger') for problem in result['problems']] or [
        html.Li('No missing Pipes or cycles.', className='text-success')]
    return [
        html.H5([html.Span(pipeline_title(draft.get('name', ''))), ' ',
                 dbc.Badge(registry, color='secondary', className='pipeline-registry-badge')]),
        html.H6('Checks'), html.Ul(checks, className='pipeline-review-checks'),
        *([html.H6('New Pipes'), html.Ul([html.Li(html.Code(_engine.ref_label(ref))) for ref in result['new']])]
          if result['new'] else []),
        html.H6('Plan'), build_plan_table(result['graph']),
        *[html.P(str(message), className='text-warning') for message in result['graph']['warnings']],
        html.H6('Tags'),
        html.Ul([html.Li([html.Code(_engine.ref_label(ref)), ' + ', html.Code(result['tag']),
                          *([html.Small(f' ({role})')] if role != 'explicit' else [])])
                 for ref, role in result['tags']], className='pipeline-review-tags'),
    ], bool(result['problems'])


@dash_app.callback(
    Output('pipeline-new-alert', 'children', allow_duplicate=True),
    Input('pipeline-new-create', 'n_clicks'),
    State('pipeline-new-draft', 'data'), State('pipeline-new-apply-tags', 'value'),
    State('pipeline-new-prefill', 'data'), State('pipeline-new-drafts', 'data'),
    State('session-store', 'data'),
    State('pipeline-new-team', 'value', allow_optional=True),
    State('pipeline-new-visibility', 'value', allow_optional=True),
    prevent_initial_call=True,
)
def create_pipeline(clicks, draft, apply_tags, prefill, drafts, session, team=None, visibility=None):
    """
    Create: register new Pipes, save the definition (the viewer's, in `team`, visible as
    `visibility`) and (when ticked) apply its tags.
    """
    if not clicks or not draft or not _authenticated(session):
        raise PreventUpdate
    from meerschaum.core.Pipeline._saved import (
        save_pipeline, analyze_pipeline, apply_pipeline_analysis,
    )
    from meerschaum.api.dash.pipeline_editing import make_pipe
    from meerschaum.api.dash import pipe_writes
    registry = (prefill or {}).get('registry') or str(_engine.get_instance())
    viewer = _viewer(session)
    try:
        if not viewer.can('pipes:register'):
            raise PermissionError('Your account may not register Pipelines.')
        pipeline_access = pipe_writes.new_access(viewer, team=team, visibility=visibility)
        for step in STEPS[:-1]:
            problem = _validate_step(step, draft, registry)
            if problem:
                raise ValueError(problem)
        ### (A member the viewer may not see is one of the review's missing Pipes.)
        if review(draft, registry)['problems']:
            raise ValueError('Resolve the checks before creating this Pipeline.')
        instance = _engine.get_instance(registry)
        for ref, parameters in sorted((draft.get('new_pipes') or {}).items()):
            if ref not in draft['members']:
                continue
            pipe = make_pipe(ref, parameters)
            if pipe.get_id() is None:
                registered, message = pipe_writes.register_pipe(
                    pipe, dict(parameters or {}), viewer=viewer, team=team, visibility=visibility,
                )
                if not registered:
                    raise ValueError(f'Could not register {pipe}: {message}')
        save_pipeline(draft['name'], draft['members'], instance,
                      parents=_depth(draft.get('parents'), draft.get('parents_depth')),
                      children=_depth(draft.get('children'), draft.get('children_depth')),
                      create=True, access=pipeline_access)
        remaining = {key: value for key, value in (drafts or {}).items() if key != registry}
        dash.set_props('pipeline-new-drafts', {'data': remaining})
        from meerschaum.api.dash import access
        if apply_tags and not access.may_sync_pipeline(viewer, draft['name'], instance)[0]:
            apply_tags = False
        if apply_tags:
            proposal = analyze_pipeline(draft['name'], instance)
            if proposal.get('changes'):
                result = apply_pipeline_analysis(proposal, instance)
                if not result['success']:
                    return dbc.Alert([f"Created Pipeline('{draft['name']}'), but tags were not applied: "
                                      f"{result['message']} ", html.A('Open', href=pipeline_path(draft['name']))],
                                     color='warning')
        dash.set_props('pipelines-navigate', {'data': pipeline_path(draft['name'])})
        return []
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))
