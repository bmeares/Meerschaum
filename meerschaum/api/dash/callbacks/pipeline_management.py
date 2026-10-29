"""Authenticated persistence, Edit mode, Manage dialogs and Job controls of one Pipeline's page."""
import dash
from urllib.parse import parse_qs, urlencode, quote, unquote
from dash import Input, Output, State, ALL, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from meerschaum.api.dash import dash_app, engine
from meerschaum.api.dash.sessions import is_state_authenticated
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.core.Pipeline._saved import (
    list_pipelines, save_pipeline, delete_pipeline, job_arguments, initialize_pipelines, derivative_job_settings,
)


def _require_session(data):
    """A Pipeline's page is for viewers who may read pipes (`pipes:read`)."""
    from meerschaum.api.dash import pipes_access
    if not pipes_access.can_read(data):
        raise PreventUpdate


def _viewer(session):
    from meerschaum.api.dash import pipes_access
    return pipes_access.viewer(session)


def _require_pipeline_rights(session, name, instance, permission='pipes:write'):
    """
    Raise `PermissionError` unless the viewer may change the saved Pipeline `name` (its
    registry pipe's access, AC7) within `permission`.
    """
    from meerschaum.api.dash import access
    viewer = _viewer(session)
    if viewer.no_auth or viewer.is_admin:
        return viewer
    parameters = access.pipeline_registry_parameters(name, instance)
    if not parameters or not access.can_see_pipe(viewer, parameters):
        raise PermissionError(f"Pipeline '{name}' is not available.")
    if not access.can_edit_pipe(viewer, parameters, permission):
        raise PermissionError(f"You may not change Pipeline('{name}').")
    return viewer


def _require_members_rights(session, name, instance, permission):
    """Raise `PermissionError` unless the viewer may `permission` every member of a Pipeline."""
    from meerschaum.api.dash import access
    viewer = _viewer(session)
    if viewer.no_auth or viewer.is_admin:
        return viewer
    if not viewer.can(permission):
        raise PermissionError(f"Your account may not do this (`{permission}`).")
    allowed, reason = access.may_sync_pipeline(viewer, name, instance)
    if not allowed:
        raise PermissionError(reason)
    return viewer


def pipeline_path(name):
    """The detail page of a Pipeline."""
    return '/dash/pipelines/' + quote(str(name), safe='')


def name_from_path(pathname, search=''):
    """The Pipeline of `/dash/pipelines/<name>` (or a legacy `?pipeline=` link)."""
    rest = str(pathname or '')[len('/dash/pipelines'):].strip('/')
    if rest:
        first = rest.split('/', 1)[0]
        return None if first in ('new', 'explore') else unquote(first)
    return parse_qs((search or '').lstrip('?')).get('pipeline', [None])[0]


def _instance(search):
    return engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])


def _draft_changes(draft, definition, layout=None):
    """How many changes an Edit-mode draft holds against its saved definition."""
    draft, definition = draft or {}, definition or {}
    saved, current = set(definition.get('targets', [])), set(draft.get('targets', []))
    count = len(saved ^ current)
    count += sum(bool(draft.get(key) != definition.get(key) and (draft.get(key) or definition.get(key)))
                 for key in ('parents', 'children'))
    count += len(draft.get('pipe_edits') or {})
    if layout and layout != (definition.get('layout') or {}):
        count += 1
    return count


@dash_app.callback(
    Output('pipeline-saved-name', 'options'),
    Output('pipeline-saved-name', 'value'),
    Output('pipeline-definition', 'data'),
    Output('pipeline-name-input', 'value'),
    Input('pipelines-location', 'pathname'), Input('pipeline-saved-name', 'value'),
    Input('pipelines-location', 'search'),
    Input('pipelines-refresh-button', 'n_clicks'), State('session-store', 'data'),
    State('pipeline-definition', 'data'), State('pipeline-saved-name', 'options'),
)
def read_selection(pathname, name, search, refresh, session, current=None, current_options=None):
    if (not str(pathname).startswith('/dash/pipelines')
        or str(pathname).rstrip('/').split('/')[-1] in ('new', 'explore')):
        raise PreventUpdate
    _require_session(session)
    query = parse_qs((search or '').lstrip('?'))
    instance = engine.get_instance(query.get('instance', [None])[0])
    requested = name_from_path(pathname, search)
    if requested and dash.ctx.triggered_id in ('pipelines-location', None):
        name = requested
    elif not name and requested:
        name = requested
    ### Only the Pipelines the viewer may see, without their hidden members (AC7).
    from meerschaum.api.dash import access
    definitions = access.visible_pipelines(
        instance, viewer=_viewer(session), definitions=list_pipelines(instance),
    )
    dash.set_props('pipeline-definitions', {'data': definitions})
    definition = definitions.get(name)
    dash.set_props('pipelines-alert-div', {'children': [] if definition or not name else
        alert_from_success_tuple((False, f"Pipeline '{name}' is not registered on {instance}."))})
    unchanged = current == definition
    if not unchanged:
        dash.set_props('pipeline-analysis', {'data': None})
        dash.set_props('pipeline-analysis-preview', {'children': []})
        dash.set_props('pipeline-apply-tags', {'disabled': True})
    options = [{'label': key, 'value': key} for key in sorted(definitions)]
    if name and name not in definitions:
        options.append({'label': f'{name} (unavailable)', 'value': name})
    return (options, name,
            dash.no_update if unchanged else definition,
            dash.no_update if unchanged else name or '')


@dash_app.callback(
    Output('pipeline-delete', 'disabled'),
    Input('pipeline-definition', 'data'),
)
def disable_delete(definition):
    return not bool(definition)


@dash_app.callback(
    Output('pipeline-delete-confirm', 'displayed'),
    Output('pipeline-delete-confirm', 'message'),
    Output('pipeline-delete-target', 'data'),
    Input('pipeline-delete', 'n_clicks'),
    State('pipeline-definition', 'data'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def confirm_delete(clicks, definition, session):
    _require_session(session)
    if not clicks or not definition:
        raise PreventUpdate
    return True, (
        f"Delete Pipeline '{definition['name']}' on {definition['registry']}? "
        "Member Pipes, their data, and execution history will be kept."
    ), {key: definition.get(key) for key in ('name', 'registry', 'id', 'revision')}


@dash_app.callback(
    Output('pipeline-manage-alert', 'children'),
    Input('pipeline-delete-confirm', 'submit_n_clicks'),
    Input('pipeline-draft-save', 'n_clicks'),
    State('pipeline-name-input', 'value'), State('pipeline-saved-name', 'value'),
    State('pipeline-draft', 'data'), State('pipeline-definition', 'data'), State('pipelines-location', 'search'), State('session-store', 'data'),
    State('pipeline-draft-members', 'value'), State('pipeline-draft-parents', 'value'),
    State('pipeline-draft-children', 'value'), State('pipeline-delete-target', 'data'),
    State('pipeline-layout-draft', 'data'),
    prevent_initial_call=True,
)
def manage_selection(delete_clicks, draft_save, name, selected, draft, current, search, session,
                     members=..., parents=..., children=..., delete_target=None, layout=None):
    _require_session(session)
    completed = []
    try:
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        from meerschaum.core.Pipeline._saved import _mutation_lock, validate_name
        with _mutation_lock(instance):
            validate_name(selected if dash.ctx.triggered_id == 'pipeline-delete-confirm' else name or selected)
            if dash.ctx.triggered_id == 'pipeline-delete-confirm':
                if not selected:
                    raise ValueError('Select a saved pipeline to delete.')
                _require_pipeline_rights(session, selected, instance, 'pipes:delete')
                saved = list_pipelines(instance).get(selected) or {}
                expected = {key: saved.get(key) for key in ('name', 'registry', 'id', 'revision')}
                if not delete_target or delete_target != expected:
                    raise ValueError('Pipeline selection changed. Review and confirm deletion again.')
                result = delete_pipeline(selected, instance)
                if result[0]:
                    result = True, f"Deleted Pipeline '{selected}' on {instance}. Member Pipes, data, and history were kept."
                    dash.set_props('pipeline-definition', {'data': None})
                    dash.set_props('pipeline-selected-run', {'data': None})
                    dash.set_props('pipeline-live-run', {'data': None})
                    dash.set_props('pipelines-navigate', {'data': '/dash/pipelines'})
            else:
                draft = dict(draft or {})
                # The form can be ahead of its asynchronous draft/history callback.
                if members is not ...:
                    draft['targets'] = members or []
                for key, value in (('parents', parents), ('children', children)):
                    if value is not ... and bool(value) != bool(draft.get(key)):
                        draft[key] = value or None
                from meerschaum.api.dash.pipeline_editing import apply_edits
                from meerschaum.api.dash import access
                edits = draft.get('pipe_edits') or {}
                definitions = list_pipelines(instance)
                if not selected and name in definitions:
                    raise ValueError('That Pipeline name is already registered. Choose another name.')
                viewer = _viewer(session)
                if selected:
                    _require_pipeline_rights(session, selected, instance, 'pipes:write')
                elif not viewer.can('pipes:register'):
                    raise PermissionError('Your account may not register Pipelines.')
                ### Members the viewer may not see stay (they cannot remove what they cannot see),
                ### and they may add only pipes they can see.
                kept_hidden = access.hidden_members(selected, instance, viewer=viewer) if selected else []
                for ref in draft.get('targets', []):
                    if ref not in (draft.get('new_pipes') or {}) and not access.ref_is_visible(viewer, ref):
                        raise PermissionError(f'{engine.ref_label(ref)} is not available.')
                draft['targets'] = list(draft.get('targets', [])) + [
                    ref for ref in kept_hidden if ref not in draft.get('targets', [])
                ]
                previous = definitions.get(selected) if selected else None
                if selected and (previous or {}).get('revision') != (current or {}).get('revision'):
                    raise ValueError('Pipeline revision changed; reload before saving.')
                for ref, parameters in sorted((draft.get('new_pipes') or {}).items()):
                    if ref not in draft.get('targets', []):
                        continue
                    from meerschaum.api.dash.pipeline_editing import make_pipe
                    from meerschaum.api.dash import pipe_writes
                    pipe = make_pipe(ref, parameters)
                    if pipe.get_id() is None:
                        registered, message = pipe_writes.register_pipe(
                            pipe, dict(pipe.parameters or {}), viewer=viewer,
                        )
                        if not registered:
                            raise ValueError(f'Could not register {pipe}: {message}')
                        completed.append(f'Registered {pipe}')
                results = apply_edits(edits, draft.get('targets', []), viewer=viewer) if edits else []
                completed.extend(f'Saved shared Pipe {ref}' for ref, success, message in results if success)
                if results:
                    remaining = {ref: edit for ref, edit in edits.items()
                                 if not any(saved_ref == ref and success for saved_ref, success, _ in results)}
                    dash.set_props('pipeline-draft', {'data': {**draft, 'pipe_edits': remaining, 'undo': [], 'redo': []}})
                failed = [f'{ref}: {message}' for ref, success, message in results if not success]
                if failed:
                    return alert_from_success_tuple((False, 'Partial shared Pipe edit results: ' + '; '.join(
                        f'{ref}: {"saved" if success else message}' for ref, success, message in results)))
                definition = save_pipeline(name, draft.get('targets', []), instance,
                    parents=draft.get('parents'), children=draft.get('children'),
                    expected_revision=(current or {}).get('revision'), create=not bool(selected),
                    layout=_clean_layout(layout) if layout else None,
                    access=(access.new_pipe_access(viewer) if not selected else None))
                result = True, f"Saved Pipeline('{name}')."
                dash.set_props('pipeline-definition', {'data': definition})
                dash.set_props('pipeline-layout-draft', {'data': None})
                dash.set_props('pipeline-draft-editor', {'open': False})
                dash.set_props('pipeline-edit-mode', {'data': False})
            dash.set_props('pipelines-refresh-button', {'n_clicks': __import__('time').time_ns()})
            return alert_from_success_tuple(result)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc) + ('. Already applied: ' + '; '.join(completed) if completed else '')))


def _clean_layout(layout):
    """Shared node positions, validated before they are saved."""
    import math
    clean = {}
    for key, position in (layout or {}).items():
        x, y = float(position['x']), float(position['y'])
        if not math.isfinite(x) or not math.isfinite(y) or max(abs(x), abs(y)) > 100000:
            raise ValueError('Layout coordinates are outside the supported range.')
        clean[key] = {'x': x, 'y': y}
    return clean


@dash_app.callback(
    Output('pipeline-jobs', 'children'),
    Input('pipeline-saved-name', 'value'), Input('pipelines-interval', 'n_intervals'),
    Input('pipeline-job-create', 'n_clicks'),
    Input({'type': 'pipeline-job-start', 'index': ALL}, 'n_clicks'),
    Input({'type': 'pipeline-job-stop', 'index': ALL}, 'n_clicks'),
    State('pipeline-job-name', 'value'), State('pipeline-job-schedule', 'value'),
    State('pipeline-job-workers', 'value'), State('pipeline-job-retries', 'value'),
    State('pipeline-job-force', 'value'), State('pipelines-location', 'search'), State('session-store', 'data'),
)
def manage_jobs(pipeline, ticks, create, starts, stops, name, schedule, workers, retries, force, search, session):
    _require_session(session)
    if not pipeline:
        raise PreventUpdate
    from meerschaum.jobs import Job, get_jobs
    message = None
    try:
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        jobs = get_jobs()
        def belongs(job):
            args = job.sysargs
            return (args[:3] in (['sync', 'pipeline', pipeline], ['sync', 'pipelines', pipeline]) and '-i' in args
                    and args.index('-i') + 1 < len(args)
                    and args[args.index('-i') + 1] == str(instance))
        related = {key: job for key, job in jobs.items() if belongs(job)}
        trigger = dash.ctx.triggered_id
        from meerschaum.api.dash import access
        viewer = _viewer(session)
        if not access.visible_pipeline(pipeline, instance, viewer=viewer):
            raise PermissionError(f"Pipeline '{pipeline}' is not available.")
        ### The jobs of this Pipeline the viewer may see (AC9).
        related = access.filter_jobs(viewer, related)
        if trigger == 'pipeline-job-create' and create:
            if not viewer.can('jobs:write'):
                raise PermissionError('Your account may not create jobs.')
            _require_members_rights(session, pipeline, instance, 'pipes:write')
            if pipeline not in list_pipelines(instance):
                raise ValueError('Save the pipeline before scheduling it.')
            from meerschaum.core.Pipeline._saved import validate_name
            validate_name(name)
            if name in jobs:
                raise ValueError('That Job name already exists. Use a new name.')
            from meerschaum.utils.schedule import parse_schedule
            parse_schedule(schedule or '')
            args = job_arguments(pipeline, instance, schedule, workers, retries or 0, force)
            ### The job is the session user's, in their primary team (AC9).
            from meerschaum.jobs import ownership_properties
            from meerschaum.api.dash.access import get_viewer
            viewer = get_viewer(session_data=session)
            job = Job(name, args, _properties=ownership_properties(
                None if viewer.no_auth else viewer.username,
                None if viewer.no_auth else viewer.primary_team,
            ))
            message = alert_from_success_tuple(job.start())
            related[name] = job
        elif isinstance(trigger, dict) and any((starts or []) + (stops or [])):
            job = related.get(trigger['index'])
            if job is None:
                raise ValueError('This Job does not belong to the selected pipeline.')
            starting = trigger['type'] == 'pipeline-job-start'
            permission = 'jobs:execute' if starting else 'jobs:stop'
            if not viewer.can(permission):
                raise PermissionError(f"Your account may not {'start' if starting else 'stop'} jobs.")
            result = job.start() if starting else job.stop()
            message = alert_from_success_tuple(result)
        rows = [html.Tr([
            html.Td(html.A(key, href='/dash/jobs', target='_blank')),
            html.Td(job.status), html.Td(html.Code(job.label)),
            html.Td(
                ([dbc.Button('Start', id={'type': 'pipeline-job-start', 'index': key},
                             size='sm', class_name='me-2')] if viewer.can('jobs:execute') else [])
                + ([dbc.Button('Use settings', id={'type': 'pipeline-job-copy', 'index': key},
                               size='sm', color='secondary', class_name='me-2')] if viewer.can('jobs:write') else [])
                + ([dbc.Button('Stop', id={'type': 'pipeline-job-stop', 'index': key},
                               size='sm', color='secondary')] if viewer.can('jobs:stop') else [])
            ),
        ]) for key, job in sorted(related.items())]
        return [message, dbc.Table([html.Tbody(rows)], size='sm') if rows else html.P('No scheduled Jobs.')]
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-job-name', 'value'), Output('pipeline-job-schedule', 'value'),
    Output('pipeline-job-workers', 'value'), Output('pipeline-job-retries', 'value'),
    Output('pipeline-job-force', 'value'), Output('pipeline-job-settings-notice', 'children'),
    Input({'type': 'pipeline-job-copy', 'index': ALL}, 'n_clicks'),
    State('pipeline-saved-name', 'value'), State('pipelines-location', 'search'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def copy_job_settings(clicks, pipeline, search, session):
    _require_session(session)
    trigger = dash.ctx.triggered_id
    if not isinstance(trigger, dict) or not any(clicks or []):
        raise PreventUpdate
    from meerschaum.jobs import get_jobs
    jobs = get_jobs()
    source = jobs.get(trigger['index'])
    if source is None:
        raise PreventUpdate
    args = source.sysargs
    instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
    if (args[:3] not in (['sync', 'pipeline', pipeline], ['sync', 'pipelines', pipeline]) or '-i' not in args
            or args.index('-i') + 1 >= len(args) or args[args.index('-i') + 1] != str(instance)):
        raise PreventUpdate
    settings = derivative_job_settings(trigger['index'], args, jobs)
    notice = dbc.Alert([
        'Copied settings from ', html.Strong(trigger['index']),
        '. Change the interval or behavior, then create the new Job. '
        'The previous Job keeps its schedule and logs. Use Stop on that Job to retire its schedule.',
    ], color='info', class_name='mt-2')
    return (settings['name'], settings['schedule'], settings['workers'], settings['retries'],
            settings['force'], notice)


@dash_app.callback(
    Output('pipeline-run-alert', 'children'),
    Input('pipeline-run-now', 'n_clicks'),
    State('pipeline-saved-name', 'value'), State('pipelines-location', 'search'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def select_or_run_pipeline(run_clicks, name, search, session):
    _require_session(session)
    if not run_clicks or not name:
        raise PreventUpdate
    from meerschaum.api.dash.execute import run_action
    from meerschaum.api.dash.sessions import get_username_from_session
    from meerschaum.core.Pipeline._saved import validate_name
    try:
        validate_name(name)
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        ### Every member it would sync must be the viewer's to change (AC7).
        _require_members_rights(session, name, instance, 'pipes:write')
        definition = list_pipelines(instance).get(name)
        if not definition:
            raise ValueError('Pipeline registration is missing.')
        from meerschaum.api.dash.callbacks.pipelines import new_console_run_id
        run_id = new_console_run_id('pipeline')
        result = run_action(['sync', 'pipelines', name, '-i', str(instance),
                             '--run-id', run_id],
                            get_username_from_session((session or {}).get('session-id')),
                            metadata={'run_id': run_id, 'pipeline': {
                                'name': name, 'registry': str(instance),
                                'id': definition.get('id'), 'revision': definition.get('revision')}})
        if result['success']:
            query = parse_qs((search or '').lstrip('?'))
            query.pop('run_id', None)
            dash.set_props('pipeline-selected-run', {'data': run_id})
            dash.set_props('pipelines-location', {'search': '?' + urlencode(query, doseq=True) if query else ''})
            dash.set_props('pipeline-live-run', {'data': {
                'pipeline': name, 'registry': str(instance), 'pipeline_id': definition.get('id'),
                'run_id': run_id, 'job': result['job'], 'stream': result['stream'],
                'session_id': (session or {}).get('session-id'),
                'status': 'running',
            }})
            ### The new run's members, at once: a member's output can be opened before the run
            ### history lists the run (`render_pipelines` writes the same store from then on).
            targets = [str(ref) for ref in (definition.get('targets') or [])]
            if targets:
                dash.set_props('pipeline-current-run', {'data': {
                    'run_id': run_id, 'instance': str(instance), 'pipeline': name,
                    'job': result['job'],
                    'labels': {ref: engine.ref_label(ref) for ref in sorted(targets)},
                }})
            dash.set_props('pipelines-interval', {'interval': 2000})
        return [] if result['success'] else alert_from_success_tuple((False, result['msg']))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-cancel-current', 'style'),
    Output('pipeline-run-now', 'disabled'),
    Output('pipeline-view-output', 'disabled'),
    Input('pipeline-definition', 'data'), Input('pipeline-live-run', 'data'),
    Input('pipeline-node-runs', 'data'),
)
def pipeline_summary(definition, live_run=None, node_runs=None):
    """Cancel only while this Pipeline's own run is active; Output reopens retained output."""
    own = bool(definition and live_run and live_run.get('pipeline_id') == definition.get('id'))
    active = own and live_run.get('status') == 'running'
    return {'display': '' if active else 'none'}, not bool(definition), not (own or bool(node_runs))


dash_app.clientside_callback(
    """
    function(store, definition, session) {
        const sessionId = (session || {})['session-id'] || null;
        if (window._mrsmPipelineOutputSession !== sessionId) {
            if (window._mrsmPipelineSource) window._mrsmPipelineSource.close();
            window._mrsmPipelineSource = null; window._mrsmPipelineJob = null;
            window._mrsmPipelineOutputSession = sessionId;
            dash_clientside.set_props('pipeline-live-run', {data:null});
            const output = document.getElementById('pipeline-command-output');
            if (output) output.textContent = '';
            return dash_clientside.no_update;
        }
        const name = definition && definition.name;
        if (!store || store.session_id !== sessionId || !store.stream || store.pipeline !== name ||
            store.registry !== (definition || {}).registry || store.pipeline_id !== (definition || {}).id) {
            if (window._mrsmPipelineSource) window._mrsmPipelineSource.close();
            window._mrsmPipelineSource = null;
            window._mrsmPipelineJob = null;
            window._mrsmPipelineRun = null;
            if (document.getElementById('pipeline-output-dock')?.dataset.outputMode === 'pipeline') window.mrsmClosePipelineOutput?.();
            const output = document.getElementById('pipeline-command-output');
            if (output) output.textContent = '';
            return dash_clientside.no_update;
        }
        window._mrsmPipelineRun = store;
        const label = document.getElementById('pipeline-command-label');
        if (label) label.textContent = `Pipeline('${store.pipeline}') · ${store.run_id} · ${store.status === 'stopped' ? 'execution finished' : store.status}`;
        if (window._mrsmPipelineSource && window._mrsmPipelineJob === store.job)
            return dash_clientside.no_update;
        if (window._mrsmPipelineSource) window._mrsmPipelineSource.close();
        window._mrsmPipelineJob = store.job;
        window.mrsmShowPipelineOutput?.(store);
        const source = new EventSource(store.stream);
        window._mrsmPipelineSource = source;
        let current = {...store}, lines = [];
        const initial = document.getElementById('pipeline-command-output');
        if (initial) { initial.textContent = ''; initial._mrsmText = null; initial._mrsmFollow = true; }
        function visible() { return window._mrsmPipelineSource === source; }
        source.onmessage = function(e) {
            if (!visible()) return;
            const line = e.data.replace(/\\n$/, '');
            lines.push(line);
            if (lines.length > 1000) lines.shift();
            const view = document.getElementById('pipeline-command-output');
            if (view) {
                const follow = view.scrollHeight - view.scrollTop - view.clientHeight < 40;
                if (window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(view, lines.join('\\n'));
                else { view.textContent = lines.join('\\n'); if (follow) view.scrollTop = view.scrollHeight; }
            }

        };
        source.addEventListener('stopped', function() {
            source.close();
            if (!visible()) return;
            current = {...current, status: 'stopped', ended: new Date().toISOString()};
            dash_clientside.set_props('pipeline-live-run', {data: current});
        });
        source.onerror = function() {
            if (source.readyState === 2) {
                current = {...current, status: 'disconnected'};
                dash_clientside.set_props('pipeline-live-run', {data: current});
            }
        };
        return dash_clientside.no_update;
    }
    """,
    Output('pipeline-run-stream-dummy', 'children'), Input('pipeline-live-run', 'data'), Input('pipeline-definition', 'data'), Input('session-store', 'data'),
)


@dash_app.callback(
    Output('pipeline-analysis-preview', 'children'),
    Output('pipeline-analysis', 'data'),
    Output('pipeline-analysis-alert', 'children'),
    Input('pipeline-analyze', 'n_clicks'), Input('pipeline-apply-tags', 'n_clicks'),
    State('pipeline-saved-name', 'value'), State('pipeline-analysis', 'data'),
    State('pipelines-location', 'search'), State('session-store', 'data'), prevent_initial_call=True,
)
def analyze_selection(analyze, apply, name, proposal, search, session):
    """Analyze is read-only; only the separately named Apply button approves edits."""
    _require_session(session)
    from meerschaum.core.Pipeline._saved import analyze_pipeline, apply_pipeline_analysis
    if not name:
        return [], None, alert_from_success_tuple((False, 'Select a registered Pipeline first.'))
    try:
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        ### Analyze lists every pipe the Pipeline reaches, and Apply edits their tags: both are for
        ### a viewer who may change each of them.
        _require_members_rights(session, name, instance, 'pipes:write')
        if dash.ctx.triggered_id == 'pipeline-apply-tags':
            if not proposal or proposal.get('definition', {}).get('name') != name:
                raise ValueError('Analyze this Pipeline before approving tags.')
            result = apply_pipeline_analysis(proposal, instance)
            dash.set_props('pipeline-apply-tags', {'disabled': True})
            return [], None, alert_from_success_tuple((result['success'], result['message']))
        proposal = analyze_pipeline(name, instance)
        reasons = {row['pipe_ref']: row.get('reason', '') for row in proposal.get('resolved', [])}
        changes = {}
        for change in proposal.get('changes', []):
            changes.setdefault(change['pipe_ref'], []).append(f"{change['action']} {change['tag']}")
        rows = [html.Tr([
            html.Td(html.Code(engine.ref_label(ref))), html.Td(reasons.get(ref, 'No longer selected')),
            html.Td('; '.join(changes.get(ref, [])) or 'No tag change'),
            html.Td(', '.join(proposal.get('shared', {}).get(ref, [])) or '—'),
        ]) for ref in sorted(set(reasons) | set(changes))]
        preview = [
            dbc.Table([html.Thead(html.Tr([html.Th('Pipe'), html.Th('Inclusion'),
                html.Th('Proposed tag edit'), html.Th('Also in Pipelines')])), html.Tbody(rows)], size='sm'),
            *[dbc.Alert(message, color='warning') for message in proposal.get('errors', [])],
        ]
        dash.set_props('pipeline-apply-tags', {'disabled': not bool(changes)})
        return preview, proposal, []
    except Exception as exc:
        return dash.no_update, dash.no_update, alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-draft', 'data'),
    Output('pipeline-draft-members', 'value'),
    Output('pipeline-draft-parents', 'value'), Output('pipeline-draft-children', 'value'),
    Output('pipeline-draft-members', 'options'),
    Output('pipeline-draft-notice', 'children'),
    Input('pipeline-definition', 'data'), Input('pipeline-draft-members', 'value'),
    Input('pipeline-draft-parents', 'value'), Input('pipeline-draft-children', 'value'),
    Input('pipeline-draft-discard', 'n_clicks'), Input('pipeline-draft-undo', 'n_clicks'),
    Input('pipeline-draft-redo', 'n_clicks'), Input('pipeline-context', 'data'),
    Input('pipeline-member-options', 'data'),
    State('pipeline-draft', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def edit_draft(definition, members, parents, children, discard, undo, redo, context, available, draft, session):
    _require_session(session)
    from copy import deepcopy
    trigger = dash.ctx.triggered_id
    def snapshot(value):
        return {key: deepcopy((value or {}).get(key, [] if key == 'targets' else {} if key in ('pipe_edits', 'new_pipes') else None))
                for key in ('targets', 'parents', 'children', 'pipe_edits', 'new_pipes')}
    baseline = snapshot(definition)
    if trigger in ('pipeline-definition', 'pipeline-draft-discard') or draft is None:
        state = {**baseline, 'undo': [], 'redo': []}
    else:
        state = deepcopy(draft)
        old = snapshot(state)
        if trigger == 'pipeline-member-options':
            ref = (available or {}).get('add')
            if ref and ref not in state['targets']:
                state.setdefault('undo', []).append(old)
                state['undo'] = state['undo'][-50:]
                state['redo'] = []
                state['targets'] = state['targets'] + [ref]
                if (available or {}).get('new'):
                    state['new_pipes'] = {**(state.get('new_pipes') or {}), **available['new']}
        elif trigger == 'pipeline-context' and (context or {}).get('action') == 'restore':
            restored = (context or {}).get('draft') or {}
            if (context or {}).get('revision') != (definition or {}).get('revision'):
                raise PreventUpdate
            state.setdefault('undo', []).append(old)
            state['redo'] = []
            state.update(snapshot(restored))
        elif trigger == 'pipeline-draft-undo' and state.get('undo'):
            state.setdefault('redo', []).append(old)
            state.update(state['undo'].pop())
        elif trigger == 'pipeline-draft-redo' and state.get('redo'):
            state.setdefault('undo', []).append(old)
            state.update(state['redo'].pop())
        else:
            new = {'targets': members or [],
                   'parents': (parents or None) if trigger == 'pipeline-draft-parents' else state.get('parents'),
                   'children': (children or None) if trigger == 'pipeline-draft-children' else state.get('children'),
                   'pipe_edits': deepcopy(state.get('pipe_edits') or {}),
                   'new_pipes': deepcopy(state.get('new_pipes') or {})}
            if trigger == 'pipeline-context':
                if not context:
                    raise PreventUpdate
                if context.get('action') == 'remove':
                    new['targets'] = [ref for ref in new['targets'] if ref != context['ref']]
                elif context.get('action') == 'edit_pipe':
                    new['pipe_edits'][context['ref']] = context['edit']
                else:
                    raise PreventUpdate
            if new != old:
                state.setdefault('undo', []).append(old)
                state['undo'] = state['undo'][-50:]
                state['redo'] = []
                state.update(new)
    dirty = snapshot(state) != baseline
    state['active'] = bool(definition) or dirty
    state['definition_id'] = (definition or {}).get('id', (definition or {}).get('name'))
    # Options and values must hydrate atomically. Retain unavailable members too.
    options = {row['value']: row for row in (available or {}).get('options', [])}
    for ref in state['targets']:
        options.setdefault(ref, {'label': engine.ref_label(ref), 'value': ref})
    changes = _draft_changes(state, definition)
    return (state if state != draft else dash.no_update,
            state['targets'], bool(state['parents']), bool(state['children']), list(options.values()),
            (f"{changes} unsaved change" + ('' if changes == 1 else 's')) if dirty else '')


def find_or_add_pipe(trigger, instance, connector, metric, location, parameters, existing, prefix,
                     viewer=None):
    """
    The member panel: `Find Pipes` lists an instance's Pipes (those `viewer` may see); `Add new
    Pipe` proposes a new Pipe, registered only when the draft is saved (Edit mode) or created
    (New pipeline), with the viewer's access.
    """
    import json
    import meerschaum as mrsm
    options = {row['value']: row for row in (existing or {}).get('options', [])}
    try:
        instance = instance or str(engine.get_instance())
        if trigger == f'{prefix}-add-register':
            if not connector or not metric:
                raise ValueError('Connector and metric keys are required.')
            values = json.loads(parameters or '{}')
            if not isinstance(values, dict):
                raise ValueError('Parameters must be a JSON object.')
            pipe = mrsm.Pipe(connector, metric, location or None, instance=instance, parameters=values)
            if pipe.get_id() is not None:
                raise ValueError(f'{pipe} already exists. Use Find Pipes.')
            ref = '|'.join((str(pipe.instance_keys), connector, metric, location or 'None'))
            options[ref] = {'label': engine.ref_label(ref) + ' (new)', 'value': ref}
            return {'options': list(options.values()), 'add': ref, 'new': {ref: values}}, []
        for pipe in engine._instance_pipes(engine.get_instance(instance, strict=True), viewer=viewer):
            ref = '|'.join((str(pipe.instance_keys), pipe.connector_keys,
                            pipe.metric_key, pipe.location_key or 'None'))
            options[ref] = {'label': engine.ref_label(ref), 'value': ref}
        return {'options': list(options.values()), 'add': None}, []
    except Exception as exc:
        return {'options': list(options.values())}, alert_from_success_tuple((False, str(exc)))


def _register_member_panel(prefix):
    @dash_app.callback(
        Output(f'{prefix}-member-options', 'data'), Output(f'{prefix}-add-alert', 'children'),
        Input(f'{prefix}-add-find', 'n_clicks'),
        Input(f'{prefix}-add-register', 'n_clicks'),
        Input(f'{prefix}-add-instance', 'value'), State(f'{prefix}-add-connector', 'value'),
        State(f'{prefix}-add-metric', 'value'), State(f'{prefix}-add-location', 'value'),
        State(f'{prefix}-add-parameters', 'value'), State(f'{prefix}-member-options', 'data'),
        State('session-store', 'data'),
    )
    def add_draft_pipe(find, register, instance, connector, metric, location, parameters,
                       existing, session):
        _require_session(session)
        return find_or_add_pipe(dash.ctx.triggered_id, instance, connector, metric, location,
                                parameters, existing, prefix, viewer=_viewer(session))
    return add_draft_pipe


add_draft_pipe = _register_member_panel('pipeline')
add_new_pipeline_pipe = _register_member_panel('pipeline-new')


@dash_app.callback(
    Output('pipeline-stage-edit-alert', 'children'),
    Input('pipeline-stage-edit', 'n_clicks', allow_optional=True),
    State('pipeline-edit-baseline', 'data', allow_optional=True),
    State('pipeline-edit-parents', 'value', allow_optional=True),
    State('pipeline-edit-children', 'value', allow_optional=True),
    State('pipeline-edit-query', 'value', allow_optional=True),
    State('pipeline-draft', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def stage_pipe_edit(clicks, baseline, parents, children, query, draft, session):
    _require_session(session)
    if not clicks or not baseline:
        raise PreventUpdate
    from copy import deepcopy
    import json
    from meerschaum.api.dash.pipeline_editing import validate_edits, raw_query, set_raw_query
    try:
        parameters = deepcopy(baseline['parameters'])
        for plural, singular, value in (('parents', 'parent', parents), ('children', 'child', children)):
            parameters.pop(singular, None)
            parameters[plural] = json.loads(value or '[]')
        original_query = raw_query(parameters)
        if query != original_query:
            set_raw_query(parameters, query or '')
        edit = {'baseline': baseline['baseline'], 'parameters': parameters}
        edits = {**((draft or {}).get('pipe_edits') or {}), baseline['ref']: edit}
        validate_edits(edits, (draft or {}).get('targets', []))
        dash.set_props('pipeline-context', {'data': {'action': 'edit_pipe', 'ref': baseline['ref'],
                                                   'edit': edit, 'token': __import__('time').time_ns()}})
        return alert_from_success_tuple((True, 'Validated and staged shared Pipe changes. Save pipeline applies them.'))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


dash_app.clientside_callback(
    """function(session) {
        const id = (session || {})['session-id'] || null;
        if (window._mrsmExecutionOutputSession === id) return dash_clientside.no_update;
        window._mrsmExecutionOutputSession = id;
        if (window._mrsmNodeOutput) {
            window._mrsmNodeOutput.forEach(item => item.source?.close());
            window._mrsmNodeOutput.clear();
        }
        if (window._mrsmPipelineSource) window._mrsmPipelineSource.close();
        window._mrsmPipelineSource = null; window._mrsmPipelineJob = null;
        window._mrsmNodeOutputSelected = null;
        document.getElementById('pipeline-output-dock')?.classList.add('pipeline-output-empty');
        window._mrsmNodeOutputSession = id; window._mrsmPipelineOutputSession = id;
        for (const key of ['pipeline-node-output', 'pipeline-command-output', 'pipeline-node-run-label']) {
            const node = document.getElementById(key); if (node) node.textContent = '';
        }
        if (document.getElementById('pipeline-output-tab')) {
            dash_clientside.set_props('pipeline-output-tab', {options:[], value:null});
            dash_clientside.set_props('pipeline-node-run', {data:null});
            dash_clientside.set_props('pipeline-node-runs', {data:[]});
            dash_clientside.set_props('pipeline-live-run', {data:null});
            dash_clientside.set_props('pipeline-selection', {data:null});
            dash_clientside.set_props('pipeline-inspector', {hidden:true, children:[]});
            window._mrsmPipelineSelectedRef = null;
        }
        return dash_clientside.no_update;
    }""",
    Output('session-store', 'clear_data'), Input('session-store', 'data'),
)


@dash_app.callback(
    Output('pipeline-edit-mode', 'data'),
    Input('pipeline-edit-toggle', 'n_clicks'), Input('pipeline-draft-discard', 'n_clicks'),
    State('pipeline-definition', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def toggle_edit_mode(edit, discard, definition, session):
    """A Pipeline is read-only until Edit; Discard (or Save) leaves Edit mode."""
    _require_session(session)
    if dash.ctx.triggered_id == 'pipeline-edit-toggle':
        if not edit or not definition:
            raise PreventUpdate
        try:
            _require_pipeline_rights(session, definition['name'], definition.get('registry'), 'pipes:write')
        except PermissionError as exc:
            dash.set_props('pipeline-edit-alert', {'children': alert_from_success_tuple((False, str(exc)))})
            raise PreventUpdate
        return True
    if not discard:
        raise PreventUpdate
    dash.set_props('pipeline-layout-draft', {'data': None})
    dash.set_props('pipeline-draft-editor', {'open': False})
    return False


dash_app.clientside_callback(
    """function(edit, draft, definition, layout) {
        // Edit mode shows its own controls; a dirty draft is kept in the browser
        // (Resume draft / Start over) and asks before the page is left.
        const page = document.getElementById('pipelines-page');
        const workspace = document.getElementById('pipeline-workspace');
        if (!page || !definition) return dash_clientside.no_update;
        page.dataset.loaded = 'true';
        // Unsaved moves stay in place when the canvas re-renders (`assets/pipelines.js`).
        if (!layout && window._mrsmLayoutDraft) document.getElementById('pipeline-canvas')?._mrsmResetLayout?.();
        window._mrsmLayoutDraft = layout ? {key: definition.registry + '|' + definition.name, layout: layout} : null;
        const key = 'mrsm-pipeline-draft:' + definition.registry + '|' + definition.name;
        const pick = value => ({targets: (value || {}).targets || [], parents: (value || {}).parents || null,
            children: (value || {}).children || null, pipe_edits: (value || {}).pipe_edits || {},
            new_pipes: (value || {}).new_pipes || {}});
        const dirty = !!edit && (JSON.stringify(pick(draft)) !== JSON.stringify(pick(definition)) || !!layout);
        for (const node of [page, workspace]) if (node) {
            node.dataset.edit = String(!!edit);
            node.dataset.dirty = String(dirty);
        }
        let stored = null;
        try { stored = JSON.parse(localStorage.getItem(key) || 'null'); } catch (_) {}
        if (edit && dirty) {
            try { localStorage.setItem(key, JSON.stringify({revision: definition.revision, draft: pick(draft), layout: layout || null})); } catch (_) {}
            stored = null;
        } else if (!edit) {
            const restore = document.getElementById('pipeline-draft-restore');
            if (restore) restore.hidden = true;
        }
        if (edit && !dirty && stored && !window._mrsmDraftOffered) {
            window._mrsmDraftOffered = key;
            const restore = document.getElementById('pipeline-draft-restore');
            if (restore) {
                restore.hidden = false;
                const resume = document.getElementById('pipeline-draft-resume');
                if (resume) resume.hidden = stored.revision !== definition.revision;
            }
        }
        return dash_clientside.no_update;
    }""",
    Output('pipeline-edit-mode-dummy', 'children'),
    Input('pipeline-edit-mode', 'data'), Input('pipeline-draft', 'data'),
    Input('pipeline-definition', 'data'), Input('pipeline-layout-draft', 'data'),
)


dash_app.clientside_callback(
    """function(resume, startOver, definition) {
        const triggered = (dash_clientside.callback_context.triggered || []).map(t => t.prop_id);
        if (!definition) return dash_clientside.no_update;
        const key = 'mrsm-pipeline-draft:' + definition.registry + '|' + definition.name;
        const restore = document.getElementById('pipeline-draft-restore');
        if (restore) restore.hidden = true;
        if (triggered.includes('pipeline-draft-resume.n_clicks') && resume) {
            let stored = null;
            try { stored = JSON.parse(localStorage.getItem(key) || 'null'); } catch (_) {}
            if (stored && stored.revision === definition.revision) {
                dash_clientside.set_props('pipeline-context', {data: {action: 'restore', draft: stored.draft,
                    revision: stored.revision, token: Date.now()}});
                if (stored.layout) dash_clientside.set_props('pipeline-layout-draft', {data: stored.layout});
            }
            return dash_clientside.no_update;
        }
        if (triggered.includes('pipeline-draft-start-over.n_clicks') && startOver) {
            try { localStorage.removeItem(key); } catch (_) {}
        }
        return dash_clientside.no_update;
    }""",
    Output('pipeline-draft-restore', 'title'),
    Input('pipeline-draft-resume', 'n_clicks'), Input('pipeline-draft-start-over', 'n_clicks'),
    State('pipeline-definition', 'data'), prevent_initial_call=True,
)


dash_app.clientside_callback(
    """function(plan, analyze, schedule, copy, rename) {
        const triggered = (dash_clientside.callback_context.triggered || []).map(t => t.prop_id.split('.')[0]);
        const open = id => triggered.includes(id);
        if (!triggered.length) return Array(5).fill(dash_clientside.no_update);
        return [
            open('pipeline-plan-open') ? true : dash_clientside.no_update,
            open('pipeline-analyze') ? true : dash_clientside.no_update,
            open('pipeline-manage-schedule') ? true : dash_clientside.no_update,
            open('pipeline-manage-copy') ? true : dash_clientside.no_update,
            open('pipeline-manage-rename') ? true : dash_clientside.no_update,
        ];
    }""",
    Output('pipeline-plan-modal', 'is_open'), Output('pipeline-analyze-modal', 'is_open'),
    Output('pipeline-schedule-modal', 'is_open'), Output('pipeline-copy-modal', 'is_open'),
    Output('pipeline-rename-modal', 'is_open'),
    Input('pipeline-plan-open', 'n_clicks'), Input('pipeline-analyze', 'n_clicks'),
    Input('pipeline-manage-schedule', 'n_clicks'), Input('pipeline-manage-copy', 'n_clicks'),
    Input('pipeline-manage-rename', 'n_clicks'), prevent_initial_call=True,
)


@dash_app.callback(
    Output('pipeline-plan-body', 'children'),
    Input('pipeline-plan-modal', 'is_open'),
    Input('pipeline-definition', 'data'), State('pipelines-location', 'search'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def render_plan(is_open, definition, search, session):
    """The plan of the saved Pipeline: dependency order, actions, readiness and bounds."""
    _require_session(session)
    if not is_open or not definition:
        raise PreventUpdate
    from meerschaum.api.dash.callbacks.pipelines import build_plan_table
    try:
        from meerschaum.api.dash.pipeline_editing import make_pipe
        graph = engine.plan_graph(_instance(search), [make_pipe(ref) for ref in definition.get('targets', [])],
                                  parents=definition.get('parents'), children=definition.get('children'))
        return [build_plan_table(graph), *[dbc.Alert(str(message), color='warning') for message in graph['warnings']]]
    except Exception as exc:
        return alert_from_success_tuple((False, f'Could not plan this Pipeline: {exc}'))


@dash_app.callback(
    Output('pipeline-copy-alert', 'children'),
    Input('pipeline-copy-confirm', 'n_clicks'),
    State('pipeline-copy-name', 'value'), State('pipeline-definition', 'data'),
    State('pipelines-location', 'search'), State('session-store', 'data'), prevent_initial_call=True,
)
def copy_selection(clicks, new_name, definition, search, session):
    _require_session(session)
    if not clicks or not definition:
        raise PreventUpdate
    from meerschaum.core.Pipeline._saved import copy_pipeline, validate_name
    try:
        validate_name(new_name or '')
        viewer = _viewer(session)
        if not viewer.can('pipes:register'):
            raise PermissionError('Your account may not register Pipelines.')
        _require_members_rights(session, definition['name'], _instance(search), 'pipes:read')
        ### The copy belongs to the viewer from its first registration (AC4).
        from meerschaum.api.dash import pipe_writes
        copy_pipeline(definition['name'], new_name, _instance(search),
                      access=pipe_writes.new_access(viewer))
        dash.set_props('pipeline-copy-modal', {'is_open': False})
        dash.set_props('pipelines-navigate', {'data': pipeline_path(new_name)})
        return []
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-rename-alert', 'children'),
    Input('pipeline-rename-confirm', 'n_clicks'),
    State('pipeline-rename-name', 'value'), State('pipeline-definition', 'data'),
    State('pipelines-location', 'search'), State('session-store', 'data'), prevent_initial_call=True,
)
def rename_selection(clicks, new_name, definition, search, session):
    _require_session(session)
    if not clicks or not definition:
        raise PreventUpdate
    from meerschaum.core.Pipeline._saved import rename_pipeline, validate_name
    try:
        validate_name(new_name or '')
        _require_pipeline_rights(session, definition['name'], _instance(search), 'pipes:write')
        rename_pipeline(definition['name'], new_name, _instance(search),
                        expected_revision=definition.get('revision'))
        dash.set_props('pipeline-rename-modal', {'is_open': False})
        dash.set_props('pipelines-navigate', {'data': pipeline_path(new_name)})
        return []
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-destructive-confirm', 'displayed'),
    Output('pipeline-destructive-confirm', 'message'),
    Output('pipeline-destructive-action', 'data'),
    Input('pipeline-manage-clear', 'n_clicks'), Input('pipeline-manage-drop', 'n_clicks'),
    State('pipeline-definition', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def confirm_destructive(clear, drop, definition, session):
    """Clear and Drop name what they touch: the explicit members' data, never registrations."""
    _require_session(session)
    action = {'pipeline-manage-clear': 'clear', 'pipeline-manage-drop': 'drop'}.get(dash.ctx.triggered_id)
    if not action or not definition or not (clear or drop):
        raise PreventUpdate
    members = definition.get('targets', [])
    verb = 'Delete the rows of' if action == 'clear' else 'Drop the tables of'
    return True, (
        f"{verb} the {len(members)} explicit member" + ('' if len(members) == 1 else 's')
        + f" of Pipeline('{definition['name']}') on {definition['registry']}? "
        + "Registrations and the Pipeline are kept."
    ), {'action': action, **{key: definition.get(key) for key in ('name', 'registry', 'revision')}}


@dash_app.callback(
    Output('pipeline-edit-alert', 'children'),
    Input('pipeline-destructive-confirm', 'submit_n_clicks'),
    Input('pipeline-edge-proposal', 'data'),
    State('pipeline-destructive-action', 'data'), State('pipeline-definition', 'data'),
    State('pipeline-draft', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def run_destructive_or_propose_edge(submit, proposal, target, definition, draft, session):
    _require_session(session)
    if dash.ctx.triggered_id == 'pipeline-edge-proposal':
        return propose_edge(proposal, draft)
    if not submit or not target or not definition:
        raise PreventUpdate
    from meerschaum.api.dash.execute import run_action
    from meerschaum.api.dash.sessions import get_username_from_session
    try:
        if target.get('revision') != definition.get('revision') or target.get('name') != definition.get('name'):
            raise ValueError('The Pipeline changed; review again.')
        ### Clear and Drop touch every member: the viewer must see and may change each one.
        if target.get('action') not in ('clear', 'drop'):
            raise ValueError('Choose Clear or Drop.')
        _require_members_rights(
            session, target['name'], target['registry'],
            'pipes:delete' if target['action'] == 'clear' else 'pipes:drop',
        )
        result = run_action([target['action'], 'pipelines', target['name'], '-i', target['registry'], '--yes'],
                            get_username_from_session((session or {}).get('session-id')))
        if not result['success']:
            return alert_from_success_tuple((False, result['msg']))
        return dbc.Alert([f"Started {target['action']} as ", html.A(result['job'], href='/dash/jobs'), '.'],
                         color='info', dismissable=True)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


def propose_edge(proposal, draft):
    """
    Dragging from one node to another proposes `parent -> child`: the child's `parents` gain the
    parent, staged in the draft and applied on Save.
    """
    if not proposal or not proposal.get('parent') or not proposal.get('child'):
        raise PreventUpdate
    from copy import deepcopy
    from meerschaum.api.dash.pipeline_editing import (
        make_pipe, fingerprint, validate_edits, dependencies,
    )
    parent, child = proposal['parent'], proposal['child']
    try:
        if parent == child:
            raise ValueError('A Pipe cannot be its own parent.')
        edits = deepcopy((draft or {}).get('pipe_edits') or {})
        pipe = make_pipe(child)
        if pipe.get_id() is None:
            raise ValueError(f'{pipe} is not registered yet; save it first.')
        actual = dict(pipe.get_parameters(apply_symlinks=False))
        edit = edits.get(child) or {'baseline': fingerprint(actual), 'parameters': deepcopy(actual)}
        parameters = edit['parameters']
        if (parent, child) in dependencies(child, parameters):
            raise ValueError(f'{engine.ref_label(parent)} is already a parent of {engine.ref_label(child)}.')
        instance, connector, metric, location = engine.split_ref(parent)
        reference = {'connector_keys': connector, 'metric_key': metric}
        if location is not None:
            reference['location_key'] = location
        if instance != str(pipe.instance_keys):
            reference['instance'] = instance
        existing = parameters.pop('parent', None)
        values = parameters.get('parents', existing if existing is not None else [])
        values = [values] if isinstance(values, (str, dict)) else list(values or [])
        parameters['parents'] = values + [reference]
        edits[child] = edit
        validate_edits(edits, (draft or {}).get('targets', []))
        dash.set_props('pipeline-context', {'data': {'action': 'edit_pipe', 'ref': child, 'edit': edit,
                                                   'token': __import__('time').time_ns()}})
        return []
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))
