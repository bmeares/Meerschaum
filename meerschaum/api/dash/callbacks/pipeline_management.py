"""Authenticated persistence and Job controls for the Pipelines page."""
import dash
from urllib.parse import parse_qs, urlencode
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
    if not is_state_authenticated(data):
        raise PreventUpdate


@dash_app.callback(Output('pipeline-manage-collapse', 'is_open'),
                   Input('pipeline-manage-toggle', 'n_clicks'),
                   State('pipeline-manage-collapse', 'is_open'), prevent_initial_call=True)
def toggle_management(clicks, opened):
    return not opened


@dash_app.callback(
    Output('pipeline-saved-name', 'options'),
    Output('pipeline-definition', 'data'),
    Output('pipeline-name-input', 'value'),
    Input('pipelines-location', 'pathname'), Input('pipeline-saved-name', 'value'),
    Input('pipelines-location', 'search'),
    Input('pipelines-refresh-button', 'n_clicks'), State('session-store', 'data'),
    State('pipeline-definition', 'data'),
)
def read_selection(pathname, name, search, refresh, session, current=None):
    if not str(pathname).startswith('/dash/pipelines'):
        raise PreventUpdate
    _require_session(session)
    query = parse_qs((search or '').lstrip('?'))
    instance = engine.get_instance(query.get('instance', [None])[0])
    if dash.ctx.triggered_id in ('pipelines-location', None):
        requested = query.get('pipeline', [name])[0]
        if requested != name:
            name = requested
            dash.set_props('pipeline-saved-name', {'value': name})
    definitions = list_pipelines(instance)
    if dash.ctx.triggered_id == 'pipeline-saved-name':
        if name:
            query['pipeline'] = [name]
        else:
            query.pop('pipeline', None)
        query.pop('run_id', None)
        updated = ('?' + urlencode(query, doseq=True)) if query else ''
        if updated != (search or ''):
            dash.set_props('pipelines-location', {'search': updated})
    dash.set_props('pipeline-definitions', {'data': definitions})
    definition = definitions.get(name)
    unchanged = current == definition
    if not unchanged:
        dash.set_props('pipeline-analysis', {'data': None})
        dash.set_props('pipeline-analysis-preview', {'children': []})
        dash.set_props('pipeline-apply-tags', {'disabled': True})
    return ([{'label': key, 'value': key} for key in sorted(definitions)],
            dash.no_update if unchanged else definition,
            dash.no_update if unchanged else name or '')


@dash_app.callback(
    Output('pipeline-manage-alert', 'children'),
    Input('pipeline-save', 'n_clicks'), Input('pipeline-delete', 'n_clicks'),
    Input('pipeline-draft-save', 'n_clicks'),
    State('pipeline-name-input', 'value'), State('pipeline-saved-name', 'value'),
    State('pipeline-draft', 'data'), State('pipeline-definition', 'data'), State('pipelines-location', 'search'), State('session-store', 'data'),
    State('pipeline-draft-members', 'value'), State('pipeline-draft-parents', 'value'),
    State('pipeline-draft-children', 'value'),
    prevent_initial_call=True,
)
def manage_selection(save_clicks, delete_clicks, draft_save, name, selected, draft, current, search, session,
                     members=..., parents=..., children=...):
    _require_session(session)
    completed = []
    try:
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        from meerschaum.core.Pipeline._saved import _mutation_lock, validate_name
        with _mutation_lock(instance):
            validate_name(name or selected)
            if dash.ctx.triggered_id == 'pipeline-delete':
                if not selected:
                    raise ValueError('Select a saved pipeline to delete.')
                result = delete_pipeline(selected, instance)
                if result[0]:
                    dash.set_props('pipeline-saved-name', {'value': None})
            else:
                if selected and name != selected:
                    from meerschaum.core.Pipeline._saved import rename_pipeline
                    renamed = rename_pipeline(selected, name, instance,
                        expected_revision=(current or {}).get('revision'))
                    completed.append(f"Renamed Pipeline '{selected}' to '{name}'")
                    selected = name
                    current = renamed
                    dash.set_props('pipeline-saved-name', {'value': name})
                draft = dict(draft or {})
                # The form can be ahead of its asynchronous draft/history callback.
                if members is not ...:
                    draft['targets'] = members or []
                for key, value in (('parents', parents), ('children', children)):
                    if value is not ... and bool(value) != bool(draft.get(key)):
                        draft[key] = value or None
                from meerschaum.api.dash.pipeline_editing import apply_edits
                edits = draft.get('pipe_edits') or {}
                definitions = list_pipelines(instance)
                if not selected and name in definitions:
                    raise ValueError('That Pipeline name is already registered. Choose another name.')
                previous = definitions.get(selected) if selected else None
                if selected and (previous or {}).get('revision') != (current or {}).get('revision'):
                    raise ValueError('Pipeline revision changed; reload before saving.')
                results = apply_edits(edits, draft.get('targets', [])) if edits else []
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
                    expected_revision=(current or {}).get('revision'), create=not bool(selected))
                result = True, f"Saved pipeline '{name}' ({len(definition['targets'])} pipes). Run analyze pipelines to detect traversals and approve tagging untagged Pipes."
                dash.set_props('pipeline-saved-name', {'value': name})
                dash.set_props('pipeline-definition', {'data': definition})
            dash.set_props('pipelines-refresh-button', {'n_clicks': __import__('time').time_ns()})
            return alert_from_success_tuple(result)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc) + ('. Already applied: ' + '; '.join(completed) if completed else '')))


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
        return html.P('Select a saved pipeline to manage its Jobs.', className='text-muted')
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
        if trigger == 'pipeline-job-create' and create:
            if pipeline not in list_pipelines(instance):
                raise ValueError('Save the pipeline before scheduling it.')
            from meerschaum.core.Pipeline._saved import validate_name
            validate_name(name)
            if name in jobs:
                raise ValueError('That Job name already exists. Use a new name.')
            from meerschaum.utils.schedule import parse_schedule
            parse_schedule(schedule or '')
            args = job_arguments(pipeline, instance, schedule, workers, retries or 0, force)
            job = Job(name, args)
            message = alert_from_success_tuple(job.start())
            related[name] = job
        elif isinstance(trigger, dict) and any((starts or []) + (stops or [])):
            job = related.get(trigger['index'])
            if job is None:
                raise ValueError('This Job does not belong to the selected pipeline.')
            result = job.start() if trigger['type'] == 'pipeline-job-start' else job.stop()
            message = alert_from_success_tuple(result)
        rows = [html.Tr([
            html.Td(html.A(key, href='/dash/jobs', target='_blank')),
            html.Td(job.status), html.Td(html.Code(job.label)),
            html.Td([
                dbc.Button('Start', id={'type': 'pipeline-job-start', 'index': key},
                           size='sm', class_name='me-2'),
                dbc.Button('Use settings', id={'type': 'pipeline-job-copy', 'index': key},
                           size='sm', color='secondary', class_name='me-2'),
                dbc.Button('Stop', id={'type': 'pipeline-job-stop', 'index': key},
                           size='sm', color='secondary'),
            ]),
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
    Input({'type': 'pipeline-group', 'index': ALL}, 'n_clicks'),
    Input('pipeline-run-now', 'n_clicks', allow_optional=True),
    State('pipeline-saved-name', 'value'), State('pipelines-location', 'search'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def select_or_run_pipeline(groups, run_clicks, name, search, session):
    _require_session(session)
    trigger = dash.ctx.triggered_id
    if isinstance(trigger, dict):
        if not any(groups or []):
            raise PreventUpdate
        dash.set_props('pipeline-saved-name', {'value': trigger['index']})
        return []
    if not run_clicks or not name:
        raise PreventUpdate
    from meerschaum.api.dash.execute import run_action
    from meerschaum.api.dash.sessions import get_username_from_session
    from meerschaum.core.Pipeline._saved import validate_name
    try:
        validate_name(name)
        import uuid
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        definition = list_pipelines(instance).get(name)
        if not definition:
            raise ValueError('Pipeline registration is missing.')
        run_id = 'pipeline-' + uuid.uuid4().hex
        result = run_action(['sync', 'pipelines', name, '-i', str(instance),
                             '--nopretty', '--run-id', run_id],
                            get_username_from_session((session or {}).get('session-id')),
                            metadata={'run_id': run_id, 'pipeline': {
                                'name': name, 'registry': str(instance),
                                'id': definition.get('id'), 'revision': definition.get('revision')}})
        if result['success']:
            query = parse_qs((search or '').lstrip('?'))
            query.pop('run_id', None)
            dash.set_props('pipelines-location', {'search': '?' + urlencode(query, doseq=True) if query else ''})
            dash.set_props('pipeline-live-run', {'data': {
                'pipeline': name, 'registry': str(instance), 'pipeline_id': definition.get('id'),
                'run_id': run_id, 'job': result['job'], 'stream': result['stream'],
                'session_id': (session or {}).get('session-id'),
                'status': 'running',
            }})
            dash.set_props('pipelines-interval', {'interval': 2000})
        return alert_from_success_tuple((result['success'], result['msg']))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-summary', 'children'),
    Input('pipeline-definition', 'data'),
)
def pipeline_summary(definition):
    if not definition:
        return html.P('Select a pipeline’s name to inspect and run it.', className='text-muted')
    return dbc.Card(dbc.CardBody([
        html.H4(definition['name']),
        html.P(f"{len(definition.get('targets', []))} member pipes"),
        dbc.Button('Run now', id='pipeline-run-now', n_clicks=0, size='sm', class_name='me-2'),
        dbc.Button('Cancel active run', id='pipeline-cancel-current', n_clicks=0, size='sm', color='danger', outline=True),
        html.Details([html.Summary('Member pipes'), html.Ul([
            html.Li(html.Code(engine.ref_label(ref))) for ref in definition.get('targets', [])
        ])]),
    ]), class_name='mb-3')


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
            const output = document.getElementById('pipeline-command-output');
            if (output) output.textContent = '';
            return dash_clientside.no_update;
        }
        if (window._mrsmPipelineSource && window._mrsmPipelineJob === store.job)
            return dash_clientside.no_update;
        if (window._mrsmPipelineSource) window._mrsmPipelineSource.close();
        window._mrsmPipelineJob = store.job;
        document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
        const source = new EventSource(store.stream);
        window._mrsmPipelineSource = source;
        let current = {...store}, lines = [];
        const initial = document.getElementById('pipeline-command-output');
        if (initial) initial.textContent = '';
        function visible() { return window._mrsmPipelineSource === source; }
        source.onmessage = function(e) {
            if (!visible()) return;
            const line = e.data.replace(/\\n$/, '');
            lines.push(line);
            if (lines.length > 1000) lines.shift();
            const view = document.getElementById('pipeline-command-output');
            if (view) {
                const follow = view.scrollHeight - view.scrollTop - view.clientHeight < 40;
                view.textContent = lines.join('\\n');
                if (follow) view.scrollTop = view.scrollHeight;
            }

        };
        source.addEventListener('stopped', function() {
            source.close();
            if (!visible()) return;
            current = {...current, status: 'stopped'};
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
            html.Td(html.Code(ref)), html.Td(reasons.get(ref, 'No longer selected')),
            html.Td('; '.join(changes.get(ref, [])) or 'No tag change'),
            html.Td(', '.join(proposal.get('shared', {}).get(ref, [])) or '—'),
        ]) for ref in sorted(set(reasons) | set(changes))]
        preview = [
            html.P('Review detected traversal Pipes and proposed tag edits. Apply proposed tags explicitly approves these edits.'),
            dbc.Table([html.Thead(html.Tr([html.Th('Pipe / instance'), html.Th('Inclusion'),
                html.Th('Proposed tag edit'), html.Th('Also in Pipelines')])), html.Tbody(rows)], size='sm'),
            *[dbc.Alert(message, color='warning') for message in proposal.get('errors', [])],
        ]
        dash.set_props('pipeline-apply-tags', {'disabled': not bool(changes)})
        if changes:
            preview.append(dbc.Alert('Untagged Pipes were detected. Apply this proposal here, or run analyze pipelines to detect traversals and approve editing their tags.', color='info'))
        return preview, proposal, []
    except Exception as exc:
        return dash.no_update, dash.no_update, alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-draft', 'data'),
    Output('pipeline-draft-members', 'value'),
    Output('pipeline-draft-parents', 'value'), Output('pipeline-draft-children', 'value'),
    Output('pipeline-draft-notice', 'children'),
    Input('pipeline-definition', 'data'), Input('pipeline-draft-members', 'value'),
    Input('pipeline-draft-parents', 'value'), Input('pipeline-draft-children', 'value'),
    Input('pipeline-draft-discard', 'n_clicks'), Input('pipeline-draft-undo', 'n_clicks'),
    Input('pipeline-draft-redo', 'n_clicks'), Input('pipeline-context', 'data'),
    State('pipeline-draft', 'data'), State('session-store', 'data'), prevent_initial_call=True,
)
def edit_draft(definition, members, parents, children, discard, undo, redo, context, draft, session):
    _require_session(session)
    from copy import deepcopy
    trigger = dash.ctx.triggered_id
    def snapshot(value):
        return {key: deepcopy((value or {}).get(key, [] if key == 'targets' else {} if key == 'pipe_edits' else None))
                for key in ('targets', 'parents', 'children', 'pipe_edits')}
    baseline = snapshot(definition)
    if trigger in ('pipeline-definition', 'pipeline-draft-discard') or draft is None:
        state = {**baseline, 'undo': [], 'redo': []}
    else:
        state = deepcopy(draft)
        old = snapshot(state)
        if trigger == 'pipeline-draft-undo' and state.get('undo'):
            state.setdefault('redo', []).append(old)
            state.update(state['undo'].pop())
        elif trigger == 'pipeline-draft-redo' and state.get('redo'):
            state.setdefault('undo', []).append(old)
            state.update(state['redo'].pop())
        else:
            new = {'targets': members or [],
                   'parents': (parents or None) if trigger == 'pipeline-draft-parents' else state.get('parents'),
                   'children': (children or None) if trigger == 'pipeline-draft-children' else state.get('children'),
                   'pipe_edits': deepcopy(state.get('pipe_edits') or {})}
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
    return (state, state['targets'], bool(state['parents']), bool(state['children']),
            'Unsaved membership changes. Use Save pipeline to apply.' if dirty else 'Draft matches saved membership.')


@dash_app.callback(
    Output('pipeline-draft-members', 'options'), Output('pipeline-add-alert', 'children'),
    Input('pipeline-focus-options', 'data'), Input('pipeline-add-find', 'n_clicks'),
    Input('pipeline-add-register', 'n_clicks'),
    State('pipeline-add-instance', 'value'), State('pipeline-add-connector', 'value'),
    State('pipeline-add-metric', 'value'), State('pipeline-add-location', 'value'),
    State('pipeline-add-parameters', 'value'), State('pipeline-draft-members', 'options'),
    State('pipeline-draft-members', 'value'), State('session-store', 'data'),
)
def add_draft_pipe(visible, find, register, instance, connector, metric, location, parameters,
                   existing, members, session):
    _require_session(session)
    import json
    import meerschaum as mrsm
    options = {row['value']: row for row in (existing or []) + (visible or [])}
    try:
        trigger = dash.ctx.triggered_id
        if trigger in ('pipeline-add-find', 'pipeline-add-register'):
            if not instance:
                raise ValueError('Choose the target instance explicitly.')
            if trigger == 'pipeline-add-register':
                if not connector or not metric:
                    raise ValueError('Connector and metric keys are required.')
                values = json.loads(parameters or '{}')
                if not isinstance(values, dict):
                    raise ValueError('Parameters must be a JSON object.')
                pipe = mrsm.Pipe(connector, metric, location or None, instance=instance, parameters=values)
                if pipe.get_id() is not None:
                    raise ValueError('That Pipe already exists. Use Find existing Pipes.')
                result = pipe.register()
                if not result[0]:
                    raise ValueError(result[1])
                ref = '|'.join((str(pipe.instance_keys), connector, metric, location or 'None'))
                options[ref] = {'label': ref, 'value': ref}
                dash.set_props('pipeline-draft-members', {'value': list(dict.fromkeys((members or []) + [ref]))})
            else:
                for pipe in engine._instance_pipes(engine.get_instance(instance)):
                    ref = '|'.join((str(pipe.instance_keys), pipe.connector_keys,
                                    pipe.metric_key, pipe.location_key or 'None'))
                    options[ref] = {'label': ref, 'value': ref}
        return list(options.values()), []
    except Exception as exc:
        return list(options.values()), alert_from_success_tuple((False, str(exc)))


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


@dash_app.callback(
    Output('pipeline-layout-alert', 'children'),
    Input('pipeline-save-layout', 'n_clicks', allow_optional=True),
    State('pipeline-layout-draft', 'data'), State('pipeline-definition', 'data'),
    State('pipeline-draft', 'data'),
    State('pipelines-location', 'search'), State('session-store', 'data'), prevent_initial_call=True,
)
def save_shared_layout(clicks, layout, definition, draft, search, session):
    _require_session(session)
    if not clicks:
        raise PreventUpdate
    try:
        if not definition:
            raise ValueError('Select a registered Pipeline before saving its layout.')
        if draft and (any(draft.get(key) != definition.get(key) for key in ('targets', 'parents', 'children')) or draft.get('pipe_edits')):
            raise ValueError('Save or discard membership and shared Pipe edits before saving the layout.')
        if not layout:
            raise ValueError('Alt-drag a Pipe to arrange the layout first.')
        clean = {}
        for key, position in layout.items():
            x, y = float(position['x']), float(position['y'])
            import math
            if not math.isfinite(x) or not math.isfinite(y) or max(abs(x), abs(y)) > 100000:
                raise ValueError('Layout coordinates are outside the supported range.')
            clean[key] = {'x': x, 'y': y}
        instance = engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        saved = save_pipeline(definition['name'], definition['targets'], instance,
            parents=definition.get('parents'), children=definition.get('children'),
            expected_revision=definition['revision'], layout=clean)
        dash.set_props('pipeline-definition', {'data': saved})
        return alert_from_success_tuple((True, 'Shared layout saved. Your zoom and pan remain personal.'))
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
