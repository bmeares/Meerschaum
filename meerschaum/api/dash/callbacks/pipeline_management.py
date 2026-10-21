"""Authenticated persistence and Job controls for the Pipelines page."""
import dash
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
    Input('pipelines-refresh-button', 'n_clicks'), State('session-store', 'data'),
)
def read_selection(pathname, name, refresh, session):
    if not str(pathname).startswith('/dash/pipelines'):
        raise PreventUpdate
    _require_session(session)
    instance = engine.get_instance()
    definitions = initialize_pipelines(lambda: engine.plan_graph(instance), instance)
    if dash.ctx.triggered_id == 'pipeline-saved-name':
        dash.set_props('pipelines-location', {'search': ''})
    dash.set_props('pipeline-definitions', {'data': definitions})
    return ([{'label': key, 'value': key} for key in sorted(definitions)],
            definitions.get(name), name or '')


@dash_app.callback(
    Output('pipeline-manage-alert', 'children'),
    Input('pipeline-save', 'n_clicks'), Input('pipeline-delete', 'n_clicks'),
    State('pipeline-name-input', 'value'), State('pipeline-saved-name', 'value'),
    State('pipeline-visible-refs', 'data'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def manage_selection(save_clicks, delete_clicks, name, selected, refs, session):
    _require_session(session)
    try:
        instance = engine.get_instance()
        if dash.ctx.triggered_id == 'pipeline-delete':
            if not selected:
                raise ValueError('Select a saved pipeline to delete.')
            result = delete_pipeline(selected, instance)
            if result[0]:
                dash.set_props('pipeline-saved-name', {'value': None})
        else:
            definition = save_pipeline(name, refs or [], instance)
            result = True, f"Saved pipeline '{name}' ({len(definition['targets'])} pipes)."
            dash.set_props('pipeline-saved-name', {'value': name})
            dash.set_props('pipeline-definition', {'data': definition})
        dash.set_props('pipelines-refresh-button', {'n_clicks': __import__('time').time_ns()})
        return alert_from_success_tuple(result)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


@dash_app.callback(
    Output('pipeline-jobs', 'children'),
    Input('pipeline-saved-name', 'value'), Input('pipelines-interval', 'n_intervals'),
    Input('pipeline-job-create', 'n_clicks'),
    Input({'type': 'pipeline-job-start', 'index': ALL}, 'n_clicks'),
    Input({'type': 'pipeline-job-stop', 'index': ALL}, 'n_clicks'),
    State('pipeline-job-name', 'value'), State('pipeline-job-schedule', 'value'),
    State('pipeline-job-workers', 'value'), State('pipeline-job-retries', 'value'),
    State('pipeline-job-force', 'value'), State('session-store', 'data'),
)
def manage_jobs(pipeline, ticks, create, starts, stops, name, schedule, workers, retries, force, session):
    _require_session(session)
    if not pipeline:
        return html.P('Select a saved pipeline to manage its Jobs.', className='text-muted')
    from meerschaum.jobs import Job, get_jobs
    message = None
    try:
        instance = engine.get_instance()
        jobs = get_jobs()
        def belongs(job):
            args = job.sysargs
            return (args[:3] == ['sync', 'pipeline', pipeline] and '-i' in args
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
    State('pipeline-saved-name', 'value'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def copy_job_settings(clicks, pipeline, session):
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
    instance = engine.get_instance()
    if (args[:3] != ['sync', 'pipeline', pipeline] or '-i' not in args
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
    State('pipeline-saved-name', 'value'), State('session-store', 'data'),
    prevent_initial_call=True,
)
def select_or_run_pipeline(groups, run_clicks, name, session):
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
        result = run_action(['sync', 'pipeline', name, '-i', str(engine.get_instance()),
                             '--nopretty'],
                            get_username_from_session((session or {}).get('session-id')))
        if result['success']:
            dash.set_props('pipelines-location', {'search': ''})
            dash.set_props('pipeline-live-run', {'data': {
                'pipeline': name, 'job': result['job'], 'stream': result['stream'],
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
        dbc.Button('Run now', id='pipeline-run-now', n_clicks=0, size='sm'),
        html.Details([html.Summary('Member pipes'), html.Ul([
            html.Li(html.Code(engine.ref_label(ref))) for ref in definition.get('targets', [])
        ])]),
    ]), class_name='mb-3')


dash_app.clientside_callback(
    """
    function(store, definition) {
        const name = definition && definition.name;
        if (!store || !store.stream || store.pipeline !== name) {
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
            const match = line.match(/(?:^|\\|\\s*)Run: ([A-Za-z0-9_.:-]+)/);
            if (match && current.run_id !== match[1]) {
                current = {...current, run_id: match[1]};
                dash_clientside.set_props('pipeline-live-run', {data: current});
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
    Output('pipeline-run-stream-dummy', 'children'), Input('pipeline-live-run', 'data'), Input('pipeline-definition', 'data'),
)
