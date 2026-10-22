"""Registered Pipeline cards for the Web Console (no graph discovery on read)."""
from urllib.parse import urlencode

from meerschaum.api.dash.components import html, dbc
from meerschaum.core.Pipeline._saved import list_pipelines


def get_pipeline_cards(state):
    instance = state.get('instance-select.value')
    definitions = list_pipelines(instance)
    from meerschaum.api.dash import engine
    registry = str(engine.get_instance(instance))
    cards = []
    try:
        from meerschaum.jobs import get_jobs
        jobs = get_jobs(executor_keys='local', include_hidden=False)
    except Exception:
        jobs = {}

    for name, definition in sorted(definitions.items()):
        targets = definition.get('targets', [])
        instances = {ref.split('|', 1)[0] for ref in targets}
        url = '/dash/pipelines?' + urlencode({'pipeline': name, 'instance': registry})
        try:
            from meerschaum.api.dash import engine
            runs = engine.get_runs(engine.get_instance(instance), limit=100, run_name=name)
            latest = next((run for run in runs if engine.run_belongs_to_pipeline(run, name, definition)), None)
            status = str((latest or {}).get('status', 'No runs recorded'))
        except Exception:
            latest, status = None, 'Run status unavailable'
        schedules = []
        for job_name, job in jobs.items():
            args = job.sysargs
            if args[:3] not in (['sync', 'pipeline', name], ['sync', 'pipelines', name]):
                continue
            job_registry = args[args.index('-i') + 1] if '-i' in args and args.index('-i') + 1 < len(args) else registry
            if job_registry == registry:
                schedules.append(job_name)
        cards.append(dbc.Card([
            dbc.CardHeader(html.A(name, href=url)),
            dbc.CardBody([
                html.P(f"Registry: {registry}"),
                html.P(f"{len(targets)} members across {len(instances)} instances"),
                html.P(f"Revision: {definition.get('revision', 'legacy')}"),
                html.P(definition.get('description', '')),
                html.P('Latest execution: ' + status),
                html.P('Membership health: run Analyze to validate references and tags.'),
                html.Div(['Schedules: '] + [html.A(label, href='/dash/jobs/' + label, className='me-2') for label in schedules])
                    if schedules else html.P('No scheduled Jobs'),
                dbc.Button('Open / Manage', href=url, size='sm', className='me-2'),
                dbc.Button('Plan', href=url + '&view=plan', size='sm', className='me-2'),
                dbc.Button('Sync', id={'type': 'sync-pipeline-card', 'name': name,
                                       'instance': registry}, size='sm', color='success'),
                html.Div(id={'type': 'sync-pipeline-card-result', 'name': name,
                             'instance': registry}),
            ]),
        ]))
    return cards, []
