"""Fast registered Pipeline summaries; fetch execution history only when expanded."""
from urllib.parse import urlencode, quote
import json
from meerschaum.api.dash.components import html, dbc
from meerschaum.core.Pipeline._saved import list_pipelines
from meerschaum.api.dash.pipeline_management import count_label


def get_pipeline_cards(state):
    from meerschaum.api.dash import engine
    instance = state.get('instance-select.value')
    connector = engine.get_instance(instance)
    registry = str(connector)
    cards = []
    for name, definition in sorted(list_pipelines(connector).items()):
        targets = definition.get('targets', [])
        identity = json.dumps({'name': name, 'instance': registry}, sort_keys=True)
        url = '/dash/pipelines/' + quote(name, safe='') + '?' + urlencode({'instance': registry})
        from meerschaum.config import get_config
        label = f'Pipeline({name!r}' + (f', instance={registry!r}' if registry != get_config('meerschaum', 'instance') else '') + ')'
        cards.append(dbc.Card([
            dbc.CardHeader(html.A(html.Code(label), href=url)),
            dbc.CardBody([
                html.P(f"{count_label(len(targets), 'member')} · "
                       f"{count_label(len({ref.split('|', 1)[0] for ref in targets}), 'instance')}"),
                html.P(definition.get('description', '')),
                dbc.Accordion([dbc.AccordionItem(
                    html.Div(id={'type': 'pipeline-card-details', 'index': identity}),
                    title='Execution history', item_id='history')],
                    id={'type': 'pipeline-card-accordion', 'index': identity}, active_item=None),
            ]),
            dbc.CardFooter(dbc.DropdownMenu([
                dbc.DropdownMenuItem('Open', href=url),
                *[dbc.DropdownMenuItem(label, id={'type': 'manage-pipeline-card', 'name': name,
                    'instance': registry, 'action': action}) for action, label in
                  [('sync', 'Sync'), ('show', 'Show'), ('analyze', 'Analyze'), ('edit', 'Edit')]]],
                label='Manage', direction='up', color='secondary', size='sm', menu_variant='dark')),
        ]))
    return cards, []
