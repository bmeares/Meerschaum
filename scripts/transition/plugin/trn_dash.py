# A v4-style plugin that owns a Dash page and callback (legacy mode of the migration guide).
__version__ = '1.0.0'
required = []

from meerschaum.plugins import dash_plugin, web_page


@dash_plugin
def init_dash(dash_app):
    from dash import html, Input, Output

    @web_page('counts', login_required=False, page_group='Trn')
    def counts_page():
        return html.Div([
            html.H2('Trn counts', id='trn-title'),
            html.Button('Click', id='trn-btn'),
            html.Div(id='trn-out'),
        ])

    @dash_app.callback(
        Output('trn-out', 'children'),
        Input('trn-btn', 'n_clicks'),
        prevent_initial_call=True,
    )
    def _clicked(n_clicks):
        return f"clicked {n_clicks}"
