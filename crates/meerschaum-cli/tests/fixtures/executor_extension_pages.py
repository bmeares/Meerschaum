"""Exact selected page/callback payload for native HTTP and Firefox acceptance.

Appended to the owning extension source archive only; never registered in Main's Python app.
"""

from meerschaum.plugins import web_page, dash_plugin


@web_page('extension-html')
def extension_html():
    # Every URL is a fixture sentinel. The browser gate must observe zero attempted
    # requests/navigation, including links, meta refresh and iframe self-navigation.
    return '''<h1 id="html-proof">Confined HTML page</h1>
<script>location='https://extension-denied.invalid/script';parent.location='https://extension-denied.invalid/parent'</script>
<meta http-equiv="refresh" content="0;url=https://extension-denied.invalid/refresh">
<base href="https://extension-denied.invalid/base/">
<a id="navigation-proof" href="https://extension-denied.invalid/link" ping="https://extension-denied.invalid/ping">Inert link</a>
<form action="https://extension-denied.invalid/form"><input name="secret"><button>Send</button></form>
<iframe src="https://extension-denied.invalid/frame"></iframe>
<img src="https://extension-denied.invalid/image" onerror="location='https://extension-denied.invalid/event'">
<svg><a xlink:href="https://extension-denied.invalid/svg">SVG navigation</a></svg>
<p style="background-image:url(https://extension-denied.invalid/style)">Inert paragraph</p>
<figure id="html-plot" data-mrsm-figure='{"data":[{"type":"bar","x":["alpha","beta"],"y":[1,3]}],"layout":{"title":{"text":"Confined HTML figure"}}}'></figure>'''


@web_page('extension-dash')
def extension_dash():
    from dash import html, dcc
    import plotly.graph_objects as go
    import numpy as np
    return html.Div([
        html.H1('Confined Dash page', id='dash-proof'),
        dcc.Input(id='extension-text', value='first', type='text'),
        html.Button('Update', id='extension-button', n_clicks=0),
        html.Div('waiting', id='extension-output'),
        # This uint32 data yields base64 containing '//', which must remain numeric data.
        dcc.Graph(id='extension-graph', figure=go.Figure(data=[go.Bar(
            x=np.array([0, 1, 2]), y=np.array([65535, 1, 2], dtype=np.uint32),
        )])),
    ])


@web_page('extension-active-figure')
def extension_active_figure():
    from dash import dcc
    return dcc.Graph(figure={
        'data': [{'type': 'scatter', 'x': [1], 'y': [1],
                  'text': ['<a href="https://extension-denied.invalid/plotly-link">active</a>']}],
        'layout': {'title': {'text': 'Active figure must be refused'}},
    })


@web_page('extension-entity-figure')
def extension_entity_figure():
    from dash import dcc
    return dcc.Graph(figure={
        'data': [{'type': 'scatter', 'x': [1], 'y': [1],
                  'text': ['&#60;a href=&#34;&#104;&#116;&#116;&#112;&#115;&#58;&#47;&#47;extension-denied.invalid/encoded&#34;&#62;active&#60;/a&#62;',
                           '&#60a href=/encoded-without-semicolon>active', '&lta href=/legacy-named-entity>active']}],
        'layout': {'title': {'text': 'Encoded HTML figure must be refused'}},
    })


@web_page('extension-custom-js')
def extension_custom_js():
    from dash import html
    return html.Script('location="https://extension-denied.invalid/custom-js"')


@dash_plugin
def extension_callbacks(app):
    from dash import Input, Output, State

    @app.callback(
        Output('extension-output', 'children'), Output('extension-graph', 'figure'),
        Input('extension-button', 'n_clicks'), State('extension-text', 'value'),
    )
    def update(clicks, text):
        import os
        assert os.environ.get('MRSM_API_EXECUTION') == '1'
        assert 'AWS_SECRET_ACCESS_KEY' not in os.environ
        assert 'DB_PASSWORD' not in os.environ
        return (
            f'Private callback {clicks}: {text}',
            {'data': [{'type': 'bar', 'x': [str(text)], 'y': [int(clicks or 0) + 1]}]},
        )


@web_page('extension-command-figure')
def extension_command_figure():
    from dash import dcc
    return dcc.Graph(figure={
        'data': [{'type': 'scatter', 'x': [1], 'y': [1]}],
        'layout': {'updatemenus': [{'buttons': [{
            'label': 'Resource update must be refused before a click',
            'method': 'relayout', 'args': [{'images[0].source': '/extension-denied-local'}],
        }]}]},
    })
