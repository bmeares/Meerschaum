#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Layouts for the error page and for a console page the viewer may not use.
"""

from meerschaum.api import endpoints, CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

layout = dbc.Container([
    dcc.Markdown(f"""
    ## Page not found
    You might have requested an invalid resource.
    Head back to the [dashboard home page]({endpoints['dash']}) and try navigating from there.
    """),
], className='jumbotron')


def not_available(pathname=None, href=None):
    """A console page the viewer's permissions do not include: a short title and the Overview."""
    from meerschaum.api.dash.components import build_pages_navbar
    return [
        build_pages_navbar(),
        dbc.Container(
            html.Div(
                [
                    html.H3('This page is not available to your account', id='page-not-available'),
                    dbc.Button('Overview', href=endpoints['dash'], color='primary'),
                ],
                className='console-empty',
            ),
            class_name='console-page',
        ),
    ]
