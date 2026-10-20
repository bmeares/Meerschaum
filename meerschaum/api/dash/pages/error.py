#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Layout for the error page.
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
