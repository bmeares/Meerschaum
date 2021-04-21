#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Layout for the error page.
"""

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from meerschaum.api import endpoints

layout = dbc.Container([
    dcc.Markdown(f"""
    ## Error encountered
    You might have requested an invalid resource.
    Head back to the [dashboard home page]({endpoints['dash']}) and try navigating from there.
    """),
], className='jumbotron')
