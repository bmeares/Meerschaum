#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The plugins of this server (`/dash/plugins`): the Installed tab (plugins installed on the server)
and the Repository tab (`?tab=repository`, the plugins published to this API), and one published
plugin's page (`/dash/plugins/{name}`).
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.jobs import build_job_logs_modal

#: How often (ms) the page checks a running plugin action.
PLUGIN_RUN_CHECK_MS: int = 2000

search_box = dbc.Input(
    id="search-plugins-input",
    placeholder="Search plugins",
    type="search",
    debounce=True,
    autoComplete='off',
)

layout = [
    build_pages_navbar(),
    dcc.Location(id='plugins-location'),
    dbc.Container([
        html.Div(html.H3('Plugins'), id='plugins-header', className='console-page-header'),
        html.Div(
            dbc.Tabs(
                [
                    dbc.Tab(label='Installed', tab_id='installed'),
                    dbc.Tab(label='Repository', tab_id='repository'),
                ],
                id='plugins-tabs',
                active_tab='installed',
            ),
            id='plugins-tabs-div',
            className='plugins-tabs',
        ),
        html.Div(id='plugins-alert-div'),
        html.Div(id='plugins-installed-div'),
        html.Div(
            [
                html.Div(search_box, id='plugins-search-wrapper', className='console-search'),
                dbc.Pagination(
                    id='plugins-pagination',
                    max_value=1,
                    active_page=1,
                    first_last=True,
                    previous_next=True,
                    fully_expanded=False,
                    style={'justify-content': 'center', 'display': 'none'},
                ),
                html.Div([], id='plugins-content-div'),
            ],
            id='plugins-repository-div',
        ),
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle(id='plugins-uninstall-title')),
                dbc.ModalFooter([
                    dbc.Button('Cancel', id='plugins-uninstall-cancel', color='secondary'),
                    dbc.Button('Uninstall', id='plugins-uninstall-confirm', color='danger'),
                ]),
            ],
            id='plugins-uninstall-modal',
            is_open=False,
        ),
        dcc.Store(id='plugins-uninstall-target', data=None),
        dcc.Store(id='plugins-refresh', data=0),
        dcc.Store(id='plugins-run', data=None),
        dcc.Interval(id='plugins-run-interval', interval=PLUGIN_RUN_CHECK_MS, disabled=True),
        build_job_logs_modal(),
    ], class_name='console-page plugins-page'),
]
