#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The plugins of this server (`/dash/plugins`): the Installed tab (plugins installed on the server)
and the Repository tab (`?tab=repository`, the plugins published to this API), one published
plugin's page (`/dash/plugins/{name}`) and one installed plugin's page
(`/dash/plugins/installed/{name}`, `?file=...&edit=1` opens a file for editing). New plugin (the
circled plus, for the administrators who may use the web terminal) asks `bootstrap plugins`'s
questions in a dialog.
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


def _field(label: str, control, field_id: str, hidden: bool = False, label_for: str = None):
    return html.Div(
        [dbc.Label(label, html_for=label_for, class_name='plugin-new-label'), control],
        id=field_id, className='plugin-new-field', hidden=hidden,
    )


#: New plugin: `bootstrap plugins`'s questions (`callbacks/plugins.py` fills the choices).
new_plugin_modal = dbc.Modal(
    [
        dbc.ModalHeader(dbc.ModalTitle('New plugin')),
        dbc.ModalBody([
            _field('Name', dbc.Input(
                id='plugins-new-name', placeholder='my_plugin', type='text', autoComplete='off',
                maxLength=128,
            ), 'plugins-new-name-field', label_for='plugins-new-name'),
            _field('Language', dbc.RadioItems(
                id='plugins-new-language', options=[], value='Python', inline=True,
                class_name='plugin-new-languages',
            ), 'plugins-new-language-field'),
            _field('Packaging', dbc.RadioItems(
                id='plugins-new-packaging', options=[], value='file',
            ), 'plugins-new-packaging-field'),
            _field('Features', dbc.Checklist(
                id='plugins-new-features', options=[], value=['fetch'],
                class_name='plugin-new-features',
            ), 'plugins-new-features-field'),
            _field('Action', dbc.Input(
                id='plugins-new-action', placeholder='sing song', type='text', autoComplete='off',
                maxLength=128,
            ), 'plugins-new-action-field', hidden=True, label_for='plugins-new-action'),
            _field('Datetime column', dbc.Input(
                id='plugins-new-dt', placeholder='timestamp', type='text', autoComplete='off',
                maxLength=128,
            ), 'plugins-new-dt-field', label_for='plugins-new-dt'),
            _field('Directory', dbc.Select(
                id='plugins-new-directory', options=[],
            ), 'plugins-new-directory-field', hidden=True, label_for='plugins-new-directory'),
            html.Div(id='plugins-new-alert'),
        ], class_name='plugin-new-body'),
        dbc.ModalFooter([
            dbc.Button('Cancel', id='plugins-new-cancel', color='secondary', n_clicks=0),
            dbc.Button('Create', id='plugins-new-create', color='primary', n_clicks=0),
        ]),
    ],
    id='plugins-new-modal',
    is_open=False,
    class_name='plugin-new-modal',
)

layout = [
    build_pages_navbar(),
    dcc.Location(id='plugins-location'),
    dbc.Container([
        html.Div(
            [html.H3('Plugins'), html.Div(id='plugins-header-actions', className='plugins-header-actions')],
            id='plugins-header', className='console-page-header',
        ),
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
        new_plugin_modal,
        ### Where New plugin goes once the plugin is written (one writer: `create_plugin`).
        dcc.Store(id='plugins-navigate', data=None),
        dcc.Store(id='plugins-uninstall-target', data=None),
        dcc.Store(id='plugins-refresh', data=0),
        dcc.Store(id='plugins-run', data=None),
        dcc.Interval(id='plugins-run-interval', interval=PLUGIN_RUN_CHECK_MS, disabled=True),
        build_job_logs_modal(),
    ], class_name='console-page plugins-page'),
]
