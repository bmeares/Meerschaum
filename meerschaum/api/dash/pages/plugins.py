#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The plugins of this server (`/dash/plugins`): the Installed tab (plugins installed on the server)
and the Repository tab (`?tab=repository`, the plugins published to this API), one published
plugin's page (`/dash/plugins/{name}`) and one installed plugin's page
(`/dash/plugins/installed/{name}`, `?file=...&edit=1` opens a file for editing). New plugin (the
circled plus, for those who may author plugins: `plugin_sources.authoring_refusal`) asks
`bootstrap plugins`'s questions in a dialog (Start from a template), or takes the plugin's files
(Upload files: `plugin_upload`).
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.jobs import build_job_logs_modal
from meerschaum.api.dash.plugin_upload import upload_limit_bytes, size_label

#: The two ways New plugin adds a plugin.
NEW_PLUGIN_MODES = (('template', 'Start from a template'), ('upload', 'Upload files'))

#: What the upload's drop zone says until files are chosen.
UPLOAD_PROMPT: str = 'Choose or drop files'

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


def _upload_control():
    """
    The files of an uploaded plugin: one `.py` or `.rs` file, one archive, or a package's files
    (`plugin_upload.plan_upload`). The zone names what was chosen (`callbacks/plugins.py`).
    """
    return dcc.Upload(
        html.Div(
            [
                html.I(className='fa-solid fa-arrow-up-from-bracket', **{'aria-hidden': 'true'}),
                html.Span(UPLOAD_PROMPT, id='plugins-upload-chosen', className='plugin-upload-chosen'),
            ],
            className='plugin-upload-zone',
        ),
        id='plugins-upload-files',
        multiple=True,
        max_size=upload_limit_bytes(),
        className='plugin-upload',
        className_active='plugin-upload plugin-upload-active',
        style={},
        style_active={},
        style_reject={},
    )


#: New plugin: `bootstrap plugins`'s questions (`callbacks/plugins.py` fills the choices), or the
#: plugin's own files. Name and Directory serve both.
new_plugin_modal = dbc.Modal(
    [
        dbc.ModalHeader(dbc.ModalTitle('New plugin')),
        dbc.ModalBody([
            dbc.RadioItems(
                id='plugins-new-mode',
                options=[{'label': label, 'value': value} for value, label in NEW_PLUGIN_MODES],
                value='template',
                inline=True,
                class_name='plugin-new-mode btn-group',
                input_class_name='btn-check',
                label_class_name='btn btn-outline-secondary',
                label_checked_class_name='active',
            ),
            html.Div(
                _field(
                    f"Files (.py, .rs, .zip or .tar.gz, up to {size_label(upload_limit_bytes())})",
                    _upload_control(), 'plugins-upload-files-field',
                ),
                id='plugins-new-upload-fields', hidden=True,
            ),
            _field('Name', dbc.Input(
                id='plugins-new-name', placeholder='my_plugin', type='text', autoComplete='off',
                maxLength=128,
            ), 'plugins-new-name-field', label_for='plugins-new-name'),
            html.Div([
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
            ], id='plugins-new-template-fields'),
            _field('Directory', dbc.Select(
                id='plugins-new-directory', options=[],
            ), 'plugins-new-directory-field', hidden=True, label_for='plugins-new-directory'),
            html.Div(
                dbc.Checkbox(id='plugins-upload-replace', label='Replace existing', value=False),
                id='plugins-upload-replace-field', className='plugin-new-field', hidden=True,
            ),
            html.Div(id='plugins-new-alert'),
        ], class_name='plugin-new-body'),
        dbc.ModalFooter([
            dbc.Button('Cancel', id='plugins-new-cancel', color='secondary', n_clicks=0),
            dbc.Button('Create', id='plugins-new-create', color='primary', n_clicks=0),
            dbc.Button('Upload', id='plugins-new-upload', color='primary', n_clicks=0,
                       style={'display': 'none'}),
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
        ### Where New plugin goes once the plugin is written (`create_plugin`, `upload_plugin`).
        dcc.Store(id='plugins-navigate', data=None),
        dcc.Store(id='plugins-uninstall-target', data=None),
        dcc.Store(id='plugins-refresh', data=0),
        dcc.Store(id='plugins-run', data=None),
        dcc.Interval(id='plugins-run-interval', interval=PLUGIN_RUN_CHECK_MS, disabled=True),
        build_job_logs_modal(),
    ], class_name='console-page plugins-page'),
]
