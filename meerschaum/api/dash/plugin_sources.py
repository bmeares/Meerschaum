#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Dash source browser and editor of a plugin's files. The Dash-free domain logic (path and
symlink validation, bounded archive reading, authoring policy, digest-checked saves, and syntax
tokenization) is `meerschaum.api.dash._plugins._lifecycle`'s.
"""

from __future__ import annotations

import pathlib
import uuid

from meerschaum.utils.typing import Any, Optional
from meerschaum.api.dash._plugins import _lifecycle
from meerschaum.api.dash._plugins._policy import PluginRights


########################################################################################
# Dash source browser & editor components.
########################################################################################

#: The Font Awesome brand logo of each plugin language.
LANGUAGE_LOGOS = {'Python': 'fa-brands fa-python', 'Rust': 'fa-brands fa-rust'}

#: Icons of a file in the tree, by suffix: `(language, logo)`; other files show their suffix.
LANGUAGE_ICONS = {
    '.py': ('Python', LANGUAGE_LOGOS['Python']),
    '.rs': ('Rust', LANGUAGE_LOGOS['Rust']),
}


def language_logo(language: str, class_name: str = ''):
    """
    A language's logo (Font Awesome) with its name as the accessible text: `role="img"`,
    `aria-label` and `title` on the logo, and the name as visually hidden text.
    """
    from dash import html
    language = str(language or '')
    logo = LANGUAGE_LOGOS.get(language)
    if not logo:
        return html.Span(language, className=('plugin-language ' + class_name).strip())
    return html.Span(
        [
            html.I(className=logo, **{'aria-hidden': 'true'}),
            html.Span(language, className='visually-hidden'),
        ],
        className=(f'plugin-language plugin-language-{language.lower()} ' + class_name).strip(),
        title=language,
        role='img',
        **{'aria-label': language},
    )


def build_source_browser(
    kind: str,
    name: str,
    rights: PluginRights,
    selected: Optional[str] = None,
    edit: bool = False,
    notice: Any = None,
):
    """
    A plugin's source: the file tree and the chosen file (`selected`, else the entry point).
    Those who may author the plugin (`rights.can_author(name)`) can edit an installed plugin's files (`edit`
    opens `selected` for editing, as New plugin does); `notice` is shown above the files.
    """
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    if not rights.can_read_source(kind, name):
        return None
    try:
        files = _lifecycle.source_files(kind, name, rights)
    except Exception:
        return dbc.Alert('Source preview is unavailable for this plugin.', color='secondary')
    if not files:
        return html.P('No source files are available to preview.', className='text-muted')
    first = selected if selected in files else files[0]
    editing = (
        {'path': first, 'nonce': uuid.uuid4().hex}
        if edit and kind == 'installed' and rights.can_author(name) else None
    )
    return html.Section([
        html.Div(html.H4('Source'), className='plugin-source-header'),
        html.Div(notice, id='plugin-source-alert', className='plugin-source-alert'),
        dcc.Input(id='plugin-source-file', type='hidden', value=first),
        ### `edit` is `{path, nonce}` while a file is edited, else `None` (one writer:
        ### `callbacks/plugins.py::plugin_source_action`).
        dcc.Store(id='plugin-source-context', data={'kind': kind, 'name': name, 'edit': editing}),
        html.Div([
            source_tree(files, first),
            dcc.Loading(
                html.Div(
                    id='plugin-source-content',
                    className='plugin-source-content',
                    **{'aria-label': 'Plugin source'},
                ),
                type='dot',
            ),
        ], className='plugin-source-workspace'),
    ], className='plugin-source-browser')


def _file_icon(name: str):
    from dash import html
    suffix = pathlib.PurePosixPath(name).suffix.lower()
    if suffix in LANGUAGE_ICONS:
        language, _ = LANGUAGE_ICONS[suffix]
        return html.Span(language_logo(language), className='plugin-source-file-kind')
    return html.Span(suffix.lstrip('.').upper() or 'FILE', className='plugin-source-file-kind')


def source_tree(files, selected):
    from dash import html
    from meerschaum.api.dash.components import build_icon
    tree = {'folders': {}, 'files': []}
    for relative in files:
        node = tree
        parts = pathlib.PurePosixPath(relative).parts
        for part in parts[:-1]:
            node = node['folders'].setdefault(part, {'folders': {}, 'files': []})
        node['files'].append((parts[-1], relative))

    def branch(node, prefix=''):
        items = []
        for name, child in sorted(node['folders'].items()):
            path = prefix + name + '/'
            ### The folder icon opens with its `details` (`console_pages.css`).
            items.append(html.Li(html.Details([
                html.Summary([
                    build_icon('folder', 'plugin-source-folder-closed'),
                    build_icon('folder-open', 'plugin-source-folder-open'),
                    html.Span(name, className='plugin-source-folder-name'),
                ], className='plugin-source-folder'),
                branch(child, path),
            ], open=selected.startswith(path))))
        for name, path in sorted(node['files']):
            items.append(html.Li(html.Button([
                _file_icon(name),
                html.Span(name, className='plugin-source-file-name'),
            ], id={'type': 'plugin-source-select', 'path': path}, n_clicks=0, type='button',
                className='plugin-source-file' + (' active' if path == selected else ''),
                title=path, **{'aria-label': path})))
        return html.Ul(items, className='plugin-source-tree-list')

    return html.Nav(branch(tree), className='plugin-source-tree', **{'aria-label': 'Plugin files'})


def _source_action_button(action: str, label: str, icon: str, style: str):
    """Edit, Save or Cancel in a file's toolbar (`callbacks/plugins.py::plugin_source_action`)."""
    from dash import html
    return html.Button(
        [html.I(className=icon, **{'aria-hidden': 'true'}), html.Span(label)],
        type='button',
        n_clicks=0,
        id={'type': 'plugin-source-action', 'action': action},
        className=f'btn btn-sm {style} plugin-source-action plugin-source-{action}',
        title=label,
    )


def source_preview(
    path,
    source,
    digest: Optional[str] = None,
    editable: bool = False,
    editing: bool = False,
):
    """
    One file: its toolbar and its code, which the browser fills from `plugin-source-data`
    (`assets/plugin_source.js`), always as text, never as the plugin's HTML.
    """
    from dash import html, dcc
    from meerschaum.api.dash.components import build_icon
    suffix = pathlib.PurePosixPath(str(path)).suffix.lower()
    logo = (
        language_logo(LANGUAGE_ICONS[suffix][0], 'plugin-source-toolbar-language')
        if suffix in LANGUAGE_ICONS else None
    )
    lines = len(source.splitlines())
    copy = html.Button(
        [build_icon('copy', 'plugin-source-icon-copy'), build_icon('check', 'plugin-source-icon-copied')],
        type='button',
        className='btn btn-sm btn-outline-secondary plugin-source-copy',
        title='Copy source',
        **{'aria-label': 'Copy source', 'data-source-action': 'copy'},
    )
    fullscreen = html.Button(
        [
            build_icon('fullscreen', 'plugin-source-icon-enter'),
            build_icon('fullscreen-exit', 'plugin-source-icon-exit'),
        ],
        type='button',
        className='btn btn-sm btn-outline-secondary plugin-source-fullscreen-button',
        title='Fullscreen',
        **{
            'aria-label': 'Fullscreen',
            'aria-pressed': 'false',
            'data-source-action': 'fullscreen',
        },
    )
    if editing:
        data = {
            'path': path,
            'mode': _lifecycle.source_mode(path),
            'source': source,
            'digest': digest,
            'editing': True,
        }
        toolbar = [
            logo,
            html.Code(path),
            _source_action_button('save', 'Save', 'fa-solid fa-floppy-disk', 'btn-primary'),
            _source_action_button('cancel', 'Cancel', 'fa-solid fa-xmark', 'btn-outline-secondary'),
            html.Button(
                'Vim',
                type='button',
                className='btn btn-sm btn-outline-secondary plugin-source-action plugin-source-vim',
                title='Vim keybindings',
                **{'aria-pressed': 'false', 'data-source-action': 'vim'},
            ),
        ]
        code = html.Div(
            html.Div(id='plugin-source-editor', className='plugin-source-editor'),
            className='plugin-source-code plugin-source-editor-code',
        )
    else:
        data = {**_lifecycle.source_tokens(path, source), 'digest': digest, 'editing': False}
        toolbar = [logo, html.Code(path), html.Span(f'{lines:,} lines', className='text-muted')]
        if editable:
            toolbar.append(_source_action_button(
                'edit', 'Edit', 'fa-solid fa-pen-to-square', 'btn-outline-secondary',
            ))
        code = html.Div(
            html.Pre(html.Code(id='plugin-source-code', className='language-' + data['mode'])),
            className='plugin-source-code',
        )
    toolbar.extend([copy, fullscreen])
    return html.Div(
        [
            html.Div([item for item in toolbar if item is not None], className='plugin-source-toolbar'),
            code,
            dcc.Store(id='plugin-source-data', data=data),
            dcc.Store(id='plugin-source-draft', data=(source if editing else None)),
            html.Div(id='plugin-source-rendered', hidden=True),
        ],
        className='plugin-source-preview' + (' plugin-source-preview-editing' if editing else ''),
    )
