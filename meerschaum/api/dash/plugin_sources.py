#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin source inspection and Dash source-browser/editor UI.
The Dash-free domain logic (path/symlink validation, bounded archive reading,
authoring policy, digest-checked CAS saves, and syntax tokenization) lives in
`meerschaum.api.dash._plugins`.
"""

from __future__ import annotations

import os  # noqa: F401 (patched via `source.os` in fallback tests)
import pathlib
import tarfile  # noqa: F401 (patched via `source.tarfile` in archive tests)
import uuid

from meerschaum.utils.typing import Any, List, Optional, Tuple
from meerschaum.api.dash._plugins import _fs, _lifecycle, _policy

MAX_FILE_BYTES = _fs.MAX_FILE_BYTES
MAX_ARCHIVE_BYTES = _fs.MAX_ARCHIVE_BYTES
MAX_SCAN_BYTES = _fs.MAX_SCAN_BYTES
MAX_FILES = _fs.MAX_FILES
MAX_ENTRIES = _fs.MAX_ENTRIES
MAX_PATH_BYTES = _fs.MAX_PATH_BYTES
MAX_PATH_PARTS = _fs.MAX_PATH_PARTS
TEXT_SUFFIXES = _fs.TEXT_SUFFIXES
SKIP_PARTS = _fs.SKIP_PARTS

AUTHORING_OFF = _policy.AUTHORING_OFF
AUTHORING_ADMINS = _policy.AUTHORING_ADMINS

SourceChanged = _fs.SourceChanged
source_digest = _fs.source_digest
_read_file = _fs.read_file_no_follow
_replace_checked_path = _fs.replace_checked_path

may_read_source = _policy.may_read_source
authoring_refusal = _policy.authoring_refusal
may_author = _policy.may_author

SOURCE_MODES = _lifecycle.SOURCE_MODES
TOKEN_STYLES = _lifecycle.TOKEN_STYLES
MAX_TOKEN_RUNS = _lifecycle.MAX_TOKEN_RUNS
source_mode = _lifecycle.source_mode


def source_files(kind: str, name: str, session: Any) -> List[str]:
    return _lifecycle.source_files(
        kind,
        name,
        session,
        max_scan_bytes=MAX_SCAN_BYTES,
        max_file_bytes=MAX_FILE_BYTES,
        max_files=MAX_FILES,
        max_entries=MAX_ENTRIES,
        max_archive_bytes=MAX_ARCHIVE_BYTES,
    )


def read_source_with_digest(kind: str, name: str, selected: str, session: Any) -> Tuple[str, str]:
    """A source file's text and the digest of its bytes (what a save is checked against)."""
    return _lifecycle.read_source_with_digest(
        kind,
        name,
        selected,
        session,
        max_scan_bytes=MAX_SCAN_BYTES,
        max_file_bytes=MAX_FILE_BYTES,
        max_files=MAX_FILES,
        max_entries=MAX_ENTRIES,
        max_archive_bytes=MAX_ARCHIVE_BYTES,
    )


def read_source(kind: str, name: str, selected: str, session: Any) -> str:
    return read_source_with_digest(kind, name, selected, session)[0]


def save_source(
    name: str,
    selected: str,
    text: Any,
    expected: Any,
    session: Any,
) -> Tuple[bool, str, Optional[str]]:
    """Save `text` as an installed plugin's listed file `selected`, checked against `expected`."""
    return _lifecycle.save_source(
        name,
        selected,
        text,
        expected,
        session,
        max_file_bytes=MAX_FILE_BYTES,
        max_files=MAX_FILES,
        max_entries=MAX_ENTRIES,
    )


def source_tokens(path: str, source: str) -> dict:
    """Highlighted source runs for `assets/plugin_source.js`."""
    return _lifecycle.source_tokens(path, source, max_token_runs=MAX_TOKEN_RUNS)


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
    session: Any,
    selected: Optional[str] = None,
    edit: bool = False,
    notice: Any = None,
):
    """
    A plugin's source: the file tree and the chosen file (`selected`, else the entry point).
    Those who may author the plugin (`may_author`) can edit an installed plugin's files (`edit`
    opens `selected` for editing, as New plugin does); `notice` is shown above the files.
    """
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    if not may_read_source(kind, session, name):
        return None
    try:
        files = source_files(kind, name, session)
    except Exception:
        return dbc.Alert('Source preview is unavailable for this plugin.', color='secondary')
    if not files:
        return html.P('No source files are available to preview.', className='text-muted')
    first = selected if selected in files else files[0]
    editing = (
        {'path': first, 'nonce': uuid.uuid4().hex}
        if edit and kind == 'installed' and may_author(session, name) else None
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
            'mode': source_mode(path),
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
        data = {**source_tokens(path, source), 'digest': digest, 'editing': False}
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
