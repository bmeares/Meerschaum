#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Plugins page views (Dash components for the Installed tab, Repository tab,
and single-plugin detail pages). Dash-free domain policy, discovery, source editing,
uploading, template creation, version checking, and team ownership reconciliation live
under `meerschaum.api.dash._plugins`.
"""

from __future__ import annotations

from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import debug, get_api_connector, endpoints, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.api.dash.components import build_cards_grid, build_icon, alert_from_success_tuple
from meerschaum.api.dash.plugin_sources import build_source_browser, language_logo
from meerschaum.api.dash.plugin_publish import build_publish_dialog
from meerschaum.api.dash._plugins import _fs, _lifecycle
from meerschaum.api.dash._plugins._policy import PluginRights, is_plugin_owner

PLUGINS_PER_PAGE: int = 12
PLUGINS_GRID_COLUMNS: int = 3


########################################################################################
# Installed plugins UI.
########################################################################################

def build_installed_plugins_table(
    plugins: List[Dict[str, Any]],
    admin: bool = False,
    manageable: Optional[Any] = None,
) -> Any:
    """
    The Installed tab's table. A plugin's page link and its Update and Uninstall are shown for
    every plugin to administrators (`admin`), else for the plugins named in `manageable`.
    """
    manageable = {plugin['name'] for plugin in plugins} if admin else set(manageable or [])
    admin = bool(manageable)
    header = [
        html.Th('Plugin'),
        html.Th('Version', className='d-none d-sm-table-cell'),
        html.Th('Language', className='d-none d-md-table-cell'),
        html.Th('Provides'),
        html.Th('Packages', className='d-none d-md-table-cell'),
    ]
    if admin:
        header.append(html.Th(html.Span('Actions', className='visually-hidden')))
    rows = []
    for plugin in plugins:
        name = plugin['name']
        managed = name in manageable
        packages_text, packages_missing = _lifecycle.packages_label(plugin)
        provides = [
            dbc.Badge(item, color='secondary', class_name='me-1')
            for item in plugin['provides']
        ]
        if plugin.get('import_error'):
            provides.append(dbc.Badge(
                'Failed to load',
                color='danger',
                class_name='me-1',
                title=str(plugin['import_error'])[:500],
            ))
        elif plugin.get('not_loaded'):
            provides.append(dbc.Badge(
                'Not loaded',
                color='warning',
                text_color='dark',
                class_name='me-1',
            ))
        cells = [
            html.Td([
                (dcc.Link(
                    name,
                    href='/dash/plugins/installed/' + name,
                    className='console-name-link fw-semibold',
                ) if managed else html.Span(name, className='fw-semibold')),
                html.Div(
                    [
                        plugin['version'] or '',
                        ' · ' if plugin['version'] else '',
                        language_logo(plugin['kind']),
                    ],
                    className='text-muted small d-sm-none plugin-mobile-meta',
                ),
            ]),
            html.Td(
                html.Span(plugin['version'] or '—', className='plugin-version-badge'),
                className='d-none d-sm-table-cell',
            ),
            html.Td(language_logo(plugin['kind']), className='plugin-kind d-none d-md-table-cell'),
            html.Td(provides or html.Span('—', className='text-muted'), className='plugin-provides'),
            html.Td(
                html.Span(
                    packages_text,
                    className=('plugin-packages-missing' if packages_missing else 'text-muted'),
                    title=(
                        ', '.join(plugin['missing'])
                        if packages_missing
                        else ', '.join(plugin['required'])
                    ),
                ),
                className='d-none d-md-table-cell',
            ),
        ]
        if admin:
            cells.append(html.Td(html.Div(
                plugin_action_buttons(plugin) if managed else [],
                className='plugin-row-actions',
            )))
        rows.append(html.Tr(cells, id={'type': 'installed-plugin-row', 'index': name}))
    return html.Div(
        [
            dbc.Table(
                [html.Thead(html.Tr(header)), html.Tbody(rows)],
                hover=True,
                size='sm',
                class_name='console-table plugins-table',
                id='installed-plugins-table',
            ),
        ] + ([update_check_store([p for p in plugins if p['name'] in manageable])] if admin else []),
        className='console-table-scroll',
    )


def plugin_action_buttons(plugin: Dict[str, Any], size: str = 'sm') -> List[Any]:
    """An installed plugin's Update and Uninstall icon buttons."""
    name = plugin['name']
    size_class = f' btn-{size}' if size in ('sm', 'lg') else ''

    def _button(icon: str, action: str, label: str, style: str):
        return html.Button(
            build_icon(icon),
            type='button',
            title=label,
            id={'type': 'plugin-action', 'action': action, 'index': name},
            className=f'btn {style}{size_class} plugin-icon-button',
            **{'aria-label': label},
        )

    actions = []
    if plugin.get('origin'):
        actions.append(html.Span(
            _button('arrow-up', 'update', f"Update {name}", 'btn-secondary'),
            id={'type': 'plugin-update-slot', 'index': name},
            className='plugin-update-slot',
            hidden=True,
        ))
    actions.append(_button('trash', 'uninstall', f"Uninstall {name}", 'btn-outline-danger'))
    return actions


def update_check_store(plugins: List[Dict[str, Any]]) -> Any:
    """The installed plugins with an origin, which the update check reads after the page shows."""
    return dcc.Store(
        id='plugins-update-check',
        data=[plugin['name'] for plugin in plugins if plugin.get('origin')],
    )


def build_installed_plugins_tab(rights: PluginRights) -> Any:
    """The Installed tab: the table, or a short empty state."""
    try:
        plugins = [
            plugin for plugin in _lifecycle.get_installed_plugins()
            if rights.can_see(plugin['name'])
        ]
        admin = rights.manages_everything
        manageable = rights.manageable_names([plugin['name'] for plugin in plugins])
    except Exception as e:
        return alert_from_success_tuple((False, f"Failed to read the installed plugins: {e}"))
    if not plugins:
        return html.Div(
            [
                html.H5('No plugins installed'),
                dbc.Button('Browse the repository', id='plugins-browse-repository-button', color='primary'),
            ],
            className='console-empty',
        )
    return build_installed_plugins_table(plugins, admin=admin, manageable=manageable)


########################################################################################
# Repository & detail UI.
########################################################################################

def _build_plugin_card(
    plugin_name: str,
    username: Optional[str],
    *,
    detail: bool = False,
    admin: bool = False,
    installed: Optional[Dict[str, str]] = None,
    can_install: Optional[bool] = None,
) -> dbc.Card:
    """Build a single plugin card (grid card or repository detail card)."""
    can_install = bool(admin) if can_install is None else bool(can_install)
    conn = get_api_connector()
    plugin = Plugin(plugin_name)
    attrs = conn.get_plugin_attributes(plugin, debug=debug) or {}
    desc = attrs.get('description', '')
    plugin_username = conn.get_plugin_username(plugin, debug=debug)
    plugin_version = (conn.get_plugin_version(plugin, debug=debug) or '').strip()

    owner = is_plugin_owner(plugin_name, username, conn=conn, debug=debug)
    desc_textarea_kw = {
        'value': desc,
        'readOnly': not owner,
        'debounce': False,
        'className': 'plugin-description',
        'draggable': False,
        'wrap': 'overflow',
        'placeholder': ("Describe the plugin" if owner else ''),
        'id': {'type': 'description-textarea', 'index': plugin_name},
    }
    if detail:
        desc_textarea_kw['style'] = {'min-height': '12em'}

    title_el = (
        html.H1(plugin_name, style={'margin': 0})
        if detail
        else html.H5(plugin_name, style={'margin': 0})
    )
    title_link = html.A(
        title_el,
        href='/dash/plugins/' + plugin_name,
        style={'text-decoration': 'none', 'color': 'inherit'},
    )

    version_badge = html.Span(
        plugin_version or '—',
        className='plugin-version-badge',
    )

    card_header = dbc.CardHeader(
        dbc.Row(
            [
                dbc.Col(title_link),
                dbc.Col(
                    version_badge,
                    width='auto',
                    style={'text-align': 'right'},
                ),
            ],
            justify='between',
            align='center',
            class_name='g-2',
        ),
    )

    body_children: List[Any] = [
        html.Div(
            [
                html.Span('👤 ', className='text-muted'),
                html.A(
                    str(plugin_username),
                    href='/dash/users/' + str(plugin_username),
                    className='text-muted',
                ),
            ],
            className='plugin-author',
        ),
        (_plugin_description(desc) if detail else dbc.Textarea(**desc_textarea_kw)),
    ]
    if owner:
        edit_controls = [
            dbc.Button(
                'Update description',
                size='sm',
                color='link',
                id={'type': 'edit-button', 'index': plugin_name},
                style={'padding-left': 0},
            ),
            html.Div(id={'type': 'edit-alert-div', 'index': plugin_name}),
        ]
        body_children += (
            [html.Details([html.Summary('Edit description'), dbc.Textarea(**desc_textarea_kw), *edit_controls])]
            if detail else edit_controls
        )

    if detail:
        install_cmd = f"mrsm install plugin {plugin_name} -r api:<label>"
        body_children += [
            html.Hr(),
            html.H5('Install from a terminal', style={'margin-bottom': '0.5em'}),
            dcc.Markdown(
                f"```bash\n{install_cmd}\n```",
                style={'margin-bottom': 0},
            ),
        ]
        body_children.pop(0)
        body_children.insert(0, html.Dl([
            html.Dt('Version'), html.Dd(plugin_version or 'Not declared'),
            html.Dt('Publisher'), html.Dd(str(plugin_username)),
            html.Dt('Status'), html.Dd(
                'Installed on this server' if plugin_name in (installed or {})
                else 'Published to this repository'
            ),
        ], className='plugin-detail-properties'))

    installed = installed or {}
    reinstall = plugin_name in installed
    install_button = (
        dbc.Button(
            'Reinstall' if reinstall else 'Install',
            size='sm',
            color='primary',
            id={
                'type': 'plugin-action',
                'action': ('reinstall' if reinstall else 'install'),
                'index': plugin_name,
            },
            class_name='plugin-card-install',
        )
        if can_install else None
    )
    download_link = html.A(
        '⬇️ Download',
        href=(endpoints['plugins'] + '/' + plugin_name),
        className='plugin-card-link',
    )
    footer_items = [download_link]
    if not detail:
        footer_items.append(html.A(
            '🔗 Share',
            href='/dash/plugins/' + plugin_name,
            className='plugin-card-link',
        ))
    if install_button is not None:
        footer_items.append(install_button)
    footer = dbc.CardFooter(html.Div(footer_items, className='plugin-card-footer-row'))

    return dbc.Card(
        [
            card_header,
            dbc.CardBody(body_children),
            footer,
        ],
        id=plugin_name + '_card',
        class_name='plugin-card' + (' plugin-card-detail' if detail else ''),
    )


def get_plugins_cards(
    rights: PluginRights,
    search_term: Optional[str] = None,
    page: int = 1,
    per_page: int = PLUGINS_PER_PAGE,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """Return the cards, alerts, total page count, and total plugin count."""
    alerts: List[SuccessTuple] = []
    all_names = rights.visible_names(
        sorted(get_api_connector().get_plugins(search_term=search_term) or [])
    )
    admin = rights.manages_everything
    may_install = admin or rights.can_manage_any
    username = getattr(rights.viewer, 'username', None)
    total = len(all_names)
    if per_page <= 0:
        per_page = PLUGINS_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    installed = _lifecycle.installed_versions() if may_install else {}
    cards = [
        _build_plugin_card(
            name,
            username,
            admin=admin,
            installed=installed,
            can_install=(admin or (may_install and rights.can_install(name, installed))),
        )
        for name in page_names
    ]
    return cards, alerts, total_pages, total


def build_plugins_listing(
    search_term: Optional[str],
    rights: PluginRights,
    page: int = 1,
) -> Tuple[Any, int, int]:
    """Build the repository listing (summary + cards grid)."""
    cards, _alerts, total_pages, total = get_plugins_cards(
        rights,
        search_term=search_term,
        page=page,
    )

    if not cards and not search_term:
        return _build_empty_plugins_placeholder(), total_pages, total

    if not cards:
        body = html.Div(
            html.H5(f"No plugins match '{search_term}'."),
            className='console-empty',
        )
    else:
        body = build_cards_grid(cards, num_columns=PLUGINS_GRID_COLUMNS)

    summary_text = (
        f"Page {page} of {total_pages} ({total} plugin"
        + ('s' if total != 1 else '')
        + ')'
    )

    content = html.Div([
        html.P(summary_text, className='text-muted', style={'text-align': 'center'}) if total_pages > 1 else None,
        body,
    ])
    return content, total_pages, total


def _build_empty_plugins_placeholder() -> Any:
    return html.Div(
        [
            html.H5('No plugins published'),
            dbc.Button(
                'Plugins guide',
                href='https://meerschaum.io/reference/plugins/',
                target='_blank',
                rel='noreferrer noopener',
                external_link=True,
                color='primary',
            ),
        ],
        className='console-empty plugins-empty-state',
    )


def build_plugin_detail(plugin_name: str, rights: PluginRights) -> Any:
    """Build the detail view for a single repository plugin."""

    back_button = dbc.Button(
        '← Plugins',
        href='/dash/plugins?tab=repository',
        color='link',
        size='lg',
        class_name='plugin-back-button',
    )

    admin = rights.manages_everything
    hidden = not rights.can_see(plugin_name)
    existing = [] if hidden else (get_api_connector().get_plugins(search_term=plugin_name) or [])
    if plugin_name not in existing:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: Plugin '{plugin_name}' not found."),
        ], className='plugin-detail-wrapper')

    may_install = admin or rights.can_manage_any
    installed = _lifecycle.installed_versions() if may_install else {}
    return html.Div([
        back_button,
        _build_plugin_card(
            plugin_name,
            getattr(rights.viewer, 'username', None),
            detail=True,
            admin=admin,
            installed=installed,
            can_install=(admin or (may_install and rights.can_install(plugin_name, installed))),
        ),
        build_source_browser('repository', plugin_name, rights),
    ], className='plugin-detail-wrapper')


def build_installed_plugin_detail(
    name: str,
    rights: PluginRights,
    selected: Optional[str] = None,
    edit: bool = False,
    created: bool = False,
) -> Any:
    """An installed plugin's metadata and source, restricted to those who may manage it."""
    back = dcc.Link('← All plugins', href='/dash/plugins', className='plugin-back-link')
    missing = html.Div([back, html.H5('This plugin is not installed.')], className='console-empty')
    if not _fs.is_safe_plugin_name(name) or not rights.can_see(name):
        return missing
    installed = next(
        (
            plugin for plugin in _lifecycle.get_installed_plugins(check_packages=False)
            if plugin['name'] == name
        ),
        None,
    )
    if not installed:
        return missing
    if not rights.can_read_source('installed', name):
        return html.Div(
            [back, html.H5("Installed source is available to administrators and the plugin's team.")],
            className='console-empty',
        )
    from meerschaum.api.dash.connector_listing import mask_text
    description = installed.get('description') or _lifecycle.installed_description(name, rights)
    actions = plugin_action_buttons(installed, size='md')
    actions.append(dbc.Button('Publish', id='plugin-publish-open', n_clicks=0, color='primary'))
    if installed.get('origin'):
        actions.append(update_check_store([installed]))
    info = [
        (
            'Language',
            html.Span(
                [language_logo(installed['kind']), html.Span(installed['kind'], **{'aria-hidden': 'true'})],
                className='plugin-detail-language',
            ),
        ),
        ('Layout', 'Single file' if installed['layout'] == 'file' else 'Package'),
        ('Version', installed['version'] or 'Not declared'),
        ('Installed from', mask_text(installed.get('origin') or 'Local files')),
    ]
    status = (
        'Failed to load' if installed.get('import_error')
        else 'Not loaded' if installed.get('not_loaded')
        else 'Installed'
    )
    details = []
    for label, value in info:
        details += [html.Dt(label), html.Dd(value)]
    requirements = installed['required'] + installed.get('plugin_requirements', [])
    return html.Div([
        back,
        html.Header([
            html.Div([html.H1(name), dbc.Badge(status, color='warning' if status != 'Installed' else 'success')]),
            html.Div(actions, className='plugin-detail-actions'),
        ], className='plugin-detail-header'),
        _plugin_description(description),
        html.Div([
            html.Section([html.H2('Details'), html.Dl(details, className='plugin-detail-properties')]),
            html.Section([
                html.H2('Provides'),
                html.Div(
                    [dbc.Badge(item, color='secondary') for item in installed['provides']],
                    className='plugin-capabilities',
                ) if installed['provides'] else html.P('No capabilities declared.'),
            ]),
            html.Section([
                html.H2('Requirements'),
                html.Ul([html.Li(html.Code(item)) for item in requirements])
                if requirements
                else html.P('No additional packages declared.', className='text-muted'),
            ]),
        ], className='plugin-detail-metadata'),
        build_source_browser(
            'installed',
            name,
            rights,
            selected=selected,
            edit=edit,
            notice=(
                dbc.Alert(
                    f"Created plugin '{name}'.",
                    color='success',
                    dismissable=True,
                    id='plugin-created-alert',
                ) if created else None
            ),
        ),
        build_publish_dialog(name, installed.get('origin'), rights),
    ], className='plugin-detail-wrapper')


def _plugin_description(value):
    text = str(value or '').strip()
    if not text:
        return None
    first = text.split('\n\n', 1)[0]
    preview = first[:500] + ('…' if len(first) > 500 else '')
    return html.Div([
        html.P(preview),
        html.Details([html.Summary('Full description'), html.Pre(text)]) if text != preview else None,
    ], className='plugin-detail-description')
