#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Plugins page: the plugins installed on this server (Installed) and the plugins
published to this API's repository (Repository). Administrators update, uninstall and install
plugins with the usual actions (`install plugins`, `uninstall plugins`), run as jobs.
"""

from __future__ import annotations

import json
import pathlib

from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, WebState, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin, User
from meerschaum.api.dash import debug
from meerschaum.api.dash.sessions import get_username_from_session
from meerschaum.api.dash.components import build_cards_grid

PLUGINS_PER_PAGE: int = 12
PLUGINS_GRID_COLUMNS: int = 3

#: The repository keys an install from this server's own repository records as its origin: a
#: connector the console defines for that job only (`MRSM_API_THIS_SERVER` = the server's URL).
THIS_SERVER_REPOSITORY: str = 'api:this_server'


########################################################################################
# Installed plugins.
########################################################################################

def _read_json(path: pathlib.Path) -> Dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _manifests() -> Dict[str, Dict[str, Any]]:
    """
    What each plugin provides, from the manifests the server caches at startup
    (`cache/plugins_manifest.json` for Python plugins, `cache/native_plugins_manifest.json` for
    Rust plugins; a Rust plugin replaces a Python plugin of the same name).
    """
    import meerschaum.config.paths as paths
    cache = pathlib.Path(paths.ROOT_DIR_PATH) / 'cache'
    plugins: Dict[str, Dict[str, Any]] = {}
    for file_name, native in (('plugins_manifest.json', False), ('native_plugins_manifest.json', True)):
        manifest = _read_json(cache / file_name)
        for name, entry in (manifest.get('plugins') or {}).items():
            if isinstance(entry, dict):
                plugins[str(name)] = {**entry, '_native': native}
        if native:
            for kind, connector in (manifest.get('connector_types') or {}).items():
                plugin = (connector or {}).get('plugin') if isinstance(connector, dict) else None
                if plugin in plugins:
                    plugins[plugin].setdefault('connectors', [])
                    if kind not in plugins[plugin]['connectors']:
                        plugins[plugin]['connectors'].append(kind)
    return plugins


def _discover_plugin_files() -> Dict[str, Dict[str, Any]]:
    """
    The plugins in the plugins directories, first directory first: `{name: {kind, layout, path}}`
    (`kind` is `Python` or `Rust`; `layout` is `file`, `package` or `directory`). A Rust plugin
    is a `<name>.rs` file or a directory with a `plugin.toml`, and replaces a Python plugin of
    the same name.
    """
    import meerschaum.config.paths as paths
    found: Dict[str, Dict[str, Any]] = {}
    for directory in paths.PLUGINS_DIR_PATHS:
        directory = pathlib.Path(directory)
        try:
            entries = sorted(directory.iterdir(), key=lambda p: p.name)
        except OSError:
            continue
        for entry in entries:
            name = entry.name
            if name.startswith(('.', '_')):
                continue
            if entry.is_dir():
                if (entry / 'plugin.toml').exists():
                    info = {'kind': 'Rust', 'layout': 'directory', 'path': str(entry)}
                elif (entry / '__init__.py').exists():
                    info = {'kind': 'Python', 'layout': 'package', 'path': str(entry)}
                else:
                    continue
            elif entry.suffix == '.py':
                name = entry.stem
                info = {'kind': 'Python', 'layout': 'file', 'path': str(entry)}
            elif entry.suffix == '.rs':
                name = entry.stem
                info = {'kind': 'Rust', 'layout': 'file', 'path': str(entry)}
            else:
                continue
            existing = found.get(name)
            if existing is None or (existing['kind'] == 'Python' and info['kind'] == 'Rust'):
                found[name] = info
    return found


def _native_version(path: str) -> Optional[str]:
    """The version in a Rust plugin directory's `plugin.toml`."""
    try:
        import re
        text = (pathlib.Path(path) / 'plugin.toml').read_text(encoding='utf-8')
        match = re.search(r'^\s*version\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
        return match.group(1) if match else None
    except Exception:
        return None


def describe_provides(entry: Dict[str, Any]) -> List[str]:
    """What a plugin provides, in plain words, from its manifest entry."""
    provides: List[str] = []
    connectors = [c for c in (entry.get('connectors') or []) if c]
    if connectors:
        provides.append(
            'Connector: ' + ', '.join(sorted(str(c) for c in connectors))
            if len(connectors) <= 2 else f"{len(connectors)} connectors"
        )
    data_functions = set(entry.get('data_functions') or [])
    if data_functions & {'fetch', 'sync'}:
        provides.append('Data source')
    actions = [
        (a.get('name') if isinstance(a, dict) else str(a))
        for a in (entry.get('actions') or [])
    ]
    actions = [str(a).replace('_', ' ') for a in actions if a]
    if actions:
        provides.append(
            ('Action: ' if len(actions) == 1 else 'Actions: ') + ', '.join(sorted(actions))
            if len(actions) <= 3 else f"{len(actions)} actions"
        )
    pages = entry.get('web_pages') or []
    if pages or entry.get('dash_plugins'):
        count = len(pages)
        provides.append(f"{count} page{'s' if count != 1 else ''}" if count else 'Pages')
    if entry.get('api_plugins'):
        provides.append('API routes')
    tools = entry.get('mcp_tools') or []
    if tools or entry.get('mcp_resources') or entry.get('mcp_prompts'):
        count = len(tools)
        provides.append(f"{count} MCP tool{'s' if count != 1 else ''}" if count else 'MCP')
    if entry.get('pre_sync_hooks') or entry.get('post_sync_hooks'):
        provides.append('Sync hooks')
    return provides


def _package_installed(requirement: str, plugin_name: str) -> bool:
    """Whether a plugin's required package is importable for it (its venv, `mrsm`, the host)."""
    import importlib.util
    from meerschaum.utils.packages import venv_contains_package, _install_to_import_name
    import_name = _install_to_import_name(requirement)
    root = import_name.split('.')[0]
    try:
        if venv_contains_package(root, venv=plugin_name) or venv_contains_package(root, venv='mrsm'):
            return True
    except Exception:
        pass
    try:
        return importlib.util.find_spec(root) is not None
    except (ImportError, ValueError):
        return False


def get_installed_plugins(check_packages: bool = True) -> List[Dict[str, Any]]:
    """
    The plugins installed on this server, sorted by name: `{name, version, kind, layout,
    provides, required, missing, import_error, origin}`.
    """
    from meerschaum.plugins._origins import read_plugin_origins
    files = _discover_plugin_files()
    manifests = _manifests()
    try:
        origins = read_plugin_origins()
    except Exception:
        origins = {}
    plugins = []
    for name, info in sorted(files.items()):
        entry = manifests.get(name) or {}
        if entry and bool(entry.get('_native')) != (info['kind'] == 'Rust'):
            entry = {}
        version = entry.get('version')
        if not version and info['kind'] == 'Rust' and info['layout'] == 'directory':
            version = _native_version(info['path'])
        required = [str(r) for r in (entry.get('required') or []) if r and not str(r).startswith('plugin:')]
        missing = (
            [r for r in required if not _package_installed(r, name)]
            if check_packages and info['kind'] == 'Python' else []
        )
        not_loaded = info['kind'] == 'Rust' and not entry and bool(manifests)
        plugins.append({
            'not_loaded': not_loaded,
            'name': name,
            'version': str(version) if version else None,
            'kind': info['kind'],
            'layout': info['layout'],
            'provides': describe_provides(entry),
            'required': required,
            'missing': missing,
            'import_error': entry.get('import_error'),
            'origin': origins.get(name),
        })
    return plugins


def plugin_kind_label(plugin: Dict[str, Any]) -> str:
    """`Python`, `Rust (single file)` or `Rust (directory)`."""
    if plugin['kind'] == 'Rust':
        return 'Rust (single file)' if plugin['layout'] == 'file' else 'Rust (directory)'
    return 'Python'


def packages_label(plugin: Dict[str, Any]) -> Tuple[str, bool]:
    """The packages column's text and whether something is missing."""
    required, missing = plugin['required'], plugin['missing']
    if plugin['kind'] != 'Python':
        return '—', False
    if not required:
        return 'None required', False
    if missing:
        return f"{len(missing)} of {len(required)} missing", True
    return f"{len(required)} installed" if len(required) != 1 else '1 installed', False


def build_installed_plugins_table(plugins: List[Dict[str, Any]], admin: bool = False) -> Any:
    """The Installed tab's table."""
    header = [
        html.Th('Plugin'),
        html.Th('Version', className='d-none d-sm-table-cell'),
        html.Th('Kind', className='d-none d-md-table-cell'),
        html.Th('Provides'),
        html.Th('Packages', className='d-none d-md-table-cell'),
    ]
    if admin:
        header.append(html.Th(html.Span('Actions', className='visually-hidden')))
    rows = []
    for plugin in plugins:
        name = plugin['name']
        packages_text, packages_missing = packages_label(plugin)
        provides = [dbc.Badge(item, color='secondary', class_name='me-1') for item in plugin['provides']]
        if plugin.get('import_error'):
            provides.append(dbc.Badge('Failed to load', color='danger', class_name='me-1',
                                      title=str(plugin['import_error'])[:500]))
        elif plugin.get('not_loaded'):
            provides.append(dbc.Badge('Not loaded', color='warning', text_color='dark', class_name='me-1'))
        cells = [
            html.Td([
                html.Span(name, className='fw-semibold'),
                html.Div(
                    [plugin['version'] or '', ' · ' if plugin['version'] else '', plugin_kind_label(plugin)],
                    className='text-muted small d-sm-none',
                ),
            ]),
            html.Td(html.Span(plugin['version'] or '—', className='plugin-version-badge'),
                    className='d-none d-sm-table-cell'),
            html.Td(plugin_kind_label(plugin), className='plugin-kind d-none d-md-table-cell'),
            html.Td(provides or html.Span('—', className='text-muted'), className='plugin-provides'),
            html.Td(
                html.Span(
                    packages_text,
                    className=('plugin-packages-missing' if packages_missing else 'text-muted'),
                    title=(', '.join(plugin['missing']) if packages_missing else ', '.join(plugin['required'])),
                ),
                className='d-none d-md-table-cell',
            ),
        ]
        if admin:
            actions = []
            if plugin.get('origin'):
                actions.append(dbc.Button(
                    'Update', size='sm', color='secondary',
                    id={'type': 'plugin-action', 'action': 'update', 'index': name},
                    title=f"Install the latest version from {plugin['origin']}",
                ))
            actions.append(dbc.Button(
                'Uninstall', size='sm', color='danger', outline=True,
                id={'type': 'plugin-action', 'action': 'uninstall', 'index': name},
            ))
            cells.append(html.Td(html.Div(actions, className='plugin-row-actions')))
        rows.append(html.Tr(cells, id={'type': 'installed-plugin-row', 'index': name}))
    return html.Div(
        dbc.Table(
            [html.Thead(html.Tr(header)), html.Tbody(rows)],
            hover=True,
            size='sm',
            class_name='console-table plugins-table',
            id='installed-plugins-table',
        ),
        className='console-table-scroll',
    )


def build_installed_plugins_tab(admin: bool = False) -> Any:
    """The Installed tab: the table, or a short empty state."""
    try:
        plugins = get_installed_plugins()
    except Exception as e:
        from meerschaum.api.dash.components import alert_from_success_tuple
        return alert_from_success_tuple((False, f"Failed to read the installed plugins: {e}"))
    if not plugins:
        return html.Div(
            [
                html.H5('No plugins installed'),
                dbc.Button('Browse the repository', id='plugins-browse-repository-button', color='primary'),
            ],
            className='console-empty',
        )
    return build_installed_plugins_table(plugins, admin=admin)


########################################################################################
# Running plugin actions (admins).
########################################################################################

#: How long (minutes) an install job may read this server's plugins on a server that asks for
#: credentials (`--private`): it downloads the plugin, and the plugins it requires, as it starts.
THIS_SERVER_TOKEN_MINUTES: int = 30


def plugins_read_token(username: str, minutes: int = THIS_SERVER_TOKEN_MINUTES) -> str:
    """
    A bearer token of `username`, signed with the secret this server checks, that may only read
    plugins (`plugins:read`) and expires after `minutes`.
    """
    from datetime import timedelta
    from meerschaum.api._oauth2 import manager
    conn = get_api_connector()
    user_type = conn.get_user_type(User(username, instance=conn), debug=debug)
    return manager.create_access_token(
        data={'sub': username, 'type': user_type},
        expires=timedelta(minutes=minutes),
        scopes=['plugins:read'],
    )


def this_server_env(username: Optional[str] = None) -> Dict[str, str]:
    """
    The environment that makes `api:this_server` this server (for an install job). On a server
    that asks for credentials to read plugins (`--private`), the connector carries a short-lived
    token of `username` that may only read plugins (`plugins_read_token`).
    """
    from meerschaum.api import no_auth, private
    from meerschaum.api.dash.execute import server_url
    url = server_url()
    if not url:
        return {}
    key = 'MRSM_API_' + THIS_SERVER_REPOSITORY.split(':', 1)[1].upper()
    if not private or no_auth or not username:
        return {key: url}
    return {key: json.dumps({'uri': url, 'api_key': plugins_read_token(username)})}


def is_safe_plugin_name(name: Any) -> bool:
    """A plugin name is one path component of letters, digits, `_` and `-`."""
    name = str(name or '')
    return bool(name) and name not in ('.', '..') and all(c.isalnum() or c in '_-' for c in name) and name.isascii()


def plugin_action_sysargs(
    action: str,
    name: str,
    origin: Optional[str] = None,
    username: Optional[str] = None,
) -> Tuple[Optional[List[str]], Dict[str, str], str]:
    """
    Return `(sysargs, env, message)` for a plugin action: `install` (from this server's
    repository), `update` (install again from where it came from) or `uninstall`. `username`
    is the administrator asking: an install from this server reads its plugins as them.
    """
    if not is_safe_plugin_name(name):
        return None, {}, f"Invalid plugin name '{name}'."
    if action == 'uninstall':
        return ['uninstall', 'plugins', name, '--yes'], {}, ''
    if action not in ('install', 'reinstall', 'update'):
        return None, {}, f"Unknown plugin action '{action}'."
    if action == 'update' and not origin:
        return None, {}, f"Plugin '{name}' was not installed from a repository."
    repository = THIS_SERVER_REPOSITORY if action != 'update' else str(origin)
    env: Dict[str, str] = {}
    if repository == THIS_SERVER_REPOSITORY:
        try:
            env = this_server_env(username)
        except Exception as e:
            return None, {}, f"Cannot install from this server's repository: {e}"
        if not env:
            return None, {}, "The console has no server URL to install from."
    force = ['--force'] if action == 'reinstall' else []
    return ['install', 'plugins', name, '-r', repository] + force, env, ''


def start_plugin_run(
    sysargs: List[str],
    username: Optional[str],
    env: Optional[Dict[str, str]] = None,
) -> Tuple[SuccessTuple, Optional[str]]:
    """
    Run a plugin action as one of the console's runs (an `execute-*` job owned by `username`,
    listed under Jobs > Runs and cleaned up like the console's other runs). Returns the start
    result and the job's name.
    """
    import uuid
    from meerschaum.jobs import Job
    name = 'execute-' + uuid.uuid4().hex[:8]
    execution = {
        'kind': 'temporary_execution',
        'origin': 'console_plugins',
        'owner': username,
        'run_id': str(uuid.uuid4()),
    }
    properties = {'mrsm_execution': execution}
    if username:
        properties['mrsm_owner'] = username
    try:
        job = Job(name, sysargs, env=(env or None), executor_keys='local', _properties=properties)
        return job.start(), name
    except Exception as e:
        return (False, f"Failed to start '{' '.join(sysargs)}': {e}"), None


########################################################################################
# The repository (plugins published to this API).
########################################################################################

def _build_plugin_card(
    plugin_name: str,
    session_data: Dict[str, Any],
    *,
    detail: bool = False,
    admin: bool = False,
    installed: Optional[Dict[str, str]] = None,
) -> dbc.Card:
    """
    Build a single plugin card. If ``detail`` is True, render the larger,
    self-contained plugin detail view with an install snippet.
    """
    conn = get_api_connector()
    plugin = Plugin(plugin_name)
    attrs = conn.get_plugin_attributes(plugin, debug=debug) or {}
    desc = attrs.get('description', '')
    plugin_username = conn.get_plugin_username(plugin, debug=debug)
    plugin_version = (conn.get_plugin_version(plugin, debug=debug) or '').strip()

    owner = is_plugin_owner(plugin_name, session_data)
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
        html.H3(plugin_name, style={'margin': 0})
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
        dbc.Textarea(**desc_textarea_kw),
    ]
    if owner:
        body_children += [
            dbc.Button(
                'Update description',
                size='sm',
                color='link',
                id={'type': 'edit-button', 'index': plugin_name},
                style={'padding-left': 0},
            ),
            html.Div(id={'type': 'edit-alert-div', 'index': plugin_name}),
        ]

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
        if admin else None
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
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
    page: int = 1,
    per_page: int = PLUGINS_PER_PAGE,
    admin: bool = False,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """
    Return the cards, alerts, total page count, and total plugin count
    for the plugins listing.
    """
    alerts: List[SuccessTuple] = []
    if session_data is None:
        session_data = {}

    all_names = sorted(get_api_connector().get_plugins(search_term=search_term) or [])
    total = len(all_names)
    if per_page <= 0:
        per_page = PLUGINS_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    installed = {}
    if admin:
        try:
            installed = {p['name']: p.get('version') for p in get_installed_plugins(check_packages=False)}
        except Exception:
            installed = {}
    cards = [
        _build_plugin_card(name, session_data, admin=admin, installed=installed)
        for name in page_names
    ]
    return cards, alerts, total_pages, total


def build_plugins_listing(
    search_term: Optional[str],
    session_data: Optional[Dict[str, Any]],
    page: int = 1,
    admin: bool = False,
) -> Tuple[Any, int, int]:
    """
    Build the repository listing (summary + cards grid) and return it along with the total page
    count and total plugin count. The ``plugins-pagination`` control is rendered persistently by
    the page layout.
    """
    cards, _alerts, total_pages, total = get_plugins_cards(
        search_term=search_term,
        session_data=session_data,
        page=page,
        admin=admin,
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
    """
    The repository's empty state: a short title and where to learn how to publish one.
    """
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


def build_plugin_detail(
    plugin_name: str,
    session_data: Optional[Dict[str, Any]],
    admin: bool = False,
) -> Any:
    """
    Build the detail view for a single plugin (shareable URL).
    """
    if session_data is None:
        session_data = {}

    back_button = dbc.Button(
        '← Plugins',
        href='/dash/plugins?tab=repository',
        color='link',
        size='lg',
        class_name='plugin-back-button',
    )

    existing = get_api_connector().get_plugins(search_term=plugin_name) or []
    if plugin_name not in existing:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: Plugin '{plugin_name}' not found."),
        ], className='plugin-detail-wrapper')

    installed = {}
    if admin:
        try:
            installed = {p['name']: p.get('version') for p in get_installed_plugins(check_packages=False)}
        except Exception:
            installed = {}
    return html.Div([
        back_button,
        html.Br(),
        html.Br(),
        _build_plugin_card(plugin_name, session_data, detail=True, admin=admin, installed=installed),
        html.Br(),
    ], className='plugin-detail-wrapper')


def is_plugin_owner(plugin_name: str, session_data: Dict['str', Any]) -> bool:
    """
    Check whether the currently logged in user is the owner of a plugin.
    """
    plugin = Plugin(plugin_name)
    session_id = (session_data or {}).get('session-id', None)
    if session_id is None:
        return False
    _username = get_username_from_session(session_id)
    _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
    return (
        _username is not None
        and _username == _plugin_username
    )
