#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Plugins page: the plugins installed on this server (Installed) and the plugins
published to this API's repository (Repository). Administrators update, uninstall and install
plugins with the usual actions (`install plugins`, `uninstall plugins`), run as jobs. The
administrators who may use the web terminal also create plugins (New plugin: `bootstrap
plugins`'s choices and files, `create_new_plugin`, or an upload, `plugin_upload`) and edit their
files in the source view (`plugin_sources.save_source`).

Teams: a walled-off viewer sees only the plugins of their teams (`can_see_plugin`), and the
members of a team that may manage plugins (`plugins_write`) install, create, upload, edit,
update, publish and uninstall their team's plugins (`may_manage_plugins`, `may_manage_plugin`,
`plugin_action_refusal`); what they add is recorded as their team's (`claim_plugin`).
"""

from __future__ import annotations

import json
import pathlib
import threading

import meerschaum as mrsm

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
# Who may see and manage which plugins.
########################################################################################

#: Why a viewer may not manage plugins at all, and why not this one.
MANAGE_REFUSAL: str = 'Only administrators and team plugin managers can manage plugins.'
NO_WRITING_TEAM: str = 'None of your teams may manage plugins.'


def foreign_plugin_message(name: str) -> str:
    """Why a team's plugin manager may not touch an installed plugin of someone else's."""
    return f"Plugin '{name}' is already installed and does not belong to your team."


def _manages_everything(viewer: Any) -> bool:
    """Administrators (and everyone under `--no-auth`) manage every plugin."""
    return bool(viewer is not None and (viewer.no_auth or (viewer.signed_in and viewer.is_admin)))


def can_see_plugin(viewer: Any, name: Any) -> bool:
    """
    Whether `viewer` may see the plugin `name`: every plugin, unless they are walled off
    (`Viewer.is_isolated`), where it is the plugins granted to or installed by their teams.
    """
    if _manages_everything(viewer):
        return True
    if viewer is None or not viewer.signed_in:
        return False
    return bool(viewer.can_use_plugin(str(name)))


def visible_plugin_names(viewer: Any, names: Any) -> List[str]:
    """The plugin names among `names` that `viewer` may see, in their order."""
    return [str(name) for name in (names or []) if can_see_plugin(viewer, name)]


def team_plugins(viewer: Any) -> set:
    """The plugins that belong to the viewer's teams (their teams' `plugins` lists)."""
    return {str(name) for name in ((getattr(viewer, 'grants', None) or {}).get('plugins', None) or [])}


def _console_is_read_only() -> bool:
    """Whether non-administrators may not act from the console (`--secure`)."""
    try:
        from meerschaum.api.dash import access
        return bool(access._secure())
    except Exception:
        return True


def may_manage_plugins(viewer: Any) -> bool:
    """
    Whether `viewer` may add plugins to the server (install from the repository, create, upload):
    administrators, and the members of a team that may manage plugins (`plugins_write`), who
    still read plugins (`plugins:read`) on a server that lets non-administrators act.
    """
    if _manages_everything(viewer):
        return True
    if viewer is None or not viewer.signed_in or not viewer.can('plugins:read'):
        return False
    if not viewer.can_write_plugins:
        return False
    return not _console_is_read_only()


def may_manage_plugin(viewer: Any, name: Any) -> bool:
    """
    Whether `viewer` may reinstall, update, uninstall, read and edit the installed source of,
    and publish the plugin `name`: administrators, and a team's plugin managers for the plugins
    of their teams.
    """
    if _manages_everything(viewer):
        return True
    return may_manage_plugins(viewer) and str(name) in team_plugins(viewer)


def manageable_plugin_names(viewer: Any, names: Any) -> List[str]:
    """The plugin names among `names` that `viewer` may manage, in their order."""
    return [str(name) for name in (names or []) if may_manage_plugin(viewer, name)]


def may_install_plugin(viewer: Any, name: Any, installed: Any) -> bool:
    """
    Whether `viewer` may install the repository's plugin `name` (`installed`: the names
    installed now): one they may see, that is not installed or is their team's.
    """
    if _manages_everything(viewer):
        return True
    if not may_manage_plugins(viewer) or not can_see_plugin(viewer, name):
        return False
    return str(name) not in set(installed or []) or may_manage_plugin(viewer, name)


def plugin_action_refusal(
    viewer: Any,
    action: str,
    name: Any,
    installed: Optional[Any] = None,
) -> Optional[str]:
    """
    Why `viewer` may not run a plugin action (`install`, `reinstall`, `update`, `uninstall`) on
    `name`, or `None` when they may. `installed` is the names installed now (read when omitted).
    """
    if _manages_everything(viewer):
        return None
    if not may_manage_plugins(viewer):
        return MANAGE_REFUSAL
    name = str(name)
    if not can_see_plugin(viewer, name):
        return f"Plugin '{name}' is not available."
    if may_manage_plugin(viewer, name):
        return None
    if installed is None:
        installed = _discover_plugin_files()
    if name in set(installed):
        return foreign_plugin_message(name)
    if action == 'install':
        return None
    return f"Plugin '{name}' does not belong to your team."


def _team_attributes(team: str, conn: Any = None) -> Tuple[Any, Optional[Dict[str, Any]]]:
    """A team and its attributes read now (`None` when the team does not exist)."""
    team_object = mrsm.Team(str(team), instance=(conn if conn is not None else get_api_connector()))
    if not team_object.exists():
        return team_object, None
    return team_object, team_object.attributes


def writing_team(viewer: Any, conn: Any = None) -> Optional[str]:
    """
    The team a non-administrator's new plugins are recorded for: their primary team if it may
    manage plugins (`plugins_write`, read now), else the first of their teams that may.
    """
    teams = list(getattr(viewer, 'own_teams', None) or [])
    primary = getattr(viewer, 'primary_team', None)
    if primary in teams:
        teams = [primary] + [team for team in teams if team != primary]
    for team in teams:
        try:
            _, attributes = _team_attributes(team, conn)
        except Exception:
            continue
        if attributes is not None and attributes.get('plugins_write', False) is True:
            return str(team)
    return None


def _forget_viewers() -> None:
    try:
        from meerschaum.api.dash import access
        access.forget()
    except Exception:
        pass


def claim_plugin(viewer: Any, name: Any, conn: Any = None) -> SuccessTuple:
    """
    Record the plugin `name`, which `viewer` just installed, created or uploaded, as their
    team's (`writing_team`: its `plugins` attribute, read fresh), so its plugin managers manage
    it. Administrators record nothing.
    """
    if _manages_everything(viewer):
        return True, ''
    name = str(name)
    if not is_safe_plugin_name(name):
        return False, f"Invalid plugin name '{name}'."
    if not may_manage_plugins(viewer):
        return False, MANAGE_REFUSAL
    try:
        team = writing_team(viewer, conn)
        if team is None:
            return False, NO_WRITING_TEAM
        team_object, attributes = _team_attributes(team, conn)
        if attributes is None:
            return False, NO_WRITING_TEAM
        listed = [str(item) for item in (attributes.get('plugins', None) or [])]
        if name not in listed:
            attributes['plugins'] = listed + [name]
            success, message = team_object.edit()
            if not success:
                return False, f"Plugin '{name}' could not be recorded for team '{team}': {message}"
    except Exception as e:
        return False, f"Plugin '{name}' could not be recorded for your team: {e}"
    finally:
        _forget_viewers()
    return True, f"Plugin '{name}' belongs to team '{team}'."


def release_plugin(viewer: Any, name: Any, conn: Any = None) -> SuccessTuple:
    """
    Take the plugin `name`, which `viewer` just uninstalled, off the `plugins` lists of their
    teams that may manage plugins. Administrators change nothing.
    """
    if _manages_everything(viewer):
        return True, ''
    name = str(name)
    if not may_manage_plugins(viewer):
        return False, MANAGE_REFUSAL
    try:
        for team in list(getattr(viewer, 'own_teams', None) or []):
            team_object, attributes = _team_attributes(team, conn)
            if attributes is None or attributes.get('plugins_write', False) is not True:
                continue
            listed = [str(item) for item in (attributes.get('plugins', None) or [])]
            if name not in listed:
                continue
            attributes['plugins'] = [item for item in listed if item != name]
            success, message = team_object.edit()
            if not success:
                return False, f"Plugin '{name}' is still listed for team '{team}': {message}"
    except Exception as e:
        return False, f"Plugin '{name}' is still listed for your team: {e}"
    finally:
        _forget_viewers()
    return True, ''


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
            'description': entry.get('description') or entry.get('docstring'),
            'plugin_requirements': [str(r) for r in (entry.get('required') or [])
                                    if str(r).startswith('plugin:')],
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


def build_installed_plugins_table(
    plugins: List[Dict[str, Any]],
    admin: bool = False,
    manageable: Optional[Any] = None,
) -> Any:
    """
    The Installed tab's table. A plugin's page link and its Update and Uninstall are shown for
    every plugin to administrators (`admin`), else for the plugins named in `manageable` (a
    team's plugin managers: `manageable_plugin_names`).
    """
    from meerschaum.api.dash.plugin_sources import language_logo
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
        packages_text, packages_missing = packages_label(plugin)
        provides = [dbc.Badge(item, color='secondary', class_name='me-1') for item in plugin['provides']]
        if plugin.get('import_error'):
            provides.append(dbc.Badge('Failed to load', color='danger', class_name='me-1',
                                      title=str(plugin['import_error'])[:500]))
        elif plugin.get('not_loaded'):
            provides.append(dbc.Badge('Not loaded', color='warning', text_color='dark', class_name='me-1'))
        cells = [
            html.Td([
                (dcc.Link(name, href='/dash/plugins/installed/' + name,
                          className='console-name-link fw-semibold')
                 if managed else html.Span(name, className='fw-semibold')),
                html.Div(
                    [plugin['version'] or '', ' · ' if plugin['version'] else '', language_logo(plugin['kind'])],
                    className='text-muted small d-sm-none plugin-mobile-meta',
                ),
            ]),
            html.Td(html.Span(plugin['version'] or '—', className='plugin-version-badge'),
                    className='d-none d-sm-table-cell'),
            html.Td(language_logo(plugin['kind']), className='plugin-kind d-none d-md-table-cell'),
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
            cells.append(html.Td(html.Div(
                plugin_action_buttons(plugin) if managed else [], className='plugin-row-actions',
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
    """
    An installed plugin's Update (an up arrow, shown only once `plugin_update_versions()` finds a
    newer version in the repository it came from) and Uninstall (a trash can) icon buttons.
    """
    from meerschaum.api.dash.components import build_icon
    name = plugin['name']
    size_class = f' btn-{size}' if size in ('sm', 'lg') else ''

    def _button(icon: str, action: str, label: str, style: str):
        return html.Button(
            build_icon(icon), type='button', title=label,
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


#: How long (seconds) a repository's answer for a plugin's latest version is reused.
REMOTE_VERSION_SECONDS: float = 15 * 60

#: How long (seconds) the update check waits for the repositories in all.
REMOTE_VERSION_TIMEOUT_SECONDS: float = 5.0

#: At most this many repository requests run at once, process-wide.
REMOTE_VERSION_WORKERS: int = 4

_remote_versions: Dict[Tuple[str, str], Tuple[float, Optional[str]]] = {}
_remote_versions_lock = threading.Lock()
_remote_version_pool = None
_remote_versions_in_flight: Dict[Tuple[str, str], Any] = {}


def remote_plugin_version(name: str, origin: str) -> Optional[str]:
    """
    The latest version of `name` in the repository `origin` (cached; `None` if unknown). Runs on
    the update check's pool (`_version_future`), never while a page renders.
    """
    import time
    key = (str(origin), str(name))
    ### This server's repository is read fresh (a quick query); other repositories are cached.
    local = str(origin) == THIS_SERVER_REPOSITORY
    with _remote_versions_lock:
        cached = None if local else _remote_versions.get(key, None)
    if cached is not None and time.monotonic() - cached[0] < REMOTE_VERSION_SECONDS:
        return cached[1]
    try:
        ### `api:this_server` exists only in an install job's environment: this server's own
        ### repository is its instance (what the Repository tab lists).
        repository = (
            get_api_connector() if str(origin) == THIS_SERVER_REPOSITORY
            else mrsm.get_connector(str(origin))
        )
        version = repository.get_plugin_version(Plugin(name, repo_connector=repository))
        version = str(version).strip() if version else None
    except Exception:
        version = None
    if not local:
        with _remote_versions_lock:
            _remote_versions[key] = (time.monotonic(), version or None)
    return version or None


def forget_remote_versions(name: str) -> None:
    """Drop the cached repository versions of a plugin (after it is updated or uninstalled)."""
    with _remote_versions_lock:
        for key in [key for key in _remote_versions if key[1] == str(name)]:
            _remote_versions.pop(key, None)


def _version_future(name: str, origin: str) -> Any:
    """
    The pending request for `(origin, name)`: one at a time per plugin and repository, on one
    pool of `REMOTE_VERSION_WORKERS` threads for the whole process, however often pages ask (a
    repository that hangs holds at most those threads; the client's connect timeout still applies).
    """
    from concurrent.futures import ThreadPoolExecutor
    global _remote_version_pool
    key = (str(origin), str(name))
    with _remote_versions_lock:
        future = _remote_versions_in_flight.get(key, None)
        if future is not None and not future.done():
            return future
        if _remote_version_pool is None:
            _remote_version_pool = ThreadPoolExecutor(
                max_workers=REMOTE_VERSION_WORKERS, thread_name_prefix='mrsm-plugin-versions',
            )
        future = _remote_version_pool.submit(remote_plugin_version, name, origin)
        _remote_versions_in_flight[key] = future

    def _done(finished, key=key):
        with _remote_versions_lock:
            if _remote_versions_in_flight.get(key, None) is finished:
                _remote_versions_in_flight.pop(key, None)

    future.add_done_callback(_done)
    return future


def is_newer_version(remote: Optional[str], installed: Optional[str]) -> bool:
    """Whether `remote` is a newer PEP 440 version than `installed` (`False` when unknown)."""
    if not remote or not installed:
        return False
    from meerschaum.utils.packages import _version_satisfies
    try:
        return bool(_version_satisfies(str(remote), '>' + str(installed)))
    except Exception:
        return False


def plugin_update_versions(names: List[str], viewer: Any = None) -> Dict[str, str]:
    """
    `{name: newer version}` for the installed plugins among `names` whose origin repository has a
    newer version than the one installed (read fresh); with a `viewer`, only the plugins they
    may manage (`may_manage_plugin`) are asked about. Answers within
    `REMOTE_VERSION_TIMEOUT_SECONDS`: a repository still busy offers no update this time, its
    request stays the one in flight, and requests that have not started are cancelled.
    """
    from concurrent.futures import wait
    wanted = {str(name) for name in (names or [])}
    if viewer is not None:
        wanted = set(manageable_plugin_names(viewer, sorted(wanted)))
    if not wanted:
        return {}
    installed = {
        plugin['name']: plugin for plugin in get_installed_plugins(check_packages=False)
        if plugin['name'] in wanted and plugin.get('origin')
    }
    if not installed:
        return {}
    futures = {_version_future(name, plugin['origin']): name for name, plugin in installed.items()}
    done, pending = wait(futures, timeout=REMOTE_VERSION_TIMEOUT_SECONDS)
    for future in pending:
        future.cancel()
    updates = {}
    for future in done:
        name = futures[future]
        try:
            remote = future.result()
        except Exception:
            continue
        if is_newer_version(remote, installed[name].get('version')):
            updates[name] = remote
    return updates


def build_installed_plugins_tab(admin: bool = False, viewer: Any = None) -> Any:
    """
    The Installed tab: the table, or a short empty state. With a `viewer`, the plugins they may
    see (`can_see_plugin`), with actions on the ones they may manage.
    """
    manageable = None
    try:
        plugins = get_installed_plugins()
        if viewer is not None:
            plugins = [plugin for plugin in plugins if can_see_plugin(viewer, plugin['name'])]
            admin = _manages_everything(viewer)
            manageable = manageable_plugin_names(viewer, [plugin['name'] for plugin in plugins])
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
    return build_installed_plugins_table(plugins, admin=admin, manageable=manageable)


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
    execution: Optional[Dict[str, Any]] = None,
) -> Tuple[SuccessTuple, Optional[str]]:
    """
    Run a plugin action as one of the console's runs (an `execute-*` job owned by `username`,
    listed under Jobs > Runs and cleaned up like the console's other runs). Returns the start
    result and the job's name. `execution` adds to the job's `mrsm_execution` record (the
    console's own note of the plugin and action the run is for: `plugin_run_record`).

    The job is started by this host (`Job(...).start()`), not through the API's `/execute`
    routes, so the caller decides who may run it (`plugin_action_refusal`).
    """
    import uuid
    from meerschaum.jobs import Job
    name = 'execute-' + uuid.uuid4().hex[:8]
    record = {
        'kind': 'temporary_execution',
        'origin': 'console_plugins',
        'owner': username,
        'run_id': str(uuid.uuid4()),
    }
    execution = {**(execution or {}), **record}
    properties = {'mrsm_execution': execution}
    if username:
        ### The run is the user's, in their primary team (AC9).
        from meerschaum.jobs import ownership_properties
        from meerschaum.api.dash.access import viewer_for
        properties.update(ownership_properties(username, viewer_for(username).primary_team))
    try:
        job = Job(name, sysargs, env=(env or None), executor_keys='local', _properties=properties)
        return job.start(), name
    except Exception as e:
        return (False, f"Failed to start '{' '.join(sysargs)}': {e}"), None


def plugin_run_record(job: Any, username: Optional[str]) -> Optional[Dict[str, str]]:
    """
    `{plugin, action}` of a plugin run this console started for `username`, from the job's own
    record and arguments (never from the browser): `None` for any other job.
    """
    try:
        properties = (job.daemon.properties if hasattr(job, 'daemon')
                      else job.get_daemon_properties()) or {}
        record = properties.get('mrsm_execution', None) or {}
        sysargs = [str(arg) for arg in (job.sysargs or [])]
    except Exception:
        return None
    if not isinstance(record, dict) or record.get('origin') != 'console_plugins':
        return None
    if (record.get('owner') or None) != (username or None):
        return None
    plugin, action = record.get('plugin'), record.get('action')
    if not is_safe_plugin_name(plugin) or action not in ('install', 'reinstall', 'update', 'uninstall'):
        return None
    verb = 'uninstall' if action == 'uninstall' else 'install'
    if sysargs[:3] != [verb, 'plugins', str(plugin)]:
        return None
    return {'plugin': str(plugin), 'action': str(action)}


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
    can_install: Optional[bool] = None,
) -> dbc.Card:
    """
    Build a single plugin card. If ``detail`` is True, render the larger,
    self-contained plugin detail view with an install snippet. Install (or Reinstall) is shown
    when ``can_install`` (by default, to administrators: ``admin``).
    """
    can_install = bool(admin) if can_install is None else bool(can_install)
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
        body_children += ([html.Details([html.Summary('Edit description'),
                          dbc.Textarea(**desc_textarea_kw),*edit_controls])]
                         if detail else edit_controls)

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
        body_children.insert(0,html.Dl([
            html.Dt('Version'),html.Dd(plugin_version or 'Not declared'),
            html.Dt('Publisher'),html.Dd(str(plugin_username)),
            html.Dt('Status'),html.Dd('Installed on this server' if plugin_name in (installed or {})
                                    else 'Published to this repository'),
        ],className='plugin-detail-properties'))

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
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
    page: int = 1,
    per_page: int = PLUGINS_PER_PAGE,
    admin: bool = False,
    viewer: Any = None,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """
    Return the cards, alerts, total page count, and total plugin count
    for the plugins listing. With a ``viewer``, only the plugins they may see are listed and
    counted (``can_see_plugin``), and Install is offered where they may (``may_install_plugin``).
    """
    alerts: List[SuccessTuple] = []
    if session_data is None:
        session_data = {}

    all_names = sorted(get_api_connector().get_plugins(search_term=search_term) or [])
    if viewer is not None:
        all_names = visible_plugin_names(viewer, all_names)
        admin = _manages_everything(viewer)
    may_install = admin or (viewer is not None and may_manage_plugins(viewer))
    total = len(all_names)
    if per_page <= 0:
        per_page = PLUGINS_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    installed = _installed_versions() if may_install else {}
    cards = [
        _build_plugin_card(
            name, session_data, admin=admin, installed=installed,
            can_install=(admin or (may_install and may_install_plugin(viewer, name, installed))),
        )
        for name in page_names
    ]
    return cards, alerts, total_pages, total


def _installed_versions() -> Dict[str, Any]:
    """`{name: version}` of the installed plugins (`{}` when they cannot be read)."""
    try:
        return {p['name']: p.get('version') for p in get_installed_plugins(check_packages=False)}
    except Exception:
        return {}


def build_plugins_listing(
    search_term: Optional[str],
    session_data: Optional[Dict[str, Any]],
    page: int = 1,
    admin: bool = False,
    viewer: Any = None,
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
        viewer=viewer,
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
    viewer: Any = None,
) -> Any:
    """
    Build the detail view for a single plugin (shareable URL). A plugin the ``viewer`` may not
    see is not found, like one that is not published.
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

    if viewer is not None:
        admin = _manages_everything(viewer)
    hidden = viewer is not None and not can_see_plugin(viewer, plugin_name)
    existing = [] if hidden else (get_api_connector().get_plugins(search_term=plugin_name) or [])
    if plugin_name not in existing:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: Plugin '{plugin_name}' not found."),
        ], className='plugin-detail-wrapper')

    may_install = admin or (viewer is not None and may_manage_plugins(viewer))
    installed = _installed_versions() if may_install else {}
    return html.Div([
        back_button,
        _build_plugin_card(
            plugin_name, session_data, detail=True, admin=admin, installed=installed,
            can_install=(admin or (may_install and may_install_plugin(viewer, plugin_name, installed))),
        ),
        _source_browser('repository', plugin_name, session_data),
    ], className='plugin-detail-wrapper')


def _source_browser(kind: str, name: str, session: Any, **kwargs):
    from meerschaum.api.dash.plugin_sources import build_source_browser
    return build_source_browser(kind, name, session, **kwargs)


########################################################################################
# New plugin (`bootstrap plugins`).
########################################################################################

#: The name New plugin words its packaging choices with until a name is typed.
NEW_PLUGIN_PLACEHOLDER: str = 'my_plugin'


def new_plugin_choices(name: Optional[str] = None) -> Dict[str, Any]:
    """
    What New plugin offers, from `bootstrap plugins` itself (`_core.bootstrap_plugin_choices`):
    `languages`, `features` (`[(key, label, example)]`), `default_features`, `packaging`
    (`{language: [single file, package]}` for `name`) and `directories`.
    """
    from meerschaum import _core
    name = str(name or '').strip() or NEW_PLUGIN_PLACEHOLDER
    choices = dict(_core.bootstrap_plugin_choices(name))
    features = []
    for key, description in choices.get('features', []):
        lines = [line.strip() for line in str(description).strip().splitlines() if line.strip()]
        label = lines[0] if lines else str(key)
        example = ' '.join(lines[1:]).strip()
        if example.startswith('(') and example.endswith(')'):
            example = example[1:-1]
        features.append((str(key), label, example))
    choices['features'] = features
    return choices


def plugin_url(name: str, source: Optional[str] = None, edit: bool = False, created: bool = False) -> str:
    """An installed plugin's page, with a file of its source (opened for editing when `edit`)."""
    from urllib.parse import urlencode
    query = {}
    if source:
        query['file'] = source
    if edit:
        query['edit'] = '1'
    if created:
        query['created'] = '1'
    return '/dash/plugins/installed/' + name + (('?' + urlencode(query)) if query else '')


def create_new_plugin(
    name: Any,
    language: Any,
    packaging: Any,
    features: Any,
    action: Any,
    dt_col: Any,
    directory: Any,
    session: Any,
) -> Tuple[bool, str, Optional[str]]:
    """
    Create a plugin with `bootstrap plugins`'s own scaffold (`_core.bootstrap_plugin`: the same
    checks of the name, features and action, and the same files; a plugin of that name is
    refused, nothing is overwritten). Returns `(success, message, URL of its source opened for
    editing)`. Only authors (`plugin_sources.authoring_refusal`, read fresh) may; a
    non-administrator's plugin is recorded as their team's (`claim_plugin`).
    """
    from meerschaum.api.dash.plugin_sources import authoring_refusal, source_viewer
    refusal = authoring_refusal(session)
    if refusal:
        return False, refusal, None
    try:
        who = source_viewer(session)
    except Exception:
        return False, MANAGE_REFUSAL, None
    name = str(name or '').strip()
    if not name:
        return False, 'Enter the name of your new plugin.', None
    if not is_safe_plugin_name(name):
        return False, f"Invalid plugin name: '{name}' (use letters, digits, '_' and '-').", None
    features = [str(feature) for feature in (features or []) if feature]
    try:
        if name in _discover_plugin_files() and not may_manage_plugin(who, name):
            return False, foreign_plugin_message(name), None
    except Exception as e:
        return False, f"Failed to create plugin '{name}': {e}", None
    try:
        from meerschaum import _core
        success, message, created = _core.bootstrap_plugin(
            name,
            str(language or 'Python'),
            packaging == 'package',
            features,
            str(action or ''),
            (str(dt_col).strip() or None) if dt_col else None,
            (str(directory) if directory else None),
        )
    except Exception as e:
        return False, f"Failed to create plugin '{name}': {e}", None
    if not success or not created:
        return False, message, None
    claimed, claim_message = claim_plugin(who, name)
    if not claimed:
        message = f"{message} {claim_message}"
    return True, message, plugin_url(name, created.get('source'), edit=True, created=True)


def build_installed_plugin_detail(
    name: str,
    session: Any,
    selected: Optional[str] = None,
    edit: bool = False,
    created: bool = False,
) -> Any:
    """
    An installed plugin's metadata and source, restricted to those who may manage it
    (`may_manage_plugin`: administrators, and its team's plugin managers; the authors among them
    edit its files: `selected` opened for editing when `edit`, as after New plugin, which also
    passes `created`). A plugin the viewer may not see is not installed, as far as they are told.
    """
    from meerschaum.api.dash.plugin_sources import may_read_source, language_logo, source_viewer
    back = dcc.Link('← All plugins', href='/dash/plugins', className='plugin-back-link')
    missing = html.Div([back, html.H5('This plugin is not installed.')], className='console-empty')
    try:
        who = source_viewer(session)
    except Exception:
        return missing
    if not is_safe_plugin_name(name) or not can_see_plugin(who, name):
        return missing
    installed = next((plugin for plugin in get_installed_plugins(check_packages=False)
                      if plugin['name'] == name), None)
    if not installed:
        return missing
    if not may_read_source('installed', session, name):
        return html.Div(
            [back, html.H5("Installed source is available to administrators and the plugin's team.")],
            className='console-empty',
        )
    from meerschaum.api.dash.connector_listing import mask_text
    description = installed.get('description') or _installed_description(name,session)
    actions = plugin_action_buttons(installed, size='md')
    actions.append(dbc.Button('Publish', id='plugin-publish-open', n_clicks=0, color='primary'))
    if installed.get('origin'):
        actions.append(update_check_store([installed]))
    info = [('Language',html.Span([language_logo(installed['kind']),
                                    html.Span(installed['kind'], **{'aria-hidden': 'true'})],
                                   className='plugin-detail-language')),
            ('Layout','Single file' if installed['layout']=='file' else 'Package'),
            ('Version',installed['version'] or 'Not declared'),
            ('Installed from',mask_text(installed.get('origin') or 'Local files'))]
    status = ('Failed to load' if installed.get('import_error') else
              'Not loaded' if installed.get('not_loaded') else 'Installed')
    details = []
    for label,value in info:
        details += [html.Dt(label),html.Dd(value)]
    requirements = installed['required']+installed.get('plugin_requirements',[])
    from meerschaum.api.dash.plugin_publish import build_publish_dialog
    return html.Div([
        back,
        html.Header([
            html.Div([html.H1(name),dbc.Badge(status,color='warning' if status!='Installed' else 'success')]),
            html.Div(actions,className='plugin-detail-actions'),
        ],className='plugin-detail-header'),
        _plugin_description(description),
        html.Div([
            html.Section([html.H2('Details'),html.Dl(details,className='plugin-detail-properties')]),
            html.Section([html.H2('Provides'),html.Div([
                dbc.Badge(item,color='secondary') for item in installed['provides']
            ],className='plugin-capabilities') if installed['provides'] else html.P('No capabilities declared.')]),
            html.Section([html.H2('Requirements'),
                html.Ul([html.Li(html.Code(item)) for item in requirements]) if requirements
                else html.P('No additional packages declared.',className='text-muted')]),
        ],className='plugin-detail-metadata'),
        _source_browser(
            'installed', name, session, selected=selected, edit=edit,
            notice=(dbc.Alert(f"Created plugin '{name}'.", color='success', dismissable=True,
                              id='plugin-created-alert') if created else None),
        ),
        build_publish_dialog(name, installed.get('origin'), session),
    ],className='plugin-detail-wrapper')


def _plugin_description(value):
    text = str(value or '').strip()
    if not text:
        return None
    first = text.split('\n\n',1)[0]
    preview = first[:500]+('…' if len(first)>500 else '')
    return html.Div([
        html.P(preview),
        html.Details([html.Summary('Full description'),html.Pre(text)]) if text!=preview else None,
    ],className='plugin-detail-description')


def _installed_description(name, session):
    """Read a bounded entry-point docstring without importing the plugin."""
    from meerschaum.api.dash.plugin_sources import source_files, read_source
    import ast
    import tomllib
    try:
        files = source_files('installed',name,session)
        if 'plugin.toml' in files:
            data = tomllib.loads(read_source('installed',name,'plugin.toml',session))
            return (data.get('plugin') or data.get('package') or data).get('description')
        first = next((file for file in files if file.endswith('.py')),None)
        if first:
            return ast.get_docstring(ast.parse(read_source('installed',name,first,session)))
    except Exception:
        pass
    return None


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
