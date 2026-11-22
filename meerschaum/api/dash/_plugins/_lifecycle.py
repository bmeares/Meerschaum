#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash-free plugin lifecycle operations: discovery & manifests, remote version checks,
source read/save with digest CAS, syntax token generation, template creation, upload
orchestration, repository description updates, action job runs, and team ownership
claim/release reconciliation.
"""

from __future__ import annotations

import ast
import json
import pathlib
import re
import threading
import time
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.parse import urlencode

import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.core import Plugin
from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, SuccessTuple, Tuple
import meerschaum.api.dash.viewer as viewer_model
from meerschaum.api.dash._plugins._archives import (
    decode_uploads,
    plan_upload,
    repository_source_files,
    write_plugin,
)
from meerschaum.api.dash._plugins._fs import (
    MAX_ARCHIVE_BYTES,
    MAX_ENTRIES,
    MAX_FILE_BYTES,
    MAX_FILES,
    MAX_SCAN_BYTES,
    MAX_UPLOAD_BYTES,
    SourceChanged,
    UploadError,
    _save_lock,
    decode_source_bytes,
    discover_plugin_files,
    existing_plugin_paths,
    installed_source_files,
    installed_source_target,
    is_safe_plugin_name,
    read_file_no_follow,
    relative_source_path,
    replace_file_cas,
    source_digest,
)
from meerschaum.api.dash._plugins._policy import (
    AUTHORING_ADMINS,
    MANAGE_REFUSAL,
    NO_WRITING_TEAM,
    PluginRights,
    foreign_plugin_message,
    is_plugin_owner,
    manageable_plugin_names,
    manages_everything,
    may_manage_plugin,
    may_manage_plugins,
    resolve_rights,
)

#: The repository keys an install from this server's own repository records as its origin: a
#: connector the console defines for that job only (`MRSM_API_THIS_SERVER` = the server's URL).
THIS_SERVER_REPOSITORY: str = 'api:this_server'

#: How long (minutes) an install job may read this server's plugins on a server that asks for
#: credentials (`--private`).
THIS_SERVER_TOKEN_MINUTES: int = 30

#: The name New plugin words its packaging choices with until a name is typed.
NEW_PLUGIN_PLACEHOLDER: str = 'my_plugin'

#: How long (seconds) a repository's answer for a plugin's latest version is reused.
REMOTE_VERSION_SECONDS: float = 15 * 60

#: How long (seconds) the update check waits for the repositories in all.
REMOTE_VERSION_TIMEOUT_SECONDS: float = 5.0

#: At most this many repository requests run at once, process-wide.
REMOTE_VERSION_WORKERS: int = 4

ACTION_WORDS: Dict[str, Tuple[str, str, str]] = {
    'install': ('Installing', 'Installed', 'install'),
    'reinstall': ('Reinstalling', 'Reinstalled', 'reinstall'),
    'update': ('Updating', 'Updated', 'update'),
    'uninstall': ('Uninstalling', 'Uninstalled', 'uninstall'),
}

#: The source viewer's language per suffix (Pygments lexer names).
SOURCE_MODES: Dict[str, str] = {
    '.py': 'python', '.rs': 'rust', '.toml': 'toml', '.json': 'json', '.yaml': 'yaml',
    '.yml': 'yaml', '.sql': 'sql', '.md': 'markdown', '.txt': 'text',
}

#: Token classes (`source-token-<name>`), most specific first.
TOKEN_STYLES = (
    ('Comment', 'comment'), ('Keyword', 'keyword'), ('Name.Function', 'function'),
    ('Name.Class', 'class'), ('Name.Builtin', 'builtin'), ('Name.Decorator', 'decorator'),
    ('Literal.String', 'string'), ('Literal.Number', 'number'), ('Operator', 'operator'),
)

#: At most this many highlighted runs per file.
MAX_TOKEN_RUNS: int = 60000

_remote_versions: Dict[Tuple[str, str], Tuple[float, Optional[str]]] = {}
_remote_versions_lock = threading.Lock()
_remote_version_pool: Optional[ThreadPoolExecutor] = None
_remote_versions_in_flight: Dict[Tuple[str, str], Any] = {}


########################################################################################
# Team plugin ownership: claim, release, and run reconciliation.
########################################################################################

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
        viewer_model.forget()
    except Exception:
        pass


def claim_plugin(viewer: Any, name: Any, conn: Any = None) -> SuccessTuple:
    """
    Record the plugin `name`, which `viewer` just installed, created or uploaded, as their
    team's (`writing_team`: its `plugins` attribute, read fresh), so its plugin managers manage
    it. Administrators record nothing.
    """
    if manages_everything(viewer):
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
    if manages_everything(viewer):
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
# Installed plugins discovery & manifests.
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
    for file_name, native in (
        ('plugins_manifest.json', False),
        ('native_plugins_manifest.json', True),
    ):
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


def _native_version(path: str) -> Optional[str]:
    """The version in a Rust plugin directory's `plugin.toml`."""
    try:
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


def get_installed_plugins(
    check_packages: bool = True,
    *,
    plugins_dirs: Optional[Iterable[Any]] = None,
) -> List[Dict[str, Any]]:
    """
    The plugins installed on this server, sorted by name: `{name, version, kind, layout,
    provides, required, missing, import_error, origin}`.
    """
    from meerschaum.plugins._origins import read_plugin_origins
    files = discover_plugin_files(plugins_dirs)
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
        required = [
            str(r) for r in (entry.get('required') or [])
            if r and not str(r).startswith('plugin:')
        ]
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
            'plugin_requirements': [
                str(r) for r in (entry.get('required') or [])
                if str(r).startswith('plugin:')
            ],
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


def installed_versions() -> Dict[str, Any]:
    """`{name: version}` of the installed plugins (`{}` when they cannot be read)."""
    try:
        return {p['name']: p.get('version') for p in get_installed_plugins(check_packages=False)}
    except Exception:
        return {}


########################################################################################
# Remote version checks.
########################################################################################

def remote_plugin_version(name: str, origin: str) -> Optional[str]:
    """
    The latest version of `name` in the repository `origin` (cached; `None` if unknown).
    """
    key = (str(origin), str(name))
    ### This server's repository is read fresh (a quick query); other repositories are cached.
    local = str(origin) == THIS_SERVER_REPOSITORY
    with _remote_versions_lock:
        cached = None if local else _remote_versions.get(key, None)
    if cached is not None and time.monotonic() - cached[0] < REMOTE_VERSION_SECONDS:
        return cached[1]
    try:
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
    """The pending request for `(origin, name)` on the shared bounded thread pool."""
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


def plugin_update_versions(
    names: List[str],
    viewer: Any = None,
    *,
    timeout: Optional[float] = None,
) -> Dict[str, str]:
    """
    `{name: newer version}` for the installed plugins among `names` whose origin repository has a
    newer version than the one installed; with a `viewer`, only the plugins they may manage.
    """
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
    futures = {
        _version_future(name, plugin['origin']): name
        for name, plugin in installed.items()
    }
    wait_timeout = REMOTE_VERSION_TIMEOUT_SECONDS if timeout is None else timeout
    done, pending = wait(futures, timeout=wait_timeout)
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


########################################################################################
# Source inspection, highlighting data, and CAS editing.
########################################################################################

def _validate_source_access(
    kind: str,
    name: str,
    rights: PluginRights,
    *,
    conn: Any = None,
) -> None:
    if kind not in ('installed', 'repository') or not is_safe_plugin_name(name):
        raise ValueError('Invalid plugin source selection.')
    if kind == 'installed':
        if not rights.can_read_source(kind, name):
            raise PermissionError('Plugin source is not available to your account.')
        return
    if not rights.can_read_source(kind):
        raise PermissionError('Plugin source is not available to your account.')
    api_conn = conn if conn is not None else get_api_connector()
    ### A repository plugin hidden from the viewer reads as one that is not there.
    if (
        not rights.can_read_source(kind, name)
        or name not in (api_conn.get_plugins(search_term=name) or [])
    ):
        raise ValueError('This plugin is not in the repository.')


def source_files(
    kind: str,
    name: str,
    rights: PluginRights,
    *,
    max_scan_bytes: int = MAX_SCAN_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_files: int = MAX_FILES,
    max_entries: int = MAX_ENTRIES,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    plugins_dirs: Optional[Iterable[Any]] = None,
    archives_root: Optional[pathlib.Path] = None,
    conn: Any = None,
) -> List[str]:
    _validate_source_access(kind, name, rights, conn=conn)
    if kind == 'installed':
        return installed_source_files(
            name, plugins_dirs, max_entries=max_entries, max_files=max_files,
        )
    return repository_source_files(
        name,
        archives_root=archives_root,
        max_archive_bytes=max_archive_bytes,
        max_scan_bytes=max_scan_bytes,
        max_entries=max_entries,
        max_files=max_files,
        max_file_bytes=max_file_bytes,
    )[0]


def read_source_with_digest(
    kind: str,
    name: str,
    selected: str,
    rights: PluginRights,
    *,
    max_scan_bytes: int = MAX_SCAN_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_files: int = MAX_FILES,
    max_entries: int = MAX_ENTRIES,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    plugins_dirs: Optional[Iterable[Any]] = None,
    archives_root: Optional[pathlib.Path] = None,
    conn: Any = None,
) -> Tuple[str, str]:
    """A source file's text and the digest of its bytes (what a save is checked against)."""
    _validate_source_access(kind, name, rights, conn=conn)
    relative_source_path(selected)
    if kind == 'installed':
        root, relative = installed_source_target(
            name, selected, plugins_dirs, max_entries=max_entries, max_files=max_files,
        )
        data = read_file_no_follow(root, relative, limit=max_file_bytes)
    else:
        files, data = repository_source_files(
            name,
            selected,
            archives_root=archives_root,
            max_archive_bytes=max_archive_bytes,
            max_scan_bytes=max_scan_bytes,
            max_entries=max_entries,
            max_files=max_files,
            max_file_bytes=max_file_bytes,
        )
        if selected not in files or data is None:
            raise ValueError('This source file is not available.')
    return decode_source_bytes(data), source_digest(data)


def read_source(kind: str, name: str, selected: str, rights: PluginRights, **kwargs) -> str:
    """The text of a plugin's listed source file `selected` (`read_source_with_digest`)."""
    return read_source_with_digest(kind, name, selected, rights, **kwargs)[0]


def save_source(
    name: str,
    selected: str,
    text: Any,
    expected: Any,
    rights: PluginRights,
    *,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_files: int = MAX_FILES,
    max_entries: int = MAX_ENTRIES,
    plugins_dirs: Optional[Iterable[Any]] = None,
) -> Tuple[bool, str, Optional[str]]:
    """
    Save `text` as an installed plugin's listed file `selected`, if it is unchanged on disk since
    it was loaded (its digest is `expected`). Returns `(success, message, new digest)`.
    """
    refusal = rights.authoring_refusal(name)
    if refusal:
        return False, refusal, None
    try:
        _validate_source_access('installed', name, rights)
    except PermissionError:
        return False, AUTHORING_ADMINS, None
    except ValueError as e:
        return False, str(e), None
    try:
        if not isinstance(text, str):
            raise ValueError('Nothing to save.')
        if not isinstance(expected, str) or len(expected) != 64:
            raise ValueError('Open the file for editing again before saving.')
        if '\0' in text:
            raise ValueError('Source files cannot contain NUL characters.')
        try:
            data = text.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('The text is not valid UTF-8.') from None
        if len(data) > max_file_bytes:
            raise ValueError(f'Source files are limited to {max_file_bytes // 1024} KiB.')
        with _save_lock:
            root, relative = installed_source_target(
                name, selected, plugins_dirs, max_entries=max_entries, max_files=max_files,
            )
            replace_file_cas(root, relative, data, expected, limit=max_file_bytes)
    except SourceChanged:
        return False, (
            f"{selected} changed on disk after it was opened, so it was not saved. "
            "Cancel to load the current file."
        ), None
    except (OSError, ValueError) as e:
        message = str(e) if isinstance(e, ValueError) else (e.strerror or str(e))
        return False, f"Failed to save {selected}: {message}", None
    return True, f"Saved {selected}.", source_digest(data)


def installed_description(name: str, rights: PluginRights) -> Optional[str]:
    """Read a bounded entry-point docstring without importing the plugin."""
    try:
        files = source_files('installed', name, rights)
        if 'plugin.toml' in files:
            data = tomllib.loads(read_source('installed', name, 'plugin.toml', rights))
            return (data.get('plugin') or data.get('package') or data).get('description')
        first = next((file for file in files if file.endswith('.py')), None)
        if first:
            return ast.get_docstring(ast.parse(read_source('installed', name, first, rights)))
    except Exception:
        pass
    return None


def source_mode(path: str) -> str:
    """The language a file is highlighted as."""
    return SOURCE_MODES.get(pathlib.PurePosixPath(path).suffix.lower(), 'text')


def _utf16_length(value: str) -> int:
    """A string's length in the browser (UTF-16 code units), which the runs count in."""
    return len(value.encode('utf-16-le')) // 2


def source_tokens(path: str, source: str, *, max_token_runs: int = MAX_TOKEN_RUNS) -> dict:
    """
    The highlighted source as data: the text, and runs of `[length, style]` (UTF-16 lengths;
    `style` indexes `styles`, `-1` is plain) that `assets/plugin_source.js` renders as escaped
    text with line numbers.
    """
    from pygments.lexers import get_lexer_by_name
    from pygments.token import string_to_tokentype
    mode = source_mode(path)
    categories = [(string_to_tokentype(name), index) for index, (name, _) in enumerate(TOKEN_STYLES)]
    runs: List[List[int]] = []
    plain = 0
    for _, token, value in get_lexer_by_name(mode).get_tokens_unprocessed(source):
        if not value:
            continue
        if len(runs) >= max_token_runs:
            plain += _utf16_length(value)
            continue
        style = next((index for category, index in categories if token in category), -1)
        length = _utf16_length(value)
        if runs and runs[-1][1] == style:
            runs[-1][0] += length
        else:
            runs.append([length, style])
    if plain:
        runs.append([plain, -1])
    return {
        'path': path,
        'mode': mode,
        'source': source,
        'styles': [name for _, name in TOKEN_STYLES],
        'runs': runs,
    }


########################################################################################
# Creation, upload, description update, and action runs.
########################################################################################

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


def plugin_url(
    name: str,
    source: Optional[str] = None,
    edit: bool = False,
    created: bool = False,
) -> str:
    """An installed plugin's page, with a file of its source (opened for editing when `edit`)."""
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
    rights: PluginRights,
    *,
    conn: Any = None,
) -> Tuple[bool, str, Optional[str]]:
    """
    Create a plugin with `bootstrap plugins`'s own scaffold (`_core.bootstrap_plugin`).
    Returns `(success, message, URL of its source opened for editing)`.
    """
    refusal = rights.authoring_refusal()
    if refusal:
        return False, refusal, None
    who = rights.viewer
    name = str(name or '').strip()
    if not name:
        return False, 'Enter the name of your new plugin.', None
    if not is_safe_plugin_name(name):
        return False, f"Invalid plugin name: '{name}' (use letters, digits, '_' and '-').", None
    features = [str(feature) for feature in (features or []) if feature]
    try:
        if name in discover_plugin_files() and not may_manage_plugin(who, name):
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
    claimed, claim_message = claim_plugin(who, name, conn=conn)
    if not claimed:
        message = f"{message} {claim_message}"
    return True, message, plugin_url(name, created.get('source'), edit=True, created=True)


def upload_plugin(
    contents: Any,
    filenames: Any,
    name: Any,
    directory: Any,
    replace: Any,
    rights: PluginRights,
    *,
    limit: int = MAX_UPLOAD_BYTES,
    max_files: Optional[int] = None,
    conn: Any = None,
) -> Tuple[bool, str, Optional[str]]:
    """
    Install the files a `dcc.Upload` holds as a plugin. Returns `(success, message, URL of the
    installed plugin's page)`.
    """
    refusal = rights.authoring_refusal()
    if refusal:
        return False, refusal, None
    who = rights.viewer
    try:
        plan = plan_upload(
            decode_uploads(contents, filenames, limit, max_files=max_files),
            name=name,
            limit=limit,
            max_files=max_files,
        )
        name = plan['name']
        directories = [str(path) for path in new_plugin_choices(name).get('directories', [])]
        if not directories:
            raise UploadError('This server has no plugins directory to upload to.')
        directory = str(directory) if directory else directories[0]
        if directory not in directories:
            raise UploadError(f"'{directory}' is not one of the plugins directories.")
        installed = discover_plugin_files().get(name)
        taken = [
            path for candidate in directories
            for path in existing_plugin_paths(pathlib.Path(candidate), name)
        ]
        if installed or taken:
            if not may_manage_plugin(who, name):
                raise UploadError(foreign_plugin_message(name))
            if replace is not True:
                raise UploadError(
                    f"Plugin '{name}' already exists. Tick Replace existing to overwrite it."
                )
            ### Replaced where it is, so no second copy shadows it from another directory.
            homes = {str(path.parent) for path in taken}
            if len(homes) > 1:
                raise UploadError(f"Plugin '{name}' is in more than one plugins directory.")
            if homes:
                directory = next(iter(homes))
        write_plugin(plan, directory, replace=(replace is True))
    except UploadError as e:
        return False, str(e), None
    except Exception as e:
        return False, f"Failed to upload the plugin: {e}", None
    message = f"Uploaded plugin '{name}'."
    claimed, claim_message = claim_plugin(who, name, conn=conn)
    if not claimed:
        message += ' ' + claim_message
    return True, message, plugin_url(name)


def update_plugin_description(
    plugin_name: str,
    description: Optional[str],
    username: Optional[str],
    *,
    conn: Any = None,
    debug: bool = False,
) -> SuccessTuple:
    """
    Update a published plugin's description when `username` owns it.
    """
    if not is_plugin_owner(plugin_name, username, conn=conn, debug=debug):
        return (
            False,
            f"Failed to update description for plugin '{plugin_name}'"
            + " due to insufficient permissions.",
        )
    api_conn = conn if conn is not None else get_api_connector()
    plugin = Plugin(plugin_name)
    plugin.user_id = api_conn.get_plugin_user_id(plugin, debug=debug)
    plugin.attributes = api_conn.get_plugin_attributes(plugin, debug=debug) or {}
    plugin.attributes.update({'description': description})
    success, msg = api_conn.register_plugin(plugin, debug=debug, force=True)
    return success, (msg if not success else "Successfully updated description.")


def plugins_read_token(username: str, minutes: int = THIS_SERVER_TOKEN_MINUTES) -> str:
    """
    A bearer token of `username`, signed with the secret this server checks, that may only read
    plugins (`plugins:read`) and expires after `minutes`.
    """
    from meerschaum.api.host._server_client import mint_user_bearer
    return str(mint_user_bearer(
        username,
        scopes=['plugins:read'],
        minutes=minutes,
        respect_no_auth=False,
    ) or '')


def this_server_env(username: Optional[str] = None) -> Dict[str, str]:
    """
    The environment that makes `api:this_server` this server (for an install job).
    """
    from meerschaum.api import no_auth, private
    from meerschaum.api.host._server_client import server_url
    url = server_url()
    if not url:
        return {}
    key = 'MRSM_API_' + THIS_SERVER_REPOSITORY.split(':', 1)[1].upper()
    if not private or no_auth or not username:
        return {key: url}
    return {key: json.dumps({'uri': url, 'api_key': plugins_read_token(username)})}


def plugin_action_sysargs(
    action: str,
    name: str,
    origin: Optional[str] = None,
    username: Optional[str] = None,
) -> Tuple[Optional[List[str]], Dict[str, str], str]:
    """
    Return `(sysargs, env, message)` for a plugin action: `install`, `reinstall`, `update`,
    or `uninstall`.
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
    Run a plugin action as one of the console's runs (`execute-*` job owned by `username`).
    """
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
        ### The run is the user's, in their primary team.
        from meerschaum.jobs import ownership_properties
        properties.update(ownership_properties(username, viewer_model.viewer_for(username).primary_team))
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
        properties = (
            job.daemon.properties
            if hasattr(job, 'daemon')
            else job.get_daemon_properties()
        ) or {}
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


def reconcile_plugin_run(
    job_name: str,
    viewer: Any,
    *,
    fallback_plugin: Optional[str] = None,
    job_loader: Optional[Any] = None,
    is_visible: Optional[Any] = None,
    conn: Any = None,
) -> Dict[str, Any]:
    """
    Inspect a finished or running console plugin job (`job_name`) for `viewer` and, once
    finished, reconcile team ownership (`claim_plugin` on install/reinstall/update,
    `release_plugin` on uninstall). Completely Dash-free.

    Returns a dict:
      - `status`: `'denied' | 'running' | 'done'`
      - When `status == 'done'`: `success`, `message`, `action`, `plugin`, `recorded`
    """
    rights = resolve_rights(viewer, webterm_enabled=False, registration_enabled=False)
    if viewer is None or not viewer.active or not rights.can_manage_any:
        return {'status': 'denied'}
    if job_loader is None:
        from meerschaum.api.dash._support import get_job
        job_loader = get_job
    record = None
    try:
        job = job_loader(str(job_name))
        exists = bool(job.exists())
        if exists and is_visible is not None:
            exists = bool(is_visible(job))
        ### Only a plugin run this console started for this user is reconciled.
        record = plugin_run_record(job, viewer.username) if exists else None
        exists = exists and record is not None
        status = job.status if exists else 'stopped'
        result = job.result if exists else (False, 'The job went away.')
    except Exception as e:
        status, result = 'stopped', (False, str(e))
    if status in ('running', 'paused'):
        return {'status': 'running', 'job_status': status}
    record = record or {}
    action = record.get('action') or 'install'
    plugin = record.get('plugin') or fallback_plugin
    done, verb = ACTION_WORDS.get(action, ACTION_WORDS['install'])[1:]
    success = bool(result and result[0])
    message = (
        f"{done} plugin '{plugin}'."
        if success
        else f"Failed to {verb} plugin '{plugin}'."
    )
    recorded: Optional[bool] = None
    if success and record and not rights.manages_everything:
        recorded, note = (
            release_plugin(viewer, plugin, conn=conn)
            if action == 'uninstall'
            else claim_plugin(viewer, plugin, conn=conn)
        )
        if not recorded:
            message = f"{message} {note}"
    return {
        'status': 'done',
        'success': success,
        'message': message,
        'action': action,
        'plugin': plugin,
        'recorded': recorded,
    }
