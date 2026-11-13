#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
What a team is created with, for the Users & Teams page's New team and Settings dialogs (the
same choices as `bootstrap teams`):

- **isolated**: the team's members do not see what belongs to Everyone;
- **storage**: a new database or a new schema on the server of an existing PostgreSQL-family
  connector (a role for the team, a read-only role for dashboards and a connector reserved for
  the team, made by `_core.team_database_create`), or an existing instance connector;
- **grants**: the connectors and plugins the team may use, and whether its members may install,
  edit and upload plugins;
- **Grafana** (`api:grafana`): the team's organization, with a data source reading the team's
  database through the read-only role.

A team's settings are its record's `attributes` (`isolated`, `instance`, `connectors`,
`plugins`, `plugins_write`, `database`, `grafana`), which `meerschaum.core.access.team_grants`
reads. Only administrators create teams and change their settings (checked by the callbacks).

The connector of a new database is written to the configuration
(`meerschaum:connectors:sql:<label>`) and to this process's live config; the API server and the
jobs read the file (`refresh_connectors()` does the same here for connectors another process
wrote).
"""

from __future__ import annotations

import copy
import os
import threading

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple, Tuple

#: The storage choices of the New team dialog: `(value, label)`.
STORAGE_CHOICES: Tuple[Tuple[str, str], ...] = (
    ('database', 'New database'),
    ('schema', 'New schema'),
    ('instance', 'Existing instance'),
)

#: The flavors a team database can be made on (`_core.team_database_supported`).
DATABASE_FLAVORS: Tuple[str, ...] = (
    'timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus',
)

_LOCK = threading.RLock()
_connectors_mtime: Optional[float] = None


def _core():
    from meerschaum import _core as core
    return core


def grafana_config() -> Dict[str, Any]:
    """`api:grafana` (`{}` when unset)."""
    from meerschaum.config import get_config
    section = get_config('api', 'grafana', warn=False)
    return dict(section) if isinstance(section, dict) else {}


def grafana_enabled() -> bool:
    """Whether the console signs its users in to a Grafana (`api:grafana:enabled` and `url`)."""
    section = grafana_config()
    return bool(section.get('enabled', False) and section.get('url', None))


def _connectors_section() -> Dict[str, Any]:
    from meerschaum.config import get_config
    section = get_config('meerschaum', 'connectors', warn=False)
    return section if isinstance(section, dict) else {}


def connector_attributes(keys: str) -> Dict[str, Any]:
    """The configured attributes of a connector (`default` applied), without building it."""
    typ, _, label = str(keys).partition(':')
    labels = _connectors_section().get(typ, None) or {}
    attributes = dict(labels.get('default', None) or {})
    attributes.update(labels.get(label, None) or {})
    return attributes


def database_bases() -> List[str]:
    """The `sql:` connectors a team database can branch from (the PostgreSQL family)."""
    labels = _connectors_section().get('sql', None) or {}
    bases = []
    for label in labels:
        if label == 'default':
            continue
        attributes = connector_attributes(f'sql:{label}')
        flavor = str(attributes.get('flavor', '') or '')
        if not flavor and isinstance(attributes.get('uri', None), str):
            flavor = attributes['uri'].split('://', 1)[0].split('+', 1)[0]
        ### A team's own connector is not a base for another team.
        if flavor in DATABASE_FLAVORS and not attributes.get('teams', None):
            bases.append(f'sql:{label}')
    return bases


def default_base() -> Optional[str]:
    """The base offered first: the API's own instance when it qualifies, else `sql:main`."""
    bases = database_bases()
    try:
        from meerschaum.api import get_api_connector
        own = str(get_api_connector())
    except Exception:
        own = None
    for candidate in (own, 'sql:main'):
        if candidate in bases:
            return candidate
    return bases[0] if bases else None


def instance_choices() -> List[str]:
    """The instance connectors a team's default instance may be."""
    from meerschaum.connectors import instance_types
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.api.dash.connector_listing import is_internal_connector
    return [
        keys for keys in get_connector_labels(*instance_types)
        if not is_internal_connector(*keys.split(':', 1))
    ]


def connector_choices() -> List[str]:
    """Every connector a team may be granted (data plugins as `plugin:<name>` included)."""
    from meerschaum.api.dash.connector_listing import list_connectors
    try:
        return [row['keys'] for row in list_connectors(None)]
    except Exception:
        return instance_choices()


def plugin_choices() -> List[str]:
    """The installed plugins a team may be granted."""
    try:
        from meerschaum.api.dash.plugins import get_installed_plugins
        return [plugin['name'] for plugin in get_installed_plugins(check_packages=False)]
    except Exception:
        return []


def slug(name: Optional[str]) -> str:
    """The default database, schema and connector label of a team named `name`."""
    try:
        return str(_core().team_database_slug(str(name or '')) or '')
    except Exception:
        return ''


def _names(values: Any) -> List[str]:
    if isinstance(values, str):
        values = values.split(',')
    out: List[str] = []
    for value in (values or []):
        text = str(value).strip()
        if text and text not in out:
            out.append(text)
    return out


def team_attributes(
    isolated: bool = False,
    instance: Optional[str] = None,
    connectors: Any = None,
    plugins: Any = None,
    plugins_write: bool = False,
    database: Optional[Dict[str, Any]] = None,
    grafana_org: Optional[int] = None,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    A team's `attributes` for these settings (`existing`: the stored attributes, whose other
    keys, `database` and `grafana` are kept).
    """
    attributes = copy.deepcopy(existing or {})
    for key in ('isolated', 'instance', 'connectors', 'plugins', 'plugins_write'):
        attributes.pop(key, None)
    if isolated:
        attributes['isolated'] = True
    granted = _names(connectors)
    if instance:
        attributes['instance'] = str(instance)
        granted = [str(instance)] + [keys for keys in granted if keys != str(instance)]
    if granted:
        attributes['connectors'] = granted
    if _names(plugins):
        attributes['plugins'] = _names(plugins)
    if plugins_write:
        attributes['plugins_write'] = True
    if database is not None:
        attributes['database'] = dict(database)
    if grafana_org is not None:
        attributes['grafana'] = {'org_id': int(grafana_org)}
    return attributes


def team_settings(attributes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The settings a team's `attributes` hold, with defaults (for the Settings dialog)."""
    attributes = attributes or {}
    database = attributes.get('database', None)
    return {
        'isolated': bool(attributes.get('isolated', False)),
        'instance': attributes.get('instance', None) or None,
        'connectors': _names(attributes.get('connectors', None)),
        'plugins': _names(attributes.get('plugins', None)),
        'plugins_write': bool(attributes.get('plugins_write', False)),
        'database': dict(database) if isinstance(database, dict) else None,
    }


def write_connector(keys: str, attributes: Dict[str, Any]) -> SuccessTuple:
    """
    Add the connector `keys` to the configuration file and to this process's live config (only
    that connector changes; an existing one is refused).
    """
    from meerschaum.config import get_config
    from meerschaum.config._read_config import read_config
    from meerschaum.config._edit import write_config
    typ, _, label = str(keys).partition(':')
    with _LOCK:
        try:
            disk = read_config(
                keys=['meerschaum'], write_missing=False, substitute=False,
                raise_parsing_errors=True,
            )
        except Exception as e:
            return False, f"The configuration could not be read: {e}"
        section = disk.setdefault('meerschaum', {})
        labels = section.setdefault('connectors', {}).setdefault(typ, {})
        if labels.get(label, None):
            return False, f"The connector '{keys}' already exists."
        labels[label] = copy.deepcopy(attributes)
        if not write_config({'meerschaum': section}):
            return False, f"The connector '{keys}' could not be written to the configuration."
        live = get_config('meerschaum', 'connectors', warn=False)
        if isinstance(live, dict):
            live.setdefault(typ, {})[label] = copy.deepcopy(attributes)
        _remember_mtime()
    return True, "Success"


def _connectors_file() -> Optional[str]:
    try:
        from meerschaum.config.paths import CONFIG_DIR_PATH
        for name in ('meerschaum.yaml', 'meerschaum.yml', 'meerschaum.json'):
            path = os.path.join(str(CONFIG_DIR_PATH), name)
            if os.path.exists(path):
                return path
    except Exception:
        pass
    return None


def _remember_mtime() -> None:
    global _connectors_mtime
    path = _connectors_file()
    try:
        _connectors_mtime = os.path.getmtime(path) if path else None
    except OSError:
        _connectors_mtime = None


def refresh_connectors() -> bool:
    """
    Pick up the connectors another process wrote to the configuration file since this one read it
    (a team's database made with `bootstrap teams`): new labels are added to the live config, and
    nothing in memory is replaced or removed. Returns whether anything was added. Cheap: one
    `stat` unless the file changed.
    """
    global _connectors_mtime
    path = _connectors_file()
    if path is None:
        return False
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return False
    if _connectors_mtime is None:
        _connectors_mtime = mtime
        return False
    if mtime == _connectors_mtime:
        return False
    added = False
    with _LOCK:
        _connectors_mtime = mtime
        try:
            from meerschaum.config import get_config
            from meerschaum.config._read_config import read_config
            disk = read_config(keys=['meerschaum'], write_missing=False, substitute=True)
            fresh = (disk.get('meerschaum', None) or {}).get('connectors', None) or {}
            live = get_config('meerschaum', 'connectors', warn=False)
            if not isinstance(live, dict):
                return False
            for typ, labels in fresh.items():
                if not isinstance(labels, dict):
                    continue
                for label, attributes in labels.items():
                    if label not in live.setdefault(typ, {}):
                        live[typ][label] = copy.deepcopy(attributes)
                        added = True
        except Exception:
            return False
    return added


def create_team(
    name: str,
    isolated: bool = True,
    storage: str = 'instance',
    base: Optional[str] = None,
    database_name: Optional[str] = None,
    instance: Optional[str] = None,
    connectors: Any = None,
    plugins: Any = None,
    plugins_write: bool = False,
    conn: Any = None,
) -> SuccessTuple:
    """
    Create a team with its settings (administrators only, checked by the caller).

    With `storage` `'database'` or `'schema'`, the team's database (or schema), roles and
    connector `sql:<slug>` are made first on the server of `base`, and that connector becomes the
    team's default instance; with `'instance'`, `instance` (if any) is its default instance.
    """
    from meerschaum.api.dash import access
    name = str(name or '').strip()
    storage = str(storage or 'instance')
    if storage not in ('database', 'schema', 'instance'):
        return False, f"Unknown storage '{storage}'."

    made: Optional[Dict[str, Any]] = None
    grafana_org: Optional[int] = None
    notes: List[str] = []
    if storage in ('database', 'schema'):
        label = slug(name)
        if not label:
            return False, f"The team name '{name}' has no letters or digits."
        database_name = str(database_name or '').strip() or label
        valid, message = _core().team_database_valid_name(database_name)
        if not valid:
            return False, message
        keys = f'sql:{label}'
        if connector_attributes(keys):
            return False, f"The connector '{keys}' already exists."
        base = str(base or default_base() or '')
        if base not in database_bases():
            return False, "Choose a PostgreSQL-family connector to branch from."
        try:
            success, message, made = _core().team_database_create(
                base,
                connector_attributes(base),
                name,
                storage,
                keys,
                name=database_name,
                grafana=(grafana_config() if grafana_enabled() else None),
            )
        except Exception as e:
            return False, f"Failed to create the team's {storage}: {e}"
        if not success or not made:
            return False, f"Failed to create the team's {storage}: {message}"
        written, message = write_connector(keys, made['attributes'])
        if not written:
            return False, (
                f"The {storage} '{database_name}' was created, but its connector was not saved: "
                f"{message}"
            )
        instance = keys
        grafana_org = made.get('grafana', None)
        notes = [str(note) for note in (made.get('notes', None) or [])]
        if made.get('grafana_error', None):
            notes.append(str(made['grafana_error']))
    elif grafana_enabled():
        try:
            success, message, grafana_org = _core().grafana_ensure_team(grafana_config(), name)
            if not success:
                notes.append(message)
        except Exception as e:
            notes.append(str(e))

    attributes = team_attributes(
        isolated=bool(isolated),
        instance=instance,
        connectors=connectors,
        plugins=plugins,
        plugins_write=bool(plugins_write),
        database=(made['summary'] if made else None),
        grafana_org=grafana_org,
    )
    try:
        team = mrsm.Team(name, instance=(conn if conn is not None else _api_connector()),
                         attributes=attributes)
        success, message = team.register()
    except Exception as e:
        success, message = False, str(e)
    if not success:
        if made:
            return False, (
                f"The connector '{instance}' and its {storage} were created, but the team was "
                f"not registered: {message}"
            )
        return False, message
    access.forget()
    message = f"Created team '{name}'"
    if made:
        message += f" with the {storage} '{database_name}' (connector '{instance}')"
    message += '.'
    if notes:
        message += '\n' + '\n'.join(notes)
    return True, message


def _api_connector() -> Any:
    from meerschaum.api import get_api_connector
    return get_api_connector()


def get_team_attributes(name: str, conn: Any = None) -> Optional[Dict[str, Any]]:
    """A team's stored attributes (`None` when the team does not exist)."""
    conn = conn if conn is not None else _api_connector()
    for team in (conn.get_teams() or []):
        team_name = str(getattr(team, 'name', None) or (team.get('name') if isinstance(team, dict) else ''))
        if team_name.lower() == str(name).lower():
            attributes = getattr(team, 'attributes', None)
            if attributes is None and isinstance(team, dict):
                attributes = team.get('attributes', None)
            return dict(attributes or {})
    return None


def edit_team_settings(
    name: str,
    isolated: bool = False,
    instance: Optional[str] = None,
    connectors: Any = None,
    plugins: Any = None,
    plugins_write: bool = False,
    conn: Any = None,
) -> SuccessTuple:
    """
    Change a team's settings (administrators only, checked by the caller). Its database and
    Grafana organization are kept as they are.
    """
    from meerschaum.api.dash import access
    conn = conn if conn is not None else _api_connector()
    existing = get_team_attributes(name, conn)
    if existing is None:
        return False, f"Team '{name}' does not exist."
    try:
        team = mrsm.Team(str(name), instance=conn)
        attributes = team.attributes
        new = team_attributes(
            isolated=bool(isolated),
            instance=instance,
            connectors=connectors,
            plugins=plugins,
            plugins_write=bool(plugins_write),
            existing=existing,
        )
        attributes.clear()
        attributes.update(new)
        success, message = team.edit()
    except Exception as e:
        return False, str(e)
    if not success:
        return False, message
    access.forget()
    return True, f"Saved the settings of team '{name}'."


def rename_grafana_team(old: str, new: str) -> None:
    """Rename a renamed team's Grafana organization (best effort)."""
    if not grafana_enabled():
        return
    try:
        _core().grafana_rename_team(grafana_config(), str(old), str(new))
    except Exception:
        pass
